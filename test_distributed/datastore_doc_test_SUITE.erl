%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains transaction process tests.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_doc_test_SUITE).
-author("Krzysztof Trzepla").

-include("modules/tp/tp.hrl").
-include("modules/datastore/datastore_doc.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    run_should_process_requests_in_order/1,
    run_multiple_parallel_requests_for_same_key_should_return_responses/1,
    datastore_doc_should_call_init_callback_on_init/1,
    datastore_doc_should_call_modify_callback_on_request/1,
    datastore_doc_should_forward_modify_exception/1,
    datastore_doc_should_ignore_modify/1,
    datastore_doc_should_commit_changes/1,
    datastore_doc_should_retry_commit_changes/1,
    datastore_doc_should_commit_changes_on_terminate/1,
    datastore_doc_should_call_terminate_callback_on_terminate/1,
    datastore_doc_should_terminate_when_idle_timeout_exceeded/1,
    datastore_doc_should_terminate_on_exception/1
]).

%% test_bases
-export([
    run_multiple_parallel_requests_for_same_key_should_return_responses_base/1
]).

all() ->
    ?ALL([
        run_should_process_requests_in_order,
        run_multiple_parallel_requests_for_same_key_should_return_responses,
        datastore_doc_should_call_init_callback_on_init,
        datastore_doc_should_call_modify_callback_on_request,
        datastore_doc_should_forward_modify_exception,
        datastore_doc_should_ignore_modify,
        datastore_doc_should_commit_changes,
        datastore_doc_should_retry_commit_changes,
        datastore_doc_should_commit_changes_on_terminate,
        datastore_doc_should_call_terminate_callback_on_terminate,
        datastore_doc_should_terminate_when_idle_timeout_exceeded,
        datastore_doc_should_terminate_on_exception
    ], [
        run_multiple_parallel_requests_for_same_key_should_return_responses
    ]).

-define(TP_ARGS, [tp_arg1, tp_arg2, tp_arg3]).
-define(TP_KEY, <<"key">>).
-define(DRIVER, memory_store_driver).
-define(TIMEOUT, timer:seconds(5)).

-define(PERF_PARAM(Name, Value, Unit, Description), [
    {name, Name},
    {value, Value},
    {description, Description},
    {unit, Unit}
]).
-define(PERF_CFG(Name, Params), {config, [
    {name, Name},
    {description, atom_to_list(Name)},
    {parameters, Params}
]}).
-define(OPS_NUM(Value), ?PERF_PARAM(ops_num, Value, "", "Number of operations.")).
-define(OP_TIME(Value), ?PERF_PARAM(op_time, Value, "ms",
    "Maximal duration of a single operation.")).
-define(OP_DELAY(Value), ?PERF_PARAM(op_delay, Value, "ms",
    "Maximal delay before single operation.")).
-define(THR_NUM(Value), ?PERF_PARAM(threads_num, Value, "Number of threads.")).

%%%===================================================================
%%% Test functions
%%%===================================================================

run_should_process_requests_in_order(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    CommitDelay = ?config(commit_delay, Config),

    lists:foreach(fun(N) ->
        rpc:call(Worker, datastore_doc, run_async, [?TP_ARGS, ?TP_KEY, fun() ->
            timer:sleep(rand:uniform(2 * CommitDelay)),
            Self ! N,
            N
        end])
    end, lists:seq(1, 10)),

    lists:foreach(fun(N) ->
        ?assertReceivedNextEqual(N, 2 * CommitDelay)
    end, lists:seq(1, 10)).

run_multiple_parallel_requests_for_same_key_should_return_responses(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 100},
        {parameters, [?OPS_NUM(100), ?OP_TIME(50), ?OP_DELAY(100)]},
        {description, "Multiple parallel operations for the same key."},
        ?PERF_CFG(small, [?OPS_NUM(1000), ?OP_TIME(30), ?OP_DELAY(6000)]),
        ?PERF_CFG(medium, [?OPS_NUM(5000), ?OP_TIME(6), ?OP_DELAY(30000)]),
        ?PERF_CFG(large, [?OPS_NUM(10000), ?OP_TIME(3), ?OP_DELAY(60000)])
    ]).
run_multiple_parallel_requests_for_same_key_should_return_responses_base(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    OpsNum = ?config(ops_num, Config),
    OpTime = ?config(op_time, Config),
    OpTimeout = OpsNum * OpTime,
    OpDelay = ?config(op_delay, Config),
    Ids = lists:seq(1, OpsNum),

    ?assertEqual(Ids, utils:pmap(fun(N) ->
        timer:sleep(rand:uniform(OpDelay)),
        rpc:call(Worker, datastore_doc, run_sync, [?TP_ARGS, ?TP_KEY, fun() ->
            timer:sleep(rand:uniform(OpTime)),
            N
        end, OpTimeout])
    end, Ids)),

    receive_commit(Ids, ?config(commit_delay, Config)),
    {ok, Pid} = ?assertMatch({ok, _}, rpc:call(Worker, tp_router, get,
        [?TP_KEY])),
    stop_datastore_doc(Worker, Pid).

datastore_doc_should_call_init_callback_on_init(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, Pid} = start_datastore_doc(Worker),
    test_utils:mock_assert_num_calls(Worker, ?DRIVER, init,
        [?TP_ARGS], 1),
    stop_datastore_doc(Worker, Pid).

datastore_doc_should_call_modify_callback_on_request(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, Pid} = start_datastore_doc(Worker),
    {ok, Ref} = ?assertMatch({ok, _}, gen_server2:call(Pid, request)),
    ?assertReceivedEqual({Ref, request}, ?TIMEOUT),
    stop_datastore_doc(Worker, Pid).

datastore_doc_should_forward_modify_exception(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, unexpected_error, _}, rpc:call(
        Worker, datastore_doc, run_sync, [?TP_ARGS, ?TP_KEY, request]
    )).

datastore_doc_should_ignore_modify(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(request, rpc:call(Worker, datastore_doc, run_sync, [
        ?TP_ARGS, ?TP_KEY, request
    ])),
    ?assertEqual({error, not_found}, rpc:call(
        Worker, tp_router, get, [?TP_KEY]
    ), 3, ?config(idle_timeout, Config)).

datastore_doc_should_commit_changes(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    CommitDelay = ?config(commit_delay, Config),

    lists:foreach(fun(N) ->
        rpc:call(Worker, datastore_doc, run_async, [?TP_ARGS, ?TP_KEY, N])
    end, lists:seq(1, 10)),

    ?assertReceivedNextEqual({committed, lists:seq(1, 10)}, 2 * CommitDelay),
    ?assertNotReceivedMatch({committed, _}, ?TIMEOUT).

datastore_doc_should_retry_commit_changes(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    CommitDelay = ?config(commit_delay, Config),

    lists:foreach(fun(N) ->
        rpc:call(Worker, datastore_doc, run_async, [?TP_ARGS, ?TP_KEY, N])
    end, lists:seq(1, 10)),

    ?assertReceivedNextEqual({not_committed, lists:seq(1, 10)}, 2 * CommitDelay),
    ?assertReceivedNextEqual(backoff, ?TIMEOUT),
    ?assertReceivedNextEqual({not_committed, lists:seq(2, 10)}, 2 * CommitDelay).

datastore_doc_should_commit_changes_on_terminate(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    CommitDelay = ?config(commit_delay, Config),

    Pid = spawn(Worker, fun Loop() ->
        receive
            {run, Request} ->
                datastore_doc:run_async(?TP_ARGS, ?TP_KEY, Request),
                Self ! tp_router:get(?TP_KEY);
            {_, Response} ->
                Self ! Response
        end,
        Loop()
    end),

    Pid ! {run, fun() -> timer:sleep(2 * CommitDelay), 1 end},
    {ok, Pid2} = ?assertReceivedNextMatch({ok, _}, ?TIMEOUT),
    Pid ! {run, fun() -> timer:sleep(2 * CommitDelay), 2 end},
    ?assertReceivedNextEqual({ok, Pid2}, ?TIMEOUT),
    stop_datastore_doc(Worker, Pid2),

    ?assertReceivedNextEqual(1, 3 * CommitDelay),
    ?assertReceivedNextEqual(2, 3 * CommitDelay).

datastore_doc_should_call_terminate_callback_on_terminate(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, Pid} = start_datastore_doc(Worker),
    ?assertEqual(1, rpc:call(
        Worker, meck, num_calls, [?DRIVER, terminate, [?TP_ARGS, '_']]
    ), 3, ?config(idle_timeout, Config)),
    stop_datastore_doc(Worker, Pid).

datastore_doc_should_terminate_when_idle_timeout_exceeded(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_doc, run_sync, [?TP_ARGS, ?TP_KEY, request]),
    ?assertEqual(1, rpc:call(
        Worker, meck, num_calls, [?DRIVER, terminate, [?TP_ARGS, '_']]
    ), 3, ?config(idle_timeout, Config)).

datastore_doc_should_terminate_on_exception(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_doc, run_sync, [?TP_ARGS, ?TP_KEY, request]),
    rpc:call(Worker, datastore_doc, run_sync, [?TP_ARGS, ?TP_KEY, request]),
    ?assertEqual({error, not_found}, rpc:call(
        Worker, tp_router, get, [?TP_KEY]
    ), 3, ?config(idle_timeout, Config)).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_testcase(Case, Config) when
    Case =:= run_multiple_parallel_requests_for_same_key_should_return_responses;
    Case =:= datastore_doc_should_commit_changes ->
    Self = self(),
    init_per_testcase(?DEFAULT_CASE(Case), [
        {commit_fun, fun(Changes, _) ->
            Self ! {committed, Changes},
            {true, []}
        end},
        {mock_opts, [no_history]}
        | Config
    ]);
init_per_testcase(datastore_doc_should_forward_modify_exception = Case, Config) ->
    init_per_testcase(?DEFAULT_CASE(Case), [
        {modify_fun, fun(_Requests, _Data, _Rev) ->
            meck:exception(error, unexpected_error)
        end} | Config
    ]);
init_per_testcase(datastore_doc_should_ignore_modify = Case, Config) ->
    init_per_testcase(?DEFAULT_CASE(Case), [
        {modify_fun, fun(Requests, Data, _Rev) ->
            {Requests, false, Data}
        end} | Config
    ]);
init_per_testcase(datastore_doc_should_retry_commit_changes = Case, Config) ->
    Self = self(),
    init_per_testcase(?DEFAULT_CASE(Case), [
        {commit_fun, fun(Changes, _) ->
            Self ! {not_committed, Changes},
            case Changes of
                [_] -> {true, []};
                _ -> {{false, tl(Changes)}, []}
            end
        end},
        {commit_backoff_fun, fun(CommitDelay) ->
            Self ! backoff,
            CommitDelay
        end}
        | Config
    ]);
init_per_testcase(datastore_doc_should_terminate_on_exception = Case, Config) ->
    init_per_testcase(?DEFAULT_CASE(Case), [
        {merge_changes_fun, fun(_, _) ->
            meck:exception(error, unexpected_error)
        end} | Config
    ]);
init_per_testcase(_Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    CommitDelay = timer:seconds(1),
    IdleTimeout = timer:seconds(3),
    ModifyFun = fun(Requests, Data, _Rev) ->
        Responses = lists:map(fun
            (Request) when is_function(Request, 0) -> Request();
            (Request) -> Request
        end, Requests),
        {Responses, {true, Responses}, Data}
    end,
    CommitFun = fun(_Changes, _Data) -> {true, []} end,
    CommitBackoffFun = fun(NextCommitDelay) -> 2 * NextCommitDelay end,
    MergeChangesFun = fun(TpChanges, NextTpChanges) ->
        TpChanges ++ NextTpChanges
    end,

    test_utils:mock_new(Workers, ?DRIVER, [passthrough |
        proplists:get_value(mock_opts, Config, [])]),
    test_utils:mock_expect(Workers, ?DRIVER, init, fun
        (Args) -> {ok, #datastore_doc_init{
            data = Args,
            rev = [],
            min_commit_delay = CommitDelay,
            max_commit_delay = CommitDelay,
            idle_timeout = IdleTimeout
        }}
    end),
    test_utils:mock_expect(Workers, ?DRIVER, modify,
        proplists:get_value(modify_fun, Config, ModifyFun)),
    test_utils:mock_expect(Workers, ?DRIVER, merge_changes,
        proplists:get_value(merge_changes_fun, Config, MergeChangesFun)),
    test_utils:mock_expect(Workers, ?DRIVER, commit,
        proplists:get_value(commit_fun, Config, CommitFun)),
    test_utils:mock_expect(Workers, ?DRIVER, handle_committed, fun
        (Data, _Rev) -> Data
    end),
    test_utils:mock_expect(Workers, ?DRIVER, commit_backoff,
        proplists:get_value(commit_backoff_fun, Config, CommitBackoffFun)),
    test_utils:mock_expect(Workers, ?DRIVER, terminate, fun
        (Data, _Rev) -> Data
    end),
    [{commit_delay, CommitDelay}, {idle_timeout, IdleTimeout} | Config].

end_per_testcase(_Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        lists:foreach(fun({_Key, Pid}) ->
            stop_datastore_doc(Worker, Pid)
        end, rpc:call(Worker, ets, tab2list, [?TP_ROUTING_TABLE])),
        rpc:call(Worker, ets, delete_all_objects, [?TP_ROUTING_TABLE])
    end, Workers),
    test_utils:mock_validate_and_unload(Workers, ?DRIVER).

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_datastore_doc(Worker) ->
    ?assertMatch({ok, _}, rpc:call(Worker, supervisor, start_child, [
        ?TP_ROUTER_SUP, [datastore_doc, ?TP_ARGS, ?TP_KEY]
    ])).

stop_datastore_doc(Worker, Pid) ->
    case self() of
        Pid ->
            ok;
        _ ->
            rpc:call(Worker, supervisor, terminate_child, [?TP_ROUTER_SUP, Pid])
    end.

receive_commit([], _) ->
    ok;
receive_commit(PendingIds, CommitDelay) ->
    {_, Ids} = ?assertReceivedMatch({committed, _}, 2 * CommitDelay),
    receive_commit(PendingIds -- Ids, CommitDelay).