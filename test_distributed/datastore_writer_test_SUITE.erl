%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore writer process tests.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_writer_test_SUITE).
-author("Krzysztof Trzepla").

-include("datastore_test_utils.hrl").
-include("global_definitions.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    requests_should_be_processed_in_order/1,
    parallel_requests_should_return_responses/1,
    datastore_writer_should_flush_cache/1,
    datastore_writer_should_retry_cache_flush/1,
    datastore_writer_should_flush_cache_on_terminate/1,
    datastore_writer_should_terminate_when_idle_timeout_exceeded/1
]).

%% test_bases
-export([
    parallel_requests_should_return_responses_base/1
]).

all() ->
    ?ALL([
        requests_should_be_processed_in_order,
        parallel_requests_should_return_responses,
        datastore_writer_should_flush_cache,
        datastore_writer_should_retry_cache_flush,
        datastore_writer_should_flush_cache_on_terminate,
        datastore_writer_should_terminate_when_idle_timeout_exceeded
    ], [
        parallel_requests_should_return_responses
    ]).

-define(CTX, #{}).
-define(DOC_BATCH, datastore_doc_batch).
-define(CACHE, datastore_cache).

-define(OP_TIME(Value), ?PERF_PARAM(op_time, Value, "ms",
    "Maximal duration of a single operation.")).
-define(OP_DELAY(Value), ?PERF_PARAM(op_delay, Value, "ms",
    "Maximal delay before single operation.")).

-define(TIMEOUT, timer:seconds(5)).

-define(TP_ROUTING_TABLE, tp_routing_table1).
-define(TP_ROUTER_SUP, tp_router_sup1).

%%%===================================================================
%%% Test functions
%%%===================================================================

requests_should_be_processed_in_order(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    OpsNum = 100,

    lists:map(fun(N) ->
        rpc:call(Worker, datastore_writer, call_async, [
            ?CTX, ?KEY, undefined, test_call, [fun() -> Self ! N end]
        ])
    end, lists:seq(1, OpsNum)),

    lists:foreach(fun(N) ->
        ?assertReceivedNextEqual(N, ?TIMEOUT)
    end, lists:seq(1, OpsNum)).

parallel_requests_should_return_responses(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 100},
        {parameters, [?OPS_NUM(100), ?OP_TIME(50), ?OP_DELAY(100)]},
        {description, "Multiple parallel operations for the same key."},
        ?PERF_CFG(small, [?OPS_NUM(1000), ?OP_TIME(30), ?OP_DELAY(6000)]),
        ?PERF_CFG(medium, [?OPS_NUM(5000), ?OP_TIME(6), ?OP_DELAY(30000)]),
        ?PERF_CFG(large, [?OPS_NUM(10000), ?OP_TIME(3), ?OP_DELAY(60000)])
    ]).
parallel_requests_should_return_responses_base(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    OpsNum = ?config(ops_num, Config),
    OpTime = ?config(op_time, Config),
    OpTimeout = OpsNum * OpTime,
    OpDelay = ?config(op_delay, Config),
    Ids = lists:seq(1, OpsNum),

    ?assertEqual(Ids, lists_utils:pmap(fun(N) ->
        timer:sleep(rand:uniform(OpDelay)),
        rpc:call(Worker, datastore_writer, call, [?CTX, ?KEY, test_call, [
            fun() ->
                timer:sleep(rand:uniform(OpTime)),
                N
            end
        ]], OpTimeout)
    end, Ids)),

    {ok, Pid} = ?assertMatch({ok, _}, rpc:call(Worker, tp_router, get, [?KEY])),
    stop_datastore_writer(Worker, Pid).

datastore_writer_should_flush_cache(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    FlushDelay = ?config(flush_delay, Config),
    OpsNum = 100,

    lists:map(fun(N) ->
        rpc:call(Worker, datastore_writer, call_async, [
            ?CTX, ?KEY, undefined, test_call, [N]
        ])
    end, lists:seq(1, OpsNum)),

    lists:foreach(fun(N) ->
        ?assertReceivedEqual({flushed, N}, 2 * FlushDelay)
    end, lists:seq(1, OpsNum)),

    ?assertNotReceivedMatch({flushed, _}, ?TIMEOUT).

datastore_writer_should_retry_cache_flush(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    FlushDelay = ?config(flush_delay, Config),

    rpc:call(Worker, datastore_writer, call_async, [
        ?CTX, ?KEY, undefined, test_call, [{flush_error, 1}]
    ]),

    {ok, Cooldown} = test_utils:get_env(Worker, ?CLUSTER_WORKER_APP_NAME,
        flush_key_cooldown_sec),

    ?assertReceivedEqual({not_flushed, 1}, 2 * FlushDelay),
    ?assertReceivedEqual({not_flushed, 1}, 2 * FlushDelay + timer:seconds(Cooldown)),
    {ok, Pid} = ?assertMatch({ok, _}, rpc:call(Worker, tp_router, get, [?KEY])),
    exit(Pid, kill).

datastore_writer_should_flush_cache_on_terminate(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    FlushDelay = ?config(flush_delay, Config),
    Key = ?KEY,

    Pid = spawn(Worker, fun Loop() ->
        receive
            {run, Request} ->
                datastore_writer:call(?CTX, Key, test_call, [Request]),
                Self ! tp_router:get(Key)
        end,
        Loop()
    end),

    Pid ! {run, fun() -> timer:sleep(FlushDelay), 1 end},
    {ok, Pid2} = ?assertReceivedNextMatch({ok, _}, ?TIMEOUT),
    Pid ! {run, fun() -> timer:sleep(FlushDelay), 2 end},
    ?assertReceivedEqual({ok, Pid2}, ?TIMEOUT),
    stop_datastore_writer(Worker, Pid2),

    ?assertReceivedEqual({flushed, 1}, 2 * FlushDelay),
    ?assertReceivedEqual({flushed, 2}, 2 * FlushDelay).

datastore_writer_should_terminate_when_idle_timeout_exceeded(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    IdleTimeout = ?config(idle_timeout, Config),

    ?assertEqual(<<"ok">>, rpc:call(Worker, datastore_writer, call, [
        ?CTX, ?KEY, test_call, [<<"ok">>]
    ])),
    ?assertMatch({ok, _}, rpc:call(Worker, tp_router, get, [?KEY])),
    ?assertEqual({error, not_found}, rpc:call(Worker, tp_router, get, [?KEY]),
        5, IdleTimeout).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_testcase(_Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    Self = self(),

    FlushDelay = timer:seconds(1),
    IdleTimeout = timer:seconds(3),
    lists:foreach(fun({Env, Value}) ->
        {_, []} = utils:rpc_multicall(Workers, application, set_env, [cluster_worker, Env, Value])
    end, [
        {tp_subtrees_number, 1},
        {throttling_idle_time, IdleTimeout},
        {datastore_writer_idle_timeout, IdleTimeout},
        {memory_store_idle_timeout_ms, IdleTimeout},
        {memory_store_min_idle_timeout_ms, IdleTimeout},
        {datastore_writer_flush_delay, FlushDelay}
    ]),

    test_utils:mock_new(Workers, ?DOC_BATCH, [non_strict]),
    test_utils:mock_expect(Workers, ?DOC_BATCH, init, fun() ->
        #{responses => []}
    end),
    test_utils:mock_expect(Workers, ?DOC_BATCH, init_request, fun
        (_Ref, Batch) -> Batch
    end),
    test_utils:mock_expect(Workers, ?DOC_BATCH, terminate_request, fun
        (_Ref, _Batch) -> ok
    end),
    test_utils:mock_expect(Workers, ?DOC_BATCH, test_call, fun
        (_Ctx, Request, Batch = #{responses := Responses}) ->
            Response = case Request of
                _ when is_function(Request, 0) -> Request();
                _ -> Request
            end,
            Ref = make_ref(),
            {{Ref, Response}, Batch#{responses => [Response | Responses]}}
    end),
    test_utils:mock_expect(Workers, ?DOC_BATCH, apply, fun(Batch) ->
        Batch
    end),
    test_utils:mock_expect(Workers, ?DOC_BATCH, create_cache_requests, fun(_Batch) ->
        []
    end),
    test_utils:mock_expect(Workers, ?DOC_BATCH, apply_cache_requests, fun(Batch, Requests) ->
        {Batch, Requests}
    end),
    test_utils:mock_expect(Workers, ?DOC_BATCH, terminate, fun
        (#{responses := Responses}) ->
            lists:foldl(fun(Response, Map) ->
                maps:put(Response, ?CTX, Map)
            end, #{}, Responses)
    end),
    test_utils:mock_expect(Workers, ?DOC_BATCH, set_link_tokens, fun(Batch, _Tokens) ->
        Batch
    end),
    test_utils:mock_expect(Workers, ?DOC_BATCH, get_link_tokens, fun(_Batch) ->
        #{}
    end),
    test_utils:mock_new(Workers, ?CACHE),
    test_utils:mock_expect(Workers, ?CACHE, flush_async, fun(_, Response) ->
        Response
    end),
    test_utils:mock_expect(Workers, ?CACHE, wait, fun(Futures) ->
        lists:map(fun
            ({flush_error, Response}) ->
                Self ! {not_flushed, Response},
                {error, flush_error};
            (Response) ->
                Self ! {flushed, Response},
                {ok, disc, Response}
        end, Futures)
    end),
    [{flush_delay, FlushDelay}, {idle_timeout, IdleTimeout} | Config].

end_per_testcase(_Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        lists:foreach(fun
            ({_Key, Pid, _}) ->
                stop_datastore_writer(Worker, Pid);
            (_) ->
                ok
        end, rpc:call(Worker, ets, tab2list, [?TP_ROUTING_TABLE])),
        rpc:call(Worker, ets, delete_all_objects, [?TP_ROUTING_TABLE])
    end, Workers),
    test_utils:mock_validate_and_unload(Workers, ?DOC_BATCH).

%%%===================================================================
%%% Internal functions
%%%===================================================================

stop_datastore_writer(Worker, Pid) ->
    rpc:call(Worker, supervisor, terminate_child, [?TP_ROUTER_SUP, Pid]).