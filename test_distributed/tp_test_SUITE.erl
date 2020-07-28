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
-module(tp_test_SUITE).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    tp_get_processes_limit_should_return_value/1,
    tp_set_processes_limit_should_change_value/1,
    tp_get_processes_number_should_return_value/1,
    tp_server_call_should_return_response/1,
    tp_server_multiple_requests_should_create_single_process/1,
    tp_server_call_should_return_timeout_error/1,
    tp_server_call_should_return_limit_exceeded_error/1,
    tp_server_call_should_pass_init_error/1,
    tp_server_call_should_pass_request_error/1,
    tp_server_cast_should_return_success/1,
    tp_server_cast_should_return_limit_exceeded_error/1,
    tp_server_send_should_return_success/1,
    tp_server_send_should_return_limit_exceeded_error/1,
    tp_server_should_remove_routing_entry_on_terminate/1,
    tp_router_create_should_return_success/1,
    tp_router_create_should_return_already_exists_error/1,
    tp_router_get_should_return_tp_server_pid/1,
    tp_router_get_should_return_missing_error/1,
    tp_router_delete_should_delete_routing_entry/1,
    tp_router_delete_should_return_missing_error/1,
    tp_router_delete_should_ignore_missing_entry/1
]).

all() ->
    ?ALL([
        tp_get_processes_limit_should_return_value,
        tp_set_processes_limit_should_change_value,
        tp_get_processes_number_should_return_value,
        tp_server_call_should_return_response,
        tp_server_multiple_requests_should_create_single_process,
        tp_server_call_should_return_timeout_error,
        tp_server_call_should_return_limit_exceeded_error,
        tp_server_call_should_pass_init_error,
        tp_server_call_should_pass_request_error,
        tp_server_cast_should_return_success,
        tp_server_cast_should_return_limit_exceeded_error,
        tp_server_send_should_return_success,
        tp_server_send_should_return_limit_exceeded_error,
        tp_server_should_remove_routing_entry_on_terminate,
        tp_router_create_should_return_success,
        tp_router_create_should_return_already_exists_error,
        tp_router_get_should_return_tp_server_pid,
        tp_router_get_should_return_missing_error,
        tp_router_delete_should_delete_routing_entry,
        tp_router_delete_should_return_missing_error,
        tp_router_delete_should_ignore_missing_entry
    ]).

-define(TP_MODULE, tp_module).
-define(TP_ARGS, [tp_arg1, tp_arg2, tp_arg3]).
-define(TP_KEY, <<"key">>).
-define(TP_PID, pid).
-define(TIMEOUT, timer:seconds(5)).
-define(TP_ROUTING_TABLE, tp_routing_table1).
-define(TP_ROUTER_SUP, tp_router_sup1).

%%%===================================================================
%%% Test functions
%%%===================================================================

tp_get_processes_limit_should_return_value(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assert(is_integer(rpc:call(Worker, tp, get_processes_limit, []))).

tp_set_processes_limit_should_change_value(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, tp, set_processes_limit, [100])).

tp_get_processes_number_should_return_value(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(0, rpc:call(Worker, tp, get_processes_number, [])),
    rpc:call(Worker, tp, call, [?TP_MODULE, ?TP_ARGS, ?TP_KEY, reply]),
    ?assertEqual(1, rpc:call(Worker, tp, get_processes_number, [])).

tp_server_call_should_return_response(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({reply, _}, rpc:call(Worker, tp, call, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, reply
    ])),
    ?assertMatch({reply, _}, rpc:call(Worker, tp, call, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, reply_hibernate
    ])),
    ?assertMatch({error, _}, rpc:call(Worker, tp, call, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, noreply
    ])),
    ?assertMatch({error, _}, rpc:call(Worker, tp, call, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, noreply_hibernate
    ])),
    ?assertMatch({reply, _}, rpc:call(Worker, tp, call, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, stop_reply
    ])),
    ?assertEqual(0, rpc:call(Worker, tp, get_processes_number, []), 30),
    ?assertMatch({error, _}, rpc:call(Worker, tp, call, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, stop_noreply
    ])),
    ?assertEqual(0, rpc:call(Worker, tp, get_processes_number, []), 30).

tp_server_multiple_requests_should_create_single_process(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    lists_utils:pforeach(fun(_) ->
        ?assertMatch({reply, _}, rpc:call(Worker, tp, call, [
            ?TP_MODULE, ?TP_ARGS, ?TP_KEY, reply
        ])),
        Self ! done
    end, lists:seq(1, 100)),
    lists:foreach(fun(_) ->
        ?assertReceivedEqual(done, ?TIMEOUT)
    end, lists:seq(1, 100)),
    ?assertEqual(1, rpc:call(Worker, tp, get_processes_number, [])).

tp_server_call_should_return_timeout_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, timeout}, rpc:call(Worker, tp, call, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, reply, 0
    ])),
    ?assertEqual({error, timeout}, rpc:call(Worker, tp, call, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, reply, ?TIMEOUT, 0
    ])).

tp_server_call_should_return_limit_exceeded_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, tp, set_processes_limit, [0]),
    ?assertEqual({error, limit_exceeded}, rpc:call(Worker, tp, call, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, reply
    ])).

tp_server_call_should_pass_init_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, _}, rpc:call(Worker, tp, call, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, reply
    ])).

tp_server_call_should_pass_request_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, _}, rpc:call(Worker, tp, call, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, reply
    ])).

tp_server_cast_should_return_success(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, tp, cast, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, noreply
    ])),
    ?assertEqual(ok, rpc:call(Worker, tp, cast, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, noreply_hibernate
    ])),
    ?assertEqual(ok, rpc:call(Worker, tp, cast, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, stop_noreply
    ])),
    ?assertEqual(0, rpc:call(Worker, tp, get_processes_number, []), 10).

tp_server_cast_should_return_limit_exceeded_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, tp, set_processes_limit, [0]),
    ?assertEqual({error, limit_exceeded}, rpc:call(Worker, tp, cast, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, reply
    ])).

tp_server_send_should_return_success(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, tp, send, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, noreply
    ])),
    ?assertEqual(ok, rpc:call(Worker, tp, send, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, noreply_hibernate
    ])),
    ?assertEqual(ok, rpc:call(Worker, tp, send, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, stop_noreply
    ])),
    ?assertEqual(0, rpc:call(Worker, tp, get_processes_number, []), 10).

tp_server_send_should_return_limit_exceeded_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, tp, set_processes_limit, [0]),
    ?assertEqual({error, limit_exceeded}, rpc:call(Worker, tp, send, [
        ?TP_MODULE, ?TP_ARGS, ?TP_KEY, reply
    ])).

tp_server_should_remove_routing_entry_on_terminate(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, tp, call, [?TP_MODULE, ?TP_ARGS, ?TP_KEY, reply]),
    ?assertEqual(1, rpc:call(Worker, tp, get_processes_number, [])),
    rpc:call(Worker, tp, call, [?TP_MODULE, ?TP_ARGS, ?TP_KEY, stop_reply]),
    ?assertEqual(0, rpc:call(Worker, tp, get_processes_number, []), 10).

tp_router_create_should_return_success(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, tp_router, create, [?TP_KEY, ?TP_PID])).

tp_router_create_should_return_already_exists_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, tp_router, create, [?TP_KEY, ?TP_PID]),
    ?assertEqual({error, already_exists},
        rpc:call(Worker, tp_router, create, [?TP_KEY, ?TP_PID])
    ).

tp_router_get_should_return_tp_server_pid(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, tp_router, create, [?TP_KEY, ?TP_PID]),
    ?assertEqual({ok, ?TP_PID}, rpc:call(Worker, tp_router, get, [?TP_KEY])).

tp_router_get_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, not_found}, rpc:call(Worker, tp_router, get, [?TP_KEY])).

tp_router_delete_should_delete_routing_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, tp_router, create, [?TP_KEY, ?TP_PID]),
    ?assertEqual(ok, rpc:call(Worker, tp_router, delete, [?TP_KEY, ?TP_PID])),
    ?assertEqual({error, not_found}, rpc:call(Worker, tp_router, get, [?TP_KEY])).

tp_router_delete_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, tp_router, delete, [?TP_KEY, otherPid])).

tp_router_delete_should_ignore_missing_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, tp_router, create, [?TP_KEY, ?TP_PID]),
    rpc:call(Worker, tp_router, delete, [?TP_KEY, otherPid]),
    ?assertEqual({ok, ?TP_PID}, rpc:call(Worker, tp_router, get, [?TP_KEY])).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_testcase(tp_server_call_should_pass_init_error = Case, Config) ->
    init_per_testcase(?DEFAULT_CASE(Case), [
        {init_fun, fun(_) -> {stop, shutdown} end} | Config
    ]);
init_per_testcase(tp_server_call_should_pass_request_error = Case, Config) ->
    init_per_testcase(?DEFAULT_CASE(Case), [
        {handle_call_fun, fun(_, _, _) ->
            meck:exception(error, unexpected_error)
        end} | Config
    ]);
init_per_testcase(_Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME,
            tp_subtrees_number, 1)
    end, Workers),

    rpc:multicall(Workers, tp, set_processes_limit, [200]),
    test_utils:mock_new(Workers, ?TP_MODULE, [passthrough, non_strict]),
    test_utils:mock_expect(Workers, ?TP_MODULE, init,
        proplists:get_value(init_fun, Config, fun
            (_Args) -> {ok, state}
        end)
    ),
    test_utils:mock_expect(Workers, ?TP_MODULE, handle_call,
        proplists:get_value(handle_call_fun, Config, fun
            (reply, _From, State) -> {reply, reply, State};
            (reply_hibernate, _From, State) -> {reply, reply, State, hibernate};
            (noreply, _From, State) -> {noreply, State};
            (noreply_hibernate, _From, State) -> {noreply, State, hibernate};
            (stop_reply, _From, State) -> {stop, shutdown, reply, State};
            (stop_noreply, _From, State) -> {stop, shutdown, State}
        end)
    ),
    test_utils:mock_expect(Workers, ?TP_MODULE, handle_cast, fun
        (noreply, State) -> {noreply, State};
        (noreply_hibernate, State) -> {noreply, State, hibernate};
        (stop_noreply, State) -> {stop, shutdown, State}
    end),
    test_utils:mock_expect(Workers, ?TP_MODULE, handle_info, fun
        (noreply, State) -> {noreply, State};
        (noreply_hibernate, State) -> {noreply, State, hibernate};
        (stop_noreply, State) -> {stop, shutdown, State}
    end),
    test_utils:mock_expect(Workers, ?TP_MODULE, terminate, fun
        (_Reason, State) -> State
    end),
    Config.

end_per_testcase(_Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        lists:foreach(fun
            ({_Key, Pid, _}) -> stop_tp_server(Worker, Pid);
            (_) -> ok
        end, rpc:call(Worker, ets, tab2list, [?TP_ROUTING_TABLE])),
        rpc:call(Worker, ets, delete_all_objects, [?TP_ROUTING_TABLE])
    end, Workers),
    test_utils:mock_validate_and_unload(Workers, ?TP_MODULE).

%%%===================================================================
%%% Internal functions
%%%===================================================================

stop_tp_server(Worker, Pid) when is_pid(Pid) ->
    rpc:call(Worker, supervisor, terminate_child, [?TP_ROUTER_SUP, Pid]);
stop_tp_server(_Worker, _Pid) ->
    ok.