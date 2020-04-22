%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains traverse multi-node tests.
%%% @end
%%%-------------------------------------------------------------------
-module(traverse_multinode_test_SUITE).
-author("Michal Wrzeszcz").

-include("datastore_test_utils.hrl").
-include("global_definitions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    traverse_test/1,
    traverse_and_queuing_test/1,
    traverse_restart_test/1
]).

all() ->
    ?ALL([
        traverse_test,
        traverse_and_queuing_test,
        traverse_restart_test
    ]).

-define(POOL, <<"traverse_test_pool">>).
-define(MASTER_POOL_NAME, traverse_test_pool_master).
-define(SLAVE_POOL_NAME, traverse_test_pool_slave).

%%%===================================================================
%%% Test functions
%%%===================================================================

traverse_test(Config) ->
    KeyBeg = <<"traverse_test">>,
    RunsNum = 8,
    traverse_base(Config, KeyBeg, RunsNum, true).

traverse_and_queuing_test(Config) ->
    KeyBeg = <<"traverse_and_queuing_test">>,
    RunsNum = 8,
    traverse_base(Config, KeyBeg, RunsNum, false).

traverse_base(Config, KeyBeg, RunsNum, CheckID) ->
    [Worker, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Num) ->
        ?assertEqual(ok, rpc:call(Worker, traverse, run,
            [?POOL, <<KeyBeg/binary, (integer_to_binary(Num))/binary>>, {self(), 1, Num}]))
    end, lists:seq(1, RunsNum)),

    {Expected0, Description} = traverse_test_pool:get_expected(),

    Ans1 = traverse_test_pool:get_node_slave_ans(Worker, CheckID),
    Ans2 = traverse_test_pool:get_node_slave_ans(Worker2, CheckID),

    case CheckID of
        true ->
            GetExpected = fun(Beg) ->
                lists:foldl(fun(ID, Acc) ->
                    lists:map(fun(Element) -> {Element, ID} end, Expected0) ++ Acc
                end, [], lists:seq(Beg, RunsNum, 2))
            end,

            ?assertEqual(lists:sort(GetExpected(1)), lists:sort(Ans1)),
            ?assertEqual(lists:sort(GetExpected(2)), lists:sort(Ans2));
        _ ->
            ?assertEqual(lists:sort(lists:flatten(lists:duplicate(RunsNum, Expected0))), lists:sort(Ans1 ++ Ans2))
    end,

    lists:foreach(fun(Num) ->
        ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
            rpc:call(Worker, traverse_task, get, [?POOL, <<KeyBeg/binary, (integer_to_binary(Num))/binary>>]), 2)
    end, lists:seq(1, RunsNum)),

    traverse_test_pool:check_schedulers_after_test(Worker, Workers, ?POOL).

traverse_restart_test(Config) ->
    [Worker, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?POOL, <<"traverse_restart_test1">>, {self(), 1, 100}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?POOL, <<"traverse_restart_test1_1">>, {self(), 1, 101}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?POOL, <<"traverse_restart_test2">>, {self(), 1, 2}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?POOL, <<"traverse_restart_test3">>, {self(), 1, 3}])),

    RecAns = receive
        {stop, W} ->
            ?assertEqual(ok, rpc:call(W, worker_pool, stop_sup_pool, [?MASTER_POOL_NAME])),
            ?assertEqual(ok, rpc:call(W, worker_pool, stop_sup_pool, [?SLAVE_POOL_NAME])),
            receive
                {stop, W2} ->
                    ?assertEqual(ok, rpc:call(W2, worker_pool, stop_sup_pool, [?MASTER_POOL_NAME])),
                    ?assertEqual(ok, rpc:call(W2, worker_pool, stop_sup_pool, [?SLAVE_POOL_NAME]))
            after
                5000 ->
                    timeout2
            end
    after
        5000 ->
            timeout
    end,
    ?assertEqual(ok, RecAns),

    lists:foreach(fun(W) ->
        ?assertMatch({ok, _}, rpc:call(W, worker_pool, start_sup_pool, [?MASTER_POOL_NAME,
            [{workers, 3}, {queue_type, lifo}]])),
        ?assertMatch({ok, _}, rpc:call(W, worker_pool, start_sup_pool, [?SLAVE_POOL_NAME,
            [{workers, 3}, {queue_type, lifo}]])),
        ?assertEqual(ok, rpc:call(W, traverse, restart_tasks, [?POOL, #{}, W]))
    end, Workers),

    {Expected, Description} = traverse_test_pool:get_expected(),
    ExpLen = length(Expected),

    Ans1 = traverse_test_pool:get_node_slave_ans(Worker, false),
    Ans2 = traverse_test_pool:get_node_slave_ans(Worker2, false),
    Ans1Len = length(Ans1),
    Ans2Len = length(Ans2),

    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"traverse_restart_test2">>]), 2),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"traverse_restart_test3">>]), 2),
    ?assertMatch({ok, #document{value = #traverse_task{status = finished}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"traverse_restart_test1">>]), 2),
    ?assertMatch({ok, #document{value = #traverse_task{status = finished}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"traverse_restart_test1_1">>]), 2),

    case Ans1Len of
        _ when Ans1Len >= 3 * ExpLen ->
            Ans1_1 = lists:sublist(Ans1, 1, Ans1Len - 2 * ExpLen),
            Ans1_2 = lists:sublist(Ans1, Ans1Len - 2* ExpLen + 1, ExpLen),
            Ans1_3 = lists:sublist(Ans1, Ans1Len - ExpLen + 1, ExpLen),

            ?assertEqual(Expected, lists:sort(Ans1_2)),
            ?assertEqual(Expected, lists:sort(Ans1_3)),
            ?assertEqual(length(Ans1_1 -- Expected), length(Ans1_1) - length(Expected)),
            ?assertEqual(length(Ans2 -- Expected), length(Ans2) - length(Expected));
        _ when Ans2Len >= 3 * ExpLen ->
            Ans2_1 = lists:sublist(Ans2, 1, Ans2Len - 2 * ExpLen),
            Ans2_2 = lists:sublist(Ans2, Ans2Len - 2* ExpLen + 1, ExpLen),
            Ans2_3 = lists:sublist(Ans2, Ans2Len - ExpLen + 1, ExpLen),

            ?assertEqual(Expected, lists:sort(Ans2_2)),
            ?assertEqual(Expected, lists:sort(Ans2_3)),
            ?assertEqual(length(Ans2_1 -- Expected), length(Ans2_1) - length(Expected)),
            ?assertEqual(length(Ans1 -- Expected), length(Ans1) - length(Expected));
        _ ->
            Ans1_1 = lists:sublist(Ans1, 1, Ans1Len - ExpLen),
            Ans1_2 = lists:sublist(Ans1, Ans1Len - ExpLen + 1, ExpLen),
            Ans2_1 = lists:sublist(Ans2, 1, Ans2Len - ExpLen),
            Ans2_2 = lists:sublist(Ans2, Ans2Len - ExpLen + 1, ExpLen),
            ?assertEqual(Expected, lists:sort(Ans1_2)),
            ?assertEqual(Expected, lists:sort(Ans2_2)),
            ?assertEqual(length(Ans1_1 -- Expected), length(Ans1_1) - length(Expected)),
            ?assertEqual(length(Ans2_1 -- Expected), length(Ans2_1) - length(Expected))
    end,

    traverse_test_pool:check_schedulers_after_test(Worker, Workers, ?POOL).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [traverse_test_pool]} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(traverse_test, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Workers, ?CLUSTER_WORKER_APP_NAME, test_job, []),
    test_utils:set_env(Workers, ?CLUSTER_WORKER_APP_NAME, ongoing_job, []),
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, traverse, init_pool,
            [?POOL, 3, 3, 10, #{parallel_orders_per_node_limit => 10}]))
    end, Workers),
    Config;
init_per_testcase(Case, Config) when
    Case =:= traverse_and_queuing_test ; Case =:= traverse_restart_test ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Workers, ?CLUSTER_WORKER_APP_NAME, test_job, []),
    test_utils:set_env(Workers, ?CLUSTER_WORKER_APP_NAME, ongoing_job, []),
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, traverse, init_pool,
            [?POOL, 3, 3, 2, #{parallel_orders_per_node_limit => 10}]))
    end, Workers),
    Config.

end_per_testcase(_, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    timer:sleep(2000), % Allow pool processes to finish jobs
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, traverse, stop_pool, [?POOL]))
    end, Workers).
