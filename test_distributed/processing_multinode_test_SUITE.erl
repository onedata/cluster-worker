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
-module(processing_multinode_test_SUITE).
-author("Michal Wrzeszcz").

-include("datastore_test_utils.hrl").
-include("global_definitions.hrl").

%% export for ct
-export([all/0, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    traverse_test/1,
    traverse_and_queuing_test/1,
    traverse_restart_test/1
]).

%% Pool callbacks
-export([do_master_job/1, do_slave_job/1, task_finished/1, save_job/3, update_job/4,
    get_job/1, list_ongoing_jobs/0]).

all() ->
    ?ALL([
        traverse_test,
        traverse_and_queuing_test,
        traverse_restart_test
    ]).

-define(MASTER_POOL_NAME(Pool), list_to_atom(atom_to_list(Pool) ++ "_master")).
-define(SLAVE_POOL_NAME(Pool), list_to_atom(atom_to_list(Pool) ++ "_slave")).

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
    [Worker, Worker2] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Num) ->
        ?assertEqual(ok, rpc:call(Worker, traverse, run,
            [?MODULE, <<KeyBeg/binary, (integer_to_binary(Num))/binary>>, {self(), 1, Num}]))
    end, lists:seq(1, RunsNum)),

    Expected0 = [2,3,4,
        11,12,13,16,17,18,
        101,102,103,106,107,108,
        151,152,153,156,157,158,
        1001,1002,1003,1006,1007,1008,
        1051,1052,1053,1056,1057,1058,
        1501,1502, 1503,1506,1507,1508,
        1551,1552,1553,1556,1557, 1558],

    SJobsNum = length(Expected0),
    MJobsNum = SJobsNum div 3,
    Description = #{
        slave_jobs_delegated => SJobsNum,
        master_jobs_delegated => MJobsNum,
        slave_jobs_done => SJobsNum,
        master_jobs_done => MJobsNum,
        master_jobs_failed => 0
    },

    Ans1 = get_node_slave_ans(Worker, CheckID),
    Ans2 = get_node_slave_ans(Worker2, CheckID),
    GetExpected = fun(Beg) ->
        lists:foldl(fun(ID, Acc) ->
            case CheckID of
                true -> lists:map(fun(Element) -> {Element, ID} end, Expected0) ++ Acc;
                _ -> Expected0 ++ Acc
            end
        end, [], lists:seq(Beg, RunsNum, 2))
    end,

    ?assertEqual(lists:sort(GetExpected(1)), lists:sort(Ans1)),
    ?assertEqual(lists:sort(GetExpected(2)), lists:sort(Ans2)),

    lists:foreach(fun(Num) ->
        ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
            rpc:call(Worker, traverse_task, get, [<<KeyBeg/binary, (integer_to_binary(Num))/binary>>]))
    end, lists:seq(1, RunsNum)),
    ok.

traverse_restart_test(Config) ->
    [Worker, Worker2] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?MODULE, <<"traverse_restart_test1">>, {self(), 1, 100}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?MODULE, <<"traverse_restart_test1_1">>, {self(), 1, 101}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?MODULE, <<"traverse_restart_test2">>, {self(), 1, 2}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?MODULE, <<"traverse_restart_test3">>, {self(), 1, 3}])),

    RecAns = receive
                 {stop, W} ->
                     ?assertEqual(ok, rpc:call(W, worker_pool, stop_sup_pool, [?MASTER_POOL_NAME(?MODULE)])),
                     ?assertEqual(ok, rpc:call(W, worker_pool, stop_sup_pool, [?SLAVE_POOL_NAME(?MODULE)])),
                     receive
                         {stop, W2} ->
                             ?assertEqual(ok, rpc:call(W2, worker_pool, stop_sup_pool, [?MASTER_POOL_NAME(?MODULE)])),
                             ?assertEqual(ok, rpc:call(W2, worker_pool, stop_sup_pool, [?SLAVE_POOL_NAME(?MODULE)]))
                     after
                         5000 ->
                             timeout2
                     end
             after
                 5000 ->
                     timeout
             end,
    ?assertEqual(ok, RecAns),
    ?assertEqual(ok, rpc:call(Worker, traverse, init_pool, [?MODULE, 3, 3, 1])),
    ?assertEqual(ok, rpc:call(Worker2, traverse, init_pool, [?MODULE, 3, 3, 1])),

    Expected = [2,3,4,
        11,12,13,16,17,18,
        101,102,103,106,107,108,
        151,152,153,156,157,158,
        1001,1002,1003,1006,1007,1008,
        1051,1052,1053,1056,1057,1058,
        1501,1502, 1503,1506,1507,1508,
        1551,1552,1553,1556,1557, 1558],
    ExpLen = length(Expected),

    Ans1 = get_node_slave_ans(Worker, false),
    Ans2 = get_node_slave_ans(Worker2, false),
    Ans1Len = length(Ans1),
    Ans2Len = length(Ans2),

    SJobsNum = length(Expected),
    MJobsNum = SJobsNum div 3,
    Description = #{
        slave_jobs_delegated => SJobsNum,
        master_jobs_delegated => MJobsNum,
        slave_jobs_done => SJobsNum,
        master_jobs_done => MJobsNum,
        master_jobs_failed => 0
    },

    Ans1_1 = lists:sublist(Ans1, 1, Ans1Len - ExpLen),
    Ans1_2 = lists:sublist(Ans1, Ans1Len - ExpLen + 1, ExpLen),
    Ans2_1 = lists:sublist(Ans2, 1, Ans2Len - ExpLen),
    Ans2_2 = lists:sublist(Ans2, Ans2Len - ExpLen + 1, ExpLen),
    ?assertEqual(Expected, lists:sort(Ans1_2)),
    ?assertEqual(Expected, lists:sort(Ans2_2)),
    ?assertEqual(length(Ans1_1 -- Expected), length(Ans1_1) - length(Expected)),
    ?assertEqual(length(Ans2_1 -- Expected), length(Ans2_1) - length(Expected)),

    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [<<"traverse_restart_test2">>])),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [<<"traverse_restart_test3">>])),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [<<"traverse_restart_test1">>])),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [<<"traverse_restart_test1_1">>])),
    ok.

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_testcase(traverse_test, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, traverse, init_pool, [?MODULE, 3, 3, 10]))
    end, Workers),
    Config;
init_per_testcase(Case, Config) when
    Case =:= traverse_and_queuing_test ; Case =:= traverse_restart_test ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, traverse, init_pool, [?MODULE, 3, 3, 2]))
    end, Workers),
    Config.

end_per_testcase(_, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, traverse, stop_pool, [?MODULE]))
    end, Workers).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_node_slave_ans(Node, AddID) ->
    receive
        {slave, Num, ID, Node} ->
            case AddID of
                true -> [{Num, ID} | get_node_slave_ans(Node, AddID)];
                _ -> [Num | get_node_slave_ans(Node, AddID)]
            end
    after
        10000 ->
            []
    end.

%%%===================================================================
%%% Pool callbacks
%%%===================================================================

do_master_job({Master, 100, ID}) when ID == 100 ; ID == 101 ->
    timer:sleep(500),
    Master ! {stop, node()},
    timer:sleep(500),
    do_master_job_helper({Master, 100, 100});
do_master_job({Master, Num, ID}) ->
    do_master_job_helper({Master, Num, ID}).

do_master_job_helper({Master, Num, ID}) ->
    MasterJobs = case Num < 1000 of
        true -> [{Master, 10 * Num, ID}, {Master, 10 * Num + 5, ID}];
        _ -> []
    end,

    SlaveJobs = [{Master, Num + 1, ID}, {Master, Num + 2, ID}, {Master, Num + 3, ID}],
    {ok, SlaveJobs, MasterJobs}.

do_slave_job({Master, Num, ID}) ->
    Master ! {slave, Num, ID, node()},
    ok.

task_finished(_) ->
    timer:sleep(1000),
    ok.

save_job(Job, TaskID, waiting) ->
    List = application:get_env(?CLUSTER_WORKER_APP_NAME, test_job, []),
    ID = list_to_binary(ref_to_list(make_ref())),
    application:set_env(?CLUSTER_WORKER_APP_NAME, test_job, [{ID, {Job, TaskID}} | List]),
    {ok, ID};
save_job(Job, TaskID, started) ->
    ID = list_to_binary(ref_to_list(make_ref())),
    save_started_job(ID, Job, TaskID),
    {ok, ID}.

update_job(ID, Job, TaskID, started) ->
    save_started_job(ID, Job, TaskID),
    ok;
update_job(ID, _Job, _TaskID, finish) ->
    List = application:get_env(?CLUSTER_WORKER_APP_NAME, ongoing_job, []),
    application:set_env(?CLUSTER_WORKER_APP_NAME, ongoing_job, proplists:delete(ID, List)),
    ok.

save_started_job(ID, Job, TaskID) ->
    List = application:get_env(?CLUSTER_WORKER_APP_NAME, ongoing_job, []),
    application:set_env(?CLUSTER_WORKER_APP_NAME, ongoing_job, [{ID, {Job, TaskID}} | proplists:delete(ID, List)]).

get_job(ID) ->
    Jobs = lists:foldl(fun(Node, Acc) ->
        Acc ++ rpc:call(Node, application, get_env, [?CLUSTER_WORKER_APP_NAME, test_job, []]) ++
            rpc:call(Node, application, get_env, [?CLUSTER_WORKER_APP_NAME, ongoing_job, []])
    end, [], consistent_hasing:get_all_nodes()),
    {Job, TaskID} =  proplists:get_value(ID, Jobs, {undefined, undefined}),
    {ok, Job, TaskID}.

list_ongoing_jobs() ->
    List = lists:foldl(fun(Node, Acc) ->
        Acc ++ rpc:call(Node, application, get_env, [?CLUSTER_WORKER_APP_NAME, ongoing_job, []])
    end, [], consistent_hasing:get_all_nodes()),
    {ok, proplists:get_keys(List)}.