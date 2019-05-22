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
    traverse_and_queuing_test/1
]).

%% Pool callbacks
-export([do_master_job/1, do_slave_job/1, task_finished/1, save_job/2, get_job/1]).

all() ->
    ?ALL([
        traverse_test,
        traverse_and_queuing_test
    ]).

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

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_testcase(traverse_test, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, traverse, init_pool, [?MODULE, 3, 3, 10]))
    end, Workers),
    Config;
init_per_testcase(traverse_and_queuing_test, Config) ->
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

do_master_job({Master, Num, ID}) ->
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

save_job(Job, waiting) ->
    List = application:get_env(?CLUSTER_WORKER_APP_NAME, test_job, []),
    ID = list_to_binary(ref_to_list(make_ref())),
    application:set_env(?CLUSTER_WORKER_APP_NAME, test_job, [{ID, Job} | List]),
    {ok, ID};
save_job(_, _) ->
    {ok, <<"ID">>}.

get_job(ID) ->
    Jobs = lists:foldl(fun(Node, Acc) ->
        Acc ++ rpc:call(Node, application, get_env, [?CLUSTER_WORKER_APP_NAME, test_job, []])
    end, [], consistent_hasing:get_all_nodes()),
    {ok, proplists:get_value(ID, Jobs, undefined), undefined}.