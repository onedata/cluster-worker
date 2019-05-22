%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tmp cache and traverse tests.
%%% @end
%%%-------------------------------------------------------------------
-module(processing_test_SUITE).
-author("Michal Wrzeszcz").

-include("datastore_test_utils.hrl").
-include("global_definitions.hrl").

%% export for ct
-export([all/0, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    basic_ops_test/1,
    clearing_test/1,
    traverse_test/1,
    traverse_multitask_concurrent_test/1,
    traverse_multitask_sequential_test/1,
    traverse_loadbalancingt_test/1,
    traverse_loadbalancingt_mixed_ids_test/1
]).

%% Pool callbacks
-export([do_master_job/1, do_slave_job/1, task_finished/1, save_job/2, get_job/1]).

all() ->
    ?ALL([
        basic_ops_test,
        clearing_test,
        traverse_test,
        traverse_multitask_concurrent_test,
        traverse_multitask_sequential_test,
        traverse_loadbalancingt_test,
        traverse_loadbalancingt_mixed_ids_test
    ]).

-define(CACHE, test_cache).
-define(CALL_CACHE(Worker, Op, Args), rpc:call(Worker, tmp_cache, Op, [?CACHE | Args])).

%%%===================================================================
%%% Test functions
%%%===================================================================

basic_ops_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),
    Key1 = key1,
    Key2 = key2,
    CalculateFun = fun(_) -> {ok, 1, calculated} end,

    ?assertEqual({error, not_found}, ?CALL_CACHE(W, get, [Key1])),
    ?assertEqual({ok, 1, calculated}, ?CALL_CACHE(W, calculate_and_cache,
        [Key1, CalculateFun, []])),
    ?assertEqual({ok, 1}, ?CALL_CACHE(W, get, [Key1])),

    ?assertEqual({ok, 1, calculated}, ?CALL_CACHE(W, get_or_calculate,
        [Key2, CalculateFun, []])),
    ?assertEqual({ok, 1, cached}, ?CALL_CACHE(W, get_or_calculate,
        [Key2, CalculateFun, []])),

    ok.

clearing_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),
    Key1 = key1,
    Key2 = key2,
    Key3 = key3,
    CalculateFun = fun(_) -> {ok, 1, calculated} end,

    ?assertEqual({ok, 1, calculated}, ?CALL_CACHE(W, calculate_and_cache,
        [Key1, CalculateFun, []])),
    ?assertEqual({ok, 1}, ?CALL_CACHE(W, get, [Key1])),
    timer:sleep(timer:seconds(15)),
    ?assertEqual({ok, 1}, ?CALL_CACHE(W, get, [Key1])),

    ?assertEqual({ok, 1, calculated}, ?CALL_CACHE(W, calculate_and_cache,
        [Key2, CalculateFun, []])),
    ?assertEqual({ok, 1, calculated}, ?CALL_CACHE(W, calculate_and_cache,
        [Key3, CalculateFun, []])),
    timer:sleep(timer:seconds(10)),
    ?assertEqual({error, not_found}, ?CALL_CACHE(W, get, [Key1])),

    ok.

traverse_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, run, [?MODULE, <<"traverse_test1">>, {self(), 1, 1}])),

    Expected = [2,3,4,
        11,12,13,16,17,18,
        101,102,103,106,107,108,
        151,152,153,156,157,158,
        1001,1002,1003,1006,1007,1008,
        1051,1052,1053,1056,1057,1058,
        1501,1502, 1503,1506,1507,1508,
        1551,1552,1553,1556,1557, 1558],
    Ans = get_slave_ans(),

    SJobsNum = length(Expected),
    MJobsNum = SJobsNum div 3,
    Description = #{
        slave_jobs_delegated => SJobsNum,
        master_jobs_delegated => MJobsNum,
        slave_jobs_done => SJobsNum,
        master_jobs_done => MJobsNum,
        master_jobs_failed => 0
    },

    ?assertEqual(Expected, lists:sort(Ans)),

    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [<<"traverse_test1">>])),
    ok.

traverse_multitask_concurrent_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?MODULE, <<"traverse_multitask_concurrent_test1">>, {self(), 1, 1}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?MODULE, <<"traverse_multitask_concurrent_test2">>, {self(), 1, 2}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?MODULE, <<"traverse_multitask_concurrent_test3">>, {self(), 1, 3}])),

    Expected0 = [2,3,4,
        11,12,13,16,17,18,
        101,102,103,106,107,108,
        151,152,153,156,157,158,
        1001,1002,1003,1006,1007,1008,
        1051,1052,1053,1056,1057,1058,
        1501,1502, 1503,1506,1507,1508,
        1551,1552,1553,1556,1557, 1558],
    Expected = Expected0 ++ Expected0 ++ Expected0,
    Ans = get_slave_ans(),

    SJobsNum = length(Expected0),
    MJobsNum = SJobsNum div 3,
    Description = #{
        slave_jobs_delegated => SJobsNum,
        master_jobs_delegated => MJobsNum,
        slave_jobs_done => SJobsNum,
        master_jobs_done => MJobsNum,
        master_jobs_failed => 0
    },

    ?assertEqual(lists:sort(Expected), lists:sort(Ans)),

    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [<<"traverse_multitask_concurrent_test1">>])),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [<<"traverse_multitask_concurrent_test2">>])),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [<<"traverse_multitask_concurrent_test3">>])),
    ok.

traverse_multitask_sequential_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?MODULE, <<"traverse_multitask_sequential_test1">>, {self(), 1, 1}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?MODULE, <<"traverse_multitask_sequential_test2">>, {self(), 1, 2}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?MODULE, <<"traverse_multitask_sequential_test3">>, {self(), 1, 3}])),

    Expected = [2,3,4,
        11,12,13,16,17,18,
        101,102,103,106,107,108,
        151,152,153,156,157,158,
        1001,1002,1003,1006,1007,1008,
        1051,1052,1053,1056,1057,1058,
        1501,1502, 1503,1506,1507,1508,
        1551,1552,1553,1556,1557, 1558],
    ExpLen = length(Expected),
    Ans = get_slave_ans(),

    SJobsNum = length(Expected),
    MJobsNum = SJobsNum div 3,
    Description = #{
        slave_jobs_delegated => SJobsNum,
        master_jobs_delegated => MJobsNum,
        slave_jobs_done => SJobsNum,
        master_jobs_done => MJobsNum,
        master_jobs_failed => 0
    },

    Ans1 = lists:sublist(Ans, 1, ExpLen),
    Ans2 = lists:sublist(Ans, ExpLen + 1, ExpLen),
    Ans3 = lists:sublist(Ans, 2 * ExpLen + 1, ExpLen),
    ?assertEqual(Expected, lists:sort(Ans1)),
    ?assertEqual(Expected, lists:sort(Ans2)),
    ?assertEqual(Expected, lists:sort(Ans3)),

    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [<<"traverse_multitask_sequential_test1">>])),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [<<"traverse_multitask_sequential_test2">>])),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [<<"traverse_multitask_sequential_test3">>])),
    ok.

traverse_loadbalancingt_test(Config) ->
    Tasks = [{<<"tlt1">>, <<"tlt1">>, 1}, {<<"tlt2">>, <<"tlt2">>, 2}, {<<"tlt3">>, <<"tlt2">>, 3},
        {<<"tlt4">>, <<"tlt2">>, 4}, {<<"tlt5">>, <<"tlt3">>, 5}, {<<"tlt6">>, <<"tlt3">>, 6}],
    Check = [{1,1}, {2,2}, {3,5}, {4,3}, {5,6}, {6,4}],
    traverse_loadbalancingt_base(Config, Tasks, Check).

traverse_loadbalancingt_mixed_ids_test(Config) ->
    Tasks = [{<<"tlmid9">>, <<"tlmid1">>, 1}, {<<"tlmid2">>, <<"tlmid2">>, 2}, {<<"tlmid3">>, <<"tlmid2">>, 3},
        {<<"tlmid8">>, <<"tlmid2">>, 4}, {<<"tlmid5">>, <<"tlmid3">>, 5}, {<<"tlmid6">>, <<"tlmid3">>, 6}],
    Check = [{1,1}, {2,2}, {3,5}, {4,3}, {5,6}, {6,4}],
    traverse_loadbalancingt_base(Config, Tasks, Check).

traverse_loadbalancingt_base(Config, Tasks, Check) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    % TODO - zmienic idki zeby nie byly po kolei
    lists:foreach(fun({ID, GR, Ans}) ->
        ?assertEqual(ok, rpc:call(Worker, traverse, run, [?MODULE, ID, GR, {self(), 1, Ans}]))
    end, Tasks),

    Expected = [2,3,4,
        11,12,13,16,17,18,
        101,102,103,106,107,108,
        151,152,153,156,157,158,
        1001,1002,1003,1006,1007,1008,
        1051,1052,1053,1056,1057,1058,
        1501,1502, 1503,1506,1507,1508,
        1551,1552,1553,1556,1557, 1558],
    ExpLen = length(Expected),
    Ans = get_slave_ans_with_id(),

    SJobsNum = length(Expected),
    MJobsNum = SJobsNum div 3,
    Description = #{
        slave_jobs_delegated => SJobsNum,
        master_jobs_delegated => MJobsNum,
        slave_jobs_done => SJobsNum,
        master_jobs_done => MJobsNum,
        master_jobs_failed => 0
    },

    AddID = fun(ID, List) ->
        lists:map(fun(Element) -> {Element, ID} end, List)
    end,
    GetRange = fun(Num) ->
        lists:sublist(Ans, (Num - 1) * ExpLen + 1, ExpLen)
    end,

    lists:foreach(fun({Range, ID}) ->
        ?assertEqual(AddID(ID, Expected), lists:sort(GetRange(Range)))
    end, Check),

    lists:foreach(fun({ID, _, _}) ->
        ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
            rpc:call(Worker, traverse_task, get, [ID]))
    end, Tasks),
    ok.

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_testcase(Case, Config) when
    Case =:= traverse_test ; Case =:= traverse_multitask_concurrent_test ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, init_pool, [?MODULE, 3, 3, 10])),
    Config;
init_per_testcase(Case, Config) when
    Case =:= traverse_multitask_sequential_test ; Case =:= traverse_loadbalancingt_test ;
    Case =:= traverse_loadbalancingt_mixed_ids_test ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, init_pool, [?MODULE, 3, 3, 1])),
    Config;
init_per_testcase(_, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    CachePid = spawn(Worker, fun() -> cache_proc(
        #{check_frequency => timer:seconds(10), size => 2}) end),
    [{cache_pid, CachePid} | Config].

end_per_testcase(Case, Config) when
    Case =:= basic_ops_test ; Case =:= clearing_test ->
    CachePid = ?config(cache_pid, Config),
    CachePid ! {finish, self()},
    ok = receive
        finished -> ok
    after
        1000 -> timeout
    end;
end_per_testcase(_, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, stop_pool, [?MODULE])).

%%%===================================================================
%%% Internal functions
%%%===================================================================

cache_proc(Options) ->
    tmp_cache:init_cache(?CACHE, Options),
    cache_proc().

cache_proc() ->
    receive
        {tmp_cache_timer, Options} ->
            tmp_cache:check_cache_size(Options),
            cache_proc();
        {finish, Pid} ->
            tmp_cache:terminate_cache(?CACHE, #{}),
            Pid ! finished
    end.

get_slave_ans() ->
    receive
        {slave, Num, _} ->
            [Num | get_slave_ans()]
    after
        10000 ->
            []
    end.

get_slave_ans_with_id() ->
    receive
        {slave, Num, ID} ->
            [{Num, ID} | get_slave_ans_with_id()]
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
    Master ! {slave, Num, ID},
    ok.

task_finished(_) ->
    timer:sleep(100),
    ok.

save_job(Job, waiting) ->
    List = application:get_env(?CLUSTER_WORKER_APP_NAME, test_job, []),
    ID = list_to_binary(ref_to_list(make_ref())),
    application:set_env(?CLUSTER_WORKER_APP_NAME, test_job, [{ID, Job} | List]),
    {ok, ID};
save_job(_, _) ->
    {ok, <<"ID">>}.

get_job(ID) ->
    {ok, Jobs} = application:get_env(?CLUSTER_WORKER_APP_NAME, test_job),
    {ok, proplists:get_value(ID, Jobs, undefined), undefined}.