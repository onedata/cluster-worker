%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tmp cache tests.
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
    traverse_test/1
]).

%% Pool callbacks
-export([do_master_job/1, do_slave_job/1, task_finished/1, save_job/2]).

all() ->
    ?ALL([
        basic_ops_test,
        clearing_test,
        traverse_test
    ]).

-define(CACHE, test_cache).
-define(CALL_CACHE(Worker, Op, Args), rpc:call(Worker, tmp_cache, Op, [?CACHE | Args])).
-define(POOL, test_pool).

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
    ?assertEqual(ok, rpc:call(Worker, traverse, run, [?POOL, ?MODULE, <<"1">>, <<"1">>, {self(), 1}])),

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
        rpc:call(Worker, traverse_task, get, [<<"1">>])),
    ok.

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_testcase(traverse_test, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, init_pool, [?POOL, 3, 3, 10])),
    Config;
init_per_testcase(_, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    CachePid = spawn(Worker, fun() -> cache_proc(
        #{check_frequency => timer:seconds(10), size => 2}) end),
    [{cache_pid, CachePid} | Config].

end_per_testcase(traverse_test, _Config) ->
    ok;
end_per_testcase(_, Config) ->
    CachePid = ?config(cache_pid, Config),
    CachePid ! {finish, self()},
    ok = receive
        finished -> ok
    after
        1000 -> timeout
    end.

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
        {slave, Num} ->
            [Num | get_slave_ans()]
    after
        10000 ->
            []
    end.

%%%===================================================================
%%% Pool callbacks
%%%===================================================================

do_master_job({Master, Num}) ->
    MasterJobs = case Num < 1000 of
        true -> [{Master, 10 * Num}, {Master, 10 * Num + 5}];
        _ -> []
    end,

    SlaveJobs = [{Master, Num + 1}, {Master, Num + 2}, {Master, Num + 3}],
    {ok, SlaveJobs, MasterJobs}.

do_slave_job({Master, Num}) ->
    Master ! {slave, Num},
    ok.

task_finished(_) ->
    ok.

save_job(_, _) ->
    ok.