%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains bounded_cache and traverse tests.
%%% @end
%%%-------------------------------------------------------------------
-module(traverse_and_utils_test_SUITE).
-author("Michal Wrzeszcz").

-include("datastore_test_utils.hrl").
-include("global_definitions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    cache_basic_ops_test/1,
    cache_clearing_test/1,
    traverse_test/1,
    sequential_traverse_test/1,
    traverse_multitask_concurrent_test/1,
    traverse_multitask_sequential_test/1,
    traverse_loadbalancingt_test/1,
    traverse_loadbalancingt_mixed_ids_test/1,
    traverse_restart_test/1,
    traverse_restart_with_ongoing_jobs_error_test/1,
    traverse_restart_with_task_links_error_test/1,
    traverse_restart_with_job_links_error_test/1,
    traverse_restart_after_node_error_test/1,
    traverse_cancel_test/1,
    traverse_multienvironment_test/1
]).

% For RPC
-export([delete_task_links/0, delete_job_links/0]).

all() ->
    ?ALL([
        cache_basic_ops_test,
        cache_clearing_test,
        traverse_test,
        sequential_traverse_test,
        traverse_multitask_concurrent_test,
        traverse_multitask_sequential_test,
        traverse_loadbalancingt_test,
        traverse_loadbalancingt_mixed_ids_test,
        traverse_restart_test,
        traverse_restart_with_ongoing_jobs_error_test,
        traverse_restart_with_task_links_error_test,
        traverse_restart_with_job_links_error_test,
        traverse_restart_after_node_error_test,
        traverse_cancel_test,
        traverse_multienvironment_test
    ]).

-define(CACHE, test_cache).
-define(CALL_CACHE(Worker, Op, Args), rpc:call(Worker, bounded_cache, Op, [?CACHE | Args])).
-define(POOL, <<"traverse_test_pool">>).
-define(MASTER_POOL_NAME, traverse_test_pool_master).
-define(SLAVE_POOL_NAME, traverse_test_pool_slave).

%%%===================================================================
%%% Test functions
%%%===================================================================

cache_basic_ops_test(Config) ->
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

cache_clearing_test(Config) ->
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
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    TestMap = #{<<"key">> => <<"value">>},
    ?assertEqual(ok, rpc:call(Worker, traverse, run, [?POOL, <<"traverse_test1">>, {self(), 1, 1},
        #{additional_data => TestMap}])),
    ?assertMatch({ok, [<<"traverse_test1">>], _}, rpc:call(Worker, traverse_task_list, list, [?POOL, ongoing])),

    {Expected, Description} = traverse_test_pool:get_expected(),
    Ans = traverse_test_pool:get_slave_ans(false),
    ?assertEqual(Expected, lists:sort(Ans)),

    ?assertMatch({ok, #document{value = #traverse_task{description = Description, enqueued = false, status = finished,
        additional_data = TestMap}}}, rpc:call(Worker, traverse_task, get, [?POOL, <<"traverse_test1">>]), 5),
    ?assertMatch({ok, TestMap}, rpc:call(Worker, traverse_task, get_additional_data, [?POOL, <<"traverse_test1">>]), 5),
    ?assertMatch({ok, [], _}, rpc:call(Worker, traverse_task_list, list, [?POOL, ongoing]), 1),
    check_ended(Worker, [<<"traverse_test1">>]),
    traverse_test_pool:check_schedulers_after_test(Worker, Workers, ?POOL).

sequential_traverse_test(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, run, [?POOL, <<"sequential_traverse_test">>, {self(), 1, 1}])),
    ?assertMatch({ok, [<<"sequential_traverse_test">>], _}, rpc:call(Worker, traverse_task_list, list, [?POOL, ongoing])),

    {Expected, Description} = traverse_test_pool:get_expected(),
    Ans = traverse_test_pool:get_slave_ans(false),
    ?assertEqual(Expected, lists:sort(Ans)),
    check_ans_sorting(Ans),

    ?assertMatch({ok, #document{value = #traverse_task{description = Description, enqueued = false, status = finished}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"sequential_traverse_test">>]), 5),
    ?assertMatch({ok, [], _}, rpc:call(Worker, traverse_task_list, list, [?POOL, ongoing]), 1),
    check_ended(Worker, [<<"sequential_traverse_test">>]),
    traverse_test_pool:check_schedulers_after_test(Worker, Workers, ?POOL).

traverse_multitask_concurrent_test(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?POOL, <<"1traverse_multitask_concurrent_test">>, {self(), 1, 1}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?POOL, <<"2traverse_multitask_concurrent_test">>, {self(), 1, 2}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?POOL, <<"3traverse_multitask_concurrent_test">>, {self(), 1, 3}])),

    {Expected0, Description} = traverse_test_pool:get_expected(),
    Expected = Expected0 ++ Expected0 ++ Expected0,
    Ans = traverse_test_pool:get_slave_ans(false),
    ?assertEqual(lists:sort(Expected), lists:sort(Ans)),

    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"1traverse_multitask_concurrent_test">>]), 2),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"2traverse_multitask_concurrent_test">>]), 2),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"3traverse_multitask_concurrent_test">>]), 2),

    traverse_test_pool:check_schedulers_after_test(Worker, Workers, ?POOL).

traverse_multitask_sequential_test(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?POOL, <<"1traverse_multitask_sequential_test">>, {self(), 1, 1}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?POOL, <<"2traverse_multitask_sequential_test">>, {self(), 1, 2}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?POOL, <<"3traverse_multitask_sequential_test">>, {self(), 1, 3}])),

    ?assertMatch({ok, [<<"1traverse_multitask_sequential_test">>], _},
        rpc:call(Worker, traverse_task_list, list, [?POOL, ongoing])),
    ?assertMatch({ok, [<<"2traverse_multitask_sequential_test">>, <<"3traverse_multitask_sequential_test">>], _},
        rpc:call(Worker, traverse_task_list, list, [?POOL, scheduled])),

    {Expected, Description} = traverse_test_pool:get_expected(),
    ExpLen = length(Expected),
    Ans = traverse_test_pool:get_slave_ans(false),

    Ans1 = lists:sublist(Ans, 1, ExpLen),
    Ans2 = lists:sublist(Ans, ExpLen + 1, ExpLen),
    Ans3 = lists:sublist(Ans, 2 * ExpLen + 1, ExpLen),
    ?assertEqual(Expected, lists:sort(Ans1)),
    ?assertEqual(Expected, lists:sort(Ans2)),
    ?assertEqual(Expected, lists:sort(Ans3)),

    ?assertMatch({ok, #document{value = #traverse_task{description = Description, enqueued = false, status = finished}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"1traverse_multitask_sequential_test">>]), 2),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description, enqueued = false, status = finished}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"2traverse_multitask_sequential_test">>]), 2),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description, enqueued = false, status = finished}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"3traverse_multitask_sequential_test">>]), 2),

    ?assertMatch({ok, [], _}, rpc:call(Worker, traverse_task_list, list, [?POOL, ongoing]), 1),
    ?assertMatch({ok, [], _}, rpc:call(Worker, traverse_task_list, list, [?POOL, scheduled])),
    check_ended(Worker, [<<"1traverse_multitask_sequential_test">>, <<"2traverse_multitask_sequential_test">>,
        <<"3traverse_multitask_sequential_test">>]),
    traverse_test_pool:check_schedulers_after_test(Worker, Workers, ?POOL).

traverse_loadbalancingt_test(Config) ->
    Tasks = [{<<"tlt1">>, <<"tlt1">>, 1}, {<<"tlt2">>, <<"tlt2">>, 2}, {<<"tlt3">>, <<"tlt2">>, 3},
        {<<"tlt4">>, <<"tlt2">>, 4}, {<<"tlt5">>, <<"tlt3">>, 5}, {<<"tlt6">>, <<"tlt3">>, 6}],
    Check = [{1,1}, {2,2}, {3,5}, {4,3}, {5,6}, {6,4}],
    traverse_loadbalancingt_base(Config, Tasks, Check).

traverse_loadbalancingt_mixed_ids_test(Config) ->
    Tasks = [{<<"9tlmid">>, <<"1tlmid">>, 1}, {<<"2tlmid">>, <<"2tlmid">>, 2}, {<<"3tlmid">>, <<"2tlmid">>, 3},
        {<<"8tlmid">>, <<"2tlmid">>, 4}, {<<"5tlmid">>, <<"3tlmid">>, 5}, {<<"6tlmid">>, <<"3tlmid">>, 6}],
    Check = [{1,1}, {2,2}, {3,5}, {4,3}, {5,6}, {6,4}],
    traverse_loadbalancingt_base(Config, Tasks, Check).

traverse_loadbalancingt_base(Config, Tasks, Check) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun({ID, GR, Ans}) ->
        ?assertEqual(ok, rpc:call(Worker, traverse, run, [?POOL, ID, {self(), 1, Ans}, #{group_id => GR}]))
    end, Tasks),

    {Expected, Description} = traverse_test_pool:get_expected(),
    ExpLen = length(Expected),
    Ans = traverse_test_pool:get_slave_ans(true),

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
            rpc:call(Worker, traverse_task, get, [?POOL, ID]), 2)
    end, Tasks),

    traverse_test_pool:check_schedulers_after_test(Worker, Workers, ?POOL).

traverse_restart_test(Config) ->
    traverse_restart_test_base(Config, <<"traverse_restart_test">>, fun() -> ok end, finished).

traverse_restart_with_ongoing_jobs_error_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    DeleteMetadataFun = fun() ->
        ?assertEqual(ok, traverse_test_pool:delete_ongoing_jobs(Worker))
    end,
    traverse_restart_test_base(Config, <<"traverse_restart_with_ongoing_jobs_error_test">>, DeleteMetadataFun, canceled).

traverse_restart_with_task_links_error_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    DeleteMetadataFun = fun() ->
        ?assertEqual(ok, rpc:call(Worker, ?MODULE, delete_task_links, []))
    end,
    traverse_restart_test_base(Config, <<"traverse_restart_with_task_links_error_test">>, DeleteMetadataFun, finished).

delete_task_links() ->
    Ctx = traverse_task:get_ctx(),
    Forest = traverse_task_list:forest_key(?POOL, ongoing),

    {ok, Tasks} = datastore_model:fold_links(Ctx, Forest, all, fun
        (#link{name = Name, tree_id = TreeId}, Acc) -> {ok, [{Name, TreeId} | Acc]}
    end, [], #{}),

    lists:foreach(fun({TaskId, Tree}) ->
        [ok] = datastore_model:delete_links(Ctx,
            Forest, Tree, [TaskId])
    end, Tasks).

traverse_restart_with_job_links_error_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    DeleteMetadataFun = fun() ->
        ?assertEqual(ok, rpc:call(Worker, ?MODULE, delete_job_links, []))
    end,
    traverse_restart_test_base(Config, <<"traverse_restart_with_job_links_error_test">>, DeleteMetadataFun, canceled).

delete_job_links() ->
    {ok, JobIds} = traverse_task_list:list_node_jobs(?POOL, traverse_test_pool, node()),
    lists:foreach(fun(JobId) ->
        ok = traverse_task_list:delete_job_link(?POOL, traverse_test_pool, node(), JobId)
    end, JobIds).

traverse_restart_after_node_error_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    DeleteMetadataFun = fun() ->
        ?assertEqual(ok, rpc:call(Worker, application, set_env, [?CLUSTER_WORKER_APP_NAME,
            application_closing_status, last_closing_procedure_failed]))
    end,
    traverse_restart_test_base(Config, <<"traverse_restart_after_node_error_test">>, DeleteMetadataFun, canceled).

traverse_restart_test_base(Config, TaskNameBase, BeforeRestartAction, FirstJobStatus) ->
    Task1 = <<"1", TaskNameBase/binary>>,
    Task2 = <<"2", TaskNameBase/binary>>,
    Task3 = <<"3", TaskNameBase/binary>>,

    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, run, [?POOL, Task1, {self(), 1, 100}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run, [?POOL, Task2, {self(), 1, 2}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run, [?POOL, Task3, {self(), 1, 3}])),

    RecAns = receive 
        {stop, _} ->
           ?assertEqual(ok, rpc:call(Worker, worker_pool, stop_sup_pool, [?MASTER_POOL_NAME])),
           ?assertEqual(ok, rpc:call(Worker, worker_pool, stop_sup_pool, [?SLAVE_POOL_NAME]))
    after
       5000 ->
           timeout
    end,
    ?assertEqual(ok, RecAns),

    BeforeRestartAction(),

    ?assertMatch({ok, _}, rpc:call(Worker, worker_pool, start_sup_pool, [?MASTER_POOL_NAME,
        [{workers, 3}, {queue_type, lifo}]])),
    ?assertMatch({ok, _}, rpc:call(Worker, worker_pool, start_sup_pool, [?SLAVE_POOL_NAME,
        [{workers, 3}, {queue_type, lifo}]])),
    ?assertEqual(ok, rpc:call(Worker, traverse, restart_tasks, [?POOL, #{}, Worker])),

    {Expected, Description} = traverse_test_pool:get_expected(),
    ExpLen = length(Expected),
    Ans = traverse_test_pool:get_slave_ans(false),
    AnsLen = length(Ans),

    Ans1 = lists:sublist(Ans, 1, AnsLen - 2*ExpLen),
    Ans2 = lists:sublist(Ans, AnsLen - 2*ExpLen  + 1, ExpLen),
    Ans3 = lists:sublist(Ans, AnsLen - ExpLen + 1, ExpLen),
    ?assertEqual(Expected, lists:sort(Ans2)), % check answers form task 2
    ?assertEqual(Expected, lists:sort(Ans3)), % check answers form task 3
    case FirstJobStatus of
        % check answers form task 1
        finished -> ?assertEqual(length(Ans1 -- Expected), length(Ans1) - length(Expected));
        canceled -> ok % first job was canceled so we cannot perform check for it
    end,

    ?assertMatch({ok, #document{value = #traverse_task{status = FirstJobStatus}}},
        rpc:call(Worker, traverse_task, get, [?POOL, Task1]), 2),
    ?assertMatch({ok, #document{value = #traverse_task{status = finished, description = Description}}},
        rpc:call(Worker, traverse_task, get, [?POOL, Task2]), 2),
    ?assertMatch({ok, #document{value = #traverse_task{status = finished, description = Description}}},
        rpc:call(Worker, traverse_task, get, [?POOL, Task3]), 2),

    traverse_test_pool:check_schedulers_after_test(Worker, Workers, ?POOL).

traverse_cancel_test(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?POOL, <<"1traverse_cancel_test">>, {self(), 1, 100}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?POOL, <<"2traverse_cancel_test">>, {self(), 1, 2}])),
    ?assertEqual(ok, rpc:call(Worker, traverse, run,
        [?POOL, <<"3traverse_cancel_test">>, {self(), 1, 3}])),

    ?assertMatch({ok, [<<"1traverse_cancel_test">>], _},
        rpc:call(Worker, traverse_task_list, list, [?POOL, ongoing])),
    ?assertMatch({ok, [<<"2traverse_cancel_test">>, <<"3traverse_cancel_test">>], _},
        rpc:call(Worker, traverse_task_list, list, [?POOL, scheduled])),

    RecAns = receive
        {stop, _} ->
            ?assertEqual(ok, rpc:call(Worker, traverse, cancel, [?POOL, <<"1traverse_cancel_test">>]))
    after
        5000 ->
            timeout
    end,
    ?assertEqual(ok, RecAns),

    {Expected, Description} = traverse_test_pool:get_expected(),
    ExpLen = length(Expected),
    Ans = traverse_test_pool:get_slave_ans(false),
    AnsLen = length(Ans),

    Ans1 = lists:sublist(Ans, 1, AnsLen - 2*ExpLen),
    Ans2 = lists:sublist(Ans, AnsLen - 2*ExpLen  + 1, ExpLen),
    Ans3 = lists:sublist(Ans, AnsLen - ExpLen + 1, ExpLen),
    ?assertEqual(Expected, lists:sort(Ans2)),
    ?assertEqual(Expected, lists:sort(Ans3)),
    ?assertEqual([], Ans1 -- Expected),
    ?assert(length(Expected) > length(Ans1)),

    ?assertMatch({ok, #document{value = #traverse_task{canceled = true, status = canceled}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"1traverse_cancel_test">>]), 2),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description, canceled = false, status = finished}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"2traverse_cancel_test">>]), 2),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description, canceled = false, status = finished}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"3traverse_cancel_test">>]), 2),

    ?assertMatch({ok, [], _}, rpc:call(Worker, traverse_task_list, list, [?POOL, ongoing]), 1),
    ?assertMatch({ok, [], _}, rpc:call(Worker, traverse_task_list, list, [?POOL, scheduled])),
    check_ended(Worker, [<<"1traverse_cancel_test">>, <<"2traverse_cancel_test">>,
        <<"3traverse_cancel_test">>]),
    traverse_test_pool:check_schedulers_after_test(Worker, Workers, ?POOL).

traverse_multienvironment_test(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, run, [?POOL, <<"traverse_multienvironment_test">>,
        {self(), 1, 1}, #{creator => <<"creator">>, executor => <<"executor">>}])),

    {Expected, Description} = traverse_test_pool:get_expected(),
    ?assertEqual([], traverse_test_pool:get_slave_ans(false)),

    {ok, Task} = ?assertMatch({ok, _}, rpc:call(Worker, traverse_task, get, [?POOL, <<"traverse_multienvironment_test">>])),
    ?assertEqual(ok, rpc:call(Worker, traverse, on_task_change, [Task, <<"executor">>])),
    Ans = traverse_test_pool:get_slave_ans(false),

    ?assertEqual(Expected, lists:sort(Ans)),

    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [?POOL, <<"traverse_multienvironment_test">>]), 2),

    traverse_test_pool:check_schedulers_after_test(Worker, Workers, ?POOL).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [traverse_test_pool]} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(sequential_traverse_test, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, application, set_env, [?CLUSTER_WORKER_APP_NAME, test_job, []]),
    rpc:call(Worker, application, set_env, [?CLUSTER_WORKER_APP_NAME, ongoing_job, []]),
    ?assertEqual(ok, rpc:call(Worker, traverse, init_pool, [?POOL, 1, 3, 10])),
    Config;
init_per_testcase(Case, Config) when
    Case =:= traverse_test ; Case =:= traverse_multitask_concurrent_test ;
    Case =:= traverse_multienvironment_test ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, application, set_env, [?CLUSTER_WORKER_APP_NAME, test_job, []]),
    rpc:call(Worker, application, set_env, [?CLUSTER_WORKER_APP_NAME, ongoing_job, []]),
    ?assertEqual(ok, rpc:call(Worker, traverse, init_pool, [?POOL, 3, 3, 10])),
    Config;
init_per_testcase(Case, Config) when
    Case =:= traverse_multitask_sequential_test ; Case =:= traverse_loadbalancingt_test ;
    Case =:= traverse_loadbalancingt_mixed_ids_test ; Case =:= traverse_restart_test ;
    Case =:= traverse_restart_with_ongoing_jobs_error_test ; Case =:= traverse_restart_with_task_links_error_test ;
    Case =:= traverse_restart_with_job_links_error_test ; Case =:= traverse_restart_after_node_error_test ;
    Case =:= traverse_cancel_test ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, application, set_env, [?CLUSTER_WORKER_APP_NAME, test_job, []]),
    rpc:call(Worker, application, set_env, [?CLUSTER_WORKER_APP_NAME, ongoing_job, []]),
    ?assertEqual(ok, rpc:call(Worker, traverse, init_pool, [?POOL, 3, 3, 1])),
    Config;
init_per_testcase(_, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    CachePid = spawn(Worker, fun() -> cache_proc(
        #{check_frequency => timer:seconds(10), size => 2}) end),
    [{cache_pid, CachePid} | Config].

end_per_testcase(Case, Config) when
    Case =:= cache_basic_ops_test ; Case =:= cache_clearing_test ->
    CachePid = ?config(cache_pid, Config),
    CachePid ! {finish, self()},
    ok = receive
        finished -> ok
    after
        1000 -> timeout
    end;
end_per_testcase(_, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    timer:sleep(2000), % Allow pool processes to finish jobs
    ?assertEqual(ok, rpc:call(Worker, traverse, stop_pool, [?POOL])).

%%%===================================================================
%%% Internal functions
%%%===================================================================

cache_proc(Options) ->
    bounded_cache:init_cache(?CACHE, Options),
    cache_proc().

cache_proc() ->
    receive
        {bounded_cache_timer, Options} ->
            bounded_cache:check_cache_size(Options),
            cache_proc();
        {finish, Pid} ->
            bounded_cache:terminate_cache(?CACHE),
            Pid ! finished
    end.

check_ended(Worker, Tasks) ->
    {ok, Ans, _} = ?assertMatch({ok, _, _}, rpc:call(Worker, traverse_task_list, list, [?POOL, ended])),
    ?assertEqual([], Tasks -- Ans).

check_ans_sorting([]) ->
    ok;
check_ans_sorting([A1, A2, A3 | Tail]) ->
    ?assertEqual([A1, A2, A3], lists:sort([A1, A2, A3])),
    check_ans_sorting(Tail).
