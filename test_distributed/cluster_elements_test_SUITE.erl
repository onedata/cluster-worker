%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This test creates many Erlang virtual machines and uses them
%% to test basic elements of oneprovider Erlang cluster.
%%% @end
%%%--------------------------------------------------------------------
-module(cluster_elements_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("elements/task_manager/task_manager.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").

-define(DICT_KEY, transactions_list).

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([cm_and_worker_test/1, task_pool_test/1, task_manager_repeats_test/1, task_manager_rerun_test/1,
    task_manager_delayed_save_test/1, transaction_test/1, transaction_rollback_test/1, transaction_rollback_stop_test/1,
    multi_transaction_test/1, transaction_retry_test/1, transaction_error_test/1,
    task_manager_delayed_save_with_type_test/1, throttling_test/1]).
-export([transaction_retry_test_base/0, transaction_error_test_base/0]).
-export([configure_throttling/0]).

all() ->
    ?ALL([
        cm_and_worker_test, task_pool_test, task_manager_repeats_test,
        task_manager_rerun_test, task_manager_delayed_save_test, transaction_test, transaction_rollback_test,
        transaction_rollback_stop_test, multi_transaction_test,
        transaction_retry_test, transaction_error_test, task_manager_delayed_save_with_type_test, throttling_test]).

-define(TIMEOUT, timer:minutes(1)).
-define(call(N, M, F, A), rpc:call(N, M, F, A, ?TIMEOUT)).
-define(call_cc(N, F, A), rpc:call(N, caches_controller, F, A, ?TIMEOUT)).
-define(call_test(N, F, A), rpc:call(N, ?MODULE, F, A, ?TIMEOUT)).

-define(MNESIA_THROTTLING_KEY, <<"mnesia_throttling">>).
-define(THROTTLING_ERROR, {error, load_to_high}).

%%%===================================================================
%%% Test functions
%%%===================================================================

throttling_test(Config) ->
    [Worker1, _Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    MockUsage = fun(FailedTasks, AllTasks, MemUsage) ->
        test_utils:mock_expect(Workers, task_pool, count_tasks,
            fun (_, _, _) -> {ok, {FailedTasks, AllTasks}} end),
        test_utils:mock_expect(Workers, monitoring, get_memory_stats,
            fun () -> [{<<"mem">>, MemUsage}] end)
    end,

    {ok,TBT} = test_utils:get_env(Worker1, ?CLUSTER_WORKER_APP_NAME, throttling_base_time_ms),
    TCI = 60,
    TOCI = 30,
    TMT = 4*TBT,
    ok = test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, throttling_check_interval_seconds, TCI),
    ok = test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, throttling_active_check_interval_seconds, TOCI),
    ok = test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, throttling_max_time_ms, TMT),

    VerifyInterval = fun(Ans) ->
        ?assertEqual(timer:seconds(TCI), Ans)
    end,

    VerifyIntervalTh = fun(Ans) ->
        ?assertEqual(timer:seconds(TOCI), Ans)
    end,

    VerifyShortInterval = fun(Ans) ->
        ?assert(timer:seconds(TCI) > Ans)
    end,

    VerifyIntervalOverload = fun(Ans) ->
        ?assertEqual(timer:seconds(TOCI), Ans)
    end,

    CheckThrottling = fun(VerifyIntervalFun, ThrottlingAns, ThrottlingConfig) ->
        {A1, A2} = ?call_test(Worker1, configure_throttling, []),
        ?assertMatch({ok, _}, {A1, A2}),

        NMConfig = case ?call(Worker1, node_management, get, [?MNESIA_THROTTLING_KEY]) of
            {ok, #document{value = #node_management{value = V}}} ->
                V;
            OtherConfig ->
                OtherConfig
        end,
        ?assertEqual(ThrottlingConfig, NMConfig),


        VerifyIntervalFun(A2),
        ?assertEqual(ThrottlingAns, ?call_cc(Worker1, throttle, [throttled_model])),
        ?assertEqual(error, ?call_cc(Worker1, throttle, [throttled_model, error])),
        ?assertEqual(ThrottlingAns, ?call_cc(Worker1, throttle, [throttled_model, ok]))
    end,

    CheckThrottlingDefault = fun() ->
        CheckThrottling(VerifyInterval, ok, {error, {not_found, node_management}})
    end,

    CheckThrottlingAns = fun(Ans, C) ->
        CheckThrottling(VerifyIntervalTh, Ans, C)
    end,

    MockUsage(0,0,10),
    CheckThrottlingDefault(),

    TSFTN = 50,
    ok = test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, throttling_start_failed_tasks_number, 2*TSFTN),
    TSPTN = 500,
    ok = test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, throttling_start_pending_tasks_number, 2*TSPTN),

    MockUsage(TSFTN + 10,0,10.0),
    CheckThrottlingAns(ok, {throttle, 2*TBT, true}),

    MockUsage(TSFTN + 2,0,10.0),
    CheckThrottlingAns(ok, {throttle, 2*TBT, true}),

    MockUsage(TSFTN - 10,0,10.0),
    CheckThrottlingAns(ok, {throttle, TBT, true}),

    MockUsage(TSFTN - 9,0,10.0),
    CheckThrottlingAns(ok, {throttle, 2*TBT, true}),

    MockUsage(TSFTN - 8,0,10.0),
    CheckThrottlingAns(ok, {throttle, 4*TBT, true}),

    MockUsage(TSFTN - 7,0,10.0),
    CheckThrottlingAns(ok, {throttle, 4*TBT, true}),

    MockUsage(TSFTN - 9,0,10.0),
    CheckThrottlingAns(ok, {throttle, 2*TBT, true}),

    MockUsage(2*TSFTN,0,10.0),
    CheckThrottling(VerifyIntervalOverload, ?THROTTLING_ERROR, {overloaded, true}),

    MockUsage(0,0,10.0),
    CheckThrottlingDefault(),

    MockUsage(0, TSPTN + 10,10.0),
    CheckThrottlingAns(ok, {throttle, 2*TBT, true}),

    MockUsage(0, TSPTN + 20,10.0),
    CheckThrottlingAns(ok, {throttle, 4*TBT, true}),

    MockUsage(0, TSPTN + 2,10.0),
    CheckThrottlingAns(ok, {throttle, 4*TBT, true}),

    MockUsage(0,2*TSPTN,10.0),
    CheckThrottling(VerifyIntervalOverload, ?THROTTLING_ERROR, {overloaded, true}),

    MockUsage(0,0,10.0),
    CheckThrottlingDefault(),

    TBMER = 95,
    ok = test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, throttling_block_mem_error_ratio, TBMER),
    NMRTCC = 80,
    ok = test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, node_mem_ratio_to_clear_cache, NMRTCC),
    MemTh = (TBMER + NMRTCC)/2,

    MockUsage(0,0,MemTh - 10.0),
    CheckThrottlingDefault(),

    MockUsage(0,0,MemTh + 1.0),
    CheckThrottlingAns(ok, {throttle, 2*TBT, false}),

    MockUsage(0,0,MemTh + 5.0),
    CheckThrottling(VerifyShortInterval, ok, {throttle, 4*TBT, false}),

    MockUsage(0,0,MemTh + 7.0),
    CheckThrottling(VerifyShortInterval, ok, {throttle, 4*TBT, false}),

    MockUsage(0,0,TBMER + 1.0),
    CheckThrottling(VerifyIntervalOverload, ?THROTTLING_ERROR, {overloaded, false}),

    MockUsage(0,0,MemTh + 1.0),
    CheckThrottling(VerifyIntervalOverload, ?THROTTLING_ERROR, {overloaded, false}),

    MockUsage(0,0,MemTh - 1.0),
    CheckThrottlingAns(ok, {throttle, round(TBT/2), false}),

    MockUsage(0,0,MemTh - 5.0),
    CheckThrottlingAns(ok, {throttle, round(TBT/4), false}),

    MockUsage(0,0,MemTh - 1.0),
    CheckThrottlingAns(ok, {throttle, round(TBT/2), false}),

    MockUsage(TSFTN + 10,0,10.0),
    CheckThrottlingAns(ok, {throttle, round(TBT), true}),

    MockUsage(0,0,MemTh + 1.0),
    CheckThrottling(VerifyShortInterval, ok, {throttle, 2*TBT, false}),

    MockUsage(0,0,NMRTCC - 1.0),
    CheckThrottlingDefault(),

    ok.

cm_and_worker_test(Config) ->
    % given
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),

    % then
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [datastore_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [dns_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker2, worker_proxy, call, [datastore_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker2, worker_proxy, call, [dns_worker, ping])).

task_pool_test(Config) ->
    % given
    [W1, W2] = ?config(cluster_worker_nodes, Config),

    Tasks = [{#task_pool{task = t1}, ?NON_LEVEL, W1}, {#task_pool{task = t2}, ?NODE_LEVEL, W1},
        {#task_pool{task = t3}, ?CLUSTER_LEVEL, W1}, {#task_pool{task = t4}, ?CLUSTER_LEVEL, W2},
        {#task_pool{task = t5}, ?PERSISTENT_LEVEL, W1}, {#task_pool{task = t6}, ?PERSISTENT_LEVEL, W2},
        {#task_pool{task = t7}, ?PERSISTENT_LEVEL, W1}
    ],

    % then
    CreateAns = lists:foldl(fun({Task, Level, Worker}, Acc) ->
        {A1, A2} = rpc:call(Worker, task_pool, create, [Level,
            #document{value = Task#task_pool{owner = pid_to_list(self()), node = node()}}]),
        ?assertMatch({ok, _}, {A1, A2}),
        [{A2, Level} | Acc]
    end, [], Tasks),
    [_ | Keys] = lists:reverse(CreateAns),

    NewNames = [t2_2, t3_2, t4_2, t5_2, t6_2, t7_2],
    ToUpdate = lists:zip(Keys, NewNames),

    lists:foreach(fun({{Key, Level}, NewName}) ->
        ?assertMatch({ok, _}, rpc:call(W1, task_pool, update, [Level, Key, #{task => NewName}]))
    end, ToUpdate),

    ListTest = [{?NODE_LEVEL, [t2_2]}, {?CLUSTER_LEVEL, [t3_2, t4_2]}],
    %TODO - add PERSISTENT_LEVEL checking when list on db will be added
%%     ListTest = [{?NODE_LEVEL, [t2_2]}, {?CLUSTER_LEVEL, [t3_2, t4_2]}, {?PERSISTENT_LEVEL, [t5_2, t6_2, t7_2]}],
    lists:foreach(fun({Level, Names}) ->
        ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),
        {A1, ListedTasks} = rpc:call(W1, task_pool, list, [Level]),
        ?assertMatch({ok, _}, {A1, ListedTasks}),
        ?assertEqual(length(Names), length(ListedTasks)),
        lists:foreach(fun(T) ->
            V = T#document.value,
            ?assert(lists:member(V#task_pool.task, Names))
        end, ListedTasks)
    end, ListTest),

    lists:foreach(fun({{Key, Level}, _}) ->
        ?assertMatch(ok, rpc:call(W1, task_pool, delete, [Level, Key]))
    end, ToUpdate).

task_manager_repeats_test(Config) ->
    task_manager_repeats_test_base(Config, ?NON_LEVEL, 0),
    task_manager_repeats_test_base(Config, ?NODE_LEVEL, 3),
    task_manager_repeats_test_base(Config, ?CLUSTER_LEVEL, 5).
% TODO Uncomment when list on db will be added (without list, task cannot be repeted)
%%     task_manager_repeats_test_base(Config, ?PERSISTENT_LEVEL, 5).

task_manager_repeats_test_base(Config, Level, FirstCheckNum) ->
    [W1, W2] = WorkersList = ?config(cluster_worker_nodes, Config),
    Workers = [W1, W2, W1, W2, W1],

    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, task_fail_min_sleep_time_ms, 500)),
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, task_fail_max_sleep_time_ms, 500))
    end, WorkersList),

    ControllerPid = start_tasks(Level, Workers, 5),
    {A1, A2} = rpc:call(W1, task_pool, list, [Level]),
    ?assertMatch({ok, _}, {A1, A2}),
    ?assertEqual(FirstCheckNum, length(A2)),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),

    ?assertEqual(0, count_answers(), 1, timer:seconds(1)),
    ?assertEqual(5, count_answers(), 1, timer:seconds(10)),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list, [Level])),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),

    ControllerPid ! kill.

task_manager_rerun_test(Config) ->
    task_manager_rerun_test_base(Config, ?NON_LEVEL, 0),
    task_manager_rerun_test_base(Config, ?NODE_LEVEL, 3),
    task_manager_rerun_test_base(Config, ?CLUSTER_LEVEL, 5).
% TODO Uncomment when list on db will be added (without list, task cannot be repeted)
%%     task_manager_rerun_test_base(Config, ?PERSISTENT_LEVEL, 5).

task_manager_rerun_test_base(Config, Level, FirstCheckNum) ->
    [W1, W2] = WorkersList = ?config(cluster_worker_nodes, Config),
    Workers = [W1, W2, W1, W2, W1],

    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, task_fail_min_sleep_time_ms, 100)),
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, task_fail_max_sleep_time_ms, 100))
    end, WorkersList),

    ControllerPid = start_tasks(Level, Workers, 15),
    {A1, A2} = rpc:call(W1, task_pool, list, [Level]),
    ?assertMatch({ok, _}, {A1, A2}),
    ?assertEqual(FirstCheckNum, length(A2)),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),

    ?assertEqual(0, count_answers(), 1, timer:seconds(30)),

    case Level of
        ?NON_LEVEL ->
            ok;
        _ ->
            lists:foreach(fun(W) ->
                gen_server:cast({?NODE_MANAGER_NAME, W}, check_tasks)
            end, WorkersList),
            ?assertEqual(5, count_answers(), 1, timer:seconds(3)),
            ?assertEqual({ok, []}, rpc:call(W1, task_pool, list, [Level])),
            ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level]))
    end,

    ControllerPid ! kill.

task_manager_delayed_save_test(Config) ->
    task_manager_delayed_save_test_base(Config, ?NODE_LEVEL, 3),
    task_manager_delayed_save_test_base(Config, ?CLUSTER_LEVEL, 5).
%%    task_manager_delayed_save_test_base(Config, ?PERSISTENT_LEVEL, 5).

task_manager_delayed_save_test_base(Config, Level, SecondCheckNum) ->
    [W1, W2] = WorkersList = ?config(cluster_worker_nodes, Config),
    Workers = [W1, W2, W1, W2, W1],

    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, task_fail_min_sleep_time_ms, 100)),
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, task_fail_max_sleep_time_ms, 100))
    end, WorkersList),

    ControllerPid = start_tasks(Level, batch, Workers, 15),
    {A1, A2} = rpc:call(W1, task_pool, list, [Level]),
    ?assertMatch({ok, _}, {A1, A2}),
    ?assertEqual(0, length(A2)),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),

    ?assertEqual(0, count_answers(), 1, timer:seconds(30)),

    {A1_2, A2_2} = rpc:call(W1, task_pool, list, [Level]),
    ?assertMatch({ok, _}, {A1_2, A2_2}),
    ?assertEqual(SecondCheckNum, length(A2_2)),
    {A1_3, A2_3} = rpc:call(W1, task_pool, list_failed, [Level]),
    ?assertMatch({ok, _}, {A1_3, A2_3}),
    ?assertEqual(SecondCheckNum, length(A2_3)),

    case Level of
        ?NODE_LEVEL ->
            lists:foreach(fun(W) ->
                gen_server:cast({?NODE_MANAGER_NAME, W}, check_tasks)
            end, WorkersList);
        _ ->
            gen_server:cast({?NODE_MANAGER_NAME, W1}, check_tasks)
    end,

    ?assertEqual(5, count_answers(), 1, timer:seconds(3)),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list, [Level])),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),

    ControllerPid ! kill.

task_manager_delayed_save_with_type_test(Config) ->
    task_manager_delayed_save_with_type_test_base(Config, ?NODE_LEVEL, 3),
    task_manager_delayed_save_with_type_test_base(Config, ?CLUSTER_LEVEL, 5).
%%    task_manager_delayed_save_with_type_test_base(Config, ?PERSISTENT_LEVEL, 5).
task_manager_delayed_save_with_type_test_base(Config, Level, SecondCheckNum) ->
    [W1, W2] = WorkersList = ?config(cluster_worker_nodes, Config),
    Workers = [W1, W2, W1, W2, W1],

    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, task_fail_min_sleep_time_ms, 300)),
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, task_fail_max_sleep_time_ms, 300))
    end, WorkersList),

    ControllerPid = start_tasks(Level, first_try, Workers, 12, type1),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list, [Level])),

    timer:sleep(2000),
    {A1, A2} = rpc:call(W1, task_pool, list, [Level]),
    ?assertMatch({ok, _}, {A1, A2}),
    ?assertEqual(SecondCheckNum, length(A2)),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),
    ?assertEqual({ok, {0,0}}, rpc:call(W1, task_pool, count_tasks, [Level, undefined, 10])),
    ?assertEqual({ok, {0,SecondCheckNum}}, rpc:call(W1, task_pool, count_tasks, [Level, type1, 10])),

    ?assertEqual(0, count_answers(), 1, timer:seconds(30)),

    {A1_2, A2_2} = rpc:call(W1, task_pool, list, [Level]),
    ?assertMatch({ok, _}, {A1_2, A2_2}),
    ?assertEqual(SecondCheckNum, length(A2_2)),
    {A1_3, A2_3} = rpc:call(W1, task_pool, list_failed, [Level]),
    ?assertMatch({ok, _}, {A1_3, A2_3}),
    ?assertEqual(SecondCheckNum, length(A2_3)),
    ?assertEqual({ok, {SecondCheckNum,SecondCheckNum}}, rpc:call(W1, task_pool, count_tasks, [Level, type1, 10])),

    case Level of
        ?NODE_LEVEL ->
            lists:foreach(fun(W) ->
                gen_server:cast({?NODE_MANAGER_NAME, W}, check_tasks)
            end, WorkersList);
        _ ->
            gen_server:cast({?NODE_MANAGER_NAME, W1}, check_tasks)
    end,

    ?assertEqual(5, count_answers(), 1, timer:seconds(3)),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list, [Level])),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),
    ?assertEqual({ok, {0,0}}, rpc:call(W1, task_pool, count_tasks, [Level, type1, 10])),

    ControllerPid ! kill.

start_tasks(Level, Workers, Num) ->
    start_tasks(Level, non, Workers, Num).

start_tasks(Level, DelaySave, Workers, Num) ->
    start_tasks(Level, DelaySave, Workers, Num, undefined).

start_tasks(Level, DelaySave, Workers, Num, TaskType) ->
    ControllerPid = spawn(fun() -> task_controller([]) end),
    Master = self(),
    {Funs, _} = lists:foldl(fun(_W, {Acc, Counter}) ->
        NewAcc = [fun() ->
            ControllerPid ! {get_num, Counter, self()},
            receive
                {value, MyNum} ->
                    case MyNum of
                        Num ->
                            Master ! task_done,
                            ok;
                        _ ->
                            error
                    end
            end
        end | Acc],
        {NewAcc, Counter + 1}
    end, {[], 1}, Workers),

    lists:foreach(fun({Fun, W}) ->
        T = case TaskType of
            undefined -> Fun;
            _ -> {TaskType, Fun}
        end,
        ?assertMatch(ok, rpc:call(W, task_manager, start_task, [T, Level, DelaySave]))
    end, lists:zip(Funs, Workers)),

    ControllerPid.

transaction_test(_Config) ->
    ?assertEqual(undefined, get(?DICT_KEY)),
    ?assertEqual(ok, transaction:start()),
    ?assertEqual(1, length(get(?DICT_KEY))),

    ?assertEqual(ok, transaction:commit()),
    ?assertEqual(0, length(get(?DICT_KEY))),
    ok.

transaction_rollback_test(_Config) ->
    ?assertEqual(ok, transaction:start()),

    Self = self(),
    Rollback1 = fun(Num) ->
        Self ! {rollback, Num},
        {ok, Num + 1}
    end,

    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),

    ?assertEqual(ok, transaction:rollback(1)),
    ?assertEqual(0, length(get(?DICT_KEY))),

    ?assertEqual(ok, get_rollback_ans(1)),
    ?assertEqual(ok, get_rollback_ans(2)),
    ?assertEqual(ok, get_rollback_ans(3)),
    ?assertEqual(non, get_rollback_ans()),

    ok.

transaction_rollback_stop_test(_Config) ->
    ?assertEqual(ok, transaction:start()),

    Self = self(),
    Rollback1 = fun(Num) ->
        Self ! {rollback, Num},
        {ok, Num + 1}
    end,

    Rollback2 = fun(_Num) ->
        {ok, stop}
    end,

    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback2)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),

    ?assertEqual(ok, transaction:rollback(1)),
    ?assertEqual(0, length(get(?DICT_KEY))),

    ?assertEqual(ok, get_rollback_ans(1)),
    ?assertEqual(ok, get_rollback_ans(2)),
    ?assertEqual(ok, get_rollback_ans(3)),
    ?assertEqual(non, get_rollback_ans()),

    ok.

multi_transaction_test(_Config) ->
    ?assertEqual(ok, transaction:start()),

    Self = self(),
    Rollback1 = fun(Num) ->
        Self ! {rollback, Num},
        {ok, Num + 1}
    end,

    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),

    ?assertEqual(ok, transaction:start()),
    ?assertEqual(2, length(get(?DICT_KEY))),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),

    ?assertEqual(ok, transaction:rollback(1)),
    ?assertEqual(1, length(get(?DICT_KEY))),

    ?assertEqual(ok, get_rollback_ans(1)),
    ?assertEqual(non, get_rollback_ans()),

    ?assertEqual(ok, transaction:rollback(11)),
    ?assertEqual(0, length(get(?DICT_KEY))),

    ?assertEqual(ok, get_rollback_ans(11)),
    ?assertEqual(ok, get_rollback_ans(12)),
    ?assertEqual(non, get_rollback_ans()),

    ok.

transaction_retry_test(Config) ->
    [Worker1, _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker1, ?MODULE, transaction_retry_test_base, [])),
    ok.

transaction_retry_test_base() ->
    ?assertEqual(ok, transaction:start()),

    Self = self(),

    Rollback1 = fun(Num) ->
        Self ! {rollback, Num},
        {ok, Num + 1}
    end,

    Rollback2 = fun(Num) ->
        Pid = self(),
        case Pid of
            Self ->
                {retry, Num + 100, some_reason};
            _ ->
                Self ! {rollback, Num},
                {ok, Num + 1}
        end
    end,

    Rollback3 = fun(Num) ->
        Pid = self(),
        case Pid of
            Self ->
                {retry, Num + 100, some_reason};
            _ when Num < 1000 ->
                {retry, Num + 1000, some_reason};
            _ ->
                Self ! {rollback, Num},
                {ok, Num + 1}
        end
    end,

    ?assertEqual(ok, transaction:rollback_point(Rollback2)),
    ?assertEqual(ok, transaction:rollback_point(Rollback3)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback2)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),

    ?assertEqual(task_sheduled, transaction:rollback(1)),
    ?assertEqual(ok, get_rollback_ans(1), 1),
    ?assertEqual(ok, get_rollback_ans(102)),
    ?assertEqual(ok, get_rollback_ans(303)),
    ?assertEqual(ok, get_rollback_ans(1304)),
    ?assertEqual(ok, get_rollback_ans(1305)),
    ?assertEqual(non, get_rollback_ans()),

    ok.

transaction_error_test(Config) ->
    [Worker1, _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker1, ?MODULE, transaction_error_test_base, [])),
    ok.

transaction_error_test_base() ->
    ?assertEqual(ok, transaction:start()),

    Self = self(),

    Rollback1 = fun(Num) ->
        Self ! {rollback, Num},
        {ok, Num + 1}
    end,

    Rollback2 = fun(Num) ->
        Pid = self(),
        case Pid of
            Self ->
                {error, some_error};
            _ ->
                Self ! {rollback, Num},
                {ok, Num + 10}
        end
    end,

    ?assertEqual(ok, transaction:rollback_point(Rollback2)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback2)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),

    ?assertEqual({rollback_fun_error, {error, some_error}}, transaction:rollback(1)),
    ?assertEqual(ok, get_rollback_ans(1), 1),
    ?assertEqual(ok, get_rollback_ans(1)),
    ?assertEqual(ok, get_rollback_ans(2)),
    ?assertEqual(ok, get_rollback_ans(12)),
    ?assertEqual(ok, get_rollback_ans(13)),
    ?assertEqual(non, get_rollback_ans()),

    ok.

get_rollback_ans() ->
    receive
        Other -> {error, Other}
    after
        0 -> non
    end.

get_rollback_ans(Num) ->
    receive
        {rollback, Num} -> ok;
        Other -> {error, Other}
    after
        0 -> non
    end.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(throttling_test, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_new(Workers, [monitoring, task_pool, caches_controller, datastore_config_plugin_default]),

    test_utils:mock_expect(Workers, datastore_config_plugin_default, throttled_models,
        fun () -> [throttled_model] end),

    test_utils:mock_expect(Workers, caches_controller, send_after,
        fun (CheckInterval, Master, Args) -> Master ! {send_after, Args, CheckInterval} end),

    lists:foreach(fun(W) ->
        ?assertEqual(ok, gen_server:call({?NODE_MANAGER_NAME, W}, disable_cache_control))
    end, Workers),

    Config;

init_per_testcase(_Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, task_repeats, 10))
    end, Workers),
    Config.

end_per_testcase(throttling_test, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_unload(Workers, [monitoring, task_pool, caches_controller, datastore_config_plugin_default]);

end_per_testcase(_Case, _Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

task_controller(State) ->
    receive
        kill ->
            ok;
        {get_num, Sender, AnsPid} ->
            Num = proplists:get_value(Sender, State, 0),
            State2 = [{Sender, Num + 1} | State -- [{Sender, Num}]],
            AnsPid ! {value, Num + 1},
            task_controller(State2)
    end.

count_answers() ->
    receive
        task_done ->
            count_answers() + 1
    after
        0 ->
            0
    end.

configure_throttling() ->
    Ans1 = caches_controller:configure_throttling(),
    Ans2 = receive
        {send_after, {timer, configure_throttling}, CheckInterval} -> CheckInterval
    after
        5000 -> timeout
    end,
    {Ans1, Ans2}.