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
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").

-define(DICT_KEY, transactions_list).

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([cm_and_worker_test/1, task_pool_test/1, task_manager_repeats_test/1,
    task_manager_rerun_test/1, task_manager_delayed_save_test/1,
    transaction_test/1, transaction_rollback_test/1,
    transaction_rollback_stop_test/1, multi_transaction_test/1,
    transaction_retry_test/1, transaction_error_test/1,
    task_manager_delayed_save_with_type_test/1, throttling_test/1]).
-export([transaction_retry_test_base/0, transaction_error_test_base/0]).
-export([configure_throttling/0]).

all() ->
    ?ALL([
        cm_and_worker_test, transaction_test, transaction_rollback_test,
        transaction_rollback_stop_test, multi_transaction_test,
        transaction_retry_test, transaction_error_test,
        task_pool_test, task_manager_repeats_test,
        task_manager_rerun_test, task_manager_delayed_save_test,
        task_manager_delayed_save_with_type_test, throttling_test
    ]).

-define(TIMEOUT, timer:minutes(1)).
-define(call(N, M, F, A), rpc:call(N, M, F, A, ?TIMEOUT)).
-define(call_dt(N, F, A), rpc:call(N, datastore_throttling, F, A, ?TIMEOUT)).
-define(call_test(N, F, A), rpc:call(N, ?MODULE, F, A, ?TIMEOUT)).

-define(MNESIA_THROTTLING_KEY, mnesia_throttling).
-define(MEMORY_PROC_IDLE_KEY, throttling_idle_time).
-define(THROTTLING_ERROR, {error, load_to_high}).

%%%===================================================================
%%% Test functions
%%%===================================================================

throttling_test(Config) ->
    [Worker1 | _] = Workers = ?config(cluster_worker_nodes, Config),
    MockUsage = fun(DBQueue, TPSize, MemUsage) ->
        test_utils:mock_expect(Workers, couchbase_pool, get_worker_queue_size_stats,
            fun(_) -> {DBQueue, 0} end),
        test_utils:mock_expect(Workers, couchbase_pool, get_worker_queue_size_stats,
            fun(_, _) -> {DBQueue, 0} end),
        test_utils:mock_expect(Workers, tp, get_processes_number,
            fun() -> TPSize end),
        test_utils:mock_expect(Workers, monitoring, get_memory_stats,
            fun() -> [{<<"mem">>, MemUsage}] end)
    end,

    DBLimit = 15000,
    DBExp = 5000,
    TPLimit = 200000,
    TPExp = 100000,
    MemLimit = 95,
    MemExp = 90,
    test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, throttling_config, [
        {default, [
            {base_time_ms, 2048},
            {strength, 5},
            {tp_param_strength, 1},
            {db_max_param_strength, 1},
            {flush_queue_param_strength, 0},
            {db_sum_param_strength, 0},
            {tp_size_sum_param_strength, 0},
            {mem_param_strength, 1},
            {tp_proc_expected, TPExp},
            {tp_proc_limit, TPLimit},
            {db_queue_max_expected, DBExp},
            {db_queue_max_limit, DBLimit},
            {db_flush_queue_expected, 100000},
            {db_flush_queue_limit, 500000},
            {db_queue_sum_expected, DBExp},
            {db_queue_sum_limit, DBLimit},
            {tp_size_sum_expected, DBExp},
            {tp_size_sum_limit, DBLimit},
            {memory_expected, MemExp},
            {memory_limit, MemLimit}
        ]}
    ]),

    TCI = 60,
    TOCI = 30,
    ok = test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, throttling_check_interval_seconds, TCI),
    ok = test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, throttling_active_check_interval_seconds, TOCI),

    VerifyInterval = fun(Ans) ->
        ?assertEqual(timer:seconds(TCI), Ans)
    end,

    VerifyIntervalTh = fun(Ans) ->
        ?assertEqual(timer:seconds(TOCI), Ans)
    end,

    CheckThrottling = fun(VerifyIntervalFun, ThrottlingAns, ThrottlingConfig) ->
        {A1, A2} = ?call_test(Worker1, configure_throttling, []),
        ?assertMatch({ok, _}, {A1, A2}),

        NMConfig = case rpc:call(Worker1, node_cache, get, [?MNESIA_THROTTLING_KEY, undefined]) of
            undefined -> undefined;
            V -> V
        end,
        ?assertEqual(ThrottlingConfig, NMConfig),


        VerifyIntervalFun(A2),
        ?assertEqual(ThrottlingAns, ?call_dt(Worker1, throttle_model, [throttled_model]))
    end,

    CheckThrottlingDefault = fun() ->
        CheckThrottling(VerifyInterval, ok, [{default,ok}])
    end,

    CheckThrottlingAns = fun(Ans, C) ->
        CheckThrottling(VerifyIntervalTh, Ans, C)
    end,

    VerifyIdle = fun(Ans) ->
        Idle = rpc:call(Worker1, node_cache, get, [?MEMORY_PROC_IDLE_KEY]),
        ?assertEqual(Idle, Ans)
    end,

    MockUsage(0,0,10),
    CheckThrottlingDefault(),

    MockUsage(DBExp + 1000, 0, 10.0),
    CheckThrottlingAns(ok, [{default,{throttle,10}}]),

    MockUsage(DBExp + 5000, 0, 10.0),
    CheckThrottlingAns(ok, [{default,{throttle,952}}]),

    MockUsage(DBExp + 9000, 0, 10.0),
    CheckThrottlingAns(ok, [{default,{throttle,1995}}]),

    MockUsage(DBLimit, 0, 10.0),
    CheckThrottlingAns({error,load_to_high}, [{default,overloaded}]),



    MockUsage(0, TPExp + 10000, 10.0),
    CheckThrottlingAns(ok, [{default,{throttle,10}}]),

    MockUsage(0, TPExp + 50000, 10.0),
    CheckThrottlingAns(ok, [{default,{throttle,952}}]),

    MockUsage(0, TPExp + 90000, 10.0),
    CheckThrottlingAns(ok, [{default,{throttle,1995}}]),

    MockUsage(0, TPLimit, 10.0),
    CheckThrottlingAns({error,load_to_high}, [{default,overloaded}]),



    MockUsage(0, 0, MemExp + 0.5),
    CheckThrottlingAns(ok, [{default,{throttle,10}}]),

    MockUsage(0, 0, MemExp + 2.5),
    CheckThrottlingAns(ok, [{default,{throttle,952}}]),

    MockUsage(0, 0, MemExp + 4.5),
    CheckThrottlingAns(ok, [{default,{throttle,1995}}]),

    MockUsage(0, 0, MemLimit),
    CheckThrottlingAns({error,load_to_high}, [{default,overloaded}]),



    MockUsage(DBExp + 1000, TPExp + 10000, MemExp + 0.5),
    CheckThrottlingAns(ok, [{default,{throttle,30}}]),

    MockUsage(DBExp + 5000, TPExp + 50000, MemExp + 2.5),
    CheckThrottlingAns(ok, [{default,{throttle,1734}}]),

    MockUsage(DBExp + 5000, TPExp + 50000, MemLimit),
    CheckThrottlingAns({error,load_to_high}, [{default,overloaded}]),

    test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, throttling_config, [
        {default, [
            {base_time_ms, 2048},
            {strength, 5},
            {tp_param_strength, 1},
            {db_max_param_strength, 1},
            {flush_queue_param_strength, 0},
            {db_sum_param_strength, 0},
            {tp_size_sum_param_strength, 0},
            {mem_param_strength, 0},
            {tp_proc_expected, TPExp},
            {tp_proc_limit, TPLimit},
            {db_queue_max_expected, DBExp},
            {db_queue_max_limit, DBLimit},
            {db_flush_queue_expected, 100000},
            {db_flush_queue_limit, 500000},
            {db_queue_sum_expected, DBExp},
            {db_queue_sum_limit, DBLimit},
            {tp_size_sum_expected, DBExp},
            {tp_size_sum_limit, DBLimit},
            {memory_expected, MemExp},
            {memory_limit, MemLimit}
        ]}
    ]),

    MockUsage(DBExp + 5000, TPExp + 50000, MemExp + 2.5),
    CheckThrottlingAns(ok, [{default,{throttle,1461}}]),

    MockUsage(DBExp + 5000, TPExp + 50000, MemLimit),
    CheckThrottlingAns(ok, [{default,{throttle,1461}}]),


    Idle1 = 100,
    Idle2 = 1100,
    ok = test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, throttling_reduce_idle_time_memory_proc_number, Idle1),
    ok = test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, throttling_min_idle_time_memory_proc_number, Idle2),
    MaxIdle = 10000,
    MinIdle = 1000,
    ok = test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, memory_store_idle_timeout_ms, MaxIdle),
    ok = test_utils:set_env(Worker1, ?CLUSTER_WORKER_APP_NAME, memory_store_min_idle_timeout_ms, MinIdle),

    MockUsage(0, 50, 10.0),
    CheckThrottlingDefault(),
    VerifyIdle(MaxIdle),

    MockUsage(0, 100, 10.0),
    CheckThrottlingDefault(),
    VerifyIdle(MaxIdle),

    MockUsage(0, 600, 10.0),
    CheckThrottlingDefault(),
    VerifyIdle((MinIdle + MaxIdle) div 2),

    MockUsage(0, 1100, 10.0),
    CheckThrottlingDefault(),
    VerifyIdle(MinIdle),

    MockUsage(0, 2000, 10.0),
    CheckThrottlingDefault(),
    VerifyIdle(MinIdle),

    ok.

cm_and_worker_test(Config) ->
    % given
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),

    % then
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [datastore_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker2, worker_proxy, call, [datastore_worker, ping])).

task_pool_test(Config) ->
    % given
    [W1, W2] = ?config(cluster_worker_nodes, Config),

    Tasks = [
        {#task_pool{task = t1}, ?NON_LEVEL, W1},
        {#task_pool{task = t2}, ?CLUSTER_LEVEL, W1},
        {#task_pool{task = t3}, ?CLUSTER_LEVEL, W2},
        {#task_pool{task = t4}, ?PERSISTENT_LEVEL, W1},
        {#task_pool{task = t5}, ?PERSISTENT_LEVEL, W2},
        {#task_pool{task = t6}, ?PERSISTENT_LEVEL, W1}
    ],

    % then
    CreateAns = lists:foldl(fun({Task, Level, Worker}, Acc) ->
        {ok, Key} = ?assertMatch({ok, _},
            rpc:call(Worker, task_pool, create, [Level, #document{
                value = Task#task_pool{
                    owner = pid_to_list(self()),
                    node = node()
                }
            }])
        ),
        [{Key, Level} | Acc]
    end, [], Tasks),
    [_ | Keys] = lists:reverse(CreateAns),

    NewNames = [t2_2, t3_2, t4_2, t5_2, t6_2],
    ToUpdate = lists:zip(Keys, NewNames),

    lists:foreach(fun({{Key, Level}, NewName}) ->
        ?assertMatch({ok, _}, rpc:call(W1, task_pool, update, [
            Level, Key, fun(TaskPool) ->
                {ok, TaskPool#task_pool{task = NewName}}
            end
        ]))
    end, ToUpdate),

    ListTest = [
        {?CLUSTER_LEVEL, [t2_2, t3_2]},
        {?PERSISTENT_LEVEL, [t4_2, t5_2, t6_2]}
    ],
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
    task_manager_repeats_test_base(Config, ?CLUSTER_LEVEL, 5),
    task_manager_repeats_test_base(Config, ?PERSISTENT_LEVEL, 5).

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

    timer:sleep(timer:seconds(1)),
    ?assertEqual(0, count_answers()),
    ?assertEqual(5, count_answers(), 2, timer:seconds(10)),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list, [Level])),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),

    ControllerPid ! kill.

task_manager_rerun_test(Config) ->
    task_manager_rerun_test_base(Config, ?NON_LEVEL, 0),
    task_manager_rerun_test_base(Config, ?CLUSTER_LEVEL, 5),
    task_manager_rerun_test_base(Config, ?PERSISTENT_LEVEL, 5).

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

    timer:sleep(timer:seconds(30)),
    ?assertEqual(0, count_answers()),

    case Level of
        ?NON_LEVEL ->
            ok;
        _ ->
            lists:foreach(fun(W) ->
                gen_server:cast({?NODE_MANAGER_NAME, W}, force_check_tasks)
            end, WorkersList),
            ?assertEqual(5, count_answers(), 2, timer:seconds(3)),
            ?assertEqual({ok, []}, rpc:call(W1, task_pool, list, [Level]), 2),
            ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level]))
    end,

    ControllerPid ! kill.

task_manager_delayed_save_test(Config) ->
    task_manager_delayed_save_test_base(Config, ?CLUSTER_LEVEL, 5),
    task_manager_delayed_save_test_base(Config, ?PERSISTENT_LEVEL, 5).

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

    timer:sleep(timer:seconds(30)),
    ?assertEqual(0, count_answers()),

    {A1_2, A2_2} = rpc:call(W1, task_pool, list, [Level]),
    ?assertMatch({ok, _}, {A1_2, A2_2}),
    ?assertEqual(SecondCheckNum, length(A2_2)),
    {A1_3, A2_3} = rpc:call(W1, task_pool, list_failed, [Level]),
    ?assertMatch({ok, _}, {A1_3, A2_3}),
    ?assertEqual(SecondCheckNum, length(A2_3)),

    gen_server:cast({?NODE_MANAGER_NAME, W1}, force_check_tasks),

    ?assertEqual(5, count_answers(), 2, timer:seconds(3)),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list, [Level])),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),

    ControllerPid ! kill.

task_manager_delayed_save_with_type_test(Config) ->
    task_manager_delayed_save_with_type_test_base(Config, ?CLUSTER_LEVEL, 5),
    task_manager_delayed_save_with_type_test_base(Config, ?PERSISTENT_LEVEL, 5).
task_manager_delayed_save_with_type_test_base(Config, Level, SecondCheckNum) ->
    [W1, W2] = WorkersList = ?config(cluster_worker_nodes, Config),
    Workers = [W1, W2, W1, W2, W1],

    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, task_fail_min_sleep_time_ms, 200)),
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, task_fail_max_sleep_time_ms, 200))
    end, WorkersList),

    ControllerPid = start_tasks(Level, first_try, Workers, 12, type1, true),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list, [Level])),

    timer:sleep(2000),
    {A1, A2} = rpc:call(W1, task_pool, list, [Level]),
    ?assertMatch({ok, _}, {A1, A2}),
    ?assertEqual(SecondCheckNum, length(A2)),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),
    ?assertEqual({ok, {0, 0}}, rpc:call(W1, task_pool, count_tasks, [Level, undefined, 10])),
    ?assertEqual({ok, {0, SecondCheckNum}}, rpc:call(W1, task_pool, count_tasks, [Level, type1, 10])),

    timer:sleep(timer:seconds(30)),
    ?assertEqual(0, count_answers()),

    {A1_2, A2_2} = rpc:call(W1, task_pool, list, [Level]),
    ?assertMatch({ok, _}, {A1_2, A2_2}),
    ?assertEqual(SecondCheckNum, length(A2_2)),
    {A1_3, A2_3} = rpc:call(W1, task_pool, list_failed, [Level]),
    ?assertMatch({ok, _}, {A1_3, A2_3}),
    ?assertEqual(SecondCheckNum, length(A2_3)),
    ?assertEqual({ok, {SecondCheckNum, SecondCheckNum}}, rpc:call(W1, task_pool, count_tasks, [Level, type1, 10])),

    gen_server:cast({?NODE_MANAGER_NAME, W1}, force_check_tasks),

    ?assertEqual(5, count_answers(), 2, timer:seconds(3)),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list, [Level])),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),
    ?assertEqual({ok, {0, 0}}, rpc:call(W1, task_pool, count_tasks, [Level, type1, 10])),

    ControllerPid ! kill.

start_tasks(Level, Workers, Num) ->
    start_tasks(Level, non, Workers, Num).

start_tasks(Level, DelaySave, Workers, Num) ->
    start_tasks(Level, DelaySave, Workers, Num, undefined).

start_tasks(Level, DelaySave, Workers, Num, TaskType) ->
    start_tasks(Level, DelaySave, Workers, Num, TaskType, false).

start_tasks(Level, DelaySave, Workers, Num, TaskType, Sleep) ->
    ControllerPid = spawn(fun() -> task_controller([]) end),
    Master = self(),
    {Funs, _} = lists:foldl(fun(_W, {Acc, Counter}) ->
        NewAcc = [fun() ->
            case Sleep of
                true ->
                    timer:sleep(100);
                _ ->
                    ok
            end,
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
        1000 -> non
    end.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(throttling_test, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_new(Workers, [
        couchbase_pool,
        monitoring,
        tp,
        datastore_throttling,
        datastore_config
    ]),

    test_utils:mock_expect(Workers, datastore_config, get_throttled_models,
        fun() -> [throttled_model] end
    ),
    test_utils:mock_expect(Workers, datastore_throttling, send_after,
        fun(CheckInterval, Master, Args) ->
            Master ! {send_after, Args, CheckInterval}
        end
    ),
    lists:foreach(fun(W) ->
        ?assertEqual(ok, gen_server:call({?NODE_MANAGER_NAME, W}, disable_throttling))
    end, Workers),

    Config;

init_per_testcase(_Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, task_repeats, 10)),
        ?assertEqual(ok, gen_server:call({?NODE_MANAGER_NAME, W}, disable_task_control))
    end, Workers),
    Config.

end_per_testcase(throttling_test, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),

    lists:foreach(fun(W) ->
        ?assertEqual(ok, gen_server:call({?NODE_MANAGER_NAME, W}, enable_throttling))
    end, Workers),

    test_utils:mock_unload(Workers, [
        couchbase_pool,
        monitoring,
        tp,
        datastore_throttling,
        datastore_config
    ]);

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
    Ans1 = datastore_throttling:configure_throttling(),
    Ans2 = receive
        {send_after, {timer, configure_throttling}, CheckInterval} ->
            CheckInterval
    after
        5000 -> timeout
    end,
    {Ans1, Ans2}.