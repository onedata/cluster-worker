%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore models multi-node tests.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_model_multinode_test_SUITE).
-author("Michal Wrzeszcz").

-include("datastore_test_utils.hrl").
-include("global_definitions.hrl").
-include("datastore_performance_tests_base.hrl").
-include_lib("ctool/include/hashing/consistent_hashing.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    save_should_succeed/1,
    disk_fetch_should_succeed/1,

    saves_should_propagate_to_backup_node_cast_ha_test/1,
    saves_should_propagate_to_backup_node_call_ha_test/1,
    calls_should_change_node_cast_ha_test/1,
    calls_should_change_node_call_ha_test/1,
    saves_should_use_recovered_node_cast_ha_test/1,
    saves_should_use_recovered_node_call_ha_test/1,
    saves_should_change_node_dynamic_cast_ha_test/1,
    saves_should_change_node_dynamic_call_ha_test/1,

    node_transition_cast_ha_test/1,
    node_transition_call_ha_test/1,
    node_transition_with_sleep_cast_ha_test/1,
    node_transition_with_sleep_call_ha_test/1,
    node_transition_delayed_ring_repair_cast_ha_test/1,
    node_transition_delayed_ring_repair_call_ha_test/1,
    node_transition_sleep_and_delayed_ring_repair_cast_ha_test/1,
    node_transition_sleep_and_delayed_ring_repair_call_ha_test/1,

    node_adding_cast_ha_test/1,
    node_adding_call_ha_test/1,
    node_adding_with_spawn_cast_ha_test/1,
    node_adding_with_spawn_call_ha_test/1,
    node_adding_without_sleep_cast_ha_test/1,
    node_adding_without_sleep_call_ha_test/1,

    node_adding_multikey_cast_ha_test/1,
    node_adding_multikey_call_ha_test/1,
    node_adding_with_spawn_multikey_cast_ha_test/1,
    node_adding_with_spawn_multikey_call_ha_test/1,
    node_adding_without_sleep_multikey_cast_ha_test/1,
    node_adding_without_sleep_multikey_call_ha_test/1,

    node_deletion_cast_ha_test/1,
    node_deletion_call_ha_test/1,
    node_deletion_with_spawn_cast_ha_test/1,
    node_deletion_with_spawn_call_ha_test/1,
    node_deletion_without_sleep_cast_ha_test/1,
    node_deletion_without_sleep_call_ha_test/1,

    node_deletion_multikey_cast_ha_test/1,
    node_deletion_multikey_call_ha_test/1,
    node_deletion_with_spawn_multikey_cast_ha_test/1,
    node_deletion_with_spawn_multikey_call_ha_test/1,
    node_deletion_without_sleep_multikey_cast_ha_test/1,
    node_deletion_without_sleep_multikey_call_ha_test/1,

    stress_performance_test/1,
    stress_performance_test_base/1,
    memory_only_stress_performance_test/1,
    memory_only_stress_performance_test_base/1,
    stress_with_check_test/1,
    stress_with_check_test_base/1,
    memory_only_stress_with_check_test/1,
    memory_only_stress_with_check_test_base/1
]).

all() ->
    ?ALL([
        save_should_succeed,
        disk_fetch_should_succeed,

        saves_should_propagate_to_backup_node_cast_ha_test,
        saves_should_propagate_to_backup_node_call_ha_test,
        calls_should_change_node_cast_ha_test,
        calls_should_change_node_call_ha_test,
        saves_should_use_recovered_node_cast_ha_test,
        saves_should_use_recovered_node_call_ha_test,
        saves_should_change_node_dynamic_cast_ha_test,
        saves_should_change_node_dynamic_call_ha_test,

        node_transition_cast_ha_test,
        node_transition_call_ha_test,
        node_transition_with_sleep_cast_ha_test,
        node_transition_with_sleep_call_ha_test,
        node_transition_delayed_ring_repair_cast_ha_test,
        node_transition_delayed_ring_repair_call_ha_test,
        node_transition_sleep_and_delayed_ring_repair_cast_ha_test,
        node_transition_sleep_and_delayed_ring_repair_call_ha_test,

        node_adding_cast_ha_test,
        node_adding_call_ha_test,
        node_adding_with_spawn_cast_ha_test,
        node_adding_with_spawn_call_ha_test,
        node_adding_without_sleep_cast_ha_test,
        node_adding_without_sleep_call_ha_test,

        node_adding_multikey_cast_ha_test,
        node_adding_multikey_call_ha_test,
        node_adding_with_spawn_multikey_cast_ha_test,
        node_adding_with_spawn_multikey_call_ha_test,
        node_adding_without_sleep_multikey_cast_ha_test,
        node_adding_without_sleep_multikey_call_ha_test,

        node_deletion_cast_ha_test,
        node_deletion_call_ha_test,
        node_deletion_with_spawn_cast_ha_test,
        node_deletion_with_spawn_call_ha_test,
        node_deletion_without_sleep_cast_ha_test,
        node_deletion_without_sleep_call_ha_test,

        node_deletion_multikey_cast_ha_test,
        node_deletion_multikey_call_ha_test,
        node_deletion_with_spawn_multikey_cast_ha_test,
        node_deletion_with_spawn_multikey_call_ha_test,
        node_deletion_without_sleep_multikey_cast_ha_test,
        node_deletion_without_sleep_multikey_call_ha_test,

        memory_only_stress_with_check_test,
        stress_with_check_test,
        memory_only_stress_performance_test,
        stress_performance_test
    ], [
        memory_only_stress_with_check_test,
        stress_with_check_test,
        memory_only_stress_performance_test,
        stress_performance_test
    ]).

-define(DOC(Model), ?DOC(?KEY, Model)).
-define(DOC(Key, Model), ?BASE_DOC(Key, ?MODEL_VALUE(Model))).

-define(ATTEMPTS, 30).

-define(REPEATS, 5).
-define(SUCCESS_RATE, 100).

%%%===================================================================
%%% Test functions
%%%===================================================================

save_should_succeed(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, save, [?DOC(Model)])
        ),
        lists:foreach(fun(W) -> assert_in_memory(W, Model, Key) end, Workers),
        assert_on_disc(Worker, Model, Key)
    end, ?TEST_MODELS).

disk_fetch_should_succeed(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, save, [?DOC(Model)])
        ),

        assert_on_disc(Worker, Model, Key),

        UniqueKey = ?UNIQUE_KEY(Model, Key),
        MemCtx = datastore_multiplier:extend_name(UniqueKey, ?MEM_CTX(Model)),
        lists:foreach(fun(W) ->
            ?assertMatch({ok, _}, rpc:call(W, ?MEM_DRV(Model), get, [MemCtx, UniqueKey])),
            ?assertEqual(ok, rpc:call(W, ?MEM_DRV(Model), delete, [MemCtx, UniqueKey])),
            ?assertMatch({error, not_found}, rpc:call(W, ?MEM_DRV(Model), get, [MemCtx, UniqueKey]))
        end, Workers),

        lists:foreach(fun(W) ->
            ?assertMatch({ok, #document{}}, rpc:call(W, Model, get, [Key])),
            ?assertMatch({ok, _}, rpc:call(W, ?MEM_DRV(Model), get, [MemCtx, UniqueKey]))
        end, Workers),

        lists:foreach(fun(W) ->
            ?assertEqual(ok, rpc:call(W, ?MEM_DRV(Model), delete, [MemCtx, UniqueKey])),
            ?assertMatch({error, not_found}, rpc:call(W, ?MEM_DRV(Model), get, [MemCtx, UniqueKey])),
            ?assertMatch({ok, #document{}}, rpc:call(W, Model, get, [Key])),
            ?assertMatch({ok, _}, rpc:call(W, ?MEM_DRV(Model), get, [MemCtx, UniqueKey]))
        end, lists:reverse(Workers))
    end, ?TEST_CACHED_MODELS).

%%%===================================================================
%%% HA tests
%%%===================================================================

saves_should_propagate_to_backup_node_cast_ha_test(Config) ->
    saves_should_propagate_to_backup_node(Config, cast).

saves_should_propagate_to_backup_node_call_ha_test(Config) ->
    saves_should_propagate_to_backup_node(Config, call).

calls_should_change_node_cast_ha_test(Config) ->
    calls_should_change_node(Config, cast).

calls_should_change_node_call_ha_test(Config) ->
    calls_should_change_node(Config, call).

saves_should_use_recovered_node_cast_ha_test(Config) ->
    saves_should_use_recovered_node(Config, cast).

saves_should_use_recovered_node_call_ha_test(Config) ->
    saves_should_use_recovered_node(Config, call).

saves_should_change_node_dynamic_cast_ha_test(Config) ->
    saves_should_change_node_dynamic(Config, cast).

saves_should_change_node_dynamic_call_ha_test(Config) ->
    saves_should_change_node_dynamic(Config, call).



node_transition_cast_ha_test(Config) ->
    node_transition_test(Config, cast, false, false).

node_transition_call_ha_test(Config) ->
    node_transition_test(Config, call, false, false).

node_transition_with_sleep_cast_ha_test(Config) ->
    node_transition_test(Config, cast, true, false).

node_transition_with_sleep_call_ha_test(Config) ->
    node_transition_test(Config, call, true, false).

node_transition_delayed_ring_repair_cast_ha_test(Config) ->
    node_transition_test(Config, cast, false, true).

node_transition_delayed_ring_repair_call_ha_test(Config) ->
    node_transition_test(Config, call, false, true).

node_transition_sleep_and_delayed_ring_repair_cast_ha_test(Config) ->
    node_transition_test(Config, cast, true, true).

node_transition_sleep_and_delayed_ring_repair_call_ha_test(Config) ->
    node_transition_test(Config, call, true, true).



node_adding_cast_ha_test(Config) ->
    node_adding_test(Config, cast, false, true).

node_adding_call_ha_test(Config) ->
    node_adding_test(Config, call, false, true).

node_adding_with_spawn_cast_ha_test(Config) ->
    node_adding_test(Config, cast, true, true).

node_adding_with_spawn_call_ha_test(Config) ->
    node_adding_test(Config, call, true, true).

node_adding_without_sleep_cast_ha_test(Config) ->
    node_adding_test(Config, cast, false, false).

node_adding_without_sleep_call_ha_test(Config) ->
    node_adding_test(Config, call, false, false).



node_adding_multikey_cast_ha_test(Config) ->
    node_adding_multikey_test(Config, cast, false, true).

node_adding_multikey_call_ha_test(Config) ->
    node_adding_multikey_test(Config, call, false, true).

node_adding_with_spawn_multikey_cast_ha_test(Config) ->
    node_adding_multikey_test(Config, cast, true, true).

node_adding_with_spawn_multikey_call_ha_test(Config) ->
    node_adding_multikey_test(Config, call, true, true).

node_adding_without_sleep_multikey_cast_ha_test(Config) ->
    node_adding_multikey_test(Config, cast, false, false).

node_adding_without_sleep_multikey_call_ha_test(Config) ->
    node_adding_multikey_test(Config, call, false, false).



node_deletion_cast_ha_test(Config) ->
    node_deletion_test(Config, cast, false, true).

node_deletion_call_ha_test(Config) ->
    node_deletion_test(Config, call, false, true).

node_deletion_with_spawn_cast_ha_test(Config) ->
    node_deletion_test(Config, cast, true, true).

node_deletion_with_spawn_call_ha_test(Config) ->
    node_deletion_test(Config, call, true, true).

node_deletion_without_sleep_cast_ha_test(Config) ->
    node_deletion_test(Config, cast, false, false).

node_deletion_without_sleep_call_ha_test(Config) ->
    node_deletion_test(Config, call, false, false).



node_deletion_multikey_cast_ha_test(Config) ->
    node_deletion_multikey_test(Config, cast, false, true).

node_deletion_multikey_call_ha_test(Config) ->
    node_deletion_multikey_test(Config, call, false, true).

node_deletion_with_spawn_multikey_cast_ha_test(Config) ->
    node_deletion_multikey_test(Config, cast, true, true).

node_deletion_with_spawn_multikey_call_ha_test(Config) ->
    node_deletion_multikey_test(Config, call, true, true).

node_deletion_without_sleep_multikey_cast_ha_test(Config) ->
    node_deletion_multikey_test(Config, cast, false, false).

node_deletion_without_sleep_multikey_call_ha_test(Config) ->
    node_deletion_multikey_test(Config, call, false, false).

%%%===================================================================
%%% HA tests skeletons and helper functions
%%%===================================================================

saves_should_propagate_to_backup_node(Config, Method) ->
    {Key, KeyNode, KeyNode2, TestWorker} = prepare_ha_test(Config),
    set_ha(Config, change_config, [2, Method]),

    lists:foreach(fun(Model) ->
        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [?DOC(Key, Model)])),

        assert_in_memory(KeyNode, Model, Key),
        assert_on_disc(TestWorker, Model, Key),
        assert_in_memory(KeyNode2, Model, Key)
    end, ?TEST_MODELS).

calls_should_change_node(Config, Method) ->
    {Key, KeyNode, KeyNode2, TestWorker} = prepare_ha_test(Config),
    set_ha(Config, change_config, [1, cast]),

    lists:foreach(fun(Model) ->
        {ok, Doc2} = ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [?DOC(Key, Model)])),

        assert_in_memory(KeyNode, Model, Key),
        assert_on_disc(TestWorker, Model, Key),
        assert_not_in_memory(KeyNode2, Model, Key),

        set_ha(Config, change_config, [2, Method]),
        mock_node_down(TestWorker, KeyNode2, KeyNode),

        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [Doc2])),
        assert_in_memory(KeyNode2, Model, Key),

        ?assertMatch(ok, rpc:call(TestWorker, Model, delete, [Key])),
        assert_in_memory(KeyNode2, Model, Key, true),
        assert_on_disc(TestWorker, Model, Key, true),

        mock_node_up(TestWorker, KeyNode2, KeyNode),
        set_ha(Config, change_config, [1, cast])
    end, ?TEST_MODELS).

saves_should_use_recovered_node(Config, Method) ->
    {Key, KeyNode, KeyNode2, TestWorker} = prepare_ha_test(Config),
    set_ha(Config, change_config, [2, Method]),

    lists:foreach(fun(Model) ->
        mock_node_down(TestWorker, KeyNode2, KeyNode),
        {ok, Doc2} = ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [?DOC(Key, Model)])),

        assert_in_memory(KeyNode2, Model, Key),
        assert_on_disc(TestWorker, Model, Key),
        assert_not_in_memory(KeyNode, Model, Key),

        set_ha(KeyNode2, set_standby_mode_and_broadcast_master_up_message, []),
        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [Doc2])),
        assert_in_memory(KeyNode, Model, Key),
        ?assertEqual(ok, rpc:call(TestWorker, consistent_hashing, report_node_recovery, [KeyNode]))
    end, ?TEST_MODELS).

saves_should_change_node_dynamic(Config, Method) ->
    {Key, KeyNode, KeyNode2, TestWorker} = prepare_ha_test(Config),
    Workers = ?config(cluster_worker_nodes, Config),
    ok = test_utils:mock_new(Workers, ha_datastore_master),
    set_ha(Config, change_config, [2, Method]),

    lists:foreach(fun(Model) ->
        UniqueKey = ?UNIQUE_KEY(Model, Key),
        ok = test_utils:mock_expect(Workers, ha_datastore_master, store_backup,
            fun(ProcessKey, Keys, CacheRequests, Data) ->
                Ans = meck:passthrough([ProcessKey, Keys, CacheRequests, Data]),
                case maps:is_key(UniqueKey, Keys) andalso node() =:= KeyNode of
                    true ->
                        tp_router:delete(ProcessKey),
                        throw(test_error);
                    _ ->
                        Ans
                end
            end),

        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [?DOC(Key, Model)])),
        assert_in_memory(KeyNode, Model, Key),
        assert_in_memory(KeyNode2, Model, Key),
        timer:sleep(10000), % Time for disk flush
        assert_not_on_disc(TestWorker, Model, Key),

        set_ha(KeyNode2, set_failover_mode_and_broadcast_master_down_message, []),
        assert_on_disc(TestWorker, Model, Key),
        set_ha(KeyNode2, set_standby_mode_and_broadcast_master_up_message, [])
    end, ?TEST_MODELS -- [disc_only_model]).

node_transition_test(Config, Method, SpawnAndSleep, DelayRingRepair) ->
    {Key, KeyNode, KeyNode2, TestWorker} = prepare_ha_test(Config),
    MasterPid = self(),
    set_ha(Config, change_config, [2, Method]),

    lists:foreach(fun(Model) ->
        mock_node_down(TestWorker, KeyNode2, KeyNode),
        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [?DOC(Key, Model)])),
        assert_in_memory(KeyNode2, Model, Key),
        assert_on_disc(TestWorker, Model, Key),
        assert_not_in_memory(KeyNode, Model, Key),

        UpdateFun = get_update_fun(MasterPid, SpawnAndSleep),

        case SpawnAndSleep of
            true ->
                % Spawn process that will start update function before calling master up but will end it after
                % next update is called (due to sleep in update fun)
                spawn(fun() -> ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])) end),
                timer:sleep(500);
            _ ->
                ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun]))
        end,

        case DelayRingRepair of
            true ->
                ?assertEqual(ok, rpc:call(TestWorker, consistent_hashing, report_node_recovery, [KeyNode]));
            _ ->
                ok
        end,
        set_ha(KeyNode2, set_standby_mode_and_broadcast_master_up_message, []),

        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])),

        check_update_fun(KeyNode2, 1),
        check_update_fun(KeyNode, 2),

        assert_value_in_memory(KeyNode, Model, Key, 3),
        assert_value_in_memory(KeyNode2, Model, Key, 3),
        timer:sleep(10000), % Wait for race on flush
        assert_value_on_disc(TestWorker, Model, Key, 3),

        ?assertEqual(ok, rpc:call(TestWorker, consistent_hashing, report_node_recovery, [KeyNode])),
        terminate_processes(Config)
    end, ?TEST_MODELS).

node_adding_test(Config, Method, SpawnAndSleep, SleepBeforeLastUpdate) ->
    cluster_reorganization_test(Config, Method, SpawnAndSleep, SleepBeforeLastUpdate, add, prev),
    cluster_reorganization_test(Config, Method, SpawnAndSleep, SleepBeforeLastUpdate, add, next).

node_deletion_test(Config, Method, SpawnAndSleep, SleepBeforeLastUpdate) ->
    cluster_reorganization_test(Config, Method, SpawnAndSleep, SleepBeforeLastUpdate, delete, prev),
    cluster_reorganization_test(Config, Method, SpawnAndSleep, SleepBeforeLastUpdate, delete, next).

cluster_reorganization_test(Config, Method, SpawnAndSleep, SleepBeforeLastUpdate, ReorganizationType, ReconfiguredNodeChoice) ->
    [Worker1 | _] = Workers = ?config(cluster_worker_nodes, Config),

    {Key, KeyNode, KeyNode2, TestWorker, NewWorkers, _Ring1, Ring2} =
        prepare_cluster_reorganization_data(Config, ReorganizationType, ReconfiguredNodeChoice),
    MasterPid = self(),

    lists:foreach(fun(Model) ->
        set_ha(Config, change_config, [1, Method]),
        set_ring(Config, Ring2),

        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [?DOC(Key, Model)])),
        assert_in_memory(KeyNode2, Model, Key),
        assert_on_disc(TestWorker, Model, Key),

        case ReorganizationType of
            add -> assert_not_in_memory(KeyNode, Model, Key);
            delete -> ok % KeyNode is used as slave so document can be in memory of this node
        end,

        UpdateFun = get_update_fun(MasterPid, SpawnAndSleep),

        case SpawnAndSleep of
            true ->
                % Spawn process that will start update function before node adding but will end it after
                % next update is called (due to sleep in update fun)
                spawn(fun() -> ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])) end),
                timer:sleep(500);
            _ ->
                ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun]))
        end,

        ?assertEqual(ok, rpc:call(Worker1, consistent_hashing, init_cluster_resizing, [NewWorkers])),
        lists:foreach(fun(Worker) ->
            ?assertEqual(ok, rpc:call(Worker, ha_datastore, reorganize_cluster, []))
        end, Workers),
        set_ha(Config, change_config, [2, Method]),
        ?assertEqual(ok, rpc:call(Worker1, consistent_hashing, finalize_cluster_resizing, [])),

        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])),

        check_update_fun(KeyNode2, 1),
        check_update_fun(KeyNode, 2),

        assert_value_in_memory(KeyNode, Model, Key, 3),
        % TODO VFS-6169 - check it
%%        case {ReorganizationType, ReconfiguredNodeChoice} of
%%            {add, prev} -> assert_value_in_memory(KeyNode2, Model, Key, 3);
%%            _ -> ok
%%        end,

        case SleepBeforeLastUpdate of
            true -> timer:sleep(10000); % Wait for race on flush
            false -> ok
        end,

        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])),
        check_update_fun(KeyNode, 3),
        assert_value_in_memory(KeyNode, Model, Key, 4),

        case SleepBeforeLastUpdate of
            true -> ok;
            false -> timer:sleep(10000) % Wait for race on flush
        end,

        lists:foreach(fun(Worker) ->
            ?assertEqual(ok, rpc:call(Worker, ha_datastore, finish_reorganization, []))
        end, Workers),
        assert_value_on_disc(TestWorker, Model, Key, 4),

        terminate_processes(Config)
    end, ?TEST_MODELS).

node_adding_multikey_test(Config, Method, SpawnAndSleep, SleepBeforeLastUpdate) ->
    cluster_reorganization_multikey_test(Config, Method, SpawnAndSleep, SleepBeforeLastUpdate, add).

node_deletion_multikey_test(Config, Method, SpawnAndSleep, SleepBeforeLastUpdate) ->
    cluster_reorganization_multikey_test(Config, Method, SpawnAndSleep, SleepBeforeLastUpdate, delete).

cluster_reorganization_multikey_test(Config, Method, SpawnAndSleep, SleepBeforeLastUpdate, ReorganizationType) ->
    KeysNum = 1, % VFS-6169 - set to 5000
    [Worker1 | _] = Workers = ?config(cluster_worker_nodes, Config),

    {_Key, _Node, _Node2, TestWorker, NewWorkers, _Ring1, Ring2} =
        prepare_cluster_reorganization_data(Config, ReorganizationType, prev),
    MasterPid = self(),

    lists:foreach(fun(Model) ->
        set_ha(Config, change_config, [1, Method]),
        set_ring(Config, Ring2),

        Keys = lists:map(fun(_) -> datastore_key:new() end, lists:seq(1, KeysNum)),
        UpdateFun = get_update_fun(MasterPid, SpawnAndSleep),

        spawn_foreach_key(Keys, MasterPid, fun(Key) ->
            ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [?DOC(Key, Model)])),

            case SpawnAndSleep of
                true ->
                    % Spawn process that will start update function before node adding but will end it after
                    % next update is called (due to sleep in update fun)
                    spawn(fun() -> ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])) end),
                    timer:sleep(500);
                _ ->
                    ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun]))
            end
        end),

        ?assertEqual(ok, rpc:call(Worker1, consistent_hashing, init_cluster_resizing, [NewWorkers])),

        lists:foreach(fun(Worker) ->
            ?assertEqual(ok, rpc:call(Worker, ha_datastore, reorganize_cluster, []))
        end, Workers),
        set_ha(Config, change_config, [2, Method]),
        ?assertEqual(ok, rpc:call(Worker1, consistent_hashing, finalize_cluster_resizing, [])),
        spawn_foreach_key(Keys, MasterPid, fun(Key) ->
            ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])),
            ?assertMatch({ok, #document{deleted = false, value = {_, 3, _, _}}},
                rpc:call(TestWorker, Model, get, [Key]), 3)
        end),

        case SleepBeforeLastUpdate of
            true -> timer:sleep(10000); % Wait for race on flush
            false -> ok
        end,

        spawn_foreach_key(Keys, MasterPid, fun(Key) ->
            ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])),
            ?assertMatch({ok, #document{deleted = false, value = {_, 4, _, _}}},
                rpc:call(TestWorker, Model, get, [Key]))
        end),

        lists:foreach(fun(Worker) ->
            ?assertEqual(ok, rpc:call(Worker, ha_datastore, finish_reorganization, []))
        end, Workers),


        case SleepBeforeLastUpdate of
            true -> ok;
            false -> timer:sleep(10000) % Wait for race on flush
        end,
        spawn_foreach_key(Keys, MasterPid, fun(Key) ->
            assert_value_on_disc(TestWorker, Model, Key, 4)
        end),

        terminate_processes(Config)
    end, ?TEST_MODELS).

prepare_ha_test(Config) ->
    [Worker0 | _] = Workers = ?config(cluster_worker_nodes, Config),
    set_ha(Config, change_config, [2, cast]),
    Key = datastore_key:new(),
    Seed = rpc:call(Worker0, datastore_key, get_chash_seed, [Key]),
    #node_routing_info{assigned_nodes = [KeyNode, KeyNode2] = KeyNodes} =
        rpc:call(Worker0, consistent_hashing, get_routing_info, [Seed]),
    [TestWorker | _] = Workers -- KeyNodes,

    {Key, KeyNode, KeyNode2, TestWorker}.

set_ha(Worker, Fun, Args) when is_atom(Worker) ->
    ?assertEqual(ok, rpc:call(Worker, ha_datastore, Fun, Args));
set_ha(Config, Fun, Args) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        set_ha(Worker, Fun, Args)
    end, Workers).

mock_node_down(CallNode, ExecuteNode, BrokenNode) ->
    set_ha(ExecuteNode, set_failover_mode_and_broadcast_master_down_message, []),
    ?assertEqual(ok, rpc:call(CallNode, consistent_hashing, report_node_failure, [BrokenNode])).

mock_node_up(CallNode, ExecuteNode, BrokenNode) ->
    ?assertEqual(ok, rpc:call(CallNode, consistent_hashing, report_node_recovery, [BrokenNode])),
    set_ha(ExecuteNode, set_standby_mode_and_broadcast_master_up_message, []).

get_update_fun(MasterPid, SpawnAndSleep) ->
    fun({Model, Field1, Field2, Field3}) ->
        case SpawnAndSleep of
            true -> timer:sleep(2000);
            _ -> ok
        end,
        MasterPid ! {update, Field1, node()},
        {ok, {Model, Field1 + 1, Field2, Field3}}
    end.

check_update_fun(Node, Value) ->
    Rec = receive
        {update, _, _} = Message -> Message
    after
        10000 -> timeout
    end,
    ?assertEqual({update, Value, Node}, Rec).

terminate_processes(Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        rpc:call(Worker, tp_router, broadcast, [force_terminate])
    end, Workers).

prepare_ring(Config, RestrictedWorkers) ->
    Workers = ?config(cluster_worker_nodes, Config),
    Ring = consistent_hashing:init_ring(lists:usort(Workers -- RestrictedWorkers), 2),
    consistent_hashing:set_ring(?CURRENT_RING, Ring),
    Ring.

% Generate key to be tested and return it together with 2 nodes responsible for it, 2 rings (current and future)
% used during the tests, test worker (node used for rpc calls) and list of all initial nodes
prepare_cluster_reorganization_data(Config, add, ReconfiguredNodeChoice) ->
    Workers = ?config(cluster_worker_nodes, Config),
    Ring1 = prepare_ring(Config, []),
    Key = datastore_key:new(),
    Seed = datastore_key:get_chash_seed(Key),
    #node_routing_info{assigned_nodes = [KeyNode, KeyNode2]} = consistent_hashing:get_routing_info(Seed),

    Ring2 = prepare_ring(Config, [KeyNode]),
    % Check which node is responsible for key in new ring
    % if relation between nodes in the rings is not correct try once more
    case {datastore_key:responsible_node(Key), ReconfiguredNodeChoice} of
        {KeyNode2, prev} ->
            consistent_hashing:cleanup(),
            [TestWorker | _] = Workers -- [KeyNode, KeyNode2],
            {Key, KeyNode, KeyNode2, TestWorker, Workers, Ring1, Ring2};
        {KeyNode3, next} when KeyNode3 =/= KeyNode , KeyNode3 =/= KeyNode2 ->
            consistent_hashing:cleanup(),
            [TestWorker | _] = Workers -- [KeyNode, KeyNode3],
            {Key, KeyNode, KeyNode3, TestWorker, Workers, Ring1, Ring2};
        _ ->
            prepare_cluster_reorganization_data(Config, add, ReconfiguredNodeChoice)
    end;
prepare_cluster_reorganization_data(Config, delete, ReconfiguredNodeChoice) ->
    {Key, KeyNode, KeyNode2, TestWorker, Workers, Ring1, Ring2} =
        prepare_cluster_reorganization_data(Config, add, ReconfiguredNodeChoice),
    % Change nodes and rings order as node will be deleted instead of adding
    {Key, KeyNode2, KeyNode, TestWorker, Workers -- [KeyNode], Ring2, Ring1}.


set_ring(Config, Ring) ->
    Workers = ?config(cluster_worker_nodes, Config),
    consistent_hashing:replicate_ring_to_nodes(Workers, ?CURRENT_RING, Ring).

spawn_foreach_key(Keys, MasterPid, Fun) ->
    lists:foreach(fun(Key) -> spawn(fun() ->
        try
            Fun(Key),
            MasterPid ! {slave_ans, ok}
        catch
            Error:Reason ->
                MasterPid ! {slave_ans, {error, Error, Reason, erlang:get_stacktrace()}}
        end
    end) end, Keys),

    lists:foreach(fun(_Key) ->
        Ans = receive
            {slave_ans, Answer} -> Answer
        after
            timer:seconds(30) -> timeout
        end,
        ?assertEqual(ok, Ans)
    end, Keys).

%%%===================================================================
%%% HA stress tests
%%%===================================================================

memory_only_stress_performance_test(Config) ->
    ?PERFORMANCE(Config, [
        ?MULTINODE_TEST(true, 1)
    ]).
memory_only_stress_performance_test_base(Config) ->
    datastore_performance_tests_base:stress_performance_test_base(Config).

stress_performance_test(Config) ->
    ?PERFORMANCE(Config, [
        ?MULTINODE_TEST(false, ?REPEATS)
    ]).
stress_performance_test_base(Config) ->
    datastore_performance_tests_base:stress_performance_test_base(Config).

memory_only_stress_with_check_test(Config) ->
    ?PERFORMANCE(Config, [
        ?MULTINODE_WITH_CHECK_TEST(true, 1)
    ]).
memory_only_stress_with_check_test_base(Config) ->
    datastore_performance_tests_base:stress_performance_test_base(Config).

stress_with_check_test(Config) ->
    ?PERFORMANCE(Config, [
        ?MULTINODE_WITH_CHECK_TEST(false, 1)
    ]).
stress_with_check_test_base(Config) ->
    datastore_performance_tests_base:stress_performance_test_base(Config).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite(?TEST_MODELS, Config,
        fun(Config2) -> Config2 end, [datastore_test_utils, datastore_performance_tests_base]).

init_per_testcase(ha_test, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    application:load(cluster_worker),
    {ok, SubtreesNum} = test_utils:get_env(Worker, cluster_worker, tp_subtrees_number),
    application:set_env(cluster_worker, tp_subtrees_number, SubtreesNum),
    Config;
init_per_testcase(Case, Config) ->
    case lists:suffix("ha_test", atom_to_list(Case)) of
        true ->
            init_per_testcase(ha_test, Config);
        _ ->
            [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
            application:load(cluster_worker),
            {ok, SubtreesNum} = test_utils:get_env(Worker, cluster_worker, tp_subtrees_number),
            application:set_env(cluster_worker, tp_subtrees_number, SubtreesNum),
            test_utils:set_env(Workers, cluster_worker, test_ctx_base, #{memory_copies => all}),
            Config
    end.

end_per_testcase(ha_test, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),

    Ring = prepare_ring(Config, []),
    consistent_hashing:cleanup(),
    set_ring(Config, Ring),

    lists:foreach(fun(Worker) ->
        lists:foreach(fun(FixedWorker) ->
            ?assertEqual(ok, rpc:call(Worker, consistent_hashing, report_node_recovery, [FixedWorker]))
        end, Workers)
    end, Workers),

    terminate_processes(Config),
    set_ha(Config, set_standby_mode_and_broadcast_master_up_message, []),
    set_ha(Config, change_config, [1, cast]),
    test_utils:mock_unload(Workers, [ha_datastore_master]);
end_per_testcase(Case, Config) ->
    case lists:suffix("ha_test", atom_to_list(Case)) of
        true ->
            end_per_testcase(ha_test, Config);
        _ ->
            Workers = ?config(cluster_worker_nodes, Config),
            test_utils:set_env(Workers, cluster_worker, test_ctx_base, #{}),
            ok
    end.

end_per_suite(_Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

assert_in_memory(Worker, Model, Key) ->
    assert_in_memory(Worker, Model, Key, false).

assert_in_memory(Worker, Model, Key, Deleted) ->
    case ?MEM_DRV(Model) of
        undefined ->
            ok;
        Driver ->
            Ctx = datastore_multiplier:extend_name(?UNIQUE_KEY(Model, Key),
                ?MEM_CTX(Model)),
            ?assertMatch({ok, #document{deleted = Deleted}},
                rpc:call(Worker, Driver, get, [
                    Ctx, ?UNIQUE_KEY(Model, Key)
                ]), 5
            )
    end.

assert_value_in_memory(Worker, Model, Key, Value) ->
    case ?MEM_DRV(Model) of
        undefined ->
            ok;
        Driver ->
            Ctx = datastore_multiplier:extend_name(?UNIQUE_KEY(Model, Key),
                ?MEM_CTX(Model)),
            ?assertMatch({ok, #document{deleted = false, value = {_, Value, _, _}}},
                rpc:call(Worker, Driver, get, [
                    Ctx, ?UNIQUE_KEY(Model, Key)
                ]), 5
            )
    end.

assert_not_in_memory(Worker, Model, Key) ->
    case ?MEM_DRV(Model) of
        undefined ->
            ok;
        Driver ->
            Ctx = datastore_multiplier:extend_name(?UNIQUE_KEY(Model, Key),
                ?MEM_CTX(Model)),
            ?assertMatch({error, not_found},
                rpc:call(Worker, Driver, get, [
                    Ctx, ?UNIQUE_KEY(Model, Key)
                ])
            )
    end.

assert_on_disc(Worker, Model, Key) ->
    assert_on_disc(Worker, Model, Key, false).

assert_on_disc(Worker, Model, Key, Deleted) ->
    assert_key_on_disc(Worker, Model, ?UNIQUE_KEY(Model, Key), Deleted).

assert_key_on_disc(Worker, Model, Key, Deleted) ->
    case ?DISC_DRV(Model) of
        undefined ->
            ok;
        Driver ->
            ?assertMatch({ok, _, #document{deleted = Deleted}},
                rpc:call(Worker, Driver, get, [
                    ?DISC_CTX, Key
                ]), ?ATTEMPTS
            )
    end.

assert_value_on_disc(Worker, Model, Key, Value) ->
    case ?DISC_DRV(Model) of
        undefined ->
            ok;
        Driver ->
            ?assertMatch({ok, _, #document{deleted = false, value = {_, Value, _, _}}},
                rpc:call(Worker, Driver, get, [
                    ?DISC_CTX, ?UNIQUE_KEY(Model, Key)
                ]), ?ATTEMPTS
            )
    end.

assert_not_on_disc(Worker, Model, Key) ->
    assert_key_not_on_disc(Worker, Model, ?UNIQUE_KEY(Model, Key)).

assert_key_not_on_disc(Worker, Model, Key) ->
    case ?DISC_DRV(Model) of
        undefined ->
            ok;
        Driver ->
            ?assertMatch({error, not_found},
                rpc:call(Worker, Driver, get, [
                    ?DISC_CTX, Key
                ]), ?ATTEMPTS
            )
    end.