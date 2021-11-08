%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore ha tests common utils.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_model_ha_test_common).
-author("Michal Wrzeszcz").

-include("datastore_test_utils.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/hashing/consistent_hashing.hrl").


%% tests
-export([
    saves_should_propagate_to_backup_node/2,
    saves_should_not_propagate_to_backup_node/3,
    saves_should_propagate_to_backup_node_after_config_change/2,
    calls_should_change_node/2,
    saves_should_use_recovered_node/2,
    saves_should_change_node_dynamic/2,
    node_transition_test/4,

    node_adding_test/4,
    node_adding_multikey_test/4,
    node_deletion_test/4,
    node_deletion_multikey_test/4
]).

%% init/teardown functions
-export([init_per_testcase_base/1, end_per_testcase_base/1]).

%% helper functions
-export([set_ha/3, assert_in_memory/3, assert_on_disc/3]).


-define(DOC(Model), ?DOC(?KEY, Model)).
-define(DOC(Key, Model), ?BASE_DOC(Key, ?MODEL_VALUE(Model))).

-define(ATTEMPTS, 30).

-define(REPEATS, 5).
-define(SUCCESS_RATE, 100).


%%%===================================================================
%%% Tests skeletons
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

saves_should_not_propagate_to_backup_node(Config, LocalRouting, Models) ->
    {Key, KeyNode, KeyNode2, TestWorker} = prepare_ha_test(Config),
    set_ha(Config, change_config, [2, call]),

    CallWorker = case LocalRouting of
        true -> KeyNode;
        false -> TestWorker
    end,

    lists:foreach(fun(Model) ->
        ?assertMatch({ok, #document{}}, rpc:call(CallWorker, Model, save, [?DOC(Key, Model)])),

        assert_in_memory(KeyNode, Model, Key),
        assert_on_disc(TestWorker, Model, Key),
        assert_not_in_memory(KeyNode2, Model, Key)
    end, Models).

saves_should_propagate_to_backup_node_after_config_change(Config, LocalRouting) ->
    {Key, KeyNode, KeyNode2, TestWorker} = prepare_ha_test(Config),

    CallWorker = case LocalRouting of
        true -> KeyNode;
        false -> TestWorker
    end,

    lists:foreach(fun(Model) ->
        set_ha(Config, change_config, [1, call]),
        ?assertMatch({ok, #document{}}, rpc:call(CallWorker, Model, save, [?DOC(Key, Model)])),
        assert_in_memory(KeyNode, Model, Key),
        assert_on_disc(TestWorker, Model, Key),
        assert_not_in_memory(KeyNode2, Model, Key),

        set_ha(Config, change_config, [2, call]),
        assert_in_memory(KeyNode2, Model, Key),
        terminate_processes(Config)
    end, [ets_only_model, mnesia_only_model]).

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
        set_ha(Config, change_config, [1, cast]),
        terminate_processes(Config)
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
        ?assertEqual(ok, rpc:call(TestWorker, consistent_hashing, report_node_recovery, [KeyNode])),
        terminate_processes(Config)
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
        set_ha(KeyNode2, set_standby_mode_and_broadcast_master_up_message, []),
        terminate_processes(Config)
    end, ?TEST_MODELS -- [disc_only_model]).

node_transition_test(Config, Method, SimulateSlowUpdate, DelayRingRepair) ->
    {Key, KeyNode, KeyNode2, TestWorker} = prepare_ha_test(Config),
    MasterPid = self(),
    set_ha(Config, change_config, [2, Method]),

    lists:foreach(fun(Model) ->
        mock_node_down(TestWorker, KeyNode2, KeyNode),
        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [?DOC(Key, Model)])),
        assert_in_memory(KeyNode2, Model, Key),
        assert_on_disc(TestWorker, Model, Key),
        assert_not_in_memory(KeyNode, Model, Key),

        UpdateFun = get_update_fun(MasterPid, SimulateSlowUpdate),

        case SimulateSlowUpdate of
            true ->
                % Spawn process that will start update function before calling master up but will end it after
                % next update is called (due to sleep in update fun)
                spawn(fun() ->
                    ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])) end),
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

node_adding_test(Config, Method, SimulateSlowUpdate, DelayLastCheck) ->
    cluster_reorganization_test(Config, Method, SimulateSlowUpdate, DelayLastCheck, add, prev),
    cluster_reorganization_test(Config, Method, SimulateSlowUpdate, DelayLastCheck, add, next).

node_deletion_test(Config, Method, SimulateSlowUpdate, DelayLastCheck) ->
    cluster_reorganization_test(Config, Method, SimulateSlowUpdate, DelayLastCheck, delete, prev),
    cluster_reorganization_test(Config, Method, SimulateSlowUpdate, DelayLastCheck, delete, next).

cluster_reorganization_test(Config, Method, SimulateSlowUpdate, DelayLastCheck, ReorganizationType, ReconfiguredNodeChoice) ->
    {Key, KeyNodeAfterReorganization, KeyNodeBeforeReorganization, TestWorker, ReorganizedNodes, RingBeforeReorganization} =
        get_full_cluster_reorganization_data(Config, ReorganizationType, ReconfiguredNodeChoice),
    MasterPid = self(),

    lists:foreach(fun(Model) ->
        set_ha(Config, change_config, [1, Method]),
        set_ring(Config, RingBeforeReorganization),

        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [?DOC(Key, Model)])),
        assert_in_memory(KeyNodeBeforeReorganization, Model, Key),
        assert_on_disc(TestWorker, Model, Key),

        case ReorganizationType of
            add -> assert_not_in_memory(KeyNodeAfterReorganization, Model, Key);
            delete -> ok % KeyNodeAfterReorganization is used as slave so document can be in memory of this node
        end,

        UpdateFun = get_update_fun(MasterPid, SimulateSlowUpdate),

        case SimulateSlowUpdate of
            true ->
                % Spawn process that will start update function before node adding but will end it after
                % next update is called (due to sleep in update fun)
                spawn(fun() ->
                    ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])) end),
                timer:sleep(500);
            _ ->
                ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun]))
        end,

        reorganize_cluster(Config, ReorganizedNodes, Method),

        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])),

        check_update_fun(KeyNodeBeforeReorganization, 1),
        check_update_fun(KeyNodeAfterReorganization, 2),

        assert_value_in_memory(KeyNodeAfterReorganization, Model, Key, 3),
        % TODO VFS-6169 - check it
%%        case {ReorganizationType, ReconfiguredNodeChoice} of
%%            {add, prev} -> assert_value_in_memory(KeyNodeBeforeReorganization, Model, Key, 3);
%%            _ -> ok
%%        end,

        case DelayLastCheck of
            true -> timer:sleep(10000); % Wait for race on flush caused by reorganization
            false -> ok
        end,

        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])),
        check_update_fun(KeyNodeAfterReorganization, 3),
        assert_value_in_memory(KeyNodeAfterReorganization, Model, Key, 4),

        % Wait for race on flush caused by reorganization
        case DelayLastCheck of
            true -> ok; % already waited before update
            false -> timer:sleep(10000)
        end,

        finish_reorganization(Config),
        assert_value_on_disc(TestWorker, Model, Key, 4),

        terminate_processes(Config)
    end, ?TEST_MODELS).

node_adding_multikey_test(Config, Method, SimulateSlowUpdate, DelayLastCheck) ->
    cluster_reorganization_multikey_test(Config, Method, SimulateSlowUpdate, DelayLastCheck, add).

node_deletion_multikey_test(Config, Method, SimulateSlowUpdate, DelayLastCheck) ->
    cluster_reorganization_multikey_test(Config, Method, SimulateSlowUpdate, DelayLastCheck, delete).

cluster_reorganization_multikey_test(Config, Method, SimulateSlowUpdate, DelayLastCheck, ReorganizationType) ->
    KeysNum = 1, % VFS-6169 - set to 5000

    {TestWorker, ReorganizedNodes, RingBeforeReorganization} =
        get_simplified_cluster_reorganization_data(Config, ReorganizationType, prev),
    MasterPid = self(),

    lists:foreach(fun(Model) ->
        set_ha(Config, change_config, [1, Method]),
        set_ring(Config, RingBeforeReorganization),

        Keys = lists:map(fun(_) -> datastore_key:new() end, lists:seq(1, KeysNum)),
        UpdateFun = get_update_fun(MasterPid, SimulateSlowUpdate),

        lists_utils:pforeach(fun(Key) ->
            ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [?DOC(Key, Model)])),

            case SimulateSlowUpdate of
                true ->
                    % Spawn process that will start update function before node adding but will end it after
                    % next update is called (due to sleep in update fun)
                    spawn(fun() ->
                        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])) end),
                    timer:sleep(500);
                _ ->
                    ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun]))
            end
        end, Keys),

        reorganize_cluster(Config, ReorganizedNodes, Method),

        lists_utils:pforeach(fun(Key) ->
            ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])),
            ?assertMatch({ok, #document{deleted = false, value = {_, 3, _, _}}},
                rpc:call(TestWorker, Model, get, [Key]), 3)
        end, Keys),

        case DelayLastCheck of
            true -> timer:sleep(10000); % Wait for race on flush
            false -> ok
        end,

        lists_utils:pforeach(fun(Key) ->
            ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, update, [Key, UpdateFun])),
            ?assertMatch({ok, #document{deleted = false, value = {_, 4, _, _}}},
                rpc:call(TestWorker, Model, get, [Key]))
        end, Keys),

        finish_reorganization(Config),

        case DelayLastCheck of
            true -> ok;
            false -> timer:sleep(10000) % Wait for race on flush
        end,
        lists_utils:pforeach(fun(Key) ->
            assert_value_on_disc(TestWorker, Model, Key, 4)
        end, Keys),

        terminate_processes(Config)
    end, ?TEST_MODELS).


%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_testcase_base(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    application:load(cluster_worker),
    {ok, SubtreesNum} = test_utils:get_env(Worker, cluster_worker, tp_subtrees_number),
    application:set_env(cluster_worker, tp_subtrees_number, SubtreesNum),
    Config.


end_per_testcase_base(Config) ->
    Workers = ?config(cluster_worker_nodes, Config),

    Ring = prepare_ring(Config, []),
    set_ring(Config, Ring),
    consistent_hashing:cleanup(),

    lists:foreach(fun(Worker) ->
        lists:foreach(fun(FixedWorker) ->
            ?assertEqual(ok, rpc:call(Worker, consistent_hashing, report_node_recovery, [FixedWorker]))
        end, Workers)
    end, Workers),

    terminate_processes(Config),
    set_ha(Config, set_standby_mode_and_broadcast_master_up_message, []),
    set_ha(Config, change_config, [1, cast]),
    test_utils:mock_unload(Workers, [ha_datastore_master]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

reorganize_cluster(Config, ReorganizedNodes, Method) ->
    [Worker1 | _] = Workers = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker1, consistent_hashing, init_cluster_resizing, [ReorganizedNodes])),
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, ha_datastore, reorganize_cluster, []))
    end, Workers),
    set_ha(Config, change_config, [2, Method]),
    ?assertEqual(ok, rpc:call(Worker1, consistent_hashing, finalize_cluster_resizing, [])).

finish_reorganization(Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, ha_datastore, finish_reorganization, []))
    end, Workers).

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

get_update_fun(MasterPid, SimulateSlowUpdate) ->
    fun({Model, Field1, Field2, Field3}) ->
        case SimulateSlowUpdate of
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

get_full_cluster_reorganization_data(Config, ReorganizationType, ReconfiguredNodeChoice) ->
    {Key, KeyNodeAfterReorganization, KeyNodeBeforeReorganization, TestWorker, ReorganizedNodes,
        _RingAfterReorganization, RingBeforeReorganization} =
        prepare_cluster_reorganization_data(Config, ReorganizationType, ReconfiguredNodeChoice),
    % RingAfterReorganization is not needed
    {Key, KeyNodeAfterReorganization, KeyNodeBeforeReorganization, TestWorker, ReorganizedNodes, RingBeforeReorganization}.

get_simplified_cluster_reorganization_data(Config, ReorganizationType, ReconfiguredNodeChoice) ->
    {_Key, _KeyNodeAfterReorganization, _KeyNodeBeforeReorganization, TestWorker, ReorganizedNodes,
        _RingAfterReorganization, RingBeforeReorganization} =
        prepare_cluster_reorganization_data(Config, ReorganizationType, ReconfiguredNodeChoice),
    {TestWorker, ReorganizedNodes, RingBeforeReorganization}.

% Generate key to be tested and return it together with 2 rings (current and future) used during the tests,
% 2 nodes responsible for it, test worker (node used for rpc calls) and list of reorganized nodes.
prepare_cluster_reorganization_data(Config, add, ReconfiguredNodeChoice) ->
    Workers = ?config(cluster_worker_nodes, Config),
    FinalRing = prepare_ring(Config, []),
    Key = datastore_key:new(),
    Seed = datastore_key:get_chash_seed(Key),
    #node_routing_info{assigned_nodes = [KeyNode, KeySlaveNode]} = consistent_hashing:get_routing_info(Seed),

    InitialRing = prepare_ring(Config, [KeyNode]),
    % Check which node is responsible for key in new ring
    % if relation between nodes in the rings is not correct try once more
    case {datastore_key:any_responsible_node(Key), ReconfiguredNodeChoice} of
        {KeySlaveNode, prev} ->
            consistent_hashing:cleanup(),
            [TestWorker | _] = Workers -- [KeyNode, KeySlaveNode],
            {Key, KeyNode, KeySlaveNode, TestWorker, Workers, FinalRing, InitialRing};
        % Different nodes are responsible for key in rings
        {OtherNode, next} when OtherNode =/= KeyNode, OtherNode =/= KeySlaveNode ->
            consistent_hashing:cleanup(),
            [TestWorker | _] = Workers -- [KeyNode, OtherNode],
            {Key, KeyNode, OtherNode, TestWorker, Workers, FinalRing, InitialRing};
        _ ->
            prepare_cluster_reorganization_data(Config, add, ReconfiguredNodeChoice)
    end;
prepare_cluster_reorganization_data(Config, delete, ReconfiguredNodeChoice) ->
    {Key, KeyNodeAfterReorganization, KeyNodeBeforeReorganization, TestWorker, ReorganizedNodes,
        RingAfterReorganization, RingBeforeReorganization} =
        prepare_cluster_reorganization_data(Config, add, ReconfiguredNodeChoice),
    % Change nodes and rings order as node will be deleted instead of adding
    {Key, KeyNodeBeforeReorganization, KeyNodeAfterReorganization, TestWorker,
            ReorganizedNodes -- [KeyNodeAfterReorganization], RingBeforeReorganization, RingAfterReorganization}.


set_ring(Config, Ring) ->
    Workers = ?config(cluster_worker_nodes, Config),
    consistent_hashing:set_ring(?CURRENT_RING, Ring),
    consistent_hashing:replicate_ring_to_nodes(Workers).


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