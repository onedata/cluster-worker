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
    services_migration_test/1,
    service_healthcheck_rescheduling_test/1,

    saves_should_propagate_to_backup_node_cast_ha_test/1,
    saves_should_propagate_to_backup_node_call_ha_test/1,
    saves_should_not_propagate_to_backup_node_when_ha_is_disabled/1,
    saves_should_not_propagate_to_backup_node_for_local_memory_model/1,
    saves_should_propagate_to_backup_node_after_config_change_global_models/1,
    saves_should_propagate_to_backup_node_after_config_change_local_models/1,
    calls_should_change_node_cast_ha_test/1,
    calls_should_change_node_call_ha_test/1,
    saves_should_use_recovered_node_cast_ha_test/1,
    saves_should_use_recovered_node_call_ha_test/1,
    saves_should_change_node_dynamic_cast_ha_test/1,
    saves_should_change_node_dynamic_call_ha_test/1,

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
        services_migration_test,
        service_healthcheck_rescheduling_test,

        saves_should_propagate_to_backup_node_cast_ha_test,
        saves_should_propagate_to_backup_node_call_ha_test,
        saves_should_not_propagate_to_backup_node_when_ha_is_disabled,
        saves_should_not_propagate_to_backup_node_for_local_memory_model,
        saves_should_propagate_to_backup_node_after_config_change_global_models,
        saves_should_propagate_to_backup_node_after_config_change_local_models,
        calls_should_change_node_cast_ha_test,
        calls_should_change_node_call_ha_test,
        saves_should_use_recovered_node_cast_ha_test,
        saves_should_use_recovered_node_call_ha_test,
        saves_should_change_node_dynamic_cast_ha_test,
        saves_should_change_node_dynamic_call_ha_test,

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
        lists:foreach(fun(W) -> datastore_model_ha_test_common:assert_in_memory(W, Model, Key) end, Workers),
        datastore_model_ha_test_common:assert_on_disc(Worker, Model, Key)
    end, ?TEST_MODELS).

disk_fetch_should_succeed(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, save, [?DOC(Model)])
        ),

        datastore_model_ha_test_common:assert_on_disc(Worker, Model, Key),

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

services_migration_test(Config) ->
    [Worker0 | _] = Workers = ?config(cluster_worker_nodes, Config),
    datastore_model_ha_test_common:set_ha(Config, change_config, [2, call]),

    NodeSelector = <<"test">>,
    ServiceName = test_service,
    MasterProc = self(),
    #node_routing_info{assigned_nodes = [Node1, Node2] = AssignedNodes} =
        rpc:call(Worker0, consistent_hashing, get_routing_info, [NodeSelector]),
    [CallWorker | _] = Workers -- AssignedNodes,

    StartTimestamp = global_clock:timestamp_millis(),
    ServiceOptions = #{
        start_function => start_service,
        stop_function => stop_service,
        healthcheck_fun => healthcheck_fun,
        start_function_args => [ServiceName, MasterProc]
    },
    ha_test_utils:set_envs(Workers, ServiceName, MasterProc),
    ?assertEqual(ok, rpc:call(CallWorker, internal_services_manager, start_service,
        [ha_test_utils, <<"test_service">>, NodeSelector, ServiceOptions])),
    ha_test_utils:assert_service_started(ServiceName, Node1, StartTimestamp),
    ha_test_utils:assert_healthcheck_done(ServiceName, Node1, StartTimestamp),

    ?assertEqual(ok, rpc:call(Node1, ha_test_utils, stop_service, [ServiceName, MasterProc])),
    StopTimestamp = global_clock:timestamp_millis(),
    ?assertEqual(ok, rpc:call(Node2, internal_services_manager, takeover, [Node1])),
    ha_test_utils:assert_service_started(ServiceName, Node2, StopTimestamp),
    ha_test_utils:assert_healthcheck_done(ServiceName, Node2, StopTimestamp),
    timer:sleep(5000), % Wait for first healthcheck on master node to end healthcheck cycle

    MigrationTimestamp = global_clock:timestamp_millis(),
    ?assertEqual(ok, rpc:call(Node2, internal_services_manager, migrate_to_recovered_master, [Node1])),
    MigrationFinishTimestamp = global_clock:timestamp_millis(),
    ha_test_utils:assert_service_started(ServiceName, Node1, MigrationTimestamp),
    ha_test_utils:assert_healthcheck_done(ServiceName, Node1, MigrationTimestamp),
    ?assertEqual(ok, rpc:call(Node1, ha_test_utils, stop_service, [ServiceName, MasterProc])),
    timer:sleep(timer:seconds(5)), % Wait for unwanted messages to verify migration
    ha_test_utils:flush_and_check_messages(ServiceName, Node2, MigrationFinishTimestamp).

service_healthcheck_rescheduling_test(Config) ->
    [Worker0 | _] = Workers = ?config(cluster_worker_nodes, Config),

    NodeSelector = <<"service_healthcheck_rescheduling_test">>,
    ServiceName = test_service,
    MasterProc = self(),
    #node_routing_info{assigned_nodes = [Node1 | _] = AssignedNodes} =
        rpc:call(Worker0, consistent_hashing, get_routing_info, [NodeSelector]),
    [CallWorker | _] = Workers -- AssignedNodes,

    StartTimestamp = global_clock:timestamp_millis(),
    ServiceOptions = #{
        start_function => start_service,
        stop_function => stop_service,
        healthcheck_fun => healthcheck_fun,
        healthcheck_interval => 3600000,  % healthcheck should be called in an hour
        start_function_args => [ServiceName, MasterProc]
    },
    ha_test_utils:set_envs(Workers, ServiceName, MasterProc),
    ?assertEqual(ok, rpc:call(CallWorker, internal_services_manager, start_service, [
        ha_test_utils, <<"test_service_reschedule">>, NodeSelector, ServiceOptions
    ])),
    ha_test_utils:assert_service_started(ServiceName, Node1, StartTimestamp),
    % the healthcheck should not be done yet
    ha_test_utils:assert_healthcheck_not_done(ServiceName, Node1),

    % after rescheduling, the healthcheck should be done in a second
    ?assertEqual(ok, rpc:call(CallWorker, internal_services_manager, reschedule_healthcheck, [
        ha_test_utils, <<"test_service_reschedule">>, NodeSelector, 1000
    ])),
    ha_test_utils:assert_healthcheck_done(ServiceName, Node1, StartTimestamp).


%%%===================================================================
%%% HA tests
%%%===================================================================

saves_should_propagate_to_backup_node_cast_ha_test(Config) ->
    datastore_model_ha_test_common:saves_should_propagate_to_backup_node(Config, cast).

saves_should_propagate_to_backup_node_call_ha_test(Config) ->
    datastore_model_ha_test_common:saves_should_propagate_to_backup_node(Config, call).

saves_should_not_propagate_to_backup_node_when_ha_is_disabled(Config) ->
    datastore_model_ha_test_common:saves_should_not_propagate_to_backup_node(Config, false, ?TEST_MODELS).

saves_should_not_propagate_to_backup_node_for_local_memory_model(Config) ->
    datastore_model_ha_test_common:saves_should_not_propagate_to_backup_node(
        Config, true, [ets_only_model, mnesia_only_model]).

saves_should_propagate_to_backup_node_after_config_change_global_models(Config) ->
    datastore_model_ha_test_common:saves_should_propagate_to_backup_node_after_config_change(Config, false).

saves_should_propagate_to_backup_node_after_config_change_local_models(Config) ->
    datastore_model_ha_test_common:saves_should_propagate_to_backup_node_after_config_change(Config, true).

calls_should_change_node_cast_ha_test(Config) ->
    datastore_model_ha_test_common:calls_should_change_node(Config, cast).

calls_should_change_node_call_ha_test(Config) ->
    datastore_model_ha_test_common:calls_should_change_node(Config, call).

saves_should_use_recovered_node_cast_ha_test(Config) ->
    datastore_model_ha_test_common:saves_should_use_recovered_node(Config, cast).

saves_should_use_recovered_node_call_ha_test(Config) ->
    datastore_model_ha_test_common:saves_should_use_recovered_node(Config, call).

saves_should_change_node_dynamic_cast_ha_test(Config) ->
    datastore_model_ha_test_common:saves_should_change_node_dynamic(Config, cast).

saves_should_change_node_dynamic_call_ha_test(Config) ->
    datastore_model_ha_test_common:saves_should_change_node_dynamic(Config, call).


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
    datastore_test_utils:init_suite(?TEST_MODELS, Config, fun(Config2) -> Config2 end,
        [datastore_test_utils, datastore_model_ha_test_common, datastore_performance_tests_base, ha_test_utils]
    ).


init_per_testcase(saves_should_not_propagate_to_backup_node_when_ha_is_disabled, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Workers, cluster_worker, test_ctx_base, #{ha_enabled => false}),
    datastore_model_ha_test_common:init_per_testcase_base(Config);
init_per_testcase(saves_should_not_propagate_to_backup_node_for_local_memory_model, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Workers, cluster_worker, test_ctx_base, #{routing => local}),
    datastore_model_ha_test_common:init_per_testcase_base(Config);
init_per_testcase(saves_should_propagate_to_backup_node_after_config_change_local_models, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Workers, cluster_worker, test_ctx_base, #{routing => local, ha_enabled => true}),
    datastore_model_ha_test_common:init_per_testcase_base(Config);
init_per_testcase(saves_should_propagate_to_backup_node_after_config_change_global_models, Config) ->
    datastore_model_ha_test_common:init_per_testcase_base(Config);
init_per_testcase(Case, Config) ->
    case lists:suffix("ha_test", atom_to_list(Case)) of
        true ->
            datastore_model_ha_test_common:init_per_testcase_base(Config);
        _ ->
            [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
            application:load(cluster_worker),
            {ok, SubtreesNum} = test_utils:get_env(Worker, cluster_worker, tp_subtrees_number),
            application:set_env(cluster_worker, tp_subtrees_number, SubtreesNum),
            test_utils:set_env(Workers, cluster_worker, test_ctx_base, #{memory_copies => all}),
            Config
    end.


end_per_testcase(Case, Config) when Case =:= saves_should_not_propagate_to_backup_node_when_ha_is_disabled orelse
    Case =:= saves_should_not_propagate_to_backup_node_for_local_memory_model orelse
    Case =:= saves_should_propagate_to_backup_node_after_config_change_local_models ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Workers, cluster_worker, test_ctx_base, #{}),
    datastore_model_ha_test_common:end_per_testcase_base(Config);
end_per_testcase(saves_should_propagate_to_backup_node_after_config_change_global_models, Config) ->
    datastore_model_ha_test_common:end_per_testcase_base(Config);
end_per_testcase(Case, Config) ->
    case lists:suffix("ha_test", atom_to_list(Case)) of
        true ->
            datastore_model_ha_test_common:end_per_testcase_base(Config);
        _ ->
            Workers = ?config(cluster_worker_nodes, Config),
            test_utils:set_env(Workers, cluster_worker, test_ctx_base, #{}),
            ok
    end.

end_per_suite(_Config) ->
    ok.