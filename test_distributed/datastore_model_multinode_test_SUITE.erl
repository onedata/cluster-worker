%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
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
-author("Michał Wrzeszcz").

-include("datastore_test_utils.hrl").
-include("global_definitions.hrl").
-include("datastore_performance_tests_base.hrl").

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
    saves_should_use_repaired_node_cast_ha_test/1,
    saves_should_use_repaired_node_call_ha_test/1,

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
        saves_should_use_repaired_node_cast_ha_test,
        saves_should_use_repaired_node_call_ha_test,

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

saves_should_use_repaired_node_cast_ha_test(Config) ->
    saves_should_use_repaired_node(Config, cast).

saves_should_use_repaired_node_call_ha_test(Config) ->
    saves_should_use_repaired_node(Config, call).

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

saves_should_use_repaired_node(Config, Method) ->
    {Key, KeyNode, KeyNode2, TestWorker} = prepare_ha_test(Config),
    set_ha(Config, change_config, [2, Method]),

    lists:foreach(fun(Model) ->
        mock_node_down(TestWorker, KeyNode2, KeyNode),
        {ok, Doc2} = ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [?DOC(Key, Model)])),

        assert_in_memory(KeyNode2, Model, Key),
        assert_on_disc(TestWorker, Model, Key),
        assert_not_in_memory(KeyNode, Model, Key),

        set_ha(KeyNode2, master_up, []),
        ?assertMatch({ok, #document{}}, rpc:call(TestWorker, Model, save, [Doc2])),
        assert_in_memory(KeyNode, Model, Key),
        ?assertEqual(ok, rpc:call(TestWorker, consistent_hashing, set_fixed_node, [KeyNode]))
    end, ?TEST_MODELS).

prepare_ha_test(Config) ->
    [Worker0 | _] = Workers = ?config(cluster_worker_nodes, Config),
    Key = datastore_key:new(),
    {[KeyNode], [KeyNode2], _, _} = rpc:call(Worker0, datastore_key, responsible_nodes, [Key, 2]),
    [TestWorker | _] = Workers -- [KeyNode, KeyNode2],

    {Key, KeyNode, KeyNode2, TestWorker}.

set_ha(Worker, Fun, Args) when is_atom(Worker) ->
    ?assertEqual(ok, rpc:call(Worker, ha_management, Fun, Args));
set_ha(Config, Fun, Args) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        set_ha(Worker, Fun, Args)
    end, Workers).

mock_node_down(CallNode, ExecuteNode, BrokenNode) ->
    set_ha(ExecuteNode, master_down, []),
    ?assertEqual(ok, rpc:call(CallNode, consistent_hashing, set_broken_node, [BrokenNode])).

mock_node_up(CallNode, ExecuteNode, BrokenNode) ->
    ?assertEqual(ok, rpc:call(CallNode, consistent_hashing, set_fixed_node, [BrokenNode])),
    set_ha(ExecuteNode, master_up, []).

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
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(FixedWorker) ->
        ?assertEqual(ok, rpc:call(Worker, consistent_hashing, set_fixed_node, [FixedWorker]))
    end, Workers),
    set_ha(Config, master_up, []),
    set_ha(Config, change_config, [1, cast]);
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
