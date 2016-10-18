%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests datastore main API.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_updater_test_SUITE).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include("datastore_test_models_def.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
%%tests
-export([
    online_upgrade_test/1,
    offline_upgrade_init_test/1
]).

all() ->
    ?ALL([
        online_upgrade_test,
        offline_upgrade_init_test
    ]).

-define(rpc(M, F, A), rpc:call(W1, M, F, A)).
-define(id(I), list_to_binary(pid_to_list(self()) ++ "_" ++ atom_to_list(I))).

%%%===================================================================
%%% Test functions
%%%===================================================================


online_upgrade_test(Config) ->
    [W1, W2] = Workers = ?config(cluster_worker_nodes, Config),
    ok = set_model_version(Config, test_record_1, 1),
    ok = set_model_version(Config, test_record_2, 1),

    ?assertMatch({ok, _}, ?rpc(test_record_1, save, [#document{key = ?id(v1), value = {test_record_1, 1, 2, 3}}])),
    ?assertMatch({ok, #document{version = 1}}, ?rpc(test_record_1, get, [?id(v1)])),

    ok = set_model_version(Config, test_record_1, 3),
    ?assertMatch({ok, #document{version = 3, value = {test_record_1, 1, 2}}},
        ?rpc(test_record_1, get, [?id(v1)])),

    ok = set_model_version(Config, test_record_1, 4),
    ?assertMatch({ok, #document{version = 4, value = {test_record_1, 1, 2, {default, 5, atom}}}},
        ?rpc(test_record_1, get, [?id(v1)])),

    ?assertMatch({ok, _}, ?rpc(test_record_1, save, [#document{key = ?id(v4), value = {test_record_1, 1, 2, {1, 2, 3}}}])),
    ?assertMatch({ok, #document{version = 4, value = {test_record_1, 1, 2, {1, 2, 3}}}},
        ?rpc(test_record_1, get, [?id(v4)])),

    ok = set_model_version(Config, test_record_1, 5),
    ?assertMatch({ok, #document{version = 5, value = {test_record_1, 1, 2, {'1', 2, '3'}, [true, false]}}},
        ?rpc(test_record_1, get, [?id(v4)])),
    ?assertMatch({ok, #document{version = 5, value = {test_record_1, 1, 2, {default, 5, atom}, [true, false]}}},
        ?rpc(test_record_1, get, [?id(v1)])),

    ok.


offline_upgrade_init_test(Config) ->

    [W1, W2] = Workers = ?config(cluster_worker_nodes, Config),
    ok = set_model_version(Config, test_record_1, 1),
    ok = set_model_version(Config, test_record_2, 1),

    hackney:start(),
    couchbeam_sup:start_link(),
    application:set_env(?CLUSTER_WORKER_APP_NAME, ?PERSISTENCE_DRIVER, couchdb_datastore_driver),
    datastore:ensure_state_loaded([node()]),
    ets:new(datastore_worker, [named_table, public, set, {read_concurrency, true}]),
    ct:print("1 ~p", [?rpc(worker_host, state_get, [datastore_worker, db_nodes])]),
    datastore_worker:init(?rpc(worker_host, state_get, [datastore_worker, db_nodes])),
    ct:print("2"),


    ?assertMatch({ok, _}, ?rpc(test_record_1, save, [#document{key = ?id(v11), value = {test_record_1, 1, 2, 3}}])),
    ?assertMatch({ok, #document{version = 1}}, ?rpc(test_record_1, get, [?id(v11)])),
    ok = set_model_version(Config, test_record_1, 2),

    ?assertMatch({ok, _}, ?rpc(test_record_2, save, [#document{key = ?id(v21), value = {test_record_2, 10, 20, 30}}])),
    ?assertMatch({ok, #document{version = 1}}, ?rpc(test_record_2, get, [?id(v21)])),
    ok = set_model_version(Config, test_record_2, 2),


    datastore_versions:stream_outdated_records(test_record_1),

    flush(timer:seconds(5)),

    datastore_versions:stream_outdated_records(test_record_2),

    flush(timer:seconds(5)),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    NewConfig = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [random]),
    NewConfig.

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:enable_datastore_models(Workers, [test_record_1, test_record_2]),
    datastore_basic_ops_utils:set_env(Case, Config).

end_per_testcase(_Case, Config) ->
    datastore_basic_ops_utils:clear_env(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================


set_model_version(Config, Model, Version) ->
    Workers = ?config(cluster_worker_nodes, Config),
    {_, []} = rpc:multicall(Workers ++ [node()], test_record_1, set_test_record_version, [Model, Version]),
    ?assertMatch(#model_config{version = Version}, Model:model_init()),
    ok.


flush(Timeout) ->
    receive
        Msg ->
            ct:print("FLUSH ~p", [Msg]),
            flush(Timeout)
    after Timeout ->
        ok
    end.