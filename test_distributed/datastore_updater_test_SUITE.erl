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
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
%%tests
-export([
    online_upgrade_test/1,
    offline_upgrade_init_test/1,
    model_rename_test/1
]).

all() ->
    ?ALL([
        % TODO - update from old to new datastore
%%        online_upgrade_test,
%%        model_rename_test,
%%        offline_upgrade_init_test
    ]).

-define(rpc(M, F, A), rpc:call(W1, M, F, A)).
-define(id(I), list_to_binary(pid_to_list(self()) ++ "_" ++ atom_to_list(I))).

%%%===================================================================
%%% Test functions
%%%===================================================================


online_upgrade_test(Config) ->
    [W1 | _] = Workers = ?config(cluster_worker_nodes, Config),
    ok = set_model_version(Config, test_record_1, 2),
    ok = set_model_version(Config, test_record_2, 1),

    ?assertMatch({ok, _}, ?rpc(test_record_1, save, [#document{key = ?id(v1), value = {test_record_1, 1, 2, 3}}])),
    ?assertMatch({ok, #document{version = 2}}, ?rpc(test_record_1, get, [?id(v1)])),

    ok = set_model_version(Config, test_record_1, 4),
    ?assertMatch({ok, #document{version = 4, value = {test_record_1, 1, 2}}},
        ?rpc(test_record_1, get, [?id(v1)])),

    ok = set_model_version(Config, test_record_1, 5),
    ?assertMatch({ok, #document{version = 5, value = {test_record_1, 1, 2, {default, 5, atom}}}},
        ?rpc(test_record_1, get, [?id(v1)])),

    ?assertMatch({ok, _}, ?rpc(test_record_1, save, [#document{key = ?id(v4), value = {test_record_1, 1, 2, {1, 2, 3}}}])),
    ?assertMatch({ok, #document{version = 5, value = {test_record_1, 1, 2, {1, 2, 3}}}},
        ?rpc(test_record_1, get, [?id(v4)])),

    ok = set_model_version(Config, test_record_1, 6),
    ?assertMatch({ok, #document{version = 6, value = {test_record_1, 1, 2, {'1', 2, '3'}, [true, false]}}},
        ?rpc(test_record_1, get, [?id(v4)])),
    ?assertMatch({ok, #document{version = 6, value = {test_record_1, 1, 2, {default, 5, atom}, [true, false]}}},
        ?rpc(test_record_1, get, [?id(v1)])),

    ok.


offline_upgrade_init_test(Config) ->
    [W1 | _] = Workers = ?config(cluster_worker_nodes, Config),
    ok = set_model_version(Config, test_record_1, 2),
    ok = set_model_version(Config, test_record_2, 1),

    TR1Num = 1374,
    TR1IDs = [?id(list_to_atom(integer_to_list(N))) || N <- lists:seq(1, TR1Num)],
    TR2Num = 231,
    TR2IDs = [?id(list_to_atom(integer_to_list(TR1Num + N))) || N <- lists:seq(1, TR2Num)],

    application:set_env(?CLUSTER_WORKER_APP_NAME, ?PERSISTENCE_DRIVER, couchdb_datastore_driver),
    {ok, DBNodes} = ?rpc(plugins, apply, [node_manager_plugin, db_nodes, []]),
    application:set_env(?CLUSTER_WORKER_APP_NAME, db_nodes, DBNodes),
    datastore:initialize_minimal_env(),


    utils:pmap(
        fun(ID) ->
            ?assertMatch({ok, _}, ?rpc(test_record_1, save, [#document{key = ID, value = {test_record_1, 1, 2, 3}}])),
            ?assertMatch({ok, #document{version = 2}}, ?rpc(test_record_1, get, [ID]))
        end, TR1IDs),

    ?assertMatch({ok, _}, ?rpc(test_record_1, save, [#document{key = lists:nth(1, TR1IDs), value = {test_record_1, 2, 2, 3}}])),
    ?assertMatch({ok, #document{version = 2, value = {_, 2,2,3}}}, ?rpc(test_record_1, get, [lists:nth(1, TR1IDs)])),
    ok = set_model_version(Config, test_record_1, 3),

    lists:foreach(
        fun(ID) ->
            ?assertMatch({ok, _}, ?rpc(test_record_2, save, [#document{key = ID, value = {test_record_2, 10, '20', <<"30">>}}])),
            ?assertMatch({ok, #document{version = 1}}, ?rpc(test_record_2, get, [ID]))
        end, TR2IDs),
    ok = set_model_version(Config, test_record_2, 2),

    Upgrade1 = datastore_versions:shell_upgrade(),
    Upgrade2 = datastore_versions:shell_upgrade(),

    ?assertMatch({ok, {TR1Num, 0}}, proplists:get_value(test_record_1, Upgrade1)),
    ?assertMatch({ok, {TR2Num, 0}}, proplists:get_value(test_record_2, Upgrade1)),

    ?assertMatch({ok, {0, 0}}, proplists:get_value(test_record_1, Upgrade2)),
    ?assertMatch({ok, {0, 0}}, proplists:get_value(test_record_2, Upgrade2)),

    lists:foreach(
        fun(ID) ->
            ?assertMatch({ok, #document{version = 3}}, ?rpc(test_record_1, get, [ID]))
        end, TR1IDs),
    lists:foreach(
        fun(ID) ->
            ?assertMatch({ok, #document{version = 2}}, ?rpc(test_record_2, get, [ID]))
        end, TR2IDs),

    ok.


model_rename_test(Config) ->
    [W1 | _] = Workers = ?config(cluster_worker_nodes, Config),
    ok = set_model_version(Config, test_record_1, 6),
    ok = set_model_version(Config, test_record_2, 1),

    ?assertMatch({ok, _}, ?rpc(test_record_2, save, [#document{key = to_rename, value = {test_record_2, 10, '20', {atom1, 9, atom2}}}])),
    ?assertMatch({ok, #document{version = 1}}, ?rpc(test_record_2, get, [to_rename])),

    test_utils:mock_new(Workers, node_manager_plugin_default),
    test_utils:mock_expect(Workers, node_manager_plugin_default, renamed_models, fun() ->
        #{test_record_2 => {1, test_record_1}}
    end),

    ?assertMatch({ok, #document{version = 6, value = {test_record_1, 10, '20', {default, 5, atom}, [true, false]}}}, ?rpc(test_record_1, get, [to_rename])),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:enable_datastore_models(Workers, [test_record_1, test_record_2]),
    test_utils:mock_unload([node()], [plugins]),
        catch test_utils:mock_new([node()], [plugins]),
    ok = test_utils:mock_expect([node()], plugins, apply,
        fun
            (datastore_config_plugin, models, []) ->
                meck:passthrough([datastore_config_plugin, models, []]) ++ [test_record_1, test_record_2];
            (A1, A2, A3) ->
                meck:passthrough([A1, A2, A3])
        end),
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