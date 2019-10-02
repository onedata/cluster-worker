%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of cluster upgrade procedure.
%%% @end
%%%--------------------------------------------------------------------
-module(cluster_upgrade_test_SUITE).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("elements/node_manager/node_manager.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([same_version_upgrade_test/1, upgrade_ok_test/1,
    upgrade_fail_test/1, too_old_version_upgrade_test/1]).

all() ->
    ?ALL([
        same_version_upgrade_test,
        upgrade_ok_test,
        upgrade_fail_test,
        too_old_version_upgrade_test
    ]).

-define(UPGRADE_ERROR, upgrade_error).

%%%===================================================================
%%% Test functions
%%%===================================================================

same_version_upgrade_test(Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    ?assertEqual({ok, ?INSTALLED_CLUSTER_GENERATION}, get_cluster_generation(Workers)),
    test_utils:mock_assert_num_calls_sum(Workers, node_manager_plugin_default, upgrade_cluster, 1, 0),
    ?assertEqual(ok, upgrade_cluster(Workers)),
    ?assertEqual({ok, ?INSTALLED_CLUSTER_GENERATION}, get_cluster_generation(Workers)).


upgrade_ok_test(Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    {ok, _} = set_cluster_generation(Workers, ?OLDEST_KNOWN_CLUSTER_GENERATION),
    ?assertEqual(ok, upgrade_cluster(Workers)),
    test_utils:mock_assert_num_calls_sum(Workers, node_manager_plugin_default, upgrade_cluster, 1,
        ?INSTALLED_CLUSTER_GENERATION - ?OLDEST_KNOWN_CLUSTER_GENERATION),
    ?assertEqual({ok, ?INSTALLED_CLUSTER_GENERATION}, get_cluster_generation(Workers)).


upgrade_fail_test(Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    {ok, _} = set_cluster_generation(Workers, ?OLDEST_KNOWN_CLUSTER_GENERATION),
    ?assertEqual({error, ?UPGRADE_ERROR}, upgrade_cluster(Workers)),
    test_utils:mock_assert_num_calls_sum(Workers, node_manager_plugin_default, upgrade_cluster, 1, 1),
    ?assertEqual({ok, ?OLDEST_KNOWN_CLUSTER_GENERATION}, get_cluster_generation(Workers)).


too_old_version_upgrade_test(Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    {ok, _} = set_cluster_generation(Workers, ?OLDEST_KNOWN_CLUSTER_GENERATION-1),
    ?assertEqual({error, too_old_cluster_generation}, upgrade_cluster(Workers)),
    test_utils:mock_assert_num_calls_sum(Workers, node_manager_plugin_default, upgrade_cluster, 1, 0),
    ?assertEqual({ok, ?OLDEST_KNOWN_CLUSTER_GENERATION-1}, get_cluster_generation(Workers)).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(upgrade_fail_test, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_new(Workers, node_manager_plugin_default, [passthrough]),
    test_utils:mock_expect(Workers, node_manager_plugin_default, upgrade_cluster,
        fun(_) -> throw({error, ?UPGRADE_ERROR}) end),
    Config;
init_per_testcase(_Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_new(Workers, node_manager_plugin_default, [passthrough]),
    Config.

end_per_testcase(_Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_unload(Workers, node_manager_plugin_default),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_cluster_generation(Workers) ->
    rpc:call(get_random_node(Workers), cluster_generation, get, []).

set_cluster_generation(Workers, Generation) ->
    rpc:call(get_random_node(Workers), cluster_generation, save, [Generation]).

upgrade_cluster(Workers) ->
    rpc:call(get_random_node(Workers), node_manager, upgrade_cluster, []).

get_random_node(Workers) ->
    lists:nth(rand:uniform(length(Workers)), Workers).
