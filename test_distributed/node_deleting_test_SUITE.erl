%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains node adding/deleting tests.
%%% @end
%%%-------------------------------------------------------------------
-module(node_deleting_test_SUITE).
-author("Michal Wrzeszcz").


-include("datastore_test_utils.hrl").


%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).


%% tests
-export([
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
    node_deletion_without_sleep_multikey_call_ha_test/1
]).


all() -> [
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
    node_deletion_without_sleep_multikey_call_ha_test
].


%%%===================================================================
%%% Test functions
%%%===================================================================

node_deletion_cast_ha_test(Config) ->
    datastore_model_ha_test_common:node_deletion_test(Config, cast, false, true).

node_deletion_call_ha_test(Config) ->
    datastore_model_ha_test_common:node_deletion_test(Config, call, false, true).

node_deletion_with_spawn_cast_ha_test(Config) ->
    datastore_model_ha_test_common:node_deletion_test(Config, cast, true, true).

node_deletion_with_spawn_call_ha_test(Config) ->
    datastore_model_ha_test_common:node_deletion_test(Config, call, true, true).

node_deletion_without_sleep_cast_ha_test(Config) ->
    datastore_model_ha_test_common:node_deletion_test(Config, cast, false, false).

node_deletion_without_sleep_call_ha_test(Config) ->
    datastore_model_ha_test_common:node_deletion_test(Config, call, false, false).



node_deletion_multikey_cast_ha_test(Config) ->
    datastore_model_ha_test_common:node_deletion_multikey_test(Config, cast, false, true).

node_deletion_multikey_call_ha_test(Config) ->
    datastore_model_ha_test_common:node_deletion_multikey_test(Config, call, false, true).

node_deletion_with_spawn_multikey_cast_ha_test(Config) ->
    datastore_model_ha_test_common:node_deletion_multikey_test(Config, cast, true, true).

node_deletion_with_spawn_multikey_call_ha_test(Config) ->
    datastore_model_ha_test_common:node_deletion_multikey_test(Config, call, true, true).

node_deletion_without_sleep_multikey_cast_ha_test(Config) ->
    datastore_model_ha_test_common:node_deletion_multikey_test(Config, cast, false, false).

node_deletion_without_sleep_multikey_call_ha_test(Config) ->
    datastore_model_ha_test_common:node_deletion_multikey_test(Config, call, false, false).


%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite(?TEST_MODELS, Config, fun(Config2) -> Config2 end,
        [datastore_test_utils, datastore_model_ha_test_common, ha_test_utils]
    ).


init_per_testcase(_Case, Config) ->
    datastore_model_ha_test_common:init_per_testcase_base(Config).


end_per_testcase(_Case, Config) ->
    datastore_model_ha_test_common:end_per_testcase_base(Config).


end_per_suite(_Config) ->
    ok.