%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore ha tests.
%%% @end
%%%-------------------------------------------------------------------
-module(node_transition_test_SUITE).
-author("Michal Wrzeszcz").


-include("datastore_test_utils.hrl").


%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).


%% tests
-export([
    node_transition_cast_ha_test/1,
    node_transition_call_ha_test/1,
    node_transition_with_sleep_cast_ha_test/1,
    node_transition_with_sleep_call_ha_test/1,
    node_transition_delayed_ring_repair_cast_ha_test/1,
    node_transition_delayed_ring_repair_call_ha_test/1,
    node_transition_sleep_and_delayed_ring_repair_cast_ha_test/1,
    node_transition_sleep_and_delayed_ring_repair_call_ha_test/1
]).


all() -> [
    node_transition_cast_ha_test,
    node_transition_call_ha_test,
    node_transition_with_sleep_cast_ha_test,
    node_transition_with_sleep_call_ha_test,
    node_transition_delayed_ring_repair_cast_ha_test,
    node_transition_delayed_ring_repair_call_ha_test,
    node_transition_sleep_and_delayed_ring_repair_cast_ha_test,
    node_transition_sleep_and_delayed_ring_repair_call_ha_test
].


%%%===================================================================
%%% Test functions
%%%===================================================================

node_transition_cast_ha_test(Config) ->
    datastore_model_ha_test_common:node_transition_test(Config, cast, false, false).

node_transition_call_ha_test(Config) ->
    datastore_model_ha_test_common:node_transition_test(Config, call, false, false).

node_transition_with_sleep_cast_ha_test(Config) ->
    datastore_model_ha_test_common:node_transition_test(Config, cast, true, false).

node_transition_with_sleep_call_ha_test(Config) ->
    datastore_model_ha_test_common:node_transition_test(Config, call, true, false).

node_transition_delayed_ring_repair_cast_ha_test(Config) ->
    datastore_model_ha_test_common:node_transition_test(Config, cast, false, true).

node_transition_delayed_ring_repair_call_ha_test(Config) ->
    datastore_model_ha_test_common:node_transition_test(Config, call, false, true).

node_transition_sleep_and_delayed_ring_repair_cast_ha_test(Config) ->
    datastore_model_ha_test_common:node_transition_test(Config, cast, true, true).

node_transition_sleep_and_delayed_ring_repair_call_ha_test(Config) ->
    datastore_model_ha_test_common:node_transition_test(Config, call, true, true).


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