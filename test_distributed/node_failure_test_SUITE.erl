%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains test of behaviour in case of node failure.
%%% NOTE: Currently it is impossible to fix node after failure during the tests so SUITE should contain single test.
%%% @end
%%%-------------------------------------------------------------------
-module(node_failure_test_SUITE).
-author("Michal Wrzeszcz").

-include("datastore_test_utils.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/hashing/consistent_hashing.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    failure_test/1
]).

all() ->
    ?ALL([
        failure_test
    ]).

-define(DOC(Key, Model), ?BASE_DOC(Key, ?MODEL_VALUE(Model))).
-define(ATTEMPTS, 30).

%%%===================================================================
%%% Test functions
%%%===================================================================

failure_test(Config) ->
    [Worker0 | _] = Workers = ?config(cluster_worker_nodes, Config),
    set_ha(Config, change_config, [2, call]),

    Model = ets_cached_model,
    Key = datastore_key:new(),
    Seed = rpc:call(Worker0, datastore_key, get_chash_seed, [Key]),
    ServiceName = test_service,
    MasterProc = self(),
    #node_routing_info{assigned_nodes = [Node1, Node2] = AssignedNodes} =
        rpc:call(Worker0, consistent_hashing, get_routing_info, [Seed]),
    [CallWorker | _] = Workers -- AssignedNodes,

    StartTimestamp = os:timestamp(),
    ?assertEqual(ok, rpc:call(CallWorker, internal_services_manager, start_service,
        [ha_test_utils, start_service, stop_service, [ServiceName, MasterProc], Seed])),
    ha_test_utils:check_service(ServiceName, Node1, StartTimestamp),

    {ok, Doc2} = ?assertMatch({ok, #document{}}, rpc:call(CallWorker, Model, save, [?DOC(Key, Model)])),

    ?assertEqual({badrpc, nodedown}, rpc:call(Node1, erlang, halt, [])),
    StopTimestamp = os:timestamp(),

    ?assertEqual({ok, Doc2}, rpc:call(CallWorker, Model, get, [Key])),
    ?assertMatch({ok, _, #document{}},
        rpc:call(CallWorker, couchbase_driver, get, [?DISC_CTX, ?UNIQUE_KEY(Model, Key)]), ?ATTEMPTS),

    ha_test_utils:check_service(ServiceName, Node2, StopTimestamp),

    ok.


%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite(?TEST_MODELS, Config,
        fun(Config2) -> Config2 end, [datastore_test_utils, ha_test_utils]).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

end_per_suite(_Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

set_ha(Worker, Fun, Args) when is_atom(Worker) ->
    ?assertEqual(ok, rpc:call(Worker, ha_datastore, Fun, Args));
set_ha(Config, Fun, Args) ->
    Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        set_ha(Worker, Fun, Args)
    end, Workers).