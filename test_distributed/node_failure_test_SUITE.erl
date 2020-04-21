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

-define(POOL, <<"traverse_test_pool">>).
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
        [ha_test_utils, <<"test_service">>, start_service, stop_service, [ServiceName, MasterProc], Seed])),
    TraverseID = start_traverse(CallWorker, Node1),

    ha_test_utils:check_service(ServiceName, Node1, StartTimestamp),
    RecAns = receive
        {stop, Node1} -> ok
    after
        5000 -> timeout
    end,
    ?assertEqual(ok, RecAns),
    ?assertEqual(ok, traverse_test_pool:copy_jobs_store(Node1, Node2)),

    {ok, Doc2} = ?assertMatch({ok, #document{}}, rpc:call(CallWorker, Model, save, [?DOC(Key, Model)])),

    ?assertEqual({badrpc, nodedown}, rpc:call(Node1, erlang, halt, [])),
    StopTimestamp = os:timestamp(),

    ?assertEqual({ok, Doc2}, rpc:call(CallWorker, Model, get, [Key])),
    ?assertMatch({ok, _, #document{}},
        rpc:call(CallWorker, couchbase_driver, get, [?DISC_CTX, ?UNIQUE_KEY(Model, Key)]), ?ATTEMPTS),
    ?assertMatch({ok, #document{value = #traverse_task{status = finished}}},
        rpc:call(CallWorker, traverse_task, get, [?POOL, TraverseID]), ?ATTEMPTS),

    ha_test_utils:check_service(ServiceName, Node2, StopTimestamp),

    ok.

start_traverse(CallWorker, ExpectedNode) ->
    start_traverse(CallWorker, ExpectedNode, 1).

start_traverse(CallWorker, ExpectedNode, Num) ->
    ID = <<"test_traverse", (integer_to_binary(Num))/binary>>,
    ?assertEqual(ok, rpc:call(CallWorker, traverse, run, [?POOL, ID, {self(), 1, 100}])),
    case rpc:call(CallWorker, traverse_task, get, [?POOL, ID]) of
        {ok, #document{value = #traverse_task{node = ExpectedNode}}} -> ID;
        _ -> start_traverse(CallWorker, ExpectedNode, Num + 1)
    end.


%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite(?TEST_MODELS, Config,
        fun(Config2) -> Config2 end, [datastore_test_utils, ha_test_utils, traverse_test_pool]).

init_per_testcase(_, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Workers, ?CLUSTER_WORKER_APP_NAME, test_job, []),
    test_utils:set_env(Workers, ?CLUSTER_WORKER_APP_NAME, ongoing_job, []),
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, traverse, init_pool, [?POOL, 2, 2, 10]))
    end, Workers),
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
    [Worker1 | _] = Workers = ?config(cluster_worker_nodes, Config),
    CMNodes = ?config(cluster_manager_nodes, Config),
    lists:foreach(fun(Worker) ->
        set_ha(Worker, Fun, Args)
    end, Workers),
    Ring = rpc:call(Worker1, ctool, get_env, [?CURRENT_RING]),
    consistent_hashing:replicate_ring_to_nodes(CMNodes, ?CURRENT_RING, Ring).