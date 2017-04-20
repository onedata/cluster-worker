%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains CouchBase worker pool tests.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_pool_test_SUITE).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0]).

%% tests
-export([
    request_queue_should_be_empty/1,
    worker_queue_should_be_full/1,
    worker_should_be_restarted_after_crash/1,
    worker_should_handle_malformed_request/1,
    worker_should_connect_to_first_active_database_node/1,
    request_should_timeout_on_database_connection_crash/1
]).

all() ->
    ?ALL([
        request_queue_should_be_empty,
        worker_queue_should_be_full,
        worker_should_be_restarted_after_crash,
        worker_should_handle_malformed_request,
        worker_should_connect_to_first_active_database_node,
        request_should_timeout_on_database_connection_crash
    ]).

-define(BUCKET, <<"default">>).

%%%===================================================================
%%% Test functions
%%%===================================================================

request_queue_should_be_empty(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(0, rpc:call(Worker, couchbase_pool, get_queue_size,
        [request]
    )),
    ?assertEqual(0, rpc:call(Worker, couchbase_pool, get_queue_size,
        [request, ?BUCKET]
    )),
    ?assertEqual(0, rpc:call(Worker, couchbase_pool, get_queue_size,
        [request, ?BUCKET, read]
    )),
    ?assertEqual(0, rpc:call(Worker, couchbase_pool, get_queue_size,
        [request, ?BUCKET, write]
    )).

worker_queue_should_be_full(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    BucketsNum = length(rpc:call(Worker, couchbase_driver, get_buckets, [])),
    ModesNum = length(rpc:call(Worker, couchbase_pool, get_modes, [])),
    {ok, PoolSize} = test_utils:get_env(Worker, cluster_worker,
        couchbase_pool_size
    ),
    ?assertEqual(BucketsNum * ModesNum * PoolSize,
        rpc:call(Worker, couchbase_pool, get_queue_size, [worker])
    ),
    ?assertEqual(ModesNum * PoolSize,
        rpc:call(Worker, couchbase_pool, get_queue_size, [worker, ?BUCKET])
    ),
    ?assertEqual(PoolSize, rpc:call(Worker, couchbase_pool, get_queue_size,
        [worker, ?BUCKET, read]
    )),
    ?assertEqual(PoolSize, rpc:call(Worker, couchbase_pool, get_queue_size,
        [worker, ?BUCKET, write]
    )).

worker_should_be_restarted_after_crash(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Pid = rpc:call(Worker, couchbase_pool_sup, get_pool, [?BUCKET, write]),
    WQueue = element(3, element(5, element(4, sys:get_state(Pid)))),
    {{value, PWorker}, _} = queue:out(WQueue),
    exit(PWorker, kill),
    ?assertEqual(false, queue:member(PWorker,
        element(3, element(5, element(4, sys:get_state(Pid))))
    ), 10),
    ?assertEqual(queue:len(WQueue), rpc:call(Worker, couchbase_pool,
        get_queue_size, [worker, ?BUCKET, write]
    ), 10).

worker_should_handle_malformed_request(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, {_, _, _}}, rpc:call(Worker, couchbase_pool, post,
        [?BUCKET, write, {save_design_doc, "designName", {[]}}]
    )).

worker_should_connect_to_first_active_database_node(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    DbHosts = rpc:call(Worker, datastore_config2, get_db_hosts, []),
    DbHosts2 = [<<"127.0.0.1">> | DbHosts],
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_pool_worker, start_link,
        [self(), ?BUCKET, DbHosts2]
    )).

request_should_timeout_on_database_connection_crash(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Worker, cluster_worker, couchbase_pool_request_timeout,
        timer:seconds(1)),
    ?assertEqual({error, timeout}, rpc:call(Worker, couchbase_pool, post,
        [?BUCKET, write, {save, key, value}]
    )).