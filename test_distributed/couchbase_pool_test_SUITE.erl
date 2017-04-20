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
    worker_should_be_restarted_after_crash/1,
    worker_should_handle_malformed_request/1,
    worker_should_connect_to_first_active_database_node/1,
    request_should_timeout_on_database_connection_crash/1
]).

all() ->
    ?ALL([
        request_queue_should_be_empty,
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
    ?assertEqual(0, rpc:call(Worker, couchbase_pool, get_request_queue_size,
        [?BUCKET]
    )),
    ?assertEqual(0, rpc:call(Worker, couchbase_pool, get_request_queue_size,
        [?BUCKET, read]
    )),
    ?assertEqual(0, rpc:call(Worker, couchbase_pool, get_request_queue_size,
        [?BUCKET, write]
    )).

worker_should_be_restarted_after_crash(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Pid = rpc:call(Worker, couchbase_pool_sup, get_worker, [?BUCKET, write, 1]),
    exit(Pid, kill),
    ?assertEqual(true, Pid =/= rpc:call(Worker, couchbase_pool_sup, get_worker,
        [?BUCKET, write, 1]
    ), 10).

worker_should_handle_malformed_request(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, {_, _}}, rpc:call(Worker, couchbase_pool, post,
        [?BUCKET, write, {save_design_doc, "designName", {[]}}]
    )).

worker_should_connect_to_first_active_database_node(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    DbHosts = rpc:call(Worker, datastore_config2, get_db_hosts, []),
    DbHosts2 = [<<"127.0.0.1">> | DbHosts],
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_pool_worker, start_link,
        [?BUCKET, read, 1, DbHosts2]
    )).

request_should_timeout_on_database_connection_crash(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Worker, cluster_worker, couchbase_request_timeout,
        timer:seconds(1)),
    ?assertEqual({error, timeout}, rpc:call(Worker, couchbase_pool, post,
        [?BUCKET, write, {save, [{#{}, key, value}]}]
    )).