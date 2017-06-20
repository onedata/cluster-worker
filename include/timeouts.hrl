%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc dns listener starting & stopping
%%% @end
%%%--------------------------------------------------------------------
-author("Jakub Kudzia").

-include("global_definitions.hrl").

% Timeout to wait for DNS listeners to start.
% After it, they are assumed to have failed to start.
-define(LISTENERS_START_TIMEOUT, timer:minutes(1)).

%%task_manager
-define(TASK_SAVE_TIMEOUT, timer:seconds(30)).

%%worker_proxy
-define(DEFAULT_REQUEST_TIMEOUT, timer:minutes(20)).

%%couchdb_datastore_driver
-define(WAIT_FOR_STATE_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME, datastore_wait_for_state_timeout, timer:seconds(30))).
-define(WAIT_FOR_CONNECTION_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME, datastore_wait_for_connection_timeout, timer:minutes(1))).
-define(DATASTORE_DRIVER_INIT_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME, datastore_driver_init_timeout, timer:minutes(3))).
-define(DATASTORE_GATEWAY_SPAWN_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME, datastore_gateway_spawn_timeout, timer:minutes(1))).
-define(TIME_FOR_RESTART, ?DATASTORE_GATEWAY_SPAWN_TIMEOUT).

%%timeout for request to couchdb in form of hackney option
-define(DEFAULT_DB_REQUEST_TIMEOUT_OPT, [{recv_timeout, timer:minutes(3)}]).

%%mnesia cache driver
-define(MNESIA_WAIT_TIMEOUT, timer:seconds(5)).
-define(MNESIA_WAIT_REPEATS, 10).

-define(DEFAULT_DNS_TCP_TIMEOUT, 60).

-define(DOCUMENT_AGGREGATE_SAVE_TIMEOUT, timer:minutes(5)).

-define(DOCUMENT_BATCH_UPGRADE_TIMEOUT, timer:minutes(1)).
