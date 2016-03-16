%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% This file contains timeout definitions
%%% @end
%%% Created : 16. Mar 2016 6:16 PM
%%%-------------------------------------------------------------------
-author("Jakub Kudzia").

% Timeout to wait for DNS listeners to start.
% After it, they are assumed to have failed to start.
-define(LISTENERS_START_TIMEOUT, 20000).

%%release_configurator
% oneprovider specific config
-define(SYNC_NODES_TIMEOUT, timer:minutes(2)).

%%task_manager
-define(TASK_SAVE_TIMEOUT, timer:seconds(20)).

%%worker_proxy
-define(DEFAULT_REQUEST_TIMEOUT, timer:seconds(20)).

%%couchdb_datastore_driver
-define(WAIT_FOR_STATE_TIMEOUT, timer:seconds(4)).
-define(WAIT_FOR_CONNECTION_TIMEOUT, timer:seconds(4)).

%%mnesia cache driver
-define(MNESIA_WAIT_TIMEOUT, timer:seconds(40)).

%%cache controller
-define(DISK_OP_TIMEOUT, timer:minutes(2)).
