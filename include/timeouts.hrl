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

% Timeout to wait for DNS listeners to start.
% After it, they are assumed to have failed to start.
-define(LISTENERS_START_TIMEOUT, timer:minutes(1)).

%%task_manager
-define(TASK_SAVE_TIMEOUT, timer:seconds(30)).

%%worker_proxy
-define(DEFAULT_REQUEST_TIMEOUT, timer:minutes(5)).

%%couchdb_datastore_driver
-define(WAIT_FOR_STATE_TIMEOUT, timer:seconds(30)).
-define(WAIT_FOR_CONNECTION_TIMEOUT, timer:seconds(30)).
-define(TIME_FOR_RESTART, timer:seconds(10)).

%%timeout for request to couchdb in form of hackney option
-define(DEFAULT_DB_REQUEST_TIMEOUT_OPT, [{recv_timeout, timer:minutes(3)}]).

%%mnesia cache driver
-define(MNESIA_WAIT_TIMEOUT, timer:seconds(60)).

-define(DEFAULT_DNS_TCP_TIMEOUT, 60).
