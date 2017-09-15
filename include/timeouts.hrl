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

%%timeout for request to couchdb in form of hackney option
-define(DEFAULT_DB_REQUEST_TIMEOUT_OPT, [{recv_timeout, timer:minutes(3)}]).

%%mnesia cache driver
-define(MNESIA_WAIT_TIMEOUT, timer:seconds(5)).
-define(MNESIA_WAIT_REPEATS, 10).

-define(DEFAULT_DNS_TCP_TIMEOUT, 60).

-define(DOCUMENT_AGGREGATE_SAVE_TIMEOUT, timer:minutes(5)).

-define(DOCUMENT_BATCH_UPGRADE_TIMEOUT, timer:minutes(1)).
