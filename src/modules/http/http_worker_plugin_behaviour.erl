%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Behaviour of plugin enhancing http_worker.
%%% @end
%%%-------------------------------------------------------------------
-module(http_worker_plugin_behaviour).
-author("Michal Zmuda").

%%--------------------------------------------------------------------
%% @doc
%% Returns list of endpoints to be examined during healthcheck.
%% Modules given should implement {@link endpoint_healthcheck_behaviour}.
%% @end
%%--------------------------------------------------------------------
-callback healthcheck_endpoints() -> list({Module :: atom(), Endpoint :: atom()}).

%%--------------------------------------------------------------------
%% @doc
%% Executed on init of http_worker.
%% @end
%%--------------------------------------------------------------------
-callback init(Args :: term()) ->
  {ok, State :: worker_host:plugin_state()} | {error, Reason :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Executed if nothing matched in http_worker.
%% @end
%%--------------------------------------------------------------------
-callback handle(Request :: term()) ->
  {ok, Answer :: term()} |
  {error, Reason :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Executed on cleanup of http_worker.
%% @end
%%--------------------------------------------------------------------
-callback cleanup() -> ok | {error, Reason :: term()}.