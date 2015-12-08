%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Enhances http_worker with op-worker specifics.
%%%      Provides (partial) definition of endpoints healthcheck.
%%% @end
%%%--------------------------------------------------------------------
-module(http_worker_plugin_default).
-author("Michal Zmuda").

-behaviour(http_worker_plugin_behaviour).

-include_lib("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% http_worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0, healthcheck_endpoints/0]).

-define(HTTP_WORKER_MODULE, http_worker).

%%%===================================================================
%%% http_worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link http_worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State :: worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link http_worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck | {spawn_handler, SocketPid :: pid()},
    Result :: nagios_handler:healthcheck_response() | ok | {ok, Response} |
    {error, Reason} | pong,
    Response :: term(),
    Reason :: term().

handle(_Request) ->
    ?log_bad_request(_Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link http_worker_plugin_behaviour} callback healthcheck_endpoints/0.
%% @end
%%--------------------------------------------------------------------

-spec healthcheck_endpoints() -> list({Module :: atom(), Endpoint :: atom()}).
healthcheck_endpoints() ->
    [
        {?HTTP_WORKER_MODULE, nagios}, {?HTTP_WORKER_MODULE, redirector}
    ].

%%--------------------------------------------------------------------
%% @doc
%% {@link http_worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    ok.
