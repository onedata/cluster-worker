%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module is responsible for redirector listener starting and stopping.
%%% @end
%%%--------------------------------------------------------------------
-module(redirector_listener).
-author("Tomasz Lichon").
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Session logic module
-define(SESSION_LOGIC_MODULE, session_logic).

% Cowboy listener references
-define(HTTP_REDIRECTOR_LISTENER, http).

-behaviour(listener_behaviour).

%% listener_behaviour callbacks
-export([port/0, start/0, stop/0, healthcheck/0]).

%%%===================================================================
%%% listener_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback port/0.
%% @end
%%--------------------------------------------------------------------
-spec port() -> integer().
port() ->
    {ok, RedirectPort} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        http_redirect_port),
    RedirectPort.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback start/0.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    {ok, RedirectPort} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        http_redirect_port),
    {ok, RedirectNbAcceptors} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        http_number_of_http_acceptors),
    {ok, Timeout} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        http_socket_timeout_seconds),
    RedirectDispatch = [
        {'_', [
            {'_', redirector_handler, []}
        ]}
    ],
    Result = cowboy:start_http(?HTTP_REDIRECTOR_LISTENER, RedirectNbAcceptors,
        [
            {port, RedirectPort}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(RedirectDispatch)}]},
            {max_keepalive, 1},
            {timeout, timer:seconds(Timeout)}
        ]),
    case Result of
        {ok, _} -> ok;
        _ -> Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback stop/0.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    case catch cowboy:stop_listener(?HTTP_REDIRECTOR_LISTENER) of
        (ok) ->
            ok;
        (Error) ->
            ?error("Error on stopping listener ~p: ~p",
                [?HTTP_REDIRECTOR_LISTENER, Error]),
            {error, redirector_stop_error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback healthcheck/0.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    Endpoint = <<"http://127.0.0.1:", (integer_to_binary(port()))/binary>>,
    case http_client:get(Endpoint) of
        {ok, _, _, _} -> ok;
        _ -> {error, server_not_responding}
    end.
