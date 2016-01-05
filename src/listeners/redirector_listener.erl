%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc redirector listener starting & stopping
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
-export([start/0, stop/0, healthcheck/0]).

%%%===================================================================
%%% listener_starter_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link listener_starter_behaviour} callback start/1.
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
%% {@link listener_starter_behaviour} callback stop/1.
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
%% Returns the status of a listener.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    {ok, RedirectorPort} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        http_redirect_port),
    case http_client:get("http://127.0.0.1:" ++ integer_to_list(RedirectorPort),
        [], <<>>, [insecure]) of
        {ok, _, _, _} ->
            ok;
        _ ->
            {error, server_not_responding}
    end.
