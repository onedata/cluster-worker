%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module is responsible for nagios listener starting and stopping.
%%% @end
%%%--------------------------------------------------------------------
-module(nagios_listener).
-author("Michal Zmuda").

-behaviour(listener_behaviour).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Cowboy listener references
-define(NAGIOS_LISTENER, nagios).


%% listener_behaviour callbacks
-export([port/0, start/0, stop/0, reload_web_certs/0, healthcheck/0]).

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
    {ok, Port} = application:get_env(?CLUSTER_WORKER_APP_NAME, http_nagios_port),
    Port.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback start/0.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    ?info("Starting '~tp' server...", [?NAGIOS_LISTENER]),

    {ok, NbAcceptors} = application:get_env(
        ?CLUSTER_WORKER_APP_NAME, http_number_of_acceptors
    ),
    {ok, MaxKeepAlive} = application:get_env(
        ?CLUSTER_WORKER_APP_NAME, http_max_keepalive
    ),
    {ok, Timeout} = application:get_env(
        ?CLUSTER_WORKER_APP_NAME, http_request_timeout
    ),

    Dispatch = cowboy_router:compile([
        {'_', [
            {"/nagios/[...]", nagios_handler, []}
        ]}
    ]),

    % Start the listener for nagios handler
    Result = cowboy:start_clear(?NAGIOS_LISTENER,
        #{
            num_acceptors => NbAcceptors,
            socket_opts => [
                {ip, any},
                {port, port()}
            ]
        },
        #{
            env => #{dispatch => Dispatch},
            max_keepalive => MaxKeepAlive,
            request_timeout => timer:seconds(Timeout)
        }),
    case Result of
        {ok, _} ->
            ?info("Server '~tp' started successfully", [?NAGIOS_LISTENER]);
        _ ->
            ?error("Could not start server '~tp' - ~tp", [?NAGIOS_LISTENER, Result]),
            Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback stop/0.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    ?info("Stopping '~tp' server...", [?NAGIOS_LISTENER]),

    case cowboy:stop_listener(?NAGIOS_LISTENER) of
        ok ->
            ?info("Server '~tp' stopped", [?NAGIOS_LISTENER]);
        {error, Error} ->
            ?error("Error on stopping server ~tp: ~tp", [?NAGIOS_LISTENER, Error]),
            {error, nagios_stop_error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback reload_web_certs/0.
%% @end
%%--------------------------------------------------------------------
-spec reload_web_certs() -> ok | {error, term()}.
reload_web_certs() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback healthcheck/0.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    Endpoint = str_utils:format_bin("http://127.0.0.1:~B", [port()]),
    case http_client:get(Endpoint) of
        {ok, _, _, _} -> ok;
        _ -> {error, server_not_responding}
    end.
