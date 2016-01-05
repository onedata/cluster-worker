%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc gui listener starting & stopping
%%% @end
%%%--------------------------------------------------------------------
-module(nagios_listener).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Cowboy listener references
-define(NAGIOS_LISTENER, nagios).

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
    {ok, Port} = application:get_env(?CLUSTER_WORKER_APP_NAME, http_worker_nagios_port),
    {ok, NbAcceptors} =
        application:get_env(?CLUSTER_WORKER_APP_NAME, http_worker_number_of_acceptors),
    {ok, MaxKeepAlive} =
        application:get_env(?CLUSTER_WORKER_APP_NAME, http_worker_max_keepalive),
    {ok, Timeout} =
        application:get_env(?CLUSTER_WORKER_APP_NAME, http_worker_socket_timeout_seconds),

    Dispatch = [
        {'_', [
            {"/nagios/[...]", nagios_handler, []}
        ]}
    ],

    % Start the listener for nagios handler
    Result = cowboy:start_http(?NAGIOS_LISTENER, NbAcceptors,
        [
            {ip, {0, 0, 0, 0}},
            {port, Port}
        ], [
            {env, [{dispatch, cowboy_router:compile(Dispatch)}]},
            {max_keepalive, MaxKeepAlive},
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
    case catch cowboy:stop_listener(?NAGIOS_LISTENER) of
        (ok) ->
            ok;
        (Error) ->
            ?error("Error on stopping listener ~p: ~p", [?NAGIOS_LISTENER, Error]),
            {error, nagios_stop_error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns the status of a listener.
%% @end
%%--------------------------------------------------------------------
-callback healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    {ok, NagiosPort} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        http_worker_nagios_port),
    case http_client:get("http://127.0.0.1:" ++ integer_to_list(NagiosPort),
        [], <<>>, [insecure]) of
        {ok, _, _, _} ->
            ok;
        _ ->
            {error, server_not_responding}
    end.
