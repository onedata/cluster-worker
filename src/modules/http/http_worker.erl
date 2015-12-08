%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module implements worker_plugin_behaviour callbacks.
%%% It is responsible for spawning processes which then process HTTP requests.
%%% @end
%%%--------------------------------------------------------------------
-module(http_worker).
-author("Michal Zmuda").

-behaviour(worker_plugin_behaviour).
-behaviour(endpoint_healthcheck_behaviour).

-include("modules/datastore/datastore.hrl").
-include_lib("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

-define(HTTP_WORKER_PLUGIN, http_worker_plugin).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% endpoint_healthcheck_behaviour callbacks
-export([healthcheck/1]).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
  Result :: {ok, State :: worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
  plugins:apply(?HTTP_WORKER_PLUGIN, init, [_Args]).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
  Request :: ping | healthcheck | {spawn_handler, SocketPid :: pid()},
  Result :: nagios_handler:healthcheck_response() | ok | {ok, Response} |
  {error, Reason} | pong,
  Response :: term(),
  Reason :: term().
handle(ping) ->
  pong;

handle(healthcheck) ->
  Endpoints = plugins:apply(?HTTP_WORKER_PLUGIN, healthcheck_endpoints, []),
  OwnResult = lists:foldl(
    fun
      ({Module, Endpoint}, ok) -> ?error("LOL ~p:healthcheck(~p)", [Module, Endpoint]), Module:healthcheck(Endpoint);
      (_, Error) -> Error
    end, ok, Endpoints),
  case OwnResult of
    ok -> plugins:apply(?HTTP_WORKER_PLUGIN, handle, [healthcheck]);
    Error -> Error
  end;

handle({spawn_handler, SocketPid}) ->
  Pid = spawn(
    fun() ->
      erlang:monitor(process, SocketPid),
      opn_cowboy_bridge:set_socket_pid(SocketPid),
      opn_cowboy_bridge:request_processing_loop()
    end),
  {ok, Pid};

handle(_Request) ->
  plugins:apply(?HTTP_WORKER_PLUGIN, handle, [_Request]).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
  Result :: ok | {error, Error},
  Error :: timeout | term().
cleanup() ->
  ok.

%%%===================================================================
%%% endpoint_healthcheck_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% healthcheck given endpoint
%% @end
%%--------------------------------------------------------------------
-spec healthcheck(Endpoint :: atom()) -> ok | {error, Reason :: atom()}.

healthcheck(nagios) ->
  {ok, GuiPort} = application:get_env(?APP_NAME, http_worker_nagios_port),
  case http_client:get("http://127.0.0.1:" ++ integer_to_list(GuiPort),
    [], <<>>, [insecure]) of
    {ok, _, _, _} ->
      ok;
    _ ->
      {error, no_gui}
  end;

healthcheck(redirector) ->
  {ok, RdrctPort} = application:get_env(?APP_NAME, http_worker_redirect_port),
  case http_client:get("http://127.0.0.1:" ++ integer_to_list(RdrctPort),
    [], <<>>, [insecure]) of
    {ok, _, _, _} ->
      ok;
    _ ->
      {error, no_http_redirector}
  end.

