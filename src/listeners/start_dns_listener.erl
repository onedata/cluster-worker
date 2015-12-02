%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc dns listener starting & stopping
%%% @end
%%%--------------------------------------------------------------------
-module(start_dns_listener).
-author("Tomasz Lichon").
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

-behaviour(listener_behaviour).

%% listener_starter_behaviour callbacks
-export([start/0, stop/0]).

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
  {ok, DNSPort} = application:get_env(?APP_NAME, dns_port),
  {ok, EdnsMaxUdpSize} = application:get_env(?APP_NAME, edns_max_udp_size),
  {ok, TCPNumAcceptors} =
    application:get_env(?APP_NAME, dns_tcp_acceptor_pool_size),
  {ok, TCPTImeout} = application:get_env(?APP_NAME, dns_tcp_timeout_seconds),
  OnFailureFun = fun() ->
    ?error("Could not start DNS server on node ~p.", [node()])
  end,
  ok = dns_server:start(?APPLICATION_SUPERVISOR_NAME, DNSPort, dns_worker,
    EdnsMaxUdpSize, TCPNumAcceptors, TCPTImeout, OnFailureFun).

%%--------------------------------------------------------------------
%% @doc
%% {@link listener_starter_behaviour} callback stop/1.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
  ok.
