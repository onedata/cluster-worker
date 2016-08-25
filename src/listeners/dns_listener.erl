%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module is responsible for DNS listener starting and stopping.
%%% @end
%%%--------------------------------------------------------------------
-module(dns_listener).
-author("Tomasz Lichon").
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("kernel/src/inet_dns.hrl").

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
    {ok, DNSPort} = application:get_env(?CLUSTER_WORKER_APP_NAME, dns_port),
    DNSPort.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback start/0.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    ok = dns_server:start().


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback stop/0.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    ok = dns_server:stop().


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback healthcheck/0.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    % @todo check TCP listener too
    {ok, HealthcheckTimeout} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        nagios_healthcheck_timeout),
    {ok, DNSPort} = application:get_env(?CLUSTER_WORKER_APP_NAME, dns_port),
    Query = inet_dns:encode(
        #dns_rec{
            header = #dns_header{
                id = crypto:rand_uniform(1, 16#FFFF),
                opcode = 'query',
                rd = true
            },
            qdlist = [#dns_query{
                domain = "localhost",
                type = soa,
                class = in
            }],
            arlist = [{dns_rr_opt, ".", opt, 1280, 0, 0, 0, <<>>}]
        }),
    {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
    gen_udp:send(Socket, "127.0.0.1", DNSPort, Query),
    case gen_udp:recv(Socket, 65535, HealthcheckTimeout) of
        {ok, _} ->
            % DNS is working
            ok;
        _ ->
            % DNS is not working
            {error, server_not_responding}
    end.
