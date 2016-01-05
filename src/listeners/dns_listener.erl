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
-module(dns_listener).
-author("Tomasz Lichon").
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

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
    {ok, DNSPort} = application:get_env(?CLUSTER_WORKER_APP_NAME, dns_port),
    {ok, EdnsMaxUdpSize} = application:get_env(?CLUSTER_WORKER_APP_NAME, edns_max_udp_size),
    {ok, TCPNumAcceptors} =
        application:get_env(?CLUSTER_WORKER_APP_NAME, dns_tcp_acceptor_pool_size),
    {ok, TCPTImeout} = application:get_env(?CLUSTER_WORKER_APP_NAME, dns_tcp_timeout_seconds),
    OnFailureFun = fun() ->
        ?error("Could not start DNS server on node ~p.", [node()])
    end,
    ok = dns_server:start(?CLUSTER_WORKER_APPLICATION_SUPERVISOR_NAME, DNSPort, dns_worker,
        EdnsMaxUdpSize, TCPNumAcceptors, TCPTImeout, OnFailureFun).


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_starter_behaviour} callback stop/1.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Returns the status of a listener.
%% @end
%%--------------------------------------------------------------------
-callback healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    {ok, HealthcheckTimeout} = application:get_env(?CLUSTER_WORKER_APP_NAME, nagios_healthcheck_timeout),
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
