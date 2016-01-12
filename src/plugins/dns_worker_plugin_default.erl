%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Plugin for DNS worker. Accepts urls according to op-worker requirements.
%%% @end
%%%-------------------------------------------------------------------
-module(dns_worker_plugin_default).
-author("Michal Zmuda").

-behavior(dns_worker_plugin_behaviour).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("kernel/src/inet_dns.hrl").

-export([resolve/3]).

%%--------------------------------------------------------------------
%% @doc
%% {@link dns_worker_plugin_behaviour} callback resolve/3.
%% @end
%%--------------------------------------------------------------------
-spec resolve(Method :: atom(), Domain :: string(), LbAdvice :: term()) ->
    refused | nx_domain | {ok, [{A :: byte(), B :: byte(), C :: byte(), D :: byte()}]}.

resolve(handle_a, Domain, LBAdvice) ->
    Nodes = load_balancing:choose_nodes_for_dns(LBAdvice),
    {ok, TTL} = application:get_env(?CLUSTER_WORKER_APP_NAME, dns_a_response_ttl),
    {ok,
            [dns_server:answer_record(Domain, TTL, ?S_A, IP) || IP <- Nodes] ++
            [dns_server:authoritative_answer_flag(true)]
    };

resolve(handle_ns, Domain, LBAdvice) ->
    Nodes = load_balancing:choose_ns_nodes_for_dns(LBAdvice),
    {ok, TTL} = application:get_env(?CLUSTER_WORKER_APP_NAME, dns_ns_response_ttl),
    {ok,
            [dns_server:answer_record(Domain, TTL, ?S_NS, inet_parse:ntoa(IP)) || IP <- Nodes] ++
            [dns_server:authoritative_answer_flag(true)]
    };

resolve(_, _, _) ->
    serv_fail.