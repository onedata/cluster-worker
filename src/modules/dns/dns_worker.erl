%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements {@link worker_plugin_behaviour} and
%%% manages a DNS server module.
%%% In addition, it implements {@link dns_handler_behaviour} -
%%% DNS query handling logic.
%%% TODO - migrate dns from ctool
%%% @end
%%%-------------------------------------------------------------------
-module(dns_worker).
-author("Lukasz Opiola").

-behaviour(worker_plugin_behaviour).
-behaviour(dns_handler_behaviour).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("kernel/src/inet_dns.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% dns_handler_behaviour callbacks
-export([handle_a/1, handle_ns/1, handle_cname/1, handle_soa/1, handle_wks/1,
    handle_ptr/1, handle_hinfo/1, handle_minfo/1, handle_mx/1, handle_txt/1]).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init([]) ->
    {ok, #{}};

init(InitialState) when is_map(InitialState) ->
    {ok, InitialState};

init(test) ->
    {ok, #{}};

init(_) ->
    throw(unknown_initial_state).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck,
    Result :: nagios_handler:healthcheck_response() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: [inet:ip4_address()],
    Reason :: term().

handle(ping) ->
    pong;

handle(healthcheck) ->
    _Reply = healthcheck();

handle({update_lb_advice, LBAdvice}) ->
    ?debug("DNS update of load_balancing advice: ~p", [LBAdvice]),
    ok = worker_host:state_put(?MODULE, last_update, now()),
    ok = worker_host:state_put(?MODULE, lb_advice, LBAdvice);

handle({Method, Domain}) ->
    LBAdvice = worker_host:state_get(?MODULE, lb_advice),
    ?debug("DNS A request: ~s, current advice: ~p", [Domain, LBAdvice]),
    case LBAdvice of
        undefined ->
            % The DNS server is still out of sync, return serv fail
            serv_fail;
        _ ->
            plugins:apply(dns_worker_plugin, resolve, [Method, Domain, LBAdvice])
    end;

handle(_Request) ->
    ?log_bad_request(_Request),
    throw({unsupported_request, _Request}).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok.
cleanup() ->
    dns_server:stop(?CLUSTER_WORKER_APPLICATION_SUPERVISOR_NAME).


%%%===================================================================
%%% dns_handler_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type A.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_a(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_a(Domain) ->
    worker_proxy:call(dns_worker, {handle_a, Domain}).


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type NS.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_ns(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_ns(Domain) ->
    worker_proxy:call(dns_worker, {handle_ns, Domain}).


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type CNAME.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_cname(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_cname(Domain) ->
    worker_proxy:call(dns_worker, {handle_cname, Domain}).


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type MX.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_mx(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_mx(Domain) ->
    worker_proxy:call(dns_worker, {handle_mx, Domain}).


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type SOA.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_soa(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_soa(Domain) ->
    worker_proxy:call(dns_worker, {handle_soa, Domain}).


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type WKS.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_wks(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_wks(Domain) ->
    worker_proxy:call(dns_worker, {handle_wks, Domain}).


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type PTR.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_ptr(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_ptr(Domain) ->
    worker_proxy:call(dns_worker, {handle_ptr, Domain}).


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type HINFO.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_hinfo(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_hinfo(Domain) ->
    worker_proxy:call(dns_worker, {handle_hinfo, Domain}).

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type MINFO.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_minfo(Domain :: string()) -> dns_handler_behaviour:handler_reply().
%% ====================================================================
handle_minfo(Domain) ->
    worker_proxy:call(dns_worker, {handle_minfo, Domain}).

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type TXT.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_txt(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_txt(Domain) ->
    worker_proxy:call(dns_worker, {handle_txt, Domain}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% healthcheck dns endpoint
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, Reason :: atom()}.
healthcheck() ->
    LastUpdate = worker_host:state_get(?MODULE, last_update),
    LBAdvice = worker_host:state_get(?MODULE, lb_advice),
    case LBAdvice of
        undefined ->
            {error, no_lb_advice_received};
        _ ->
            {ok, Threshold} = application:get_env(?CLUSTER_WORKER_APP_NAME, dns_disp_out_of_sync_threshold),
            % Threshold is in millisecs, now_diff is in microsecs
            case timer:now_diff(now(), LastUpdate) > Threshold * 1000 of
                true ->
                    % DNS is out of sync
                    out_of_sync;
                false ->
                    % DNS is synced. No need to check for DNS server
                    % connectivity here, as it will be checked with
                    % other listeners.
                    ok
            end
    end.
