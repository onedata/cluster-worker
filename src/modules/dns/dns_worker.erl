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
%%% In addition, it implements part of DNS query handling logic.
%%% This module relies on dns_worker_plugin_behaviour providing the
%%% actual DNS answers.
%%% @end
%%%-------------------------------------------------------------------
-module(dns_worker).
-author("Lukasz Opiola").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("kernel/src/inet_dns.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([resolve/2]).

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
    try
        ok = worker_host:state_put(?MODULE, last_update, erlang:monotonic_time(milli_seconds)),
        ok = worker_host:state_put(?MODULE, lb_advice, LBAdvice)
    catch
        error:badarg ->
            % Possible error during node init - log only debug
            ?debug("Cannot update lb advice: badarg"),
            ok
    end;

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
    dns_server:stop().


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries.
%% See {@link dns_worker_plugin_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec resolve(Method :: dns_worker_plugin_behaviour:handle_method(),
    Domain :: string()) -> dns_worker_plugin_behaviour:handler_reply() | {error, term()}.
resolve(Method, Domain) ->
    worker_proxy:call(dns_worker, {Method, Domain}).

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
            % Threshold is in millisecs, LastUpdate is in millisecs
            case (erlang:monotonic_time(milli_seconds) - LastUpdate) > Threshold of
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
