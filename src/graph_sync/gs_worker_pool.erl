%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% High-level abstraction of worker_pool for the needs of processing
%%% GraphSync requests.
%%%
%%% GS requests are posted to the pool for processing and the results
%%% are reported back to the caller pid.
%%%
%%% Each GS connection is represented as a #tenant{} and treated
%%% separately (the same client can have multiple connections and they
%%% are treated as different tenants). All tenants share the common
%%% processing resources of the pool.
%%%
%%% This module also handles throttling of connections that post too many
%%% concurrent requests, in two ways:
%%%
%%%   * Gives throttling_recommendation() to the WebSocket handler
%%%     so it may go into passive mode if the client is sending
%%%     too many requests. This mechanism works in best-effort
%%%     manner as some messages may be already in the buffer, so
%%%     despite the passive mode, new requests may be still coming
%%%     in for some time.
%%%
%%%   * Applies an artificial delay on every request if the queue
%%%     is too big (proportional to the queue size).
%%% @end
%%%-------------------------------------------------------------------
-module(gs_worker_pool).
-author("Lukasz Opiola").


-include("modules/datastore/datastore_models.hrl").
-include("graph_sync/graph_sync.hrl").
-include_lib("ctool/include/logging.hrl").


%% Pool management API
-export([init/1, stop/0]).

%% Tenant interfacing API
-export([max_concurrent_requests/0]).
-export([new_tenant/0]).
-export([post_job/3]).
-export([process_outcome/3]).
-export([prune_stale_requests/2]).

%% Pool worker callbacks
-export([handle_request/2]).


% see the module doc
-type throttling_recommendation() :: start_throttling | resume_processing.

% represents a request that was posted to the worker pool
-record(pending_request, {
    subtype :: gs_protocol:message_subtype(),
    job_posted_stopwatch :: stopwatch:instance()
}).

% see the module doc
-record(tenant, {
    current_throttling_recommendation = resume_processing :: throttling_recommendation(),
    pending_requests = #{} :: #{gs_protocol:message_id() => #pending_request{}}
}).
-type tenant() :: #tenant{}.
-export_type([tenant/0]).


-define(ENV(Name, Default), cluster_worker:get_env(Name, Default)).
%% @see cluster_worker.app.src for descriptions
-define(BATCH_PARALLELISM, ?ENV(graph_sync_batch_parallelism, 10)).
-define(MAX_CONCURRENT_REQUESTS, ?ENV(graph_sync_max_concurrent_requests, 4)).
-define(THROTTLING_FACTOR, ?ENV(graph_sync_throttling_factor, 2)).
-define(CALL_TIMEOUT, timer:seconds(?ENV(graph_sync_request_processing_timeout_sec, 60))).
-define(STALE_REQUEST_PRUNING_ENABLED, ?ENV(graph_sync_stale_request_pruning_enabled, true)).
-define(STALE_REQUEST_THRESHOLD, timer:seconds(?ENV(graph_sync_stale_request_threshold_sec, 120))).


-define(POOL_NAME, ?MODULE).


%%%===================================================================
%%% Pool management API
%%%===================================================================


-spec init(pos_integer()) -> ok.
init(PoolSize) ->
    {ok, _} = worker_pool:start_sup_pool(?POOL_NAME, [{workers, PoolSize}]),
    ok.


-spec stop() -> ok.
stop() ->
    ok = worker_pool:stop_sup_pool(?POOL_NAME).


%%%===================================================================
%%% Tenant interfacing API
%%%===================================================================


-spec max_concurrent_requests() -> pos_integer().
max_concurrent_requests() ->
    ?MAX_CONCURRENT_REQUESTS.


-spec new_tenant() -> tenant().
new_tenant() ->
    #tenant{}.


-spec post_job(tenant(), gs_session:data(), gs_protocol:req_wrapper()) ->
    {throttling_recommendation(), tenant()}.
post_job(#tenant{pending_requests = PendingRequests} = Tenant, SessionData, #gs_req{
    id = RequestId,
    subtype = RequestSubtype,
    auth_override = AuthOverride
} = RequestWrapper) ->

    CallerPid = self(),
    Stopwatch = stopwatch:start(),

    spawn(fun() ->
        ResponseMessage = try
            gs_verbose_logger:report_request_received(SessionData, RequestId, RequestWrapper),

            enforce_throttling_delay(Tenant, SessionData),

            EffSessionData = case gs_server:verify_auth_override(SessionData#gs_session.auth, AuthOverride) of
                {error, _} = AuthOverrideError ->
                    gs_verbose_logger:report_auth_override_error(
                        SessionData, RequestId, AuthOverride, AuthOverrideError, Stopwatch
                    ),
                    throw(AuthOverrideError);
                false ->
                    SessionData;
                {true, OverriddenAuth} ->
                    gs_verbose_logger:report_auth_override_success(
                        SessionData, RequestId, AuthOverride, OverriddenAuth, Stopwatch
                    ),
                    SessionData#gs_session{auth = OverriddenAuth}
            end,

            {ok, CallResult} = worker_pool:call(
                ?POOL_NAME,
                {?MODULE, handle_request, [EffSessionData, RequestWrapper]},
                worker_pool:default_strategy(),
                ?CALL_TIMEOUT
            ),
            CallResult
        catch
            exit:timeout ->
                gs_protocol:generate_error_response(RequestWrapper, ?ERROR_TIMEOUT);
            exit:{killed, {gen_server, call, _}} ->
                gs_protocol:generate_error_response(RequestWrapper, ?ERROR_TIMEOUT);
            Class:Reason:Stacktrace ->
                Error = ?examine_exception(Class, Reason, Stacktrace),
                gs_protocol:generate_error_response(RequestWrapper, Error)
        end,

        gs_verbose_logger:report_job_finished(SessionData, RequestId, ResponseMessage, Stopwatch),

        CallerPid ! ?GS_WORKER_POOL_JOB_OUTCOME(ResponseMessage)
    end),

    calculate_throttling_recommendation(Tenant#tenant{
        pending_requests = PendingRequests#{
            RequestId => #pending_request{
                subtype = RequestSubtype,
                job_posted_stopwatch = Stopwatch
            }
        }
    }, SessionData).


-spec process_outcome(tenant(), gs_session:data(), {gs_worker_pool_result, gs_protocol:resp_wrapper()}) ->
    {gs_protocol:resp_wrapper(), {throttling_recommendation(), tenant()}}.
process_outcome(
    #tenant{pending_requests = PendingRequests} = Tenant,
    SessionData,
    ?GS_WORKER_POOL_JOB_OUTCOME(#gs_resp{id = RequestId} = ResponseMessage)
) ->
    Stopwatch = case maps:find(RequestId, PendingRequests) of
        error ->
            unknown;
        {ok, #pending_request{job_posted_stopwatch = S}} ->
            S
    end,
    gs_verbose_logger:report_sending_reply(SessionData, RequestId, ResponseMessage, Stopwatch),

    {ResponseMessage, calculate_throttling_recommendation(Tenant#tenant{
        pending_requests = maps:remove(RequestId, PendingRequests)
    }, SessionData)}.


-spec prune_stale_requests(tenant(), gs_session:data()) ->
    {[gs_protocol:resp_wrapper()], {throttling_recommendation(), tenant()}}.
prune_stale_requests(Tenant, SessionData) ->
    case ?STALE_REQUEST_PRUNING_ENABLED of
        true ->
            prune_stale_requests_internal(Tenant, SessionData);
        false ->
            {[], calculate_throttling_recommendation(Tenant, SessionData)}
    end.

%% @private
-spec prune_stale_requests_internal(tenant(), gs_session:data()) ->
    {[gs_protocol:resp_wrapper()], {throttling_recommendation(), tenant()}}.
prune_stale_requests_internal(#tenant{pending_requests = PendingRequests} = Tenant, SessionData) ->
    StaleReqs = maps:filter(fun(_RequestId, #pending_request{job_posted_stopwatch = Stopwatch}) ->
        stopwatch:read_millis(Stopwatch) > ?STALE_REQUEST_THRESHOLD
    end, PendingRequests),

    ResponseMessages = lists:map(fun({RequestId, #pending_request{
        subtype = RequestSubtype,
        job_posted_stopwatch = Stopwatch
    }}) ->
        TimeoutError = ?ERROR_TIMEOUT,
        gs_verbose_logger:report_stale_request_pruned(SessionData, RequestId, TimeoutError, Stopwatch),
        gs_protocol:generate_error_response(RequestId, RequestSubtype, TimeoutError)
    end, maps:to_list(StaleReqs)),

    {ResponseMessages, calculate_throttling_recommendation(Tenant#tenant{
        pending_requests = maps:without(maps:keys(StaleReqs), PendingRequests)
    }, SessionData)}.


%%%===================================================================
%%% Pool worker callbacks
%%%===================================================================


-spec handle_request(gs_session:data(), gs_protocol:req_wrapper()) ->
    gs_protocol:resp_wrapper().
handle_request(SessionData, RequestWrapper = #gs_req{request = Request}) ->
    Result = case Request of
        #gs_req_batch{requests = Requests} ->
            ?catch_exceptions(
                {ok, #gs_resp_batch{
                    responses = lists_utils:pmap(fun(InnerRequest) ->
                        handle_request(SessionData, InnerRequest)
                    end, Requests, ?BATCH_PARALLELISM)
                }}
            );
        _ ->
            gs_server:handle_request(SessionData, Request)
    end,
    case Result of
        {ok, Resp} ->
            gs_protocol:generate_success_response(RequestWrapper, Resp);
        {error, _} = Error ->
            gs_protocol:generate_error_response(RequestWrapper, Error)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec calculate_throttling_recommendation(tenant(), gs_session:data()) ->
    {throttling_recommendation(), tenant()}.
calculate_throttling_recommendation(#tenant{
    current_throttling_recommendation = resume_processing
} = Tenant, SessionData) ->
    QueueSize = maps:size(Tenant#tenant.pending_requests),
    NewRecommendation = case QueueSize > ?MAX_CONCURRENT_REQUESTS of
        true ->
            gs_verbose_logger:report_throttling_triggered(SessionData, QueueSize),
            start_throttling;
        false ->
            resume_processing
    end,
    {NewRecommendation, Tenant#tenant{current_throttling_recommendation = NewRecommendation}};

calculate_throttling_recommendation(#tenant{
    current_throttling_recommendation = start_throttling
} = Tenant, SessionData) ->
    QueueSize = maps:size(Tenant#tenant.pending_requests),
    MaxConRequests = ?MAX_CONCURRENT_REQUESTS,
    NewRecommendation = case QueueSize =< MaxConRequests div 2 of
        true ->
            gs_verbose_logger:report_throttling_stopped(SessionData, QueueSize),
            resume_processing;
        false ->
            % If there are many WS messages in the buffer, it's possible that
            % the queue will keep rising, despite the throttling. Log from time
            % to time (with exponential growth).
            QueueSize rem MaxConRequests == 0 andalso is_power_of_two(QueueSize div MaxConRequests) andalso
                gs_verbose_logger:report_throttling_triggered(SessionData, QueueSize),
            start_throttling
    end,
    {NewRecommendation, Tenant#tenant{current_throttling_recommendation = NewRecommendation}}.


%% @private
-spec enforce_throttling_delay(tenant(), gs_session:data()) -> ok.
enforce_throttling_delay(#tenant{
    current_throttling_recommendation = resume_processing
}, _SessionData) ->
    ok;
enforce_throttling_delay(#tenant{
    current_throttling_recommendation = start_throttling
} = Tenant, SessionData) ->
    QueueSize = maps:size(Tenant#tenant.pending_requests),
    case QueueSize > ?MAX_CONCURRENT_REQUESTS of
        true ->
            Delay = ?THROTTLING_FACTOR * QueueSize div ?MAX_CONCURRENT_REQUESTS,
            gs_verbose_logger:report_request_throttled(SessionData, QueueSize, Delay),
            timer:sleep(Delay);
        false ->
            ok
    end.


%% @private
-spec is_power_of_two(integer()) -> boolean().
is_power_of_two(N) when is_integer(N), N > 0 ->
    (N band (N - 1)) =:= 0;
is_power_of_two(_) ->
    false.
