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
%%% This module also handles backpressure of connections that post too many
%%% concurrent requests, in two ways:
%%%
%%%   * Gives backpressure_recommendation() to the WebSocket handler
%%%     so it may go into passive mode if the client is sending
%%%     too many requests. This mechanism works in best-effort
%%%     manner as some messages may be already in the buffer, so
%%%     despite the passive mode, new requests may be still coming
%%%     in for some time.
%%%
%%%   * Accumulates requests in an in-memory queue if their number exceed
%%%     a value allowed per connection. Upon every finished request or
%%%     regular pruning, attempts to dispatch the queued requests again.
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
-export([current_backpressure_recommendation/1]).
-export([queue_job/2]).
-export([dispatch_jobs/2]).
-export([process_outcome/3]).
-export([prune_stale_requests/2]).

%% Pool worker callbacks
-export([handle_request/2]).


% see the module doc
-type backpressure_recommendation() :: back_off | accept_requests.

% represents a request that was posted to the worker pool
-record(posted_job, {
    subtype :: gs_protocol:message_subtype(),
    processing_time_stopwatch :: stopwatch:instance()
}).

% see the module doc
-record(tenant, {
    current_backpressure_recommendation = accept_requests :: backpressure_recommendation(),
    posted_jobs = #{} :: #{gs_protocol:message_id() => #posted_job{}},
    queued_jobs = queue:new() :: queue:queue(gs_protocol:req_wrapper()),
    next_continued_backpressure_log_at = 0 :: time:millis()
}).
-type tenant() :: #tenant{}.
-export_type([tenant/0]).


-define(ENV(Name, Default), cluster_worker:get_env(Name, Default)).
%% @see cluster_worker.app.src for descriptions
-define(BATCH_PARALLELISM, ?ENV(graph_sync_batch_parallelism, 10)).
-define(MAX_POOL_USAGE_PER_CONNECTION, ?ENV(graph_sync_max_pool_usage_per_connection, 0.05)).
-define(CALL_TIMEOUT, timer:seconds(?ENV(graph_sync_request_processing_timeout_sec, 60))).
-define(STALE_REQUEST_PRUNING_ENABLED, ?ENV(graph_sync_stale_request_pruning_enabled, true)).
-define(STALE_REQUEST_THRESHOLD, timer:seconds(?ENV(graph_sync_stale_request_threshold_sec, 120))).
% calculated dynamically and set during init
-define(MAX_CONCURRENT_REQUESTS, ?ENV(graph_sync_max_concurrent_requests, 1)).

-define(NOW_MILLIS(), global_clock:timestamp_millis()).
-define(CONTINUED_BACKPRESSURE_LOG_INTENSITY_MILLIS, ?ENV(graph_sync_continued_backpressure_log_intensity_millis, 10000)).

-define(POOL_NAME, ?MODULE).


%%%===================================================================
%%% Pool management API
%%%===================================================================


-spec init(pos_integer()) -> ok.
init(PoolSize) ->
    {ok, _} = worker_pool:start_sup_pool(?POOL_NAME, [{workers, PoolSize}]),
    cluster_worker:set_env(graph_sync_max_concurrent_requests, ceil(PoolSize * ?MAX_POOL_USAGE_PER_CONNECTION)),
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


-spec current_backpressure_recommendation(tenant()) -> backpressure_recommendation().
current_backpressure_recommendation(#tenant{current_backpressure_recommendation = CTR}) ->
    CTR.


-spec queue_job(tenant(), gs_protocol:req_wrapper()) -> tenant().
queue_job(Tenant = #tenant{queued_jobs = QueuedJobs}, RequestWrapper) ->
    Tenant#tenant{
        queued_jobs = queue:in(RequestWrapper, QueuedJobs)
    }.


%%--------------------------------------------------------------------
%% @doc
%% Attempts to post all queued jobs. In case the maximum pool usage of a
%% single tenant is exceeded, it backpressurizes the tenant: resigns and sends
%% a delayed notification to the called pid to attempt resubmission later.
%% @end
%%--------------------------------------------------------------------
-spec dispatch_jobs(tenant(), gs_session:data()) -> tenant().
dispatch_jobs(Tenant0, SessionData) ->
    Tenant1 = calculate_backpressure_recommendation(Tenant0, SessionData),
    case Tenant1 of
        #tenant{current_backpressure_recommendation = back_off} ->
            Tenant1;
        #tenant{current_backpressure_recommendation = accept_requests, queued_jobs = QueuedJobs} ->
            case queue:out(QueuedJobs) of
                {empty, QueuedJobs} ->
                    Tenant1;
                {{value, RequestWrapper}, NewQueuedJobs} ->
                    Tenant2 = post_job_to_pool(Tenant1, SessionData, RequestWrapper),
                    dispatch_jobs(Tenant2#tenant{
                        queued_jobs = NewQueuedJobs
                    }, SessionData)
            end
    end.


-spec process_outcome(tenant(), gs_session:data(), {gs_worker_pool_result, gs_protocol:resp_wrapper()}) ->
    {gs_protocol:resp_wrapper(), tenant()}.
process_outcome(
    #tenant{posted_jobs = PostedJobs} = Tenant,
    SessionData,
    ?GS_WORKER_POOL_JOB_OUTCOME(#gs_resp{id = RequestId} = ResponseMessage)
) ->
    Stopwatch = case maps:find(RequestId, PostedJobs) of
        error ->
            unknown;
        {ok, #posted_job{processing_time_stopwatch = S}} ->
            S
    end,
    gs_verbose_logger:report_sending_reply(SessionData, RequestId, ResponseMessage, Stopwatch),

    {ResponseMessage, dispatch_jobs(Tenant#tenant{
        posted_jobs = maps:remove(RequestId, PostedJobs)
    }, SessionData)}.


-spec prune_stale_requests(tenant(), gs_session:data()) ->
    {[gs_protocol:resp_wrapper()], tenant()}.
prune_stale_requests(Tenant, SessionData) ->
    case ?STALE_REQUEST_PRUNING_ENABLED of
        true ->
            prune_stale_requests_internal(Tenant, SessionData);
        false ->
            {[], dispatch_jobs(Tenant, SessionData)}
    end.

%% @private
-spec prune_stale_requests_internal(tenant(), gs_session:data()) ->
    {[gs_protocol:resp_wrapper()], tenant()}.
prune_stale_requests_internal(#tenant{posted_jobs = PostedJobs} = Tenant, SessionData) ->
    StaleReqs = maps:filter(fun(_RequestId, #posted_job{processing_time_stopwatch = Stopwatch}) ->
        stopwatch:read_millis(Stopwatch) > ?STALE_REQUEST_THRESHOLD
    end, PostedJobs),

    ResponseMessages = lists:map(fun({RequestId, #posted_job{
        subtype = RequestSubtype,
        processing_time_stopwatch = Stopwatch
    }}) ->
        TimeoutError = ?ERROR_TIMEOUT,
        gs_verbose_logger:report_stale_request_pruned(SessionData, RequestId, TimeoutError, Stopwatch),
        gs_protocol:generate_error_response(RequestId, RequestSubtype, TimeoutError)
    end, maps:to_list(StaleReqs)),

    {ResponseMessages, dispatch_jobs(Tenant#tenant{
        posted_jobs = maps:without(maps:keys(StaleReqs), PostedJobs)
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
-spec post_job_to_pool(tenant(), gs_session:data(), gs_protocol:req_wrapper()) -> tenant().
post_job_to_pool(#tenant{posted_jobs = PostedJobs} = Tenant, SessionData, #gs_req{
    id = RequestId,
    subtype = RequestSubtype,
    auth_override = AuthOverride
} = RequestWrapper) ->

    CallerPid = self(),
    Stopwatch = stopwatch:start(),

    spawn(fun() ->
        ResponseMessage = try
            gs_verbose_logger:report_request_received(SessionData, RequestId, RequestWrapper),

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

    Tenant#tenant{
        posted_jobs = PostedJobs#{
            RequestId => #posted_job{
                subtype = RequestSubtype,
                processing_time_stopwatch = Stopwatch
            }
        }
    }.


%% @private
-spec calculate_backpressure_recommendation(tenant(), gs_session:data()) ->
    tenant().
calculate_backpressure_recommendation(#tenant{
    posted_jobs = PostedJobs,
    queued_jobs = QueuedJobs,
    current_backpressure_recommendation = CTR
} = Tenant, SessionData) ->
    NowMillis = ?NOW_MILLIS(),
    PostedJobCount = maps:size(PostedJobs),
    QueueSize = queue:len(QueuedJobs),
    MaxRequests = ?MAX_CONCURRENT_REQUESTS,

    if
        CTR == accept_requests, PostedJobCount >= MaxRequests, QueueSize > 0 ->
            gs_verbose_logger:report_backpressure_triggered(SessionData, PostedJobCount, QueueSize),
            Tenant#tenant{
                current_backpressure_recommendation = back_off,
                next_continued_backpressure_log_at = NowMillis + ?CONTINUED_BACKPRESSURE_LOG_INTENSITY_MILLIS
            };

        CTR == back_off, PostedJobCount =< MaxRequests div 2 ->
            gs_verbose_logger:report_backpressure_stopped(SessionData, PostedJobCount, QueueSize),
            Tenant#tenant{
                current_backpressure_recommendation = accept_requests
            };

        CTR == back_off, NowMillis > Tenant#tenant.next_continued_backpressure_log_at ->
            gs_verbose_logger:report_backpressure_continues(SessionData, PostedJobCount, QueueSize),
            Tenant#tenant{
                next_continued_backpressure_log_at = NowMillis + ?CONTINUED_BACKPRESSURE_LOG_INTENSITY_MILLIS
            };

        true ->
            Tenant
    end.
