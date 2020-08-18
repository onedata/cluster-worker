%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an interface for a CouchBase worker pool.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_pool).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("exometer_utils.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([post_async/3, post_async/4, post/3, wait/2]).
-export([get_timeout/0, get_modes/0, get_size/2]).
-export([get_request_queue_size/1, get_request_queue_size/2,
    get_worker_queue_size_stats/1, get_worker_queue_size_stats/2,
    reset_request_queue_size/3, update_request_queue_size/4]).
-export([init_counters/0, init_report/0]).
-export([get_workers/1, get_workers/2]).

-type mode() :: changes | read | write.
-type ctx() :: couchbase_driver:ctx().
-type key() :: couchbase_driver:key().
-type value() :: couchbase_driver:value().
-type design() :: couchbase_driver:design().
-type view() :: couchbase_driver:view().
-type view_opt() :: couchbase_driver:view_opt().
-type request() :: {save, ctx(), key(), value()} |
                   {get, key()} |
                   {delete, ctx(), key()} |
                   {get_counter, key(), cberl:arithmetic_default()} |
                   {update_counter, key(), cberl:arithmetic_delta(),
                       cberl:arithmetic_default()} |
                   {save_design_doc, design(), datastore_json:ejson()} |
                   {get_design_doc, design()} |
                   {delete_design_doc, design()} |
                   {query_view, design(), view(), [view_opt()]}.
-type response() :: ok | {ok, term()} | {ok, term(), term()} | {error, term()}.
-type future() :: {reference(), pid()}.

-export_type([mode/0, request/0, response/0, future/0]).

-define(EXOMETER_NAME(Bucket, Mode),  ?exometer_name(?MODULE, Bucket, Mode)).
-define(EXOMETER_DEFAULT_DATA_POINTS_NUMBER, 10000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes exometer counters used by this module.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
    Size = application:get_env(?CLUSTER_WORKER_APP_NAME, 
        exometer_data_points_number, ?EXOMETER_DEFAULT_DATA_POINTS_NUMBER),
    Counters = lists:foldl(fun(Bucket, Acc1) ->
        lists:foldl(fun(Mode, Acc2) ->
            [{?EXOMETER_NAME(Bucket, Mode), uniform, [{size, Size}]} | Acc2]
        end, Acc1, get_modes())
    end, [], couchbase_config:get_buckets()),
    ?init_counters(Counters).

%%--------------------------------------------------------------------
%% @doc
%% Sets exometer report connected with counters used by this module.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    Reports = lists:foldl(fun(Bucket, Acc1) ->
        lists:foldl(fun(Mode, Acc2) ->
            [{?EXOMETER_NAME(Bucket, Mode), [min, max, median, mean, n]} | Acc2]
        end, Acc1, get_modes())
    end, [], couchbase_config:get_buckets()),
    ?init_reports(Reports).

%%--------------------------------------------------------------------
%% @doc
%% Schedules request execution on a worker pool.
%% @equiv post_async(Bucket, Mode, Request, self())
%% @end
%%--------------------------------------------------------------------
-spec post_async(couchbase_config:bucket(), mode(), request()) ->
    future().
post_async(Bucket, Mode, Request) ->
    post_async(Bucket, Mode, Request, self()).

%%--------------------------------------------------------------------
%% @doc
%% Schedules request execution on a worker pool.
%% @end
%%--------------------------------------------------------------------
-spec post_async(couchbase_config:bucket(), mode(), request(), pid()) ->
    future().
post_async(Bucket, Mode, Request, ResponseTo) ->
    Ref = make_ref(),
    {Id, Worker} = get_worker(Bucket, Mode),
    update_request_queue_size(Bucket, Mode, Id, 1),
    Worker ! {post, {Ref, ResponseTo, Request}},
    {Ref, Worker}.

%%--------------------------------------------------------------------
%% @doc
%% Schedules request execution on a worker pool and awaits response.
%% @end
%%--------------------------------------------------------------------
-spec post(couchbase_config:bucket(), mode(), request()) -> response().
post(Bucket, Mode, Request) ->
    wait(post_async(Bucket, Mode, Request), true).

%%--------------------------------------------------------------------
%% @doc
%% Waits for response associated with a reference.
%% @end
%%--------------------------------------------------------------------
-spec wait(future(), boolean()) -> response().
wait({Future, Worker} = WaitData, CheckAndRetry) ->
    Timeout = get_timeout(),
    receive
        {Future, Response} -> Response
    after
        Timeout ->
            case {CheckAndRetry, erlang:is_process_alive(Worker)} of
                {true, true} ->
                    wait(WaitData, CheckAndRetry);
                {true, _} ->
                    wait(WaitData, false); % retry last time to prevent race between
                                           % answer sending / process terminating
                _ ->
                    {error, timeout}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns request handling timeout.
%% @end
%%--------------------------------------------------------------------
-spec get_timeout() -> timeout().
get_timeout() ->
    application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_operation_timeout, 1800000).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of available worker pool modes.
%% @end
%%--------------------------------------------------------------------
-spec get_modes() -> [mode()].
get_modes() ->
    [changes, read, write].

%%--------------------------------------------------------------------
%% @doc
%% Returns size of worker pool for given mode.
%% @end
%%--------------------------------------------------------------------
-spec get_size(couchbase_config:bucket(), mode()) -> non_neg_integer().
get_size(Bucket, Mode) ->
    PoolSizeByBucket = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_size, []),
    PoolSizeByMode = proplists:get_value(
        Bucket, PoolSizeByBucket, proplists:get_value('_', PoolSizeByBucket, [])
    ),
    proplists:get_value(Mode, PoolSizeByMode, 20).

%%--------------------------------------------------------------------
%% @doc
%% Returns requests queue size for given bucket and all modes.
%% @end
%%--------------------------------------------------------------------
-spec get_request_queue_size(couchbase_config:bucket()) -> non_neg_integer().
get_request_queue_size(Bucket) ->
    lists:foldl(fun(Mode, Size) ->
        Size + get_request_queue_size(Bucket, Mode)
    end, 0, get_modes()).

%%--------------------------------------------------------------------
%% @doc
%% Returns requests queue size for given bucket and mode.
%% @end
%%--------------------------------------------------------------------
-spec get_request_queue_size(couchbase_config:bucket(), mode()) ->
    non_neg_integer().
get_request_queue_size(Bucket, Mode) ->
    lists:foldl(fun(Id, Size) ->
        Key = {request_queue_size, Bucket, Mode, Id},
        Size + ets:lookup_element(couchbase_pool_stats, Key, 2)
    end, 0, lists:seq(1, get_size(Bucket, Mode))).

%%--------------------------------------------------------------------
%% @doc
%% Returns {max, sum} of queue sizes for given bucket and all modes.
%% @end
%%--------------------------------------------------------------------
-spec get_worker_queue_size_stats(couchbase_config:bucket()) ->
    {Max :: non_neg_integer(), Sum :: non_neg_integer()}.
get_worker_queue_size_stats(Bucket) ->
    lists:foldl(fun(Mode, {Max, Sum}) ->
        {M, S} = get_worker_queue_size_stats(Bucket, Mode),
        {max(Max, M), Sum + S}
    end, {0, 0}, get_modes()).

%%--------------------------------------------------------------------
%% @doc
%% Returns {max, sum} of queue sizes for given bucket and mode.
%% @end
%%--------------------------------------------------------------------
-spec get_worker_queue_size_stats(couchbase_config:bucket(), mode()) ->
    {Max :: non_neg_integer(), Sum :: non_neg_integer()}.
get_worker_queue_size_stats(Bucket, Mode) ->
    lists:foldl(fun(Id, {Max, Sum}) ->
        Key = {request_queue_size, Bucket, Mode, Id},
        ModeSize = ets:lookup_element(couchbase_pool_stats, Key, 2),
        ?update_counter(?EXOMETER_NAME(Bucket, Mode), ModeSize),
        {max(Max, ModeSize), Sum + ModeSize}
    end, {0, 0}, lists:seq(1, get_size(Bucket, Mode))).

%%--------------------------------------------------------------------
%% @doc
%% Set worker's requests queue size to zero.
%% @end
%%--------------------------------------------------------------------
-spec reset_request_queue_size(couchbase_config:bucket(), mode(),
    couchbase_pool_worker:id()) -> ok.
reset_request_queue_size(Bucket, Mode, Id) ->
    Key = {request_queue_size, Bucket, Mode, Id},
    ets:insert(couchbase_pool_stats, {Key, 0}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Updates worker's requests queue size by delta. Prevents queue size from
%% getting below zero.
%% @end
%%--------------------------------------------------------------------
-spec update_request_queue_size(couchbase_config:bucket(), mode(),
    couchbase_pool_worker:id(), integer()) -> ok.
update_request_queue_size(Bucket, Mode, Id, Delta) when Delta < 0 ->
    Key = {request_queue_size, Bucket, Mode, Id},
    ets:update_counter(couchbase_pool_stats, Key, {2, Delta, 0, 0}, {Key, 0}),
    ok;
update_request_queue_size(Bucket, Mode, Id, Delta) ->
    Key = {request_queue_size, Bucket, Mode, Id},
    ets:update_counter(couchbase_pool_stats, Key, {2, Delta}, {Key, 0}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns all workers for mode.
%% @end
%%--------------------------------------------------------------------
-spec get_workers(mode()) -> [pid()].
get_workers(Mode) ->
    lists:foldl(fun(Bucket, Acc1) ->
        Acc1 ++ get_workers(Bucket, Mode)
    end, [], couchbase_config:get_buckets()).

%%--------------------------------------------------------------------
%% @doc
%% Returns all workers for bucket and mode.
%% @end
%%--------------------------------------------------------------------
-spec get_workers(couchbase_config:bucket(), mode()) -> [pid()].
get_workers(Bucket, Mode) ->
    Size = get_size(Bucket, Mode),
    lists:map(fun(Id) ->
        couchbase_pool_sup:get_worker(Bucket, Mode, Id)
    end, lists:seq(1, Size)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns worker Id to be used by next operation.
%% @end
%%--------------------------------------------------------------------
-spec get_worker(couchbase_config:bucket(), mode()) ->
    {couchbase_pool_worker:id(), pid()}.
get_worker(Bucket, write = Mode) ->
    Alg = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_worker_algorithm, {big_limited_batch, 10, 104857600}),
    case Alg of
        round_robin ->
            Id = get_next_worker_id(Bucket, Mode, 0),
            {Id, couchbase_pool_sup:get_worker(Bucket, Mode, Id)};
        _ ->
            Key = {next_worker_id, Bucket, Mode},
            Id = case ets:lookup(couchbase_pool_stats, Key) of
                [{Key, FoundId}] -> FoundId;
                _ -> 1
            end,

            WorkerKey = {request_queue_size, Bucket, Mode, Id},
            Size = case ets:lookup(couchbase_pool_stats, WorkerKey) of
                [{WorkerKey, S}] -> S;
                _ -> 0
            end,

            MaxSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_batch_size, 1000),
            case Size < MaxSize of
                true ->
                    case Alg of
                        {big_limited_batch, CheckSize, MemoryLimit} ->
                            Worker = couchbase_pool_sup:get_worker(Bucket, Mode, Id),
                            case Size < CheckSize of
                                true ->
                                    {Id, Worker};
                                _ ->
                                    {memory, Mem} = erlang:process_info(Worker, memory),
                                    case Mem < MemoryLimit of
                                        true ->
                                            {Id, Worker};
                                        _ ->
                                            Id2 = get_next_worker_id(Bucket, Mode, 1),
                                            {Id2, couchbase_pool_sup:get_worker(Bucket, Mode, Id2)}
                                    end
                            end;
                        _ ->
                            {Id, couchbase_pool_sup:get_worker(Bucket, Mode, Id)}
                    end;
                _ ->
                    Id2 = get_next_worker_id(Bucket, Mode, 1),
                    {Id2, couchbase_pool_sup:get_worker(Bucket, Mode, Id2)}
            end
    end;
get_worker(Bucket, Mode) ->
    Id = get_next_worker_id(Bucket, Mode, 0),
    {Id, couchbase_pool_sup:get_worker(Bucket, Mode, Id)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns next worker Id.
%% @end
%%--------------------------------------------------------------------
-spec get_next_worker_id(couchbase_config:bucket(), mode(), non_neg_integer()) ->
    couchbase_pool_worker:id().
get_next_worker_id(Bucket, Mode, StartNum) ->
    Key = {next_worker_id, Bucket, Mode},
    Size = get_size(Bucket, Mode),
    ets:update_counter(couchbase_pool_stats, Key, {2, 1, Size, 1},
        {Key, StartNum}).
