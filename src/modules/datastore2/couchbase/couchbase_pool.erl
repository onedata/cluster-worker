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

%% API
-export([post_async/3, post/3, wait/1]).
-export([get_timeout/0, get_modes/0, get_size/2]).
-export([get_request_queue_size/1, get_request_queue_size/2,
    get_max_worker_queue_size/1, get_max_worker_queue_size/2,
    reset_request_queue_size/3, update_request_queue_size/4]).
-export([init_counters/0, init_report/0]).

-type mode() :: changes | read | write.
-type request() :: {save, couchbase_driver:ctx(), couchbase_driver:item()} |
                   {get, datastore:key()} |
                   {delete, couchbase_driver:ctx(), datastore:key()} |
                   {get_counter, datastore:key(), cberl:arithmetic_default()} |
                   {update_counter, datastore:key(), cberl:arithmetic_delta(),
                       cberl:arithmetic_default()} |
                   {save_design_doc, couchbase_driver:design(),
                       datastore_json2:ejson()} |
                   {get_design_doc, couchbase_driver:design()} |
                   {delete_design_doc, couchbase_driver:design()} |
                   {query_view, couchbase_driver:design(),
                       couchbase_driver:view(), [couchbase_driver:view_opt()]}.
-type response() :: ok | {ok, term()} | {ok, term(), term()} | {error, term()}.
-type future() :: reference().

-export_type([mode/0, request/0, response/0, future/0]).

-define(EXOMETER_NAME(Bucket, Mode), [db_queue_size, Bucket, Mode]).
-define(EXOMETER_DEFAULT_LOGGING_INTERVAL, 60000).

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
    lists:foreach(fun(Bucket) ->
        lists:foreach(fun(Mode) ->
            init_counter(Bucket, Mode)
        end, get_modes())
    end, couchbase_config:get_buckets()).

%%--------------------------------------------------------------------
%% @doc
%% Sets exometer report connected with counters used by this module.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    lists:foreach(fun(Bucket) ->
        lists:foreach(fun(Mode) ->
            init_report(Bucket, Mode)
        end, get_modes())
    end, couchbase_config:get_buckets()).

%%--------------------------------------------------------------------
%% @doc
%% Schedules request execution on a worker pool.
%% @end
%%--------------------------------------------------------------------
-spec post_async(couchbase_config:bucket(), mode(), request()) -> future().
post_async(Bucket, Mode, Request) ->
    Ref = make_ref(),
    Id = get_next_worker_id(Bucket, Mode),
    Worker = couchbase_pool_sup:get_worker(Bucket, Mode, Id),
    update_request_queue_size(Bucket, Mode, Id, 1),
    Worker ! {post, {Ref, self(), Request}},
    Ref.

%%--------------------------------------------------------------------
%% @doc
%% Schedules request execution on a worker pool and awaits response.
%% @end
%%--------------------------------------------------------------------
-spec post(couchbase_config:bucket(), mode(), request()) -> response().
post(Bucket, Mode, Request) ->
    wait(post_async(Bucket, Mode, Request)).

%%--------------------------------------------------------------------
%% @doc
%% Waits for response associated with a reference.
%% @end
%%--------------------------------------------------------------------
-spec wait(future()) -> response().
wait(Future) ->
    Timeout = get_timeout(),
    receive
        {Future, Response} -> Response
    after
        Timeout -> {error, timeout}
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
%% Returns maximal queue size for given bucket and all modes.
%% @end
%%--------------------------------------------------------------------
-spec get_max_worker_queue_size(couchbase_config:bucket()) -> non_neg_integer().
get_max_worker_queue_size(Bucket) ->
    lists:foldl(fun(Mode, Size) ->
        max(Size, get_max_worker_queue_size(Bucket, Mode))
    end, 0, get_modes()).

%%--------------------------------------------------------------------
%% @doc
%% Returns maximal queue size for given bucket and mode.
%% @end
%%--------------------------------------------------------------------
-spec get_max_worker_queue_size(couchbase_config:bucket(), mode()) ->
    non_neg_integer().
get_max_worker_queue_size(Bucket, Mode) ->
    lists:foldl(fun(Id, Size) ->
        Key = {request_queue_size, Bucket, Mode, Id},
        ModeSize = ets:lookup_element(couchbase_pool_stats, Key, 2),
        ok = exometer:update(?EXOMETER_NAME(Bucket, Mode), ModeSize),
        max(Size, ModeSize)
    end, 0, lists:seq(1, get_size(Bucket, Mode))).

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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns next worker ID.
%% @end
%%--------------------------------------------------------------------
-spec get_next_worker_id(couchbase_config:bucket(), mode()) ->
    couchbase_pool_worker:id().
get_next_worker_id(Bucket, Mode) ->
    Key = {next_worker_id, Bucket, Mode},
    Size = get_size(Bucket, Mode),
    ets:update_counter(couchbase_pool_stats, Key, {2, 1, Size, 1}, {Key, 0}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes exometer counter.
%% @end
%%--------------------------------------------------------------------
-spec init_counter(Bucket :: atom(), Mode :: atom()) -> ok.
init_counter(Bucket, Mode) ->
    Name = ?EXOMETER_NAME(Bucket, Mode),
    exometer:new(Name, histogram, [{time_span,
        application:get_env(?CLUSTER_WORKER_APP_NAME, exometer_pool_time_span, 600000)}]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets exometer report connected with particular counter.
%% @end
%%--------------------------------------------------------------------
-spec init_report(Bucket :: atom(), Mode :: atom()) -> ok.
init_report(Bucket, Mode) ->
    Name = ?EXOMETER_NAME(Bucket, Mode),
    exometer_report:subscribe(exometer_report_lager, Name,
        [min, max, median, mean],
        application:get_env(?CLUSTER_WORKER_APP_NAME,
            exometer_logging_interval, ?EXOMETER_DEFAULT_LOGGING_INTERVAL)),

    exometer_report:subscribe(exometer_report_graphite, Name,
        [min, max, median, mean],
        application:get_env(?CLUSTER_WORKER_APP_NAME,
            exometer_logging_interval, ?EXOMETER_DEFAULT_LOGGING_INTERVAL)).