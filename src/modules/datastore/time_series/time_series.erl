%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal datastore API used for operating on time_series collections.
%%% Time series collection consists of several time series. Each time series consists of
%%% several metrics. Metric is set of windows that aggregate multiple
%%% measurements from particular period of time. E.g.,
%%% MyTimeSeriesCollection = #{
%%%    TimeSeries1 = #{
%%%       Metric1 = [Window1, Window2, ...],
%%%       Metric2 = ...
%%%    },
%%%    TimeSeries2 = ...
%%% }
%%% Window = {Timestamp, aggregator(Measurement1Value, Measurement2Value, ...)}
%%% See ts_windows:aggregate/3 to see possible aggregation functions.
%%%
%%% The module delegates operations on single metric to ts_metric module
%%% that is able to handle infinite number of windows inside single metric splitting
%%% windows set to subsets stored in multiple records that are saved to multiple
%%% datastore documents by ts_persistence helper module.
%%%
%%% All metrics from all time series are kept together. If any metric windows count
%%% is so high that it cannot be stored in single datastore document, the metric's
%%% windows set is divided into several records by ts_metric module and then
%%% persisted as several datastore documents by ts_persistence module. Thus,
%%% windows of each metric form linked list of records storing parts of windows set,
%%% with head of the list treated exceptionally (heads are kept together for all
%%% metrics while rest of records with windows are kept separately for each metric).
%%% @end
%%%-------------------------------------------------------------------
-module(time_series).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_time_series.hrl").
-include("modules/datastore/ts_metric_config.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/4, update/5, update/6, update_many/4, get/4, get/5, delete/3]).
%% Test API
-export([create_doc_splitting_strategies/1]).

-type time_series_id() :: binary().
-type collection_id() :: binary().

-type time_series() :: #{ts_metric:id() => ts_metric:metric()}.
-type collection() :: #{time_series_id() => time_series()}.
-type collection_config() :: #{time_series_id() => #{ts_metric:id() => ts_metric:config()}}.

% Metrics are stored in hierarchical map and time_series_id is used to get map
% #{ts_metric:id() => ts_metric:metric()} for particular time series.
% Other way of presentation is flattened map that uses keys {time_series_id(), ts_metric:id()}. It is used
% mainly to return requested data (get can return only chosen metrics so it does not return hierarchical map).
-type full_metric_id() :: {time_series_id(), ts_metric:id()}.
-type windows_map() :: #{full_metric_id() => [ts_windows:window()]}.
-type flat_config_map() :: #{full_metric_id() => ts_metric:config()}.
-type windows_count_map() :: #{full_metric_id() => non_neg_integer()}.

-type time_series_range() :: time_series_id() | [time_series_id()].
-type metrics_range() :: ts_metric:id() | [ts_metric:id()].
-type range() :: time_series_id() | {time_series_range(), metrics_range()}.
-type request_range() :: range() | [range()].
-type update_range() :: {request_range(), ts_windows:value()} | [{request_range(), ts_windows:value()}].

-export_type([collection_id/0, collection/0, collection_config/0, time_series_id/0, full_metric_id/0,
    request_range/0, update_range/0, windows_map/0]).

-type ctx() :: datastore:ctx().
-type batch() :: datastore_doc:batch().

% Warning: do not use this env in app.config (setting it to very high value can result in creation of
% datastore documents that are too big for couchbase). Use of env limited to tests.
-define(MAX_VALUES_IN_DOC, application:get_env(?CLUSTER_WORKER_APP_NAME, time_series_max_doc_size, 50000)).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(ctx(), collection_id(), collection_config(), batch()) -> {ok | {error, term()}, batch()}.
create(Ctx, Id, ConfigMap, Batch) ->
    try
        DocSplittingStrategies = create_doc_splitting_strategies(ConfigMap),

        TimeSeriesCollection = maps:map(fun(TimeSeriesId, MetricsConfigs) ->
            maps:map(fun(MetricsId, Config) ->
                 #metric{
                     config = Config,
                     splitting_strategy = maps:get({TimeSeriesId, MetricsId}, DocSplittingStrategies)
                 }
            end, MetricsConfigs)
        end, ConfigMap),

        PersistenceCtx = ts_persistence:init_for_new_collection(Ctx, Id, TimeSeriesCollection, Batch),
        {ok, ts_persistence:finalize(PersistenceCtx)}
    catch
        _:{error, to_many_metrics} ->
            {{error, to_many_metrics}, Batch};
        _:{error, empty_metric} ->
            {{error, empty_metric}, Batch};
        _:{error, wrong_resolution} ->
            {{error, wrong_resolution}, Batch};
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Time series collection ~p init error: ~p:~p~nConfig map: ~p",
                [Id, Error, Reason, ConfigMap], Stacktrace),
            {{error, create_failed}, Batch}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Puts metrics value for particular timestamp. It updates all metrics from all time series or only chosen subset
%% of metrics depending on value of 4th function argument. In second case different measurement values can be specified
%% (see update_range() type).
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), collection_id(), ts_windows:timestamp(), ts_windows:value() | update_range(), batch()) ->
    {ok | {error, term()}, batch()}.
update(Ctx, Id, NewTimestamp, NewValue, Batch) when is_number(NewValue) ->
    try
        {TimeSeriesCollection, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
        FinalPersistenceCtx = update_time_series(maps:to_list(TimeSeriesCollection), NewTimestamp, NewValue, PersistenceCtx),
        {ok, ts_persistence:finalize(FinalPersistenceCtx)}
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Time series collection ~p update error: ~p:~p~nFailed to update measurement {~p, ~p}",
                [Id, Error, Reason, NewTimestamp, NewValue], Stacktrace),
            {{error, update_failed}, Batch}
    end;

update(Ctx, Id, NewTimestamp, MetricsToUpdateWithValues, Batch) when is_list(MetricsToUpdateWithValues) ->
    try
        {TimeSeriesCollection, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
        FinalPersistenceCtx = lists:foldl(fun({MetricsToUpdate, NewValue}, Acc) ->
            FilteredTimeSeries = filter_collection(TimeSeriesCollection, MetricsToUpdate),
            update_time_series(maps:to_list(FilteredTimeSeries), NewTimestamp, NewValue, Acc)
        end, PersistenceCtx, MetricsToUpdateWithValues),
        {ok, ts_persistence:finalize(FinalPersistenceCtx)}
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Time series collection ~p update error: ~p:~p~nFailed to update values ~p for timestamp ~p",
                [Id, Error, Reason, MetricsToUpdateWithValues, NewTimestamp], Stacktrace),
            {{error, update_failed}, Batch}
    end;

update(Ctx, Id, NewTimestamp, {MetricsToUpdate, NewValue}, Batch) ->
    update(Ctx, Id, NewTimestamp, MetricsToUpdate, NewValue, Batch).


%%--------------------------------------------------------------------
%% @doc
%% Updates single metric.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), collection_id(), ts_windows:timestamp(), request_range() , ts_windows:value(), batch()) ->
    {ok | {error, term()}, batch()}.
update(Ctx, Id, NewTimestamp, MetricsToUpdate, NewValue, Batch) ->
    try
        {TimeSeriesCollection, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
        FilteredTimeSeries = filter_collection(TimeSeriesCollection, MetricsToUpdate),
        FinalPersistenceCtx = update_time_series(maps:to_list(FilteredTimeSeries), NewTimestamp, NewValue, PersistenceCtx),
        {ok, ts_persistence:finalize(FinalPersistenceCtx)}
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Time series collection ~p update error: ~p:~p~nFailed to update measurement {~p, ~p} for metrics ~p",
                [Id, Error, Reason, NewTimestamp, NewValue, MetricsToUpdate], Stacktrace),
            {{error, update_failed}, Batch}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Puts multiple measurements to all metrics from all time series.
%% Usage of this function allows reduction of datastore overhead.
%% @end
%%--------------------------------------------------------------------
-spec update_many(ctx(), collection_id(), [{ts_windows:timestamp(), ts_windows:value()}], batch()) ->
    {ok | {error, term()}, batch()}.
update_many(_Ctx, _Id, [], Batch) ->
    {ok, Batch};
update_many(Ctx, Id, [{NewTimestamp, NewValue} | Measurements], Batch) ->
    case update(Ctx, Id, NewTimestamp, NewValue, Batch) of
        {ok, UpdatedBatch} -> update_many(Ctx, Id, Measurements, UpdatedBatch);
        Other -> Other
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns windows for requested ranges. Windows for all metrics from all time series are included in answer.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), collection_id(), ts_windows:get_options(), batch() | undefined) ->
    {{ok, windows_map()} | {error, term()}, batch() | undefined}.
get(Ctx, Id, Options, Batch) ->
    try
        {TimeSeriesCollection, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
        FillMetricsIds = maps:fold(fun(TimeSeriesId, MetricsConfigs, Acc) ->
            maps:fold(fun(MetricsId, _Config, InternalAcc) ->
                [{TimeSeriesId, MetricsId} | InternalAcc]
            end, Acc, MetricsConfigs)
        end, [], TimeSeriesCollection),
        {Ans, FinalPersistenceCtx} = get_internal(TimeSeriesCollection, FillMetricsIds, Options, PersistenceCtx),
        {{ok, Ans}, ts_persistence:finalize(FinalPersistenceCtx)}
    catch
        Error:Reason:Stacktrace when Reason =/= {fetch_error, not_found} ->
            ?error_stacktrace("Time series collection ~p get error: ~p:~p~nOptions: ~p",
                [Id, Error, Reason, Options], Stacktrace),
            {{error, get_failed}, Batch}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns windows for requested ranges. If windows from single metric are requested, the function returns list of windows.
%% Otherwise, map containing list of windows for each requested metric is returned.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), collection_id(), request_range(), ts_windows:get_options(), batch() | undefined) ->
    {{ok, [ts_windows:window()] | windows_map()} | {error, term()}, batch() | undefined}.
get(Ctx, Id, RequestedMetrics, Options, Batch) ->
    try
        {TimeSeriesCollection, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
        {Ans, FinalPersistenceCtx} = get_internal(TimeSeriesCollection, RequestedMetrics, Options, PersistenceCtx),
        {{ok, Ans}, ts_persistence:finalize(FinalPersistenceCtx)}
    catch
        Error:Reason:Stacktrace when Reason =/= {fetch_error, not_found} ->
            ?error_stacktrace("Time series collection ~p get error: ~p:~p~nRequested metrics: ~p~nOptions: ~p",
                [Id, Error, Reason, RequestedMetrics, Options], Stacktrace),
            {{error, get_failed}, Batch}
    end.


-spec delete(ctx(), collection_id(), batch() ) -> {ok | {error, term()}, batch()}.
delete(Ctx, Id, Batch) ->
    try
        {TimeSeriesCollection, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
        FinalPersistenceCtx = delete(maps:to_list(TimeSeriesCollection), PersistenceCtx),
        {ok, ts_persistence:finalize(FinalPersistenceCtx)}
    catch
        Error:Reason:Stacktrace when Reason =/= {fetch_error, not_found} ->
            ?error_stacktrace("Time series collection ~p delete error: ~p:~p", [Id, Error, Reason], Stacktrace),
            {{error, delete_failed}, Batch}
    end.


%%=====================================================================
%% Internal functions
%%=====================================================================

-spec filter_collection(collection(), request_range()) -> collection().
filter_collection(_TimeSeriesCollection, []) ->
    #{};

filter_collection(TimeSeriesCollection, [{TimeSeriesId, MetricIds} | Tail]) ->
    Ans = filter_collection(TimeSeriesCollection, Tail),
    case maps:get(TimeSeriesId, TimeSeriesCollection, undefined) of
        undefined ->
            Ans;
        TimeSeries ->
            FilteredTimeSeries = lists:foldl(fun(MetricId, Acc) ->
                case maps:get(MetricId, TimeSeries, undefined) of
                    undefined -> Acc;
                    Metric -> maps:put(MetricId, Metric, Acc)
                end
            end, #{}, utils:ensure_list(MetricIds)),

            case maps:size(FilteredTimeSeries) of
                0 -> Ans;
                _ -> maps:put(TimeSeriesId, FilteredTimeSeries, Ans)
            end
    end;

filter_collection(TimeSeriesCollection, [TimeSeriesId | Tail]) ->
    Ans = filter_collection(TimeSeriesCollection, Tail),
    case maps:get(TimeSeriesId, TimeSeriesCollection, undefined) of
        undefined -> Ans;
        TimeSeries -> maps:put(TimeSeriesId, TimeSeries, Ans)
    end;

filter_collection(TimeSeriesCollection, ToBeIncluded) ->
    filter_collection(TimeSeriesCollection, [ToBeIncluded]).


-spec update_time_series([{time_series_id(), time_series()}], ts_windows:timestamp(),
    ts_windows:value(), ts_persistence:ctx()) -> ts_persistence:ctx().
update_time_series([], _NewTimestamp, _NewValue, PersistenceCtx) ->
    PersistenceCtx;

update_time_series([{TimeSeriesId, TimeSeries} | Tail], NewTimestamp, NewValue, PersistenceCtx) ->
    PersistenceCtxWithIdSet = ts_persistence:set_active_time_series(TimeSeriesId, PersistenceCtx),
    UpdatedPersistenceCtx = update_metrics(maps:to_list(TimeSeries), NewTimestamp, NewValue, PersistenceCtxWithIdSet),
    update_time_series(Tail, NewTimestamp, NewValue, UpdatedPersistenceCtx).


-spec update_metrics([{ts_metric:id(), ts_metric:metric()}], ts_windows:timestamp(),
    ts_windows:value(), ts_persistence:ctx()) -> ts_persistence:ctx().
update_metrics([], _NewTimestamp, _NewValue, PersistenceCtx) ->
    PersistenceCtx;
update_metrics([{MetricId, Metric} | Tail], NewTimestamp, NewValue, PersistenceCtx) ->
    UpdatedPersistenceCtx = ts_persistence:set_active_metric(MetricId, PersistenceCtx),
    FinalPersistenceCtx = ts_metric:update(Metric, NewTimestamp, NewValue, UpdatedPersistenceCtx),
    update_metrics(Tail, NewTimestamp, NewValue, FinalPersistenceCtx).


-spec get_internal(collection(), request_range(), ts_windows:get_options(), ts_persistence:ctx()) ->
    {[ts_windows:window()] | windows_map(), ts_persistence:ctx()}.
get_internal(_TimeSeriesCollection, [], _Options, PersistenceCtx) ->
    {#{}, PersistenceCtx};

get_internal(TimeSeriesCollection, [{TimeSeriesIds, MetricIds} | RequestedMetrics], Options, PersistenceCtx) ->
    {Ans, UpdatedPersistenceCtx} = lists:foldl(fun(TimeSeriesId, Acc) ->
        MetricsMap = maps:get(TimeSeriesId, TimeSeriesCollection, #{}),
        lists:foldl(fun(MetricId, {TmpAns, TmpPersistenceCtx}) ->
            {Values, UpdatedTmpPersistenceCtx} = case maps:get(MetricId, MetricsMap, undefined) of
                undefined -> {undefined, TmpPersistenceCtx};
                Metric -> ts_metric:get(Metric, Options, TmpPersistenceCtx)
            end,
            {TmpAns#{{TimeSeriesId, MetricId} => Values}, UpdatedTmpPersistenceCtx}
        end, Acc, utils:ensure_list(MetricIds))
    end, {#{}, PersistenceCtx}, utils:ensure_list(TimeSeriesIds)),

    {Ans2, FinalPersistenceCtx} = get_internal(TimeSeriesCollection, RequestedMetrics, Options, UpdatedPersistenceCtx),
    {maps:merge(Ans, Ans2), FinalPersistenceCtx};

get_internal(TimeSeriesCollection, [TimeSeriesId | RequestedMetrics], Options, PersistenceCtx) ->
    {Ans, UpdatedPersistenceCtx} = case maps:get(TimeSeriesId, TimeSeriesCollection, undefined) of
        undefined ->
            {#{TimeSeriesId => undefined}, PersistenceCtx};
        MetricsMap ->
            lists:foldl(fun({MetricId, Metric}, {TmpAns, TmpPersistenceCtx}) ->
                {Values, UpdatedTmpPersistenceCtx} = ts_metric:get(Metric, Options, TmpPersistenceCtx),
                {TmpAns#{{TimeSeriesId, MetricId} => Values}, UpdatedTmpPersistenceCtx}
            end, {#{}, PersistenceCtx}, maps:to_list(MetricsMap))
    end,

    {Ans2, FinalPersistenceCtx} = get_internal(TimeSeriesCollection, RequestedMetrics, Options, UpdatedPersistenceCtx),
    {maps:merge(Ans, Ans2), FinalPersistenceCtx};

get_internal(TimeSeriesCollection, Request, Options, PersistenceCtx) ->
    {Ans, FinalPersistenceCtx} = get_internal(TimeSeriesCollection, [Request], Options, PersistenceCtx),
    case maps:is_key(Request, Ans) of
        true ->
            % Single key is requested - return value for the key instead of map
            {maps:get(Request, Ans), FinalPersistenceCtx};
        false ->
            {Ans, FinalPersistenceCtx}
    end.


-spec delete([{time_series_id(), time_series()}], ts_persistence:ctx()) -> ts_persistence:ctx().
delete([], PersistenceCtx) ->
    ts_persistence:delete_hub(PersistenceCtx);
delete([{_TimeSeriesId, TimeSeries} | Tail], PersistenceCtx) ->
    UpdatedPersistenceCtx = delete_metric(maps:to_list(TimeSeries), PersistenceCtx),
    delete(Tail, UpdatedPersistenceCtx).


-spec delete_metric([{ts_metric:id(), ts_metric:metric()}], ts_persistence:ctx()) ->
    ts_persistence:ctx().
delete_metric([], PersistenceCtx) ->
    PersistenceCtx;
delete_metric([{_MetricId, Metric} | Tail], PersistenceCtx) ->
    UpdatedPersistenceCtx = ts_metric:delete(Metric, PersistenceCtx),
    delete_metric(Tail, UpdatedPersistenceCtx).


%%=====================================================================
%% Helper functions creating splitting strategy
%%=====================================================================

-spec create_doc_splitting_strategies(collection_config()) -> #{full_metric_id() => ts_metric:splitting_strategy()}.
create_doc_splitting_strategies(ConfigMap) ->
    FlattenedMap = maps:fold(fun(TimeSeriesId, MetricsConfigs, Acc) ->
        maps:fold(fun
            (_, #metric_config{retention = Retention}, _) when Retention =< 0 ->
                throw({error, empty_metric});
            (_, #metric_config{resolution = Resolution}, _) when Resolution =< 0 ->
                throw({error, wrong_resolution});
            (MetricsId, Config, InternalAcc) ->
                InternalAcc#{{TimeSeriesId, MetricsId} => Config}
        end, Acc, MetricsConfigs)
    end, #{}, ConfigMap),

    MaxValuesInDoc = ?MAX_VALUES_IN_DOC,
    MaxWindowsInHeadMap = calculate_windows_in_head_doc_count(FlattenedMap),
    maps:map(fun(Key, MaxWindowsInHead) ->
        #metric_config{retention = Retention} = maps:get(Key, FlattenedMap),
        {MaxWindowsInTailDoc, MaxDocsCount} = case MaxWindowsInHead of
            Retention ->
                % All windows can be stored in head
                {0, 1};
            _ ->
                % It is guaranteed that each tail document uses at least half of its capacity after split
                % so windows count should be multiplied by 2 when calculating number of tail documents
                % (head can be cleared to optimize upload so tail documents have to store required windows count).
                case {Retention =< MaxValuesInDoc, Retention =< MaxValuesInDoc div 2} of
                    {true, true} ->
                        {2 * Retention, 2};
                    {true, false} ->
                        {Retention, 3};
                    _ ->
                        {MaxValuesInDoc, 1 + ceil(2 * Retention / MaxValuesInDoc)}
                end
        end,
        #splitting_strategy{
            max_docs_count = MaxDocsCount,
            max_windows_in_head_doc = MaxWindowsInHead,
            max_windows_in_tail_doc = MaxWindowsInTailDoc
        }
    end, MaxWindowsInHeadMap).


-spec calculate_windows_in_head_doc_count(flat_config_map()) -> windows_count_map().
calculate_windows_in_head_doc_count(FlattenedMap) ->
    MaxValuesInHead = ?MAX_VALUES_IN_DOC,
    case maps:size(FlattenedMap) > MaxValuesInHead of
        true ->
            throw({error, to_many_metrics});
        false ->
            NotFullyStoredInHead = maps:map(fun(_, _) -> 0 end, FlattenedMap),
            calculate_windows_in_head_doc_count(#{}, NotFullyStoredInHead, MaxValuesInHead, FlattenedMap)

    end.


-spec calculate_windows_in_head_doc_count(windows_count_map(), windows_count_map(), non_neg_integer(),
    flat_config_map()) -> windows_count_map().
calculate_windows_in_head_doc_count(FullyStoredInHead, NotFullyStoredInHead, 0, _FlattenedMap) ->
    maps:merge(FullyStoredInHead, NotFullyStoredInHead);
calculate_windows_in_head_doc_count(FullyStoredInHead, NotFullyStoredInHead, RemainingWindowsInHead, FlattenedMap) ->
    LimitUpdate = max(1, RemainingWindowsInHead div maps:size(NotFullyStoredInHead)),
    {UpdatedFullyStoredInHead, UpdatedNotFullyStoredInHead, UpdatedRemainingWindowsInHead} =
        update_windows_in_head_doc_count(maps:keys(NotFullyStoredInHead), FullyStoredInHead, NotFullyStoredInHead,
            FlattenedMap, LimitUpdate, RemainingWindowsInHead),
    case maps:size(UpdatedNotFullyStoredInHead) of
        0 ->
            UpdatedFullyStoredInHead;
        _ ->
            calculate_windows_in_head_doc_count(UpdatedFullyStoredInHead, UpdatedNotFullyStoredInHead,
                UpdatedRemainingWindowsInHead, FlattenedMap)
    end.


-spec update_windows_in_head_doc_count([full_metric_id()], windows_count_map(), windows_count_map(), flat_config_map(),
    non_neg_integer(), non_neg_integer()) -> {windows_count_map(), windows_count_map(), non_neg_integer()}.
update_windows_in_head_doc_count(_MetricsKeys, FullyStoredInHead, NotFullyStoredInHead, _FlattenedMap, _LimitUpdate, 0) ->
    {FullyStoredInHead, NotFullyStoredInHead, 0};
update_windows_in_head_doc_count([], FullyStoredInHead, NotFullyStoredInHead, _FlattenedMap,
    _LimitUpdate, RemainingWindowsInHead) ->
    {FullyStoredInHead, NotFullyStoredInHead, RemainingWindowsInHead};
update_windows_in_head_doc_count([Key | MetricsKeys], FullyStoredInHead, NotFullyStoredInHead, FlattenedMap,
    LimitUpdate, RemainingWindowsInHead) ->
    Update = min(LimitUpdate, RemainingWindowsInHead),
    #metric_config{retention = Retention} = maps:get(Key, FlattenedMap),
    CurrentLimit = maps:get(Key, NotFullyStoredInHead),
    NewLimit = CurrentLimit + Update,
    {UpdatedFullyStoredInHead, UpdatedNotFullyStoredInHead, FinalUpdate} = case NewLimit >= Retention of
        true ->
            {FullyStoredInHead#{Key => Retention},
                maps:remove(Key, NotFullyStoredInHead), Retention - CurrentLimit};
        false ->
            {FullyStoredInHead, NotFullyStoredInHead#{Key => NewLimit}, Update}
    end,
    update_windows_in_head_doc_count(MetricsKeys, UpdatedFullyStoredInHead, UpdatedNotFullyStoredInHead, FlattenedMap,
        LimitUpdate, RemainingWindowsInHead - FinalUpdate).