%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal datastore API used for operating on time series collections.
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
%%% Window = {WindowTimestamp, aggregator(PrevAggregatedValue, MeasurementValue)} where
%%% PrevAggregatedValue is result of previous aggregator function executions
%%% (window can be created using several measurements).
%%% See ts_windows:insert_value/4 to see possible aggregation functions.
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
-module(time_series_collection).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_time_series.hrl").
-include("modules/datastore/ts_metric_config.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/4, add_metrics/5, delete_metrics/4, list_time_series_ids/3, list_metrics_by_time_series/3,
    update/5, update/6, update_many/4, list_windows/4, list_windows/5, delete/3]).

-type time_series_id() :: binary().
-type collection_id() :: binary().
-type collection_config() :: #{time_series_id() => #{ts_metric:id() => ts_metric:config()}}.

% Metrics are stored in hierarchical map and time_series_id is used to get map
% #{ts_metric:id() => ts_metric:metric()} for particular time series.
% Other way of presentation is flattened map that uses keys {time_series_id(), ts_metric:id()}. It is used
% mainly to return requested data (get can return only chosen metrics so it does not return hierarchical map).
-type full_metric_id() :: {time_series_id(), ts_metric:id()}.
-type windows_map() :: #{full_metric_id() => ts_windows:descending_windows_list()}.
-type metrics_by_time_series() :: #{time_series_id() => [ts_metric:id()]}.

-type time_series_range() :: time_series_id() | [time_series_id()].
-type metrics_range() :: ts_metric:id() | [ts_metric:id()].
-type range() :: time_series_id() | {time_series_range(), metrics_range()}.
-type request_range() :: range() | [range()].
-type update_range() :: {request_range(), ts_windows:value()} | [{request_range(), ts_windows:value()}].

-type add_metrics_option() :: #{
    time_series_conflict_resulution_strategy => merge | override | fail,
    metric_conflict_resulution_strategy => override | fail
}.

-export_type([collection_id/0, collection_config/0, time_series_id/0, full_metric_id/0,
    request_range/0, update_range/0, windows_map/0, metrics_by_time_series/0, add_metrics_option/0]).

-type ctx() :: datastore:ctx().
-type batch() :: datastore_doc:batch().

-define(CATCH_LIST_UNEXPECTED_ERRORS(Expr, ErrorLog, ErrorLogArgs, ErrorReturnValue),
    try
        Expr
    catch
        throw:{{error, hub_not_found}, Batch} ->
            {{error, not_found}, Batch};
        Error:Reason:Stacktrace when Reason =/= {fetch_error, not_found} ->
            ?error_stacktrace(ErrorLog ++ "~nerror type: ~p, error reason: ~p",
                ErrorLogArgs ++ [Error, Reason], Stacktrace),
            ErrorReturnValue
    end
).

-define(CATCH_UPDATE_UNEXPECTED_ERRORS(Expr, ErrorLog, ErrorLogArgs, ErrorReturnValue),
    try
        Expr
    catch
        throw:{{error, hub_not_found}, Batch} ->
            {{error, not_found}, Batch};
        Error:Reason:Stacktrace ->
            ?error_stacktrace(ErrorLog ++ "~nerror type: ~p, error reason: ~p",
                ErrorLogArgs ++ [Error, Reason], Stacktrace),
            ErrorReturnValue
    end
).


%%%===================================================================
%%% API
%%%===================================================================

-spec create(ctx(), collection_id(), collection_config(), batch()) -> {ok | {error, term()}, batch()}.
create(Ctx, Id, ConfigMap, Batch) ->
    try
        DocSplittingStrategies = ts_doc_splitting_strategies:calculate(ConfigMap),

        TimeSeriesCollectionHeads = maps:map(fun(TimeSeriesId, MetricsConfigs) ->
            maps:map(fun(MetricId, Config) ->
                ts_metric:init(Config, maps:get({TimeSeriesId, MetricId}, DocSplittingStrategies))
            end, MetricsConfigs)
        end, ConfigMap),

        case ts_persistence:init_for_new_collection(Ctx, Id, TimeSeriesCollectionHeads, Batch) of
            {{error, already_exists}, UpdatedBatch} -> {{error, collection_already_exists}, UpdatedBatch};
            PersistenceCtx -> {ok, ts_persistence:finalize(PersistenceCtx)}
        end
    catch
        _:{error, too_many_metrics} ->
            {{error, too_many_metrics}, Batch};
        _:{error, empty_metric} ->
            {{error, empty_metric}, Batch};
        _:{error, wrong_resolution} ->
            {{error, wrong_resolution}, Batch};
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Time series collection ~p init error: ~p:~p~nConfig map: ~p",
                [Id, Error, Reason, ConfigMap], Stacktrace),
            {{error, create_failed}, Batch}
    end.


-spec add_metrics(ctx(), collection_id(), collection_config(), add_metrics_option(), batch()) -> 
    {ok | {error, term()}, batch()}.
add_metrics(Ctx, Id, ConfigMapExtension, Options, Batch) ->
    try
        {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
            
        ExistingConfigMap = get_config_map(TimeSeriesCollectionHeads),
        NewConfigMap = merge_config_maps(ExistingConfigMap, ConfigMapExtension, Options),
        NewDocSplittingStrategies = ts_doc_splitting_strategies:calculate(NewConfigMap),
            
        UpdatedPersistenceCtx = delete_overridden_metrics(
            TimeSeriesCollectionHeads, ExistingConfigMap, ConfigMapExtension, Options, PersistenceCtx),
        FinalPersistenceCtx = reconfigure_metrics(
            NewConfigMap, ConfigMapExtension, NewDocSplittingStrategies, UpdatedPersistenceCtx),
            
        {ok, ts_persistence:finalize(FinalPersistenceCtx)}
    catch
        _:{error, too_many_metrics} ->
            {{error, too_many_metrics}, Batch};
        _:{error, empty_metric} ->
            {{error, empty_metric}, Batch};
        _:{error, wrong_resolution} ->
            {{error, wrong_resolution}, Batch};
        _:{error, time_series_already_exists} ->
            {{error, time_series_already_exists}, Batch};
        _:{error, metric_already_exists} ->
            {{error, metric_already_exists}, Batch};
        throw:{{error, hub_not_found}, UpdatedBatch} ->
            {{error, not_found}, UpdatedBatch};
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Adding metrics to time series collection ~p error: ~p:~p~nConfig map: ~p~nOptions: ~p",
                [Id, Error, Reason, ConfigMapExtension, Options], Stacktrace),
            {{error, add_metrics_failed}, Batch}
    end.


-spec delete_metrics(ctx(), collection_id(), request_range(), batch()) -> {ok | {error, term()}, batch()}.
delete_metrics(Ctx, Id, MetricsToDelete, Batch) ->
    ?CATCH_UPDATE_UNEXPECTED_ERRORS(
        begin
            {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
            SelectedHeads = select_heads(TimeSeriesCollectionHeads, MetricsToDelete),
            PersistenceCtxAfterDelete = delete_time_series(maps:to_list(SelectedHeads), false, PersistenceCtx),

            UpdatedTimeSeriesCollectionHeads = ts_persistence:get_time_series_collection_heads(PersistenceCtxAfterDelete),
            ConfigMap = get_config_map(UpdatedTimeSeriesCollectionHeads),
            NewDocSplittingStrategies = ts_doc_splitting_strategies:calculate(ConfigMap),
            FinalPersistenceCtx = reconfigure_metrics(ConfigMap, #{}, NewDocSplittingStrategies, PersistenceCtxAfterDelete),

            {ok, ts_persistence:finalize(FinalPersistenceCtx)}
        end,
        "Time series collection ~p delete metrics error. Failed to delete metrics ~p",
        [Id, MetricsToDelete], {{error, delete_metrics_failed}, Batch}
    ).


-spec list_time_series_ids(ctx(), collection_id(), batch()) -> {{ok, [time_series_id()]} | {error, term()}, batch()}.
list_time_series_ids(Ctx, Id, Batch) ->
    ?CATCH_LIST_UNEXPECTED_ERRORS(
        begin
            {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
            TimeSeriesIds = maps:keys(TimeSeriesCollectionHeads),
            {{ok, TimeSeriesIds}, ts_persistence:finalize(PersistenceCtx)}
        end,
        "Error listing ids of time series in collection ~p", [Id], {{error, list_failed}, Batch}
    ).


-spec list_metrics_by_time_series(ctx(), collection_id(), batch()) -> {{ok, metrics_by_time_series()} | {error, term()}, batch()}.
list_metrics_by_time_series(Ctx, Id, Batch) ->
    ?CATCH_LIST_UNEXPECTED_ERRORS(
        begin
            {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
            MetricIds = maps:map(fun(_TimeSeriesId, TimeSeriesHeads) ->
                maps:keys(TimeSeriesHeads)
            end, TimeSeriesCollectionHeads),
            {{ok, MetricIds}, ts_persistence:finalize(PersistenceCtx)}
        end,
        "Error listing ids of metrics in collection ~p", [Id], {{error, list_failed}, Batch}
    ).


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
    ?CATCH_UPDATE_UNEXPECTED_ERRORS(
        begin
            {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
            FinalPersistenceCtx = update_time_series(
                maps:to_list(TimeSeriesCollectionHeads), NewTimestamp, NewValue, PersistenceCtx),
            {ok, ts_persistence:finalize(FinalPersistenceCtx)}
        end,
        "Time series collection ~p update error. Failed to update measurement {~p, ~p}",
                [Id, NewTimestamp, NewValue], {{error, update_failed}, Batch}
    );

update(Ctx, Id, NewTimestamp, MetricsToUpdateWithValues, Batch) when is_list(MetricsToUpdateWithValues) ->
    ?CATCH_UPDATE_UNEXPECTED_ERRORS(
        begin
            {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
            FinalPersistenceCtx = lists:foldl(fun({MetricsToUpdate, NewValue}, Acc) ->
                SelectedHeads = select_heads(TimeSeriesCollectionHeads, MetricsToUpdate),
                update_time_series(maps:to_list(SelectedHeads), NewTimestamp, NewValue, Acc)
            end, PersistenceCtx, MetricsToUpdateWithValues),
            {ok, ts_persistence:finalize(FinalPersistenceCtx)}
        end,
        "Time series collection ~p update error. Failed to update values ~p for timestamp ~p",
                [Id, MetricsToUpdateWithValues, NewTimestamp], {{error, update_failed}, Batch}
    );

update(Ctx, Id, NewTimestamp, {MetricsToUpdate, NewValue}, Batch) ->
    update(Ctx, Id, NewTimestamp, MetricsToUpdate, NewValue, Batch).


%%--------------------------------------------------------------------
%% @doc
%% Puts metrics value for particular timestamp. Updated value is the same for all metrics provided in 4th argument.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), collection_id(), ts_windows:timestamp(), request_range() , ts_windows:value(), batch()) ->
    {ok | {error, term()}, batch()}.
update(Ctx, Id, NewTimestamp, MetricsToUpdate, NewValue, Batch) ->
    ?CATCH_UPDATE_UNEXPECTED_ERRORS(
        begin
            {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
            SelectedHeads = select_heads(TimeSeriesCollectionHeads, MetricsToUpdate),
            FinalPersistenceCtx = update_time_series(maps:to_list(SelectedHeads), NewTimestamp, NewValue, PersistenceCtx),
            {ok, ts_persistence:finalize(FinalPersistenceCtx)}
        end,
        "Time series collection ~p update error. Failed to update measurement {~p, ~p} for metrics ~p",
                [Id, NewTimestamp, NewValue, MetricsToUpdate], {{error, update_failed}, Batch}
    ).


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
%% Returns windows according options provided in 4th argument.
%% Windows for all metrics from all time series are included in answer.
%% @end
%%--------------------------------------------------------------------
-spec list_windows(ctx(), collection_id(), ts_windows:list_options(), batch() | undefined) ->
    {{ok, windows_map()} | {error, term()}, batch() | undefined}.
list_windows(Ctx, Id, Options, Batch) ->
    ?CATCH_LIST_UNEXPECTED_ERRORS(
        begin
            {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
            FillMetricIds = maps:fold(fun(TimeSeriesId, MetricsConfigs, Acc) ->
                maps:fold(fun(MetricId, _Config, InternalAcc) ->
                    [{TimeSeriesId, MetricId} | InternalAcc]
                end, Acc, MetricsConfigs)
            end, [], TimeSeriesCollectionHeads),
            {Ans, FinalPersistenceCtx} = list_time_series_windows(
                TimeSeriesCollectionHeads, FillMetricIds, Options, PersistenceCtx),
            {{ok, Ans}, ts_persistence:finalize(FinalPersistenceCtx)}
        end,
        "Time series collection ~p list error~nOptions: ~p", [Id, Options], {{error, list_failed}, Batch}
    ).


%%--------------------------------------------------------------------
%% @doc
%% Returns windows according options provided in 4th argument.
%% If windows from single metric are requested, the function returns list of windows.
%% Otherwise, map containing list of windows for each requested metric is returned.
%% @end
%%--------------------------------------------------------------------
-spec list_windows(ctx(), collection_id(), request_range(), ts_windows:list_options(), batch() | undefined) ->
    {{ok, ts_windows:descending_windows_list() | windows_map()} | {error, term()}, batch() | undefined}.
list_windows(Ctx, Id, RequestedMetrics, Options, Batch) ->
    ?CATCH_LIST_UNEXPECTED_ERRORS(
        begin
            {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
            {Ans, FinalPersistenceCtx} = list_time_series_windows(
                TimeSeriesCollectionHeads, RequestedMetrics, Options, PersistenceCtx),
            {{ok, Ans}, ts_persistence:finalize(FinalPersistenceCtx)}
        end,
        "Time series collection ~p list error~nRequested metrics: ~p~nOptions: ~p",
        [Id, RequestedMetrics, Options], {{error, list_failed}, Batch}
    ).


-spec delete(ctx(), collection_id(), batch() ) -> {ok | {error, term()}, batch()}.
delete(Ctx, Id, Batch) ->
    ?CATCH_UPDATE_UNEXPECTED_ERRORS(
        begin
            {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
            FinalPersistenceCtx = delete_time_series(maps:to_list(TimeSeriesCollectionHeads), true, PersistenceCtx),
            {ok, ts_persistence:finalize(FinalPersistenceCtx)}
        end,
        "Time series collection ~p delete error.", [Id], {{error, delete_failed}, Batch}
    ).


%%=====================================================================
%% Internal functions
%%=====================================================================

-spec merge_config_maps(collection_config(), collection_config(), add_metrics_option()) -> collection_config().
merge_config_maps(ExistingConfigMap, ConfigMapExtension, Options) ->
    TimeSeriesConflictResolution = maps:get(time_series_conflict_resulution_strategy, Options, merge),
    MetricConflictResolution = maps:get(metric_conflict_resulution_strategy, Options, fail),
    maps:fold(fun(TimeSeriesId, MetricsConfigs, Acc) ->
        case maps:get(TimeSeriesId, Acc, undefined) of
            undefined ->
                Acc#{TimeSeriesId => MetricsConfigs};
            _ExistingMetricsConfigs when TimeSeriesConflictResolution =:= override ->
                Acc#{TimeSeriesId => MetricsConfigs};
            _ExistingMetricsConfigs when TimeSeriesConflictResolution =:= fail ->
                throw({error, time_series_already_exists});
            ExistingMetricsConfigs when TimeSeriesConflictResolution =:= merge ->
                MergedMetricsConfigs = maps:fold(fun
                    (MetricId, Config, InternalAcc) ->
                        case maps:is_key(MetricId, InternalAcc) of
                            true when MetricConflictResolution =:= fail -> throw({error, metric_already_exists});
                            true when MetricConflictResolution =:= override -> InternalAcc#{MetricId => Config};
                            false -> InternalAcc#{MetricId => Config}
                        end
                end, ExistingMetricsConfigs, MetricsConfigs),
                Acc#{TimeSeriesId => MergedMetricsConfigs}
        end
    end, ExistingConfigMap, ConfigMapExtension).


-spec delete_overridden_metrics(ts_hub:time_series_collection_heads(), collection_config(), collection_config(), 
    add_metrics_option(), ts_persistence:ctx()) -> ts_persistence:ctx().
delete_overridden_metrics(TimeSeriesCollectionHeads, ExistingConfigMap, ConfigMapExtension, Options, PersistenceCtx) ->
    TimeSeriesConflictResolution = maps:get(time_series_conflict_resulution_strategy, Options, merge),
    MetricsToDelete = maps:fold(fun(TimeSeriesId, TimeSeriesConfigMapExtension, Acc) ->
        case maps:get(TimeSeriesId, ExistingConfigMap, undefined) of
            undefined ->
                Acc;
            _ExistingMetricsConfigs when TimeSeriesConflictResolution =:= override ->
                [TimeSeriesId | Acc];
            ExistingMetricsConfigs when TimeSeriesConflictResolution =:= merge ->
                lists:foldl(fun(MetricId, InternalAcc) ->
                    case maps:is_key(MetricId, ExistingMetricsConfigs) of
                        true -> [{TimeSeriesId, MetricId} | InternalAcc];
                        false -> InternalAcc
                    end
                end, Acc, maps:keys(TimeSeriesConfigMapExtension))
        end
    end, [], ConfigMapExtension),

    SelectedHeads = select_heads(TimeSeriesCollectionHeads, MetricsToDelete),
    delete_time_series(maps:to_list(SelectedHeads), false, PersistenceCtx).


-spec reconfigure_metrics(collection_config(), collection_config(),
    ts_doc_splitting_strategies:splitting_strategies_map(), ts_persistence:ctx()) -> ts_persistence:ctx().
reconfigure_metrics(NewConfigMap, ConfigMapExtension, DocSplittingStrategies, PersistenceCtx) ->
    maps:fold(fun(TimeSeriesId, MetricsConfigs, PersistenceCtxAcc) ->
        UpdatedPersistenceCtxAcc = ts_persistence:set_currently_processed_time_series(TimeSeriesId, PersistenceCtxAcc),
        TimeSeriesConfigMapExtension = maps:get(TimeSeriesId, ConfigMapExtension, #{}),
        maps:fold(fun(MetricId, Config, InternalPersistenceCtx) ->
            UpdatedInternalPersistenceCtx = ts_persistence:set_currently_processed_metric(
                MetricId, InternalPersistenceCtx),
            case maps:get(MetricId, TimeSeriesConfigMapExtension, undefined) of
                undefined ->
                    CurrentMetric = ts_persistence:get_currently_processed_metric(UpdatedInternalPersistenceCtx),
                    ts_metric:reconfigure(CurrentMetric, maps:get({TimeSeriesId, MetricId}, DocSplittingStrategies),
                        UpdatedInternalPersistenceCtx);
                Config ->
                    Metric = ts_metric:init(Config, maps:get({TimeSeriesId, MetricId}, DocSplittingStrategies)),
                    ts_persistence:insert_metric(Metric, UpdatedInternalPersistenceCtx)
            end
        end, UpdatedPersistenceCtxAcc, MetricsConfigs)
    end, PersistenceCtx, NewConfigMap).


-spec select_heads(ts_hub:time_series_collection_heads(), request_range()) ->
    ts_hub:time_series_collection_heads().
select_heads(_TimeSeriesCollectionHeads, []) ->
    #{};

select_heads(TimeSeriesCollectionHeads, [{TimeSeriesId, MetricIds} | Tail]) ->
    Ans = select_heads(TimeSeriesCollectionHeads, Tail),
    case maps:get(TimeSeriesId, TimeSeriesCollectionHeads, undefined) of
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

select_heads(TimeSeriesCollectionHeads, [TimeSeriesId | Tail]) ->
    Ans = select_heads(TimeSeriesCollectionHeads, Tail),
    case maps:get(TimeSeriesId, TimeSeriesCollectionHeads, undefined) of
        undefined -> Ans;
        TimeSeries -> maps:put(TimeSeriesId, TimeSeries, Ans)
    end;

select_heads(TimeSeriesCollectionHeads, ToBeIncluded) ->
    select_heads(TimeSeriesCollectionHeads, [ToBeIncluded]).


-spec get_config_map(ts_hub:time_series_collection_heads()) -> collection_config().
get_config_map(TimeSeriesCollectionHeads) ->
    maps:map(fun(_TimeSeriesId, TimeSeriesHeads) ->
        maps:map(fun(_MetricId, #metric{config = Config}) ->
            Config
        end, TimeSeriesHeads)
    end, TimeSeriesCollectionHeads).


-spec update_time_series([{time_series_id(), ts_hub:time_series_heads()}], ts_windows:timestamp(),
    ts_windows:value(), ts_persistence:ctx()) -> ts_persistence:ctx().
update_time_series([], _NewTimestamp, _NewValue, PersistenceCtx) ->
    PersistenceCtx;

update_time_series([{TimeSeriesId, TimeSeries} | Tail], NewTimestamp, NewValue, PersistenceCtx) ->
    PersistenceCtxWithIdSet = ts_persistence:set_currently_processed_time_series(TimeSeriesId, PersistenceCtx),
    UpdatedPersistenceCtx = update_metrics(maps:to_list(TimeSeries), NewTimestamp, NewValue, PersistenceCtxWithIdSet),
    update_time_series(Tail, NewTimestamp, NewValue, UpdatedPersistenceCtx).


-spec update_metrics([{ts_metric:id(), ts_metric:metric()}], ts_windows:timestamp(),
    ts_windows:value(), ts_persistence:ctx()) -> ts_persistence:ctx().
update_metrics([], _NewTimestamp, _NewValue, PersistenceCtx) ->
    PersistenceCtx;
update_metrics([{MetricId, Metric} | Tail], NewTimestamp, NewValue, PersistenceCtx) ->
    UpdatedPersistenceCtx = ts_persistence:set_currently_processed_metric(MetricId, PersistenceCtx),
    FinalPersistenceCtx = ts_metric:update(Metric, NewTimestamp, NewValue, UpdatedPersistenceCtx),
    update_metrics(Tail, NewTimestamp, NewValue, FinalPersistenceCtx).


-spec list_time_series_windows(ts_hub:time_series_collection_heads(), request_range(), ts_windows:list_options(),
    ts_persistence:ctx()) -> {ts_windows:descending_windows_list() | windows_map(), ts_persistence:ctx()}.
list_time_series_windows(_TimeSeriesCollectionHeads, [], _Options, PersistenceCtx) ->
    {#{}, PersistenceCtx};

list_time_series_windows(TimeSeriesCollectionHeads, [{TimeSeriesIds, MetricIds} | RequestedMetrics], Options, PersistenceCtx) ->
    {Ans, UpdatedPersistenceCtx} = lists:foldl(fun(TimeSeriesId, Acc) ->
        MetricsMap = maps:get(TimeSeriesId, TimeSeriesCollectionHeads, #{}),
        lists:foldl(fun(MetricId, {TmpAns, TmpPersistenceCtx}) ->
            {Values, UpdatedTmpPersistenceCtx} = case maps:get(MetricId, MetricsMap, undefined) of
                undefined -> {undefined, TmpPersistenceCtx};
                Metric -> ts_metric:list_windows(Metric, Options, TmpPersistenceCtx)
            end,
            {TmpAns#{{TimeSeriesId, MetricId} => Values}, UpdatedTmpPersistenceCtx}
        end, Acc, utils:ensure_list(MetricIds))
    end, {#{}, PersistenceCtx}, utils:ensure_list(TimeSeriesIds)),

    {Ans2, FinalPersistenceCtx} = list_time_series_windows(
        TimeSeriesCollectionHeads, RequestedMetrics, Options, UpdatedPersistenceCtx),
    {maps:merge(Ans, Ans2), FinalPersistenceCtx};

list_time_series_windows(TimeSeriesCollectionHeads, [TimeSeriesId | RequestedMetrics], Options, PersistenceCtx) ->
    {Ans, UpdatedPersistenceCtx} = case maps:get(TimeSeriesId, TimeSeriesCollectionHeads, undefined) of
        undefined ->
            {#{TimeSeriesId => undefined}, PersistenceCtx};
        MetricsMap ->
            lists:foldl(fun({MetricId, Metric}, {TmpAns, TmpPersistenceCtx}) ->
                {Values, UpdatedTmpPersistenceCtx} = ts_metric:list_windows(Metric, Options, TmpPersistenceCtx),
                {TmpAns#{{TimeSeriesId, MetricId} => Values}, UpdatedTmpPersistenceCtx}
            end, {#{}, PersistenceCtx}, maps:to_list(MetricsMap))
    end,

    {Ans2, FinalPersistenceCtx} = list_time_series_windows(TimeSeriesCollectionHeads, RequestedMetrics, Options, UpdatedPersistenceCtx),
    {maps:merge(Ans, Ans2), FinalPersistenceCtx};

list_time_series_windows(TimeSeriesCollectionHeads, Request, Options, PersistenceCtx) ->
    {Ans, FinalPersistenceCtx} = list_time_series_windows(TimeSeriesCollectionHeads, [Request], Options, PersistenceCtx),
    case maps:is_key(Request, Ans) of
        true ->
            % Single key is requested - return value for the key instead of map
            {maps:get(Request, Ans), FinalPersistenceCtx};
        false ->
            {Ans, FinalPersistenceCtx}
    end.


-spec delete_time_series([{time_series_id(), ts_hub:time_series_heads()}], boolean(), ts_persistence:ctx()) -> 
    ts_persistence:ctx().
delete_time_series([], true = _DeleteHub, PersistenceCtx) ->
    ts_persistence:delete_hub(PersistenceCtx);
delete_time_series([], false = _DeleteHub, PersistenceCtx) ->
    PersistenceCtx;
delete_time_series([{TimeSeriesId, TimeSeries} | Tail], DeleteHub, PersistenceCtx) ->
    UpdatedPersistenceCtx = ts_persistence:set_currently_processed_time_series(TimeSeriesId, PersistenceCtx),
    UpdatedPersistenceCtx2 = delete_metrics(maps:to_list(TimeSeries), UpdatedPersistenceCtx),
    delete_time_series(Tail, DeleteHub, UpdatedPersistenceCtx2).


-spec delete_metrics([{ts_metric:id(), ts_metric:metric()}], ts_persistence:ctx()) ->
    ts_persistence:ctx().
delete_metrics([], PersistenceCtx) ->
    PersistenceCtx;
delete_metrics([{MetricId, Metric} | Tail], PersistenceCtx) ->
    UpdatedPersistenceCtx = ts_persistence:set_currently_processed_metric(MetricId, PersistenceCtx),
    UpdatedPersistenceCtx2 = ts_metric:delete_data_nodes(Metric, UpdatedPersistenceCtx),
    UpdatedPersistenceCtx3 = ts_persistence:delete_metric(UpdatedPersistenceCtx2),
    delete_metrics(Tail, UpdatedPersistenceCtx3).