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
%%% Window = {WindowTimestamp, aggregator(PrevAggregatedValue, MeasurementValue)} where
%%% PrevAggregatedValue is result of previous aggregator function executions
%%% (window can be created using several measurements).
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
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/4, update/5, update/6, update_many/4, list/4, list/5, delete/3]).

-type time_series_id() :: binary().
-type collection_id() :: binary().
-type collection_config() :: #{time_series_id() => #{ts_metric:id() => ts_metric:config()}}.

% Metrics are stored in hierarchical map and time_series_id is used to get map
% #{ts_metric:id() => ts_metric:metric()} for particular time series.
% Other way of presentation is flattened map that uses keys {time_series_id(), ts_metric:id()}. It is used
% mainly to return requested data (get can return only chosen metrics so it does not return hierarchical map).
-type full_metric_id() :: {time_series_id(), ts_metric:id()}.
-type windows_map() :: #{full_metric_id() => [ts_windows:window()]}.

-type time_series_range() :: time_series_id() | [time_series_id()].
-type metrics_range() :: ts_metric:id() | [ts_metric:id()].
-type range() :: time_series_id() | {time_series_range(), metrics_range()}.
-type request_range() :: range() | [range()].
-type update_range() :: {request_range(), ts_windows:value()} | [{request_range(), ts_windows:value()}].

-export_type([collection_id/0, collection_config/0, time_series_id/0, full_metric_id/0,
    request_range/0, update_range/0, windows_map/0]).

-type ctx() :: datastore:ctx().
-type batch() :: datastore_doc:batch().

-define(CATCH_EXCEPT_FETCH_ERROR(Expr, ErrorLog, ErrorLogArgs, ErrorReturnValue),
    try
        Expr
    catch
        Error:Reason:Stacktrace when Reason =/= {fetch_error, not_found} ->
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

        TimeSeriesHeadsCollection = maps:map(fun(TimeSeriesId, MetricsConfigs) ->
            maps:map(fun(MetricsId, Config) ->
                 #metric{
                     config = Config,
                     splitting_strategy = maps:get({TimeSeriesId, MetricsId}, DocSplittingStrategies)
                 }
            end, MetricsConfigs)
        end, ConfigMap),

        PersistenceCtx = ts_persistence:init_for_new_collection(Ctx, Id, TimeSeriesHeadsCollection, Batch),
        {ok, ts_persistence:finalize(PersistenceCtx)}
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
        {TimeSeriesHeadsCollection, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
        FinalPersistenceCtx = update_time_series(
            maps:to_list(TimeSeriesHeadsCollection), NewTimestamp, NewValue, PersistenceCtx),
        {ok, ts_persistence:finalize(FinalPersistenceCtx)}
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Time series collection ~p update error: ~p:~p~nFailed to update measurement {~p, ~p}",
                [Id, Error, Reason, NewTimestamp, NewValue], Stacktrace),
            {{error, update_failed}, Batch}
    end;

update(Ctx, Id, NewTimestamp, MetricsToUpdateWithValues, Batch) when is_list(MetricsToUpdateWithValues) ->
    try
        {TimeSeriesHeadsCollection, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
        FinalPersistenceCtx = lists:foldl(fun({MetricsToUpdate, NewValue}, Acc) ->
            SelectedHeads = select_heads(TimeSeriesHeadsCollection, MetricsToUpdate),
            update_time_series(maps:to_list(SelectedHeads), NewTimestamp, NewValue, Acc)
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
%% Puts metrics value for particular timestamp. Updated value is the same for all metrics provided in 4th argument.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), collection_id(), ts_windows:timestamp(), request_range() , ts_windows:value(), batch()) ->
    {ok | {error, term()}, batch()}.
update(Ctx, Id, NewTimestamp, MetricsToUpdate, NewValue, Batch) ->
    try
        {TimeSeriesHeadsCollection, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
        SelectedHeads = select_heads(TimeSeriesHeadsCollection, MetricsToUpdate),
        FinalPersistenceCtx = update_time_series(maps:to_list(SelectedHeads), NewTimestamp, NewValue, PersistenceCtx),
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
%% Returns windows according options provided in 4th argument.
%% Windows for all metrics from all time series are included in answer.
%% @end
%%--------------------------------------------------------------------
-spec list(ctx(), collection_id(), ts_windows:list_options(), batch() | undefined) ->
    {{ok, windows_map()} | {error, term()}, batch() | undefined}.
list(Ctx, Id, Options, Batch) ->
    ?CATCH_EXCEPT_FETCH_ERROR(
        begin
            {TimeSeriesHeadsCollection, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
            FillMetricsIds = maps:fold(fun(TimeSeriesId, MetricsConfigs, Acc) ->
                maps:fold(fun(MetricsId, _Config, InternalAcc) ->
                    [{TimeSeriesId, MetricsId} | InternalAcc]
                end, Acc, MetricsConfigs)
            end, [], TimeSeriesHeadsCollection),
            {Ans, FinalPersistenceCtx} = list_time_series(
                TimeSeriesHeadsCollection, FillMetricsIds, Options, PersistenceCtx),
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
-spec list(ctx(), collection_id(), request_range(), ts_windows:list_options(), batch() | undefined) ->
    {{ok, [ts_windows:window()] | windows_map()} | {error, term()}, batch() | undefined}.
list(Ctx, Id, RequestedMetrics, Options, Batch) ->
    ?CATCH_EXCEPT_FETCH_ERROR(
        begin
            {TimeSeriesHeadsCollection, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
            {Ans, FinalPersistenceCtx} = list_time_series(
                TimeSeriesHeadsCollection, RequestedMetrics, Options, PersistenceCtx),
            {{ok, Ans}, ts_persistence:finalize(FinalPersistenceCtx)}
        end,
        "Time series collection ~p list error~nRequested metrics: ~p~nOptions: ~p",
        [Id, RequestedMetrics, Options], {{error, list_failed}, Batch}
    ).


-spec delete(ctx(), collection_id(), batch() ) -> {ok | {error, term()}, batch()}.
delete(Ctx, Id, Batch) ->
    ?CATCH_EXCEPT_FETCH_ERROR(
        begin
            {TimeSeriesHeadsCollection, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
            FinalPersistenceCtx = delete_time_series(maps:to_list(TimeSeriesHeadsCollection), PersistenceCtx),
            {ok, ts_persistence:finalize(FinalPersistenceCtx)}
        end,
        "Time series collection ~p delete error", [Id], {{error, delete_failed}, Batch}
    ).


%%=====================================================================
%% Internal functions
%%=====================================================================

-spec select_heads(ts_hub:time_series_heads_collection(), request_range()) ->
    ts_hub:time_series_heads_collection().
select_heads(_TimeSeriesHeadsCollection, []) ->
    #{};

select_heads(TimeSeriesHeadsCollection, [{TimeSeriesId, MetricIds} | Tail]) ->
    Ans = select_heads(TimeSeriesHeadsCollection, Tail),
    case maps:get(TimeSeriesId, TimeSeriesHeadsCollection, undefined) of
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

select_heads(TimeSeriesHeadsCollection, [TimeSeriesId | Tail]) ->
    Ans = select_heads(TimeSeriesHeadsCollection, Tail),
    case maps:get(TimeSeriesId, TimeSeriesHeadsCollection, undefined) of
        undefined -> Ans;
        TimeSeries -> maps:put(TimeSeriesId, TimeSeries, Ans)
    end;

select_heads(TimeSeriesHeadsCollection, ToBeIncluded) ->
    select_heads(TimeSeriesHeadsCollection, [ToBeIncluded]).


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


-spec list_time_series(ts_hub:time_series_heads_collection(), request_range(), ts_windows:list_options(),
    ts_persistence:ctx()) -> {[ts_windows:window()] | windows_map(), ts_persistence:ctx()}.
list_time_series(_TimeSeriesHeadsCollection, [], _Options, PersistenceCtx) ->
    {#{}, PersistenceCtx};

list_time_series(TimeSeriesHeadsCollection, [{TimeSeriesIds, MetricIds} | RequestedMetrics], Options, PersistenceCtx) ->
    {Ans, UpdatedPersistenceCtx} = lists:foldl(fun(TimeSeriesId, Acc) ->
        MetricsMap = maps:get(TimeSeriesId, TimeSeriesHeadsCollection, #{}),
        lists:foldl(fun(MetricId, {TmpAns, TmpPersistenceCtx}) ->
            {Values, UpdatedTmpPersistenceCtx} = case maps:get(MetricId, MetricsMap, undefined) of
                undefined -> {undefined, TmpPersistenceCtx};
                Metric -> ts_metric:list(Metric, Options, TmpPersistenceCtx)
            end,
            {TmpAns#{{TimeSeriesId, MetricId} => Values}, UpdatedTmpPersistenceCtx}
        end, Acc, utils:ensure_list(MetricIds))
    end, {#{}, PersistenceCtx}, utils:ensure_list(TimeSeriesIds)),

    {Ans2, FinalPersistenceCtx} = list_time_series(
        TimeSeriesHeadsCollection, RequestedMetrics, Options, UpdatedPersistenceCtx),
    {maps:merge(Ans, Ans2), FinalPersistenceCtx};

list_time_series(TimeSeriesHeadsCollection, [TimeSeriesId | RequestedMetrics], Options, PersistenceCtx) ->
    {Ans, UpdatedPersistenceCtx} = case maps:get(TimeSeriesId, TimeSeriesHeadsCollection, undefined) of
        undefined ->
            {#{TimeSeriesId => undefined}, PersistenceCtx};
        MetricsMap ->
            lists:foldl(fun({MetricId, Metric}, {TmpAns, TmpPersistenceCtx}) ->
                {Values, UpdatedTmpPersistenceCtx} = ts_metric:list(Metric, Options, TmpPersistenceCtx),
                {TmpAns#{{TimeSeriesId, MetricId} => Values}, UpdatedTmpPersistenceCtx}
            end, {#{}, PersistenceCtx}, maps:to_list(MetricsMap))
    end,

    {Ans2, FinalPersistenceCtx} = list_time_series(TimeSeriesHeadsCollection, RequestedMetrics, Options, UpdatedPersistenceCtx),
    {maps:merge(Ans, Ans2), FinalPersistenceCtx};

list_time_series(TimeSeriesHeadsCollection, Request, Options, PersistenceCtx) ->
    {Ans, FinalPersistenceCtx} = list_time_series(TimeSeriesHeadsCollection, [Request], Options, PersistenceCtx),
    case maps:is_key(Request, Ans) of
        true ->
            % Single key is requested - return value for the key instead of map
            {maps:get(Request, Ans), FinalPersistenceCtx};
        false ->
            {Ans, FinalPersistenceCtx}
    end.


-spec delete_time_series([{time_series_id(), ts_hub:time_series_heads()}], ts_persistence:ctx()) -> ts_persistence:ctx().
delete_time_series([], PersistenceCtx) ->
    ts_persistence:delete_hub(PersistenceCtx);
delete_time_series([{_TimeSeriesId, TimeSeries} | Tail], PersistenceCtx) ->
    UpdatedPersistenceCtx = delete_metric(maps:to_list(TimeSeries), PersistenceCtx),
    delete_time_series(Tail, UpdatedPersistenceCtx).


-spec delete_metric([{ts_metric:id(), ts_metric:metric()}], ts_persistence:ctx()) ->
    ts_persistence:ctx().
delete_metric([], PersistenceCtx) ->
    PersistenceCtx;
delete_metric([{_MetricId, Metric} | Tail], PersistenceCtx) ->
    UpdatedPersistenceCtx = ts_metric:delete(Metric, PersistenceCtx),
    delete_metric(Tail, UpdatedPersistenceCtx).