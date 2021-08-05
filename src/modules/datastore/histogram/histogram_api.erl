%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal datastore API used for operating on histograms.
%%% Histogram consists of several time series. Each time series consists of
%%% several metrics. Metrics is set of windows that aggregate multiple
%%% measurements from particular period of time. E.g.,
%%% MyHistogram = #{
%%%    TimeSeries1 = #{
%%%       Metric1 = [Window1, Window2, ...],
%%%       Metric2 = ...
%%%    },
%%%    TimeSeries2 = ...
%%% }
%%% Window = {Timestamp, aggregator(Measurement1Value, Measurement2Value, ...)}
%%% See histogram_windows:aggregate/3 to see possible aggregation functions.
%%%
%%% The module works on #data{} record that represents part of
%%% windows of particular metric. #data{} record is encapsulated in document
%%% and saved/get to/from datastore by histogram_persistence
%%% helper module.
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_api).
-author("Michal Wrzeszcz").

-include("modules/datastore/histogram_api.hrl").
-include("modules/datastore/histogram.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/4, update/5, update/4, get/5]).
%% Time series map API
-export([update_time_series_map_data/4]).
%% Test API
-export([create_doc_splitting_strategies/1]).

-type id() :: binary(). % Id of histogram
-type time_series_id() :: binary().
-type metric_id() :: binary().

-type metric() :: #metric{}.
-type time_series() :: #{metric_id() => metric()}.
-type time_series_map() :: #{time_series_id() => time_series()}.
-type time_series_config() :: #{time_series_id() => #{metric_id() => metric_config()}}.

-type legend() :: binary().
-type metric_config() :: #metric_config{}.
-type splitting_strategy() :: #splitting_strategy{}.
-type data() :: #data{}.

-type requested_metrics() :: {time_series_id() | [time_series_id()], metric_id() | [metric_id()]}.
% Metrics are stored in hierarchical map when time_series_id is used to get map with #{metric_id() => metric()}.
% Other way of presentation is in flattened map using key {time_series_id(), metric_id()}. It is used
% mainly to return requested data (get can return only chosen metrics so it does not return hierarchical map).
-type full_metric_id() :: {time_series_id(), metric_id()}.
-type windows_map() :: #{full_metric_id() => [histogram_windows:window()]}.
-type flat_config_map() :: #{full_metric_id() => metric_config()}.
-type windows_count_map() :: #{full_metric_id() => non_neg_integer()}.

-export_type([id/0, time_series_map/0, time_series_config/0, time_series_id/0, metric_id/0,
    data/0, metric_config/0, splitting_strategy/0, legend/0, requested_metrics/0, windows_map/0]).

-type key() :: datastore:key().
-type ctx() :: datastore:ctx().
-type batch() :: datastore_doc:batch().

-export_type([key/0]).

% Warning: do not use this env in app.config. Use of env limited to tests.
-define(MAX_VALUES_IN_DOC, application:get_env(?CLUSTER_WORKER_APP_NAME, histogram_max_doc_size, 50000)).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(ctx(), id(), time_series_config(), batch()) -> {ok | {error, term()}, batch()}.
create(Ctx, Id, ConfigMap, Batch) ->
    try
        DocSplittingStrategies = create_doc_splitting_strategies(ConfigMap),

        TimeSeries = maps:map(fun(TimeSeriesId, MetricsConfigs) ->
            maps:map(fun(MetricsId, Config) ->
                 #metric{
                     config = Config,
                     splitting_strategy = maps:get({TimeSeriesId, MetricsId}, DocSplittingStrategies)
                 }
            end, MetricsConfigs)
        end, ConfigMap),

        PersistenceCtx = histogram_persistence:init_for_new_histogram(Ctx, Id, TimeSeries, Batch),
        {ok, histogram_persistence:finalize(PersistenceCtx)}
    catch
        _:{error, to_many_metrics} ->
            {error, to_many_metrics};
        _:{error, empty_metric} ->
            {error, empty_metric};
        _:{error, wrong_window_timespan} ->
            {error, wrong_window_timespan};
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Histogram ~p init error: ~p:~p~nConfig map: ~p",
                [Id, Error, Reason, ConfigMap], Stacktrace),
            {{error, histogram_create_failed}, Batch}
    end.


-spec update(ctx(), id(), histogram_windows:timestamp(), histogram_windows:value(), batch()) ->
    {ok | {error, term()}, batch()}.
update(Ctx, Id, NewTimestamp, NewValue, Batch) ->
    try
        {TimeSeriesMap, PersistenceCtx} = histogram_persistence:init_for_existing_histogram(Ctx, Id, Batch),
        FinalPersistenceCtx = update_time_series(maps:to_list(TimeSeriesMap), NewTimestamp, NewValue, PersistenceCtx),
        {ok, histogram_persistence:finalize(FinalPersistenceCtx)}
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Histogram ~p update error: ~p:~p~nFailed to update measurement {~p, ~p}",
                [Id, Error, Reason, NewTimestamp, NewValue], Stacktrace),
            {{error, histogram_update_failed}, Batch}
    end.


-spec update(ctx(), id(), [{histogram_windows:timestamp(), histogram_windows:value()}], batch()) ->
    {ok | {error, term()}, batch()}.
update(_Ctx, _Id, [], Batch) ->
    {ok, Batch};
update(Ctx, Id, [{NewTimestamp, NewValue} | Measurements], Batch) ->
    case update(Ctx, Id, NewTimestamp, NewValue, Batch) of
        {ok, UpdatedBatch} -> update(Ctx, Id, Measurements, UpdatedBatch);
        Other -> Other
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns windows for requested ranges. If windows from single metric are requested, the function returns list of windows.
%% Otherwise, map containing list of windows for each requested metric is returned.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), id(), requested_metrics() | [requested_metrics()], histogram_windows:get_options(), batch() | undefined) ->
    {{ok, [histogram_windows:window()] | windows_map()} | {error, term()}, batch() | undefined}.
get(Ctx, Id, RequestedMetrics, Options, Batch) ->
    try
        {TimeSeriesMap, PersistenceCtx} = histogram_persistence:init_for_existing_histogram(Ctx, Id, Batch),
        {Ans, FinalPersistenceCtx} = get(TimeSeriesMap, RequestedMetrics, Options, PersistenceCtx),
        {{ok, Ans}, histogram_persistence:finalize(FinalPersistenceCtx)}
    catch
        Error:Reason:Stacktrace when Reason =/= {fetch_error, not_found} ->
            ?error_stacktrace("Histogram ~p get error: ~p:~p~nRequested metrics: ~p~nOptions: ~p",
                [Id, Error, Reason, RequestedMetrics, Options], Stacktrace),
            {{error, histogram_get_failed}, Batch}
    end.


%%=====================================================================
%% Internal functions operation on time series
%%=====================================================================

-spec update_time_series([{time_series_id(), time_series()}], histogram_windows:timestamp(), 
    histogram_windows:value(), histogram_persistence:ctx()) -> histogram_persistence:ctx().
update_time_series([], _NewTimestamp, _NewValue, PersistenceCtx) ->
    PersistenceCtx;

update_time_series([{TimeSeriesId, TimeSeries} | Tail], NewTimestamp, NewValue, PersistenceCtx) ->
    PersistenceCtxWithIdSet = histogram_persistence:set_active_time_series(TimeSeriesId, PersistenceCtx),
    UpdatedPersistenceCtx = update_metrics(maps:to_list(TimeSeries), NewTimestamp, NewValue, PersistenceCtxWithIdSet),
    update_time_series(Tail, NewTimestamp, NewValue, UpdatedPersistenceCtx).


-spec get(time_series_map(), requested_metrics() | [requested_metrics()],
    histogram_windows:get_options(), histogram_persistence:ctx()) ->
    {[histogram_windows:window()] | windows_map(), histogram_persistence:ctx()}.
get(_TimeSeriesMap, [], _Options, PersistenceCtx) ->
    {#{}, PersistenceCtx};

get(TimeSeriesMap, [{TimeSeriesIds, MetricsIds} | RequestedMetrics], Options, PersistenceCtx) ->
    {Ans, UpdatedPersistenceCtx} = lists:foldl(fun(TimeSeriesId, Acc) ->
        lists:foldl(fun(MetricsId, {TmpAns, TmpPersistenceCtx}) ->
            MetricsMap = maps:get(TimeSeriesId, TimeSeriesMap, #{}),
            {Values, UpdatedTmpPersistenceCtx} = case maps:get(MetricsId, MetricsMap, undefined) of
                undefined ->
                    {undefined, TmpPersistenceCtx};
                #metric{
                    data = Data,
                    config = Config
                } ->
                    Window = get_window(maps:get(start, Options, undefined), Config),
                    get_from_metric(Data, Window, Options, TmpPersistenceCtx)
            end,
            {TmpAns#{{TimeSeriesId, MetricsId} => Values}, UpdatedTmpPersistenceCtx}
        end, Acc, utils:ensure_list(MetricsIds))
    end, {#{}, PersistenceCtx}, utils:ensure_list(TimeSeriesIds)),

    {Ans2, FinalPersistenceCtx} = get(TimeSeriesMap, RequestedMetrics, Options, UpdatedPersistenceCtx),
    {maps:merge(Ans, Ans2), FinalPersistenceCtx};

get(TimeSeriesMap, {_, _} = Request, Options, PersistenceCtx) ->
    {Ans, FinalPersistenceCtx} = get(TimeSeriesMap, [Request], Options, PersistenceCtx),
    case maps:get(Request, Ans, undefined) of
        undefined -> {Ans, FinalPersistenceCtx};
        GetAns -> {GetAns, FinalPersistenceCtx}
    end.


%%=====================================================================
%% Internal functions operating on metrics
%%=====================================================================

-spec update_metrics([{metric_id(), metric()}], histogram_windows:timestamp(),
    histogram_windows:value(), histogram_persistence:ctx()) -> histogram_persistence:ctx().
update_metrics([], _NewTimestamp, _NewValue, PersistenceCtx) ->
    PersistenceCtx;
update_metrics(
    [{MetricsId, #metric{
        config = #metric_config{aggregator = Aggregator} = Config,
        splitting_strategy = DocSplittingStrategy,
        data = Data
    }} | Tail], NewTimestamp, NewValue, PersistenceCtx) ->
    WindowToBeUpdated = get_window(NewTimestamp, Config),
    DataDocKey = histogram_persistence:get_head_key(PersistenceCtx),
    PersistenceCtxWithIdSet = histogram_persistence:set_active_metric(MetricsId, PersistenceCtx),
    UpdatedPersistenceCtx = update_metric(
        Data, DataDocKey, 1, DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtxWithIdSet),
    update_metrics(Tail, NewTimestamp, NewValue, UpdatedPersistenceCtx).


-spec update_metric(data(), key(), non_neg_integer(), splitting_strategy(),
    histogram_windows:aggregator(), histogram_windows:timestamp(),
    histogram_windows:value(), histogram_persistence:ctx()) -> histogram_persistence:ctx().
update_metric(
    #data{
        windows = Windows
    } = Data, DataDocKey, _DataDocPosition,
    #splitting_strategy{
        max_docs_count = 1,
        max_windows_in_head_doc = MaxWindowsCount
    }, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    UpdatedWindows = histogram_windows:aggregate(Windows, WindowToBeUpdated, NewValue, Aggregator),
    FinalWindows = histogram_windows:prune_overflowing_windows(UpdatedWindows, MaxWindowsCount),
    histogram_persistence:update(DataDocKey, Data#data{windows = FinalWindows}, PersistenceCtx);

update_metric(
    #data{
        prev_record = undefined,
        prev_record_timestamp = PrevRecordTimestamp
    }, _DataDocKey, _DataDocPosition,
    _DocSplittingStrategy, _Aggregator, WindowToBeUpdated, _NewValue, PersistenceCtx)
    when PrevRecordTimestamp =/= undefined andalso PrevRecordTimestamp >= WindowToBeUpdated ->
    PersistenceCtx;

update_metric(
    #data{
        prev_record = undefined,
        windows = Windows
    } = Data, DataDocKey, DataDocPosition,
    DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    UpdatedWindows = histogram_windows:aggregate(Windows, WindowToBeUpdated, NewValue, Aggregator),
    {MaxWindowsCount, SplitPosition} = get_max_windows_and_split_position(DataDocKey, DocSplittingStrategy, PersistenceCtx),

    case histogram_windows:should_reorganize_windows(UpdatedWindows, MaxWindowsCount) of
        true ->
            % Prev record is undefined so splitting is only possible way of reorganization
            {Windows1, Windows2, SplitTimestamp} = histogram_windows:split_windows(UpdatedWindows, SplitPosition),
            {_, _, UpdatedPersistenceCtx} = split_record(DataDocKey, Data,
                Windows1, Windows2, SplitTimestamp, DataDocPosition, DocSplittingStrategy, PersistenceCtx),
            UpdatedPersistenceCtx;
        false ->
            histogram_persistence:update(DataDocKey, Data#data{windows = UpdatedWindows}, PersistenceCtx)
    end;

update_metric(
    #data{
        prev_record = PrevRecordKey,
        prev_record_timestamp = PrevRecordTimestamp
    }, _DataDocKey, DataDocPosition,
    DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx)
    when PrevRecordTimestamp >= WindowToBeUpdated ->
    {PrevRecordData, UpdatedPersistenceCtx} = histogram_persistence:get(PrevRecordKey, PersistenceCtx),
    update_metric(PrevRecordData, PrevRecordKey, DataDocPosition + 1,
        DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, UpdatedPersistenceCtx);

update_metric(
    #data{
        windows = Windows,
        prev_record = PrevRecordKey
    } = Data, DataDocKey, DataDocPosition,
    #splitting_strategy{
        max_windows_in_tail_doc = MaxWindowsInTail
    } = DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    WindowsWithAggregatedMeasurement = histogram_windows:aggregate(Windows, WindowToBeUpdated, NewValue, Aggregator),
    {MaxWindowsCount, SplitPosition} = get_max_windows_and_split_position(DataDocKey, DocSplittingStrategy, PersistenceCtx),

    case histogram_windows:should_reorganize_windows(WindowsWithAggregatedMeasurement, MaxWindowsCount) of
        true ->
            {#data{windows = WindowsInPrevRecord} = PrevRecordData, UpdatedPersistenceCtx} =
                histogram_persistence:get(PrevRecordKey, PersistenceCtx),
            Actions = histogram_windows:reorganize_windows(
                WindowsInPrevRecord, WindowsWithAggregatedMeasurement, MaxWindowsInTail, SplitPosition),

            lists:foldl(fun
                ({update_current_record, UpdatedPrevRecordTimestamp, UpdatedWindows}, TmpPersistenceCtx) ->
                    histogram_persistence:update(DataDocKey, Data#data{windows = UpdatedWindows,
                        prev_record_timestamp = UpdatedPrevRecordTimestamp}, TmpPersistenceCtx);
                ({split_current_record, {Windows1, Windows2, SplitTimestamp}}, TmpPersistenceCtx) ->
                    {CreatedRecordKey, CreatedRecord, UpdatedTmpPersistenceCtx} = split_record(DataDocKey, Data,
                        Windows1, Windows2, SplitTimestamp, DataDocPosition, DocSplittingStrategy, TmpPersistenceCtx),
                    maybe_delete_last_doc(CreatedRecordKey, CreatedRecord, PrevRecordKey, PrevRecordData,
                        DocSplittingStrategy, DataDocPosition + 2, UpdatedTmpPersistenceCtx);
                ({update_previous_record, UpdatedWindowsInPrevRecord}, TmpPersistenceCtx) ->
                    histogram_persistence:update(PrevRecordKey,
                        PrevRecordData#data{windows = UpdatedWindowsInPrevRecord}, TmpPersistenceCtx)
            end, UpdatedPersistenceCtx, Actions);
        false ->
            histogram_persistence:update(DataDocKey, Data#data{windows = WindowsWithAggregatedMeasurement}, PersistenceCtx)
    end.


-spec maybe_delete_last_doc(key(), data(), key(), data() | undefined, splitting_strategy(), non_neg_integer(),
    histogram_persistence:ctx()) -> histogram_persistence:ctx().
maybe_delete_last_doc(NextRecordKey, NextRecordData, Key, _Data,
    #splitting_strategy{max_docs_count = MaxCount}, DocumentNumber, PersistenceCtx) when DocumentNumber > MaxCount ->
    UpdatedNextRecordData = NextRecordData#data{prev_record = undefined},
    UpdatedPersistenceCtx = histogram_persistence:update(NextRecordKey, UpdatedNextRecordData, PersistenceCtx),
    histogram_persistence:delete(Key, UpdatedPersistenceCtx);
maybe_delete_last_doc(NextRecordKey, NextRecordData, Key, undefined,
    DocSplittingStrategy, DocumentNumber, PersistenceCtx) ->
    {Data, UpdatedPersistenceCtx} = histogram_persistence:get(Key, PersistenceCtx),
    maybe_delete_last_doc(NextRecordKey, NextRecordData, Key, Data,
        DocSplittingStrategy, DocumentNumber, UpdatedPersistenceCtx);
maybe_delete_last_doc(_NextRecordKey, _NextRecordData, _Key, #data{prev_record = undefined},
    _DocSplittingStrategy, _DocumentNumber, PersistenceCtx) ->
    PersistenceCtx;
maybe_delete_last_doc(_NextRecordKey, _NextRecordData, Key, #data{prev_record = PrevRecordKey} = Data,
    DocSplittingStrategy, DocumentNumber, PersistenceCtx) ->
    maybe_delete_last_doc(Key, Data, PrevRecordKey, undefined, DocSplittingStrategy, DocumentNumber + 1, PersistenceCtx).


-spec get_from_metric(data(), histogram_windows:timestamp() | undefined, histogram_windows:get_options(),
    histogram_persistence:ctx()) -> {[histogram_windows:window()], histogram_persistence:ctx()}.
get_from_metric(
    #data{
        windows = Windows,
        prev_record = undefined
    }, Window, Options, PersistenceCtx) ->
    {_, WindowsToReturn} = histogram_windows:get(Windows, Window, Options),
    {WindowsToReturn, PersistenceCtx};

get_from_metric(
    #data{
        prev_record = PrevRecordKey,
        prev_record_timestamp = PrevRecordTimestamp
    }, Window, Options, PersistenceCtx)
    when PrevRecordTimestamp >= Window ->
    {PrevRecordData, UpdatedPersistenceCtx} = histogram_persistence:get(PrevRecordKey, PersistenceCtx),
    get_from_metric(PrevRecordData, Window, Options, UpdatedPersistenceCtx);

get_from_metric(
    #data{
        windows = Windows,
        prev_record = PrevRecordKey
    }, Window, Options, PersistenceCtx) ->
    case histogram_windows:get(Windows, Window, Options) of
        {ok, WindowsToReturn} ->
            {WindowsToReturn, PersistenceCtx};
        {{continue, NewOptions}, WindowsToReturn} ->
            {PrevRecordData, UpdatedPersistenceCtx} = histogram_persistence:get(PrevRecordKey, PersistenceCtx),
            {NextWindowsToReturn, FinalPersistenceCtx} =
                get_from_metric(PrevRecordData, undefined, NewOptions, UpdatedPersistenceCtx),
            {WindowsToReturn ++ NextWindowsToReturn, FinalPersistenceCtx}
    end.


%%=====================================================================
%% Time series map API
%%=====================================================================

-spec update_time_series_map_data(time_series_map(), histogram_api:time_series_id(), histogram_api:metric_id(),
    histogram_api:data()) -> time_series_map().
update_time_series_map_data(TimeSeriesMap, TimeSeriesId, MetricsId, UpdatedData) ->
    TimeSeries = maps:get(TimeSeriesId, TimeSeriesMap),
    Metrics = maps:get(MetricsId, TimeSeries),
    TimeSeriesMap#{TimeSeriesId => TimeSeries#{MetricsId => Metrics#metric{data = UpdatedData}}}.


%%=====================================================================
%% Helper functions
%%=====================================================================

-spec create_doc_splitting_strategies(time_series_config()) -> #{full_metric_id() => splitting_strategy()}.
create_doc_splitting_strategies(ConfigMap) ->
    FlattenedMap = maps:fold(fun(TimeSeriesId, MetricsConfigs, Acc) ->
        maps:fold(fun
            (_, #metric_config{max_windows_count = WindowsCount}, _) when WindowsCount =< 0 ->
                throw({error, empty_metric});
            (_, #metric_config{window_timespan = WindowSize}, _) when WindowSize =< 0 ->
                throw({error, wrong_window_timespan});
            (MetricsId, Config, InternalAcc) ->
                InternalAcc#{{TimeSeriesId, MetricsId} => Config}
        end, Acc, MetricsConfigs)
    end, #{}, ConfigMap),

    MaxValuesInDoc = ?MAX_VALUES_IN_DOC,
    MaxWindowsInHeadMap = calculate_windows_in_head_doc_count(FlattenedMap),
    maps:map(fun(Key, MaxWindowsInHead) ->
        #metric_config{max_windows_count = MaxWindowsCount} = maps:get(Key, FlattenedMap),
        {MaxWindowsInTailDoc, MaxDocsCount} = case MaxWindowsInHead of
            MaxWindowsCount ->
                % All windows can be stored in head
                {0, 1};
            _ ->
                % It is guaranteed that each tail document used at least half of its capacity after split
                % so windows count should be multiplied by 2 when calculating number of tail documents
                % (head can be cleared to optimize upload so tail documents have to store required windows count).
                case {MaxWindowsCount =< MaxValuesInDoc, MaxWindowsCount =< MaxValuesInDoc div 2} of
                    {true, true} ->
                        {2 * MaxWindowsCount, 2};
                    {true, false} ->
                        {MaxWindowsCount, 3};
                    _ ->
                        {MaxValuesInDoc, 1 + ceil(2 * MaxWindowsCount / MaxValuesInDoc)}
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
    #metric_config{max_windows_count = MaxWindowsCount} = maps:get(Key, FlattenedMap),
    CurrentLimit = maps:get(Key, NotFullyStoredInHead),
    NewLimit = CurrentLimit + Update,
    {UpdatedFullyStoredInHead, UpdatedNotFullyStoredInHead, FinalUpdate} = case NewLimit >= MaxWindowsCount of
        true ->
            {FullyStoredInHead#{Key => MaxWindowsCount},
                maps:remove(Key, NotFullyStoredInHead), MaxWindowsCount - CurrentLimit};
        false ->
            {FullyStoredInHead, NotFullyStoredInHead#{Key => NewLimit}, Update}
    end,
    update_windows_in_head_doc_count(MetricsKeys, UpdatedFullyStoredInHead, UpdatedNotFullyStoredInHead, FlattenedMap,
        LimitUpdate, RemainingWindowsInHead - FinalUpdate).


-spec get_window(histogram_windows:timestamp() | undefined, metric_config()) -> histogram_windows:timestamp() | undefined.
get_window(undefined, _) ->
    undefined;
get_window(Time, #metric_config{window_timespan = WindowSize}) ->
    Time - Time rem WindowSize.


-spec get_max_windows_and_split_position(key(), splitting_strategy(), histogram_persistence:ctx()) ->
    {non_neg_integer(), non_neg_integer()}.
get_max_windows_and_split_position(
    DataDocKey,
    #splitting_strategy{
        max_windows_in_head_doc = MaxWindowsCountInHead,
        max_windows_in_tail_doc = MaxWindowsCountInTail
    },
    PersistenceCtx) ->
    case histogram_persistence:is_head(DataDocKey, PersistenceCtx) of
        true ->
            % If adding of single window results in reorganization split should be at first element
            % to move most of windows to tail doc
            {MaxWindowsCountInHead, 1};
        false ->
            % Split of tail doc should result in two documents with at list of half of capacity used
            {MaxWindowsCountInTail, ceil(MaxWindowsCountInTail / 2)}
    end.


-spec split_record(key(), data(), histogram_windows:windows(), histogram_windows:windows(),
    histogram_windows:timestamp(), non_neg_integer(), splitting_strategy(), histogram_persistence:ctx()) ->
    {key() | undefined, data() | undefined, histogram_persistence:ctx()}.
split_record(DataDocKey, Data, Windows1, _Windows2, SplitTimestamp, MaxCount,
    #splitting_strategy{max_docs_count = MaxCount}, PersistenceCtx) ->
    UpdatedData = Data#data{windows = Windows1, prev_record_timestamp = SplitTimestamp},
    {undefined, undefined, histogram_persistence:update(DataDocKey, UpdatedData, PersistenceCtx)};
split_record(DataDocKey, Data, Windows1, Windows2, SplitTimestamp, _DocumentNumber, _DocSplittingStrategy, PersistenceCtx) ->
    DataToCreate = Data#data{windows = Windows2},
    {CreatedRecordKey, UpdatedPersistenceCtx} = histogram_persistence:create(DataToCreate, PersistenceCtx),
    UpdatedData = Data#data{windows = Windows1, prev_record = CreatedRecordKey, prev_record_timestamp = SplitTimestamp},
    {CreatedRecordKey, DataToCreate, histogram_persistence:update(DataDocKey, UpdatedData, UpdatedPersistenceCtx)}.