%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal datastore API used to operate on histograms.
%%% The module works on #data{} record that represents part of
%%% metric's windows. #data{} record is encapsulated in document
%%% and saved/get to/from datastore by histogram_persistence
%%% helper module.
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_api).
-author("Michal Wrzeszcz").

-include("modules/datastore/histogram.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/4, update/5, update/4, get/5]).
%% Test API
-export([create_doc_splitting_strategies/1]).

% TODO - moze przeniesc do naglowka? gdzie najlepiej ten rekord pasuje?
-record(data, {
    windows = histogram_windows:init() :: histogram_windows:windows(),
    prev_record :: key() | undefined,
    % Timestamp of newest point in previous record
    prev_record_timestamp :: histogram_windows:timestamp() | undefined
}).

-record(doc_splitting_strategy, {
    max_docs_count :: non_neg_integer(),
    max_windows_in_head_doc :: non_neg_integer(),
    max_windows_in_tail_doc :: non_neg_integer()
}).

-type id() :: binary(). % Id of histogram
-type time_series_id() :: binary().
-type metrics_id() :: binary().

-type metrics() :: #metrics{}.
-type time_series() :: #{metrics_id() => metrics()}.
-type time_series_map() :: #{time_series_id() => time_series()}.
-type time_series_config() :: #{time_series_id() => #{metrics_id() => config()}}.

-type legend() :: binary().
-type config() :: #histogram_config{}.
-type doc_splitting_strategy() :: #doc_splitting_strategy{}.
-type data() :: #data{}.

-type requested_metrics() :: {time_series_id() | [time_series_id()], metrics_id() | [metrics_id()]}.
% Metrics are stored in hierarchical map when time_series_id is used to get map with #{metrics_id() => metrics()}.
% Other way of presentation is in flattened map using key {time_series_id(), metrics_id()}. It is used
% mainly to return requested data (get can return only chosen metrics so it does not return hierarchical map).
-type full_metrics_id() :: {time_series_id(), metrics_id()}.
-type metrics_values_map() :: #{full_metrics_id() => [histogram_windows:window()]}.
-type flat_config_map() :: #{full_metrics_id() => config()}.
-type windows_count_map() :: #{full_metrics_id() => non_neg_integer()}.

-export_type([id/0, time_series_map/0, time_series_config/0, time_series_id/0, metrics_id/0,
    data/0, config/0, doc_splitting_strategy/0, legend/0, requested_metrics/0, metrics_values_map/0]).

-type key() :: datastore:key().
-type ctx() :: datastore:ctx().
-type batch() :: datastore_doc:batch().

% Warning: do not use this env in app.config. Use of env limited to tests.
-define(MAX_VALUES_IN_DOC, application:get_env(?CLUSTER_WORKER_APP_NAME, histogram_max_doc_size, 50000)).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(ctx(), id(), time_series_config(), batch()) -> {ok | {error, term()}, batch()}.
init(Ctx, Id, ConfigMap, Batch) ->
    try
        DocSplittingStrategies = create_doc_splitting_strategies(ConfigMap),

        TimeSeries = maps:map(fun(TimeSeriesId, MetricsConfigs) ->
            maps:map(fun(MetricsId, Config) ->
                 #metrics{
                     config = Config,
                     doc_splitting_strategy = maps:get({TimeSeriesId, MetricsId}, DocSplittingStrategies),
                     data = #data{} % TODO - czy init jest potrzebny po przeniesieniu?
                 }
            end, MetricsConfigs)
        end, ConfigMap),

        PersistenceCtx = histogram_persistence:new(Ctx, Id, TimeSeries, Batch),
        {ok, histogram_persistence:finalize(PersistenceCtx)}
    catch
        _:{error, to_many_metrics} ->
            {error, to_many_metrics};
        _:{error, empty_metrics} ->
            {error, empty_metrics};
        _:{error, wrong_window_size} ->
            {error, wrong_window_size};
        Error:Reason ->
            ?error_stacktrace("Histogram ~p init error: ~p:~p~nConfig map: ~p", [Id, Error, Reason, ConfigMap]),
            {{error, historgam_init_failed}, Batch}
    end.


-spec update(ctx(), id(), histogram_windows:timestamp(), histogram_windows:value(), batch()) ->
    {ok | {error, term()}, batch()}.
update(Ctx, Id, NewTimestamp, NewValue, Batch) ->
    try
        {TimeSeriesMap, PersistenceCtx} = histogram_persistence:init(Ctx, Id, Batch),
        FinalPersistenceCtx = update_time_series(maps:to_list(TimeSeriesMap), NewTimestamp, NewValue, PersistenceCtx),
        {ok, histogram_persistence:finalize(FinalPersistenceCtx)}
    catch
        Error:Reason ->
            ?error_stacktrace("Histogram ~p update error: ~p:~p~nFailed to update point {~p, ~p}",
                [Id, Error, Reason, NewTimestamp, NewValue]),
            {{error, historgam_update_failed}, Batch}
    end.


-spec update(ctx(), id(), [{histogram_windows:timestamp(), histogram_windows:value()}], batch()) ->
    {ok | {error, term()}, batch()}.
update(_Ctx, _Id, [], Batch) ->
    {ok, Batch};
update(Ctx, Id, [{NewTimestamp, NewValue} | Points], Batch) ->
    case update(Ctx, Id, NewTimestamp, NewValue, Batch) of
        {ok, UpdatedBatch} -> update(Ctx, Id, Points, UpdatedBatch);
        Other -> Other
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns points for requested ranges. If points from single metrics are requested, the function returns list of points. 
%% Otherwise, map containing list of points for each requested metrics is returned.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), id(), requested_metrics() | [requested_metrics()], histogram_windows:options(), batch() | undefined) ->
    {{ok, [histogram_windows:window()] | metrics_values_map()} | {error, term()}, batch() | undefined}.
get(Ctx, Id, RequestedMetrics, Options, Batch) ->
    try
        {TimeSeriesMap, PersistenceCtx} = histogram_persistence:init(Ctx, Id, Batch),
        {Ans, FinalPersistenceCtx} = get_time_series_values(TimeSeriesMap, RequestedMetrics, Options, PersistenceCtx),
        {{ok, Ans}, histogram_persistence:finalize(FinalPersistenceCtx)}
    catch
        Error:Reason when Reason =/= {fetch_error, not_found} ->
            ?error_stacktrace("Histogram ~p get error: ~p:~p~nRequested metrics: ~p~nOptions: ~p",
                [Id, Error, Reason, RequestedMetrics, Options]),
            {{error, historgam_get_failed}, Batch}
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


-spec get_time_series_values(time_series_map(), requested_metrics() | [requested_metrics()],
    histogram_windows:options(), histogram_persistence:ctx()) ->
    {[histogram_windows:window()] | metrics_values_map(), histogram_persistence:ctx()}.
get_time_series_values(_TimeSeriesMap, [], _Options, PersistenceCtx) ->
    {#{}, PersistenceCtx};

get_time_series_values(TimeSeriesMap, [{TimeSeriesIds, MetricsIds} | RequestedMetrics], Options, PersistenceCtx) ->
    {Ans, UpdatedPersistenceCtx} = lists:foldl(fun(TimeSeriesId, Acc) ->
        lists:foldl(fun(MetricsId, {TmpAns, TmpPersistenceCtx}) ->
            MetricsMap = maps:get(TimeSeriesId, TimeSeriesMap, #{}),
            {Values, UpdatedTmpPersistenceCtx} = case maps:get(MetricsId, MetricsMap, undefined) of
                undefined ->
                    {undefined, TmpPersistenceCtx};
                #metrics{
                    data = Data,
                    config = Config
                } ->
                    Window = get_window(maps:get(start, Options, undefined), Config),
                    get_metrics_values(Data, Window, Options, TmpPersistenceCtx)
            end,
            {TmpAns#{{TimeSeriesId, MetricsId} => Values}, UpdatedTmpPersistenceCtx}
        end, Acc, utils:ensure_list(MetricsIds))
    end, {#{}, PersistenceCtx}, utils:ensure_list(TimeSeriesIds)),

    {Ans2, FinalPersistenceCtx} = get_time_series_values(TimeSeriesMap, RequestedMetrics, Options, UpdatedPersistenceCtx),
    {maps:merge(Ans, Ans2), FinalPersistenceCtx};

get_time_series_values(TimeSeriesMap, {_, _} = Request, Options, PersistenceCtx) ->
    {Ans, FinalPersistenceCtx} = get_time_series_values(TimeSeriesMap, [Request], Options, PersistenceCtx),
    case maps:get(Request, Ans, undefined) of
        undefined -> {Ans, FinalPersistenceCtx};
        GetAns -> {GetAns, FinalPersistenceCtx}
    end.


%%=====================================================================
%% Internal functions operating on metrics
%%=====================================================================

-spec update_metrics([{metrics_id(), metrics()}], histogram_windows:timestamp(),
    histogram_windows:value(), histogram_persistence:ctx()) -> histogram_persistence:ctx().
update_metrics([], _NewTimestamp, _NewValue, PersistenceCtx) ->
    PersistenceCtx;
update_metrics(
    [{MetricsId, #metrics{
        config = #histogram_config{apply_function = ApplyFunction} = Config,
        doc_splitting_strategy = DocSplittingStrategy,
        data = Data
    }} | Tail], NewTimestamp, NewValue, PersistenceCtx) ->
    WindowToBeUpdated = get_window(NewTimestamp, Config),
    DataDocKey = histogram_persistence:get_head_key(PersistenceCtx),
    PersistenceCtxWithIdSet = histogram_persistence:set_active_metrics(MetricsId, PersistenceCtx),
    UpdatedPersistenceCtx = update_metrics(
        Data, DataDocKey, 1, DocSplittingStrategy, ApplyFunction, WindowToBeUpdated, NewValue, PersistenceCtxWithIdSet),
    update_metrics(Tail, NewTimestamp, NewValue, UpdatedPersistenceCtx).


-spec update_metrics(data(), key(), non_neg_integer(), doc_splitting_strategy(),
    histogram_windows:apply_function(), histogram_windows:timestamp(),
    histogram_windows:value(), histogram_persistence:ctx()) -> histogram_persistence:ctx().
update_metrics(
    #data{
        windows = Windows
    } = Data, DataDocKey, _DataDocPosition,
    #doc_splitting_strategy{
        max_docs_count = 1,
        max_windows_in_head_doc = MaxWindowsCount
    }, ApplyFunction, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    UpdatedWindows = histogram_windows:apply_value(Windows, WindowToBeUpdated, NewValue, ApplyFunction),
    FinalWindows = histogram_windows:maybe_delete_last(UpdatedWindows, MaxWindowsCount),
    histogram_persistence:update(DataDocKey, Data#data{windows = FinalWindows}, PersistenceCtx);

update_metrics(
    #data{
        prev_record = undefined,
        prev_record_timestamp = PrevRecordTimestamp
    }, _DataDocKey, _DataDocPosition,
    _DocSplittingStrategy, _ApplyFunction, WindowToBeUpdated, _NewValue, PersistenceCtx)
    when PrevRecordTimestamp =/= undefined andalso PrevRecordTimestamp >= WindowToBeUpdated ->
    PersistenceCtx;

update_metrics(
    #data{
        prev_record = undefined,
        windows = Windows
    } = Data, DataDocKey, DataDocPosition,
    DocSplittingStrategy, ApplyFunction, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    UpdatedWindows = histogram_windows:apply_value(Windows, WindowToBeUpdated, NewValue, ApplyFunction),
    {MaxWindowsCount, SplitPoint} = get_max_windows_and_split_point(DataDocKey, DocSplittingStrategy, PersistenceCtx),

    case histogram_windows:should_reorganize_windows(UpdatedWindows, MaxWindowsCount) of
        true ->
            {Windows1, Windows2, SplitTimestamp} = histogram_windows:split_windows(UpdatedWindows, SplitPoint),
            {_, _, UpdatedPersistenceCtx} = split_record(DataDocKey, Data,
                Windows1, Windows2, SplitTimestamp, DataDocPosition, DocSplittingStrategy, PersistenceCtx),
            UpdatedPersistenceCtx;
        false ->
            histogram_persistence:update(DataDocKey, Data#data{windows = UpdatedWindows}, PersistenceCtx)
    end;

update_metrics(
    #data{
        prev_record = PrevRecordKey,
        prev_record_timestamp = PrevRecordTimestamp
    }, _DataDocKey, DataDocPosition,
    DocSplittingStrategy, ApplyFunction, WindowToBeUpdated, NewValue, PersistenceCtx)
    when PrevRecordTimestamp >= WindowToBeUpdated ->
    {PrevRecordData, UpdatedPersistenceCtx} = histogram_persistence:get(PrevRecordKey, PersistenceCtx),
    update_metrics(PrevRecordData, PrevRecordKey, DataDocPosition + 1,
        DocSplittingStrategy, ApplyFunction, WindowToBeUpdated, NewValue, UpdatedPersistenceCtx);

update_metrics(
    #data{
        windows = Windows,
        prev_record = PrevRecordKey
    } = Data, DataDocKey, DataDocPosition,
    #doc_splitting_strategy{
        max_windows_in_tail_doc = MaxWindowsInTail
    } = DocSplittingStrategy, ApplyFunction, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    WindowsWithAppliedPoint = histogram_windows:apply_value(Windows, WindowToBeUpdated, NewValue, ApplyFunction),
    {MaxWindowsCount, SplitPoint} = get_max_windows_and_split_point(DataDocKey, DocSplittingStrategy, PersistenceCtx),

    case histogram_windows:should_reorganize_windows(WindowsWithAppliedPoint, MaxWindowsCount) of
        true ->
            {#data{windows = WindowsInPrevRecord} = PrevRecordData, UpdatedPersistenceCtx} =
                histogram_persistence:get(PrevRecordKey, PersistenceCtx),
            Actions = histogram_windows:reorganize_windows(
                WindowsInPrevRecord, WindowsWithAppliedPoint, MaxWindowsInTail, SplitPoint),

            lists:foldl(fun
                ({update_current_record, UpdatedPrevRecordTimestamp, UpdatedWindows}, TmpPersistenceCtx) ->
                    histogram_persistence:update(DataDocKey, Data#data{windows = UpdatedWindows,
                        prev_record_timestamp = UpdatedPrevRecordTimestamp}, TmpPersistenceCtx);
                ({split_current_record, {Windows1, Windows2, SplitTimestamp}}, TmpPersistenceCtx) ->
                    {CreatedRecordKey, CreatedRecord, UpdatedTmpPersistenceCtx} = split_record(DataDocKey, Data,
                        Windows1, Windows2, SplitTimestamp, DataDocPosition, DocSplittingStrategy, TmpPersistenceCtx),
                    maybe_delete_last_doc(CreatedRecordKey, CreatedRecord, PrevRecordKey, PrevRecordData,
                        DocSplittingStrategy, DataDocPosition + 2, UpdatedTmpPersistenceCtx);
                ({update_previos_record, UpdatedWindowsInPrevRecord}, TmpPersistenceCtx) ->
                    histogram_persistence:update(PrevRecordKey,
                        PrevRecordData#data{windows = UpdatedWindowsInPrevRecord}, TmpPersistenceCtx)
            end, UpdatedPersistenceCtx, Actions);
        false ->
            histogram_persistence:update(DataDocKey, Data#data{windows = WindowsWithAppliedPoint}, PersistenceCtx)
    end.


-spec maybe_delete_last_doc(key(), data(), key(), data() | undefined, doc_splitting_strategy(), non_neg_integer(),
    histogram_persistence:ctx()) -> histogram_persistence:ctx().
maybe_delete_last_doc(NextRecordKey, NextRecordData, Key, _Data,
    #doc_splitting_strategy{max_docs_count = MaxCount}, DocumentNumber, PersistenceCtx) when DocumentNumber > MaxCount ->
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


-spec get_metrics_values(data(), histogram_windows:timestamp() | undefined, histogram_windows:options(),
    histogram_persistence:ctx()) -> {[histogram_windows:window()], histogram_persistence:ctx()}.
get_metrics_values(
    #data{
        windows = Windows,
        prev_record = undefined
    }, Window, Options, PersistenceCtx) ->
    {_, Points} = histogram_windows:get(Windows, Window, Options),
    {Points, PersistenceCtx};

get_metrics_values(
    #data{
        prev_record = PrevRecordKey,
        prev_record_timestamp = PrevRecordTimestamp
    }, Window, Options, PersistenceCtx)
    when PrevRecordTimestamp >= Window ->
    {PrevRecordData, UpdatedPersistenceCtx} = histogram_persistence:get(PrevRecordKey, PersistenceCtx),
    get_metrics_values(PrevRecordData, Window, Options, UpdatedPersistenceCtx);

get_metrics_values(
    #data{
        windows = Windows,
        prev_record = PrevRecordKey
    }, Window, Options, PersistenceCtx) ->
    case histogram_windows:get(Windows, Window, Options) of
        {ok, Points} ->
            {Points, PersistenceCtx};
        {{continue, NewOptions}, Points} ->
            {PrevRecordData, UpdatedPersistenceCtx} = histogram_persistence:get(PrevRecordKey, PersistenceCtx),
            {NextPoints, FinalPersistenceCtx} =
                get_metrics_values(PrevRecordData, undefined, NewOptions, UpdatedPersistenceCtx),
            {Points ++ NextPoints, FinalPersistenceCtx}
    end.


%%=====================================================================
%% Helper functions
%%=====================================================================

-spec create_doc_splitting_strategies(time_series_config()) -> #{full_metrics_id() => doc_splitting_strategy()}.
create_doc_splitting_strategies(ConfigMap) ->
    FlattenedMap = maps:fold(fun(TimeSeriesId, MetricsConfigs, Acc) ->
        maps:fold(fun
            (_, #histogram_config{max_windows_count = WindowsCount}, _) when WindowsCount =< 0 ->
                throw({error, empty_metrics});
            (_, #histogram_config{window_size = WindowSize}, _) when WindowSize =< 0 ->
                throw({error, wrong_window_size});
            (MetricsId, Config, InternalAcc) ->
                InternalAcc#{{TimeSeriesId, MetricsId} => Config}
        end, Acc, MetricsConfigs)
    end, #{}, ConfigMap),

    MaxValuesInDoc = ?MAX_VALUES_IN_DOC,
    MaxWindowsInHeadMap = calculate_windows_in_head_doc_count(FlattenedMap),
    maps:map(fun(Key, MaxWindowsInHead) ->
        #histogram_config{max_windows_count = MaxWindowsCount} = maps:get(Key, FlattenedMap),
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
        #doc_splitting_strategy{
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


-spec update_windows_in_head_doc_count([full_metrics_id()], windows_count_map(), windows_count_map(), flat_config_map(),
    non_neg_integer(), non_neg_integer()) -> {windows_count_map(), windows_count_map(), non_neg_integer()}.
update_windows_in_head_doc_count(_MetricsKeys, FullyStoredInHead, NotFullyStoredInHead, _FlattenedMap, _LimitUpdate, 0) ->
    {FullyStoredInHead, NotFullyStoredInHead, 0};
update_windows_in_head_doc_count([], FullyStoredInHead, NotFullyStoredInHead, _FlattenedMap,
    _LimitUpdate, RemainingWindowsInHead) ->
    {FullyStoredInHead, NotFullyStoredInHead, RemainingWindowsInHead};
update_windows_in_head_doc_count([Key | MetricsKeys], FullyStoredInHead, NotFullyStoredInHead, FlattenedMap,
    LimitUpdate, RemainingWindowsInHead) ->
    Update = min(LimitUpdate, RemainingWindowsInHead),
    #histogram_config{max_windows_count = MaxWindowsCount} = maps:get(Key, FlattenedMap),
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


-spec get_window(histogram_windows:timestamp() | undefined, config()) -> histogram_windows:timestamp() | undefined.
get_window(undefined, _) ->
    undefined;
get_window(Time, #histogram_config{window_size = WindowSize}) ->
    Time - Time rem WindowSize.


-spec get_max_windows_and_split_point(key(), doc_splitting_strategy(), histogram_persistence:ctx()) ->
    {non_neg_integer(), non_neg_integer()}.
get_max_windows_and_split_point(
    DataDocKey,
    #doc_splitting_strategy{
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
    histogram_windows:timestamp(), non_neg_integer(), doc_splitting_strategy(), histogram_persistence:ctx()) ->
    {key() | undefined, data() | undefined, histogram_persistence:ctx()}.
split_record(DataDocKey, Data, Windows1, _Windows2, SplitTimestamp, MaxCount,
    #doc_splitting_strategy{max_docs_count = MaxCount}, PersistenceCtx) ->
    UpdatedData = Data#data{windows = Windows1, prev_record_timestamp = SplitTimestamp},
    {undefined, undefined, histogram_persistence:update(DataDocKey, UpdatedData, PersistenceCtx)};
split_record(DataDocKey, Data, Windows1, Windows2, SplitTimestamp, _DocumentNumber, _DocSplittingStrategy, PersistenceCtx) ->
    DataToCreate = Data#data{windows = Windows2},
    {CreatedRecordKey, UpdatedPersistenceCtx} = histogram_persistence:create(DataToCreate, PersistenceCtx),
    UpdatedData = Data#data{windows = Windows1, prev_record = CreatedRecordKey, prev_record_timestamp = SplitTimestamp},
    {CreatedRecordKey, DataToCreate, histogram_persistence:update(DataDocKey, UpdatedData, UpdatedPersistenceCtx)}.