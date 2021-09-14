%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for time_series module operating on single metric.
%%% Metric is described by #metric{} record containing among others
%%% #data{} records that stores windows. If there are too many windows
%%% to store in single #data{} record, further #data{} records are added
%%% forming linked list of records. ts_persistence module is then
%%% responsible for persisting #metric{} and #data{} records into datastore.
%%%
%%% NOTE: first record (head) of #data{} records linked list is unique as it
%%% is stored inside #metric{} record and all #metric{} records are
%%% persisted in single datastore document (see ts_persistence).
%%% Thus, capacity of head record and other (tail) #data{} records can differ.
%%% Other records (tail) of #data{} records linked list are not wrapped
%%% in #metric{} record.
%%% @end
%%%-------------------------------------------------------------------
-module(ts_metric).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_time_series.hrl").
-include("modules/datastore/ts_metric_config.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([update/4, list/3, delete/2]).

-type id() :: binary().
-type metric() :: #metric{}.
-type legend() :: binary().
-type config() :: #metric_config{}.
-type splitting_strategy() :: #splitting_strategy{}.
-type data() :: #data{}.

-export_type([id/0, metric/0, legend/0, config/0, splitting_strategy/0, data/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec update(metric(), ts_windows:timestamp(), ts_windows:value(), ts_persistence:ctx()) ->
    ts_persistence:ctx().
update(#metric{
    config = #metric_config{aggregator = Aggregator} = Config,
    splitting_strategy = DocSplittingStrategy,
    head_data = Data
}, NewTimestamp, NewValue, PersistenceCtx) ->
    WindowToBeUpdated = get_window(NewTimestamp, Config),
    DataRecordKey = ts_persistence:get_time_series_collection_id(PersistenceCtx),
    update(Data, DataRecordKey, 1, DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx).


-spec list(metric(), ts_windows:list_options(), ts_persistence:ctx()) -> {[ts_windows:window()], ts_persistence:ctx()}.
list(#metric{
    head_data = Data,
    config = Config
}, Options, PersistenceCtx) ->
    Window = get_window(maps:get(start, Options, undefined), Config),
    list(Data, Window, Options, PersistenceCtx).


-spec delete(metric(), ts_persistence:ctx()) -> ts_persistence:ctx().
delete(#metric{splitting_strategy = #splitting_strategy{max_docs_count = 1}}, PersistenceCtx) ->
    PersistenceCtx;
delete(#metric{head_data = #data{prev_record = undefined}}, PersistenceCtx) ->
    PersistenceCtx;
delete(#metric{head_data = #data{prev_record = PrevRecordKey}}, PersistenceCtx) ->
    {PrevRecordData, UpdatedPersistenceCtx} = ts_persistence:get(PrevRecordKey, PersistenceCtx),
    delete(PrevRecordKey, PrevRecordData, UpdatedPersistenceCtx).


%%=====================================================================
%% Internal functions
%%=====================================================================

-spec update(data(), ts_metric_data:key(), non_neg_integer(), splitting_strategy(), ts_windows:aggregator(),
    ts_windows:timestamp(), ts_windows:value(), ts_persistence:ctx()) -> ts_persistence:ctx().
update(
    #data{
        windows = Windows
    } = Data, DataRecordKey, _DataDocPosition,
    #splitting_strategy{
        max_docs_count = 1,
        max_windows_in_head_doc = MaxWindowsCount
    }, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    % All windows are stored in single record - update it
    UpdatedWindows = ts_windows:aggregate(Windows, WindowToBeUpdated, NewValue, Aggregator),
    FinalWindows = ts_windows:prune_overflowing(UpdatedWindows, MaxWindowsCount),
    ts_persistence:update(DataRecordKey, Data#data{windows = FinalWindows}, PersistenceCtx);

update(
    #data{
        prev_record = undefined,
        prev_record_timestamp = PrevRecordTimestamp
    }, _DataRecordKey, _DataDocPosition,
    _DocSplittingStrategy, _Aggregator, WindowToBeUpdated, _NewValue, PersistenceCtx)
    when PrevRecordTimestamp =/= undefined andalso PrevRecordTimestamp >= WindowToBeUpdated ->
    % There are too many newer windows - skip it
    PersistenceCtx;

update(
    #data{
        prev_record = undefined,
        windows = Windows
    } = Data, DataRecordKey, DataDocPosition,
    DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    % Updating last record
    UpdatedWindows = ts_windows:aggregate(Windows, WindowToBeUpdated, NewValue, Aggregator),
    {MaxWindowsCount, SplitPosition} = get_max_windows_and_split_position(
        DataRecordKey, DocSplittingStrategy, PersistenceCtx),

    case ts_windows:is_size_exceeded(UpdatedWindows, MaxWindowsCount) of
        true ->
            % Prev record is undefined so windows should be slitted
            {Windows1, Windows2, SplitTimestamp} = ts_windows:split(UpdatedWindows, SplitPosition),
            {_, _, UpdatedPersistenceCtx} = split_record(DataRecordKey, Data,
                Windows1, Windows2, SplitTimestamp, DataDocPosition, DocSplittingStrategy, PersistenceCtx),
            UpdatedPersistenceCtx;
        false ->
            ts_persistence:update(DataRecordKey, Data#data{windows = UpdatedWindows}, PersistenceCtx)
    end;

update(
    #data{
        prev_record = PrevRecordKey,
        prev_record_timestamp = PrevRecordTimestamp
    }, _DataRecordKey, DataDocPosition,
    DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx)
    when PrevRecordTimestamp >= WindowToBeUpdated ->
    % Window should be stored in one one previous records
    {PrevRecordData, UpdatedPersistenceCtx} = ts_persistence:get(PrevRecordKey, PersistenceCtx),
    update(PrevRecordData, PrevRecordKey, DataDocPosition + 1,
        DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, UpdatedPersistenceCtx);

update(
    #data{
        windows = Windows,
        prev_record = PrevRecordKey
    } = Data, DataRecordKey, DataDocPosition,
    #splitting_strategy{
        max_windows_in_tail_doc = MaxWindowsInTail
    } = DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    % Updating record in the middle of records' list (prev_record is not undefined)
    WindowsWithAggregatedMeasurement = ts_windows:aggregate(Windows, WindowToBeUpdated, NewValue, Aggregator),
    {MaxWindowsCount, SplitPosition} = get_max_windows_and_split_position(
        DataRecordKey, DocSplittingStrategy, PersistenceCtx),

    case ts_windows:is_size_exceeded(WindowsWithAggregatedMeasurement, MaxWindowsCount) of
        true ->
            {#data{windows = WindowsInPrevRecord} = PrevRecordData, UpdatedPersistenceCtx} =
                ts_persistence:get(PrevRecordKey, PersistenceCtx),
            % Window size is exceeded - split record or move part of windows to prev record
            % (reorganize_windows will analyse records and decide if record should be spited or windows moved)
            Actions = ts_windows:reorganize(
                WindowsInPrevRecord, WindowsWithAggregatedMeasurement, MaxWindowsInTail, SplitPosition),

            lists:foldl(fun
                ({update_current_record, UpdatedPrevRecordTimestamp, UpdatedWindows}, TmpPersistenceCtx) ->
                    ts_persistence:update(DataRecordKey, Data#data{windows = UpdatedWindows,
                        prev_record_timestamp = UpdatedPrevRecordTimestamp}, TmpPersistenceCtx);
                ({split_current_record, {Windows1, Windows2, SplitTimestamp}}, TmpPersistenceCtx) ->
                    {CreatedRecordKey, CreatedRecord, UpdatedTmpPersistenceCtx} = split_record(DataRecordKey, Data,
                        Windows1, Windows2, SplitTimestamp, DataDocPosition, DocSplittingStrategy, TmpPersistenceCtx),
                    prune_overflowing_record(CreatedRecordKey, CreatedRecord, PrevRecordKey, PrevRecordData,
                        DocSplittingStrategy, DataDocPosition + 2, UpdatedTmpPersistenceCtx);
                ({update_previous_record, UpdatedWindowsInPrevRecord}, TmpPersistenceCtx) ->
                    ts_persistence:update(PrevRecordKey,
                        PrevRecordData#data{windows = UpdatedWindowsInPrevRecord}, TmpPersistenceCtx)
            end, UpdatedPersistenceCtx, Actions);
        false ->
            ts_persistence:update(
                DataRecordKey, Data#data{windows = WindowsWithAggregatedMeasurement}, PersistenceCtx)
    end.


-spec prune_overflowing_record(ts_metric_data:key(), data(), ts_metric_data:key(), data() | undefined,
    splitting_strategy(), non_neg_integer(), ts_persistence:ctx()) -> ts_persistence:ctx().
prune_overflowing_record(NextRecordKey, NextRecordData, Key, _Data,
    #splitting_strategy{max_docs_count = MaxCount}, DocumentNumber, PersistenceCtx) when DocumentNumber > MaxCount ->
    UpdatedNextRecordData = NextRecordData#data{prev_record = undefined},
    UpdatedPersistenceCtx = ts_persistence:update(NextRecordKey, UpdatedNextRecordData, PersistenceCtx),
    ts_persistence:delete(Key, UpdatedPersistenceCtx);
prune_overflowing_record(NextRecordKey, NextRecordData, Key, undefined,
    DocSplittingStrategy, DocumentNumber, PersistenceCtx) ->
    {Data, UpdatedPersistenceCtx} = ts_persistence:get(Key, PersistenceCtx),
    prune_overflowing_record(NextRecordKey, NextRecordData, Key, Data,
        DocSplittingStrategy, DocumentNumber, UpdatedPersistenceCtx);
prune_overflowing_record(_NextRecordKey, _NextRecordData, _Key, #data{prev_record = undefined},
    _DocSplittingStrategy, _DocumentNumber, PersistenceCtx) ->
    PersistenceCtx;
prune_overflowing_record(_NextRecordKey, _NextRecordData, Key, #data{prev_record = PrevRecordKey} = Data,
    DocSplittingStrategy, DocumentNumber, PersistenceCtx) ->
    prune_overflowing_record(Key, Data, PrevRecordKey, undefined, DocSplittingStrategy, DocumentNumber + 1, PersistenceCtx).


-spec list(data(), ts_windows:timestamp() | undefined, ts_windows:list_options(),
    ts_persistence:ctx()) -> {[ts_windows:window()], ts_persistence:ctx()}.
list(
    #data{
        windows = Windows,
        prev_record = undefined
    }, Window, Options, PersistenceCtx) ->
    {_, WindowsToReturn} = ts_windows:list(Windows, Window, Options),
    {WindowsToReturn, PersistenceCtx};

list(
    #data{
        prev_record = PrevRecordKey,
        prev_record_timestamp = PrevRecordTimestamp
    }, Window, Options, PersistenceCtx)
    when PrevRecordTimestamp >= Window ->
    {PrevRecordData, UpdatedPersistenceCtx} = ts_persistence:get(PrevRecordKey, PersistenceCtx),
    list(PrevRecordData, Window, Options, UpdatedPersistenceCtx);

list(
    #data{
        windows = Windows,
        prev_record = PrevRecordKey
    }, Window, Options, PersistenceCtx) ->
    case ts_windows:list(Windows, Window, Options) of
        {ok, WindowsToReturn} ->
            {WindowsToReturn, PersistenceCtx};
        {{continue, NewOptions}, WindowsToReturn} ->
            {PrevRecordData, UpdatedPersistenceCtx} = ts_persistence:get(PrevRecordKey, PersistenceCtx),
            {NextWindowsToReturn, FinalPersistenceCtx} =
                list(PrevRecordData, undefined, NewOptions, UpdatedPersistenceCtx),
            {WindowsToReturn ++ NextWindowsToReturn, FinalPersistenceCtx}
    end.


-spec delete(ts_metric_data:key(), data(), ts_persistence:ctx()) -> ts_persistence:ctx().
delete(DataRecordKey, #data{prev_record = undefined}, PersistenceCtx) ->
    ts_persistence:delete(DataRecordKey, PersistenceCtx);
delete(DataRecordKey, #data{prev_record = PrevRecordKey}, PersistenceCtx) ->
    {PrevRecordData, UpdatedPersistenceCtx} = ts_persistence:get(PrevRecordKey, PersistenceCtx),
    UpdatedPersistenceCtx2 = ts_persistence:delete(DataRecordKey, UpdatedPersistenceCtx),
    delete(PrevRecordKey, PrevRecordData, UpdatedPersistenceCtx2).


-spec get_window(ts_windows:timestamp() | undefined, config()) -> ts_windows:timestamp() | undefined.
get_window(undefined, _) ->
    undefined;
get_window(Time, #metric_config{resolution = Resolution}) ->
    Time - Time rem Resolution.


-spec get_max_windows_and_split_position(ts_metric_data:key(), splitting_strategy(), ts_persistence:ctx()) ->
    {non_neg_integer(), non_neg_integer()}.
get_max_windows_and_split_position(
    DataRecordKey,
    #splitting_strategy{
        max_windows_in_head_doc = MaxWindowsCountInHead,
        max_windows_in_tail_doc = MaxWindowsCountInTail
    },
    PersistenceCtx) ->
    case ts_persistence:is_hub_key(DataRecordKey, PersistenceCtx) of
        true ->
            % If adding of single window results in reorganization split should be at first element
            % to move most of windows to tail doc
            {MaxWindowsCountInHead, 1};
        false ->
            % Split of tail doc should result in two documents with at list of half of capacity used
            {MaxWindowsCountInTail, ceil(MaxWindowsCountInTail / 2)}
    end.


-spec split_record(ts_metric_data:key(), data(), ts_windows:windows(), ts_windows:windows(),
    ts_windows:timestamp(), non_neg_integer(), splitting_strategy(), ts_persistence:ctx()) ->
    {ts_metric_data:key() | undefined, data() | undefined, ts_persistence:ctx()}.
split_record(DataRecordKey, Data, Windows1, _Windows2, SplitTimestamp, MaxCount,
    #splitting_strategy{max_docs_count = MaxCount}, PersistenceCtx) ->
    % Splitting last record - do not create new record for older windows as it would be deleted immediately
    UpdatedData = Data#data{windows = Windows1, prev_record_timestamp = SplitTimestamp},
    {undefined, undefined, ts_persistence:update(DataRecordKey, UpdatedData, PersistenceCtx)};
split_record(DataRecordKey, Data, Windows1, Windows2, SplitTimestamp, _DocumentNumber, _DocSplittingStrategy, PersistenceCtx) ->
    DataToCreate = Data#data{windows = Windows2},
    {CreatedRecordKey, UpdatedPersistenceCtx} = ts_persistence:create(DataToCreate, PersistenceCtx),
    UpdatedData = Data#data{windows = Windows1, prev_record = CreatedRecordKey, prev_record_timestamp = SplitTimestamp},
    {CreatedRecordKey, DataToCreate, ts_persistence:update(DataRecordKey, UpdatedData, UpdatedPersistenceCtx)}.