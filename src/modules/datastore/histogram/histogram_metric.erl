%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for histogram_time_series operating on single metric.
%%% Metric is described by #metric{} record containing among others
%%% #data{} records that stores windows. If there are too many windows
%%% to store in single #data{} record, further #data{} records are added
%%% forming linked list of records. histogram_persistence module is then
%%% responsible for persisting #metric{} and #data{} records into datastore.
%%%
%%% NOTE: first record (head) of #data{} records linked list is unique as it
%%% is stored inside #metric{} record and all #metric{} records are
%%% persisted in single datastore document (see histogram_persistence).
%%% Thus, capacity of head record and other (tail) #data{} records can differ.
%%% Other records (tail) of #data{} records linked list are not wrapped
%%% in #metric{} record.
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_metric).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_histogram.hrl").
-include("modules/datastore/metric_config.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([update/4, get/3, delete/2]).

-type id() :: binary().
-type metric() :: #metric{}.
-type legend() :: binary().
-type config() :: #metric_config{}.
-type splitting_strategy() :: #splitting_strategy{}.
-type data() :: #data{}.

-type key() :: datastore:key().

-export_type([id/0, metric/0, legend/0, config/0, splitting_strategy/0, data/0, key/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec update(metric(), histogram_windows:timestamp(), histogram_windows:value(), histogram_persistence:ctx()) ->
    histogram_persistence:ctx().
update(#metric{
    config = #metric_config{aggregator = Aggregator} = Config,
    splitting_strategy = DocSplittingStrategy,
    data = Data
}, NewTimestamp, NewValue, PersistenceCtx) ->
    WindowToBeUpdated = get_window(NewTimestamp, Config),
    DataRecordKey = histogram_persistence:get_histogram_id(PersistenceCtx),
    update(Data, DataRecordKey, 1, DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx).


-spec get(metric(), histogram_windows:get_options(), histogram_persistence:ctx()) ->
    {[histogram_windows:window()], histogram_persistence:ctx()}.
get(#metric{
    data = Data,
    config = Config
}, Options, PersistenceCtx) ->
    Window = get_window(maps:get(start, Options, undefined), Config),
    get(Data, Window, Options, PersistenceCtx).


-spec delete(metric(), histogram_persistence:ctx()) -> histogram_persistence:ctx().
delete(#metric{splitting_strategy = #splitting_strategy{max_docs_count = 1}}, PersistenceCtx) ->
    PersistenceCtx;
delete(#metric{data = #data{prev_record = undefined}}, PersistenceCtx) ->
    PersistenceCtx;
delete(#metric{data = #data{prev_record = PrevRecordKey}}, PersistenceCtx) ->
    {PrevRecordData, UpdatedPersistenceCtx} = histogram_persistence:get(PrevRecordKey, PersistenceCtx),
    delete(PrevRecordKey, PrevRecordData, UpdatedPersistenceCtx).


%%=====================================================================
%% Internal functions
%%=====================================================================

-spec update(data(), key(), non_neg_integer(), splitting_strategy(), histogram_windows:aggregator(),
    histogram_windows:timestamp(), histogram_windows:value(), histogram_persistence:ctx()) -> histogram_persistence:ctx().
update(
    #data{
        windows = Windows
    } = Data, DataRecordKey, _DataDocPosition,
    #splitting_strategy{
        max_docs_count = 1,
        max_windows_in_head_doc = MaxWindowsCount
    }, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    % All windows are stored in single record - update it
    UpdatedWindows = histogram_windows:aggregate(Windows, WindowToBeUpdated, NewValue, Aggregator),
    FinalWindows = histogram_windows:prune_overflowing_windows(UpdatedWindows, MaxWindowsCount),
    histogram_persistence:update(DataRecordKey, Data#data{windows = FinalWindows}, PersistenceCtx);

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
    UpdatedWindows = histogram_windows:aggregate(Windows, WindowToBeUpdated, NewValue, Aggregator),
    {MaxWindowsCount, SplitPosition} = get_max_windows_and_split_position(
        DataRecordKey, DocSplittingStrategy, PersistenceCtx),

    case histogram_windows:should_reorganize_windows(UpdatedWindows, MaxWindowsCount) of
        true ->
            % Prev record is undefined so splitting is only possible way of reorganization
            {Windows1, Windows2, SplitTimestamp} = histogram_windows:split_windows(UpdatedWindows, SplitPosition),
            {_, _, UpdatedPersistenceCtx} = split_record(DataRecordKey, Data,
                Windows1, Windows2, SplitTimestamp, DataDocPosition, DocSplittingStrategy, PersistenceCtx),
            UpdatedPersistenceCtx;
        false ->
            histogram_persistence:update(DataRecordKey, Data#data{windows = UpdatedWindows}, PersistenceCtx)
    end;

update(
    #data{
        prev_record = PrevRecordKey,
        prev_record_timestamp = PrevRecordTimestamp
    }, _DataRecordKey, DataDocPosition,
    DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx)
    when PrevRecordTimestamp >= WindowToBeUpdated ->
    % Window should be stored in one one previous records
    {PrevRecordData, UpdatedPersistenceCtx} = histogram_persistence:get(PrevRecordKey, PersistenceCtx),
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
    % Updating record in the middles of records' list (prev_record is not undefined)
    WindowsWithAggregatedMeasurement = histogram_windows:aggregate(Windows, WindowToBeUpdated, NewValue, Aggregator),
    {MaxWindowsCount, SplitPosition} = get_max_windows_and_split_position(
        DataRecordKey, DocSplittingStrategy, PersistenceCtx),

    case histogram_windows:should_reorganize_windows(WindowsWithAggregatedMeasurement, MaxWindowsCount) of
        true ->
            {#data{windows = WindowsInPrevRecord} = PrevRecordData, UpdatedPersistenceCtx} =
                histogram_persistence:get(PrevRecordKey, PersistenceCtx),
            Actions = histogram_windows:reorganize_windows(
                WindowsInPrevRecord, WindowsWithAggregatedMeasurement, MaxWindowsInTail, SplitPosition),

            lists:foldl(fun
                ({update_current_record, UpdatedPrevRecordTimestamp, UpdatedWindows}, TmpPersistenceCtx) ->
                    histogram_persistence:update(DataRecordKey, Data#data{windows = UpdatedWindows,
                        prev_record_timestamp = UpdatedPrevRecordTimestamp}, TmpPersistenceCtx);
                ({split_current_record, {Windows1, Windows2, SplitTimestamp}}, TmpPersistenceCtx) ->
                    {CreatedRecordKey, CreatedRecord, UpdatedTmpPersistenceCtx} = split_record(DataRecordKey, Data,
                        Windows1, Windows2, SplitTimestamp, DataDocPosition, DocSplittingStrategy, TmpPersistenceCtx),
                    prune_overflowing_record(CreatedRecordKey, CreatedRecord, PrevRecordKey, PrevRecordData,
                        DocSplittingStrategy, DataDocPosition + 2, UpdatedTmpPersistenceCtx);
                ({update_previous_record, UpdatedWindowsInPrevRecord}, TmpPersistenceCtx) ->
                    histogram_persistence:update(PrevRecordKey,
                        PrevRecordData#data{windows = UpdatedWindowsInPrevRecord}, TmpPersistenceCtx)
            end, UpdatedPersistenceCtx, Actions);
        false ->
            histogram_persistence:update(
                DataRecordKey, Data#data{windows = WindowsWithAggregatedMeasurement}, PersistenceCtx)
    end.


-spec prune_overflowing_record(key(), data(), key(), data() | undefined, splitting_strategy(), non_neg_integer(),
    histogram_persistence:ctx()) -> histogram_persistence:ctx().
prune_overflowing_record(NextRecordKey, NextRecordData, Key, _Data,
    #splitting_strategy{max_docs_count = MaxCount}, DocumentNumber, PersistenceCtx) when DocumentNumber > MaxCount ->
    UpdatedNextRecordData = NextRecordData#data{prev_record = undefined},
    UpdatedPersistenceCtx = histogram_persistence:update(NextRecordKey, UpdatedNextRecordData, PersistenceCtx),
    histogram_persistence:delete(Key, UpdatedPersistenceCtx);
prune_overflowing_record(NextRecordKey, NextRecordData, Key, undefined,
    DocSplittingStrategy, DocumentNumber, PersistenceCtx) ->
    {Data, UpdatedPersistenceCtx} = histogram_persistence:get(Key, PersistenceCtx),
    prune_overflowing_record(NextRecordKey, NextRecordData, Key, Data,
        DocSplittingStrategy, DocumentNumber, UpdatedPersistenceCtx);
prune_overflowing_record(_NextRecordKey, _NextRecordData, _Key, #data{prev_record = undefined},
    _DocSplittingStrategy, _DocumentNumber, PersistenceCtx) ->
    PersistenceCtx;
prune_overflowing_record(_NextRecordKey, _NextRecordData, Key, #data{prev_record = PrevRecordKey} = Data,
    DocSplittingStrategy, DocumentNumber, PersistenceCtx) ->
    prune_overflowing_record(Key, Data, PrevRecordKey, undefined, DocSplittingStrategy, DocumentNumber + 1, PersistenceCtx).


-spec get(data(), histogram_windows:timestamp() | undefined, histogram_windows:get_options(),
    histogram_persistence:ctx()) -> {[histogram_windows:window()], histogram_persistence:ctx()}.
get(
    #data{
        windows = Windows,
        prev_record = undefined
    }, Window, Options, PersistenceCtx) ->
    {_, WindowsToReturn} = histogram_windows:get(Windows, Window, Options),
    {WindowsToReturn, PersistenceCtx};

get(
    #data{
        prev_record = PrevRecordKey,
        prev_record_timestamp = PrevRecordTimestamp
    }, Window, Options, PersistenceCtx)
    when PrevRecordTimestamp >= Window ->
    {PrevRecordData, UpdatedPersistenceCtx} = histogram_persistence:get(PrevRecordKey, PersistenceCtx),
    get(PrevRecordData, Window, Options, UpdatedPersistenceCtx);

get(
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
                get(PrevRecordData, undefined, NewOptions, UpdatedPersistenceCtx),
            {WindowsToReturn ++ NextWindowsToReturn, FinalPersistenceCtx}
    end.


-spec delete(key(), data(), histogram_persistence:ctx()) -> histogram_persistence:ctx().
delete(DataRecordKey, #data{prev_record = undefined}, PersistenceCtx) ->
    histogram_persistence:delete(DataRecordKey, PersistenceCtx);
delete(DataRecordKey, #data{prev_record = PrevRecordKey}, PersistenceCtx) ->
    {PrevRecordData, UpdatedPersistenceCtx} = histogram_persistence:get(PrevRecordKey, PersistenceCtx),
    UpdatedPersistenceCtx2 = histogram_persistence:delete(DataRecordKey, UpdatedPersistenceCtx),
    delete(PrevRecordKey, PrevRecordData, UpdatedPersistenceCtx2).


-spec get_window(histogram_windows:timestamp() | undefined, config()) -> histogram_windows:timestamp() | undefined.
get_window(undefined, _) ->
    undefined;
get_window(Time, #metric_config{resolution = Resolution}) ->
    Time - Time rem Resolution.


-spec get_max_windows_and_split_position(key(), splitting_strategy(), histogram_persistence:ctx()) ->
    {non_neg_integer(), non_neg_integer()}.
get_max_windows_and_split_position(
    DataRecordKey,
    #splitting_strategy{
        max_windows_in_head_doc = MaxWindowsCountInHead,
        max_windows_in_tail_doc = MaxWindowsCountInTail
    },
    PersistenceCtx) ->
    case histogram_persistence:is_hub_key(DataRecordKey, PersistenceCtx) of
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
split_record(DataRecordKey, Data, Windows1, _Windows2, SplitTimestamp, MaxCount,
    #splitting_strategy{max_docs_count = MaxCount}, PersistenceCtx) ->
    % Splitting last record - do not create new record for older windows as it would be deleted immediately
    UpdatedData = Data#data{windows = Windows1, prev_record_timestamp = SplitTimestamp},
    {undefined, undefined, histogram_persistence:update(DataRecordKey, UpdatedData, PersistenceCtx)};
split_record(DataRecordKey, Data, Windows1, Windows2, SplitTimestamp, _DocumentNumber, _DocSplittingStrategy, PersistenceCtx) ->
    DataToCreate = Data#data{windows = Windows2},
    {CreatedRecordKey, UpdatedPersistenceCtx} = histogram_persistence:create(DataToCreate, PersistenceCtx),
    UpdatedData = Data#data{windows = Windows1, prev_record = CreatedRecordKey, prev_record_timestamp = SplitTimestamp},
    {CreatedRecordKey, DataToCreate, histogram_persistence:update(DataRecordKey, UpdatedData, UpdatedPersistenceCtx)}.