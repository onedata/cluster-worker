%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for time_series_collection module operating on single
%%% metric. Metric is described by #metric{} record containing among others
%%% #data_node{} records that stores windows. If there are too many windows
%%% to store in single #data_node{} record, further #data_node{} records are added
%%% forming linked list of records. ts_persistence module is then
%%% responsible for persisting #metric{} and #data_node{} records into datastore.
%%%
%%% NOTE: first record (head) of #data_node{} records linked list is unique as it
%%% is stored inside #metric{} record and all #metric{} records are
%%% persisted in single datastore document (see ts_persistence).
%%% Thus, capacity of head record and other (tail) #data_node{} records can differ.
%%% Other records (tail) of #data_node{} records linked list are not wrapped
%%% in #metric{} record.
%%% @end
%%%-------------------------------------------------------------------
-module(ts_metric).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_time_series.hrl").
-include_lib("ctool/include/time_series/common.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([build/2, insert/4, consume_measurements/4, list_windows/4, delete_data_nodes/3,
    reconfigure/4, delete/3, generate_dump/2, create_from_dump/4]).

-type record() :: #metric{}.
-type splitting_strategy() :: #splitting_strategy{}.
-type data_node() :: #data_node{}.
-type dump() :: #metric_dump{}.

-export_type([record/0, splitting_strategy/0, data_node/0, dump/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec build(metric_config:record(), ts_metric:splitting_strategy()) -> record().
build(Config, DocSplittingStrategy) ->
    #metric{
        config = Config,
        splitting_strategy = DocSplittingStrategy
    }.


-spec insert(
    time_series:name(),
    time_series:metric_name(),
    record(),
    ts_persistence:ctx()
) ->
    ts_persistence:ctx().
insert(TimeSeriesName, MetricName, Metric, PersistenceCtx) ->
    ts_persistence:insert_metric(Metric, set_as_currently_processed(TimeSeriesName, MetricName, PersistenceCtx)).


-spec consume_measurements(
    time_series:name(),
    time_series:metric_name(),
    [ts_window:measurement()],
    ts_persistence:ctx()
) ->
    ts_persistence:ctx().
consume_measurements(TimeSeriesName, MetricName, Measurements, PersistenceCtx0) ->
    PersistenceCtx1 = set_as_currently_processed(TimeSeriesName, MetricName, PersistenceCtx0),
    lists:foldl(fun({NewTimestamp, _NewValue} = Measurement, PersistenceAcc) ->
        #metric{
            config = #metric_config{aggregator = Aggregator} = Config,
            splitting_strategy = DocSplittingStrategy,
            head_data = Data
        } = ts_persistence:get_currently_processed_metric(PersistenceAcc),
        WindowToBeUpdated = get_window_id(NewTimestamp, Config),
        DataNodeKey = ts_persistence:get_time_series_collection_id(PersistenceAcc),
        update_window(
            Data,
            DataNodeKey,
            1,
            DocSplittingStrategy,
            {aggregate_measurement, Aggregator},
            WindowToBeUpdated,
            Measurement,
            PersistenceAcc
        )
    end, PersistenceCtx1, Measurements).


-spec list_windows(
    time_series:name(),
    time_series:metric_name(),
    ts_windows:list_options(),
    ts_persistence:ctx()
) ->
    {ts_windows:descending_window_infos_list(), ts_persistence:ctx()}.
list_windows(TimeSeriesName, MetricName, Options, PersistenceCtx0) ->
    PersistenceCtx = set_as_currently_processed(TimeSeriesName, MetricName, PersistenceCtx0),
    list_windows(ts_persistence:get_currently_processed_metric(PersistenceCtx), Options, PersistenceCtx).

%% @private
-spec list_windows(record(), ts_windows:list_options(), ts_persistence:ctx()) ->
    {ts_windows:descending_window_infos_list(), ts_persistence:ctx()}.
list_windows(#metric{
    head_data = Data,
    config = #metric_config{aggregator = Aggregator} = Config
}, Options, PersistenceCtx) ->
    Window = get_window_id(maps:get(start_timestamp, Options, undefined), Config),
    InternalOptions = ts_windows:map_list_options(Options, Aggregator, maps:get(extended_info, Options, false)),
    list_windows_internal(Data, Window, InternalOptions, PersistenceCtx).


-spec delete_data_nodes(
    time_series:name(),
    time_series:metric_name(),
    ts_persistence:ctx()
) ->
    ts_persistence:ctx().
delete_data_nodes(TimeSeriesName, MetricName, PersistenceCtx0) ->
    PersistenceCtx = set_as_currently_processed(TimeSeriesName, MetricName, PersistenceCtx0),
    delete_data_nodes(ts_persistence:get_currently_processed_metric(PersistenceCtx), PersistenceCtx).

%% @private
-spec delete_data_nodes(record(), ts_persistence:ctx()) -> ts_persistence:ctx().
delete_data_nodes(#metric{splitting_strategy = #splitting_strategy{max_docs_count = 1}}, PersistenceCtx) ->
    PersistenceCtx;
delete_data_nodes(#metric{head_data = #data_node{older_node_key = undefined}}, PersistenceCtx) ->
    PersistenceCtx;
delete_data_nodes(#metric{head_data = #data_node{older_node_key = OlderNodeKey}}, PersistenceCtx0) ->
    {OlderDataNode, PersistenceCtx1} = ts_persistence:get(OlderNodeKey, PersistenceCtx0),
    delete_data_nodes_internal(OlderNodeKey, OlderDataNode, PersistenceCtx1).


-spec reconfigure(
    time_series:name(),
    time_series:metric_name(),
    ts_metric:splitting_strategy(),
    ts_persistence:ctx()
) ->
    ts_persistence:ctx().
reconfigure(TimeSeriesName, MetricName, DocSplittingStrategy, PersistenceCtx0) ->
    PersistenceCtx1 = set_as_currently_processed(TimeSeriesName, MetricName, PersistenceCtx0),
    reconfigure(ts_persistence:get_currently_processed_metric(PersistenceCtx1), DocSplittingStrategy, PersistenceCtx1).

%% @private
-spec reconfigure(record(), ts_metric:splitting_strategy(), ts_persistence:ctx()) -> ts_persistence:ctx().
reconfigure(#metric{
    splitting_strategy = DocSplittingStrategy
} = _CurrentMetric, DocSplittingStrategy, PersistenceCtx) ->
    % Doc splitting strategy has not changed - do nothing
    PersistenceCtx;
reconfigure(#metric{
    head_data = #data_node{
        windows = ExistingWindowsSet
    } = Data,
    splitting_strategy = #splitting_strategy{
        max_windows_in_tail_doc = MaxInTail
    }
} = CurrentMetric, #splitting_strategy{
    max_windows_in_tail_doc = MaxInTail
} = NewDocSplittingStrategy, PersistenceCtx) ->
    % Doc splitting strategy has changed for head - get all windows from head, clean windows from head and
    % than set them once more
    NewMetric = CurrentMetric#metric{
        head_data = Data#data_node{
            windows = ts_windows:init() % init cleans all windows from head (they are replaced by new record)
        },
        splitting_strategy = NewDocSplittingStrategy
    },
    PersistenceCtxAfterInit = ts_persistence:insert_metric(NewMetric, PersistenceCtx),
    ExistingWindows = ts_windows:list_full_data(ExistingWindowsSet),
    DataNodeKey = ts_persistence:get_time_series_collection_id(PersistenceCtxAfterInit),
    prepend_sorted_windows(DataNodeKey, NewDocSplittingStrategy, ExistingWindows, PersistenceCtxAfterInit);
reconfigure(#metric{
    head_data = Data,
    config = Config
} = CurrentMetric, NewDocSplittingStrategy, PersistenceCtx) ->
    % Doc splitting strategy has changed - get all windows from head and data nodes, delete all data nodes
    % and clean all windows from head and then set them once more
    {ExistingWindows, UpdatedPersistenceCtx} = list_windows_internal(
        Data, undefined, #{return_type => full_data}, PersistenceCtx),
    PersistenceCtxAfterCleaning = delete_data_nodes(CurrentMetric, UpdatedPersistenceCtx),

    NewMetric = build(Config, NewDocSplittingStrategy), % init cleans all windows from head
    PersistenceCtxAfterInit = ts_persistence:insert_metric(NewMetric, PersistenceCtxAfterCleaning),

    DataNodeKey = ts_persistence:get_time_series_collection_id(PersistenceCtxAfterInit),
    prepend_sorted_windows(DataNodeKey, NewDocSplittingStrategy, ExistingWindows, PersistenceCtxAfterInit).


-spec delete(time_series:name(), time_series:metric_name(), ts_persistence:ctx()) ->
    ts_persistence:ctx().
delete(TimeSeriesName, MetricName, PersistenceCtx) ->
    ts_persistence:delete_metric(set_as_currently_processed(TimeSeriesName, MetricName, PersistenceCtx)).


-spec generate_dump(record(), ts_persistence:ctx()) -> {dump(), ts_persistence:ctx()}.
generate_dump(#metric{
    head_data = #data_node{older_node_key = OlderNodeKey}
} = Metric, PersistenceCtx) ->
    {DataNodes, UpdatedPersistenceCtx} = generate_data_nodes_dump(OlderNodeKey, PersistenceCtx),
    {#metric_dump{head_record = Metric, data_nodes = DataNodes}, UpdatedPersistenceCtx}.


-spec create_from_dump(time_series:name(), time_series:metric_name(), dump(), ts_persistence:ctx()) ->
    ts_persistence:ctx().
create_from_dump(_TimeSeriesName, _MetricName, #metric_dump{data_nodes = []}, PersistenceCtx) ->
    PersistenceCtx;
create_from_dump(TimeSeriesName, MetricName, #metric_dump{
    head_record = #metric{head_data = HeadData}, 
    data_nodes = DataNodes
}, PersistenceCtx) ->
    PersistenceCtx2 = set_as_currently_processed(TimeSeriesName, MetricName, PersistenceCtx),
    {NewOlderNodeKey, PersistenceCtx3} = create_data_nodes_from_dump(DataNodes, PersistenceCtx2),
    DataNodeKey = ts_persistence:get_time_series_collection_id(PersistenceCtx3),
    ts_persistence:update(DataNodeKey, HeadData#data_node{older_node_key = NewOlderNodeKey}, PersistenceCtx3).


%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec update_window(data_node(), ts_metric_data_node:key(), non_neg_integer(), splitting_strategy(),
    ts_windows:insert_strategy(), ts_window:window_id(), ts_window:measurement() | ts_window:window(),
    ts_persistence:ctx()) -> ts_persistence:ctx().
update_window(
    #data_node{
        windows = Windows
    } = Data, DataNodeKey, _DataDocPosition,
    #splitting_strategy{
        max_docs_count = 1,
        max_windows_in_head_doc = MaxWindowsCount
    }, InsertStrategy, WindowToBeUpdated, WindowOrMeasurement, PersistenceCtx) ->
    % All windows are stored in single data node - update it
    UpdatedWindows = ts_windows:insert(Windows, WindowToBeUpdated, WindowOrMeasurement, InsertStrategy),
    FinalWindows = ts_windows:prune_overflowing(UpdatedWindows, MaxWindowsCount),
    ts_persistence:update(DataNodeKey, Data#data_node{windows = FinalWindows}, PersistenceCtx);

update_window(
    #data_node{
        older_node_key = undefined,
        older_node_timestamp = OlderNodeTimestamp
    }, _DataNodeKey, _DataDocPosition,
    _DocSplittingStrategy, _InsertStrategy, WindowToBeUpdated, _WindowOrMeasurement, PersistenceCtx)
    when OlderNodeTimestamp =/= undefined andalso OlderNodeTimestamp >= WindowToBeUpdated ->
    % There are too many newer windows - skip it
    PersistenceCtx;

update_window(
    #data_node{
        older_node_key = undefined,
        windows = Windows
    } = Data, DataNodeKey, DataDocPosition,
    DocSplittingStrategy, InsertStrategy, WindowToBeUpdated, WindowOrMeasurement, PersistenceCtx) ->
    % Updating last data node
    UpdatedWindows = ts_windows:insert(Windows, WindowToBeUpdated, WindowOrMeasurement, InsertStrategy),
    {MaxWindowsCount, SplitPosition} = get_max_windows_and_split_position(
        DataNodeKey, DocSplittingStrategy, PersistenceCtx),

    case ts_windows:is_size_exceeded(UpdatedWindows, MaxWindowsCount) of
        true ->
            % Prev data node is undefined so windows should be slitted
            {Windows1, Windows2, SplitTimestamp} = ts_windows:split(UpdatedWindows, SplitPosition),
            {_, _, UpdatedPersistenceCtx} = split_node(DataNodeKey, Data,
                Windows1, Windows2, SplitTimestamp, DataDocPosition, DocSplittingStrategy, PersistenceCtx),
            UpdatedPersistenceCtx;
        false ->
            ts_persistence:update(DataNodeKey, Data#data_node{windows = UpdatedWindows}, PersistenceCtx)
    end;

update_window(
    #data_node{
        older_node_key = OlderNodeKey,
        older_node_timestamp = OlderNodeTimestamp
    }, _DataNodeKey, DataDocPosition,
    DocSplittingStrategy, InsertStrategy, WindowToBeUpdated, WindowOrMeasurement, PersistenceCtx)
    when OlderNodeTimestamp >= WindowToBeUpdated ->
    % Window should be stored in one one previous data nodes
    {OlderDataNode, UpdatedPersistenceCtx} = ts_persistence:get(OlderNodeKey, PersistenceCtx),
    update_window(OlderDataNode, OlderNodeKey, DataDocPosition + 1,
        DocSplittingStrategy, InsertStrategy, WindowToBeUpdated, WindowOrMeasurement, UpdatedPersistenceCtx);

update_window(
    #data_node{
        windows = Windows,
        older_node_key = OlderNodeKey
    } = Data, DataNodeKey, DataDocPosition,
    #splitting_strategy{
        max_windows_in_tail_doc = MaxWindowsInTail
    } = DocSplittingStrategy, InsertStrategy, WindowToBeUpdated, WindowOrMeasurement, PersistenceCtx) ->
    % Updating data node in the middle of data nodes' list (older_node_key is not undefined)
    WindowsWithAggregatedMeasurement = ts_windows:insert(Windows, WindowToBeUpdated, WindowOrMeasurement, InsertStrategy),
    {MaxWindowsCount, SplitPosition} = get_max_windows_and_split_position(
        DataNodeKey, DocSplittingStrategy, PersistenceCtx),

    case ts_windows:is_size_exceeded(WindowsWithAggregatedMeasurement, MaxWindowsCount) of
        true ->
            {#data_node{windows = WindowsInOlderDataNode} = OlderDataNode, UpdatedPersistenceCtx} =
                ts_persistence:get(OlderNodeKey, PersistenceCtx),
            % Window size is exceeded - split data node or move part of windows to prev data node
            % (reorganize_windows will analyse data nodes and decide if data node should be spited or windows moved)
            Actions = ts_windows:reorganize(
                WindowsInOlderDataNode, WindowsWithAggregatedMeasurement, MaxWindowsInTail, SplitPosition),

            lists:foldl(fun
                ({update_current_data_node, UpdatedOlderNodeTimestamp, UpdatedWindows}, TmpPersistenceCtx) ->
                    ts_persistence:update(DataNodeKey, Data#data_node{windows = UpdatedWindows,
                        older_node_timestamp = UpdatedOlderNodeTimestamp}, TmpPersistenceCtx);
                ({split_current_data_node, {Windows1, Windows2, SplitTimestamp}}, TmpPersistenceCtx) ->
                    {CreatedNodeKey, CreatedDataNode, UpdatedTmpPersistenceCtx} = split_node(DataNodeKey, Data,
                        Windows1, Windows2, SplitTimestamp, DataDocPosition, DocSplittingStrategy, TmpPersistenceCtx),
                    prune_overflowing_node(CreatedNodeKey, CreatedDataNode, OlderNodeKey, OlderDataNode,
                        DocSplittingStrategy, DataDocPosition + 2, UpdatedTmpPersistenceCtx);
                ({update_previous_data_node, UpdatedWindowsInOlderDataNode}, TmpPersistenceCtx) ->
                    ts_persistence:update(OlderNodeKey,
                        OlderDataNode#data_node{windows = UpdatedWindowsInOlderDataNode}, TmpPersistenceCtx)
            end, UpdatedPersistenceCtx, Actions);
        false ->
            ts_persistence:update(
                DataNodeKey, Data#data_node{windows = WindowsWithAggregatedMeasurement}, PersistenceCtx)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function allows setting multiple windows in single call. Same result can be obtained calling
%% multiple times update/8 function. However, prepend_sorted_windows is optimized for prepending sorted
%% sets of windows so it is much faster than multiple update/8 function calls.
%% @end
%%--------------------------------------------------------------------
-spec prepend_sorted_windows(ts_metric_data_node:key(), splitting_strategy(),
    ts_windows:descending_windows_list(), ts_persistence:ctx()) -> ts_persistence:ctx().
prepend_sorted_windows(_DataNodeKey, _DocSplittingStrategy, [], PersistenceCtx) ->
    PersistenceCtx;
prepend_sorted_windows(DataNodeKey,
    #splitting_strategy{
        max_windows_in_head_doc = MaxWindowsCount
    } = DocSplittingStrategy, WindowsToPrepend, PersistenceCtx) ->
    {#data_node{windows = ExistingWindows} = Data, PersistenceCtxAfterGet} =
        ts_persistence:get(DataNodeKey, PersistenceCtx),

    {RemainingWindowsToPrepend, FinalPersistenceCtx} = case
        ts_windows:get_remaining_windows_count(ExistingWindows, MaxWindowsCount) of
        0 ->
            % Use update function to reorganize documents and allow further adding to head
            {Timestamp, WindowValue} = lists:last(WindowsToPrepend),
            UpdatedPersistenceCtx = update_window(Data, DataNodeKey, 1, DocSplittingStrategy, override,
                Timestamp, WindowValue, PersistenceCtxAfterGet),
            {lists:droplast(WindowsToPrepend), UpdatedPersistenceCtx};
        WindowsToUseCount ->
            % Set whole windows batch as windows in argument are sorted
            WindowsCount = length(WindowsToPrepend),
            SplitPosition = WindowsCount - min(WindowsToUseCount, WindowsCount),
            UpdatedExistingWindows = ts_windows:prepend_windows_list(ExistingWindows,
                lists:sublist(WindowsToPrepend, SplitPosition + 1, WindowsCount - SplitPosition)),
            UpdatedPersistenceCtx = ts_persistence:update(
                DataNodeKey, Data#data_node{windows = UpdatedExistingWindows}, PersistenceCtxAfterGet),
            {lists:sublist(WindowsToPrepend, SplitPosition), UpdatedPersistenceCtx}
    end,

    prepend_sorted_windows(DataNodeKey, DocSplittingStrategy, RemainingWindowsToPrepend, FinalPersistenceCtx).


%% @private
-spec prune_overflowing_node(ts_metric_data_node:key(), data_node(), ts_metric_data_node:key(), data_node() | undefined,
    splitting_strategy(), non_neg_integer(), ts_persistence:ctx()) -> ts_persistence:ctx().
prune_overflowing_node(NewerNodeKey, NewerDataNode, Key, _Data,
    #splitting_strategy{max_docs_count = MaxCount}, DocumentNumber, PersistenceCtx) when DocumentNumber > MaxCount ->
    UpdatedNewerDataNode = NewerDataNode#data_node{older_node_key = undefined},
    UpdatedPersistenceCtx = ts_persistence:update(NewerNodeKey, UpdatedNewerDataNode, PersistenceCtx),
    ts_persistence:delete_data_node(Key, UpdatedPersistenceCtx);
prune_overflowing_node(NewerNodeKey, NewerDataNode, Key, undefined,
    DocSplittingStrategy, DocumentNumber, PersistenceCtx) ->
    {Data, UpdatedPersistenceCtx} = ts_persistence:get(Key, PersistenceCtx),
    prune_overflowing_node(NewerNodeKey, NewerDataNode, Key, Data,
        DocSplittingStrategy, DocumentNumber, UpdatedPersistenceCtx);
prune_overflowing_node(_NewerNodeKey, _NewerDataNode, _Key, #data_node{older_node_key = undefined},
    _DocSplittingStrategy, _DocumentNumber, PersistenceCtx) ->
    PersistenceCtx;
prune_overflowing_node(_NewerNodeKey, _NewerDataNode, Key, #data_node{older_node_key = OlderNodeKey} = Data,
    DocSplittingStrategy, DocumentNumber, PersistenceCtx) ->
    prune_overflowing_node(Key, Data, OlderNodeKey, undefined, DocSplittingStrategy, DocumentNumber + 1, PersistenceCtx).


%% @private
-spec list_windows_internal(data_node(), ts_window:window_id() | undefined, ts_windows:internal_list_options(),
    ts_persistence:ctx()) ->
    {ts_windows:descending_window_infos_list() | ts_windows:descending_windows_list(), ts_persistence:ctx()}.
list_windows_internal(
    #data_node{
        windows = Windows,
        older_node_key = undefined
    }, Window, Options, PersistenceCtx) ->
    {_, WindowsToReturn} = ts_windows:list(Windows, Window, Options),
    {WindowsToReturn, PersistenceCtx};

list_windows_internal(
    #data_node{
        older_node_key = OlderNodeKey,
        older_node_timestamp = OlderNodeTimestamp
    }, Window, Options, PersistenceCtx)
    when OlderNodeTimestamp >= Window ->
    {OlderDataNode, UpdatedPersistenceCtx} = ts_persistence:get(OlderNodeKey, PersistenceCtx),
    list_windows_internal(OlderDataNode, Window, Options, UpdatedPersistenceCtx);

list_windows_internal(
    #data_node{
        windows = Windows,
        older_node_key = OlderNodeKey
    }, Window, Options, PersistenceCtx) ->
    case ts_windows:list(Windows, Window, Options) of
        {ok, WindowsToReturn} ->
            {WindowsToReturn, PersistenceCtx};
        {{continue, NewOptions}, WindowsToReturn} ->
            {OlderDataNode, UpdatedPersistenceCtx} = ts_persistence:get(OlderNodeKey, PersistenceCtx),
            {NextWindowsToReturn, FinalPersistenceCtx} =
                list_windows_internal(OlderDataNode, undefined, NewOptions, UpdatedPersistenceCtx),
            {WindowsToReturn ++ NextWindowsToReturn, FinalPersistenceCtx}
    end.


%% @private
-spec delete_data_nodes_internal(ts_metric_data_node:key(), data_node(), ts_persistence:ctx()) -> ts_persistence:ctx().
delete_data_nodes_internal(DataNodeKey, #data_node{older_node_key = undefined}, PersistenceCtx) ->
    ts_persistence:delete_data_node(DataNodeKey, PersistenceCtx);
delete_data_nodes_internal(DataNodeKey, #data_node{older_node_key = OlderNodeKey}, PersistenceCtx) ->
    {OlderDataNode, UpdatedPersistenceCtx} = ts_persistence:get(OlderNodeKey, PersistenceCtx),
    UpdatedPersistenceCtx2 = ts_persistence:delete_data_node(DataNodeKey, UpdatedPersistenceCtx),
    delete_data_nodes_internal(OlderNodeKey, OlderDataNode, UpdatedPersistenceCtx2).


%% @private
-spec get_window_id(ts_window:timestamp_seconds() | undefined, metric_config:record()) ->
    ts_window:window_id() | undefined.
get_window_id(undefined, _) ->
    undefined;
get_window_id(_Time, #metric_config{resolution = 0}) ->
    0;
get_window_id(Time, #metric_config{resolution = Resolution}) ->
    Time - Time rem Resolution.


%% @private
-spec get_max_windows_and_split_position(ts_metric_data_node:key(), splitting_strategy(), ts_persistence:ctx()) ->
    {non_neg_integer(), non_neg_integer()}.
get_max_windows_and_split_position(
    DataNodeKey,
    #splitting_strategy{
        max_windows_in_head_doc = MaxWindowsCountInHead,
        max_windows_in_tail_doc = MaxWindowsCountInTail
    },
    PersistenceCtx) ->
    case {ts_persistence:is_hub_key(DataNodeKey, PersistenceCtx), MaxWindowsCountInHead =:= MaxWindowsCountInTail} of
        {true, true} ->
            % If adding of single window results in hub reorganization and MaxWindowsCountInHead =:= MaxWindowsCountInTail,
            % moving all windows would result in creation of too large tail document and split should be at first element
            % to move most of windows to tail doc
            {MaxWindowsCountInHead, 1};
        {true, false} ->
            % If adding of single window results in hub reorganization and MaxWindowsCountInHead =/= MaxWindowsCountInTail,
            % all windows should be moved to tail doc (capacity of head doc is always equal or smaller to capacity of tail doc)
            {MaxWindowsCountInHead, 0};
        {false, _} ->
            % Split of tail doc should result in two documents with at list of half of capacity used
            {MaxWindowsCountInTail, ceil(MaxWindowsCountInTail / 2)}
    end.


%% @private
-spec split_node(ts_metric_data_node:key(), data_node(), ts_windows:windows_collection(), ts_windows:windows_collection(),
    ts_window:timestamp_seconds(), non_neg_integer(), splitting_strategy(), ts_persistence:ctx()) ->
    {ts_metric_data_node:key() | undefined, data_node() | undefined, ts_persistence:ctx()}.
split_node(DataNodeKey, Data, Windows1, _Windows2, SplitTimestamp, MaxCount,
    #splitting_strategy{max_docs_count = MaxCount}, PersistenceCtx) ->
    % Splitting last data node - do not create new data node for older windows as it would be deleted immediately
    UpdatedData = Data#data_node{windows = Windows1, older_node_timestamp = SplitTimestamp},
    {undefined, undefined, ts_persistence:update(DataNodeKey, UpdatedData, PersistenceCtx)};
split_node(DataNodeKey, Data, Windows1, Windows2, SplitTimestamp, _DocumentNumber, _DocSplittingStrategy, PersistenceCtx) ->
    DataToCreate = Data#data_node{windows = Windows2},
    {CreatedNodeKey, UpdatedPersistenceCtx} = ts_persistence:create(DataToCreate, PersistenceCtx),
    UpdatedData = Data#data_node{windows = Windows1, older_node_key = CreatedNodeKey, older_node_timestamp = SplitTimestamp},
    {CreatedNodeKey, DataToCreate, ts_persistence:update(DataNodeKey, UpdatedData, UpdatedPersistenceCtx)}.


%% @private
-spec set_as_currently_processed(
    time_series:name(),
    time_series:metric_name(),
    ts_persistence:ctx()
) ->
    ts_persistence:ctx().
set_as_currently_processed(TimeSeriesName, MetricName, PersistenceCtx0) ->
    PersistenceCtx1 = ts_persistence:set_currently_processed_time_series(TimeSeriesName, PersistenceCtx0),
    ts_persistence:set_currently_processed_metric(MetricName, PersistenceCtx1).


%% @private
-spec generate_data_nodes_dump(ts_metric_data_node:key() | undefined, ts_persistence:ctx()) ->
    {[data_node()], ts_persistence:ctx()}.
generate_data_nodes_dump(undefined = _NodeKey, PersistenceCtx) ->
    {[], PersistenceCtx};
generate_data_nodes_dump(NodeKey, PersistenceCtx) ->
    {#data_node{older_node_key = OlderNodeKey} = CurrentDataNode, UpdatedPersistenceCtx} =
        ts_persistence:get(NodeKey, PersistenceCtx),
    {DataNodesTail, FinalPersistenceCtx} = generate_data_nodes_dump(OlderNodeKey, UpdatedPersistenceCtx),
    {[CurrentDataNode | DataNodesTail], FinalPersistenceCtx}.


%% @private
-spec create_data_nodes_from_dump([data_node()], ts_persistence:ctx()) ->
    {ts_metric_data_node:key() | undefined, ts_persistence:ctx()}.
create_data_nodes_from_dump([], PersistenceCtx) ->
    {undefined, PersistenceCtx};
create_data_nodes_from_dump([CurrentDataNode | DataNodesTail], PersistenceCtx) ->
    {OlderNodeKey, UpdatedPersistenceCtx} = create_data_nodes_from_dump(DataNodesTail, PersistenceCtx),
    ts_persistence:create(CurrentDataNode#data_node{older_node_key = OlderNodeKey}, UpdatedPersistenceCtx).