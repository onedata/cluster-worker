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
-include("modules/datastore/ts_metric_config.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/2, update/4, list_windows/3, delete_data_nodes/2, reconfigure/4]).

-type id() :: binary().
-type metric() :: #metric{}.
-type legend() :: binary().
-type config() :: #metric_config{}.
-type splitting_strategy() :: #splitting_strategy{}.
-type data_node() :: #data_node{}.

-export_type([id/0, metric/0, legend/0, config/0, splitting_strategy/0, data_node/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(ts_metric:config(), ts_metric:splitting_strategy()) -> metric().
init(Config,  DocSplittingStrategy) ->
    #metric{
        config = Config,
        splitting_strategy = DocSplittingStrategy
    }.


-spec update(metric(), ts_windows:timestamp(), ts_windows:value(), ts_persistence:ctx()) ->
    ts_persistence:ctx().
update(#metric{
    config = #metric_config{aggregator = Aggregator} = Config,
    splitting_strategy = DocSplittingStrategy,
    head_data = Data
}, NewTimestamp, NewValue, PersistenceCtx) ->
    WindowToBeUpdated = get_window_id(NewTimestamp, Config),
    DataNodeKey = ts_persistence:get_time_series_collection_id(PersistenceCtx),
    update(Data, DataNodeKey, 1, DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx).


-spec list_windows(metric(), ts_windows:list_options(), ts_persistence:ctx()) -> {[ts_windows:window()], ts_persistence:ctx()}.
list_windows(#metric{
    head_data = Data,
    config = Config
}, Options, PersistenceCtx) ->
    Window = get_window_id(maps:get(start, Options, undefined), Config),
    list_windows(Data, Window, Options, PersistenceCtx).


-spec delete_data_nodes(metric(), ts_persistence:ctx()) -> ts_persistence:ctx().
delete_data_nodes(#metric{splitting_strategy = #splitting_strategy{max_docs_count = 1}}, PersistenceCtx) ->
    PersistenceCtx;
delete_data_nodes(#metric{head_data = #data_node{older_node_key = undefined}}, PersistenceCtx) ->
    PersistenceCtx;
delete_data_nodes(#metric{head_data = #data_node{older_node_key = OlderNodeKey}}, PersistenceCtx) ->
    {OlderDataNode, UpdatedPersistenceCtx} = ts_persistence:get(OlderNodeKey, PersistenceCtx),
    delete_data_nodes(OlderNodeKey, OlderDataNode, UpdatedPersistenceCtx).


-spec reconfigure(metric(), ts_metric:config(), ts_metric:splitting_strategy(), ts_persistence:ctx()) ->
    ts_persistence:ctx().
reconfigure(#metric{
    splitting_strategy = DocSplittingStrategy
} = _CurrentMetric, _NewConfig, DocSplittingStrategy, PersistenceCtx) ->
    PersistenceCtx;
reconfigure(CurrentMetric, NewConfig, NewDocSplittingStrategy, PersistenceCtx) ->
    {ExistingWindows, UpdatedPersistenceCtx} = list_windows(CurrentMetric, #{}, PersistenceCtx),
    PersistenceCtxAfterCleaning = delete_data_nodes(CurrentMetric, UpdatedPersistenceCtx),

    NewMetric = init(NewConfig,  NewDocSplittingStrategy),
    PersistenceCtxAfterInit = ts_persistence:init_metric(NewMetric, PersistenceCtxAfterCleaning),

    DataNodeKey = ts_persistence:get_time_series_collection_id(PersistenceCtxAfterInit),
    lists:foldr(fun({Timestamp, WindowValue}, TmpPersistenceCtx) ->
        WindowToBeUpdated = get_window_id(Timestamp, NewConfig),
        {Data, UpdatedTmpPersistenceCtx} = ts_persistence:get(DataNodeKey, TmpPersistenceCtx),
        update(Data, DataNodeKey, 1, NewDocSplittingStrategy, set,
            WindowToBeUpdated, WindowValue, UpdatedTmpPersistenceCtx)
    end, PersistenceCtxAfterInit, ExistingWindows).


%%=====================================================================
%% Internal functions
%%=====================================================================

-spec update(data_node(), ts_metric_data_node:key(), non_neg_integer(), splitting_strategy(),
    ts_windows:setter_or_aggregator(), ts_windows:window_id(), ts_windows:value(), ts_persistence:ctx()) ->
    ts_persistence:ctx().
update(
    #data_node{
        windows = Windows
    } = Data, DataNodeKey, _DataDocPosition,
    #splitting_strategy{
        max_docs_count = 1,
        max_windows_in_head_doc = MaxWindowsCount
    }, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    % All windows are stored in single data node - update it
    UpdatedWindows = ts_windows:set_or_aggregate(Windows, WindowToBeUpdated, NewValue, Aggregator),
    FinalWindows = ts_windows:prune_overflowing(UpdatedWindows, MaxWindowsCount),
    ts_persistence:update(DataNodeKey, Data#data_node{windows = FinalWindows}, PersistenceCtx);

update(
    #data_node{
        older_node_key = undefined,
        older_node_timestamp = OlderNodeTimestamp
    }, _DataNodeKey, _DataDocPosition,
    _DocSplittingStrategy, _Aggregator, WindowToBeUpdated, _NewValue, PersistenceCtx)
    when OlderNodeTimestamp =/= undefined andalso OlderNodeTimestamp >= WindowToBeUpdated ->
    % There are too many newer windows - skip it
    PersistenceCtx;

update(
    #data_node{
        older_node_key = undefined,
        windows = Windows
    } = Data, DataNodeKey, DataDocPosition,
    DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    % Updating last data node
    UpdatedWindows = ts_windows:set_or_aggregate(Windows, WindowToBeUpdated, NewValue, Aggregator),
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

update(
    #data_node{
        older_node_key = OlderNodeKey,
        older_node_timestamp = OlderNodeTimestamp
    }, _DataNodeKey, DataDocPosition,
    DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx)
    when OlderNodeTimestamp >= WindowToBeUpdated ->
    % Window should be stored in one one previous data nodes
    {OlderDataNode, UpdatedPersistenceCtx} = ts_persistence:get(OlderNodeKey, PersistenceCtx),
    update(OlderDataNode, OlderNodeKey, DataDocPosition + 1,
        DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, UpdatedPersistenceCtx);

update(
    #data_node{
        windows = Windows,
        older_node_key = OlderNodeKey
    } = Data, DataNodeKey, DataDocPosition,
    #splitting_strategy{
        max_windows_in_tail_doc = MaxWindowsInTail
    } = DocSplittingStrategy, Aggregator, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    % Updating data node in the middle of data nodes' list (older_node_key is not undefined)
    WindowsWithAggregatedMeasurement = ts_windows:set_or_aggregate(Windows, WindowToBeUpdated, NewValue, Aggregator),
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


-spec list_windows(data_node(), ts_windows:window_id() | undefined, ts_windows:list_options(),
    ts_persistence:ctx()) -> {[ts_windows:window()], ts_persistence:ctx()}.
list_windows(
    #data_node{
        windows = Windows,
        older_node_key = undefined
    }, Window, Options, PersistenceCtx) ->
    {_, WindowsToReturn} = ts_windows:list(Windows, Window, Options),
    {WindowsToReturn, PersistenceCtx};

list_windows(
    #data_node{
        older_node_key = OlderNodeKey,
        older_node_timestamp = OlderNodeTimestamp
    }, Window, Options, PersistenceCtx)
    when OlderNodeTimestamp >= Window ->
    {OlderDataNode, UpdatedPersistenceCtx} = ts_persistence:get(OlderNodeKey, PersistenceCtx),
    list_windows(OlderDataNode, Window, Options, UpdatedPersistenceCtx);

list_windows(
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
                list_windows(OlderDataNode, undefined, NewOptions, UpdatedPersistenceCtx),
            {WindowsToReturn ++ NextWindowsToReturn, FinalPersistenceCtx}
    end.


-spec delete_data_nodes(ts_metric_data_node:key(), data_node(), ts_persistence:ctx()) -> ts_persistence:ctx().
delete_data_nodes(DataNodeKey, #data_node{older_node_key = undefined}, PersistenceCtx) ->
    ts_persistence:delete_data_node(DataNodeKey, PersistenceCtx);
delete_data_nodes(DataNodeKey, #data_node{older_node_key = OlderNodeKey}, PersistenceCtx) ->
    {OlderDataNode, UpdatedPersistenceCtx} = ts_persistence:get(OlderNodeKey, PersistenceCtx),
    UpdatedPersistenceCtx2 = ts_persistence:delete_data_node(DataNodeKey, UpdatedPersistenceCtx),
    delete_data_nodes(OlderNodeKey, OlderDataNode, UpdatedPersistenceCtx2).


-spec get_window_id(ts_windows:timestamp() | undefined, config()) -> ts_windows:window_id() | undefined.
get_window_id(undefined, _) ->
    undefined;
get_window_id(Time, #metric_config{resolution = Resolution}) ->
    Time - Time rem Resolution.


-spec get_max_windows_and_split_position(ts_metric_data_node:key(), splitting_strategy(), ts_persistence:ctx()) ->
    {non_neg_integer(), non_neg_integer()}.
get_max_windows_and_split_position(
    DataNodeKey,
    #splitting_strategy{
        max_windows_in_head_doc = MaxWindowsCountInHead,
        max_windows_in_tail_doc = MaxWindowsCountInTail
    },
    PersistenceCtx) ->
    case ts_persistence:is_hub_key(DataNodeKey, PersistenceCtx) of
        true ->
            % If adding of single window results in reorganization split should be at first element
            % to move most of windows to tail doc
            {MaxWindowsCountInHead, 1};
        false ->
            % Split of tail doc should result in two documents with at list of half of capacity used
            {MaxWindowsCountInTail, ceil(MaxWindowsCountInTail / 2)}
    end.


-spec split_node(ts_metric_data_node:key(), data_node(), ts_windows:windows(), ts_windows:windows(),
    ts_windows:timestamp(), non_neg_integer(), splitting_strategy(), ts_persistence:ctx()) ->
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