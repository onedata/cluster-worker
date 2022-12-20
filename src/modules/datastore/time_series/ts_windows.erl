%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module operating on time_series_collection metric windows' set.
%%% @end
%%%-------------------------------------------------------------------
-module(ts_windows).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_time_series.hrl").


%% API
-export([init/0, list/5, update/3, prepend_windows_list/2, prune_overflowing/2, split/2,
    is_size_exceeded/2, get_remaining_windows_count/2, reorganize/4
]).
%% Encoding/decoding  API
-export([db_encode/1, db_decode/1]).
%% Exported for unit and ct tests
-export([get/2, get_size/1, to_list/1]).

-compile({no_auto_import, [get/1]}).


-type window_collection() :: gb_trees:tree(ts_window:timestamp_seconds(), ts_window:record()).
-type descending_list(Element) :: [Element].
-type update_spec() :: {override_with, ts_window:record()} |
{consume_measurement, ts_window:measurement(), metric_config:aggregator()}.

% Mapping function that will be applied to each listed window_id/#window{} pair before the result is returned.
-type listing_postprocessor(MappingResult) :: fun((ts_window:id(), ts_window:record()) -> MappingResult).

-export_type([window_collection/0, descending_list/1, update_spec/0, listing_postprocessor/1]).

-define(EPOCH_INFINITY, 9999999999). % GMT: Saturday, 20 November 2286 17:46:39
-define(MINIMAL_TIMESTAMP, 0).

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> window_collection().
init() ->
    init_windows_set().


-spec list(
    window_collection(),
    ts_window:id() | undefined,
    ts_window:timestamp_seconds() | undefined,
    non_neg_integer(),
    listing_postprocessor(MappingResult)
) ->
    {done | partial, descending_list(MappingResult)}.
list(Windows, StartWindowId, undefined, Limit, ListingPostprocessor) ->
    list(Windows, StartWindowId, ?MINIMAL_TIMESTAMP, Limit, ListingPostprocessor);
list(Windows, StartWindowId, StopTimestamp, Limit, ListingPostprocessor) ->
    list_internal(iterator(Windows, StartWindowId), StopTimestamp, Limit, ListingPostprocessor).


-spec update(window_collection(), ts_window:id(), update_spec()) -> window_collection().
update(Windows, WindowId, {override_with, Window}) ->
    overwrite(WindowId, Window, Windows);
update(Windows, WindowId, {consume_measurement, NewMeasurement, Aggregator}) ->
    NewWindow = case get(WindowId, Windows) of
        undefined -> ts_window:new(NewMeasurement, Aggregator);
        CurrentWindow -> ts_window:consume_measurement(CurrentWindow, NewMeasurement, Aggregator)
    end,
    overwrite(WindowId, NewWindow, Windows).


-spec prepend_windows_list(window_collection(), descending_list({ts_window:id(), ts_window:record()})) ->
    window_collection().
prepend_windows_list(Windows, NewWindowsList) ->
    NewWindowsListToMerge = lists:map(fun({Timestamp, Window}) ->
        {reverse_timestamp(Timestamp), Window}
    end, NewWindowsList),
    from_list(NewWindowsListToMerge ++ to_list(Windows)).


-spec prune_overflowing(window_collection(), non_neg_integer()) -> window_collection().
prune_overflowing(Windows, MaxWindowsCount) ->
    case get_size(Windows) > MaxWindowsCount of
        true ->
            delete_last(Windows);
        false ->
            Windows
    end.


-spec split(window_collection(), non_neg_integer()) ->
    {window_collection(), window_collection(), ts_window:timestamp_seconds()}.
split(Windows, SplitPosition) ->
    split_internal(Windows, SplitPosition).


-spec is_size_exceeded(window_collection(), non_neg_integer()) -> boolean().
is_size_exceeded(Windows, MaxWindowsCount) ->
    get_size(Windows) > MaxWindowsCount.


-spec get_remaining_windows_count(window_collection(), non_neg_integer()) -> non_neg_integer().
get_remaining_windows_count(Windows, MaxWindowsCount) ->
    MaxWindowsCount - get_size(Windows).


%%--------------------------------------------------------------------
%% @doc
%% Reorganizes two sets of windows (from current and older data node). The functions is used when capacity of
%% current data node is exceeded. If older data node is not full, the function migrates windows from current data node
%% to older data node to decrease number of windows in current data node. Otherwise, the function splits windows set
%% stored in current data node.
%% @end
%%--------------------------------------------------------------------
-spec reorganize(window_collection(), window_collection(), non_neg_integer(), non_neg_integer()) ->
    ActionsToApplyOnDataNodes :: [{update_previous_data_node, window_collection()} |
    {update_current_data_node, ts_window:timestamp_seconds(), window_collection()} |
    {split_current_data_node, {window_collection(), window_collection(), ts_window:timestamp_seconds()}}].
reorganize(WindowsInOlderDataNode, WindowsInCurrentDataNode, MaxWindowsInOlderDataNode, SplitPosition) ->
    WindowsInOlderDataNodeCount = get_size(WindowsInOlderDataNode),
    WindowsInCurrentDataNodeCount = get_size(WindowsInCurrentDataNode),

    case WindowsInOlderDataNodeCount of
        MaxWindowsInOlderDataNode ->
            [{split_current_data_node, split_internal(WindowsInCurrentDataNode, SplitPosition)}];
        _ ->
            case WindowsInOlderDataNodeCount + WindowsInCurrentDataNodeCount > MaxWindowsInOlderDataNode of
                true ->
                    {WindowsInCurrentDataNodePart1, WindowsInCurrentDataNodePart2, SplitTimestamp} =
                        split_internal(WindowsInCurrentDataNode,
                            WindowsInCurrentDataNodeCount - (MaxWindowsInOlderDataNode - WindowsInOlderDataNodeCount)),
                    UpdatedWindowsInOlderDataNode = merge(WindowsInCurrentDataNodePart2, WindowsInOlderDataNode),
                    [{update_previous_data_node, UpdatedWindowsInOlderDataNode},
                        {update_current_data_node, SplitTimestamp, WindowsInCurrentDataNodePart1}];
                false ->
                    TimestampToUpdate = get_first_timestamp(WindowsInCurrentDataNode),
                    [{update_previous_data_node, merge(WindowsInCurrentDataNode, WindowsInOlderDataNode)},
                        {update_current_data_node, TimestampToUpdate, init_windows_set()}]
            end
    end.


%%%===================================================================
%%% Encoding/decoding API
%%%===================================================================

-spec db_encode(window_collection()) -> binary().
db_encode(Windows) ->
    json_utils:encode(lists:map(fun({WindowId, WindowRecord}) ->
        [WindowId | ts_window:db_encode(WindowRecord)]
    end, to_list(Windows))).


-spec db_decode(binary()) -> window_collection().
db_decode(Term) ->
    InputList = json_utils:decode(Term),
    from_list(lists:map(fun([WindowId | EncodedWindowRecord]) ->
        {WindowId, ts_window:db_decode(EncodedWindowRecord)}
    end, InputList)).


%%=====================================================================
%% Functions operating on structure that represents set of windows
%% NOTE: do not use gb_trees API directly, always use these functions
%% to allow easy change of structure if needed
%%=====================================================================

-spec reverse_timestamp(ts_window:timestamp_seconds()) -> ts_window:timestamp_seconds().
reverse_timestamp(Timestamp) ->
    ?EPOCH_INFINITY - Timestamp. % Reversed order of timestamps is required for listing


-spec init_windows_set() -> window_collection().
init_windows_set() ->
    gb_trees:empty().


-spec get(ts_window:id(), window_collection()) -> ts_window:record() | undefined.
get(WindowId, WindowCollection) ->
    case gb_trees:lookup(reverse_timestamp(WindowId), WindowCollection) of
        {value, Value} -> Value;
        none -> undefined
    end.


-spec overwrite(ts_window:id(), ts_window:record(), window_collection()) -> window_collection().
overwrite(WindowId, WindowRecord, Windows) ->
    gb_trees:enter(reverse_timestamp(WindowId), WindowRecord, Windows).


-spec get_first_timestamp(window_collection()) -> ts_window:timestamp_seconds().
get_first_timestamp(Windows) ->
    {Timestamp, _} = gb_trees:smallest(Windows),
    reverse_timestamp(Timestamp).


-spec delete_last(window_collection()) -> window_collection().
delete_last(Windows) ->
    {_, _, UpdatedWindows} = gb_trees:take_largest(Windows),
    UpdatedWindows.


-spec get_size(window_collection()) -> non_neg_integer().
get_size(Windows) ->
    gb_trees:size(Windows).


-spec list_internal(
    gb_trees:iter(ts_window:id(), ts_window:record()),
    ts_window:timestamp_seconds(),
    non_neg_integer(),
    listing_postprocessor(MappingResult)
) -> {done | partial, descending_list(MappingResult)}.
list_internal(_Iterator, _StopTimestamp, 0, _ListingPostprocessor) ->
    {done, []};
list_internal(Iterator, StopTimestamp, Limit, ListingPostprocessor) ->
    case gb_trees:next(Iterator) of
        none ->
            {partial, []};
        {Key, Value, NextIterator} ->
            CurrentTimestamp = reverse_timestamp(Key),
            if
                CurrentTimestamp < StopTimestamp ->
                    {done, []};
                CurrentTimestamp =:= StopTimestamp ->
                    {done, [ListingPostprocessor(CurrentTimestamp, Value)]};
                true ->
                    {Ans, List} = list_internal(NextIterator, StopTimestamp, Limit - 1, ListingPostprocessor),
                    {Ans, [ListingPostprocessor(CurrentTimestamp, Value) | List]}
            end
    end.


-spec split_internal(window_collection(), non_neg_integer()) ->
    {window_collection(), window_collection(), ts_window:timestamp_seconds()}.
split_internal(Windows, 0) ->
    {init_windows_set(), Windows, get_first_timestamp(Windows)};
split_internal(Windows, SplitPosition) ->
    WindowsList = gb_trees:to_list(Windows),
    Windows1 = lists:sublist(WindowsList, SplitPosition),
    [{SplitKey, _} | _] = Windows2 = lists:sublist(WindowsList, SplitPosition + 1, length(WindowsList) - SplitPosition),
    {gb_trees:from_orddict(Windows1), gb_trees:from_orddict(Windows2), reverse_timestamp(SplitKey)}.


-spec merge(window_collection(), window_collection()) -> window_collection().
merge(Windows1, Windows2) ->
    from_list(to_list(Windows1) ++ to_list(Windows2)).


-spec to_list(window_collection()) -> descending_list({ts_window:id(), ts_window:record()}).
to_list(Windows) ->
    gb_trees:to_list(Windows).


-spec from_list(descending_list({ts_window:id(), ts_window:record()})) -> window_collection().
from_list(WindowsList) ->
    gb_trees:from_orddict(WindowsList).


%% @private
-spec iterator(window_collection(), ts_window:id() | undefined) ->
    gb_trees:iter(ts_window:id(), ts_window:record()).
iterator(Windows, undefined) ->
    gb_trees:iterator(Windows);
iterator(Windows, StartWindowId) ->
    gb_trees:iterator_from(reverse_timestamp(StartWindowId), Windows).
