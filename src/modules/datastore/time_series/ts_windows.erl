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
-export([init/0, list/3, list_full_data/1, map_list_options/3,
    update/3, prepend_windows_list/2, prune_overflowing/2, split/2,
    is_size_exceeded/2, get_remaining_windows_count/2, reorganize/4
]).
%% Encoding/decoding  API
-export([db_encode/1, db_decode/1]).
%% Exported for unit and ct tests
-export([get/2, get_size/1, to_list/1]).

-compile({no_auto_import, [get/1]}).


-type windows_collection() :: gb_trees:tree(ts_window:timestamp_seconds(), ts_window:record()).
-type descending_windows_list() :: [{ts_window:id(), ts_window:record()}].
-type descending_infos_list() :: [ts_window:info()].
-type update_spec() :: {override_with, ts_window:record()} |
    {consume_measurement,  ts_window:measurement(), metric_config:aggregator()}.

% List options provided with datastore requests and internal list options that extend
% listing possibilities for datastore internal usage
-type list_options() :: #{
    % newest timestamp from which descending listing will begin
    start_timestamp => ts_window:timestamp_seconds(),
    % oldest timestamp when the listing should stop (unless it hits the window_limit)
    stop_timestamp => ts_window:timestamp_seconds(),
    % maximum number of time windows to be listed
    window_limit => non_neg_integer(),
    % If true, additional fields of #window_info{} record are set (if false, only timestamp and value are defined)
    extended_info => boolean()
}.
-type internal_list_options() :: #{
    % value returned for each window
    return_type := basic_info | extended_info | full_data,
    % knowledge about aggregator is required during listing as method of generation of #window_info.value
    % from #window.aggregated_measurements differs for different aggregators ; this knowledge is not required
    % if full_data is requested (#window{} record is not mapped to #window_info{} record is such case)
    aggregator => metric_config:aggregator(),
    % newest timestamp from which descending listing will begin
    start_timestamp => ts_window:timestamp_seconds(),
    % oldest timestamp when the listing should stop (unless it hits the window_limit)
    stop_timestamp => ts_window:timestamp_seconds(),
    % maximum number of time windows to be listed
    window_limit => non_neg_integer()
}.

-export_type([windows_collection/0, descending_windows_list/0, descending_infos_list/0,
    update_spec/0, list_options/0, internal_list_options/0]).

-define(EPOCH_INFINITY, 9999999999). % GMT: Saturday, 20 November 2286 17:46:39
-define(MAX_WINDOW_LIMIT, 1000).

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> windows_collection().
init() ->
    init_windows_set().


-spec list(windows_collection(), ts_window:timestamp_seconds() | undefined, internal_list_options()) ->
    {ok | {continue, internal_list_options()}, descending_windows_list() | descending_infos_list()}.
list(Windows, Timestamp, Options) ->
    SanitizedWindowLimit = case maps:find(window_limit, Options) of
        error ->
            ?MAX_WINDOW_LIMIT;
        {ok, Limit} ->
            min(Limit, ?MAX_WINDOW_LIMIT)
    end,
    list_internal(Timestamp, Windows, Options#{window_limit => SanitizedWindowLimit}).


%% @doc should be used only internally as the complete window list can be large
-spec list_full_data(windows_collection()) -> descending_windows_list().
list_full_data(Windows) ->
    {_, WindowsList} = list_internal(undefined, Windows, #{return_type => full_data}),
    WindowsList.


-spec map_list_options(list_options(), metric_config:aggregator(), boolean()) -> internal_list_options().
map_list_options(ListOption, Aggregator, true = _ExtendedInfo) ->
    maps:remove(extended_info, ListOption#{return_type => extended_info, aggregator => Aggregator});
map_list_options(ListOption, Aggregator, false = _ExtendedInfo) ->
    maps:remove(extended_info, ListOption#{return_type => basic_info, aggregator => Aggregator}).


-spec update(windows_collection(), ts_window:timestamp_seconds(), update_spec()) -> windows_collection().
update(Windows, WindowTimestamp, {override_with, Window}) ->
    set_value(WindowTimestamp, Window, Windows);
update(Windows, WindowTimestamp, {consume_measurement, NewMeasurement, Aggregator}) ->
    NewWindow = case get(WindowTimestamp, Windows) of
        undefined -> ts_window:new(NewMeasurement, Aggregator);
        CurrentWindow -> ts_window:consume_measurement(CurrentWindow, NewMeasurement, Aggregator)
    end,
    set_value(WindowTimestamp, NewWindow, Windows).


-spec prepend_windows_list(windows_collection(), descending_windows_list()) -> windows_collection().
prepend_windows_list(Windows, NewWindowsList) ->
    prepend_windows_list_internal(Windows, NewWindowsList).


-spec prune_overflowing(windows_collection(), non_neg_integer()) -> windows_collection().
prune_overflowing(Windows, MaxWindowsCount) ->
    case get_size(Windows) > MaxWindowsCount of
        true ->
            delete_last(Windows);
        false ->
            Windows
    end.


-spec split(windows_collection(), non_neg_integer()) ->
    {windows_collection(), windows_collection(), ts_window:timestamp_seconds()}.
split(Windows, SplitPosition) ->
    split_internal(Windows, SplitPosition).


-spec is_size_exceeded(windows_collection(), non_neg_integer()) -> boolean().
is_size_exceeded(Windows, MaxWindowsCount) ->
    get_size(Windows) > MaxWindowsCount.


-spec get_remaining_windows_count(windows_collection(), non_neg_integer()) -> non_neg_integer().
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
-spec reorganize(windows_collection(), windows_collection(), non_neg_integer(), non_neg_integer()) ->
    ActionsToApplyOnDataNodes :: [{update_previous_data_node, windows_collection()} |
    {update_current_data_node, ts_window:timestamp_seconds(), windows_collection()} |
    {split_current_data_node, {windows_collection(), windows_collection(), ts_window:timestamp_seconds()}}].
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

-spec db_encode(windows_collection()) -> binary().
db_encode(Windows) ->
    json_utils:encode(lists:map(fun({Timestamp, Window}) ->
        [Timestamp | ts_window:db_encode(Window)]
    end, to_list(Windows))).


-spec db_decode(binary()) -> windows_collection().
db_decode(Term) ->
    InputList = json_utils:decode(Term),
    from_list(lists:map(fun([Timestamp | EncodedWindow]) ->
        {Timestamp, ts_window:db_decode(EncodedWindow)}
    end, InputList)).


%%=====================================================================
%% Functions operating on structure that represents set of windows
%% NOTE: do not use gb_trees API directly, always use these functions
%% to allow easy change of structure if needed
%%=====================================================================

-spec reverse_timestamp(ts_window:timestamp_seconds()) -> ts_window:timestamp_seconds().
reverse_timestamp(Timestamp) ->
    ?EPOCH_INFINITY - Timestamp. % Reversed order of timestamps is required for listing


-spec init_windows_set() -> windows_collection().
init_windows_set() ->
    gb_trees:empty().


-spec get(ts_window:timestamp_seconds(), windows_collection()) -> ts_window:record() | undefined.
get(Timestamp, Windows) ->
    case gb_trees:lookup(reverse_timestamp(Timestamp), Windows) of
        {value, Value} -> Value;
        none -> undefined
    end.


-spec set_value(ts_window:timestamp_seconds(), ts_window:record(), windows_collection()) -> windows_collection().
set_value(Timestamp, Value, Windows) ->
    gb_trees:enter(reverse_timestamp(Timestamp), Value, Windows).


-spec get_first_timestamp(windows_collection()) -> ts_window:timestamp_seconds().
get_first_timestamp(Windows) ->
    {Timestamp, _} = gb_trees:smallest(Windows),
    reverse_timestamp(Timestamp).


-spec delete_last(windows_collection()) -> windows_collection().
delete_last(Windows) ->
    {_, _, UpdatedWindows} = gb_trees:take_largest(Windows),
    UpdatedWindows.


-spec get_size(windows_collection()) -> non_neg_integer().
get_size(Windows) ->
    gb_trees:size(Windows).


-spec list_internal(ts_window:timestamp_seconds() | undefined, windows_collection(), internal_list_options()) ->
    {ok | {continue, internal_list_options()}, descending_windows_list() | descending_infos_list()}.
list_internal(undefined, Windows, Options) ->
    list_internal(gb_trees:iterator(Windows), Options);
list_internal(Timestamp, Windows, Options) ->
    list_internal(gb_trees:iterator_from(reverse_timestamp(Timestamp), Windows), Options).


-spec list_internal(gb_trees:iter(ts_window:timestamp_seconds(), ts_window:record()), internal_list_options()) ->
    {ok | {continue, internal_list_options()}, descending_windows_list() | descending_infos_list()}.
list_internal(_Iterator, #{window_limit := 0}) ->
    {ok, []};
list_internal(Iterator, Options) ->
    case gb_trees:next(Iterator) of
        none ->
            {{continue, Options}, []};
        {Key, Value, NextIterator} ->
            Timestamp = reverse_timestamp(Key),
            case Options of
                #{stop_timestamp := StopTimestamp} when Timestamp < StopTimestamp ->
                    {ok, []};
                #{stop_timestamp := StopTimestamp} when Timestamp =:= StopTimestamp ->
                    {ok, [map_to_info_or_full_data(Timestamp, Value, Options)]};
                #{window_limit := Limit} ->
                    {FinishOrContinue, List} = list_internal(NextIterator, Options#{window_limit := Limit - 1}),
                    {FinishOrContinue, [map_to_info_or_full_data(Timestamp, Value, Options) | List]};
                _ ->
                    {FinishOrContinue, List} = list_internal(NextIterator, Options),
                    {FinishOrContinue, [map_to_info_or_full_data(Timestamp, Value, Options) | List]}
            end
    end.


-spec split_internal(windows_collection(), non_neg_integer()) ->
    {windows_collection(), windows_collection(), ts_window:timestamp_seconds()}.
split_internal(Windows, 0) ->
    {init_windows_set(), Windows, get_first_timestamp(Windows)};
split_internal(Windows, SplitPosition) ->
    WindowsList = gb_trees:to_list(Windows),
    Windows1 = lists:sublist(WindowsList, SplitPosition),
    [{SplitKey, _} | _] = Windows2 = lists:sublist(WindowsList, SplitPosition + 1, length(WindowsList) - SplitPosition),
    {gb_trees:from_orddict(Windows1), gb_trees:from_orddict(Windows2), reverse_timestamp(SplitKey)}.


-spec merge(windows_collection(), windows_collection()) -> windows_collection().
merge(Windows1, Windows2) ->
    from_list(to_list(Windows1) ++ to_list(Windows2)).


-spec to_list(windows_collection()) -> descending_windows_list().
to_list(Windows) ->
    gb_trees:to_list(Windows).


-spec from_list(descending_windows_list()) -> windows_collection().
from_list(WindowsList) ->
    gb_trees:from_orddict(WindowsList).


-spec prepend_windows_list_internal(windows_collection(), descending_windows_list()) -> windows_collection().
prepend_windows_list_internal(Windows, NewWindowsList) ->
    NewWindowsListToMerge = lists:map(fun({Timestamp, Window}) ->
        {reverse_timestamp(Timestamp), Window}
    end, NewWindowsList),
    from_list(NewWindowsListToMerge ++ to_list(Windows)).


%%=====================================================================
%% Internal functions
%%=====================================================================

-spec map_to_info_or_full_data(ts_window:id(), ts_window:record(), internal_list_options()) ->
    ts_window:info() | {ts_window:id(), ts_window:record()}.
map_to_info_or_full_data(WindowId, Window, #{return_type := full_data}) ->
    {WindowId, Window};

map_to_info_or_full_data(
    WindowId,
    Window,
    #{return_type := ReturnType, aggregator := Aggregator}
) ->
    ts_window:to_info(WindowId, Window, ReturnType, Aggregator).

