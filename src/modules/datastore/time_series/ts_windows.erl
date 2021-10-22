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

%% API
-export([init/0, list/3, set_or_aggregate/4, set_many/2, prune_overflowing/2, split/2,
    is_size_exceeded/2, get_remaining_windows_count/2, reorganize/4]).
%% Encoding/decoding  API
-export([encode/1, decode/1]).
%% Exported for unit tests
-export([get_value/2, get_size/1]).

-type timestamp() :: time:seconds().
-type value() :: number().
-type window_id() :: timestamp().
-type window_value() :: value() | {ValuesCount :: non_neg_integer(), ValuesSum :: value()}.
-type window() :: {timestamp(), window_value()}.
-type windows() :: gb_trees:tree(timestamp(), window_value()).
-type aggregator() :: sum | max | min | last | first. % | {gather, Max}. % TODO VFS-8164 - extend functions list
-type setter_or_aggregator() :: set | aggregator().

-type list_options() :: #{
    start => timestamp(),
    stop => timestamp(),
    limit => non_neg_integer()
}.

-export_type([timestamp/0, value/0, window_id/0, window_value/0, window/0, windows/0,
    aggregator/0, setter_or_aggregator/0, list_options/0]).

-define(EPOCH_INFINITY, 9999999999). % GMT: Saturday, 20 November 2286 17:46:39

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> windows().
init() ->
    init_windows_set().


-spec list(windows(), timestamp() | undefined, list_options()) -> {ok | {continue, list_options()}, [window()]}.
list(Windows, Timestamp, Options) ->
    list_internal(Timestamp, Windows, Options).


-spec set_or_aggregate(windows(), timestamp(), value() | window_value(), setter_or_aggregator()) -> windows().
set_or_aggregate(Windows, WindowToBeUpdatedTimestamp, WindowValue, set) ->
    set_value(WindowToBeUpdatedTimestamp, WindowValue, Windows);
set_or_aggregate(Windows, WindowToBeUpdatedTimestamp, NewValue, Aggregator) ->
    CurrentValue = get_value(WindowToBeUpdatedTimestamp, Windows),
    NewWindowValue = aggregate(CurrentValue, NewValue, Aggregator),
    set_value(WindowToBeUpdatedTimestamp, NewWindowValue, Windows).


-spec set_many(windows(), [window()]) -> windows().
set_many(Windows, NewWindowsList) ->
    from_list(prepare_windows_input_list(NewWindowsList) ++ to_list(Windows)).


-spec prune_overflowing(windows(), non_neg_integer()) -> windows().
prune_overflowing(Windows, MaxWindowsCount) ->
    case get_size(Windows) > MaxWindowsCount of
        true ->
            delete_last(Windows);
        false ->
            Windows
    end.


-spec split(windows(), non_neg_integer()) -> {windows(), windows(), timestamp()}.
split(Windows, SplitPosition) ->
    split_internal(Windows, SplitPosition).


-spec is_size_exceeded(windows(), non_neg_integer()) -> boolean().
is_size_exceeded(Windows, MaxWindowsCount) ->
    get_size(Windows) > MaxWindowsCount.


-spec get_remaining_windows_count(windows(), non_neg_integer()) -> non_neg_integer().
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
-spec reorganize(windows(), windows(), non_neg_integer(), non_neg_integer()) ->
    ActionsToApplyOnDataNodes :: [{update_previous_data_node, windows()} | {update_current_data_node, timestamp(), windows()} |
        {split_current_data_node, {windows(), windows(), timestamp()}}].
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
%%% Encoding/decoding  API
%%%===================================================================

-spec encode(windows()) -> binary().
encode(Windows) ->
    json_utils:encode(lists:map(fun
        ({Timestamp, {ValuesCount, ValuesSum}}) -> [Timestamp, ValuesCount, ValuesSum];
        ({Timestamp, Value}) -> [Timestamp, Value]
    end, to_list(Windows))).


-spec decode(binary()) -> windows().
decode(Term) ->
    InputList = json_utils:decode(Term),
    from_list(lists:map(fun
        ([Timestamp, ValuesCount, ValuesSum]) -> {Timestamp, {ValuesCount, ValuesSum}};
        ([Timestamp, Value]) -> {Timestamp, Value}
    end, InputList)).


%%=====================================================================
%% Internal functions
%%=====================================================================

-spec aggregate(window_value() | undefined, value(), aggregator()) -> window_value().
aggregate(undefined, NewValue, sum) ->
    {1, NewValue};
aggregate({CurrentCount, CurrentSum}, NewValue, sum) ->
    {CurrentCount + 1, CurrentSum + NewValue};
aggregate(undefined, NewValue, max) ->
    NewValue;
aggregate(CurrentValue, NewValue, max) ->
    max(CurrentValue, NewValue);
aggregate(undefined, NewValue, min) ->
    NewValue;
aggregate(CurrentValue, NewValue, min) ->
    min(CurrentValue, NewValue);
aggregate(_CurrentValue, NewValue, last) ->
    NewValue;
aggregate(undefined, NewValue, first) ->
    NewValue;
aggregate(CurrentValue, _NewValue, first) ->
    CurrentValue.


%%=====================================================================
%% Functions operating on structure that represents set of windows
%% NOTE: do not use gb_trees API directly, always use these functions
%% to allow easy change of structure if needed
%%=====================================================================

-spec reverse_timestamp(timestamp()) -> timestamp().
reverse_timestamp(Timestamp) ->
    ?EPOCH_INFINITY - Timestamp. % Reversed order of timestamps is required for listing


-spec init_windows_set() -> windows().
init_windows_set() ->
    gb_trees:empty().


-spec get_value(timestamp(), windows()) -> window_value() | undefined.
get_value(Timestamp, Windows) ->
    case gb_trees:lookup(reverse_timestamp(Timestamp), Windows) of
        {value, Value} -> Value;
        none -> undefined
    end.


-spec set_value(timestamp(), window_value(), windows()) -> windows().
set_value(Timestamp, Value, Windows) ->
    gb_trees:enter(reverse_timestamp(Timestamp), Value, Windows).


-spec get_first_timestamp(windows()) -> timestamp().
get_first_timestamp(Windows) ->
    {Timestamp, _} = gb_trees:smallest(Windows),
    reverse_timestamp(Timestamp).


-spec delete_last(windows()) -> windows().
delete_last(Windows) ->
    {_, _, UpdatedWindows} = gb_trees:take_largest(Windows),
    UpdatedWindows.


-spec get_size(windows()) -> non_neg_integer().
get_size(Windows) ->
    gb_trees:size(Windows).


-spec list_internal(timestamp() | undefined, windows(), list_options()) -> {ok | {continue, list_options()}, [window()]}.
list_internal(undefined, Windows, Options) ->
    list_internal(gb_trees:iterator(Windows), Options);
list_internal(Timestamp, Windows, Options) ->
    list_internal(gb_trees:iterator_from(reverse_timestamp(Timestamp), Windows), Options).


-spec list_internal(gb_trees:iter(timestamp(), window_value()), list_options()) ->
    {ok | {continue, list_options()}, [window()]}.
list_internal(_Iterator, #{limit := 0}) ->
    {ok, []};
list_internal(Iterator, Options) ->
    case gb_trees:next(Iterator) of
        none ->
            {{continue, Options}, []};
        {Key, Value, NextIterator} ->
            Timestamp = reverse_timestamp(Key),
            case Options of
                #{stop := Stop} when Timestamp < Stop ->
                    {ok, []};
                #{stop := Stop} when Timestamp =:= Stop ->
                    {ok, [{Timestamp, Value}]};
                #{limit := Limit} ->
                    {FinishOrContinue, List} = list_internal(NextIterator, Options#{limit := Limit - 1}),
                    {FinishOrContinue, [{Timestamp, Value} | List]};
                _ ->
                    {FinishOrContinue, List} = list_internal(NextIterator, Options),
                    {FinishOrContinue, [{Timestamp, Value} | List]}
            end
    end.


-spec split_internal(windows(), non_neg_integer()) -> {windows(), windows(), timestamp()}.
split_internal(Windows, 0) ->
    {init_windows_set(), Windows, get_first_timestamp(Windows)};
split_internal(Windows, SplitPosition) ->
    WindowsList = gb_trees:to_list(Windows),
    Windows1 = lists:sublist(WindowsList, SplitPosition),
    [{SplitKey, _} | _] = Windows2 = lists:sublist(WindowsList, SplitPosition + 1, length(WindowsList) - SplitPosition),
    {gb_trees:from_orddict(Windows1), gb_trees:from_orddict(Windows2), reverse_timestamp(SplitKey)}.


-spec merge(windows(), windows()) -> windows().
merge(Windows1, Windows2) ->
    from_list(to_list(Windows1) ++ to_list(Windows2)).


-spec to_list(windows()) -> [window()].
to_list(Windows) ->
    gb_trees:to_list(Windows).


-spec from_list([window()]) -> windows().
from_list(WindowsList) ->
    gb_trees:from_orddict(WindowsList).


-spec prepare_windows_input_list([window()]) -> [window()].
prepare_windows_input_list(NewWindowsList) ->
    lists:reverse(lists:map(fun({Timestamp, Value}) -> {reverse_timestamp(Timestamp), Value} end, NewWindowsList)).