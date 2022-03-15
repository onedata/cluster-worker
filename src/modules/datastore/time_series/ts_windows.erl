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
-export([init/0, list/3, insert_value/4, prepend_windows_list/2, prune_overflowing/2, split/2,
    is_size_exceeded/2, get_remaining_windows_count/2, reorganize/4]).
%% Encoding/decoding  API
-export([encode/1, decode/1]).
%% Exported for unit tests
-export([get_value/2, get_size/1]).

-type timestamp_seconds() :: time_series:time_seconds().
-type value() :: number().
-type window_id() :: timestamp_seconds().
-type window_value() :: value() | {ValuesCount :: non_neg_integer(), ValuesSum :: value()}.
-type window() :: {timestamp_seconds(), window_value()}.
-type windows_collection() :: gb_trees:tree(timestamp_seconds(), window_value()).
-type descending_windows_list() :: [window()].
-type insert_strategy() :: {aggregate, metric_config:aggregator()} | ignore_existing.

-type list_options() :: #{
    % newest timestamp from which descending listing will begin
    start => timestamp_seconds(),
    % oldest timestamp when the listing should stop (unless it hits the limit)
    stop => timestamp_seconds(),
    % maximum number of time windows to be listed
    limit => non_neg_integer()
    %% @TODO VFS-8941 as limit is optional, it seems that if no limit is specified, the
    %% listing can return possibly to large list of windows (there should be a cap on that)
}.

-export_type([timestamp_seconds/0, value/0, window_id/0, window_value/0, window/0, windows_collection/0,
    descending_windows_list/0, insert_strategy/0, list_options/0]).

-define(EPOCH_INFINITY, 9999999999). % GMT: Saturday, 20 November 2286 17:46:39

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> windows_collection().
init() ->
    init_windows_set().


-spec list(windows_collection(), timestamp_seconds() | undefined, list_options()) ->
    {ok | {continue, list_options()}, descending_windows_list()}.
list(Windows, Timestamp, Options) ->
    list_internal(Timestamp, Windows, Options).


-spec insert_value(windows_collection(), timestamp_seconds(), value() | window_value(), insert_strategy()) -> windows_collection().
insert_value(Windows, WindowToBeUpdatedTimestamp, WindowValue, ignore_existing) ->
    set_value(WindowToBeUpdatedTimestamp, WindowValue, Windows);
insert_value(Windows, WindowToBeUpdatedTimestamp, NewValue, {aggregate, Aggregator}) ->
    CurrentValue = get_value(WindowToBeUpdatedTimestamp, Windows),
    NewWindowValue = aggregate(CurrentValue, NewValue, Aggregator),
    set_value(WindowToBeUpdatedTimestamp, NewWindowValue, Windows).


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


-spec split(windows_collection(), non_neg_integer()) -> {windows_collection(), windows_collection(), timestamp_seconds()}.
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
    {update_current_data_node, timestamp_seconds(), windows_collection()} |
    {split_current_data_node, {windows_collection(), windows_collection(), timestamp_seconds()}}].
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

-spec encode(windows_collection()) -> binary().
encode(Windows) ->
    json_utils:encode(lists:map(fun
        ({Timestamp, {ValuesCount, ValuesSum}}) -> [Timestamp, ValuesCount, ValuesSum];
        ({Timestamp, Value}) -> [Timestamp, Value]
    end, to_list(Windows))).


-spec decode(binary()) -> windows_collection().
decode(Term) ->
    InputList = json_utils:decode(Term),
    from_list(lists:map(fun
        ([Timestamp, ValuesCount, ValuesSum]) -> {Timestamp, {ValuesCount, ValuesSum}};
        ([Timestamp, Value]) -> {Timestamp, Value}
    end, InputList)).


%%=====================================================================
%% Internal functions
%%=====================================================================

-spec aggregate(window_value() | undefined, value(), metric_config:aggregator()) -> window_value().
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

-spec reverse_timestamp(timestamp_seconds()) -> timestamp_seconds().
reverse_timestamp(Timestamp) ->
    ?EPOCH_INFINITY - Timestamp. % Reversed order of timestamps is required for listing


-spec init_windows_set() -> windows_collection().
init_windows_set() ->
    gb_trees:empty().


-spec get_value(timestamp_seconds(), windows_collection()) -> window_value() | undefined.
get_value(Timestamp, Windows) ->
    case gb_trees:lookup(reverse_timestamp(Timestamp), Windows) of
        {value, Value} -> Value;
        none -> undefined
    end.


-spec set_value(timestamp_seconds(), window_value(), windows_collection()) -> windows_collection().
set_value(Timestamp, Value, Windows) ->
    gb_trees:enter(reverse_timestamp(Timestamp), Value, Windows).


-spec get_first_timestamp(windows_collection()) -> timestamp_seconds().
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


-spec list_internal(timestamp_seconds() | undefined, windows_collection(), list_options()) ->
    {ok | {continue, list_options()}, descending_windows_list()}.
list_internal(undefined, Windows, Options) ->
    list_internal(gb_trees:iterator(Windows), Options);
list_internal(Timestamp, Windows, Options) ->
    list_internal(gb_trees:iterator_from(reverse_timestamp(Timestamp), Windows), Options).


-spec list_internal(gb_trees:iter(timestamp_seconds(), window_value()), list_options()) ->
    {ok | {continue, list_options()}, descending_windows_list()}.
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


-spec split_internal(windows_collection(), non_neg_integer()) -> {windows_collection(), windows_collection(), timestamp_seconds()}.
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
    NewWindowsListToMerge = lists:map(fun({Timestamp, Value}) ->
        {reverse_timestamp(Timestamp), Value}
    end, NewWindowsList),
    from_list(NewWindowsListToMerge ++ to_list(Windows)).