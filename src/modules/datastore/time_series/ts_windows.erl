%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module operating on time_series metric windows' set.
%%% @end
%%%-------------------------------------------------------------------
-module(ts_windows).
-author("Michal Wrzeszcz").

%% API
-export([init/0, get/3, aggregate/4, prune_overflowing_windows/2,
    split_windows/2, should_reorganize_windows/2, reorganize_windows/4]).
%% Encoding/decoding  API
-export([encode/1, decode/1]).
%% Exported for unit tests
-export([get_value/2, get_size/1]).

-type timestamp() :: time:seconds().
-type value() :: number().
-type window_value() :: value() | {ValuesCount :: non_neg_integer(), ValuesSum :: value()}.
-type window() :: {timestamp(), window_value()}.
-type windows() :: gb_trees:tree(timestamp(), window_value()).
-type aggregator() :: sum | max | min | last | first. % | {gather, Max}. % TODO VFS-8164 - extend functions list

-type get_options() :: #{
    start => timestamp(),
    stop => timestamp(),
    limit => non_neg_integer()
}.

-export_type([timestamp/0, value/0, window_value/0, window/0, windows/0, aggregator/0, get_options/0]).

-define(EPOCH_INFINITY, 9999999999). % GMT: Saturday, 20 November 2286 17:46:39

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> windows().
init() ->
    init_windows_set().


-spec get(windows(), timestamp() | undefined, get_options()) -> {ok | {continue, get_options()}, [window()]}.
get(Windows, Timestamp, Options) ->
    list(Timestamp, Windows, Options).


-spec aggregate(windows(), timestamp(), value(), aggregator()) -> windows().
aggregate(Windows, WindowToBeUpdatedTimestamp, NewValue, Aggregator) ->
    CurrentValue = get_value(WindowToBeUpdatedTimestamp, Windows),
    NewWindowValue = aggregate(CurrentValue, NewValue, Aggregator),
    set_value(WindowToBeUpdatedTimestamp, NewWindowValue, Windows).


-spec prune_overflowing_windows(windows(), non_neg_integer()) -> windows().
prune_overflowing_windows(Windows, MaxWindowsCount) ->
    case get_size(Windows) > MaxWindowsCount of
        true ->
            delete_last(Windows);
        false ->
            Windows
    end.


-spec split_windows(windows(), non_neg_integer()) -> {windows(), windows(), timestamp()}.
split_windows(Windows, SplitPosition) ->
    split(Windows, SplitPosition).


-spec should_reorganize_windows(windows(), non_neg_integer()) -> boolean().
should_reorganize_windows(Windows, MaxWindowsCount) ->
    get_size(Windows) > MaxWindowsCount.


%%--------------------------------------------------------------------
%% @doc
%% Reorganizes two sets of windows (from current and previous record). The functions is used when capacity of
%% current record is exceeded. If previous record is not full, the function migrates windows from current record
%% to previous record to decrease number of windows in current record. Otherwise, the function splits windows set
%% stored in current record.
%% @end
%%--------------------------------------------------------------------
-spec reorganize_windows(windows(), windows(), non_neg_integer(), non_neg_integer()) ->
    ActionsToApplyOnRecords :: [{update_previous_record, windows()} | {update_current_record, timestamp(), windows()} |
        {split_current_record, {windows(), windows(), timestamp()}}].
reorganize_windows(WindowsInPrevRecord, WindowsInCurrentRecord, MaxWindowsInPrevRecord, SplitPosition) ->
    WindowsInPrevRecordSize = get_size(WindowsInPrevRecord),
    WindowsInCurrentRecordSize = get_size(WindowsInCurrentRecord),

    case WindowsInPrevRecordSize of
        MaxWindowsInPrevRecord ->
            [{split_current_record, split(WindowsInCurrentRecord, SplitPosition)}];
        _ ->
            case WindowsInPrevRecordSize + WindowsInCurrentRecordSize > MaxWindowsInPrevRecord of
                true ->
                    {WindowsInCurrentRecordPart1, WindowsInCurrentRecordPart2, SplitTimestamp} =
                        split(WindowsInCurrentRecord,
                            WindowsInCurrentRecordSize - (MaxWindowsInPrevRecord - WindowsInPrevRecordSize)),
                    UpdatedWindowsInPrevRecord = merge(WindowsInCurrentRecordPart2, WindowsInPrevRecord),
                    [{update_previous_record, UpdatedWindowsInPrevRecord},
                        {update_current_record, SplitTimestamp, WindowsInCurrentRecordPart1}];
                false ->
                    TimestampToUpdate = get_first_timestamp(WindowsInCurrentRecord),
                    [{update_previous_record, merge(WindowsInCurrentRecord, WindowsInPrevRecord)},
                        {update_current_record, TimestampToUpdate, init_windows_set()}]
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


-spec list(timestamp() | undefined, windows(), get_options()) -> {ok | {continue, get_options()}, [window()]}.
list(undefined, Windows, Options) ->
    list(gb_trees:iterator(Windows), Options);
list(Timestamp, Windows, Options) ->
    list(gb_trees:iterator_from(reverse_timestamp(Timestamp), Windows), Options).


-spec list(gb_trees:iter(timestamp(), window_value()), get_options()) ->
    {ok | {continue, get_options()}, [window()]}.
list(_Iterator, #{limit := 0}) ->
    {ok, []};
list(Iterator, Options) ->
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
                    {FinishOrContinue, List} = list(NextIterator, Options#{limit := Limit - 1}),
                    {FinishOrContinue, [{Timestamp, Value} | List]};
                _ ->
                    {FinishOrContinue, List} = list(NextIterator, Options),
                    {FinishOrContinue, [{Timestamp, Value} | List]}
            end
    end.


-spec split(windows(), non_neg_integer()) -> {windows(), windows(), timestamp()}.
split(Windows, SplitPosition) ->
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