%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module operating on histogram windows' set.
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_windows).
-author("Michal Wrzeszcz").

%% API
-export([init/0, get/3, apply_value/4, maybe_delete_last/2,
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
-type apply_function() :: sum | max | min | last | first. % | {gather, Max}. % TODO VFS-8164 - extend functions list

-type options() :: #{
    start => timestamp(),
    stop => timestamp(),
    limit => non_neg_integer()
}.

-export_type([timestamp/0, value/0, window_value/0, window/0, windows/0, apply_function/0, options/0]).

-define(EPOCH_INFINITY, 9999999999). % GMT: Saturday, 20 November 2286 17:46:39

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> windows().
init() ->
    init_windows_set().


-spec get(windows(), timestamp() | undefined, options()) -> {ok | {continue, options()}, [window_value()]}.
get(Windows, Timestamp, Options) ->
    list_values(Timestamp, Windows, Options).


-spec apply_value(windows(), timestamp(), value(), apply_function()) -> windows().
apply_value(Windows, WindowToBeUpdatedTimestamp, NewValue, ApplyFunction) ->
    CurrentValue = get_value(WindowToBeUpdatedTimestamp, Windows),
    NewWindowValue = apply_value(CurrentValue, NewValue, ApplyFunction),
    set_value(WindowToBeUpdatedTimestamp, NewWindowValue, Windows).


-spec maybe_delete_last(windows(), non_neg_integer()) -> windows().
maybe_delete_last(Windows, MaxWindowsCount) ->
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


-spec reorganize_windows(windows(), windows(), non_neg_integer(), non_neg_integer()) ->
    ActionsToApplyOnRecords :: [{update_previos_record, windows()} | {update_current_record, timestamp(), windows()} |
        {split_current_record, {windows(), windows(), timestamp()}}].
reorganize_windows(WindowsInPrevRecord, WindowsInCurrentRecord, MaxWindowsInPrevRecord, SplitPoint) ->
    WindowsInPrevRecordSize = get_size(WindowsInPrevRecord),
    WindowsInCurrentRecordSize = get_size(WindowsInCurrentRecord),

    case WindowsInPrevRecordSize of
        MaxWindowsInPrevRecord ->
            [{split_current_record, split(WindowsInCurrentRecord, SplitPoint)}]; % One window has been added before reorganization
        _ ->
            case WindowsInPrevRecordSize + WindowsInCurrentRecordSize > MaxWindowsInPrevRecord of
                true ->
                    {WindowsInCurrentRecordPart1, WindowsInCurrentRecordPart2, SplitTimestamp} =
                        split(WindowsInCurrentRecord,
                            WindowsInCurrentRecordSize - (MaxWindowsInPrevRecord - WindowsInPrevRecordSize)),
                    UpdatedWindowsInPrevRecord = merge(WindowsInCurrentRecordPart2, WindowsInPrevRecord),
                    [{update_previos_record, UpdatedWindowsInPrevRecord},
                        {update_current_record, SplitTimestamp, WindowsInCurrentRecordPart1}];
                false ->
                    TimestampToUpdate = get_first_timestamp(WindowsInCurrentRecord),
                    [{update_previos_record, merge(WindowsInCurrentRecord, WindowsInPrevRecord)},
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

-spec apply_value(window_value() | undefined, value(), apply_function()) -> window_value().
apply_value(undefined, NewValue, sum) ->
    {1, NewValue};
apply_value({CurrentCount, CurrentSum}, NewValue, sum) ->
    {CurrentCount + 1, CurrentSum + NewValue};
apply_value(undefined, NewValue, max) ->
    NewValue;
apply_value(CurrentValue, NewValue, max) ->
    max(CurrentValue, NewValue);
apply_value(undefined, NewValue, min) ->
    NewValue;
apply_value(CurrentValue, NewValue, min) ->
    min(CurrentValue, NewValue);
apply_value(_CurrentValue, NewValue, last) ->
    NewValue;
apply_value(undefined, NewValue, first) ->
    NewValue;
apply_value(CurrentValue, _NewValue, first) ->
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


-spec list_values(timestamp() | undefined, windows(), options()) -> {ok | {continue, options()}, [window_value()]}.
list_values(undefined, Windows, Options) ->
    list_values(gb_trees:iterator(Windows), Options);
list_values(Timestamp, Windows, Options) ->
    list_values(gb_trees:iterator_from(reverse_timestamp(Timestamp), Windows), Options).


-spec list_values(gb_trees:iter(timestamp(), window_value()), options()) ->
    {ok | {continue, options()}, [window_value()]}.
list_values(_Iterator, #{limit := 0}) ->
    {ok, []};
list_values(Iterator, Options) ->
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
                    {Ans, Points} = list_values(NextIterator, Options#{limit := Limit - 1}),
                    {Ans, [{Timestamp, Value} | Points]};
                _ ->
                    {Ans, Points} = list_values(NextIterator, Options),
                    {Ans, [{Timestamp, Value} | Points]}
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
    gb_trees:from_orddict(gb_trees:to_list(Windows1) ++ gb_trees:to_list(Windows2)).


-spec to_list(windows()) -> [window()].
to_list(Windows) ->
    gb_trees:to_list(Windows).


-spec from_list([window()]) -> windows().
from_list(WindowsList) ->
    gb_trees:from_orddict(WindowsList).