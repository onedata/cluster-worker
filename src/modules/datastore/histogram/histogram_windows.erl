%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_windows).
-author("Michal Wrzeszcz").

%% API
-export([init/0, get/3, apply_value/4, maybe_delete_last/2,
    split_windows/2, should_reorganize_windows/2, reorganize_windows/3]).

-type timestamp() :: time:seconds().
-type window_value() :: value() | {ValuesCount :: non_neg_integer(), ValuesSum :: value()}.
-type window() :: {timestamp(), window_value()}.
-type windows() :: gb_trees:tree(timestamp(), window_value()).

-type options() :: #{
    start => timestamp(),
    stop => timestamp(),
    limit => non_neg_integer()
}.

-export_type([window_value/0, window/0, windows/0, options]).

-define(EPOCH_INFINITY, 9999999999). % GMT: Saturday, 20 November 2286 17:46:39

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> windows().
init() ->
    init_windows_set().


-spec get(windows(), timestamp(), options()) -> {ok, [value()]} | {continue, [value()], options()}.
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


-spec reorganize_windows(windows(), windows(), non_neg_integer()) ->
    ActionsToApplyOnRecords :: [{update_current_record | update_previos_record, windows()} |
        {split_current_record, {windows(), windows(), timestamp()}}].
reorganize_windows(WindowsInPrevRecord, WindowsInCurrentRecord, MaxWindowsInPrevRecord) ->
    WindowsInPrevRecordSize = get_size(WindowsInPrevRecord),
    WindowsInCurrentRecordSize = get_size(WindowsInCurrentRecord),

    case WindowsInPrevRecordSize of
        MaxWindowsInPrevRecord ->
            [{split_current_record, split(WindowsInCurrentRecord, 1)}]; % One window has been added before reorganization
        _ ->
            case WindowsInPrevRecordSize + WindowsInCurrentRecordSize > MaxWindowsInPrevRecord of
                true ->
                    {WindowsInCurrentRecordPart1, WindowsInCurrentRecordPart2} = split(WindowsInCurrentRecord,
                        WindowsInCurrentRecordSize - (MaxWindowsInPrevRecord - WindowsInPrevRecordSize)),
                    UpdatedWindowsInPrevRecord = merge(WindowsInCurrentRecordPart2, WindowsInPrevRecord),
                    [{update_previos_record, UpdatedWindowsInPrevRecord},
                        {update_current_record, WindowsInCurrentRecordPart1}];
                false ->
                    [{update_previos_record, merge(WindowsInCurrentRecord, WindowsInPrevRecord)}]
            end
    end.


%%=====================================================================
%% Internal functions
%%=====================================================================

-spec apply_value(window_value() | undefined, window_value, apply_function()) -> window_value().
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

% TODO - zmieniamy kolejnosc zeby bylo od najwiekszego do najmniejszego

key_timestamp_swap(Timestamp) ->
    ?EPOCH_INFINITY - Timestamp. % Reversed order of timestamps is required for listing


init_windows_set() ->
    gb_trees:empty().


get_value(Timestamp, Windows) ->
    case gb_trees:lookup(key_timestamp_swap(Timestamp), Windows) of
        {value, Value} -> Value;
        none -> undefined
    end.


set_value(Timestamp, Value, Windows) ->
    gb_trees:enter(key_timestamp_swap(Timestamp), Value, Windows).


delete_last(Windows) ->
    {_, _, UpdatedWindows} = gb_trees:take_largest(Windows),
    UpdatedWindows.


get_size(Windows) ->
    gb_trees:size(Windows).


list_values(Windows, undefined, Options) ->
    list_values(gb_sets:iterator(Windows), Options);
list_values(Windows, Timestamp, Options) ->
    list_values(gb_sets:iterator_from(key_timestamp_swap(Timestamp), Windows), Options).


list_values(_Iterator, #{limit := 0}) ->
    {ok, []};
list_values(Iterator, Options) ->
    case gb_sets:next(Iterator) of
        none ->
            {{continue, Options}, []};
        {Key, Value, NextIterator} ->
            Timestamp = key_timestamp_swap(Key),
            case Options of
                #{stop := Stop} when Timestamp < Stop ->
                    {ok, []};
                #{limit := Limit} ->
                    {Ans, Points} = list_values(NextIterator, Options#{limit := Limit - 1}),
                    {Ans, [{Timestamp, Value} | Points]};
                _ ->
                    {Ans, Points} = list_values(NextIterator, Options),
                    {Ans, [{Timestamp, Value} | Points]}
            end
    end.


split(Windows, SplitPosition) ->
    WindowsList = gb_sets:to_list(Windows),
    Windows1 = lists:sublist(WindowsList, SplitPosition),
    [{SplitKey, _} | _] = Windows2 = lists:sublist(WindowsList, SplitPosition + 1, length(Windows) - SplitPosition),
    {gb_sets:from_list(Windows1), gb_sets:from_list(Windows2), key_timestamp_swap(SplitKey)}.


merge(Windows1, Windows2) ->
    gb_sets:from_list(gb_sets:to_list(Windows1) ++ gb_sets:to_list(Windows2)).