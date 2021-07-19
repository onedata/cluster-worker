%%%-------------------------------------------------------------------
%%% @author michal
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Jul 2021 11:21 AM
%%%-------------------------------------------------------------------
-module(histogram_tmp).
-author("michal").

%% API
-export([]).

-record(histogram, {
    configs
}).

% TODO - podzielic na config wlasciwy i serie ktora max_windows_in_document i reszte rzeczy per dokument
-record(histogram_config, {
    id, % tworzymy propliste w odpowiedniej kolejnosci

    window_size,
    max_windows_in_document,
    max_documents_count,
    windows,
    next_doc_timestamp, % TODO - zmienic next na prev
    next_doc_key
}).

-record(persistence, {
    ctx,
    batch
}).

update(Ctx, HistogramId, NewTimestamp, NewValue, UpdateType, Batch) ->
    try
        {Configs, Persistence} = init_persistance(HistogramId, Ctx, Batch),
        update(Configs, NewTimestamp, NewValue, UpdateType, Persistence)
    catch
        E1:E2 ->
            ok
    end.

update([FirstConfig | Configs], NewTimestamp, NewValue, UpdateType, Persistence) ->
    {SlotValueChange, UpdatedPersistence} = update_config(FirstConfig, NewTimestamp, NewValue, UpdateType, Persistence),
    lists:foldl(fun(Config, PersistenceAcc) ->
        {_, UpdatedPersistenceAbc} = update_config(Config, NewTimestamp, SlotValueChange, add, PersistenceAcc),
        UpdatedPersistenceAbc
    end, UpdatedPersistence, Configs).

get(Ctx, Id, TimeSeriesId, Options, Batch) ->

    ok.


init_persistance(HistogramId, Ctx, Batch) ->
    #persistence{ctx = Ctx, batch = Batch}.

%%=====================================================================
%% Internal API to operate on single config
%%=====================================================================

update_config(Config, NewTimestamp, NewValue, UpdateType, Persistence) ->
    WindowToBeUpdated = get_window(NewTimestamp, Config),
    update_window_value(Config, WindowToBeUpdated, NewValue, UpdateType, Persistence, 1).


get_window(Time, #histogram_config{window_size = WindowSize}) ->
    Time - Time rem WindowSize.

% TODO - w sentinelu trzymamy Config, i poczatki wszystkich serii natomiast jesli przekrocza one pojemnosc
% to wypychamy je do kolejnych dokumentow (kazda seria ma juz swoj osobny dokument)

get_slot_value(#histogram_config{next_doc_key = NextDocKey = next_doc_timestamp = NextDocTimestamp},
    Slot, Persistence) when NextDocTimestamp =/= undefined andalso NextDocTimestamp >= Slot ->
    {NextConfig, UpdatedPersistence} = get_config(NextDocKey, Persistence),
    get_slot_value(NextConfig, Slot, UpdatedPersistence);
get_slot_value(#histogram_config{windows = Windows}, Slot, Persistence) ->
    {histogram_windows:get_value(Slot, Windows), Persistence}.

update_window_value(
    Config = #histogram_config{
        windows = Windows,
        max_documents_count = 1,
        max_windows_in_document = MaxWindowsCount
    }, WindowToBeUpdated, NewValue, UpdateType, Persistence, _DocumentNumber) ->
    {SlotValueChange, UpdatedWindows} = add_or_reset_window(Windows, WindowToBeUpdated, NewValue, UpdateType),
    FinalWindows = maybe_delete_last(UpdatedWindows, MaxWindowsCount),
    {SlotValueChange, update_config(Config#histogram_config{windows = FinalWindows}, Persistence)};
update_window_value(
    #histogram_config{
        next_doc_key = NextDocKey,
        next_doc_timestamp = NextDocTimestamp
    }, WindowToBeUpdated, NewValue, UpdateType, Persistence, _DocumentNumber)
    when NextDocTimestamp =/= undefined andalso NextDocTimestamp >= WindowToBeUpdated ->
    {NextConfig, UpdatedPersistence} = get_config(NextDocKey, Persistence),
    update_window_value(NextConfig, WindowToBeUpdated, NewValue, UpdateType, UpdatedPersistence);
update_window_value(
    Config = #histogram_config{
        windows = Windows,
        max_windows_in_document = MaxWindowsCount,
        next_doc_key = NextDocKey
    }, WindowToBeUpdated, NewValue, UpdateType, Persistence, DocumentNumber) ->
    {SlotValueChange, UpdatedWindows} = add_or_reset_window(Windows, WindowToBeUpdated, NewValue, UpdateType),

    case should_split_windows_set(Windows, MaxWindowsCount) of
        true ->
            {#histogram_config{windows = PreviousWindows} = NextConfig, UpdatedPersistence} =
                get_config(NextDocKey, Persistence),
            Actions = reorganize_windows(PreviousWindows, UpdatedWindows, MaxWindowsCount),

            FinalPersistence = lists:foldl(fun
                ({update_new, UpdatedWindows}, TmpPersistence) ->
                    update_config(Config#histogram_config{windows = UpdatedWindows}, TmpPersistence);
                ({split_new, {Window1, Window2, SplitTimestamp}}, TmpPersistence) ->
                    NewConfig = Config#histogram_config{windows = Window2, next_doc_key = NextDocKey},
                    UpdatedTmpPersistence = update_config(NewConfig, TmpPersistence),
                    UpdatedConfig = Config#histogram_config{
                        windows = Window1, next_doc_key = NewDocKey, next_doc_timestamp = SplitTimestamp},
                    UpdatedTmpPersistence2 = update_config(UpdatedConfig, UpdatedTmpPersistence),
                    maybe_delete_last_doc(NewConfig, UpdatedTmpPersistence2, DocumentNumber + 1);
                ({update_previos, UpdatedPreviousWindows}, TmpPersistence) ->
                    update_config(NextConfig#histogram_config{windows = UpdatedPreviousWindows}, TmpPersistence)
            end, UpdatedPersistence, Actions),

            {SlotValueChange, FinalPersistence};
        false ->
            {SlotValueChange, update_config(Config#histogram_config{windows = UpdatedWindows}, Persistence)}
    end.

maybe_delete_last_doc(Config = #histogram_config{max_documents_count = MaxCount}, Persistence, DocumentNumber)
    when DocumentNumber > MaxCount ->
    delete_config(Config, Persistence);
maybe_delete_last_doc(#histogram_config{next_doc_key = undefined}, Persistence, _DocumentNumber) ->
    Persistence;
maybe_delete_last_doc(#histogram_config{next_doc_key = NextDocKey}, Persistence, DocumentNumber) ->
    {NextConfig, UpdatedPersistence} = get_config(NextDocKey, Persistence),
    maybe_delete_last_doc(NextConfig, UpdatedPersistence, DocumentNumber + 1).


%%    % TODO - dodac informacje do splita od razu ile mozna wyciagnac do nowego docka
%%    case maybe_split_windows_set(UpdatedWindows, MaxWindowsCount) of
%%        split_not_required ->
%%            {SlotValueChange, update_config(Config#histogram_config{windows = UpdatedWindows}, Persistence)};
%%        [Window1, Window2, SplitTimestamp] ->
%%            % TODO - w nowych okna ustawiac inny max_windows_in_document
%%            {#histogram_config{windows = NextConfigWindows} = NextConfig, UpdatedPersistence} =
%%                get_config(NextDocKey, Persistence),
%%            FinalPersistence = case reorganize_windows(NextConfigWindows, Window2) of
%%                nothing_changed ->
%%                    NewConfig = Config#histogram_config{windows = Window2, next_doc_key = NextDocKey},
%%                    UpdatedPersistence = update_config(NewConfig, Persistence),
%%                    UpdatedConfig = Config#histogram_config{
%%                        windows = Window1, next_doc_key = NewDocKey, next_doc_timestamp = SplitTimestamp},
%%                    update_config(UpdatedConfig, UpdatedPersistence);
%%                {merged, UpdatedNextConfigWindows} ->
%%                    UpdatedPersistence = update_config(
%%                        NextConfig#histogram_config{windows = UpdatedNextConfigWindows}, Persistence),
%%                    update_config(Config#histogram_config{
%%                        windows = Window1, next_doc_timestamp = SplitTimestamp}, UpdatedPersistence);
%%                {UpdatedNextConfigWindows, UpdatedWindow2} ->
%%                    UpdatedPersistence = update_config(
%%                        NextConfig#histogram_config{windows = UpdatedNextConfigWindows}, Persistence),
%%                    NewConfig = Config#histogram_config{windows = UpdatedWindow2, next_doc_key = NextDocKey},
%%                    UpdatedPersistence2 = update_config(NewConfig, UpdatedPersistence),
%%                    UpdatedConfig = Config#histogram_config{
%%                        windows = Window1, next_doc_key = NewDocKey, next_doc_timestamp = SplitTimestamp},
%%                    update_config(UpdatedConfig, UpdatedPersistence2);
%%            end,
%%
%%            % TODO - moze skasowac ostatni doc?
%%            {SlotValueChange, FinalPersistence}
%%    end.

add_or_reset_window(Windows, WindowToBeUpdated, NewValue, UpdateType) ->
    {ValuesCount, ValuesSum} = case histogram_windows:get_value(WindowToBeUpdated, Windows) of
        undefined -> {0, 0};
        Value -> Value
    end,

    {NewSlotValue, SlotValueChange} = case UpdateType of
        add -> {{ValuesCount + 1, ValuesSum + NewValue}, NewValue};
        reset -> {{1, NewValue}, -1 * ValuesSum + NewValue}
    end,


    UpdatedWindows = histogram_windows:set_value(WindowToBeUpdated, NewSlotValue, Windows),
    {SlotValueChange, UpdatedWindows}.

maybe_delete_last(Windows, MaxWindowsCount) ->
    case histogram_windows:get_size(Windows) > MaxWindowsCount of
        true ->
            histogram_windows:delete_last(Windows);
        false ->
            Windows
    end.

should_split_windows_set(Windows, MaxWindowsCount) ->
    histogram_windows:get_size(Windows) > MaxWindowsCount.

reorganize_windows(PreviousWindows, NewWindows, MaxWindowsCount) ->
    ExistingSize = histogram_windows:get_size(PreviousWindows),
    NewSize = histogram_windows:get_size(NewWindows),

    case ExistingSize of
        ?MAX_DOC_WINDOWS_COUNT ->
            [{split_new, histogram_windows:split(NewWindows, ?MAX_DOC_WINDOWS_COUNT)}];
        _ ->
            case ExistingSize + NewSize > ?MAX_DOC_WINDOWS_COUNT of
                true ->
                    {NewWindowsPart1, NewWindowsPart2} =
                        histogram_windows:split(NewWindows, NewSize - (?MAX_DOC_WINDOWS_COUNT - ExistingSize)),
                    UpdatedExistingWindows = histogram_windows:merge(NewWindowsPart2, PreviousWindows),

                    case histogram_windows:get_size(NewWindowsPart1) > MaxWindowsCount of
                        true ->
                            [{update_previos, UpdatedExistingWindows},
                                {split_new, histogram_windows:split(NewWindowsPart1, ?MAX_DOC_WINDOWS_COUNT)}];
                        false ->
                            [{update_previos, UpdatedExistingWindows}, {update_new, NewWindowsPart1}]
                    end;
                false ->
                    [{update_previos, histogram_windows:merge(NewWindows, PreviousWindows)}]
            end
    end.



%%maybe_split_windows_set(Windows, MaxWindowsCount) ->
%%    case histogram_windows:get_size(Windows) > MaxWindowsCount of
%%        true ->
%%            histogram_windows:split(Windows, ?MAX_DOC_WINDOWS_COUNT);
%%        false ->
%%            split_not_required
%%    end.
%%
%%reorganize_windows(ExistingWindows, NewWindows) ->
%%    ExistingSize = histogram_windows:get_size(ExistingWindows),
%%    NewSize = histogram_windows:get_size(NewWindows),
%%
%%    case ExistingSize of
%%        ?MAX_DOC_WINDOWS_COUNT ->
%%            nothing_changed;
%%        _ ->
%%            case ExistingSize + NewSize > ?MAX_DOC_WINDOWS_COUNT of
%%                true ->
%%                    {NewWindowsPart1, NewWindowsPart2} =
%%                        histogram_windows:split(NewWindows, NewSize - (?MAX_DOC_WINDOWS_COUNT - ExistingSize)),
%%                    {histogram_windows:merge(NewWindowsPart2, ExistingWindows), NewWindowsPart1};
%%                false ->
%%                    {merged, histogram_windows:merge(NewWindows, ExistingWindows)}
%%            end
%%    end.

%%=====================================================================
%% Internal API to operate on windows map
%%=====================================================================

put_entry(Entries, Key, NewEntry) ->
    ok.

get_entry_value(Entries, Key) ->
    % Tutaj dodajemy dokument jesli nowy slot jest potrzebny
    ok.

delete_last_entry(Entries) ->
    ok.

get_entries_count(Entries) ->
    ok.