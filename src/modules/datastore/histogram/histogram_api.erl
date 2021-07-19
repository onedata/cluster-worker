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
-module(histogram_api).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/logging.hrl").

%% API
-export([init/4, update/5, get/5]).

-record(histogram, {
    time_series :: time_series()
}).

-record(metrics, {
    config :: config(),
    % NOTE: Doc splitting strategy may result in keeping more windows than required by config
    % (in order to optimize documents management)
    doc_splitting_strategy :: doc_splitting_strategy(),
    data :: data()
}).

-record(config, {
    legend :: legend(),
    window_size :: non_neg_integer(),
    max_windows_count :: non_neg_integer(),
    apply_function :: apply_function()
}).

-record(doc_splitting_strategy, {
    max_docs_count :: non_neg_integer(),
    max_windows_in_head_doc :: non_neg_integer(),
    max_windows_in_tail_doc :: non_neg_integer()
}).

-record(data, {
    windows = histogram_windows:init() :: histogram_windows:windows(),
    prev_record_timestamp :: timestamp(),
    prev_record :: datastore:key()
}).

-type id() :: binary(). % Id of histogram
-type time_series_id() :: binary().
-type metrics_id() :: binary().

-type metrics() :: #metrics{}.
-type time_series() :: #{time_series_id() => #{metrics_id() => metrics()}}.
-type time_series_config() :: #{time_series_id() => #{metrics_id() => config()}}.

-type legend() :: binary().
-type config() :: #config{}.
-type doc_splitting_strategy() :: #doc_splitting_strategy{}.
-type data() :: #data{}.

-type apply_function() :: sum | max | min | last | first. % | {gather, Max}.
-type value() :: number().

-type requested_range() :: ok.
-type metrics_values_map() :: ok.
-type ctx() :: datastore:ctx().
-type batch() :: datastore_doc:batch().

-define(MAX_VALUES_IN_DOC, 50000).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(ctx(), id(), time_series_config(), batch()) -> {ok | {error, term()}, batch()}.
init(Ctx, Id, ConfigMap, Batch) ->
    try
        TimeSeries = maps:fold(fun(TimeSeriesId, MetricsConfigs, Acc) ->
            MetricsMap = maps:fold(fun(MetricsId, Config, InternalAcc) ->
                 InternalAcc#{MetricsId => #metrics{
                     config = Config,
                     doc_splitting_strategy = create_doc_splitting_strategy(Config)
                 }}
            end, Acc, MetricsConfigs),
            Acc#{TimeSeriesId => MetricsMap}
        end, #{}, ConfigMap),

        PersistenceCtx = histogram_persistence:new(Ctx, Id, #histogram{time_series = TimeSeries}, Batch),
        {ok, histogram_persistence:finalize(PersistenceCtx)}
    catch
        Error:Reason ->
            ?error_stacktrace("Histogram ~p init error: ~p:~p~nConfig: ~p",
                [Id, Error, Reason, ConfigMap]),
            {{error, historgam_init_failed}, Batch}
    end.


-spec update(ctx(), id(), timestamp(), value(), batch()) -> {ok | {error, term()}, batch()}.
update(Ctx, Id, NewTimestamp, NewValue, Batch) ->
    try
        {TimeSeries, PersistenceCtx} = histogram_persistence:init(Ctx, Id, Batch),
        FinalPersistenceCtx = update_time_series(maps:to_list(TimeSeries), NewTimestamp, NewValue, PersistenceCtx),
        {ok, histogram_persistence:finalize(FinalPersistenceCtx)}
    catch
        Error:Reason ->
            ?error_stacktrace("Histogram ~p update error: ~p:~p~nFailed to update point {~p, ~p}",
                [Id, Error, Reason, NewTimestamp, NewValue]),
            {{error, historgam_update_failed}, Batch}
    end.


-spec get(ctx(), id(), requested_range() | [requested_range()], histogram_windows:options(), batch()) ->
    {{ok, [histogram_windows:window()]} | {ok, metrics_values_map()} | {error, term()}, batch()}.
get(Ctx, Id, RequestedRanges, Options, Batch) ->
    try
        {TimeSeries, PersistenceCtx} = histogram_persistence:init(Id, Ctx, Batch),
        {Ans, FinalPersistenceCtx} = get_time_series_values(TimeSeries, RequestedRanges, Options, PersistenceCtx),
        {{ok, Ans}, histogram_persistence:finalize(FinalPersistenceCtx)}
    catch
        Error:Reason ->
            ?error_stacktrace("Histogram ~p get error: ~p:~p~nRequested ranges: ~p~nOptions: ~p",
                [Id, Error, Reason, RequestedRanges, Options]),
            {{error, historgam_get_failed}, Batch}
    end.


%%=====================================================================
%% Internal functions operation on time series
%%=====================================================================

update_time_series([], _NewTimestamp, _NewValue, PersistenceCtx) ->
    PersistenceCtx;
update_time_series([{_TimeSeriesId, TimeSeries} | Tail], NewTimestamp, NewValue, PersistenceCtx) ->
    UpdatedPersistenceCtx = update_metrics(maps:to_list(TimeSeries), NewTimestamp, NewValue, PersistenceCtx),
    update_time_series(Tail, NewTimestamp, NewValue, UpdatedPersistenceCtx).


get_time_series_values(_TimeSeriesMap, [], _Options, PersistenceCtx) ->
    {#{}, PersistenceCtx};
get_time_series_values(TimeSeriesMap, [{TimeSeriesIds, MetricsIds} | Requests], Options, PersistenceCtx) ->
    {Ans, UpdatedPersistenceCtx} = lists:foldl(fun(TimeSeriesId, Acc) ->
        lists:foldl(fun(MetricsId, {TmpAns, TmpPersistenceCtx}) ->
            MetricsMap = maps:get(TimeSeriesId, TimeSeriesMap, #{}),
            Values = case maps:get(MetricsId, MetricsMap, undefined) of
                undefined ->
                    undefined;
                #metrics{
                    data = Data,
                    config = Config
                } ->
                    Window = get_window(maps:get(start, Options, undefined), Config),
                    get_metrics_values(Data, Window, Options, TmpPersistenceCtx)
            end,
            {TmpAns#{{TimeSeriesId, MetricsId} => Values}, TmpPersistenceCtx}
        end, Acc, utils:ensure_list(MetricsIds))
    end, {#{}, PersistenceCtx}, utils:ensure_list(TimeSeriesIds)),

    {Ans2, FinalPersistenceCtx} = get_time_series_values(TimeSeriesMap, Requests, Options, UpdatedPersistenceCtx),
    {maps:merge(Ans, Ans2), FinalPersistenceCtx};
get_time_series_values(TimeSeriesMap, Request, Options, PersistenceCtx) ->
    {Ans, FinalPersistenceCtx} = get_time_series_values(TimeSeriesMap, [Request], Options, PersistenceCtx),
    case maps:get(Request, Ans, undefined) of
        undefined -> {Ans, FinalPersistenceCtx};
        GetAns -> {GetAns, FinalPersistenceCtx}
    end.



%%=====================================================================
%% Internal functions operating on metrics
%%=====================================================================

update_metrics([], _NewTimestamp, _NewValue, PersistenceCtx) ->
    PersistenceCtx;
update_metrics(
    [{_MetricsId, #metrics{
        config = #config{apply_function = ApplyFunction} = Config,
        doc_splitting_strategy = DocSplittingStrategy,
        data = Data
    }} | Tail], NewTimestamp, NewValue, PersistenceCtx) ->
    WindowToBeUpdated = get_window(NewTimestamp, Config),
    DataDocKey = histogram_persistence:get_head_key(PersistenceCtx),
    UpdatedPersistenceCtx = update_metrics(
        Data, DataDocKey, 1, DocSplittingStrategy, ApplyFunction, WindowToBeUpdated, NewValue, PersistenceCtx),
    update_metrics(Tail, NewTimestamp, NewValue, UpdatedPersistenceCtx).


update_metrics(
    #data{
        windows = Windows
    } = Data, DataDocKey, _DataDocPosition,
    #doc_splitting_strategy{
        max_docs_count = 1,
        max_windows_in_head_doc = MaxWindowsCount
    }, ApplyFunction, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    UpdatedWindows = histogram_windows:apply_value(Windows, WindowToBeUpdated, NewValue, ApplyFunction),
    FinalWindows = histogram_windows:maybe_delete_last(UpdatedWindows, MaxWindowsCount),
    histogram_persistence:update(DataDocKey, Data#data{windows = FinalWindows}, PersistenceCtx);

update_metrics(
    #data{
        prev_record = undefined,
        prev_record_timestamp = PrevRecordTimestamp
    }, _DataDocKey, _DataDocPosition,
    _DocSplittingStrategy, _ApplyFunction, WindowToBeUpdated, _NewValue, PersistenceCtx)
    when PrevRecordTimestamp =/= undefined andalso PrevRecordTimestamp >= WindowToBeUpdated ->
    PersistenceCtx;

update_metrics(
    #data{
        prev_record = undefined,
        windows = Windows
    } = Data, DataDocKey, DataDocPosition,
    DocSplittingStrategy, ApplyFunction, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    UpdatedWindows = histogram_windows:apply_value(Windows, WindowToBeUpdated, NewValue, ApplyFunction),
    MaxWindowsCount = get_max_windows_in_doc(DataDocKey, DocSplittingStrategy, PersistenceCtx),

    case histogram_windows:should_reorganize_windows(Windows, MaxWindowsCount) of
        true ->
            % Adding of single window resulted in reorganization so split should be at first element
            {Windows1, Windows2, SplitTimestamp} = histogram_windows:split_windows(UpdatedWindows, 1),
            {CreatedRecordKey, CreatedRecord, UpdatedPersistenceCtx} = split_record(
                DataDocKey, Data, Windows1, Windows2, SplitTimestamp, PersistenceCtx),
            maybe_delete_last_doc(
                CreatedRecordKey, CreatedRecord, DocSplittingStrategy, DataDocPosition + 1, UpdatedPersistenceCtx);
        false ->
            histogram_persistence:update(DataDocKey, Data#data{windows = UpdatedWindows}, PersistenceCtx)
    end;

update_metrics(
    #data{
        prev_record = PrevRecordKey,
        prev_record_timestamp = PrevRecordTimestamp
    }, _DataDocKey, DataDocPosition,
    DocSplittingStrategy, ApplyFunction, WindowToBeUpdated, NewValue, PersistenceCtx)
    when PrevRecordTimestamp >= WindowToBeUpdated ->
    {PrevRecordData, UpdatedPersistenceCtx} = histogram_persistence:get(PrevRecordKey, PersistenceCtx),
    update_metrics(PrevRecordData, PrevRecordKey, DataDocPosition + 1,
        DocSplittingStrategy, ApplyFunction, WindowToBeUpdated, NewValue, UpdatedPersistenceCtx);

update_metrics(
    #data{
        windows = Windows,
        prev_record = PrevRecordKey
    } = Data, DataDocKey, DataDocPosition,
    #doc_splitting_strategy{
        max_windows_in_tail_doc = MaxWindowsInTail
    } = DocSplittingStrategy, ApplyFunction, WindowToBeUpdated, NewValue, PersistenceCtx) ->
    UpdatedWindows = histogram_windows:apply_value(Windows, WindowToBeUpdated, NewValue, ApplyFunction),
    MaxWindowsCount = get_max_windows_in_doc(DataDocKey, DocSplittingStrategy, PersistenceCtx),

    case histogram_windows:should_reorganize_windows(Windows, MaxWindowsCount) of
        true ->
            {#data{windows = WindowsInPrevRecord} = PrevRecordData, UpdatedPersistenceCtx} =
                histogram_persistence:get(PrevRecordKey, PersistenceCtx),
            Actions = histogram_windows:reorganize_windows(WindowsInPrevRecord, UpdatedWindows, MaxWindowsInTail),

            lists:foldl(fun
                ({update_current_record, UpdatedWindows}, TmpPersistenceCtx) ->
                    histogram_persistence:update(DataDocKey, Data#data{windows = UpdatedWindows}, TmpPersistenceCtx);
                ({split_current_record, {Windows1, Windows2, SplitTimestamp}}, TmpPersistenceCtx) ->
                    {_CreatedRecordKey, _CreatedRecord, UpdatedTmpPersistenceCtx} = split_record(
                        DataDocKey, Data, Windows1, Windows2, SplitTimestamp, TmpPersistenceCtx),
                    maybe_delete_last_doc(
                        PrevRecordKey, PrevRecordData, DocSplittingStrategy, DataDocPosition + 2, UpdatedTmpPersistenceCtx);
                ({update_previos_record, UpdatedWindowsInPrevRecord}, TmpPersistenceCtx) ->
                    histogram_persistence:update(PrevRecordKey,
                        PrevRecordData#data{windows = UpdatedWindowsInPrevRecord}, TmpPersistenceCtx)
            end, UpdatedPersistenceCtx, Actions);
        false ->
            histogram_persistence:update(DataDocKey, Data#data{windows = UpdatedWindows}, PersistenceCtx)
    end.


maybe_delete_last_doc(Key, _Data, #doc_splitting_strategy{max_docs_count = MaxCount}, DocumentNumber, PersistenceCtx)
    when DocumentNumber > MaxCount ->
    histogram_persistence:delete(Key, PersistenceCtx);
maybe_delete_last_doc(Key, undefined, DocSplittingStrategy, DocumentNumber, PersistenceCtx) ->
    {Data, UpdatedPersistenceCtx} = histogram_persistence:get(Key, PersistenceCtx),
    maybe_delete_last_doc(Key, Data, DocSplittingStrategy, DocumentNumber, UpdatedPersistenceCtx);
maybe_delete_last_doc(_Key, #data{prev_record = undefined}, _DocSplittingStrategy, _DocumentNumber, PersistenceCtx) ->
    PersistenceCtx;
maybe_delete_last_doc(_Key, #data{prev_record = PrevRecordKey}, DocSplittingStrategy, DocumentNumber, PersistenceCtx) ->
    maybe_delete_last_doc(PrevRecordKey, undefined, DocSplittingStrategy, DocumentNumber + 1, PersistenceCtx).


get_metrics_values(
    #data{
        prev_record = PrevRecordKey,
        prev_record_timestamp = PrevRecordTimestamp
    }, Window, Options, PersistenceCtx)
    when Window =/= undefined andalso PrevRecordTimestamp >= Window ->
    {PrevRecordData, UpdatedPersistenceCtx} = histogram_persistence:get(PrevRecordKey, PersistenceCtx),
    get_metrics_values(PrevRecordData, Window, Options, UpdatedPersistenceCtx);

get_metrics_values(
    #data{
        windows = Windows,
        prev_record = PrevRecordKey
    }, Window, Options, PersistenceCtx) ->
    case histogram_windows:get(Windows, Window, Options) of
        {ok, Points} ->
            Points;
        {{continue, NewOptions}, Points} ->
            {PrevRecordData, UpdatedPersistenceCtx} = histogram_persistence:get(PrevRecordKey, PersistenceCtx),
            {NextPoints, FinalPersistenceCtx} =
                get_metrics_values(PrevRecordData, undefined, NewOptions, UpdatedPersistenceCtx),
            {Points ++ NextPoints, FinalPersistenceCtx}
    end.


%%=====================================================================
%% Helper functions
%%=====================================================================

% TODO - przy inicie sprawdzic ze liczba okien jest wieksza od zera
% TODO - a co jesli dokument taila nie jest pelen (bo sie podzielil w wyniku dodawania starego elementu) - trzeba to uwzglednic wyznaczajac ilosc dokumentow i przy dzieleniu
% do liczenia ilosci dokumentow wszystkie docki przyjmujemy jako polowa pojemnosci bo tyle moze miec najmniej po splicie
% wyjatkiem jest drugi dokument puki sie nie wypelni po raz pierwszy ale wtedy nie bedziemy probowali kasowac
create_doc_splitting_strategy(#config{
    max_windows_count = MaxWindowsCount
}) ->
    #doc_splitting_strategy{
        max_docs_count = ceil(MaxWindowsCount / ?MAX_VALUES_IN_DOC),
        max_windows_in_head_doc = ?MAX_VALUES_IN_DOC,
        max_windows_in_tail_doc = ?MAX_VALUES_IN_DOC
}.


get_window(undefined, _) ->
    undefined;
get_window(Time, #config{window_size = WindowSize}) ->
    Time - Time rem WindowSize.


get_max_windows_in_doc(
    DataDocKey,
    #doc_splitting_strategy{
        max_windows_in_head_doc = MaxWindowsCountInHead,
        max_windows_in_tail_doc = MaxWindowsCountInTail
    },
    PersistenceCtx) ->
    case histogram_persistence:is_head(DataDocKey, PersistenceCtx) of
        true -> MaxWindowsCountInHead;
        false -> MaxWindowsCountInTail
    end.


split_record(DataDocKey, Data, Windows1, Windows2, SplitTimestamp, PersistenceCtx) ->
    DataToCreate = Data#data{windows = Windows2},
    {CreatedRecordKey, UpdatedPersistenceCtx} = histogram_persistence:create(DataToCreate, PersistenceCtx),
    UpdatedData = Data#data{windows = Windows1, prev_record = CreatedRecordKey, prev_record_timestamp = SplitTimestamp},
    {CreatedRecordKey, DataToCreate, histogram_persistence:update(DataDocKey, UpdatedData, UpdatedPersistenceCtx)}.