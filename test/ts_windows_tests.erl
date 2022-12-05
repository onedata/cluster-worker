%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Eunit tests for the ts_windows module.
%%% @end
%%%-------------------------------------------------------------------
-module(ts_windows_tests).
-author("Michal Wrzeszcz").

-ifdef(TEST).

-include("modules/datastore/datastore_time_series.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/hashing/consistent_hashing.hrl").


-define(EXEMPLARY_AGGREGATOR, max). % When test verifies mechanism that works the same for all aggregators,
                                    % ?EXEMPLARY_AGGREGATOR is used instead of repeating test for all aggregators.
-define(LIST_ALL(Windows), ?LIST(Windows, undefined, #{})).
-define(LIST(Windows, Timestamp, Options),
    ts_windows:list(Windows, Timestamp, Options#{return_type => basic_info, aggregator => ?EXEMPLARY_AGGREGATOR})).
-define(LIST_ALL_RESULT(List), ?LIST_CONTINUE_RESULT(List, #{window_limit => 1000 - length(List)})).
-define(WINDOWS_INFO_LIST(List),
    lists:map(fun({T, V}) -> #window_info{
        timestamp = T, value = V, first_measurement_timestamp = undefined, last_measurement_timestamp = undefined
    } end, List)
).
-define(LIST_CONTINUE_RESULT(List, ContinueOptions), begin
    {
        {continue, ContinueOptions#{return_type => basic_info, aggregator => ?EXEMPLARY_AGGREGATOR}},
        ?WINDOWS_INFO_LIST(List)
    }
end).
-define(WINDOW(AggregatedMeasurements, Timestamp), #window{
    aggregated_measurements = AggregatedMeasurements,
    first_measurement_timestamp = Timestamp,
    last_measurement_timestamp = Timestamp
}).

%%%===================================================================
%%% Tests
%%%===================================================================

add_new_measurement_test() ->
    Windows = ts_windows:init(),
    WindowTimestamp = 1,
    MeasurementTimestamp = 2,

    ?assertEqual(undefined, ts_windows:get(WindowTimestamp, Windows)),

    Test1 = ts_windows:insert(Windows, WindowTimestamp, {MeasurementTimestamp, 1}, {aggregate_measurement, avg}),
    ?assertEqual(?WINDOW({1, 1}, MeasurementTimestamp), ts_windows:get(WindowTimestamp, Test1)),

    Test2 = ts_windows:insert(Windows, WindowTimestamp, {MeasurementTimestamp, 1}, {aggregate_measurement, max}),
    ?assertEqual(?WINDOW(1, MeasurementTimestamp), ts_windows:get(WindowTimestamp, Test2)),

    Test3 = ts_windows:insert(Windows, WindowTimestamp, {MeasurementTimestamp, 1}, {aggregate_measurement, min}),
    ?assertEqual(?WINDOW(1, MeasurementTimestamp), ts_windows:get(WindowTimestamp, Test3)),

    Test4 = ts_windows:insert(Windows, WindowTimestamp, {MeasurementTimestamp, 1}, {aggregate_measurement, last}),
    ?assertEqual(?WINDOW(1, MeasurementTimestamp), ts_windows:get(WindowTimestamp, Test4)),

    Test5 = ts_windows:insert(Windows, WindowTimestamp, {MeasurementTimestamp, 1}, {aggregate_measurement, first}),
    ?assertEqual(?WINDOW(1, MeasurementTimestamp), ts_windows:get(WindowTimestamp, Test5)),

    Test6 = ts_windows:insert(Windows, WindowTimestamp, {MeasurementTimestamp, 1}, {aggregate_measurement, sum}),
    ?assertEqual(?WINDOW(1, MeasurementTimestamp), ts_windows:get(WindowTimestamp, Test6)).


add_with_existing_timestamp_test() ->
    WindowTimestamp = 1,
    MeasurementTimestamp = 2,
    AddedMeasurementTimestamp = 3,
    AddedMeasurementLowerTimestamp = 1,
    Windows = ts_windows:insert(
        ts_windows:init(), WindowTimestamp, {MeasurementTimestamp, 2}, {aggregate_measurement, max}),

    Test1 = ts_windows:insert(Windows, WindowTimestamp, {AddedMeasurementTimestamp, 3}, {aggregate_measurement, max}),
    ?assertEqual(#window{
        aggregated_measurements = 3,
        first_measurement_timestamp = MeasurementTimestamp,
        last_measurement_timestamp = AddedMeasurementTimestamp
    }, ts_windows:get(WindowTimestamp, Test1)),
    Test2 = ts_windows:insert(Windows, WindowTimestamp, {AddedMeasurementTimestamp, 1}, {aggregate_measurement, max}),
    ?assertEqual(#window{
        aggregated_measurements = 2,
        first_measurement_timestamp = MeasurementTimestamp,
        last_measurement_timestamp = AddedMeasurementTimestamp
    }, ts_windows:get(WindowTimestamp, Test2)),

    Test3 = ts_windows:insert(Windows, WindowTimestamp, {AddedMeasurementTimestamp, 1}, {aggregate_measurement, min}),
    ?assertEqual(#window{
        aggregated_measurements = 1,
        first_measurement_timestamp = MeasurementTimestamp,
        last_measurement_timestamp = AddedMeasurementTimestamp
    }, ts_windows:get(WindowTimestamp, Test3)),
    Test4 = ts_windows:insert(Windows, WindowTimestamp, {AddedMeasurementTimestamp, 3}, {aggregate_measurement, min}),
    ?assertEqual(#window{
        aggregated_measurements = 2,
        first_measurement_timestamp = MeasurementTimestamp,
        last_measurement_timestamp = AddedMeasurementTimestamp
    }, ts_windows:get(WindowTimestamp, Test4)),

    Test5 = ts_windows:insert(Windows, WindowTimestamp, {AddedMeasurementLowerTimestamp, 3}, {aggregate_measurement, last}),
    ?assertEqual(#window{
        aggregated_measurements = 2,
        first_measurement_timestamp = AddedMeasurementLowerTimestamp,
        last_measurement_timestamp = MeasurementTimestamp
    }, ts_windows:get(WindowTimestamp, Test5)),
    Test6 = ts_windows:insert(Windows, WindowTimestamp, {AddedMeasurementTimestamp, 3}, {aggregate_measurement, last}),
    ?assertEqual(#window{
        aggregated_measurements = 3,
        first_measurement_timestamp = MeasurementTimestamp,
        last_measurement_timestamp = AddedMeasurementTimestamp
    }, ts_windows:get(WindowTimestamp, Test6)),

    Test7 = ts_windows:insert(Windows, WindowTimestamp, {AddedMeasurementLowerTimestamp, 3}, {aggregate_measurement, first}),
    ?assertEqual(#window{
        aggregated_measurements = 3,
        first_measurement_timestamp = AddedMeasurementLowerTimestamp,
        last_measurement_timestamp = MeasurementTimestamp
    }, ts_windows:get(WindowTimestamp, Test7)),
    Test8 = ts_windows:insert(Windows, WindowTimestamp, {AddedMeasurementTimestamp, 3}, {aggregate_measurement, first}),
    ?assertEqual(#window{
        aggregated_measurements = 2,
        first_measurement_timestamp = MeasurementTimestamp,
        last_measurement_timestamp = AddedMeasurementTimestamp
    }, ts_windows:get(WindowTimestamp, Test8)),

    Test9 = ts_windows:insert(Windows, WindowTimestamp, {AddedMeasurementTimestamp, 3}, {aggregate_measurement, sum}),
    ?assertEqual(#window{
        aggregated_measurements = 5,
        first_measurement_timestamp = MeasurementTimestamp,
        last_measurement_timestamp = AddedMeasurementTimestamp
    }, ts_windows:get(WindowTimestamp, Test9)),
    
    WindowsWithAvg = ts_windows:insert(
        ts_windows:init(), WindowTimestamp, {MeasurementTimestamp, 2}, {aggregate_measurement, avg}),
    Test10 = ts_windows:insert(WindowsWithAvg, WindowTimestamp, {AddedMeasurementTimestamp, 3}, {aggregate_measurement, avg}),
    ?assertEqual(#window{
        aggregated_measurements = {2, 5},
        first_measurement_timestamp = MeasurementTimestamp,
        last_measurement_timestamp = AddedMeasurementTimestamp
    }, ts_windows:get(WindowTimestamp, Test10)).


prune_overflowing_windows_test() ->
    Timestamp1 = 1,
    Timestamp2 = 2,
    Windows1 = ts_windows:insert(ts_windows:init(), Timestamp1, {Timestamp1, 1}, {aggregate_measurement, ?EXEMPLARY_AGGREGATOR}),
    Windows2 = ts_windows:insert(Windows1, Timestamp2, {Timestamp2, 1}, {aggregate_measurement, ?EXEMPLARY_AGGREGATOR}),

    Test1 = ts_windows:prune_overflowing(Windows2, 3),
    ?assertEqual(Windows2, Test1),

    Test2 = ts_windows:prune_overflowing(Test1, 2),
    ?assertEqual(Windows2, Test2),

    Test3 = ts_windows:prune_overflowing(Test2, 1),
    ?assertEqual(undefined, ts_windows:get(Timestamp1, Test3)),
    ?assertEqual(?WINDOW(1, Timestamp2), ts_windows:get(Timestamp2, Test3)),

    Test4 = ts_windows:prune_overflowing(Test3, 1),
    ?assertEqual(Test3, Test4),

    Test5 = ts_windows:prune_overflowing(Test4, 0),
    ?assertEqual(ts_windows:init(), Test5).


get_test() ->
    MeasurementsCount = 10,
    MeasurementsToAdd = lists:map(fun(I) -> {I, I - 20} end, lists:seq(1, MeasurementsCount)),
    Windows = lists:foldl(fun({Timestamp, Value}, Acc) ->
        ts_windows:insert(Acc, Timestamp, {Timestamp, Value}, {aggregate_measurement, ?EXEMPLARY_AGGREGATOR})
    end, ts_windows:init(), MeasurementsToAdd),
    ReversedMeasurements = lists:reverse(MeasurementsToAdd),

    ?assertEqual(?LIST_ALL_RESULT(ReversedMeasurements), ?LIST_ALL(Windows)),
    ?assertEqual(?LIST_ALL_RESULT(ReversedMeasurements), ?LIST(Windows, MeasurementsCount, #{})),

    ?assertEqual(?LIST_ALL_RESULT(lists:sublist(ReversedMeasurements, 3, MeasurementsCount - 2)),
        ?LIST(Windows, MeasurementsCount - 2, #{})),

    ?assertEqual({ok, []}, ?LIST(Windows, MeasurementsCount - 2, #{window_limit => 0})),
    ?assertEqual(?LIST_CONTINUE_RESULT(lists:sublist(ReversedMeasurements, 3, MeasurementsCount - 2), #{window_limit => 1}),
        ?LIST(Windows, MeasurementsCount - 2, #{window_limit => MeasurementsCount - 1})),
    ?assertEqual({ok, ?WINDOWS_INFO_LIST(lists:sublist(ReversedMeasurements, 3, 5))},
        ?LIST(Windows, MeasurementsCount - 2, #{window_limit => 5})),
    ?assertEqual({ok, ?WINDOWS_INFO_LIST(lists:sublist(ReversedMeasurements, 3, 8))},
        ?LIST(Windows, MeasurementsCount - 2, #{window_limit => 8})),

    ?assertEqual({ok, []}, ?LIST(Windows, MeasurementsCount - 2, #{stop_timestamp => MeasurementsCount - 1})),
    ?assertEqual({ok, ?WINDOWS_INFO_LIST(lists:sublist(ReversedMeasurements, 3, 1))},
        ?LIST(Windows, MeasurementsCount - 2, #{stop_timestamp => MeasurementsCount - 2})),
    ?assertEqual(
        ?LIST_CONTINUE_RESULT(
            lists:sublist(ReversedMeasurements, 3, MeasurementsCount - 2),
            #{stop_timestamp => 0, window_limit => 992}
        ),
        ?LIST(Windows, MeasurementsCount - 2, #{stop_timestamp => 0})),
    ?assertEqual({ok, ?WINDOWS_INFO_LIST(lists:sublist(ReversedMeasurements, 3, MeasurementsCount - 2))},
        ?LIST(Windows, MeasurementsCount - 2, #{stop_timestamp => 1})),
    ?assertEqual({ok, ?WINDOWS_INFO_LIST(lists:sublist(ReversedMeasurements, 3, 4))},
        ?LIST(Windows, MeasurementsCount - 2, #{stop_timestamp => 5})),

    ?assertEqual({ok, ?WINDOWS_INFO_LIST(lists:sublist(ReversedMeasurements, 3, 4))},
        ?LIST(Windows, MeasurementsCount - 2, #{window_limit => 5, stop_timestamp => 5})),
    ?assertEqual({ok, ?WINDOWS_INFO_LIST(lists:sublist(ReversedMeasurements, 3, 5))},
        ?LIST(Windows, MeasurementsCount - 2, #{window_limit => 5, stop_timestamp => 1})),
    ?assertEqual(
        ?LIST_CONTINUE_RESULT(
            lists:sublist(ReversedMeasurements, 3, MeasurementsCount - 2),
            #{window_limit => 2, stop_timestamp => 0}
        ),
        ?LIST(Windows, MeasurementsCount - 2, #{window_limit => MeasurementsCount, stop_timestamp => 0})).


split_test() ->
    MeasurementsCount = 10,
    MeasurementsToAdd = lists:map(fun(I) -> {I, I - 20} end, lists:seq(1, MeasurementsCount)),
    Windows = lists:foldl(fun({Timestamp, Value}, Acc) ->
        ts_windows:insert(Acc, Timestamp, {Timestamp, Value}, {aggregate_measurement, ?EXEMPLARY_AGGREGATOR})
    end, ts_windows:init(), MeasurementsToAdd),
    ReversedMeasurements = lists:reverse(MeasurementsToAdd),

    SplitAns = ts_windows:split(Windows, 3),
    ?assertMatch({_, _, 7}, SplitAns),
    {Windows1, Windows2, _} = SplitAns,
    ?assertEqual(?LIST_ALL_RESULT(lists:sublist(ReversedMeasurements, 3)), ?LIST_ALL(Windows1)),
    ?assertEqual(?LIST_ALL_RESULT(lists:sublist(ReversedMeasurements, 4, MeasurementsCount - 3)), ?LIST_ALL(Windows2)).


reorganization_test() ->
    MeasurementsCount = 10,

    MeasurementsToAdd1 = lists:map(fun(I) -> {I, I - 20} end, lists:seq(1, MeasurementsCount)),
    Windows1 = lists:foldl(fun({Timestamp, Value}, Acc) ->
        ts_windows:insert(Acc, Timestamp, {Timestamp, Value}, {aggregate_measurement, ?EXEMPLARY_AGGREGATOR})
    end, ts_windows:init(), MeasurementsToAdd1),
    ReversedMeasurements1 = lists:reverse(MeasurementsToAdd1),

    MeasurementsToAdd2 = lists:map(fun(I) -> {I, I - 120} end, lists:seq(21, 20 + MeasurementsCount)),
    Windows2 = lists:foldl(fun({Timestamp, Value}, Acc) ->
        ts_windows:insert(Acc, Timestamp, {Timestamp, Value}, {aggregate_measurement, ?EXEMPLARY_AGGREGATOR})
    end, ts_windows:init(), MeasurementsToAdd2),
    ReversedMeasurements2 = lists:reverse(MeasurementsToAdd2),

    ?assertEqual(true, ts_windows:is_size_exceeded(Windows1, MeasurementsCount - 1)),
    ?assertEqual(false, ts_windows:is_size_exceeded(Windows1, MeasurementsCount)),
    ?assertEqual(false, ts_windows:is_size_exceeded(Windows1, MeasurementsCount + 1)),

    Test1 = ts_windows:reorganize(Windows1, Windows2, 10, 1),
    ?assertMatch([{split_current_data_node, {_, _, 29}}], Test1),
    [{_, {Test1Split1, Test1Split2, _}}] = Test1,
    ?assertEqual(?LIST_ALL_RESULT(lists:sublist(ReversedMeasurements2, 1)), ?LIST_ALL(Test1Split1)),
    ?assertEqual(?LIST_ALL_RESULT(lists:sublist(ReversedMeasurements2, 2, MeasurementsCount - 1)), ?LIST_ALL(Test1Split2)),

    Test2 = ts_windows:reorganize(Windows1, Windows2, 20, 1),
    ?assertMatch([{update_previous_data_node, _}, {update_current_data_node, 30, _}], Test2),
    [{_, Test2Windows1}, {_, _, Test2Windows2}] = Test2,
    ?assertEqual(?LIST_ALL_RESULT(ReversedMeasurements2 ++ ReversedMeasurements1), ?LIST_ALL(Test2Windows1)),
    ?assertEqual(ts_windows:init(), Test2Windows2),

    Test3 = ts_windows:reorganize(Windows1, Windows2, 13, 1),
    ?assertMatch([{update_previous_data_node, _}, {update_current_data_node, 23, _}], Test3),
    [{_, Test3Windows1}, {_, _, Test3Windows2}] = Test3,
    ?assertEqual(?LIST_ALL_RESULT(lists:sublist(ReversedMeasurements2, 8, 3) ++ ReversedMeasurements1),
        ?LIST_ALL(Test3Windows1)),
    ?assertEqual(?LIST_ALL_RESULT(lists:sublist(ReversedMeasurements2, 7)), ?LIST_ALL(Test3Windows2)).

-endif.