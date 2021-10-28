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

-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/hashing/consistent_hashing.hrl").

-define(LIST_ALL(Windows), ts_windows:list(Windows, undefined, #{})).
-define(LIST_ALL_RESULT(List), {{continue, #{}}, List}).

%%%===================================================================
%%% Tests
%%%===================================================================

add_new_measurement_test() ->
    Windows = ts_windows:init(),
    Timestamp = 1,

    ?assertEqual(undefined, ts_windows:get_value(Timestamp, Windows)),

    Test1 = ts_windows:insert_value(Windows, Timestamp, 1, {aggregate, sum}),
    ?assertEqual({1, 1}, ts_windows:get_value(Timestamp, Test1)),

    Test2 = ts_windows:insert_value(Windows, Timestamp, 1, {aggregate, max}),
    ?assertEqual(1, ts_windows:get_value(Timestamp, Test2)),

    Test3 = ts_windows:insert_value(Windows, Timestamp, 1, {aggregate, min}),
    ?assertEqual(1, ts_windows:get_value(Timestamp, Test3)),

    Test4 = ts_windows:insert_value(Windows, Timestamp, 1, {aggregate, last}),
    ?assertEqual(1, ts_windows:get_value(Timestamp, Test4)),

    Test5 = ts_windows:insert_value(Windows, Timestamp, 1, {aggregate, first}),
    ?assertEqual(1, ts_windows:get_value(Timestamp, Test5)).


add_with_existing_timestamp_test() ->
    Timestamp = 1,
    Windows = ts_windows:insert_value(ts_windows:init(), Timestamp, 2, {aggregate, max}),

    Test1 = ts_windows:insert_value(Windows, Timestamp, 3, {aggregate, max}),
    ?assertEqual(3, ts_windows:get_value(Timestamp, Test1)),
    Test2 = ts_windows:insert_value(Windows, Timestamp, 1, {aggregate, max}),
    ?assertEqual(2, ts_windows:get_value(Timestamp, Test2)),

    Test3 = ts_windows:insert_value(Windows, Timestamp, 1, {aggregate, min}),
    ?assertEqual(1, ts_windows:get_value(Timestamp, Test3)),
    Test4 = ts_windows:insert_value(Windows, Timestamp, 3, {aggregate, min}),
    ?assertEqual(2, ts_windows:get_value(Timestamp, Test4)),

    Test5 = ts_windows:insert_value(Windows, Timestamp, 3, {aggregate, last}),
    ?assertEqual(3, ts_windows:get_value(Timestamp, Test5)),

    Test6 = ts_windows:insert_value(Windows, Timestamp, 3, {aggregate, first}),
    ?assertEqual(2, ts_windows:get_value(Timestamp, Test6)),

    WindowsWithSum = ts_windows:insert_value(ts_windows:init(), Timestamp, 2, {aggregate, sum}),
    Test7 = ts_windows:insert_value(WindowsWithSum, Timestamp, 3, {aggregate, sum}),
    ?assertEqual({2, 5}, ts_windows:get_value(Timestamp, Test7)).


prune_overflowing_windows_test() ->
    Timestamp1 = 1,
    Timestamp2 = 2,
    Windows1 = ts_windows:insert_value(ts_windows:init(), Timestamp1, 1, {aggregate, max}),
    Windows2 = ts_windows:insert_value(Windows1, Timestamp2, 1, {aggregate, max}),

    Test1 = ts_windows:prune_overflowing(Windows2, 3),
    ?assertEqual(Windows2, Test1),

    Test2 = ts_windows:prune_overflowing(Test1, 2),
    ?assertEqual(Windows2, Test2),

    Test3 = ts_windows:prune_overflowing(Test2, 1),
    ?assertEqual(undefined, ts_windows:get_value(Timestamp1, Test3)),
    ?assertEqual(1, ts_windows:get_value(Timestamp2, Test3)),

    Test4 = ts_windows:prune_overflowing(Test3, 1),
    ?assertEqual(Test3, Test4),

    Test5 = ts_windows:prune_overflowing(Test4, 0),
    ?assertEqual(ts_windows:init(), Test5).


get_test() ->
    MeasurementsCount = 10,
    MeasurementsToAdd = lists:map(fun(I) -> {I, I - 20} end, lists:seq(1, MeasurementsCount)),
    Windows = lists:foldl(fun({Timestamp, Value}, Acc) ->
        ts_windows:insert_value(Acc, Timestamp, Value, {aggregate, max})
    end, ts_windows:init(), MeasurementsToAdd),
    ReversedMeasurements = lists:reverse(MeasurementsToAdd),

    ?assertEqual(?LIST_ALL_RESULT(ReversedMeasurements), ?LIST_ALL(Windows)),
    ?assertEqual(?LIST_ALL_RESULT(ReversedMeasurements), ts_windows:list(Windows, MeasurementsCount, #{})),

    ?assertEqual(?LIST_ALL_RESULT(lists:sublist(ReversedMeasurements, 3, MeasurementsCount - 2)),
        ts_windows:list(Windows, MeasurementsCount - 2, #{})),

    ?assertEqual({ok, []}, ts_windows:list(Windows, MeasurementsCount - 2, #{limit => 0})),
    ?assertEqual({{continue, #{limit => 1}}, lists:sublist(ReversedMeasurements, 3, MeasurementsCount - 2)},
        ts_windows:list(Windows, MeasurementsCount - 2, #{limit => MeasurementsCount - 1})),
    ?assertEqual({ok, lists:sublist(ReversedMeasurements, 3, 5)},
        ts_windows:list(Windows, MeasurementsCount - 2, #{limit => 5})),
    ?assertEqual({ok, lists:sublist(ReversedMeasurements, 3, 8)},
        ts_windows:list(Windows, MeasurementsCount - 2, #{limit => 8})),

    ?assertEqual({ok, []}, ts_windows:list(Windows, MeasurementsCount - 2, #{stop => MeasurementsCount - 1})),
    ?assertEqual({ok, lists:sublist(ReversedMeasurements, 3, 1)},
        ts_windows:list(Windows, MeasurementsCount - 2, #{stop => MeasurementsCount - 2})),
    ?assertEqual({{continue, #{stop => 0}}, lists:sublist(ReversedMeasurements, 3, MeasurementsCount - 2)},
        ts_windows:list(Windows, MeasurementsCount - 2, #{stop => 0})),
    ?assertEqual({ok, lists:sublist(ReversedMeasurements, 3, MeasurementsCount - 2)},
        ts_windows:list(Windows, MeasurementsCount - 2, #{stop => 1})),
    ?assertEqual({ok, lists:sublist(ReversedMeasurements, 3, 4)},
        ts_windows:list(Windows, MeasurementsCount - 2, #{stop => 5})),

    ?assertEqual({ok, lists:sublist(ReversedMeasurements, 3, 4)},
        ts_windows:list(Windows, MeasurementsCount - 2, #{limit => 5, stop => 5})),
    ?assertEqual({ok, lists:sublist(ReversedMeasurements, 3, 5)},
        ts_windows:list(Windows, MeasurementsCount - 2, #{limit => 5, stop => 1})),
    ?assertEqual({{continue, #{limit => 2, stop => 0}}, lists:sublist(ReversedMeasurements, 3, MeasurementsCount - 2)},
        ts_windows:list(Windows, MeasurementsCount - 2, #{limit => MeasurementsCount, stop => 0})).


split_test() ->
    MeasurementsCount = 10,
    MeasurementsToAdd = lists:map(fun(I) -> {I, I - 20} end, lists:seq(1, MeasurementsCount)),
    Windows = lists:foldl(fun({Timestamp, Value}, Acc) ->
        ts_windows:insert_value(Acc, Timestamp, Value, {aggregate, max})
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
        ts_windows:insert_value(Acc, Timestamp, Value, {aggregate, max})
    end, ts_windows:init(), MeasurementsToAdd1),
    ReversedMeasurements1 = lists:reverse(MeasurementsToAdd1),

    MeasurementsToAdd2 = lists:map(fun(I) -> {I, I - 120} end, lists:seq(21, 20 + MeasurementsCount)),
    Windows2 = lists:foldl(fun({Timestamp, Value}, Acc) ->
        ts_windows:insert_value(Acc, Timestamp, Value, {aggregate, max})
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