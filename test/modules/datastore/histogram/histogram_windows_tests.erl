%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Eunit tests for the histogram_windows module.
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_windows_tests).
-author("Michal Wrzeszcz").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/hashing/consistent_hashing.hrl").

-define(GET_ALL(Windows), histogram_windows:get(Windows, undefined, #{})).
-define(GET_ALL_RESULT(List), {{continue, #{}}, List}).

%%%===================================================================
%%% Tests
%%%===================================================================

apply_new_point_test() ->
    Windows = histogram_windows:init(),
    Timestamp = 1,

    ?assertEqual(undefined, histogram_windows:get_value(Timestamp, Windows)),

    Test1 = histogram_windows:apply_value(Windows, Timestamp, 1, sum),
    ?assertEqual({1, 1}, histogram_windows:get_value(Timestamp, Test1)),

    Test2 = histogram_windows:apply_value(Windows, Timestamp, 1, max),
    ?assertEqual(1, histogram_windows:get_value(Timestamp, Test2)),

    Test3 = histogram_windows:apply_value(Windows, Timestamp, 1, min),
    ?assertEqual(1, histogram_windows:get_value(Timestamp, Test3)),

    Test4 = histogram_windows:apply_value(Windows, Timestamp, 1, last),
    ?assertEqual(1, histogram_windows:get_value(Timestamp, Test4)),

    Test5 = histogram_windows:apply_value(Windows, Timestamp, 1, first),
    ?assertEqual(1, histogram_windows:get_value(Timestamp, Test5)).


apply_with_existing_timestamp_test() ->
    Timestamp = 1,
    Windows = histogram_windows:apply_value(histogram_windows:init(), Timestamp, 2, max),

    Test1 = histogram_windows:apply_value(Windows, Timestamp, 3, max),
    ?assertEqual(3, histogram_windows:get_value(Timestamp, Test1)),
    Test2 = histogram_windows:apply_value(Windows, Timestamp, 1, max),
    ?assertEqual(2, histogram_windows:get_value(Timestamp, Test2)),

    Test3 = histogram_windows:apply_value(Windows, Timestamp, 1, min),
    ?assertEqual(1, histogram_windows:get_value(Timestamp, Test3)),
    Test4 = histogram_windows:apply_value(Windows, Timestamp, 3, min),
    ?assertEqual(2, histogram_windows:get_value(Timestamp, Test4)),

    Test5 = histogram_windows:apply_value(Windows, Timestamp, 3, last),
    ?assertEqual(3, histogram_windows:get_value(Timestamp, Test5)),

    Test6 = histogram_windows:apply_value(Windows, Timestamp, 3, first),
    ?assertEqual(2, histogram_windows:get_value(Timestamp, Test6)),

    WindowsWithSum = histogram_windows:apply_value(histogram_windows:init(), Timestamp, 2, sum),
    Test7 = histogram_windows:apply_value(WindowsWithSum, Timestamp, 3, sum),
    ?assertEqual({2, 5}, histogram_windows:get_value(Timestamp, Test7)).


maybe_delete_last_test() ->
    Timestamp1 = 1,
    Timestamp2 = 2,
    Windows1 = histogram_windows:apply_value(histogram_windows:init(), Timestamp1, 1, max),
    Windows2 = histogram_windows:apply_value(Windows1, Timestamp2, 1, max),

    Test1 = histogram_windows:maybe_delete_last(Windows2, 3),
    ?assertEqual(Windows2, Test1),

    Test2 = histogram_windows:maybe_delete_last(Test1, 2),
    ?assertEqual(Windows2, Test2),

    Test3 = histogram_windows:maybe_delete_last(Test2, 1),
    ?assertEqual(undefined, histogram_windows:get_value(Timestamp1, Test3)),
    ?assertEqual(1, histogram_windows:get_value(Timestamp2, Test3)),

    Test4 = histogram_windows:maybe_delete_last(Test3, 1),
    ?assertEqual(Test3, Test4),

    Test5 = histogram_windows:maybe_delete_last(Test4, 0),
    ?assertEqual(histogram_windows:init(), Test5).


get_test() ->
    PointsCount = 10,
    PointsToAdd = lists:map(fun(I) -> {I, I - 20} end, lists:seq(1, PointsCount)),
    Windows = lists:foldl(fun({Timestamp, Value}, Acc) ->
        histogram_windows:apply_value(Acc, Timestamp, Value, max)
    end, histogram_windows:init(), PointsToAdd),
    Points = lists:reverse(PointsToAdd),

    ?assertEqual(?GET_ALL_RESULT(Points), ?GET_ALL(Windows)),
    ?assertEqual(?GET_ALL_RESULT(Points), histogram_windows:get(Windows, PointsCount, #{})),

    ?assertEqual(?GET_ALL_RESULT(lists:sublist(Points, 3, PointsCount - 2)),
        histogram_windows:get(Windows, PointsCount - 2, #{})),

    ?assertEqual({ok, []}, histogram_windows:get(Windows, PointsCount - 2, #{limit => 0})),
    ?assertEqual({{continue, #{limit => 1}}, lists:sublist(Points, 3, PointsCount - 2)},
        histogram_windows:get(Windows, PointsCount - 2, #{limit => PointsCount - 1})),
    ?assertEqual({ok, lists:sublist(Points, 3, 5)},
        histogram_windows:get(Windows, PointsCount - 2, #{limit => 5})),
    ?assertEqual({ok, lists:sublist(Points, 3, 8)},
        histogram_windows:get(Windows, PointsCount - 2, #{limit => 8})),

    ?assertEqual({ok, []}, histogram_windows:get(Windows, PointsCount - 2, #{stop => PointsCount - 1})),
    ?assertEqual({ok, lists:sublist(Points, 3, 1)},
        histogram_windows:get(Windows, PointsCount - 2, #{stop => PointsCount - 2})),
    ?assertEqual({{continue, #{stop => 0}}, lists:sublist(Points, 3, PointsCount - 2)},
        histogram_windows:get(Windows, PointsCount - 2, #{stop => 0})),
    ?assertEqual({ok, lists:sublist(Points, 3, PointsCount - 2)},
        histogram_windows:get(Windows, PointsCount - 2, #{stop => 1})),
    ?assertEqual({ok, lists:sublist(Points, 3, 4)},
        histogram_windows:get(Windows, PointsCount - 2, #{stop => 5})),

    ?assertEqual({ok, lists:sublist(Points, 3, 4)},
        histogram_windows:get(Windows, PointsCount - 2, #{limit => 5, stop => 5})),
    ?assertEqual({ok, lists:sublist(Points, 3, 5)},
        histogram_windows:get(Windows, PointsCount - 2, #{limit => 5, stop => 1})),
    ?assertEqual({{continue, #{limit => 2, stop => 0}}, lists:sublist(Points, 3, PointsCount - 2)},
        histogram_windows:get(Windows, PointsCount - 2, #{limit => PointsCount, stop => 0})).


split_test() ->
    PointsCount = 10,
    PointsToAdd = lists:map(fun(I) -> {I, I - 20} end, lists:seq(1, PointsCount)),
    Windows = lists:foldl(fun({Timestamp, Value}, Acc) ->
        histogram_windows:apply_value(Acc, Timestamp, Value, max)
    end, histogram_windows:init(), PointsToAdd),
    Points = lists:reverse(PointsToAdd),

    SplitAns = histogram_windows:split_windows(Windows, 3),
    ?assertMatch({_, _, 7}, SplitAns),
    {Windows1, Windows2, _} = SplitAns,
    ?assertEqual(?GET_ALL_RESULT(lists:sublist(Points, 3)), ?GET_ALL(Windows1)),
    ?assertEqual(?GET_ALL_RESULT(lists:sublist(Points, 4, PointsCount - 3)), ?GET_ALL(Windows2)).


reorganization_test() ->
    PointsCount = 10,

    PointsToAdd1 = lists:map(fun(I) -> {I, I - 20} end, lists:seq(1, PointsCount)),
    Windows1 = lists:foldl(fun({Timestamp, Value}, Acc) ->
        histogram_windows:apply_value(Acc, Timestamp, Value, max)
    end, histogram_windows:init(), PointsToAdd1),
    Points1 = lists:reverse(PointsToAdd1),

    PointsToAdd2 = lists:map(fun(I) -> {I, I - 120} end, lists:seq(21, 20 + PointsCount)),
    Windows2 = lists:foldl(fun({Timestamp, Value}, Acc) ->
        histogram_windows:apply_value(Acc, Timestamp, Value, max)
    end, histogram_windows:init(), PointsToAdd2),
    Points2 = lists:reverse(PointsToAdd2),

    ?assertEqual(true, histogram_windows:should_reorganize_windows(Windows1, PointsCount - 1)),
    ?assertEqual(false, histogram_windows:should_reorganize_windows(Windows1, PointsCount)),
    ?assertEqual(false, histogram_windows:should_reorganize_windows(Windows1, PointsCount + 1)),

    Test1 = histogram_windows:reorganize_windows(Windows1, Windows2, 10),
    ?assertMatch([{split_current_record, {_, _, 29}}], Test1),
    [{_, {Test1Split1, Test1Split2, _}}] = Test1,
    ?assertEqual(?GET_ALL_RESULT(lists:sublist(Points2, 1)), ?GET_ALL(Test1Split1)),
    ?assertEqual(?GET_ALL_RESULT(lists:sublist(Points2, 2, PointsCount - 1)), ?GET_ALL(Test1Split2)),

    Test2 = histogram_windows:reorganize_windows(Windows1, Windows2, 20),
    ?assertMatch([{update_previos_record, _}, {update_current_record, 30, _}], Test2),
    [{_, Test2Windows1}, {_, _, Test2Windows2}] = Test2,
    ?assertEqual(?GET_ALL_RESULT(Points2 ++ Points1), ?GET_ALL(Test2Windows1)),
    ?assertEqual(histogram_windows:init(), Test2Windows2),

    Test3 = histogram_windows:reorganize_windows(Windows1, Windows2, 13),
    ?assertMatch([{update_previos_record, _}, {update_current_record, 23, _}], Test3),
    [{_, Test3Windows1}, {_, _, Test3Windows2}] = Test3,
    ?assertEqual(?GET_ALL_RESULT(lists:sublist(Points2, 8, 3) ++ Points1), ?GET_ALL(Test3Windows1)),
    ?assertEqual(?GET_ALL_RESULT(lists:sublist(Points2, 7)), ?GET_ALL(Test3Windows2)).

-endif.