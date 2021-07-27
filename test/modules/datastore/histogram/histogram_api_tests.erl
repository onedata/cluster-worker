%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Eunit tests for the histogram_api module.
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_api_tests).
-author("Michal Wrzeszcz").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include("modules/datastore/histogram.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("global_definitions.hrl").

%%%===================================================================
%%% Setup and teardown
%%%===================================================================

histogram_api_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            fun single_metrics_single_node/0,
            {timeout, 300, fun single_metrics_multiple_nodes/0},
            fun single_time_series_single_node/0,
            {timeout, 300, fun single_time_series_multiple_nodes/0},
            fun multiple_time_series_single_node/0,
            {timeout, 300, fun multiple_time_series_multiple_nodes/0}
        ]
    }.

setup() ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, histogram_max_doc_size, 2000),
    meck:new([datastore_doc_batch, datastore_doc], [passthrough, no_history]),
    meck:expect(datastore_doc_batch, init, fun() -> #{} end),
    meck:expect(datastore_doc, save, fun(_Ctx, Key, Doc, Batch) -> {{ok, Doc}, Batch#{Key => Doc}} end),
    meck:expect(datastore_doc, fetch, fun(_Ctx, Key, Batch) ->
        {{ok, maps:get(Key, Batch, {error, not_found})}, Batch}
    end),
    meck:expect(datastore_doc, delete, fun(_Ctx, Key, Batch) -> {ok, maps:remove(Key, Batch)} end).

teardown(_) ->
    meck:unload([datastore_doc_batch, datastore_doc]).

%%%===================================================================
%%% Tests
%%%===================================================================

single_metrics_single_node() ->
    Ctx = #{},
    Id = datastore_key:new(),
    TimeSeriesId = <<"TS1">>,
    MetricsId = <<"M1">>,
    MetricsConfig = #histogram_config{window_size = 10, max_windows_count = 5, apply_function = sum},
    ConfigMap = #{TimeSeriesId => #{MetricsId => MetricsConfig}},
    Batch = init_histogram(Ctx, Id, ConfigMap),

    Points = lists:map(fun(I) -> {I, I/2} end, lists:seq(10, 49) ++ lists:seq(60, 69)),
    Batch2 = update_many(Ctx, Id, Points, Batch),

    ExpectedGetAns = lists:reverse(lists:map(fun(N) -> {N, {10, 5 * N + 22.5}} end, lists:seq(10, 40, 10) ++ [60])),
    ?assertMatch({{ok, ExpectedGetAns}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch2)),
    ?assertMatch({{ok, ExpectedGetAns}, _},
        histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{start => 1000}, Batch2)),
    ?assertMatch({{ok, []}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{start => 1}, Batch2)),

    ExpectedGetAns2 = lists:sublist(ExpectedGetAns, 2),
    ?assertMatch({{ok, ExpectedGetAns2}, _},
        histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{limit => 2}, Batch2)),
    ?assertMatch({{ok, ExpectedGetAns2}, _},
        histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{stop => 35}, Batch2)),

    ExpectedGetAns3 = lists:sublist(ExpectedGetAns, 2, 2),
    ?assertMatch({{ok, ExpectedGetAns3}, _},
        histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{start => 45, limit => 2}, Batch2)),
    GetAns = histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{start => 45, stop => 25}, Batch2),
    ?assertMatch({{ok, ExpectedGetAns3}, _}, GetAns),
    {_, Batch3} = GetAns,

    Batch4 = update(Ctx, Id, 100, 5, Batch3),
    ExpectedGetAns4 = [{100, {1, 5}} | lists:sublist(ExpectedGetAns, 4)],
    GetAns2 = histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch4),
    ?assertMatch({{ok, ExpectedGetAns4}, _}, GetAns2),

    Batch5 = update(Ctx, Id, 1, 5, Batch4),
    GetAns3 = histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch5),
    ?assertEqual(GetAns2, GetAns3),

    Batch6 = update(Ctx, Id, 53, 5, Batch5),
    ExpectedGetAns5 = [{100, {1, 5}}] ++ lists:sublist(ExpectedGetAns, 1) ++
        [{50, {1, 5}}] ++ lists:sublist(ExpectedGetAns, 2, 2),
    ?assertMatch({{ok, ExpectedGetAns5}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch6)).

single_metrics_multiple_nodes() ->
    Ctx = #{},
    Id = datastore_key:new(),
    TimeSeriesId = <<"TS1">>,
    MetricsId = <<"M1">>,
    MaxWindowsCount = 8000,
    MetricsConfig = #histogram_config{window_size = 1, max_windows_count = MaxWindowsCount, apply_function = max},
    ConfigMap = #{TimeSeriesId => #{MetricsId => MetricsConfig}},
    Batch = init_histogram(Ctx, Id, ConfigMap),

    Points = lists:map(fun(I) -> {2 * I, 4 * I} end, lists:seq(1, MaxWindowsCount)),

%%    Start = os:timestamp(),
    Batch2 = update_many(Ctx, Id, Points, Batch),
%%    T = timer:now_diff(os:timestamp(), Start),
%%    io:format("xxxxx ~p", [T]),

    ExpectedGetAns = lists:reverse(Points),
    ExpectedMap = #{{TimeSeriesId, MetricsId} => ExpectedGetAns},
    ?assertMatch({{ok, ExpectedGetAns}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch2)),
    ?assertMatch({{ok, ExpectedMap}, _}, histogram_api:get(Ctx, Id, [{TimeSeriesId, MetricsId}], #{}, Batch2)),

    ?assertEqual(4, maps:size(Batch2)),
    DocsNums = lists:foldl(fun
        (#document{value = {histogram_tail_node, {data, Windows, _, _}}}, {HeadsCountAcc, TailsCountAcc}) ->
            ?assertEqual(2000, histogram_windows:get_size(Windows)),
            {HeadsCountAcc, TailsCountAcc + 1};
        (#document{value = {histogram_hub, TimeSeries}}, {HeadsCountAcc, TailsCountAcc}) ->
            [Metrics] = maps:values(TimeSeries),
            [#metrics{data = {data, Windows, _, _}}] = maps:values(Metrics),
            ?assertEqual(2000, histogram_windows:get_size(Windows)),
            {HeadsCountAcc + 1, TailsCountAcc}
    end, {0, 0}, maps:values(Batch2)),
    ?assertEqual({1, 3}, DocsNums),

    [NewPoint1, NewPoint2 | Points2Tail] = Points2 =
        lists:map(fun(I) -> {2 * I, 4 * I} end, lists:seq(MaxWindowsCount + 1, 10000)),
    Batch3 = update_many(Ctx, Id, [NewPoint2, NewPoint1], Batch2),
    ExpectedGetAns2 = [NewPoint2, NewPoint1 | lists:sublist(ExpectedGetAns, 6000)],
    ?assertMatch({{ok, ExpectedGetAns2}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch3)),

    Batch4 = update_many(Ctx, Id, Points2Tail, Batch3),
    ExpectedGetAns3 = lists:sublist(lists:reverse(Points ++ Points2), 8000),
    ?assertMatch({{ok, ExpectedGetAns3}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch4)),

    Batch5 = update(Ctx, Id, 1, 0, Batch4),
    ?assertMatch({{ok, ExpectedGetAns3}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch5)),

    Batch6 = update(Ctx, Id, 4003, 0, Batch5),
    ExpectedGetAns4 = lists:sublist(ExpectedGetAns3, 6001),
    ?assertMatch({{ok, ExpectedGetAns4}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch6)),

    Batch7 = update(Ctx, Id, 12005, 0, Batch6),
    ExpectedGetAns5 = lists:reverse(lists:sort([{12005, 0} | lists:sublist(ExpectedGetAns3, 4001)])),
%%    ?assertMatch({{ok, ExpectedGetAns5}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch7)),

    % TODO - test dodania kiedy dokument do ktorego dodajemy jest pelny, a nastepny ma wolne sloty
    ok.

single_time_series_single_node() ->
    Ctx = #{},
    Id = datastore_key:new(),
    TimeSeriesId = <<"TS1">>,
    MetricsConfigs = lists:foldl(fun(N, Acc) ->
        MetricsConfig = #histogram_config{window_size = N, max_windows_count = 600 div N + 10, apply_function = sum},
        Acc#{<<"M", N>> => MetricsConfig}
    end, #{}, lists:seq(1, 5)),
    ConfigMap = #{TimeSeriesId => MetricsConfigs},
    Batch = init_histogram(Ctx, Id, ConfigMap),

    PointsCount = 1199,
    Points = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(0, PointsCount)),
    Batch2 = update_many(Ctx, Id, Points, Batch),

    ExpectedMap = maps:map(fun(_MetricsId, #histogram_config{window_size = WindowSize, max_windows_count = MaxWindowsCount}) ->
        lists:sublist(
            lists:reverse(
                lists:map(fun(N) ->
                    {N, {WindowSize, (N + N + WindowSize - 1) * WindowSize}}
                end, lists:seq(0, PointsCount, WindowSize))
            ), MaxWindowsCount)
    end, MetricsConfigs),

    lists:foldl(fun({MetricsId, Expected}, GetMetricsAcc) ->
        ?assertMatch({{ok, Expected}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch2)),

        UpdatedGetMetrics = [MetricsId | GetMetricsAcc],
        ExpectedAcc = maps:from_list(lists:map(fun(MId) ->
            {{TimeSeriesId, MId}, maps:get(MId, ExpectedMap)}
        end, UpdatedGetMetrics)),
        ExpectedSingleValue = maps:get(MetricsId, ExpectedMap),
        ExpectedSingleValueMap = #{{TimeSeriesId, MetricsId} => ExpectedSingleValue},

        ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, UpdatedGetMetrics}, #{}, Batch2)),
        ?assertMatch({{ok, ExpectedSingleValue}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch2)),
        ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, {[TimeSeriesId], UpdatedGetMetrics}, #{}, Batch2)),
        ?assertMatch({{ok, ExpectedSingleValueMap}, _}, histogram_api:get(Ctx, Id, {[TimeSeriesId], MetricsId}, #{}, Batch2)),
        ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, [{TimeSeriesId, UpdatedGetMetrics}], #{}, Batch2)),
        ?assertMatch({{ok, ExpectedSingleValueMap}, _}, histogram_api:get(Ctx, Id, [{TimeSeriesId, MetricsId}], #{}, Batch2)),
        ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, [{[TimeSeriesId], UpdatedGetMetrics}], #{}, Batch2)),
        ?assertMatch({{ok, ExpectedSingleValueMap}, _}, histogram_api:get(Ctx, Id, [{[TimeSeriesId], MetricsId}], #{}, Batch2)),
        MappedUpdatedGetMetrics = lists:map(fun(MId) -> {TimeSeriesId, MId} end, UpdatedGetMetrics),
        ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, MappedUpdatedGetMetrics, #{}, Batch2)),

        UpdatedGetMetrics
    end, [], maps:to_list(ExpectedMap)).
% TODO - dorzucic nieistniejaca metryke do zapytania kazdego typu zapytania

single_time_series_multiple_nodes() ->
    Ctx = #{},
    Id = datastore_key:new(),
    TimeSeriesId = <<"TS1">>,
    MetricsConfigs = lists:foldl(fun(N, Acc) ->
        MetricsConfig = #histogram_config{window_size = 1, max_windows_count = 2000 * N, apply_function = min},
        Acc#{<<"M", N>> => MetricsConfig}
    end, #{}, lists:seq(1, 4)),
    ConfigMap = #{TimeSeriesId => MetricsConfigs},
    Batch = init_histogram(Ctx, Id, ConfigMap),

    PointsCount = 8000,
    Points = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, PointsCount)),
    Batch2 = update_many(Ctx, Id, Points, Batch),

    ?assertEqual(7, maps:size(Batch2)),
    DocsNums = lists:foldl(fun
        (#document{value = {histogram_tail_node, {data, Windows, _, _}}}, {HeadsCountAcc, TailsCountAcc}) ->
            ?assertEqual(2000, histogram_windows:get_size(Windows)),
            {HeadsCountAcc, TailsCountAcc + 1};
        (#document{value = {histogram_hub, TimeSeries}}, {HeadsCountAcc, TailsCountAcc}) ->
            [MetricsMap] = maps:values(TimeSeries),
            ?assertEqual(4, maps:size(MetricsMap)),
            lists:foreach(fun(#metrics{data = {data, Windows, _, _}}) ->
                ?assertEqual(2000, histogram_windows:get_size(Windows))
            end, maps:values(MetricsMap)),
            {HeadsCountAcc + 1, TailsCountAcc}
    end, {0, 0}, maps:values(Batch2)),
    ?assertEqual({1, 6}, DocsNums),

    ExpectedMap = maps:map(fun(_MetricsId, #histogram_config{max_windows_count = MaxWindowsCount}) ->
        lists:sublist(lists:reverse(Points), MaxWindowsCount)
    end, MetricsConfigs),

    lists:foldl(fun({MetricsId, Expected}, GetMetricsAcc) ->
        ?assertMatch({{ok, Expected}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch2)),

        UpdatedGetMetrics = [MetricsId | GetMetricsAcc],
        ExpectedAcc = maps:from_list(lists:map(fun(MId) ->
            {{TimeSeriesId, MId}, maps:get(MId, ExpectedMap)}
        end, UpdatedGetMetrics)),

        ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, UpdatedGetMetrics}, #{}, Batch2)),
        ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, {[TimeSeriesId], UpdatedGetMetrics}, #{}, Batch2)),
        ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, [{TimeSeriesId, UpdatedGetMetrics}], #{}, Batch2)),
        ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, [{[TimeSeriesId], UpdatedGetMetrics}], #{}, Batch2)),
        MappedUpdatedGetMetrics = lists:map(fun(MId) -> {TimeSeriesId, MId} end, UpdatedGetMetrics),
        ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, MappedUpdatedGetMetrics, #{}, Batch2)),

        UpdatedGetMetrics
    end, [], maps:to_list(ExpectedMap)).

multiple_time_series_single_node() ->
    Ctx = #{},
    Id = datastore_key:new(),
    ConfigMap = lists:foldl(fun(N, Acc) ->
        TimeSeries = <<"TS", (N rem 2)>>,
        MetricsMap = maps:get(TimeSeries, Acc, #{}),
        MetricsConfig = #histogram_config{window_size = N, max_windows_count = 600 div N + 10, apply_function = sum},
        Acc#{TimeSeries => MetricsMap#{<<"M", (N div 2)>> => MetricsConfig}}
    end, #{}, lists:seq(1, 5)),
    Batch = init_histogram(Ctx, Id, ConfigMap),

    PointsCount = 1199,
    Points = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(0, PointsCount)),
    Batch2 = update_many(Ctx, Id, Points, Batch),

    ExpectedMap = maps:map(fun(_TimeSeriesId, MetricsConfigs) ->
        maps:map(fun(_MetricsId, #histogram_config{window_size = WindowSize, max_windows_count = MaxWindowsCount}) ->
            lists:sublist(
                lists:reverse(
                    lists:map(fun(N) ->
                        {N, {WindowSize, (N + N + WindowSize - 1) * WindowSize}}
                    end, lists:seq(0, PointsCount, WindowSize))
                ), MaxWindowsCount)
        end, MetricsConfigs)
    end, ConfigMap),

    AllMetricsIds = lists:foldl(fun({TimeSeriesId, Metrics}, Acc) ->
        TimeSeriesExpectedMap = maps:get(TimeSeriesId, ExpectedMap),
        Acc ++ lists:foldl(fun({MetricsId, Expected}, GetMetricsAcc) ->
            ?assertMatch({{ok, Expected}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch2)),

            UpdatedGetMetrics = [MetricsId | GetMetricsAcc],
            ExpectedAcc = maps:from_list(lists:map(fun(MId) ->
                {{TimeSeriesId, MId}, maps:get(MId, TimeSeriesExpectedMap)}
            end, UpdatedGetMetrics)),

            ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, UpdatedGetMetrics}, #{}, Batch2)),
            ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, {[TimeSeriesId], UpdatedGetMetrics}, #{}, Batch2)),
            ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, [{TimeSeriesId, UpdatedGetMetrics}], #{}, Batch2)),
            ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, [{[TimeSeriesId], UpdatedGetMetrics}], #{}, Batch2)),
            MappedUpdatedGetMetrics = lists:map(fun(MId) -> {TimeSeriesId, MId} end, UpdatedGetMetrics),
            ?assertMatch({{ok, ExpectedAcc}, _}, histogram_api:get(Ctx, Id, MappedUpdatedGetMetrics, #{}, Batch2)),

            UpdatedGetMetrics
        end, [], maps:to_list(Metrics))
    end, [], maps:to_list(ExpectedMap)),

    GetAllArg = maps:to_list(maps:map(fun(_TimeSeriesId, MetricsConfigs) -> maps:keys(MetricsConfigs) end, ConfigMap)),
    GetAllArg2 = lists:flatten(lists:map(fun({TimeSeriesId, MetricsIds}) ->
        lists:map(fun(MetricsId) -> {TimeSeriesId, MetricsId} end, MetricsIds)
    end, GetAllArg)),
    GetAllArgExpected = maps:from_list(lists:map(fun({TimeSeriesId, MetricsId} = Key) ->
        {Key, maps:get(MetricsId, maps:get(TimeSeriesId, ExpectedMap))}
    end, GetAllArg2)),
    ?assertMatch({{ok, GetAllArgExpected}, _}, histogram_api:get(Ctx, Id, GetAllArg, #{}, Batch2)),
    ?assertMatch({{ok, GetAllArgExpected}, _}, histogram_api:get(Ctx, Id, GetAllArg2, #{}, Batch2)),

    % TODO - sprawdzic na bazie AllMetricsIds gdzie niie wszystkie sie powtarzaja, dodac niiestniejaca time series

    ok.

multiple_time_series_multiple_nodes() ->
    Ctx = #{},
    Id = datastore_key:new(),
    ConfigMap = lists:foldl(fun(N, Acc) ->
        TimeSeries = <<"TS", (N rem 2)>>,
        MetricsMap = maps:get(TimeSeries, Acc, #{}),
        MetricsConfig = #histogram_config{window_size = 1, max_windows_count = 2000 * N, apply_function = last},
        Acc#{TimeSeries => MetricsMap#{<<"M", (N div 2)>> => MetricsConfig}}
    end, #{}, lists:seq(1, 5)),
    Batch = init_histogram(Ctx, Id, ConfigMap),

    PointsCount = 12000,
    Points = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, PointsCount)),
    Batch2 = update_many(Ctx, Id, Points, Batch),

    ?assertEqual(11, maps:size(Batch2)),
    DocsNums = lists:foldl(fun
        (#document{value = {histogram_tail_node, {data, Windows, _, _}}}, {HeadsCountAcc, TailsCountAcc}) ->
            ?assertEqual(2000, histogram_windows:get_size(Windows)),
            {HeadsCountAcc, TailsCountAcc + 1};
        (#document{value = {histogram_hub, TimeSeries}}, {HeadsCountAcc, TailsCountAcc}) ->
            ?assertEqual(2, maps:size(TimeSeries)),
            MetricsMap0 = maps:get(<<"TS", 0>>, TimeSeries),
            MetricsMap1 = maps:get(<<"TS", 1>>, TimeSeries),
            ?assertEqual(2, maps:size(MetricsMap0)),
            ?assertEqual(3, maps:size(MetricsMap1)),
            lists:foreach(fun(#metrics{data = {data, Windows, _, _}}) ->
                ?assertEqual(2000, histogram_windows:get_size(Windows))
            end, maps:values(MetricsMap0) ++ maps:values(MetricsMap1)),
            {HeadsCountAcc + 1, TailsCountAcc}
    end, {0, 0}, maps:values(Batch2)),
    ?assertEqual({1, 10}, DocsNums),

    ExpectedMap = maps:map(fun(_TimeSeriesId, MetricsConfigs) ->
        maps:map(fun(_MetricsId, #histogram_config{max_windows_count = MaxWindowsCount}) ->
            lists:sublist(lists:reverse(Points), MaxWindowsCount)
        end, MetricsConfigs)
    end, ConfigMap),

    GetAllArg = maps:to_list(maps:map(fun(_TimeSeriesId, MetricsConfigs) -> maps:keys(MetricsConfigs) end, ConfigMap)),
    GetAllArg2 = lists:flatten(lists:map(fun({TimeSeriesId, MetricsIds}) ->
        lists:map(fun(MetricsId) -> {TimeSeriesId, MetricsId} end, MetricsIds)
    end, GetAllArg)),
    GetAllArgExpected = maps:from_list(lists:map(fun({TimeSeriesId, MetricsId} = Key) ->
        {Key, maps:get(MetricsId, maps:get(TimeSeriesId, ExpectedMap))}
    end, GetAllArg2)),
    ?assertMatch({{ok, GetAllArgExpected}, _}, histogram_api:get(Ctx, Id, GetAllArg, #{}, Batch2)),
    ?assertMatch({{ok, GetAllArgExpected}, _}, histogram_api:get(Ctx, Id, GetAllArg2, #{}, Batch2)).

not_existing_time_series_or_metrics_get() ->
    ok.

%%%===================================================================
%%% Helper functions
%%%===================================================================

init_histogram(Ctx, Id, ConfigMap) ->
    Batch = datastore_doc_batch:init(),
    InitAns = histogram_api:init(Ctx, Id, ConfigMap, Batch),
    ?assertMatch({ok, _}, InitAns),
    {ok, Batch2} = InitAns,
    Batch2.

update(Ctx, Id, NewTimestamp, NewValue, Batch) ->
    UpdateAns = histogram_api:update(Ctx, Id, NewTimestamp, NewValue, Batch),
    ?assertMatch({ok, _}, UpdateAns),
    {ok, Batch2} = UpdateAns,
    Batch2.

update_many(Ctx, Id, Points, Batch) ->
    lists:foldl(fun({NewTimestamp, NewValue}, Acc) ->
        update(Ctx, Id, NewTimestamp, NewValue, Acc)
    end, Batch, Points).

-endif.