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
            fun single_doc_splitting_strategies_create/0,
            fun multiple_metrics_splitting_strategies_create/0,
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

single_doc_splitting_strategies_create() ->
    Ctx = #{},
    Id = datastore_key:new(),
    Batch = datastore_doc_batch:init(),
    ConfigMap = #{<<"TS1">> => #{<<"M1">> => #histogram_config{max_windows_count = 0}}},
    ?assertEqual({error, empty_metrics}, histogram_api:init(Ctx, Id, ConfigMap, Batch)),

    single_doc_splitting_strategies_create_testcase(10, 10, 0, 1),
    single_doc_splitting_strategies_create_testcase(2000, 2000, 0, 1),
    single_doc_splitting_strategies_create_testcase(2100, 2000, 2000, 4),
    single_doc_splitting_strategies_create_testcase(3100, 2000, 2000, 5),
    single_doc_splitting_strategies_create_testcase(6500, 2000, 2000, 8).


multiple_metrics_splitting_strategies_create() ->
    multiple_metrics_splitting_strategies_create_testcase(10, 20, 30,
        {doc_splitting_strategy, 1, 10, 0}, {doc_splitting_strategy, 1, 20, 0}, {doc_splitting_strategy, 1, 30, 0}),

    multiple_metrics_splitting_strategies_create_testcase(10, 2000, 30,
        {doc_splitting_strategy, 1, 10, 0}, {doc_splitting_strategy, 3, 1960, 2000}, {doc_splitting_strategy, 1, 30, 0}),

    multiple_metrics_splitting_strategies_create_testcase(10, 6000, 30,
        {doc_splitting_strategy, 1, 10, 0}, {doc_splitting_strategy, 7, 1960, 2000}, {doc_splitting_strategy, 1, 30, 0}),

    multiple_metrics_splitting_strategies_create_testcase(1000, 1000, 30,
        {doc_splitting_strategy, 2, 985, 2000}, {doc_splitting_strategy, 2, 985, 2000}, {doc_splitting_strategy, 1, 30, 0}),

    multiple_metrics_splitting_strategies_create_testcase(900, 1500, 300,
        {doc_splitting_strategy, 2, 850, 1800}, {doc_splitting_strategy, 3, 850, 1500}, {doc_splitting_strategy, 1, 300, 0}),

    ConfigMap = #{<<"TS1">> => #{
        <<"M1">> => #histogram_config{max_windows_count = 3000},
        <<"M2">> => #histogram_config{max_windows_count = 4000},
        <<"M3">> => #histogram_config{max_windows_count = 5500}
    }},
    ExpectedMap = #{
        {<<"TS1">>, <<"M1">>} => {doc_splitting_strategy, 4, 667, 2000},
        {<<"TS1">>, <<"M2">>} => {doc_splitting_strategy, 5, 667, 2000},
        {<<"TS1">>, <<"M3">>} => {doc_splitting_strategy, 7, 666, 2000}
    },
    ?assertEqual(ExpectedMap, histogram_api:create_doc_splitting_strategies(ConfigMap)),

    ConfigMap2 = #{
        <<"TS1">> => #{
            <<"M1">> => #histogram_config{max_windows_count = 3500},
            <<"M2">> => #histogram_config{max_windows_count = 4000},
            <<"M3">> => #histogram_config{max_windows_count = 5000}
        },
        <<"TS2">> => #{
            <<"M1">> => #histogram_config{max_windows_count = 3000},
            <<"M2">> => #histogram_config{max_windows_count = 10000}
        }
    },
    ExpectedMap2 = #{
        {<<"TS1">>, <<"M1">>} => {doc_splitting_strategy, 5, 400, 2000},
        {<<"TS1">>, <<"M2">>} => {doc_splitting_strategy, 5, 400, 2000},
        {<<"TS1">>, <<"M3">>} => {doc_splitting_strategy, 6, 400, 2000},
        {<<"TS2">>, <<"M1">>} => {doc_splitting_strategy, 4, 400, 2000},
        {<<"TS2">>, <<"M2">>} => {doc_splitting_strategy, 11, 400, 2000}
    },
    ?assertEqual(ExpectedMap2, histogram_api:create_doc_splitting_strategies(ConfigMap2)),

    GetLargeTimeSeries = fun() -> maps:from_list(lists:map(fun(Seq) ->
        {<<(integer_to_binary(Seq))/binary>>, #histogram_config{max_windows_count = Seq}}
    end, lists:seq(1, 1500))) end,
    ConfigMap3 = #{<<"TS1">> => GetLargeTimeSeries(), <<"TS2">> => GetLargeTimeSeries()},
    Ctx = #{},
    Id = datastore_key:new(),
    Batch = datastore_doc_batch:init(),
    ?assertEqual({error, to_many_metrics}, histogram_api:init(Ctx, Id, ConfigMap3, Batch)).


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
    MetricsConfig = #histogram_config{window_size = 1, max_windows_count = 4000, apply_function = max},
    ConfigMap = #{TimeSeriesId => #{MetricsId => MetricsConfig}},
    Batch = init_histogram(Ctx, Id, ConfigMap),

    Points = lists:map(fun(I) -> {2 * I, 4 * I} end, lists:seq(1, 10000)),

%%    Start = os:timestamp(),
    Batch2 = update_many(Ctx, Id, Points, Batch),
%%    T = timer:now_diff(os:timestamp(), Start),
%%    io:format("xxxxx ~p", [T]),

    ExpectedGetAns = lists:reverse(Points),
    ExpectedMap = #{{TimeSeriesId, MetricsId} => ExpectedGetAns},
    ?assertMatch({{ok, ExpectedGetAns}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch2)),
    ?assertMatch({{ok, ExpectedMap}, _}, histogram_api:get(Ctx, Id, [{TimeSeriesId, MetricsId}], #{}, Batch2)),

    ?assertEqual(5, maps:size(Batch2)),
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
    ?assertEqual({1, 4}, DocsNums),

    [NewPoint1, NewPoint2 | Points2Tail] = Points2 =
        lists:map(fun(I) -> {2 * I, 4 * I} end, lists:seq(10001, 12000)),
    Batch3 = update_many(Ctx, Id, [NewPoint2, NewPoint1], Batch2),
    ExpectedGetAns2 = [NewPoint2, NewPoint1 | lists:sublist(ExpectedGetAns, 8000)],
    ?assertMatch({{ok, ExpectedGetAns2}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch3)),

    Batch4 = update_many(Ctx, Id, Points2Tail, Batch3),
    ExpectedGetAns3 = lists:sublist(lists:reverse(Points ++ Points2), 10000),
    ?assertMatch({{ok, ExpectedGetAns3}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch4)),

    Batch5 = update(Ctx, Id, 1, 0, Batch4),
    ?assertMatch({{ok, ExpectedGetAns3}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch5)),

    Batch6 = update(Ctx, Id, 4003, 0, Batch5),
    ExpectedGetAns4 = lists:sublist(ExpectedGetAns3, 9000),
    ?assertMatch({{ok, ExpectedGetAns4}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch6)),

    Batch7 = update(Ctx, Id, 6001, 0, Batch6),
    ExpectedGetAns5 = ExpectedGetAns4 ++ [{6001, 0}],
    ?assertMatch({{ok, ExpectedGetAns5}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch7)),

    Batch8 = update(Ctx, Id, 8003, 0, Batch7),
    ExpectedGetAns6 = lists:sublist(ExpectedGetAns5, 7999) ++ [{8003, 0}] ++ lists:sublist(ExpectedGetAns5, 8000, 1002),
    ?assertMatch({{ok, ExpectedGetAns6}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch8)),

    Batch9 = update(Ctx, Id, 16003, 0, Batch8),
    ExpectedGetAns7 = lists:sublist(ExpectedGetAns6, 3999) ++ [{16003, 0}] ++ lists:sublist(ExpectedGetAns5, 4000, 3003),
    ?assertMatch({{ok, ExpectedGetAns7}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch9)),

    Batch10 = update(Ctx, Id, 24001, 0, Batch9),
    ExpectedGetAns8 = [{24001, 0} | ExpectedGetAns7],
    ?assertMatch({{ok, ExpectedGetAns8}, _}, histogram_api:get(Ctx, Id, {TimeSeriesId, MetricsId}, #{}, Batch10)).


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
    end, [], maps:to_list(ExpectedMap)),

    NotExistingMetricsValue = #{{TimeSeriesId, <<"not_existing">>} => undefined},
    ?assertMatch({{ok, NotExistingMetricsValue}, _},
        histogram_api:get(Ctx, Id, {TimeSeriesId, <<"not_existing">>}, #{}, Batch2)),

    ExpectedWithNotExistingMetrics = maps:merge(maps:from_list(lists:map(fun(MId) ->
        {{TimeSeriesId, MId}, maps:get(MId, ExpectedMap)}
    end, maps:keys(ExpectedMap))), NotExistingMetricsValue),
    ?assertMatch({{ok, ExpectedWithNotExistingMetrics}, _},
        histogram_api:get(Ctx, Id, {TimeSeriesId, [<<"not_existing">> | maps:keys(ExpectedMap)]}, #{}, Batch2)).


single_time_series_multiple_nodes() ->
    Ctx = #{},
    Id = datastore_key:new(),
    TimeSeriesId = <<"TS1">>,
    MetricsConfigs = lists:foldl(fun(N, Acc) ->
        MetricsConfig = #histogram_config{window_size = 1, max_windows_count = 500 * N, apply_function = min},
        Acc#{<<"M", N>> => MetricsConfig}
    end, #{}, lists:seq(1, 4)),
    ConfigMap = #{TimeSeriesId => MetricsConfigs},
    Batch = init_histogram(Ctx, Id, ConfigMap),

    PointsCount = 12500,
    Points = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, PointsCount)),
    Batch2 = update_many(Ctx, Id, Points, Batch),

    ?assertEqual(6, maps:size(Batch2)),
    TailSizes = [1500, 1500, 2000, 2000, 2000],
    RemainingTailSizes = lists:foldl(fun
        (#document{value = {histogram_tail_node, {data, Windows, _, _}}}, TmpTailSizes) ->
            Size = histogram_windows:get_size(Windows),
            ?assert(lists:member(Size, TmpTailSizes)),
            TmpTailSizes -- [Size];
        (#document{value = {histogram_hub, TimeSeries}}, TmpTailSizes) ->
            [MetricsMap] = maps:values(TimeSeries),
            ?assertEqual(4, maps:size(MetricsMap)),
            lists:foreach(fun(#metrics{data = {data, Windows, _, _}}) ->
                ?assertEqual(500, histogram_windows:get_size(Windows))
            end, maps:values(MetricsMap)),
            TmpTailSizes
    end, TailSizes, maps:values(Batch2)),
    ?assertEqual([], RemainingTailSizes),

    ExpectedWindowsCounts = #{500 => 500, 1000 => 2500, 1500 => 3500, 2000 => 4500},
    ExpectedMap = maps:map(fun(_MetricsId, #histogram_config{max_windows_count = MaxWindowsCount}) ->
        lists:sublist(lists:reverse(Points), maps:get(MaxWindowsCount, ExpectedWindowsCounts))
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
        MetricsConfig = #histogram_config{window_size = 1, max_windows_count = 400 * N, apply_function = last},
        Acc#{TimeSeries => MetricsMap#{<<"M", (N div 2)>> => MetricsConfig}}
    end, #{}, lists:seq(1, 5)),
    Batch = init_histogram(Ctx, Id, ConfigMap),

    PointsCount = 24400,
    Points = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, PointsCount)),
    Batch2 = update_many(Ctx, Id, Points, Batch),

    ?assertEqual(8, maps:size(Batch2)),
    TailSizes = [1200, 1200, 1600, 1600, 1600, 2000, 2000],
    RemainingTailSizes = lists:foldl(fun
        (#document{value = {histogram_tail_node, {data, Windows, _, _}}}, TmpTailSizes) ->
            Size = histogram_windows:get_size(Windows),
            ?assert(lists:member(Size, TmpTailSizes)),
            TmpTailSizes -- [Size];
        (#document{value = {histogram_hub, TimeSeries}}, TmpTailSizes) ->
            ?assertEqual(2, maps:size(TimeSeries)),
            MetricsMap0 = maps:get(<<"TS", 0>>, TimeSeries),
            MetricsMap1 = maps:get(<<"TS", 1>>, TimeSeries),
            ?assertEqual(2, maps:size(MetricsMap0)),
            ?assertEqual(3, maps:size(MetricsMap1)),
            lists:foreach(fun(#metrics{data = {data, Windows, _, _}}) ->
                ?assertEqual(400, histogram_windows:get_size(Windows))
            end, maps:values(MetricsMap0) ++ maps:values(MetricsMap1)),
            TmpTailSizes
    end, TailSizes, maps:values(Batch2)),
    ?assertEqual([], RemainingTailSizes),

    ExpectedWindowsCounts = #{400 => 400, 800 => 2000, 1200 => 2800, 1600 => 3600, 2000 => 4400},
    ExpectedMap = maps:map(fun(_TimeSeriesId, MetricsConfigs) ->
        maps:map(fun(_MetricsId, #histogram_config{max_windows_count = MaxWindowsCount}) ->
            lists:sublist(lists:reverse(Points), maps:get(MaxWindowsCount, ExpectedWindowsCounts))
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

single_doc_splitting_strategies_create_testcase(MaxWindowsCount, WindowsInHead, WindowsInTail, DocCount) ->
    ConfigMap = #{<<"TS1">> => #{<<"M1">> => #histogram_config{max_windows_count = MaxWindowsCount}}},
    ExpectedMap = #{{<<"TS1">>, <<"M1">>} => {doc_splitting_strategy, DocCount, WindowsInHead, WindowsInTail}},
    ?assertEqual(ExpectedMap, histogram_api:create_doc_splitting_strategies(ConfigMap)).


multiple_metrics_splitting_strategies_create_testcase(WindowsCount1, WindowsCount2, WindowsCount3,
    DocSplittingStrategy1, DocSplittingStrategy2, DocSplittingStrategy3) ->
    ConfigMap = #{<<"TS1">> => #{
        <<"M1">> => #histogram_config{max_windows_count = WindowsCount1},
        <<"M2">> => #histogram_config{max_windows_count = WindowsCount2},
        <<"M3">> => #histogram_config{max_windows_count = WindowsCount3}
    }},
    ExpectedMap = #{
        {<<"TS1">>, <<"M1">>} => DocSplittingStrategy1,
        {<<"TS1">>, <<"M2">>} => DocSplittingStrategy2,
        {<<"TS1">>, <<"M3">>} => DocSplittingStrategy3
    },
    ?assertEqual(ExpectedMap, histogram_api:create_doc_splitting_strategies(ConfigMap)).


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