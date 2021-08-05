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
-include("modules/datastore/histogram_api.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("global_definitions.hrl").

-define(GET(Id, Requested, Batch), ?GET(Id, Requested, #{}, Batch)).
-define(GET(Id, Requested, Options, Batch), histogram_api:get(#{}, Id, Requested, Options, Batch)).
-define(GET_OK_ANS(Expected), {{ok, Expected}, _}).

%%%===================================================================
%%% Setup and teardown
%%%===================================================================

histogram_api_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            fun single_metric_single_node/0,
            {timeout, 300, fun single_metric_multiple_nodes/0},
            fun single_time_series_single_node/0,
            {timeout, 300, fun single_time_series_multiple_nodes/0},
            fun multiple_time_series_single_node/0,
            {timeout, 300, fun multiple_time_series_multiple_nodes/0},
            fun single_doc_splitting_strategies_create/0,
            fun multiple_metrics_splitting_strategies_create/0
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

single_metric_single_node() ->
    Id = datastore_key:new(),
    TimeSeriesId = <<"TS1">>,
    MetricsId = <<"M1">>,
    MetricsConfig = #metric_config{window_timespan = 10, max_windows_count = 5, aggregator = sum},
    ConfigMap = #{TimeSeriesId => #{MetricsId => MetricsConfig}},
    Batch = init_histogram(Id, ConfigMap),

    Measurements = lists:map(fun(I) -> {I, I/2} end, lists:seq(10, 49) ++ lists:seq(60, 69)),
    Batch2 = update_many(Id, Measurements, Batch),

    ExpectedGetAns = lists:reverse(lists:map(fun(N) -> {N, {10, 5 * N + 22.5}} end, lists:seq(10, 40, 10) ++ [60])),
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns), ?GET(Id, {TimeSeriesId, MetricsId}, Batch2)),
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns),
        ?GET(Id, {TimeSeriesId, MetricsId}, #{start => 1000}, Batch2)),
    ?assertMatch(?GET_OK_ANS([]), ?GET(Id, {TimeSeriesId, MetricsId}, #{start => 1}, Batch2)),

    ExpectedGetAns2 = lists:sublist(ExpectedGetAns, 2),
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns2),
        ?GET(Id, {TimeSeriesId, MetricsId}, #{limit => 2}, Batch2)),
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns2),
        ?GET(Id, {TimeSeriesId, MetricsId}, #{stop => 35}, Batch2)),

    ExpectedGetAns3 = lists:sublist(ExpectedGetAns, 2, 2),
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns3),
        ?GET(Id, {TimeSeriesId, MetricsId}, #{start => 45, limit => 2}, Batch2)),
    GetAns = ?GET(Id, {TimeSeriesId, MetricsId}, #{start => 45, stop => 25}, Batch2),
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns3), GetAns),
    {_, Batch3} = GetAns,

    Batch4 = update(Id, 100, 5, Batch3),
    ExpectedGetAns4 = [{100, {1, 5}} | lists:sublist(ExpectedGetAns, 4)],
    GetAns2 = ?GET(Id, {TimeSeriesId, MetricsId}, Batch4),
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns4), GetAns2),

    Batch5 = update(Id, 1, 5, Batch4),
    GetAns3 = ?GET(Id, {TimeSeriesId, MetricsId}, Batch5),
    ?assertEqual(GetAns2, GetAns3),

    Batch6 = update(Id, 53, 5, Batch5),
    ExpectedGetAns5 = [{100, {1, 5}}] ++ lists:sublist(ExpectedGetAns, 1) ++
        [{50, {1, 5}}] ++ lists:sublist(ExpectedGetAns, 2, 2),
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns5), ?GET(Id, {TimeSeriesId, MetricsId}, Batch6)),

    ?assertMatch({{error, histogram_get_failed}, _}, ?GET(Id, very_bad_arg, Batch6)).


single_metric_multiple_nodes() ->
    Id = datastore_key:new(),
    TimeSeriesId = <<"TS1">>,
    MetricsId = <<"M1">>,
    MetricsConfig = #metric_config{window_timespan = 1, max_windows_count = 4000, aggregator = max},
    ConfigMap = #{TimeSeriesId => #{MetricsId => MetricsConfig}},
    Batch = init_histogram(Id, ConfigMap),

    Measurements = lists:map(fun(I) -> {2 * I, 4 * I} end, lists:seq(1, 10000)),
    Batch2 = update_many(Id, Measurements, Batch),

    ExpectedGetAns = lists:reverse(Measurements),
    ExpectedMap = #{{TimeSeriesId, MetricsId} => ExpectedGetAns},
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns), ?GET(Id, {TimeSeriesId, MetricsId}, Batch2)),
    ?assertMatch(?GET_OK_ANS(ExpectedMap), ?GET(Id, [{TimeSeriesId, MetricsId}], Batch2)),

    ExpectedSublist = lists:sublist(ExpectedGetAns, 1001, 4000),
    ?assertMatch(?GET_OK_ANS(ExpectedSublist),
        ?GET(Id, {TimeSeriesId, MetricsId}, #{start => 18000, limit => 4000}, Batch2)),
    ?assertMatch(?GET_OK_ANS(ExpectedSublist),
        ?GET(Id, {TimeSeriesId, MetricsId}, #{start => 18000, stop => 10002}, Batch2)),

    ExpectedSublist2 = lists:sublist(ExpectedGetAns, 3001, 4000),
    ?assertMatch(?GET_OK_ANS(ExpectedSublist2),
        ?GET(Id, {TimeSeriesId, MetricsId}, #{start => 14000, limit => 4000}, Batch2)),
    ?assertMatch(?GET_OK_ANS(ExpectedSublist2),
        ?GET(Id, {TimeSeriesId, MetricsId}, #{start => 14000, stop => 6002}, Batch2)),

    ?assertEqual(5, maps:size(Batch2)),
    DocsNums = lists:foldl(fun
        (#document{value = {histogram_tail_node, #data{windows = Windows}}}, {HeadsCountAcc, TailsCountAcc}) ->
            ?assertEqual(2000, histogram_windows:get_size(Windows)),
            {HeadsCountAcc, TailsCountAcc + 1};
        (#document{value = {histogram_hub, TimeSeries}}, {HeadsCountAcc, TailsCountAcc}) ->
            [Metrics] = maps:values(TimeSeries),
            [#metric{data = #data{windows = Windows}}] = maps:values(Metrics),
            ?assertEqual(2000, histogram_windows:get_size(Windows)),
            {HeadsCountAcc + 1, TailsCountAcc}
    end, {0, 0}, maps:values(Batch2)),
    ?assertEqual({1, 4}, DocsNums),

    [NewMeasurement1, NewMeasurement2 | Measurements2Tail] = Measurements2 =
        lists:map(fun(I) -> {2 * I, 4 * I} end, lists:seq(10001, 12000)),
    Batch3 = update_many(Id, [NewMeasurement2, NewMeasurement1], Batch2),
    ExpectedGetAns2 = [NewMeasurement2, NewMeasurement1 | lists:sublist(ExpectedGetAns, 8000)],
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns2), ?GET(Id, {TimeSeriesId, MetricsId}, Batch3)),

    Batch4 = update_many(Id, Measurements2Tail, Batch3),
    ExpectedGetAns3 = lists:sublist(lists:reverse(Measurements ++ Measurements2), 10000),
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns3), ?GET(Id, {TimeSeriesId, MetricsId}, Batch4)),

    Batch5 = update(Id, 1, 0, Batch4),
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns3), ?GET(Id, {TimeSeriesId, MetricsId}, Batch5)),

    Batch6 = update(Id, 4003, 0, Batch5),
    ExpectedGetAns4 = lists:sublist(ExpectedGetAns3, 9000),
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns4), ?GET(Id, {TimeSeriesId, MetricsId}, Batch6)),

    Batch7 = update(Id, 6001, 0, Batch6),
    ExpectedGetAns5 = ExpectedGetAns4 ++ [{6001, 0}],
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns5), ?GET(Id, {TimeSeriesId, MetricsId}, Batch7)),

    Batch8 = update(Id, 8003, 0, Batch7),
    ExpectedGetAns6 = lists:sublist(ExpectedGetAns5, 7999) ++ [{8003, 0}] ++ lists:sublist(ExpectedGetAns5, 8000, 1002),
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns6), ?GET(Id, {TimeSeriesId, MetricsId}, Batch8)),

    Batch9 = update(Id, 16003, 0, Batch8),
    ExpectedGetAns7 = lists:sublist(ExpectedGetAns6, 3999) ++ [{16003, 0}] ++ lists:sublist(ExpectedGetAns5, 4000, 3003),
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns7), ?GET(Id, {TimeSeriesId, MetricsId}, Batch9)),

    Batch10 = update(Id, 24001, 0, Batch9),
    ExpectedGetAns8 = [{24001, 0} | ExpectedGetAns7],
    ?assertMatch(?GET_OK_ANS(ExpectedGetAns8), ?GET(Id, {TimeSeriesId, MetricsId}, Batch10)).


single_time_series_single_node() ->
    Id = datastore_key:new(),
    TimeSeriesId = <<"TS1">>,
    MetricsConfigs = lists:foldl(fun(N, Acc) ->
        MetricsConfig = #metric_config{window_timespan = N, max_windows_count = 600 div N + 10, aggregator = sum},
        Acc#{<<"M", N>> => MetricsConfig}
    end, #{}, lists:seq(1, 5)),
    ConfigMap = #{TimeSeriesId => MetricsConfigs},
    Batch = init_histogram(Id, ConfigMap),

    MeasurementsCount = 1199,
    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(0, MeasurementsCount)),
    Batch2 = update_many(Id, Measurements, Batch),

    ExpectedMap = maps:map(fun(_MetricsId, #metric_config{window_timespan = WindowSize, max_windows_count = MaxWindowsCount}) ->
        lists:sublist(
            lists:reverse(
                lists:map(fun(N) ->
                    {N, {WindowSize, (N + N + WindowSize - 1) * WindowSize}}
                end, lists:seq(0, MeasurementsCount, WindowSize))
            ), MaxWindowsCount)
    end, MetricsConfigs),

    lists:foldl(fun({MetricsId, Expected}, GetMetricsAcc) ->
        ?assertMatch(?GET_OK_ANS(Expected), ?GET(Id, {TimeSeriesId, MetricsId}, Batch2)),

        UpdatedGetMetrics = [MetricsId | GetMetricsAcc],
        ExpectedAcc = maps:from_list(lists:map(fun(MId) ->
            {{TimeSeriesId, MId}, maps:get(MId, ExpectedMap)}
        end, UpdatedGetMetrics)),
        ExpectedSingleValue = maps:get(MetricsId, ExpectedMap),
        ExpectedSingleValueMap = #{{TimeSeriesId, MetricsId} => ExpectedSingleValue},

        ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, {TimeSeriesId, UpdatedGetMetrics}, Batch2)),
        ?assertMatch(?GET_OK_ANS(ExpectedSingleValue), ?GET(Id, {TimeSeriesId, MetricsId}, Batch2)),
        ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, {[TimeSeriesId], UpdatedGetMetrics}, Batch2)),
        ?assertMatch(?GET_OK_ANS(ExpectedSingleValueMap), ?GET(Id, {[TimeSeriesId], MetricsId}, Batch2)),
        ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, [{TimeSeriesId, UpdatedGetMetrics}], Batch2)),
        ?assertMatch(?GET_OK_ANS(ExpectedSingleValueMap), ?GET(Id, [{TimeSeriesId, MetricsId}], Batch2)),
        ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, [{[TimeSeriesId], UpdatedGetMetrics}], Batch2)),
        ?assertMatch(?GET_OK_ANS(ExpectedSingleValueMap), ?GET(Id, [{[TimeSeriesId], MetricsId}], Batch2)),
        MappedUpdatedGetMetrics = lists:map(fun(MId) -> {TimeSeriesId, MId} end, UpdatedGetMetrics),
        ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, MappedUpdatedGetMetrics, Batch2)),

        UpdatedGetMetrics
    end, [], maps:to_list(ExpectedMap)),

    NotExistingMetricsValue = #{{TimeSeriesId, <<"not_existing">>} => undefined},
    ?assertMatch(?GET_OK_ANS(NotExistingMetricsValue),
        ?GET(Id, {TimeSeriesId, <<"not_existing">>}, Batch2)),

    ExpectedWithNotExistingMetrics = maps:merge(maps:from_list(lists:map(fun(MId) ->
        {{TimeSeriesId, MId}, maps:get(MId, ExpectedMap)}
    end, maps:keys(ExpectedMap))), NotExistingMetricsValue),
    ?assertMatch(?GET_OK_ANS(ExpectedWithNotExistingMetrics),
        ?GET(Id, {TimeSeriesId, [<<"not_existing">> | maps:keys(ExpectedMap)]}, Batch2)).


single_time_series_multiple_nodes() ->
    Id = datastore_key:new(),
    TimeSeriesId = <<"TS1">>,
    MetricsConfigs = lists:foldl(fun(N, Acc) ->
        MetricsConfig = #metric_config{window_timespan = 1, max_windows_count = 500 * N, aggregator = min},
        Acc#{<<"M", N>> => MetricsConfig}
    end, #{}, lists:seq(1, 4)),
    ConfigMap = #{TimeSeriesId => MetricsConfigs},
    Batch = init_histogram(Id, ConfigMap),

    MeasurementsCount = 12500,
    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, MeasurementsCount)),
    Batch2 = update_many(Id, Measurements, Batch),

    ?assertEqual(6, maps:size(Batch2)),
    TailSizes = [1500, 1500, 2000, 2000, 2000],
    RemainingTailSizes = lists:foldl(fun
        (#document{value = {histogram_tail_node, #data{windows = Windows}}}, TmpTailSizes) ->
            Size = histogram_windows:get_size(Windows),
            ?assert(lists:member(Size, TmpTailSizes)),
            TmpTailSizes -- [Size];
        (#document{value = {histogram_hub, TimeSeries}}, TmpTailSizes) ->
            [MetricsMap] = maps:values(TimeSeries),
            ?assertEqual(4, maps:size(MetricsMap)),
            lists:foreach(fun(#metric{data = #data{windows = Windows}}) ->
                ?assertEqual(500, histogram_windows:get_size(Windows))
            end, maps:values(MetricsMap)),
            TmpTailSizes
    end, TailSizes, maps:values(Batch2)),
    ?assertEqual([], RemainingTailSizes),

    ExpectedWindowsCounts = #{500 => 500, 1000 => 2500, 1500 => 3500, 2000 => 4500},
    ExpectedMap = maps:map(fun(_MetricsId, #metric_config{max_windows_count = MaxWindowsCount}) ->
        lists:sublist(lists:reverse(Measurements), maps:get(MaxWindowsCount, ExpectedWindowsCounts))
    end, MetricsConfigs),

    lists:foldl(fun({MetricsId, Expected}, GetMetricsAcc) ->
        ?assertMatch(?GET_OK_ANS(Expected), ?GET(Id, {TimeSeriesId, MetricsId}, Batch2)),

        UpdatedGetMetrics = [MetricsId | GetMetricsAcc],
        ExpectedAcc = maps:from_list(lists:map(fun(MId) ->
            {{TimeSeriesId, MId}, maps:get(MId, ExpectedMap)}
        end, UpdatedGetMetrics)),

        ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, {TimeSeriesId, UpdatedGetMetrics}, Batch2)),
        ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, {[TimeSeriesId], UpdatedGetMetrics}, Batch2)),
        ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, [{TimeSeriesId, UpdatedGetMetrics}], Batch2)),
        ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, [{[TimeSeriesId], UpdatedGetMetrics}], Batch2)),
        MappedUpdatedGetMetrics = lists:map(fun(MId) -> {TimeSeriesId, MId} end, UpdatedGetMetrics),
        ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, MappedUpdatedGetMetrics, Batch2)),

        UpdatedGetMetrics
    end, [], maps:to_list(ExpectedMap)).


multiple_time_series_single_node() ->
    Id = datastore_key:new(),
    ConfigMap = lists:foldl(fun(N, Acc) ->
        TimeSeries = <<"TS", (N rem 2)>>,
        MetricsMap = maps:get(TimeSeries, Acc, #{}),
        MetricsConfig = #metric_config{window_timespan = N, max_windows_count = 600 div N + 10, aggregator = sum},
        Acc#{TimeSeries => MetricsMap#{<<"M", (N div 2)>> => MetricsConfig}}
    end, #{}, lists:seq(1, 5)),
    Batch = init_histogram(Id, ConfigMap),

    MeasurementsCount = 1199,
    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(0, MeasurementsCount)),
    Batch2 = update_many(Id, Measurements, Batch),

    ExpectedMap = maps:map(fun(_TimeSeriesId, MetricsConfigs) ->
        maps:map(fun(_MetricsId, #metric_config{window_timespan = WindowSize, max_windows_count = MaxWindowsCount}) ->
            lists:sublist(
                lists:reverse(
                    lists:map(fun(N) ->
                        {N, {WindowSize, (N + N + WindowSize - 1) * WindowSize}}
                    end, lists:seq(0, MeasurementsCount, WindowSize))
                ), MaxWindowsCount)
        end, MetricsConfigs)
    end, ConfigMap),

    lists:foreach(fun({TimeSeriesId, Metrics}) ->
        TimeSeriesExpectedMap = maps:get(TimeSeriesId, ExpectedMap),
        lists:foldl(fun({MetricsId, Expected}, GetMetricsAcc) ->
            ?assertMatch(?GET_OK_ANS(Expected), ?GET(Id, {TimeSeriesId, MetricsId}, Batch2)),

            UpdatedGetMetrics = [MetricsId | GetMetricsAcc],
            ExpectedAcc = maps:from_list(lists:map(fun(MId) ->
                {{TimeSeriesId, MId}, maps:get(MId, TimeSeriesExpectedMap)}
            end, UpdatedGetMetrics)),

            ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, {TimeSeriesId, UpdatedGetMetrics}, Batch2)),
            ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, {[TimeSeriesId], UpdatedGetMetrics}, Batch2)),
            ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, [{TimeSeriesId, UpdatedGetMetrics}], Batch2)),
            ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, [{[TimeSeriesId], UpdatedGetMetrics}], Batch2)),
            MappedUpdatedGetMetrics = lists:map(fun(MId) -> {TimeSeriesId, MId} end, UpdatedGetMetrics),
            ?assertMatch(?GET_OK_ANS(ExpectedAcc), ?GET(Id, MappedUpdatedGetMetrics, Batch2)),

            UpdatedGetMetrics
        end, [], maps:to_list(Metrics))
    end, maps:to_list(ExpectedMap)),

    GetAllArg = maps:to_list(maps:map(fun(_TimeSeriesId, MetricsConfigs) -> maps:keys(MetricsConfigs) end, ConfigMap)),
    GetAllArg2 = lists:flatten(lists:map(fun({TimeSeriesId, MetricsIds}) ->
        lists:map(fun(MetricsId) -> {TimeSeriesId, MetricsId} end, MetricsIds)
    end, GetAllArg)),
    GetAllExpected = maps:from_list(lists:map(fun({TimeSeriesId, MetricsId} = Key) ->
        {Key, maps:get(MetricsId, maps:get(TimeSeriesId, ExpectedMap))}
    end, GetAllArg2)),
    ?assertMatch(?GET_OK_ANS(GetAllExpected), ?GET(Id, GetAllArg, Batch2)),
    ?assertMatch(?GET_OK_ANS(GetAllExpected), ?GET(Id, GetAllArg2, Batch2)),

    MetricsWithNotExisting = [<<"M", 0>>, <<"M", 1>>, <<"M", 2>>, <<"M", 3>>],
    GetWithNotExistingArg = [{[<<"TS", 0>>, <<"TS", 1>>, <<"TS", 2>>], MetricsWithNotExisting}],
    NotExistingTimeSeriesExpectedMap = maps:from_list(lists:map(fun(MId) ->
        {{<<"TS", 2>>, MId}, undefined}
    end, MetricsWithNotExisting)),
    GetAllWithNotExistingExpected = maps:merge(GetAllExpected#{
        {<<"TS", 0>>, <<"M", 0>>} => undefined,
        {<<"TS", 0>>, <<"M", 3>>} => undefined,
        {<<"TS", 1>>, <<"M", 3>>} => undefined
    }, NotExistingTimeSeriesExpectedMap),
    ?assertMatch(?GET_OK_ANS(GetAllWithNotExistingExpected),
        ?GET(Id, GetWithNotExistingArg, Batch2)).


multiple_time_series_multiple_nodes() ->
    Id = datastore_key:new(),
    ConfigMap = lists:foldl(fun(N, Acc) ->
        TimeSeries = <<"TS", (N rem 2)>>,
        MetricsMap = maps:get(TimeSeries, Acc, #{}),
        MetricsConfig = #metric_config{window_timespan = 1, max_windows_count = 400 * N, aggregator = last},
        Acc#{TimeSeries => MetricsMap#{<<"M", (N div 2)>> => MetricsConfig}}
    end, #{}, lists:seq(1, 5)),
    Batch = init_histogram(Id, ConfigMap),

    MeasurementsCount = 24400,
    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, MeasurementsCount)),
    Batch2 = update_many(Id, Measurements, Batch),

    ?assertEqual(8, maps:size(Batch2)),
    TailSizes = [1200, 1200, 1600, 1600, 1600, 2000, 2000],
    RemainingTailSizes = lists:foldl(fun
        (#document{value = {histogram_tail_node, #data{windows = Windows}}}, TmpTailSizes) ->
            Size = histogram_windows:get_size(Windows),
            ?assert(lists:member(Size, TmpTailSizes)),
            TmpTailSizes -- [Size];
        (#document{value = {histogram_hub, TimeSeries}}, TmpTailSizes) ->
            ?assertEqual(2, maps:size(TimeSeries)),
            MetricsMap0 = maps:get(<<"TS", 0>>, TimeSeries),
            MetricsMap1 = maps:get(<<"TS", 1>>, TimeSeries),
            ?assertEqual(2, maps:size(MetricsMap0)),
            ?assertEqual(3, maps:size(MetricsMap1)),
            lists:foreach(fun(#metric{data = #data{windows = Windows}}) ->
                ?assertEqual(400, histogram_windows:get_size(Windows))
            end, maps:values(MetricsMap0) ++ maps:values(MetricsMap1)),
            TmpTailSizes
    end, TailSizes, maps:values(Batch2)),
    ?assertEqual([], RemainingTailSizes),

    ExpectedWindowsCounts = #{400 => 400, 800 => 2000, 1200 => 2800, 1600 => 3600, 2000 => 4400},
    ExpectedMap = maps:map(fun(_TimeSeriesId, MetricsConfigs) ->
        maps:map(fun(_MetricsId, #metric_config{max_windows_count = MaxWindowsCount}) ->
            lists:sublist(lists:reverse(Measurements), maps:get(MaxWindowsCount, ExpectedWindowsCounts))
        end, MetricsConfigs)
    end, ConfigMap),

    GetAllArg = maps:to_list(maps:map(fun(_TimeSeriesId, MetricsConfigs) -> maps:keys(MetricsConfigs) end, ConfigMap)),
    GetAllArg2 = lists:flatten(lists:map(fun({TimeSeriesId, MetricsIds}) ->
        lists:map(fun(MetricsId) -> {TimeSeriesId, MetricsId} end, MetricsIds)
    end, GetAllArg)),
    GetAllExpected = maps:from_list(lists:map(fun({TimeSeriesId, MetricsId} = Key) ->
        {Key, maps:get(MetricsId, maps:get(TimeSeriesId, ExpectedMap))}
    end, GetAllArg2)),
    ?assertMatch(?GET_OK_ANS(GetAllExpected), ?GET(Id, GetAllArg, Batch2)),
    ?assertMatch(?GET_OK_ANS(GetAllExpected), ?GET(Id, GetAllArg2, Batch2)).


single_doc_splitting_strategies_create() ->
    Id = datastore_key:new(),
    Batch = datastore_doc_batch:init(),
    ConfigMap = #{<<"TS1">> => #{<<"M1">> => #metric_config{max_windows_count = 0}}},
    ?assertEqual({error, empty_metric}, histogram_api:create(#{}, Id, ConfigMap, Batch)),
    ConfigMap2 = #{<<"TS1">> => #{<<"M1">> => #metric_config{max_windows_count = 10, window_timespan = 0}}},
    ?assertEqual({error, wrong_window_timespan}, histogram_api:create(#{}, Id, ConfigMap2, Batch)),

    single_doc_splitting_strategies_create_testcase(10, 10, 0, 1),
    single_doc_splitting_strategies_create_testcase(2000, 2000, 0, 1),
    single_doc_splitting_strategies_create_testcase(2100, 2000, 2000, 4),
    single_doc_splitting_strategies_create_testcase(3100, 2000, 2000, 5),
    single_doc_splitting_strategies_create_testcase(6500, 2000, 2000, 8).


multiple_metrics_splitting_strategies_create() ->
    multiple_metrics_splitting_strategies_create_testcase(10, 20, 30,
        #splitting_strategy{max_docs_count = 1, max_windows_in_head_doc = 10, max_windows_in_tail_doc = 0},
        #splitting_strategy{max_docs_count = 1, max_windows_in_head_doc = 20, max_windows_in_tail_doc = 0},
        #splitting_strategy{max_docs_count = 1, max_windows_in_head_doc = 30, max_windows_in_tail_doc = 0}),

    multiple_metrics_splitting_strategies_create_testcase(10, 2000, 30,
        #splitting_strategy{max_docs_count = 1, max_windows_in_head_doc = 10, max_windows_in_tail_doc = 0},
        #splitting_strategy{max_docs_count = 3, max_windows_in_head_doc = 1960, max_windows_in_tail_doc = 2000},
        #splitting_strategy{max_docs_count = 1, max_windows_in_head_doc = 30, max_windows_in_tail_doc = 0}),

    multiple_metrics_splitting_strategies_create_testcase(10, 6000, 30,
        #splitting_strategy{max_docs_count = 1, max_windows_in_head_doc = 10, max_windows_in_tail_doc = 0},
        #splitting_strategy{max_docs_count = 7, max_windows_in_head_doc = 1960, max_windows_in_tail_doc = 2000},
        #splitting_strategy{max_docs_count = 1, max_windows_in_head_doc = 30, max_windows_in_tail_doc = 0}),

    multiple_metrics_splitting_strategies_create_testcase(1000, 1000, 30,
        #splitting_strategy{max_docs_count = 2, max_windows_in_head_doc = 985, max_windows_in_tail_doc = 2000},
        #splitting_strategy{max_docs_count = 2, max_windows_in_head_doc = 985, max_windows_in_tail_doc = 2000},
        #splitting_strategy{max_docs_count = 1, max_windows_in_head_doc = 30, max_windows_in_tail_doc = 0}),

    multiple_metrics_splitting_strategies_create_testcase(900, 1500, 300,
        #splitting_strategy{max_docs_count = 2, max_windows_in_head_doc = 850, max_windows_in_tail_doc = 1800},
        #splitting_strategy{max_docs_count = 3, max_windows_in_head_doc = 850, max_windows_in_tail_doc = 1500},
        #splitting_strategy{max_docs_count = 1, max_windows_in_head_doc = 300, max_windows_in_tail_doc = 0}),

    ConfigMap = #{<<"TS1">> => #{
        <<"M1">> => #metric_config{max_windows_count = 3000},
        <<"M2">> => #metric_config{max_windows_count = 4000},
        <<"M3">> => #metric_config{max_windows_count = 5500}
    }},
    ExpectedMap = #{
        {<<"TS1">>, <<"M1">>} => #splitting_strategy{
            max_docs_count = 4, max_windows_in_head_doc = 667, max_windows_in_tail_doc = 2000},
        {<<"TS1">>, <<"M2">>} => #splitting_strategy{
            max_docs_count = 5, max_windows_in_head_doc = 667, max_windows_in_tail_doc = 2000},
        {<<"TS1">>, <<"M3">>} => #splitting_strategy{
            max_docs_count = 7, max_windows_in_head_doc = 666, max_windows_in_tail_doc = 2000}
    },
    ?assertEqual(ExpectedMap, histogram_api:create_doc_splitting_strategies(ConfigMap)),

    ConfigMap2 = #{
        <<"TS1">> => #{
            <<"M1">> => #metric_config{max_windows_count = 3500},
            <<"M2">> => #metric_config{max_windows_count = 4000},
            <<"M3">> => #metric_config{max_windows_count = 5000}
        },
        <<"TS2">> => #{
            <<"M1">> => #metric_config{max_windows_count = 3000},
            <<"M2">> => #metric_config{max_windows_count = 10000}
        }
    },
    ExpectedMap2 = #{
        {<<"TS1">>, <<"M1">>} => #splitting_strategy{
            max_docs_count = 5, max_windows_in_head_doc = 400, max_windows_in_tail_doc = 2000},
        {<<"TS1">>, <<"M2">>} => #splitting_strategy{
            max_docs_count = 5, max_windows_in_head_doc = 400, max_windows_in_tail_doc = 2000},
        {<<"TS1">>, <<"M3">>} => #splitting_strategy{
            max_docs_count = 6, max_windows_in_head_doc = 400, max_windows_in_tail_doc = 2000},
        {<<"TS2">>, <<"M1">>} => #splitting_strategy{
            max_docs_count = 4, max_windows_in_head_doc = 400, max_windows_in_tail_doc = 2000},
        {<<"TS2">>, <<"M2">>} => #splitting_strategy{
            max_docs_count = 11, max_windows_in_head_doc = 400, max_windows_in_tail_doc = 2000}
    },
    ?assertEqual(ExpectedMap2, histogram_api:create_doc_splitting_strategies(ConfigMap2)),

    GetLargeTimeSeries = fun() -> maps:from_list(lists:map(fun(Seq) ->
        {<<(integer_to_binary(Seq))/binary>>, #metric_config{max_windows_count = Seq}}
    end, lists:seq(1, 1500))) end,
    ConfigMap3 = #{<<"TS1">> => GetLargeTimeSeries(), <<"TS2">> => GetLargeTimeSeries()},
    Id = datastore_key:new(),
    Batch = datastore_doc_batch:init(),
    ?assertEqual({error, to_many_metrics}, histogram_api:create(#{}, Id, ConfigMap3, Batch)).

%%%===================================================================
%%% Helper functions
%%%===================================================================

single_doc_splitting_strategies_create_testcase(MaxWindowsCount, WindowsInHead, WindowsInTail, DocCount) ->
    ConfigMap = #{<<"TS1">> => #{<<"M1">> => #metric_config{max_windows_count = MaxWindowsCount}}},
    ExpectedMap = #{{<<"TS1">>, <<"M1">>} => #splitting_strategy{
        max_docs_count = DocCount, max_windows_in_head_doc = WindowsInHead, max_windows_in_tail_doc = WindowsInTail}},
    ?assertEqual(ExpectedMap, histogram_api:create_doc_splitting_strategies(ConfigMap)).


multiple_metrics_splitting_strategies_create_testcase(WindowsCount1, WindowsCount2, WindowsCount3,
    DocSplittingStrategy1, DocSplittingStrategy2, DocSplittingStrategy3) ->
    ConfigMap = #{<<"TS1">> => #{
        <<"M1">> => #metric_config{max_windows_count = WindowsCount1},
        <<"M2">> => #metric_config{max_windows_count = WindowsCount2},
        <<"M3">> => #metric_config{max_windows_count = WindowsCount3}
    }},
    ExpectedMap = #{
        {<<"TS1">>, <<"M1">>} => DocSplittingStrategy1,
        {<<"TS1">>, <<"M2">>} => DocSplittingStrategy2,
        {<<"TS1">>, <<"M3">>} => DocSplittingStrategy3
    },
    ?assertEqual(ExpectedMap, histogram_api:create_doc_splitting_strategies(ConfigMap)).


init_histogram(Id, ConfigMap) ->
    Batch = datastore_doc_batch:init(),
    InitAns = histogram_api:create(#{}, Id, ConfigMap, Batch),
    ?assertMatch({ok, _}, InitAns),
    {ok, Batch2} = InitAns,
    Batch2.


update(Id, NewTimestamp, NewValue, Batch) ->
    UpdateAns = histogram_api:update(#{}, Id, NewTimestamp, NewValue, Batch),
    ?assertMatch({ok, _}, UpdateAns),
    {ok, Batch2} = UpdateAns,
    Batch2.


update_many(Id, Measurements, Batch) ->
    lists:foldl(fun({NewTimestamp, NewValue}, Acc) ->
        update(Id, NewTimestamp, NewValue, Acc)
    end, Batch, Measurements).

-endif.