%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Eunit tests for the time_series_collection module.
%%% @end
%%%-------------------------------------------------------------------
-module(time_series_collection_tests).
-author("Michal Wrzeszcz").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include("modules/datastore/ts_metric_config.hrl").
-include("modules/datastore/datastore_time_series.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("global_definitions.hrl").

-define(LIST(Id, Requested, Batch), ?LIST(Id, Requested, #{}, Batch)).
-define(LIST(Id, Requested, Options, Batch), time_series_collection:list_windows(#{}, Id, Requested, Options, Batch)).
-define(LIST_ALL(Id, Batch), time_series_collection:list_windows(#{}, Id, #{}, Batch)).
-define(LIST_OK_ANS(Expected), {{ok, Expected}, _}).

-define(MAX_DOC_SIZE, 200).

%%%===================================================================
%%% Setup and teardown
%%%===================================================================

ts_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            fun single_metric_single_node/0,
            {timeout, 5, fun single_metric_multiple_nodes/0},
            fun single_time_series_single_node/0,
            {timeout, 5, fun single_time_series_multiple_nodes/0},
            fun multiple_time_series_single_node/0,
            {timeout, 5, fun multiple_time_series_multiple_nodes/0},
            fun update_subset/0,
            {timeout, 5, fun metric_adding_and_deleting/0},
            fun errors_when_collection_does_not_exist/0
        ]
    }.


setup() ->
    % TODO VFS-8539 - set time_series_max_doc_size to 100
    application:set_env(?CLUSTER_WORKER_APP_NAME, time_series_max_doc_size, ?MAX_DOC_SIZE),
    meck:new([datastore_doc_batch, datastore_doc], [passthrough, no_history]),
    meck:expect(datastore_doc_batch, init, fun() -> #{} end),
    meck:expect(datastore_doc, save, fun(_Ctx, Key, Doc, Batch) -> {{ok, Doc}, Batch#{Key => Doc}} end),
    meck:expect(datastore_doc, fetch, fun(_Ctx, Key, Batch) ->
        case maps:get(Key, Batch, {error, not_found}) of
            {error, not_found} -> {{error, not_found}, Batch};
            Doc -> {{ok, Doc}, Batch}
        end
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
    MetricId = <<"M1">>,
    MetricsConfig = #metric_config{resolution = 10, retention = 5, aggregator = sum},
    ConfigMap = #{TimeSeriesId => #{MetricId => MetricsConfig}},
    Batch = init(Id, ConfigMap),
    ?assertEqual({{error, collection_already_exists}, Batch}, time_series_collection:create(#{}, Id, ConfigMap, Batch)),

    % Prepare time series collection to be used in tests (batch stores collection)
    Measurements = lists:map(fun(I) -> {I, I/2} end, lists:seq(10, 49) ++ lists:seq(60, 69)),
    Batch2 = update_many(Id, Measurements, Batch),

    % Get and verify all windows (measurements are arithmetic sequence so values of windows
    % are calculated using formula for the sum of an arithmetic sequence)
    ExpectedGetAns = lists:reverse(lists:map(fun(N) -> {N, {10, 5 * N + 22.5}} end, lists:seq(10, 40, 10) ++ [60])),
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns), ?LIST(Id, {TimeSeriesId, MetricId}, Batch2)),
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns), ?LIST(Id, {TimeSeriesId, MetricId}, #{start => 1000}, Batch2)),
    ?assertMatch(?LIST_OK_ANS([]), ?LIST(Id, {TimeSeriesId, MetricId}, #{start => 1}, Batch2)),

    % Get and verify different ranges of windows using single option
    ExpectedGetAns2 = lists:sublist(ExpectedGetAns, 2),
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns2), ?LIST(Id, {TimeSeriesId, MetricId}, #{limit => 2}, Batch2)),
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns2), ?LIST(Id, {TimeSeriesId, MetricId}, #{stop => 35}, Batch2)),

    % Get and verify different ranges of windows using multiple options
    ExpectedGetAns3 = lists:sublist(ExpectedGetAns, 2, 2),
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns3), ?LIST(Id, {TimeSeriesId, MetricId}, #{start => 45, limit => 2}, Batch2)),
    GetAns = ?LIST(Id, {TimeSeriesId, MetricId}, #{start => 45, stop => 25}, Batch2),
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns3), GetAns),
    {_, Batch3} = GetAns,

    % Add new measurement and verify if last window is dropped
    Batch4 = update(Id, 100, 5, Batch3),
    ExpectedGetAns4 = [{100, {1, 5}} | lists:sublist(ExpectedGetAns, 4)],
    GetAns2 = ?LIST(Id, {TimeSeriesId, MetricId}, Batch4),
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns4), GetAns2),

    % Add measurement and verify if nothing changed (measurement is too old)
    Batch5 = update(Id, 1, 5, Batch4),
    ?assertEqual(GetAns2, ?LIST(Id, {TimeSeriesId, MetricId}, Batch5)),

    % Add measurement in the middle of existing windows and verify windows
    Batch6 = update(Id, 53, 5, Batch5),
    ExpectedGetAns5 = [{100, {1, 5}}] ++ lists:sublist(ExpectedGetAns, 1) ++
        [{50, {1, 5}}] ++ lists:sublist(ExpectedGetAns, 2, 2),
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns5), ?LIST(Id, {TimeSeriesId, MetricId}, Batch6)),

    % Get not existing metric and verify answer
    ?assertMatch(?LIST_OK_ANS(undefined), ?LIST(Id, very_bad_arg, Batch6)).


single_metric_multiple_nodes() ->
    Id = datastore_key:new(),
    TimeSeriesId = <<"TS1">>,
    MetricId = <<"M1">>,
    MetricsConfig = #metric_config{resolution = 1, retention = 400, aggregator = max},
    ConfigMap = #{TimeSeriesId => #{MetricId => MetricsConfig}},
    Batch = init(Id, ConfigMap),

    % Prepare time series collection to be used in tests (batch stores collection)
    Measurements = lists:map(fun(I) -> {2 * I, 4 * I} end, lists:seq(1, 1000)),
    Batch2 = update_many(Id, Measurements, Batch),

    % Get and verify all windows
    ExpectedGetAns = lists:reverse(Measurements),
    ExpectedMap = #{{TimeSeriesId, MetricId} => ExpectedGetAns},
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns), ?LIST(Id, {TimeSeriesId, MetricId}, Batch2)),
    ?assertMatch(?LIST_OK_ANS(ExpectedMap), ?LIST(Id, [{TimeSeriesId, MetricId}], Batch2)),

    % Get and verify different ranges of windows
    ExpectedSublist = lists:sublist(ExpectedGetAns, 101, 400),
    ?assertMatch(?LIST_OK_ANS(ExpectedSublist),
        ?LIST(Id, {TimeSeriesId, MetricId}, #{start => 1800, limit => 400}, Batch2)),
    ?assertMatch(?LIST_OK_ANS(ExpectedSublist),
        ?LIST(Id, {TimeSeriesId, MetricId}, #{start => 1800, stop => 1002}, Batch2)),

    ExpectedSublist2 = lists:sublist(ExpectedGetAns, 301, 400),
    ?assertMatch(?LIST_OK_ANS(ExpectedSublist2),
        ?LIST(Id, {TimeSeriesId, MetricId}, #{start => 1400, limit => 400}, Batch2)),
    ?assertMatch(?LIST_OK_ANS(ExpectedSublist2),
        ?LIST(Id, {TimeSeriesId, MetricId}, #{start => 1400, stop => 602}, Batch2)),

    % Verify if windows are stored using multiple datastore documents
    ?assertEqual(5, maps:size(Batch2)),
    DocsNums = lists:foldl(fun
        (#document{value = {ts_metric_data_node, #data_node{windows = Windows}}}, {HeadsCountAcc, TailsCountAcc}) ->
            ?assertEqual(200, ts_windows:get_size(Windows)),
            {HeadsCountAcc, TailsCountAcc + 1};
        (#document{value = {ts_hub, TimeSeries}}, {HeadsCountAcc, TailsCountAcc}) ->
            [Metrics] = maps:values(TimeSeries),
            [#metric{head_data = #data_node{windows = Windows}}] = maps:values(Metrics),
            ?assertEqual(200, ts_windows:get_size(Windows)),
            {HeadsCountAcc + 1, TailsCountAcc}
    end, {0, 0}, maps:values(Batch2)),
    ?assertEqual({1, 4}, DocsNums),

    % Add new measurements and verify if last windows are dropped
    [NewMeasurement1, NewMeasurement2 | Measurements2Tail] = Measurements2 =
        lists:map(fun(I) -> {2 * I, 4 * I} end, lists:seq(1001, 1200)),
    Batch3 = update_many(Id, [NewMeasurement2, NewMeasurement1], Batch2),
    ExpectedGetAns2 = [NewMeasurement2, NewMeasurement1 | lists:sublist(ExpectedGetAns, 800)],
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns2), ?LIST(Id, {TimeSeriesId, MetricId}, Batch3)),

    % Add new measurements and verify if no window is dropped
    % (windows were dropped during previous update so new windows can be added)
    Batch4 = update_many(Id, Measurements2Tail, Batch3),
    ExpectedGetAns3 = lists:sublist(lists:reverse(Measurements ++ Measurements2), 1000),
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns3), ?LIST(Id, {TimeSeriesId, MetricId}, Batch4)),

    % Add measurement and verify if nothing changed (measurement is too old)
    Batch5 = update(Id, 1, 0, Batch4),
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns3), ?LIST(Id, {TimeSeriesId, MetricId}, Batch5)),

    % Add measurement in the middle of existing windows and verify windows
    Batch6 = update(Id, 403, 0, Batch5),
    ExpectedGetAns4 = lists:sublist(ExpectedGetAns3, 900),
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns4), ?LIST(Id, {TimeSeriesId, MetricId}, Batch6)),

    % Add measurement after existing windows and verify windows
    Batch7 = update(Id, 601, 0, Batch6),
    ExpectedGetAns5 = ExpectedGetAns4 ++ [{601, 0}],
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns5), ?LIST(Id, {TimeSeriesId, MetricId}, Batch7)),

    % Add measurement that results in datastore documents splitting and verify windows
    Batch8 = update(Id, 803, 0, Batch7),
    ExpectedGetAns6 = lists:sublist(ExpectedGetAns5, 799) ++ [{803, 0}] ++ lists:sublist(ExpectedGetAns5, 800, 102),
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns6), ?LIST(Id, {TimeSeriesId, MetricId}, Batch8)),

    Batch9 = update(Id, 1603, 0, Batch8),
    ExpectedGetAns7 = lists:sublist(ExpectedGetAns6, 399) ++ [{1603, 0}] ++ lists:sublist(ExpectedGetAns5, 400, 303),
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns7), ?LIST(Id, {TimeSeriesId, MetricId}, Batch9)),

    Batch10 = update(Id, 2401, 0, Batch9),
    ExpectedGetAns8 = [{2401, 0} | ExpectedGetAns7],
    ?assertMatch(?LIST_OK_ANS(ExpectedGetAns8), ?LIST(Id, {TimeSeriesId, MetricId}, Batch10)).


single_time_series_single_node() ->
    Id = datastore_key:new(),
    TimeSeriesId = <<"TS1">>,
    MetricsConfigs = lists:foldl(fun(N, Acc) ->
        MetricsConfig = #metric_config{resolution = N, retention = 60 div N + 10, aggregator = sum},
        Acc#{<<"M", N>> => MetricsConfig}
    end, #{}, lists:seq(1, 5)),
    ConfigMap = #{TimeSeriesId => MetricsConfigs},
    Batch = init(Id, ConfigMap),

    % Prepare time series collection to be used in tests (batch stores collection)
    MeasurementsCount = 119,
    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(0, MeasurementsCount)),
    Batch2 = update_many(Id, Measurements, Batch),

    % Prepare expected answer (measurements are arithmetic sequence so values of windows
    % are calculated using formula for the sum of an arithmetic sequence)
    ExpectedMap = maps:map(fun(_MetricId, #metric_config{resolution = Resolution, retention = Retention}) ->
        lists:sublist(
            lists:reverse(
                lists:map(fun(N) ->
                    {N, {Resolution, (N + N + Resolution - 1) * Resolution}}
                end, lists:seq(0, MeasurementsCount, Resolution))
            ), Retention)
    end, MetricsConfigs),

    % Test getting different subsets of metrics
    lists:foldl(fun({MetricId, Expected}, GetMetricsAcc) ->
        ?assertMatch(?LIST_OK_ANS(Expected), ?LIST(Id, {TimeSeriesId, MetricId}, Batch2)),

        UpdatedGetMetrics = [MetricId | GetMetricsAcc],
        ExpectedAcc = maps:from_list(lists:map(fun(MId) ->
            {{TimeSeriesId, MId}, maps:get(MId, ExpectedMap)}
        end, UpdatedGetMetrics)),
        ExpectedSingleValue = maps:get(MetricId, ExpectedMap),
        ExpectedSingleValueMap = #{{TimeSeriesId, MetricId} => ExpectedSingleValue},

        ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, {TimeSeriesId, UpdatedGetMetrics}, Batch2)),
        ?assertMatch(?LIST_OK_ANS(ExpectedSingleValue), ?LIST(Id, {TimeSeriesId, MetricId}, Batch2)),
        ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, {[TimeSeriesId], UpdatedGetMetrics}, Batch2)),
        ?assertMatch(?LIST_OK_ANS(ExpectedSingleValueMap), ?LIST(Id, {[TimeSeriesId], MetricId}, Batch2)),
        ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, [{TimeSeriesId, UpdatedGetMetrics}], Batch2)),
        ?assertMatch(?LIST_OK_ANS(ExpectedSingleValueMap), ?LIST(Id, [{TimeSeriesId, MetricId}], Batch2)),
        ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, [{[TimeSeriesId], UpdatedGetMetrics}], Batch2)),
        ?assertMatch(?LIST_OK_ANS(ExpectedSingleValueMap), ?LIST(Id, [{[TimeSeriesId], MetricId}], Batch2)),
        MappedUpdatedGetMetrics = lists:map(fun(MId) -> {TimeSeriesId, MId} end, UpdatedGetMetrics),
        ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, MappedUpdatedGetMetrics, Batch2)),

        UpdatedGetMetrics
    end, [], maps:to_list(ExpectedMap)),

    % Test getting not existing metric
    ?assertMatch(?LIST_OK_ANS(undefined), ?LIST(Id, {TimeSeriesId, <<"not_existing">>}, Batch2)),
    ExpectedWithNotExistingMetrics = (maps:from_list(lists:map(fun(MId) ->
        {{TimeSeriesId, MId}, maps:get(MId, ExpectedMap)}
    end, maps:keys(ExpectedMap))))#{{TimeSeriesId, <<"not_existing">>} => undefined},
    ?assertMatch(?LIST_OK_ANS(ExpectedWithNotExistingMetrics),
        ?LIST(Id, {TimeSeriesId, [<<"not_existing">> | maps:keys(ExpectedMap)]}, Batch2)).


single_time_series_multiple_nodes() ->
    Id = datastore_key:new(),
    TimeSeriesId = <<"TS1">>,
    MetricsConfigs = lists:foldl(fun(N, Acc) ->
        MetricsConfig = #metric_config{resolution = 1, retention = 50 * N, aggregator = min},
        Acc#{<<"M", N>> => MetricsConfig}
    end, #{}, lists:seq(1, 4)),
    ConfigMap = #{TimeSeriesId => MetricsConfigs},
    Batch = init(Id, ConfigMap),

    % Prepare time series collection to be used in tests (batch stores collection)
    MeasurementsCount = 1250,
    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, MeasurementsCount)),
    Batch2 = update_many(Id, Measurements, Batch),

    % Verify if windows are stored using multiple datastore documents
    ?assertEqual(6, maps:size(Batch2)),
    TailSizes = [150, 150, 200, 200, 200],
    RemainingTailSizes = lists:foldl(fun
        (#document{value = {ts_metric_data_node, #data_node{windows = Windows}}}, TmpTailSizes) ->
            Size = ts_windows:get_size(Windows),
            ?assert(lists:member(Size, TmpTailSizes)),
            TmpTailSizes -- [Size];
        (#document{value = {ts_hub, TimeSeries}}, TmpTailSizes) ->
            [MetricsMap] = maps:values(TimeSeries),
            ?assertEqual(4, maps:size(MetricsMap)),
            lists:foreach(fun(#metric{head_data = #data_node{windows = Windows}}) ->
                ?assertEqual(50, ts_windows:get_size(Windows))
            end, maps:values(MetricsMap)),
            TmpTailSizes
    end, TailSizes, maps:values(Batch2)),
    ?assertEqual([], RemainingTailSizes),

    ExpectedWindowsCounts = #{50 => 50, 100 => 250, 150 => 350, 200 => 450},
    ExpectedMap = maps:map(fun(_MetricId, #metric_config{retention = Retention}) ->
        lists:sublist(lists:reverse(Measurements), maps:get(Retention, ExpectedWindowsCounts))
    end, MetricsConfigs),

    % Test getting different subsets of metrics
    lists:foldl(fun({MetricId, Expected}, GetMetricsAcc) ->
        ?assertMatch(?LIST_OK_ANS(Expected), ?LIST(Id, {TimeSeriesId, MetricId}, Batch2)),

        UpdatedGetMetrics = [MetricId | GetMetricsAcc],
        ExpectedAcc = maps:from_list(lists:map(fun(MId) ->
            {{TimeSeriesId, MId}, maps:get(MId, ExpectedMap)}
        end, UpdatedGetMetrics)),

        ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, {TimeSeriesId, UpdatedGetMetrics}, Batch2)),
        ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, {[TimeSeriesId], UpdatedGetMetrics}, Batch2)),
        ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, [{TimeSeriesId, UpdatedGetMetrics}], Batch2)),
        ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, [{[TimeSeriesId], UpdatedGetMetrics}], Batch2)),
        MappedUpdatedGetMetrics = lists:map(fun(MId) -> {TimeSeriesId, MId} end, UpdatedGetMetrics),
        ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, MappedUpdatedGetMetrics, Batch2)),

        UpdatedGetMetrics
    end, [], maps:to_list(ExpectedMap)).


multiple_time_series_single_node() ->
    Id = datastore_key:new(),
    ConfigMap = lists:foldl(fun(N, Acc) ->
        TimeSeries = <<"TS", (N rem 2)>>,
        MetricsMap = maps:get(TimeSeries, Acc, #{}),
        MetricsConfig = #metric_config{resolution = N, retention = 60 div N + 10, aggregator = sum},
        Acc#{TimeSeries => MetricsMap#{<<"M", (N div 2)>> => MetricsConfig}}
    end, #{}, lists:seq(1, 5)),
    Batch = init(Id, ConfigMap),

    % Prepare time series collection to be used in tests (batch stores collection)
    MeasurementsCount = 119,
    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(0, MeasurementsCount)),
    Batch2 = update_many(Id, Measurements, Batch),

    % Prepare expected answer (measurements are arithmetic sequence so values of windows
    % are calculated using formula for the sum of an arithmetic sequence)
    ExpectedMap = maps:map(fun(_TimeSeriesId, MetricsConfigs) ->
        maps:map(fun(_MetricId, #metric_config{resolution = Resolution, retention = Retention}) ->
            lists:sublist(
                lists:reverse(
                    lists:map(fun(N) ->
                        {N, {Resolution, (N + N + Resolution - 1) * Resolution}}
                    end, lists:seq(0, MeasurementsCount, Resolution))
                ), Retention)
        end, MetricsConfigs)
    end, ConfigMap),

    % Test getting different subsets of metrics
    lists:foreach(fun({TimeSeriesId, Metrics}) ->
        TimeSeriesExpectedMap = maps:get(TimeSeriesId, ExpectedMap),
        lists:foldl(fun({MetricId, Expected}, GetMetricsAcc) ->
            ?assertMatch(?LIST_OK_ANS(Expected), ?LIST(Id, {TimeSeriesId, MetricId}, Batch2)),

            UpdatedGetMetrics = [MetricId | GetMetricsAcc],
            ExpectedAcc = maps:from_list(lists:map(fun(MId) ->
                {{TimeSeriesId, MId}, maps:get(MId, TimeSeriesExpectedMap)}
            end, UpdatedGetMetrics)),

            ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, {TimeSeriesId, UpdatedGetMetrics}, Batch2)),
            ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, {[TimeSeriesId], UpdatedGetMetrics}, Batch2)),
            ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, [{TimeSeriesId, UpdatedGetMetrics}], Batch2)),
            ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, [{[TimeSeriesId], UpdatedGetMetrics}], Batch2)),
            MappedUpdatedGetMetrics = lists:map(fun(MId) -> {TimeSeriesId, MId} end, UpdatedGetMetrics),
            ?assertMatch(?LIST_OK_ANS(ExpectedAcc), ?LIST(Id, MappedUpdatedGetMetrics, Batch2)),

            UpdatedGetMetrics
        end, [], maps:to_list(Metrics))
    end, maps:to_list(ExpectedMap)),

    % Test getting all metrics
    GetAllArg = maps:to_list(maps:map(fun(_TimeSeriesId, MetricsConfigs) -> maps:keys(MetricsConfigs) end, ConfigMap)),
    GetAllArg2 = lists:flatten(lists:map(fun({TimeSeriesId, MetricIds}) ->
        lists:map(fun(MetricId) -> {TimeSeriesId, MetricId} end, MetricIds)
    end, GetAllArg)),
    GetAllExpected = maps:from_list(lists:map(fun({TimeSeriesId, MetricId} = Key) ->
        {Key, maps:get(MetricId, maps:get(TimeSeriesId, ExpectedMap))}
    end, GetAllArg2)),
    ?assertMatch(?LIST_OK_ANS(GetAllExpected), ?LIST(Id, GetAllArg, Batch2)),
    ?assertMatch(?LIST_OK_ANS(GetAllExpected), ?LIST(Id, GetAllArg2, Batch2)),

    % Test getting not existing metric
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
    ?assertMatch(?LIST_OK_ANS(GetAllWithNotExistingExpected),
        ?LIST(Id, GetWithNotExistingArg, Batch2)).


multiple_time_series_multiple_nodes() ->
    Id = datastore_key:new(),
    ConfigMap = lists:foldl(fun(N, Acc) ->
        TimeSeries = <<"TS", (N rem 2)>>,
        MetricsMap = maps:get(TimeSeries, Acc, #{}),
        MetricsConfig = #metric_config{resolution = 1, retention = 40 * N, aggregator = last},
        Acc#{TimeSeries => MetricsMap#{<<"M", (N div 2)>> => MetricsConfig}}
    end, #{}, lists:seq(1, 5)),
    Batch = init(Id, ConfigMap),

    % Prepare time series collection to be used in tests (batch stores collection)
    MeasurementsCount = 2440,
    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, MeasurementsCount)),
    Batch2 = update_many(Id, Measurements, Batch),

    % Verify if windows are stored using multiple datastore documents
    ?assertEqual(8, maps:size(Batch2)),
    TailSizes = [120, 120, 160, 160, 160, 200, 200],
    RemainingTailSizes = lists:foldl(fun
        (#document{value = {ts_metric_data_node, #data_node{windows = Windows}}}, TmpTailSizes) ->
            Size = ts_windows:get_size(Windows),
            ?assert(lists:member(Size, TmpTailSizes)),
            TmpTailSizes -- [Size];
        (#document{value = {ts_hub, TimeSeries}}, TmpTailSizes) ->
            ?assertEqual(2, maps:size(TimeSeries)),
            MetricsMap0 = maps:get(<<"TS", 0>>, TimeSeries),
            MetricsMap1 = maps:get(<<"TS", 1>>, TimeSeries),
            ?assertEqual(2, maps:size(MetricsMap0)),
            ?assertEqual(3, maps:size(MetricsMap1)),
            lists:foreach(fun(#metric{head_data = #data_node{windows = Windows}}) ->
                ?assertEqual(40, ts_windows:get_size(Windows))
            end, maps:values(MetricsMap0) ++ maps:values(MetricsMap1)),
            TmpTailSizes
    end, TailSizes, maps:values(Batch2)),
    ?assertEqual([], RemainingTailSizes),

    % Test getting all metrics
    ExpectedWindowsCounts = #{40 => 40, 80 => 200, 120 => 280, 160 => 360, 200 => 440},
    ExpectedMap = maps:map(fun(_TimeSeriesId, MetricsConfigs) ->
        maps:map(fun(_MetricId, #metric_config{retention = Retention}) ->
            lists:sublist(lists:reverse(Measurements), maps:get(Retention, ExpectedWindowsCounts))
        end, MetricsConfigs)
    end, ConfigMap),

    GetAllArg = maps:to_list(maps:map(fun(_TimeSeriesId, MetricsConfigs) -> maps:keys(MetricsConfigs) end, ConfigMap)),
    GetAllArg2 = lists:flatten(lists:map(fun({TimeSeriesId, MetricIds}) ->
        lists:map(fun(MetricId) -> {TimeSeriesId, MetricId} end, MetricIds)
    end, GetAllArg)),
    GetAllExpected = maps:from_list(lists:map(fun({TimeSeriesId, MetricId} = Key) ->
        {Key, maps:get(MetricId, maps:get(TimeSeriesId, ExpectedMap))}
    end, GetAllArg2)),
    ?assertMatch(?LIST_OK_ANS(GetAllExpected), ?LIST(Id, GetAllArg, Batch2)),
    ?assertMatch(?LIST_OK_ANS(GetAllExpected), ?LIST(Id, GetAllArg2, Batch2)),

    % Test listing time series ids
    ListTimeSeriesIdsAns = time_series_collection:list_time_series_ids(#{}, Id, Batch2),
    ?assertMatch(?LIST_OK_ANS(_), ListTimeSeriesIdsAns),
    ?LIST_OK_ANS(TimeSeriesIds) = ListTimeSeriesIdsAns,
    ?assertEqual([<<"TS", 0>>, <<"TS", 1>>], lists:sort(TimeSeriesIds)),

    % Test listing metrics ids
    ListMetricsIdsAns = time_series_collection:list_metrics_by_time_series(#{}, Id, Batch2),
    ?assertMatch(?LIST_OK_ANS(_), ListMetricsIdsAns),
    ?LIST_OK_ANS(MetricsIds) = ListMetricsIdsAns,
    SortedMetricsIds = lists:sort(lists:map(fun({K, V}) -> {K, lists:sort(V)} end, maps:to_list(MetricsIds))),
    ?assertEqual([{<<"TS", 0>>, [<<"M",1>>, <<"M",2>>]}, {<<"TS", 1>>, [<<"M",0>>, <<"M",1>>, <<"M",2>>]}],
        SortedMetricsIds).


update_subset() ->
    Id = datastore_key:new(),
    ConfigMap = lists:foldl(fun(N, Acc) ->
        TimeSeries = <<"TS", (N rem 2)>>,
        MetricsMap = maps:get(TimeSeries, Acc, #{}),
        MetricsConfig = #metric_config{resolution = 1, retention = 100, aggregator = max},
        Acc#{TimeSeries => MetricsMap#{<<"M", (N div 2)>> => MetricsConfig}}
    end, #{}, lists:seq(1, 5)),
    Batch = init(Id, ConfigMap),

    % Update only chosen metrics
    Batch2 = update(Id, 0, <<"TS", 0>>, 0, Batch),
    Batch3 = update(Id, 0, <<"TS", 1>>, 1, Batch2),
    Batch4 = update(Id, 1, [<<"TS", 0>>], 2, Batch3),
    Batch5 = update(Id, 2, [<<"TS", 1>>], 3, Batch4),
    Batch6 = update(Id, 3, {<<"TS", 0>>, <<"M", 1>>}, 4, Batch5),
    Batch7 = update(Id, 4, [{<<"TS", 1>>, [<<"M", 1>>, <<"M", 2>>]}], 5, Batch6),
    Batch8 = update(Id, 5, [{<<"TS", 0>>, <<"M", 1>>}, {<<"TS", 1>>, <<"M", 0>>}], 6, Batch7),
    Batch9 = update(Id, 6, [{{<<"TS", 0>>, <<"M", 1>>}, 7}, {[{<<"TS", 1>>, [<<"M", 0>>, <<"M", 1>>]}], 8}], Batch8),
    Batch10 = update(Id, 7, [{<<"TS", 0>>, 9}, {<<"TS", 1>>, 10}], Batch9),

    % Verify if metrics were updated properly
    GetAllArg = maps:to_list(maps:map(fun(_TimeSeriesId, MetricsConfigs) -> maps:keys(MetricsConfigs) end, ConfigMap)),
    GetAllExpected = #{
        {<<"TS", 0>>, <<"M", 1>>} => [{7, 9}, {6,7}, {5,6}, {3,4}, {1,2}, {0,0}],
        {<<"TS", 0>>, <<"M", 2>>} => [{7, 9}, {1,2}, {0,0}],
        {<<"TS", 1>>, <<"M", 0>>} => [{7, 10}, {6,8}, {5,6}, {2,3}, {0,1}],
        {<<"TS", 1>>, <<"M", 1>>} => [{7, 10}, {6,8}, {4,5}, {2,3}, {0,1}],
        {<<"TS", 1>>, <<"M", 2>>} => [{7, 10}, {4,5}, {2,3}, {0,1}]
    },
    ?assertMatch(?LIST_OK_ANS(GetAllExpected), ?LIST(Id, GetAllArg, Batch10)).


metric_adding_and_deleting() ->
    Id = datastore_key:new(),
    InitialConfigMap = #{<<"TS1">> => #{<<"M1">> => #metric_config{resolution = 1, retention = 200, aggregator = sum}}},
    Batch = init(Id, InitialConfigMap),

    % Prepare time series collection to be used in tests (batch stores collection)
    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, 200)),
    Batch2 = update_many(Id, Measurements, Batch),

    % Add metric to collection
    ConfigMapExtension = #{<<"TS1">> => #{<<"M2">> => #metric_config{resolution = 1, retention = 100, aggregator = last}}},
    Batch3 = extend_collection(Id, ConfigMapExtension, Batch2),
    ExpectedMap = #{
        {<<"TS1">>, <<"M1">>} => lists:reverse(lists:map(fun(I) -> {I, {1, 2 * I}} end, lists:seq(1, 200))),
        {<<"TS1">>, <<"M2">>} => []
    },
    ?assertMatch(?LIST_OK_ANS(ExpectedMap), ?LIST_ALL(Id, Batch3)),

    % Add time series to collection
    ConfigMapExtension2 = #{<<"TS2">> => #{<<"M1">> => #metric_config{resolution = 1, retention = 400, aggregator = last}}},
    Batch4 = extend_collection(Id, ConfigMapExtension2, Batch3),
    ExpectedMap2 = ExpectedMap#{
        {<<"TS2">>, <<"M1">>} => []
    },
    ?assertMatch(?LIST_OK_ANS(ExpectedMap2), ?LIST_ALL(Id, Batch4)),

    % Add multiple metrics to collection
    Batch5 = update_many(Id, Measurements, <<"TS2">>, Batch4),
    ConfigMapExtension3 = #{
        <<"TS1">> => #{
            <<"M3">> => #metric_config{resolution = 1, retention = 10, aggregator = max}
        },
        <<"TS2">> => #{
            <<"M2">> => #metric_config{resolution = 1, retention = 50, aggregator = last},
            <<"M3">> => #metric_config{resolution = 1, retention = 2000, aggregator = min}
        }
    },
    Batch6 = extend_collection(Id, ConfigMapExtension3, Batch5),
    ExpectedMap3 = ExpectedMap2#{
        {<<"TS1">>, <<"M3">>} => [],
        {<<"TS2">>, <<"M1">>} => lists:reverse(lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, 200))),
        {<<"TS2">>, <<"M2">>} => [],
        {<<"TS2">>, <<"M3">>} => []
    },
    ?assertMatch(?LIST_OK_ANS(ExpectedMap3), ?LIST_ALL(Id, Batch6)),

    % Add measurements to added metrics
    Measurements2 = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(201, 300)),
    Batch7 = update_many(Id, Measurements2, {<<"TS2">>, [<<"M1">>, <<"M3">>]}, Batch6),
    Measurements3 = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(301, 900)),
    Batch8 = update_many(Id, Measurements3, {<<"TS2">>, <<"M3">>}, Batch7),
    ExpectedMap4 = ExpectedMap3#{
        {<<"TS2">>, <<"M1">>} => lists:reverse(lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, 300))),
        {<<"TS2">>, <<"M3">>} => lists:reverse(lists:map(fun(I) -> {I, 2 * I} end, lists:seq(201, 900)))
    },
    ?assertMatch(?LIST_OK_ANS(ExpectedMap4), ?LIST_ALL(Id, Batch8)),

    % Test errors handling using different add options
    ConfigMapExtension4 = #{<<"TS2">> => #{
        <<"M0">> => #metric_config{resolution = 1, retention = 10, aggregator = min},
        <<"M3">> => #metric_config{resolution = 1, retention = 10, aggregator = max}
    }},
    extend_collection_with_error(Id, ConfigMapExtension4, metric_already_exists, Batch8),
    extend_collection_with_error(Id, ConfigMapExtension4, metric_already_exists,
        #{metric_conflict_resulution_strategy => fail}, Batch8),
    extend_collection_with_error(Id, ConfigMapExtension4, metric_already_exists,
        #{time_series_conflict_resulution_strategy => merge, metric_conflict_resulution_strategy => fail}, Batch8),
    extend_collection_with_error(Id, ConfigMapExtension4, time_series_already_exists,
        #{time_series_conflict_resulution_strategy => fail}, Batch8),

    % Test overriding metric
    ConfigMapExtension5 = #{<<"TS2">> => #{
        <<"M2">> => #metric_config{resolution = 1, retention = 50, aggregator = last},
        <<"M3">> => #metric_config{resolution = 1, retention = 1000, aggregator = max},
        <<"M4">> => #metric_config{resolution = 1, retention = 10, aggregator = min}
    }},
    Batch9 = extend_collection(Id, ConfigMapExtension5, #{metric_conflict_resulution_strategy => override}, Batch8),
    ExpectedMap5 = ExpectedMap4#{
        {<<"TS2">>, <<"M2">>} => [],
        {<<"TS2">>, <<"M3">>} => [],
        {<<"TS2">>, <<"M4">>} => []
    },
    ?assertMatch(?LIST_OK_ANS(ExpectedMap5), ?LIST_ALL(Id, Batch9)),
    Batch10 = extend_collection(Id, ConfigMapExtension5,
        #{time_series_conflict_resulution_strategy => merge, metric_conflict_resulution_strategy => override}, Batch9),
    ?assertMatch(?LIST_OK_ANS(ExpectedMap5), ?LIST_ALL(Id, Batch10)),
    ?assertEqual(Batch9, Batch10),

    % Add measurements to overridden metric
    Measurements4 = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, 2)),
    Batch11 = update_many(Id, Measurements4, {<<"TS2">>, <<"M3">>}, Batch10),
    ExpectedMap6 = ExpectedMap5#{
        {<<"TS2">>, <<"M3">>} => lists:reverse(lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, 2)))
    },
    ?assertMatch(?LIST_OK_ANS(ExpectedMap6), ?LIST_ALL(Id, Batch11)),

    % Test time series overriding
    ConfigMapExtension6 = #{<<"TS1">> => #{
        <<"M0">> => #metric_config{resolution = 1, retention = 10, aggregator = last},
        <<"M1">> => #metric_config{resolution = 1, retention = 10, aggregator = max}
    }},
    Batch12 = extend_collection(Id, ConfigMapExtension6, #{time_series_conflict_resulution_strategy => override}, Batch11),
    ExpectedMap7 = (maps:remove({<<"TS1">>, <<"M2">>}, maps:remove({<<"TS1">>, <<"M3">>}, ExpectedMap6)))#{
        {<<"TS1">>, <<"M0">>} => [],
        {<<"TS1">>, <<"M1">>} => []
    },
    ?assertMatch(?LIST_OK_ANS(ExpectedMap7), ?LIST_ALL(Id, Batch12)),
    Batch13 = extend_collection(Id, ConfigMapExtension6, #{time_series_conflict_resulution_strategy => override}, Batch12),
    ?assertMatch(?LIST_OK_ANS(ExpectedMap7), ?LIST_ALL(Id, Batch13)),
    ?assertEqual(Batch12, Batch13),
    Batch14 = extend_collection(Id, ConfigMapExtension6,
        #{time_series_conflict_resulution_strategy => override, metric_conflict_resulution_strategy => override}, Batch13),
    ?assertMatch(?LIST_OK_ANS(ExpectedMap7), ?LIST_ALL(Id, Batch14)),
    ?assertEqual(Batch12, Batch14),
    Batch15 = extend_collection(Id, ConfigMapExtension6,
        #{time_series_conflict_resulution_strategy => override, metric_conflict_resulution_strategy => fail}, Batch14),
    ?assertMatch(?LIST_OK_ANS(ExpectedMap7), ?LIST_ALL(Id, Batch15)),
    ?assertEqual(Batch12, Batch15),

    % Add measurements to overridden metric
    Batch16 = update_many(Id, Measurements, {<<"TS1">>, <<"M0">>}, Batch15),
    ExpectedMap8 = ExpectedMap7#{
        {<<"TS1">>, <<"M0">>} => lists:reverse(lists:map(fun(I) -> {I, 2 * I} end, lists:seq(191, 200)))
    },
    ?assertMatch(?LIST_OK_ANS(ExpectedMap8), ?LIST_ALL(Id, Batch16)),

    % Verify if windows are stored using multiple datastore documents
    Documents = lists:sort(maps:values(Batch16)),
    ?assertMatch([#document{}, #document{}, #document{}], Documents),
    [#document{value = Record1}, #document{value = Record2}, #document{value = Record3}] = Documents,
    [{ts_hub, TimeSeries}, {ts_metric_data_node, #data_node{windows = DataNodeWindows1}},
        {ts_metric_data_node, #data_node{windows = DataNodeWindows2}}] = lists:sort([Record1, Record2, Record3]),
    % Note: data_nodes were created for metric {<<"TS2">>, <<"M1">>} ; size of node equal 78 is a result of
    % splitting_strategy when measurements were added
    % (it would be 60 if doc_splitting_strategy for current batch is used)
    ?assertEqual([78, 200], lists:sort([ts_windows:get_size(DataNodeWindows1), ts_windows:get_size(DataNodeWindows2)])),
    ?assertEqual(2, maps:size(TimeSeries)),
    verify_time_series_heads(maps:get(<<"TS1">>, TimeSeries), [<<"M0">>, <<"M1">>],
        [0, 10], [10, 10]),
    % Note: value 22 is for metric {<<"TS2">>, <<"M1">>} as a result of doc_splitting_strategy when measurements
    % were added (it would be 40 if splitting_strategy for current batch is used)
    verify_time_series_heads(maps:get(<<"TS2">>, TimeSeries), [<<"M1">>, <<"M2">>, <<"M3">>, <<"M4">>],
        [0, 0, 2, 22], [10, 50, 60, 60]),

    % Test metric deletion
    Batch17 = delete_metrics(Id, {<<"TS2">>, <<"M1">>}, Batch16),
    ExpectedMap9 = maps:remove({<<"TS2">>, <<"M1">>}, ExpectedMap8),
    ?assertMatch(?LIST_OK_ANS(ExpectedMap9), ?LIST_ALL(Id, Batch17)),

    % Test measurements adding to remaining metric
    Batch18 = update_many(Id, Measurements, {<<"TS1">>, <<"M1">>}, Batch17),
    ExpectedMap10 = ExpectedMap9#{
        {<<"TS1">>, <<"M1">>} => lists:reverse(lists:map(fun(I) -> {I, 2 * I} end, lists:seq(191, 200)))
    },
    ?assertMatch(?LIST_OK_ANS(ExpectedMap10), ?LIST_ALL(Id, Batch18)),

    % Test deletion of multiple metrics
    Batch19 = delete_metrics(Id, [{<<"TS1">>, <<"M1">>}, {<<"TS2">>, <<"M2">>}], Batch18),
    ExpectedMap11 = maps:remove({<<"TS1">>, <<"M1">>}, maps:remove({<<"TS2">>, <<"M2">>}, ExpectedMap10)),
    ?assertMatch(?LIST_OK_ANS(ExpectedMap11), ?LIST_ALL(Id, Batch19)),

    % Test time series deletion
    Batch20 = delete_metrics(Id, <<"TS1">>, Batch19),
    ExpectedMap12 = maps:remove({<<"TS1">>, <<"M0">>}, ExpectedMap11),
    ?assertMatch(?LIST_OK_ANS(ExpectedMap12), ?LIST_ALL(Id, Batch20)),

    % Verify documents used to store measurements after deletion
    Documents2 = lists:sort(maps:values(Batch20)),
    ?assertMatch([#document{value = {ts_hub, _}}], Documents2),
    [#document{value = {ts_hub, TimeSeries2}}] = Documents2,
    ?assertEqual(1, maps:size(TimeSeries2)),
    verify_time_series_heads(maps:get(<<"TS2">>, TimeSeries2), [<<"M3">>, <<"M4">>],
        [0, 2], [10, 190]),

    % Verify collection deletion
    DeleteAns = time_series_collection:delete(#{}, Id, Batch20),
    ?assertMatch({ok, _}, DeleteAns),
    {ok, Batch21} = DeleteAns,
    ?assertEqual({{error, not_found}, Batch21}, ?LIST_ALL(Id, Batch21)),
    ?assertEqual(#{}, Batch21).


errors_when_collection_does_not_exist() ->
    Id = datastore_key:new(),
    Batch = datastore_doc_batch:init(),
    Ctx = #{},

    ?assertEqual({{error, not_found}, Batch}, time_series_collection:add_metrics(Ctx, Id, #{}, #{}, Batch)),
    ?assertEqual({{error, not_found}, Batch}, time_series_collection:delete_metrics(Ctx, Id, [], Batch)),
    ?assertEqual({{error, not_found}, Batch}, time_series_collection:list_time_series_ids(Ctx, Id, Batch)),
    ?assertEqual({{error, not_found}, Batch}, time_series_collection:list_metrics_by_time_series(Ctx, Id, Batch)),
    ?assertEqual({{error, not_found}, Batch}, time_series_collection:update(Ctx, Id, 0, 0, Batch)),
    ?assertEqual({{error, not_found}, Batch}, time_series_collection:update(Ctx, Id, 0, [], Batch)),
    ?assertEqual({{error, not_found}, Batch}, time_series_collection:update(Ctx, Id, 0, [], 0, Batch)),
    ?assertEqual({{error, not_found}, Batch}, time_series_collection:list_windows(Ctx, Id, #{}, Batch)),
    ?assertEqual({{error, not_found}, Batch}, time_series_collection:list_windows(Ctx, Id, [], #{}, Batch)),
    ?assertEqual({{error, not_found}, Batch}, time_series_collection:delete(Ctx, Id, Batch)).


%%%===================================================================
%%% Helper functions
%%%===================================================================

init(Id, ConfigMap) ->
    Batch = datastore_doc_batch:init(),
    InitAns = time_series_collection:create(#{}, Id, ConfigMap, Batch),
    ?assertMatch({ok, _}, InitAns),
    {ok, Batch2} = InitAns,
    Batch2.


update(Id, NewTimestamp, ValueOrUpdateRange, Batch) ->
    UpdateAns = time_series_collection:update(#{}, Id, NewTimestamp, ValueOrUpdateRange, Batch),
    ?assertMatch({ok, _}, UpdateAns),
    {ok, Batch2} = UpdateAns,
    Batch2.


update(Id, NewTimestamp, MetricsToUpdate, NewValue, Batch) ->
    UpdateAns = time_series_collection:update(#{}, Id, NewTimestamp, MetricsToUpdate, NewValue, Batch),
    ?assertMatch({ok, _}, UpdateAns),
    {ok, Batch2} = UpdateAns,
    Batch2.


update_many(Id, Measurements, Batch) ->
    lists:foldl(fun({NewTimestamp, ValueOrUpdateRange}, Acc) ->
        update(Id, NewTimestamp, ValueOrUpdateRange, Acc)
    end, Batch, Measurements).


update_many(Id, Measurements, MetricsToUpdate, Batch) ->
    lists:foldl(fun({NewTimestamp, NewValue}, Acc) ->
        update(Id, NewTimestamp, MetricsToUpdate, NewValue, Acc)
    end, Batch, Measurements).


extend_collection(Id, ConfigMapExtension, Batch) ->
    extend_collection(Id, ConfigMapExtension, #{}, Batch).

extend_collection(Id, ConfigMapExtension, Options, Batch) ->
    AddAns = time_series_collection:add_metrics(#{}, Id, ConfigMapExtension, Options, Batch),
    ?assertMatch({ok, _}, AddAns),
    {ok, Batch2} = AddAns,
    Batch2.


extend_collection_with_error(Id, ConfigMapExtension, Reason, Batch) ->
    extend_collection_with_error(Id, ConfigMapExtension, Reason, #{}, Batch).

extend_collection_with_error(Id, ConfigMapExtension, Reason, Options, Batch) ->
    AddAns = time_series_collection:add_metrics(#{}, Id, ConfigMapExtension, Options, Batch),
    ?assertMatch({{error, Reason}, _}, AddAns),
    {_, Batch2} = AddAns,
    ?assertEqual(Batch, Batch2).


delete_metrics(Id, MetricsToDelete, Batch) ->
    DeleteAns = time_series_collection:delete_metrics(#{}, Id, MetricsToDelete, Batch),
    ?assertMatch({ok, _}, DeleteAns),
    {ok, Batch2} = DeleteAns,
    Batch2.


verify_time_series_heads(MetricsMap, ExpectedMetricIds, ExpectedMetricsMapSizes, ExpectedMetricsMaxWindowsInHead) ->
    ?assertEqual(ExpectedMetricIds, lists:sort(maps:keys(MetricsMap))),
    {MetricsMapSizes, MetricsMaxWindowsInHead} = lists:unzip(lists:map(fun(#metric{
        head_data = #data_node{windows = Windows},
        splitting_strategy = #splitting_strategy{max_windows_in_head_doc = MaxWindowsInHead}
    }) ->
        {ts_windows:get_size(Windows), MaxWindowsInHead}
    end, maps:values(MetricsMap))),

    ?assertEqual(ExpectedMetricsMapSizes, lists:sort(MetricsMapSizes)),
    ?assertEqual(ExpectedMetricsMaxWindowsInHead, lists:sort(MetricsMaxWindowsInHead)).


-endif.