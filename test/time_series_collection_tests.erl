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
-include_lib("ctool/include/time_series/common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include("modules/datastore/datastore_time_series.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("global_definitions.hrl").

-define(MAX_DOC_SIZE, 200).

% This SUITE verifies functionality of whole collection, not particular aggregators. Thus, tests are not repeated for
% all aggregators. Instead, tests use only subset of aggregators (more than one aggregator is required for some tests).
-define(AGGREGATOR1, avg).
-define(AGGREGATOR2, max).
-define(AGGREGATOR3, min).
-define(AGGREGATOR4, last).

%%%===================================================================
%%% Setup and teardown
%%%===================================================================

ts_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            fun empty_collection_creation/0,
            fun metric_config_sanitization/0,
            fun invalid_incorporate_config_request_with_conflicting_metric_config/0,
            fun invalid_consume_measurements_request/0,
            fun invalid_get_slice_request/0,
            fun errors_when_collection_does_not_exist/0,
            fun get_layout_request/0,
            fun single_metric_single_node/0,
            fun single_metric_infinite_resolution/0,
            {timeout, 5, fun single_metric_multiple_nodes/0},
            fun single_time_series_single_node/0,
            {timeout, 5, fun single_time_series_multiple_nodes/0},
            fun multiple_time_series_single_node/0,
            {timeout, 5, fun multiple_time_series_multiple_nodes/0},
            fun update_subset/0,
            {timeout, 5, fun lifecycle_with_config_incorporation/0}
        ]
    }.


setup() ->
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

empty_collection_creation() ->
    init_test_with_newly_created_collection(#{}),

    ?assertEqual(ok, call_incorporate_config(#{
        <<"TS1">> => #{
            <<"M1">> => #metric_config{resolution = ?SECOND_RESOLUTION, retention = 10, aggregator = ?AGGREGATOR2}
        },
        <<"TS2">> => #{
            <<"M1">> => #metric_config{resolution = ?SECOND_RESOLUTION, retention = 50, aggregator = ?AGGREGATOR4},
            <<"M2">> => #metric_config{resolution = ?SECOND_RESOLUTION, retention = 2000, aggregator = ?AGGREGATOR3}
        }
    })),
    ?assertEqual({ok, #{
        <<"TS1">> => [<<"M1">>],
        <<"TS2">> => [<<"M1">>, <<"M2">>]
    }}, call_get_layout()).


metric_config_sanitization() ->
    VeryLongName = ?TOO_LONG_NAME,
    TestCases = [{
        ?ERROR_BAD_VALUE_NAME(<<"timeSeriesName">>),
        #{<<1, 2, 3>> => #{<<"M1">> => #metric_config{retention = 1, resolution = ?MINUTE_RESOLUTION, aggregator = ?AGGREGATOR1}}}
    }, {
        ?ERROR_BAD_VALUE_NAME(<<"metricName">>),
        #{<<"TS1">> => #{VeryLongName => #metric_config{retention = 1, resolution = ?MINUTE_RESOLUTION, aggregator = ?AGGREGATOR1}}}
    }, {
        ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"retention">>, 1, 1000000),
        #{<<"TS1">> => #{<<"M1">> => #metric_config{retention = 0, resolution = ?MINUTE_RESOLUTION, aggregator = ?AGGREGATOR1}}}
    }, {
        ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"retention">>, 1, 1000000),
        #{<<"TS1">> => #{<<"M1">> => #metric_config{retention = 999999999, resolution = ?MINUTE_RESOLUTION, aggregator = ?AGGREGATOR2}}}
    }, {
        ?ERROR_BAD_DATA(<<"retention">>, <<"Retention must be set to 1 if resolution is set to 0 (infinite window resolution)">>),
        #{<<"TS1">> => #{<<"M1">> => #metric_config{retention = 10, resolution = ?INFINITY_RESOLUTION, aggregator = ?AGGREGATOR3}}}
    }, {
        ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"resolution">>, ?ALLOWED_METRIC_RESOLUTIONS),
        #{<<"TS1">> => #{<<"M1">> => #metric_config{retention = 10, resolution = -1, aggregator = ?AGGREGATOR2}}}
    }, {
        ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"aggregator">>, ?ALLOWED_METRIC_AGGREGATORS),
        #{<<"TS1">> => #{<<"M1">> => #metric_config{retention = 10, resolution = 60, aggregator = bad}}}
    }],
    lists:foreach(fun({ExpError, Config}) ->
        init_batch(),
        ?assertEqual(ExpError, call_create(datastore_key:new(), Config)),

        init_test_with_newly_created_collection(#{
            <<"TSX">> => #{
                <<"MX">> => #metric_config{resolution = ?DAY_RESOLUTION, retention = 5, aggregator = ?AGGREGATOR1}
            }
        }),
        ?assertEqual(ExpError, call_incorporate_config(Config))
    end, TestCases).


invalid_incorporate_config_request_with_conflicting_metric_config() ->
    init_test_with_newly_created_collection(#{
        <<"TS1">> => #{
            <<"M1">> => #metric_config{resolution = ?DAY_RESOLUTION, retention = 5, aggregator = ?AGGREGATOR1}
        },
        <<"TS2">> => #{
            <<"M2">> => #metric_config{resolution = ?MINUTE_RESOLUTION, retention = 1, aggregator = ?AGGREGATOR4}
        }
    }),
    ?assertEqual(
        call_incorporate_config(#{
            <<"TS1">> => #{
                <<"M1">> => #metric_config{resolution = ?DAY_RESOLUTION, retention = 5, aggregator = ?AGGREGATOR1}
            },
            <<"TS2">> => #{
                <<"M2">> => #metric_config{resolution = ?YEAR_RESOLUTION, retention = 10, aggregator = ?AGGREGATOR2}
            }
        }),
        ?ERROR_BAD_VALUE_TSC_CONFLICTING_METRIC_CONFIG(<<"TS2">>, <<"M2">>,
            #metric_config{resolution = ?MINUTE_RESOLUTION, retention = 1, aggregator = ?AGGREGATOR4},
            #metric_config{resolution = ?YEAR_RESOLUTION, retention = 10, aggregator = ?AGGREGATOR2}
        )
    ).


invalid_consume_measurements_request() ->
    init_test_with_newly_created_collection(#{
        <<"TS1">> => #{
            <<"M1">> => #metric_config{resolution = ?FIVE_SECONDS_RESOLUTION, retention = 12, aggregator = ?AGGREGATOR1},
            <<"M2">> => #metric_config{resolution = ?MINUTE_RESOLUTION, retention = 24, aggregator = ?AGGREGATOR3},
            <<"M3">> => #metric_config{resolution = ?HOUR_RESOLUTION, retention = 36, aggregator = ?AGGREGATOR2}
        },
        <<"TSX">> => #{
            <<"M1">> => #metric_config{resolution = ?FIVE_SECONDS_RESOLUTION, retention = 12, aggregator = ?AGGREGATOR1}
        }
    }),
    ?assertEqual(
        call_consume_measurements(#{
            <<"TS1">> => #{
                <<"M1">> => [{1, 2}],
                <<"M2">> => lists:map(fun(I) -> {I, I * 2} end, lists:seq(1, 100)),
                <<"M3">> => [],
                <<"M4">> => [{1, 2}]
            },
            <<"TS2">> => #{
                ?ALL_METRICS => lists:map(fun(I) -> {I, I * 2} end, lists:seq(0, 10, 20))
            },
            <<"TS3">> => #{},
            <<"TS4">> => #{<<"M4">> => []}
        }),
        ?ERROR_TSC_MISSING_LAYOUT(#{
            <<"TS1">> => [<<"M4">>],
            <<"TS2">> => [?ALL_METRICS],
            <<"TS3">> => [],
            <<"TS4">> => [<<"M4">>]
        })
    ),
    ?assertEqual(
        call_consume_measurements(#{
            ?ALL_TIME_SERIES => #{
                <<"M3">> => [{1, 2}]
            }
        }),
        ?ERROR_TSC_MISSING_LAYOUT(#{
            <<"TSX">> => [<<"M3">>]
        })
    ).


invalid_get_slice_request() ->
    init_test_with_newly_created_collection(#{
        <<"TS1">> => #{
            <<"M1">> => #metric_config{resolution = ?DAY_RESOLUTION, retention = 5, aggregator = ?AGGREGATOR1}
        },
        <<"TS2">> => #{
            <<"M1">> => #metric_config{resolution = ?MINUTE_RESOLUTION, retention = 1, aggregator = ?AGGREGATOR4},
            <<"M2">> => #metric_config{resolution = ?HOUR_RESOLUTION, retention = 10, aggregator = ?AGGREGATOR2}
        }
    }),
    ?assertEqual(
        call_get_slice(#{
            <<"TS1">> => [<<"M1.X">>, <<"M1">>],
            <<"TS2">> => [<<"M1">>, <<"M3">>],
            <<"TS3">> => [],
            <<"TS4">> => [?ALL_METRICS]
        }),
        ?ERROR_TSC_MISSING_LAYOUT(#{
            <<"TS1">> => [<<"M1.X">>],
            <<"TS2">> => [<<"M3">>],
            <<"TS3">> => [],
            <<"TS4">> => [?ALL_METRICS]
        })
    ),
    ?assertEqual(
        call_get_slice(#{
            ?ALL_TIME_SERIES => [<<"M2">>]
        }),
        ?ERROR_TSC_MISSING_LAYOUT(#{
            <<"TS1">> => [<<"M2">>]
        })
    ).


get_layout_request() ->
    init_test_with_newly_created_collection(#{
        <<"TS1">> => #{
            <<"M1">> => #metric_config{resolution = ?INFINITY_RESOLUTION, retention = 1, aggregator = ?AGGREGATOR1}
        },
        <<"TS2">> => #{
            <<"M2.1">> => #metric_config{resolution = ?MINUTE_RESOLUTION, retention = 1, aggregator = ?AGGREGATOR3},
            <<"M2.2">> => #metric_config{resolution = ?HOUR_RESOLUTION, retention = 10, aggregator = ?AGGREGATOR2}
        }
    }),
    ?assertEqual(ok, call_incorporate_config(#{
        <<"TS2">> => #{
            <<"M2.1">> => #metric_config{resolution = ?MINUTE_RESOLUTION, retention = 1, aggregator = ?AGGREGATOR3},
            <<"M2.2">> => #metric_config{resolution = ?HOUR_RESOLUTION, retention = 10, aggregator = ?AGGREGATOR2},
            <<"M2.3">> => #metric_config{resolution = ?INFINITY_RESOLUTION, retention = 1, aggregator = ?AGGREGATOR4}
        },
        <<"TS3">> => #{
            <<"M3.1">> => #metric_config{resolution = ?HOUR_RESOLUTION, retention = 24, aggregator = ?AGGREGATOR2}
        }
    })),
    ?assertEqual(call_get_layout(), {ok, #{
        <<"TS1">> => [<<"M1">>],
        <<"TS2">> => [<<"M2.1">>, <<"M2.2">>, <<"M2.3">>],
        <<"TS3">> => [<<"M3.1">>]
    }}).


single_metric_single_node() ->
    TimeSeriesName = <<"TS1">>,
    MetricName = <<"M1">>,
    init_test_with_newly_created_collection(#{
        TimeSeriesName => #{
            MetricName => #metric_config{resolution = ?FIVE_SECONDS_RESOLUTION, retention = 10, aggregator = ?AGGREGATOR1}
        }
    }),

    Measurements = lists:map(fun(I) -> {I, I / 2} end, lists:seq(10, 49) ++ lists:seq(60, 69)),
    consume_measurements_foreach_metric(Measurements),

    % Measurements are arithmetic sequence so values of windows
    % are calculated using formula for the sum of an arithmetic sequence
    ExpWindows = lists:reverse(lists:map(fun(I) ->
        {I, 0.5 * I + 1} end, lists:seq(10, 49, 5) ++ lists:seq(60, 69, 5))),
    ?assert(compare_windows_list(ExpWindows, TimeSeriesName, MetricName)),
    ?assert(compare_windows_list(ExpWindows, TimeSeriesName, MetricName, #{start_timestamp => 1000})),
    ?assert(compare_windows_list([], TimeSeriesName, MetricName, #{start_timestamp => 1})),

    ExpWindows2 = lists:sublist(ExpWindows, 2),
    ?assert(compare_windows_list(ExpWindows2, TimeSeriesName, MetricName, #{window_limit => 2})),
    ?assert(compare_windows_list(ExpWindows2, TimeSeriesName, MetricName, #{stop_timestamp => 47})),

    ExpWindows3 = lists:sublist(ExpWindows, 3, 2),
    ?assert(compare_windows_list(ExpWindows3, TimeSeriesName, MetricName, #{start_timestamp => 47, window_limit => 2})),
    ?assert(compare_windows_list(ExpWindows3, TimeSeriesName, MetricName, #{start_timestamp => 45, stop_timestamp => 36})),

    % Add a new measurement and verify if last window is dropped
    consume_measurements_foreach_metric([{100, 5}]),
    ExpWindows4 = [{100, 5.0} | lists:sublist(ExpWindows, 9)],
    ?assert(compare_windows_list(ExpWindows4, TimeSeriesName, MetricName)),

    % Add a measurement and verify if nothing changed (measurement is too old)
    consume_measurements_foreach_metric([{1, 5}]),
    ?assert(compare_windows_list(ExpWindows4, TimeSeriesName, MetricName)),

    % Add measurement in the middle of existing windows and verify windows
    consume_measurements_foreach_metric([{53, 5}]),
    ExpWindows5 = lists:flatten([
        {100, 5.0},
        lists:sublist(ExpWindows, 1, 2),
        {50, 5.0},
        lists:sublist(ExpWindows, 3, 6)
    ]),
    ?assert(compare_windows_list(ExpWindows5, TimeSeriesName, MetricName)).


single_metric_infinite_resolution() ->
    TimeSeriesName = <<"TS1">>,
    MetricName = <<"M1">>,
    init_test_with_newly_created_collection(#{
        TimeSeriesName => #{
            MetricName => #metric_config{resolution = ?INFINITY_RESOLUTION, retention = 1, aggregator = ?AGGREGATOR1}
        }
    }),

    Measurements = lists:map(fun(I) -> {I, I / 2} end, lists:seq(10, 49) ++ lists:seq(60, 69)),
    consume_measurements_foreach_metric(Measurements),

    % Measurements are arithmetic sequence so values of windows
    % are calculated using formula for the sum of an arithmetic sequence
    ExpWindows = [{0, (40 * (24.5 + 5) / 2 + 10 * (34.5 + 30) / 2) / 50}],
    ?assert(compare_windows_list(ExpWindows, TimeSeriesName, MetricName)),
    ?assert(compare_windows_list(ExpWindows, TimeSeriesName, MetricName, #{start_timestamp => 1000})),
    ?assert(compare_windows_list(ExpWindows, TimeSeriesName, MetricName, #{window_limit => 2})),
    ?assert(compare_windows_list([], TimeSeriesName, MetricName, #{stop_timestamp => 35})).


single_metric_multiple_nodes() ->
    TimeSeriesName = <<"TS1">>,
    MetricName = <<"M1">>,
    init_test_with_newly_created_collection(#{
        TimeSeriesName => #{
            MetricName => #metric_config{resolution = ?SECOND_RESOLUTION, retention = 400, aggregator = ?AGGREGATOR2}
        }
    }),

    Measurements = lists:map(fun(I) -> {2 * I, 4 * I} end, lists:seq(1, 1000)),
    consume_measurements_foreach_metric(Measurements),

    ExpWindows = lists:reverse(Measurements),
    ?assert(compare_windows_list(ExpWindows, TimeSeriesName, MetricName)),

    ExpWindows2 = lists:sublist(ExpWindows, 101, 400),
    ?assert(compare_windows_list(ExpWindows2, TimeSeriesName, MetricName, #{start_timestamp => 1800, window_limit => 400})),
    ?assert(compare_windows_list(ExpWindows2, TimeSeriesName, MetricName, #{start_timestamp => 1800, stop_timestamp => 1002})),

    ExpWindows3 = lists:sublist(ExpWindows, 301, 400),
    ?assert(compare_windows_list(ExpWindows3, TimeSeriesName, MetricName, #{start_timestamp => 1400, window_limit => 400})),
    ?assert(compare_windows_list(ExpWindows3, TimeSeriesName, MetricName, #{start_timestamp => 1400, stop_timestamp => 602})),

    % Verify if windows are stored using multiple datastore documents
    ?assertEqual(5, maps:size(get_current_batch())),
    DocsNums = lists:foldl(fun
        (#document{value = {ts_metric_data_node, #data_node{windows = Windows}}}, {HeadsCountAcc, TailsCountAcc}) ->
            ?assertEqual(200, ts_windows:get_size(Windows)),
            {HeadsCountAcc, TailsCountAcc + 1};
        (#document{value = {ts_hub, TimeSeries}}, {HeadsCountAcc, TailsCountAcc}) ->
            [Metrics] = maps:values(TimeSeries),
            [#metric{head_data = #data_node{windows = Windows}}] = maps:values(Metrics),
            ?assertEqual(200, ts_windows:get_size(Windows)),
            {HeadsCountAcc + 1, TailsCountAcc}
    end, {0, 0}, maps:values(get_current_batch())),
    ?assertEqual({1, 4}, DocsNums),

    % Add new measurements and verify if last windows are dropped
    [NewMeasurement1, NewMeasurement2 | Measurements2Tail] = Measurements2 =
        lists:map(fun(I) -> {2 * I, 4 * I} end, lists:seq(1001, 1200)),
    consume_measurements_foreach_metric([NewMeasurement2, NewMeasurement1]),
    ExpWindows4 = [NewMeasurement2, NewMeasurement1 | lists:sublist(ExpWindows, 800)],
    ?assert(compare_windows_list(ExpWindows4, TimeSeriesName, MetricName)),

    % Add new measurements and verify if no window is dropped
    % (windows were dropped during previous update so new windows can be added)
    consume_measurements_foreach_metric(Measurements2Tail),
    ExpWindows5 = lists:sublist(lists:reverse(Measurements ++ Measurements2), 1000),
    ?assert(compare_windows_list(ExpWindows5, TimeSeriesName, MetricName)),

    % Add measurement and verify if nothing changed (measurement is too old)
    consume_measurements_foreach_metric([{1, 0}]),
    ?assert(compare_windows_list(ExpWindows5, TimeSeriesName, MetricName)),

    % Add measurement in the middle of existing windows and verify windows
    consume_measurements_foreach_metric([{403, 0}]),
    ExpWindows6 = lists:sublist(ExpWindows5, 900),
    ?assert(compare_windows_list(ExpWindows6, TimeSeriesName, MetricName)),

    % Add measurement after existing windows and verify windows
    consume_measurements_foreach_metric([{601, 0}]),
    ExpWindows7 = ExpWindows6 ++ [{601, 0}],
    ?assert(compare_windows_list(ExpWindows7, TimeSeriesName, MetricName)),

    % Add measurement that results in datastore documents splitting and verify windows
    consume_measurements_foreach_metric([{803, 0}]),
    ExpWindows8 = lists:sublist(ExpWindows7, 799) ++ [{803, 0}] ++ lists:sublist(ExpWindows7, 800, 102),
    ?assert(compare_windows_list(ExpWindows8, TimeSeriesName, MetricName)),

    consume_measurements_foreach_metric([{1603, 0}]),
    ExpWindows9 = lists:sublist(ExpWindows8, 399) ++ [{1603, 0}] ++ lists:sublist(ExpWindows8, 400, 303),
    ?assert(compare_windows_list(ExpWindows9, TimeSeriesName, MetricName)),

    consume_measurements_foreach_metric([{2401, 0}]),
    ExpWindows10 = [{2401, 0} | ExpWindows9],
    ?assert(compare_windows_list(ExpWindows10, TimeSeriesName, MetricName)).


single_time_series_single_node() ->
    TimeSeriesName = <<"TS1">>,
    MetricConfigs = maps_utils:generate(fun(N) ->
        {<<"M", (integer_to_binary(N))/binary>>, #metric_config{
            resolution = ?RAND_ELEMENT([?SECOND_RESOLUTION, ?FIVE_SECONDS_RESOLUTION, ?MINUTE_RESOLUTION]),
            retention = 60 div N + 10,
            aggregator = ?AGGREGATOR1}
        }
    end, 5),
    init_test_with_newly_created_collection(#{
        TimeSeriesName => MetricConfigs
    }),

    MeasurementCount = 119,
    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(0, MeasurementCount)),
    consume_measurements_foreach_metric(Measurements),

    % Measurements are arithmetic sequence so values of windows
    % are calculated using formula for the sum of an arithmetic sequence
    ExpWindowsPerMetric = maps:map(fun(_MetricName, #metric_config{resolution = Resolution, retention = Retention}) ->
        lists:sublist(
            lists:reverse(
                lists:map(fun(N) ->
                    {N, N + N + Resolution - 1.0}
                end, lists:seq(0, MeasurementCount, Resolution))
            ), Retention)
    end, MetricConfigs),

    verify_cumulative_slicing(#{TimeSeriesName => ExpWindowsPerMetric}).


single_time_series_multiple_nodes() ->
    TimeSeriesName = <<"TS1">>,
    MetricConfigs = maps_utils:generate(fun(N) ->
        {<<"M", (integer_to_binary(N))/binary>>, #metric_config{
            resolution = ?SECOND_RESOLUTION,
            retention = 50 * N,
            aggregator = ?AGGREGATOR3}
        }
    end, 4),
    init_test_with_newly_created_collection(#{
        TimeSeriesName => MetricConfigs
    }),

    MeasurementCount = 1250,
    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, MeasurementCount)),
    consume_measurements_foreach_metric(Measurements),

    % Verify if windows are stored using multiple datastore documents
    ?assertEqual(6, maps:size(get_current_batch())),
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
    end, TailSizes, maps:values(get_current_batch())),
    ?assertEqual([], RemainingTailSizes),

    ExpectedWindowCounts = #{50 => 50, 100 => 250, 150 => 350, 200 => 450},
    ExpWindowsPerMetric = maps:map(fun(_MetricName, #metric_config{retention = Retention}) ->
        lists:sublist(lists:reverse(Measurements), maps:get(Retention, ExpectedWindowCounts))
    end, MetricConfigs),

    verify_cumulative_slicing(#{TimeSeriesName => ExpWindowsPerMetric}).


multiple_time_series_single_node() ->
    Config = lists:foldl(fun(N, Acc) ->
        TimeSeriesName = <<"TS", (integer_to_binary(N rem 2))/binary>>,
        TimeSeriesConfig = maps:get(TimeSeriesName, Acc, #{}),
        MetricConfig = #metric_config{
            resolution = ?RAND_ELEMENT([?SECOND_RESOLUTION, ?FIVE_SECONDS_RESOLUTION, ?MINUTE_RESOLUTION]),
            retention = 60 div N + 10,
            aggregator = ?AGGREGATOR1
        },
        Acc#{TimeSeriesName => TimeSeriesConfig#{<<"M", (integer_to_binary(N div 2))/binary>> => MetricConfig}}
    end, #{}, lists:seq(1, 5)),
    init_test_with_newly_created_collection(Config),

    MeasurementCount = 119,
    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(0, MeasurementCount)),
    consume_measurements_foreach_metric(Measurements),

    % Measurements are arithmetic sequence so values of windows
    % are calculated using formula for the sum of an arithmetic sequence
    ExpCompleteSlice = maps:map(fun(_TimeSeriesName, MetricConfigs) ->
        maps:map(fun(_MetricName, #metric_config{resolution = Resolution, retention = Retention}) ->
            lists:sublist(
                lists:reverse(
                    lists:map(fun(N) ->
                        {N, N + N + Resolution - 1.0}
                    end, lists:seq(0, MeasurementCount, Resolution))
                ), Retention)
        end, MetricConfigs)
    end, Config),

    ?assert(compare_slice(ExpCompleteSlice, tsc_structure:to_layout(Config))),

    verify_cumulative_slicing(ExpCompleteSlice).


multiple_time_series_multiple_nodes() ->
    Config = lists:foldl(fun(N, Acc) ->
        TimeSeriesName = <<"TS", (integer_to_binary(N rem 2))/binary>>,
        TimeSeriesConfig = maps:get(TimeSeriesName, Acc, #{}),
        MetricConfig = #metric_config{resolution = ?SECOND_RESOLUTION, retention = 40 * N, aggregator = ?AGGREGATOR4},
        Acc#{TimeSeriesName => TimeSeriesConfig#{<<"M", (integer_to_binary(N div 2))/binary>> => MetricConfig}}
    end, #{}, lists:seq(1, 5)),
    init_test_with_newly_created_collection(Config),

    MeasurementCount = 2440,
    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, MeasurementCount)),
    consume_measurements_foreach_metric(Measurements),

    % Verify if windows are stored using multiple datastore documents
    ?assertEqual(8, maps:size(get_current_batch())),
    TailSizes = [120, 120, 160, 160, 160, 200, 200],
    RemainingTailSizes = lists:foldl(fun
        (#document{value = {ts_metric_data_node, #data_node{windows = Windows}}}, TmpTailSizes) ->
            Size = ts_windows:get_size(Windows),
            ?assert(lists:member(Size, TmpTailSizes)),
            TmpTailSizes -- [Size];
        (#document{value = {ts_hub, TimeSeries}}, TmpTailSizes) ->
            ?assertEqual(2, maps:size(TimeSeries)),
            MetricsMap0 = maps:get(<<"TS0">>, TimeSeries),
            MetricsMap1 = maps:get(<<"TS1">>, TimeSeries),
            ?assertEqual(2, maps:size(MetricsMap0)),
            ?assertEqual(3, maps:size(MetricsMap1)),
            lists:foreach(fun(#metric{head_data = #data_node{windows = Windows}}) ->
                ?assertEqual(40, ts_windows:get_size(Windows))
            end, maps:values(MetricsMap0) ++ maps:values(MetricsMap1)),
            TmpTailSizes
    end, TailSizes, maps:values(get_current_batch())),
    ?assertEqual([], RemainingTailSizes),

    % Test getting all metrics
    ExpectedWindowCounts = #{40 => 40, 80 => 200, 120 => 280, 160 => 360, 200 => 440},
    ExpCompleteSlice = tsc_structure:map(fun(_TimeSeriesName, _MetricName, #metric_config{retention = Retention}) ->
        lists:sublist(lists:reverse(Measurements), maps:get(Retention, ExpectedWindowCounts))
    end, Config),

    ?assert(compare_slice(ExpCompleteSlice, tsc_structure:to_layout(Config))),
    ?assert(compare_slice(ExpCompleteSlice, ?COMPLETE_LAYOUT)).


update_subset() ->
    Config = lists:foldl(fun(N, Acc) ->
        TimeSeriesName = <<"TS", (integer_to_binary(N rem 2))/binary>>,
        TimeSeriesConfig = maps:get(TimeSeriesName, Acc, #{}),
        MetricConfig = #metric_config{resolution = ?SECOND_RESOLUTION, retention = 100, aggregator = ?AGGREGATOR2},
        Acc#{TimeSeriesName => TimeSeriesConfig#{<<"M", (integer_to_binary(N div 2))/binary>> => MetricConfig}}
    end, #{}, lists:seq(1, 5)),
    init_test_with_newly_created_collection(Config),

    consume_measurements(#{<<"TS0">> => #{?ALL_METRICS => [{0, 0}]}}),
    consume_measurements(#{<<"TS1">> => #{?ALL_METRICS => [{0, 1}]}}),
    consume_measurements(#{<<"TS0">> => #{?ALL_METRICS => [{1, 2}]}}),
    consume_measurements(#{<<"TS1">> => #{?ALL_METRICS => [{2, 3}]}}),
    consume_measurements(#{<<"TS0">> => #{<<"M1">> => [{3, 4}]}}),
    consume_measurements(#{
        <<"TS1">> => #{
            <<"M1">> => [{4, 5}],
            <<"M2">> => [{4, 5}]
        }
    }),
    consume_measurements(#{
        <<"TS0">> => #{<<"M1">> => [{5, 6}]},
        <<"TS1">> => #{<<"M0">> => [{5, 6}]}
    }),
    consume_measurements(#{
        <<"TS0">> => #{<<"M1">> => [{6, 7}]},
        <<"TS1">> => #{
            <<"M0">> => [{6, 8}],
            <<"M1">> => [{6, 8}]
        }
    }),
    consume_measurements(#{
        <<"TS0">> => #{?ALL_METRICS => [{7, 9}]},
        <<"TS1">> => #{?ALL_METRICS => [{7, 10}]}
    }),

    ExpCompleteSlice = #{
        <<"TS0">> => #{
            <<"M1">> => [{7, 9}, {6, 7}, {5, 6}, {3, 4}, {1, 2}, {0, 0}],
            <<"M2">> => [{7, 9}, {1, 2}, {0, 0}]
        },
        <<"TS1">> => #{
            <<"M0">> => [{7, 10}, {6, 8}, {5, 6}, {2, 3}, {0, 1}],
            <<"M1">> => [{7, 10}, {6, 8}, {4, 5}, {2, 3}, {0, 1}],
            <<"M2">> => [{7, 10}, {4, 5}, {2, 3}, {0, 1}]
        }
    },
    ?assert(compare_slice(ExpCompleteSlice, tsc_structure:to_layout(Config))),
    ?assert(compare_slice(ExpCompleteSlice, ?COMPLETE_LAYOUT)),
    ?assert(compare_slice(ExpCompleteSlice, #{<<"TS0">> => [?ALL_METRICS], <<"TS1">> => [<<"M0">>, <<"M1">>, <<"M2">>]})),
    ?assert(compare_slice(ExpCompleteSlice, #{<<"TS0">> => [<<"M1">>, <<"M2">>], <<"TS1">> => [?ALL_METRICS]})),

    ExpSliceWithM1 = maps:map(fun(_TimeSeriesName, WindowsPerMetric) ->
        maps:with([<<"M1">>], WindowsPerMetric)
    end, ExpCompleteSlice),
    ?assert(compare_slice(ExpSliceWithM1, #{?ALL_TIME_SERIES => [<<"M1">>]})),
    ?assert(compare_slice(ExpSliceWithM1, #{<<"TS0">> => [<<"M1">>], <<"TS1">> => [<<"M1">>]})).


lifecycle_with_config_incorporation() ->
    init_test_with_newly_created_collection(#{
        <<"TS1">> => #{
            <<"M1">> => #metric_config{resolution = ?SECOND_RESOLUTION, retention = 200, aggregator = ?AGGREGATOR1}
        }
    }),

    Measurements = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, 200)),
    consume_measurements_foreach_metric(Measurements),

    ?assertEqual(ok, call_incorporate_config(#{
        <<"TS1">> => #{
            <<"M2">> => #metric_config{resolution = ?SECOND_RESOLUTION, retention = 100, aggregator = ?AGGREGATOR4}
        }
    })),
    ?assert(compare_complete_slice(#{
        <<"TS1">> => #{
            <<"M1">> => lists:reverse(lists:map(fun(I) -> {I, 2.0 * I} end, lists:seq(1, 200))),
            <<"M2">> => []
        }
    })),

    ?assertEqual(ok, call_incorporate_config(#{
        <<"TS2">> => #{
            <<"M1">> => #metric_config{resolution = ?SECOND_RESOLUTION, retention = 400, aggregator = ?AGGREGATOR4}
        }
    })),
    ?assert(compare_complete_slice(#{
        <<"TS1">> => #{
            <<"M1">> => lists:reverse(lists:map(fun(I) -> {I, 2.0 * I} end, lists:seq(1, 200))),
            <<"M2">> => []
        },
        <<"TS2">> => #{
            <<"M1">> => []
        }
    })),

    consume_measurements(#{<<"TS2">> => #{?ALL_METRICS => Measurements}}),

    ?assertEqual(ok, call_incorporate_config(#{
        <<"TS1">> => #{
            <<"M3">> => #metric_config{resolution = ?SECOND_RESOLUTION, retention = 10, aggregator = ?AGGREGATOR2}
        },
        <<"TS2">> => #{
            <<"M2">> => #metric_config{resolution = ?SECOND_RESOLUTION, retention = 50, aggregator = ?AGGREGATOR4},
            <<"M3">> => #metric_config{resolution = ?SECOND_RESOLUTION, retention = 2000, aggregator = ?AGGREGATOR3}
        }
    })),
    ?assert(compare_complete_slice(#{
        <<"TS1">> => #{
            <<"M1">> => lists:reverse(lists:map(fun(I) -> {I, 2.0 * I} end, lists:seq(1, 200))),
            <<"M2">> => [],
            <<"M3">> => []
        },
        <<"TS2">> => #{
            <<"M1">> => lists:reverse(lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, 200))),
            <<"M2">> => [],
            <<"M3">> => []
        }
    })),

    Measurements2 = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(201, 300)),
    consume_measurements(#{<<"TS2">> => #{<<"M1">> => Measurements2, <<"M3">> => Measurements2}}),

    Documents = lists:sort(maps:values(get_current_batch())),
    ?assertMatch([
        #document{},
        #document{},
        #document{},
        #document{},
        #document{}
    ], Documents),
    [
        #document{value = Record1},
        #document{value = Record2},
        #document{value = Record3},
        #document{value = Record4},
        #document{value = Record5}
    ] = Documents,
    [
        {ts_hub, TimeSeries},
        {ts_metric_data_node, #data_node{windows = DataNodeWindows1}},
        {ts_metric_data_node, #data_node{windows = DataNodeWindows2}},
        {ts_metric_data_node, #data_node{windows = DataNodeWindows3}},
        {ts_metric_data_node, #data_node{windows = DataNodeWindows4}}
    ] = lists:sort([Record1, Record2, Record3, Record4, Record5]),
    ?assertEqual([78, 78, 169, 200], lists:sort([
        ts_windows:get_size(DataNodeWindows1),
        ts_windows:get_size(DataNodeWindows2),
        ts_windows:get_size(DataNodeWindows3),
        ts_windows:get_size(DataNodeWindows4)
    ])),
    ?assertEqual(2, maps:size(TimeSeries)),
    verify_time_series_heads(maps:get(<<"TS1">>, TimeSeries), [<<"M1">>, <<"M2">>, <<"M3">>],
        [0, 0, 31], [10, 38, 38]),
    verify_time_series_heads(maps:get(<<"TS2">>, TimeSeries), [<<"M1">>, <<"M2">>, <<"M3">>],
        [0, 22, 22], [38, 38, 38]),

    Measurements3 = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(301, 900)),
    consume_measurements(#{?ALL_TIME_SERIES => #{<<"M3">> => Measurements3}}),

    ?assert(compare_complete_slice(#{
        <<"TS1">> => #{
            <<"M1">> => lists:reverse(lists:map(fun(I) -> {I, 2.0 * I} end, lists:seq(1, 200))),
            <<"M2">> => [],
            <<"M3">> => lists:reverse(lists:map(fun(I) -> {I, 2 * I} end, lists:seq(891, 900)))
        },
        <<"TS2">> => #{
            <<"M1">> => lists:reverse(lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, 300))),
            <<"M2">> => [],
            <<"M3">> => lists:reverse(lists:map(fun(I) -> {I, 2 * I} end, lists:seq(201, 900)))
        }
    })),

    % Verify collection deletion
    {ok, Layout} = call_get_layout(),
    ?assertEqual(ok, call_delete()),
    ?assertEqual({error, not_found}, call_get_slice(Layout)),
    ?assertEqual(#{}, get_current_batch()).


errors_when_collection_does_not_exist() ->
    Id = datastore_key:new(),
    Batch = datastore_doc_batch:init(),
    Ctx = #{},

    ?assertEqual({{error, not_found}, Batch}, time_series_collection:incorporate_config(Ctx, Id, #{}, Batch)),
    ?assertEqual({{error, not_found}, Batch}, time_series_collection:delete(Ctx, Id, Batch)),
    ?assertEqual({{error, not_found}, Batch}, time_series_collection:get_layout(Ctx, Id, Batch)),
    ?assertEqual({{error, not_found}, Batch}, time_series_collection:consume_measurements(Ctx, Id, #{}, Batch)),
    ?assertEqual({{error, not_found}, Batch}, time_series_collection:get_slice(Ctx, Id, #{}, #{}, Batch)).


%%%===================================================================
%%% Helper functions
%%%===================================================================


set_collection_id(Id) ->
    put(collection_id, Id).

get_collection_id() ->
    get(collection_id).

set_current_batch(Batch) ->
    put(current_batch, Batch).

get_current_batch() ->
    get(current_batch).


init_batch() ->
    EmptyBatch = datastore_doc_batch:init(),
    set_current_batch(EmptyBatch).


call_create(Id, Config) ->
    {Result, NewBatch} = time_series_collection:create(#{}, Id, Config, get_current_batch()),
    case Result of
        {error, _} ->
            ?assertEqual(NewBatch, get_current_batch());
        ok ->
            set_current_batch(NewBatch),
            set_collection_id(Id)
    end,
    Result.


call_delete() ->
    {Result, NewBatch} = time_series_collection:delete(#{}, get_collection_id(), get_current_batch()),
    case Result of
        {error, _} ->
            ?assertEqual(NewBatch, get_current_batch());
        ok ->
            set_current_batch(NewBatch)
    end,
    Result.


call_incorporate_config(Config) ->
    {Result, NewBatch} = time_series_collection:incorporate_config(#{}, get_collection_id(), Config, get_current_batch()),
    case Result of
        {error, _} ->
            ?assertEqual(NewBatch, get_current_batch());
        ok ->
            % config incorporation should be idempotent
            ?assertEqual({Result, NewBatch}, time_series_collection:incorporate_config(
                #{}, get_collection_id(), Config, NewBatch
            )),
            set_current_batch(NewBatch)
    end,
    Result.


call_get_layout() ->
    {Result, NewBatch} = time_series_collection:get_layout(#{}, get_collection_id(), get_current_batch()),
    case Result of
        {error, _} ->
            ?assertEqual(NewBatch, get_current_batch());
        {ok, _} ->
            set_current_batch(NewBatch)
    end,
    Result.


call_consume_measurements(ConsumeSpec) ->
    {Result, NewBatch} = time_series_collection:consume_measurements(#{}, get_collection_id(), ConsumeSpec, get_current_batch()),
    case Result of
        {error, _} ->
            ?assertEqual(NewBatch, get_current_batch());
        ok ->
            set_current_batch(NewBatch)
    end,
    Result.


call_get_slice(SliceLayout) ->
    call_get_slice(SliceLayout, #{}).

call_get_slice(SliceLayout, ListWindowsOptions) ->
    {Result, NewBatch} = time_series_collection:get_slice(#{}, get_collection_id(), SliceLayout, ListWindowsOptions, get_current_batch()),
    case Result of
        {error, _} ->
            ?assertEqual(NewBatch, get_current_batch());
        {ok, _} ->
            set_current_batch(NewBatch)
    end,
    Result.


init_test_with_newly_created_collection(Config) ->
    init_batch(),

    Id = datastore_key:new(),
    ?assertEqual(ok, call_create(Id, Config)),

    ?assertEqual({error, already_exists}, call_create(Id, Config)).


consume_measurements(ConsumeSpec) ->
    ?assertMatch(ok, call_consume_measurements(ConsumeSpec)).


consume_measurements_foreach_metric(Measurements) ->
    {ok, Layout} = call_get_layout(),
    ConsumeSpec = case ?RAND_INT(1, 3) of
        1 ->
            tsc_structure:build_from_layout(fun(_TimeSeriesName, _MetricName) -> Measurements end, Layout);
        2 ->
            #{?ALL_TIME_SERIES => #{?ALL_METRICS => Measurements}};
        3 ->
            maps:map(fun(_TimeSeriesName, _) -> #{?ALL_METRICS => Measurements} end, Layout)
    end,
    consume_measurements(ConsumeSpec).


compare_complete_slice(ExpectedSlice) ->
    {ok, Layout} = call_get_layout(),
    SliceLayout = case ?RAND_INT(1, 3) of
        1 ->
            Layout;
        2 ->
            ?COMPLETE_LAYOUT;
        3 ->
            maps:map(fun(_TimeSeriesName, _) -> [?ALL_METRICS] end, Layout)
    end,
    compare_slice(ExpectedSlice, SliceLayout, #{}).

compare_slice(ExpectedSlice, SliceLayout) ->
    compare_slice(ExpectedSlice, SliceLayout, #{}).

compare_slice(ExpectedSlice, SliceLayout, ListWindowsOptions) ->
    Result = call_get_slice(SliceLayout, ListWindowsOptions),
    ?assertMatch({ok, _}, Result),
    {ok, Slice} = Result,
    MappedExpectedSlice = tsc_structure:map(fun(_, _, ValuesList) -> lists:map(fun({T, V}) -> #window_info{
        timestamp = T, value = V, first_measurement_timestamp = undefined, last_measurement_timestamp = undefined
    } end, ValuesList) end, ExpectedSlice),
    AreSlicesEqual = Slice =:= MappedExpectedSlice,
    AreSlicesEqual orelse eunit_utils:debug_log("ExpectedSlice: ~p~nActualSlice  : ~p", [ExpectedSlice, Slice]),
    AreSlicesEqual.


compare_windows_list(ExpectedWindows, TimeSeriesName, MetricName) ->
    compare_windows_list(ExpectedWindows, TimeSeriesName, MetricName, #{}).

compare_windows_list(ExpectedWindows, TimeSeriesName, MetricName, Options) ->
    SliceLayout = #{TimeSeriesName => [MetricName]},
    ExpectedSlice = #{TimeSeriesName => #{MetricName => ExpectedWindows}},
    compare_slice(ExpectedSlice, SliceLayout, Options).


verify_cumulative_slicing(ExpCompleteSlice) ->
    maps:foreach(fun(TimeSeriesName, ExpWindowsPerMetric) ->
        maps:fold(fun(CurrentMetricName, ExpWindows, MetricsToCheckAcc) ->
            ?assert(compare_windows_list(ExpWindows, TimeSeriesName, CurrentMetricName)),

            UpdatedMetricsToCheckAcc = [CurrentMetricName | MetricsToCheckAcc],
            ExpectedSlice = #{TimeSeriesName => maps:with(UpdatedMetricsToCheckAcc, ExpWindowsPerMetric)},
            ?assert(compare_slice(ExpectedSlice, #{TimeSeriesName => UpdatedMetricsToCheckAcc})),

            UpdatedMetricsToCheckAcc
        end, [], ExpWindowsPerMetric)
    end, ExpCompleteSlice).


verify_time_series_heads(MetricsMap, ExpectedMetricNames, ExpectedMetricsMapSizes, ExpectedMetricsMaxWindowsInHead) ->
    ?assertEqual(ExpectedMetricNames, lists:sort(maps:keys(MetricsMap))),
    {MetricsMapSizes, MetricsMaxWindowsInHead} = lists:unzip(lists:map(fun(#metric{
        head_data = #data_node{windows = Windows},
        splitting_strategy = #splitting_strategy{max_windows_in_head_doc = MaxWindowsInHead}
    }) ->
        {ts_windows:get_size(Windows), MaxWindowsInHead}
    end, maps:values(MetricsMap))),

    ?assertEqual(ExpectedMetricsMapSizes, lists:sort(MetricsMapSizes)),
    ?assertEqual(ExpectedMetricsMaxWindowsInHead, lists:sort(MetricsMaxWindowsInHead)).


-endif.