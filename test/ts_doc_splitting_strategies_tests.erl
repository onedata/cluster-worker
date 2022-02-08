%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Eunit tests for the ts_doc_splitting_strategies module.
%%% @end
%%%-------------------------------------------------------------------
-module(ts_doc_splitting_strategies_tests).
-author("Michal Wrzeszcz").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/time_series/common.hrl").
-include("modules/datastore/datastore_time_series.hrl").
-include("global_definitions.hrl").


-define(MAX_DOC_SIZE, 2000).


%%%===================================================================
%%% Setup
%%%===================================================================

splitting_strategies_test_() ->
    {foreach,
        fun setup/0,
        [
            fun single_doc_splitting_strategies_create/0,
            fun multiple_metrics_splitting_strategies_create/0
        ]
    }.


setup() ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, time_series_max_doc_size, ?MAX_DOC_SIZE).


%%%===================================================================
%%% Tests
%%%===================================================================

single_doc_splitting_strategies_create() ->
    Id = datastore_key:new(),
    Batch = datastore_doc_batch:init(),
    ConfigMap = #{<<"TS1">> => #{<<"M1">> => #metric_config{retention = 0}}},
    ?assertEqual({{error, empty_metric}, Batch}, time_series_collection:create(#{}, Id, ConfigMap, Batch)),
    ConfigMap2 = #{<<"TS1">> => #{<<"M1">> => #metric_config{retention = 10, resolution = -1}}},
    ?assertEqual({{error, wrong_resolution}, Batch}, time_series_collection:create(#{}, Id, ConfigMap2, Batch)),
    ConfigMap3 = #{<<"TS1">> => #{<<"M1">> => #metric_config{retention = 10, resolution = 0}}},
    ?assertEqual({{error, wrong_retention}, Batch}, time_series_collection:create(#{}, Id, ConfigMap3, Batch)),

    single_doc_splitting_strategies_create_testcase(10, #splitting_strategy{
        max_windows_in_head_doc = 10, max_windows_in_tail_doc = 0, max_docs_count = 1}),
    single_doc_splitting_strategies_create_testcase(2000, #splitting_strategy{
        max_windows_in_head_doc = 2000, max_windows_in_tail_doc = 0, max_docs_count = 1}),
    single_doc_splitting_strategies_create_testcase(2100, #splitting_strategy{
        max_windows_in_head_doc = 2000, max_windows_in_tail_doc = 2000, max_docs_count = 4}),
    single_doc_splitting_strategies_create_testcase(3100, #splitting_strategy{
        max_windows_in_head_doc = 2000, max_windows_in_tail_doc = 2000, max_docs_count = 5}),
    single_doc_splitting_strategies_create_testcase(6500, #splitting_strategy{
        max_windows_in_head_doc = 2000, max_windows_in_tail_doc = 2000, max_docs_count = 8}).


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
        <<"M1">> => #metric_config{retention = 3000},
        <<"M2">> => #metric_config{retention = 4000},
        <<"M3">> => #metric_config{retention = 5500}
    }},
    ExpectedMap = #{
        {<<"TS1">>, <<"M1">>} => #splitting_strategy{
            max_docs_count = 4, max_windows_in_head_doc = 667, max_windows_in_tail_doc = 2000},
        {<<"TS1">>, <<"M2">>} => #splitting_strategy{
            max_docs_count = 5, max_windows_in_head_doc = 667, max_windows_in_tail_doc = 2000},
        {<<"TS1">>, <<"M3">>} => #splitting_strategy{
            max_docs_count = 7, max_windows_in_head_doc = 666, max_windows_in_tail_doc = 2000}
    },
    ?assertEqual(ExpectedMap, ts_doc_splitting_strategies:calculate(ConfigMap)),

    ConfigMap2 = #{
        <<"TS1">> => #{
            <<"M1">> => #metric_config{retention = 3500},
            <<"M2">> => #metric_config{retention = 4000},
            <<"M3">> => #metric_config{retention = 5000}
        },
        <<"TS2">> => #{
            <<"M1">> => #metric_config{retention = 3000},
            <<"M2">> => #metric_config{retention = 10000}
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
    ?assertEqual(ExpectedMap2, ts_doc_splitting_strategies:calculate(ConfigMap2)),

    GetLargeTimeSeries = fun() -> maps:from_list(lists:map(fun(Seq) ->
        {<<(integer_to_binary(Seq))/binary>>, #metric_config{retention = Seq}}
    end, lists:seq(1, 1500))) end,
    ConfigMap3 = #{<<"TS1">> => GetLargeTimeSeries(), <<"TS2">> => GetLargeTimeSeries()},
    Id = datastore_key:new(),
    Batch = datastore_doc_batch:init(),
    ?assertEqual({{error, too_many_metrics}, Batch}, time_series_collection:create(#{}, Id, ConfigMap3, Batch)).


%%%===================================================================
%%% Helper functions
%%%===================================================================

single_doc_splitting_strategies_create_testcase(Retention, SplittingStrategy) ->
    ConfigMap = #{<<"TS1">> => #{<<"M1">> => #metric_config{retention = Retention}}},
    ExpectedMap = #{{<<"TS1">>, <<"M1">>} => SplittingStrategy},
    ?assertEqual(ExpectedMap, ts_doc_splitting_strategies:calculate(ConfigMap)).


multiple_metrics_splitting_strategies_create_testcase(Retention1, Retention2, Retention3,
    DocSplittingStrategy1, DocSplittingStrategy2, DocSplittingStrategy3) ->
    ConfigMap = #{<<"TS1">> => #{
        <<"M1">> => #metric_config{retention = Retention1},
        <<"M2">> => #metric_config{retention = Retention2},
        <<"M3">> => #metric_config{retention = Retention3}
    }},
    ExpectedMap = #{
        {<<"TS1">>, <<"M1">>} => DocSplittingStrategy1,
        {<<"TS1">>, <<"M2">>} => DocSplittingStrategy2,
        {<<"TS1">>, <<"M3">>} => DocSplittingStrategy3
    },
    ?assertEqual(ExpectedMap, ts_doc_splitting_strategies:calculate(ConfigMap)).

-endif.