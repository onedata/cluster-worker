%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal datastore API used for operating on time series collections.
%%% Time series collection consists of several time series. Each time series consists of
%%% several metrics. Metric is a set of windows that aggregate multiple
%%% measurements from particular period of time. E.g.,
%%% MyTimeSeriesCollection = #{
%%%    TimeSeries1 = #{
%%%       Metric1 = [Window1, Window2, ...],
%%%       Metric2 = ...
%%%    },
%%%    TimeSeries2 = ...
%%% }
%%% Window = {WindowTimestamp, aggregator(PrevAggregatedValue, MeasurementValue)} where
%%% PrevAggregatedValue is result of previous aggregator function executions
%%% (window can be created using several measurements).
%%% See ts_windows:insert_value/4 to see possible aggregation functions.
%%%
%%% Consult @see tsc_structure module for more information about the structure of
%%% time series collection as perceived by higher level modules.
%%%
%%% The module delegates operations on single metric to ts_metric module
%%% that is able to handle infinite number of windows inside single metric splitting
%%% windows set to subsets stored in multiple records that are saved to multiple
%%% datastore documents by ts_persistence helper module.
%%%
%%% All metrics from all time series are kept together. If any metric windows count
%%% is so high that it cannot be stored in single datastore document, the metric
%%% window set is divided into several records by ts_metric module and then
%%% persisted as several datastore documents by ts_persistence module. Thus,
%%% windows of each metric form linked list of records storing parts of windows set,
%%% with head of the list treated exceptionally (heads are kept together for all
%%% metrics while rest of records with windows are kept separately for each metric). 
%%% 
%%% Metric can also be created with infinite resolution (0), in such a case only one 
%%% window will need to be kept for this metric. Therefore retention should be set to 1.
%%% @end
%%%-------------------------------------------------------------------
-module(time_series_collection).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_time_series.hrl").
-include_lib("ctool/include/time_series/common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/4, delete/3]).
-export([incorporate_config/4]).
-export([get_layout/3]).
-export([consume_measurements/4]).
-export([get_slice/5]).


-type id() :: binary().
-export_type([id/0]).

-type time_series_name() :: binary().
-type metric_name() :: binary().
-export_type([time_series_name/0, metric_name/0]).

%% @see tsc_structure
-type structure(MetricKeyType, ValueType) :: #{time_series_name() => #{MetricKeyType => ValueType}}.
-type structure(ValueType) :: structure(metric_name(), ValueType).
-export_type([structure/2, structure/1]).

%% @see tsc_structure
-type layout() :: #{time_series_name() => [metric_name()]}.
-export_type([layout/0]).

-type config() :: structure(metric_config:record()).
-type consume_spec() :: structure(metric_name() | all, [{ts_windows:timestamp_seconds(), ts_windows:value()}]).
-type slice() :: structure(ts_windows:descending_windows_list()).
-export_type([config/0, consume_spec/0, slice/0]).


-type ctx() :: datastore:ctx().
-type batch() :: datastore_doc:batch().


-define(handle_errors(Batch, FunctionArgs, Class, Reason, Stacktrace),
    case {Class, Reason} of
        {_, {fetch_error, not_found}} ->
            erlang:raise(Class, Reason, Stacktrace);
        {throw, {error, _} = Error} ->
            {Error, Batch};
        {throw, {{error, _} = Error, UpdatedDatastoreBatch}} ->
            {Error, UpdatedDatastoreBatch};
        _ ->
            ErrorRef = str_utils:rand_hex(5),
            ?error_stacktrace(
                "[~p:~p] Unexpected error (ref. ~s): ~w:~p~nArgs: ~p",
                [?MODULE, ?FUNCTION_NAME, ErrorRef, Class, Reason, FunctionArgs],
                Stacktrace
            ),
            {?ERROR_UNEXPECTED_ERROR(ErrorRef), Batch}
    end
).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(ctx(), id(), config(), batch()) -> {ok | {error, term()}, batch()}.
create(Ctx, Id, Config, Batch) ->
    try
        sanitize_config(Config),

        DocSplittingStrategies = ts_doc_splitting_strategies:calculate(Config),

        TimeSeriesCollectionHeads = tsc_structure:map(fun(TimeSeriesName, MetricName, MetricConfig) ->
            ts_metric:build(MetricConfig, maps:get({TimeSeriesName, MetricName}, DocSplittingStrategies))
        end, Config),

        case ts_persistence:init_for_new_collection(Ctx, Id, TimeSeriesCollectionHeads, Batch) of
            {{error, already_exists}, UpdatedBatch} ->
                {{error, already_exists}, UpdatedBatch};
            {ok, PersistenceCtx} ->
                {ok, ts_persistence:finalize(PersistenceCtx)}
        end
    catch Class:Reason:Stacktrace ->
        ?handle_errors(Batch, [Id, Config], Class, Reason, Stacktrace)
    end.


-spec delete(ctx(), id(), batch()) -> {ok | {error, term()}, batch()}.
delete(Ctx, Id, Batch) ->
    try
        {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),

        UpdatedPersistenceCtx = tsc_structure:fold(fun(TimeSeriesName, MetricName, _, PersistenceCtxAcc) ->
            UpdatedPersistenceCtxAcc = ts_metric:delete_data_nodes(TimeSeriesName, MetricName, PersistenceCtxAcc),
            ts_metric:delete(TimeSeriesName, MetricName, UpdatedPersistenceCtxAcc)
        end, PersistenceCtx, TimeSeriesCollectionHeads),

        FinalPersistenceCtx = ts_persistence:delete_hub(UpdatedPersistenceCtx),

        {ok, ts_persistence:finalize(FinalPersistenceCtx)}
    catch Class:Reason:Stacktrace ->
        ?handle_errors(Batch, [Id], Class, Reason, Stacktrace)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Ensures that provided config is a part of the collection config, creating
%% missing metrics if required. Will fail if the config to incorporate specifies
%% a different metric config than in the original collection config under the
%% same time series and metric name.
%% @end
%%--------------------------------------------------------------------
-spec incorporate_config(ctx(), id(), config(), batch()) -> {ok | {error, term()}, batch()}.
incorporate_config(Ctx, Id, ConfigToIncorporate, Batch) ->
    try
        sanitize_config(ConfigToIncorporate),

        {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
        PreviousConfig = tsc_structure:map(fun(_, _, #metric{config = Config}) ->
            Config
        end, TimeSeriesCollectionHeads),

        NewConfig = merge_configs(PreviousConfig, ConfigToIncorporate),
        case NewConfig of
            PreviousConfig ->
                {ok, ts_persistence:finalize(PersistenceCtx)};
            _ ->
                NewDocSplittingStrategies = ts_doc_splitting_strategies:calculate(NewConfig),
                FinalPersistenceCtx = reconfigure(PreviousConfig, NewConfig, NewDocSplittingStrategies, PersistenceCtx),
                {ok, ts_persistence:finalize(FinalPersistenceCtx)}
        end
    catch Class:Reason:Stacktrace ->
        ?handle_errors(Batch, [Id, ConfigToIncorporate], Class, Reason, Stacktrace)
    end.


-spec get_layout(ctx(), id(), batch() | undefined) -> {{ok, layout()} | {error, term()}, batch()}.
get_layout(Ctx, Id, Batch) ->
    try
        {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
        Layout = tsc_structure:to_layout(TimeSeriesCollectionHeads),
        {{ok, Layout}, ts_persistence:finalize(PersistenceCtx)}
    catch Class:Reason:Stacktrace ->
        ?handle_errors(Batch, [Id], Class, Reason, Stacktrace)
    end.


-spec consume_measurements(ctx(), id(), consume_spec(), batch()) -> {ok | {error, term()}, batch()}.
consume_measurements(Ctx, Id, ConsumeSpec, Batch) ->
    try
        {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
        ExpandedConsumeSpec = expand_consume_spec(ConsumeSpec, TimeSeriesCollectionHeads),
        RequestLayout = tsc_structure:to_layout(ExpandedConsumeSpec),
        ActualLayout = tsc_structure:to_layout(TimeSeriesCollectionHeads),
        assert_is_sub_layout(ActualLayout, RequestLayout),

        FinalPersistenceCtx = tsc_structure:fold(
            fun ts_metric:consume_measurements/4, PersistenceCtx, ExpandedConsumeSpec
        ),

        {ok, ts_persistence:finalize(FinalPersistenceCtx)}
    catch Class:Reason:Stacktrace ->
        ?handle_errors(Batch, [Id, ConsumeSpec], Class, Reason, Stacktrace)
    end.


-spec get_slice(ctx(), id(), layout(), ts_windows:list_options(), batch() | undefined) ->
    {{ok, slice()} | {error, term()}, batch() | undefined}.
get_slice(Ctx, Id, SliceLayout, ListWindowsOptions, Batch) ->
    try
        {TimeSeriesCollectionHeads, PersistenceCtx} = ts_persistence:init_for_existing_collection(Ctx, Id, Batch),
        ActualLayout = tsc_structure:to_layout(TimeSeriesCollectionHeads),
        assert_is_sub_layout(ActualLayout, SliceLayout),

        {Slice, FinalPersistenceCtx} = tsc_structure:mapfold_from_layout(fun(TimeSeriesName, MetricName, PersistenceCtxAcc) ->
            {_Windows, _UpdatedPersistenceCtxAcc} = ts_metric:list_windows(
                TimeSeriesName, MetricName, ListWindowsOptions, PersistenceCtxAcc
            )
        end, PersistenceCtx, SliceLayout),
        {{ok, Slice}, ts_persistence:finalize(FinalPersistenceCtx)}
    catch Class:Reason:Stacktrace ->
        ?handle_errors(Batch, [Id, SliceLayout, ListWindowsOptions], Class, Reason, Stacktrace)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec sanitize_config(config()) -> ok | no_return().
sanitize_config(Config) ->
    tsc_structure:foreach(fun(TimeSeriesName, MetricName, MetricConfig) ->
        str_utils:validate_name(TimeSeriesName) orelse throw(?ERROR_BAD_VALUE_NAME(<<"timeSeriesName">>)),
        str_utils:validate_name(MetricName) orelse throw(?ERROR_BAD_VALUE_NAME(<<"metricName">>)),

        lists:member(MetricConfig#metric_config.resolution, ?ALLOWED_METRIC_RESOLUTIONS) orelse throw(
            ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"resolution">>, ?ALLOWED_METRIC_RESOLUTIONS)
        ),
        lists:member(MetricConfig#metric_config.aggregator, ?ALLOWED_METRIC_AGGREGATORS) orelse throw(
            ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"aggregator">>, ?ALLOWED_METRIC_AGGREGATORS)
        ),

        case MetricConfig of
            #metric_config{retention = Retention} when Retention =< 0 orelse Retention > ?MAX_METRIC_RETENTION ->
                throw(?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"retention">>, 1, ?MAX_METRIC_RETENTION));
            #metric_config{resolution = 0, retention = Retention} when Retention /= 1 ->
                throw(?ERROR_BAD_DATA(<<"retention">>, <<
                    "Retention must be set to 1 if resolution is set to 0 (infinite window resolution)"
                >>));
            _ ->
                ok
        end
    end, Config).


%% @private
-spec merge_configs(config(), config()) -> config().
merge_configs(First, Second) ->
    tsc_structure:merge_with(fun(TimeSeriesName, MetricName, FirstMetricConfig, SecondMetricConfig) ->
        case FirstMetricConfig =:= SecondMetricConfig of
            true ->
                FirstMetricConfig;
            false ->
                throw(?ERROR_BAD_VALUE_TSC_CONFLICTING_METRIC_CONFIG(
                    TimeSeriesName, MetricName, FirstMetricConfig, SecondMetricConfig
                ))
        end
    end, First, Second).


-spec reconfigure(config(), config(), ts_doc_splitting_strategies:splitting_strategies_map(), ts_persistence:ctx()) ->
    ts_persistence:ctx().
reconfigure(PreviousConfig, NewConfig, DocSplittingStrategies, PersistenceCtx) ->
    tsc_structure:fold(fun(TimeSeriesName, MetricName, MetricConfig, PersistenceCtxAcc) ->
        MetricDocSplittingStrategy = maps:get({TimeSeriesName, MetricName}, DocSplittingStrategies),
        case tsc_structure:has(TimeSeriesName, MetricName, PreviousConfig) of
            true ->
                ts_metric:reconfigure(TimeSeriesName, MetricName, MetricDocSplittingStrategy, PersistenceCtxAcc);
            false ->
                Metric = ts_metric:build(MetricConfig, MetricDocSplittingStrategy),
                ts_metric:insert(TimeSeriesName, MetricName, Metric, PersistenceCtxAcc)
        end
    end, PersistenceCtx, NewConfig).


%% @private
-spec expand_consume_spec(consume_spec(), ts_hub:time_series_collection_heads()) -> consume_spec().
expand_consume_spec(ConsumeSpec, TimeSeriesCollectionHeads) ->
    maps:map(fun(TimeSeriesName, MeasurementsPerMetric) ->
        case MeasurementsPerMetric of
            #{all := Measurements} ->
                TimeSeriesConfig = maps:get(TimeSeriesName, TimeSeriesCollectionHeads, #{}),
                maps:map(fun(_MetricName, _MetricConfig) -> Measurements end, TimeSeriesConfig);
            _ ->
                MeasurementsPerMetric
        end
    end, ConsumeSpec).


%% @private
-spec assert_is_sub_layout(layout(), layout()) -> ok | no_return().
assert_is_sub_layout(ReferenceLayout, AllegedSubLayout) ->
    MissingLayout = maps:filtermap(fun(SubLayoutTimeSeriesName, SubLayoutMetricNames) ->
        case maps:find(SubLayoutTimeSeriesName, ReferenceLayout) of
            error ->
                {true, SubLayoutMetricNames};
            {ok, ReferenceMetricNames} ->
                NewMetricNames = lists_utils:subtract(SubLayoutMetricNames, ReferenceMetricNames),
                case lists_utils:is_empty(NewMetricNames) of
                    true ->
                        false;
                    false ->
                        {true, NewMetricNames}
                end
        end
    end, AllegedSubLayout),
    maps_utils:is_empty(MissingLayout) orelse throw(?ERROR_BAD_VALUE_TSC_LAYOUT(MissingLayout)),
    ok.
