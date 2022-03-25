%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Types and functions related to the structure of time series collection.
%%% Different aspects of a time series collection (e.g. config, slice, consume spec)
%%% use the same structure of two-level nested maps, but with different types
%%% of values assigned to each metric. This module provides an abstraction for
%%% processing these structures and gathers all the concepts that the time series
%%% collection external API is based on.
%%%
%%% Time series and metrics are identified by their names. A structure
%%% holding values of an arbitrary type 'Type' looks like the following:
%%% #{
%%%    TimeSeries1 => #{
%%%       Metric1 => Value1 :: Type
%%%       Metric2 => Value2 :: Type
%%%    },
%%%    TimeSeries2 => ...
%%% }
%%%
%%% Time series collections also use the concept of layout, which is a list
%%% of metric names per time series name:
%%% #{
%%%    TimeSeries1 => [Metric1, Metric2],
%%%    TimeSeries2 => ...
%%% }
%%% @end
%%%-------------------------------------------------------------------
-module(tsc_structure).
-author("Lukasz Opiola").


%% API
-export([has/3, foreach/2, map/2, fold/3, merge_with/3]).
-export([to_layout/1, build_from_layout/2, mapfold_from_layout/3]).


% merely wrappers for shorter specs
-type time_series_name() :: time_series_collection:time_series_name().
-type metric_name() :: time_series_collection:metric_name().
-type structure(ValueType) :: time_series_collection:structure(ValueType).
-type layout() :: time_series_collection:layout().

%%%===================================================================
%%% API
%%%===================================================================


-spec has(time_series_name(), metric_name(), structure(_)) -> boolean().
has(TimeSeriesName, MetricName, Structure) ->
    kv_utils:is_key([TimeSeriesName, MetricName], Structure).


-spec foreach(fun((time_series_name(), metric_name(), InputType) -> term()), structure(InputType)) ->
    ok.
foreach(ForeachFun, Structure) ->
    maps:foreach(fun(TimeSeriesName, ValuesPerMetric) ->
        maps:foreach(fun(MetricName, ValueForMetric) ->
            ForeachFun(TimeSeriesName, MetricName, ValueForMetric)
        end, ValuesPerMetric)
    end, Structure).


-spec map(fun((time_series_name(), metric_name(), InputType) -> OutputType), structure(InputType)) ->
    structure(OutputType).
map(MapFun, Structure) ->
    maps:map(fun(TimeSeriesName, ValuesPerMetric) ->
        maps:map(fun(MetricName, ValueForMetric) ->
            MapFun(TimeSeriesName, MetricName, ValueForMetric)
        end, ValuesPerMetric)
    end, Structure).


-spec fold(fun((time_series_name(), metric_name(), Type, Acc) -> Acc), Acc, structure(Type)) ->
    Acc.
fold(FoldFun, InitialAcc, Structure) ->
    maps:fold(fun(TimeSeriesName, ValuesPerMetric, OuterAcc) ->
        maps:fold(fun(MetricName, ValueForMetric, InnerAcc) ->
            FoldFun(TimeSeriesName, MetricName, ValueForMetric, InnerAcc)
        end, OuterAcc, ValuesPerMetric)
    end, InitialAcc, Structure).


-spec merge_with(fun((time_series_name(), metric_name(), Type, Type) -> Type), structure(Type), structure(Type)) ->
    structure(Type).
merge_with(MergeFun, First, Second) ->
    maps:merge_with(fun(TimeSeriesName, FirstValuesPerMetric, SecondValuesPerMetric) ->
        maps:merge_with(fun(MetricName, FirstValueForMetric, SecondValueForMetric) ->
            MergeFun(TimeSeriesName, MetricName, FirstValueForMetric, SecondValueForMetric)
        end, FirstValuesPerMetric, SecondValuesPerMetric)
    end, First, Second).


-spec to_layout(structure(_)) -> layout().
to_layout(Structure) ->
    maps:map(fun(_TimeSeriesName, ValuesPerMetric) ->
        maps:keys(ValuesPerMetric)
    end, Structure).


-spec build_from_layout(fun((time_series_name(), metric_name()) -> Type), layout()) -> structure(Type).
build_from_layout(BuildFun, Structure) ->
    maps:map(fun(TimeSeriesName, MetricNames) ->
        maps_utils:generate_from_list(fun(MetricName) ->
            {MetricName, BuildFun(TimeSeriesName, MetricName)}
        end, MetricNames)
    end, Structure).


-spec mapfold_from_layout(fun((time_series_name(), metric_name(), Acc) -> {Type, Acc}), Acc, layout()) ->
    {structure(Type), Acc}.
mapfold_from_layout(MapFoldFun, InitialAcc, Layout) ->
    maps:fold(fun(TimeSeriesName, MetricNames, {OuterMapAcc, OuterFoldAcc}) ->
        {InnerMapResult, InnerFoldResult} = lists:foldl(fun(MetricName, {InnerMapAcc, InnerFoldAcc}) ->
            {MappedValue, UpdatedInnerFoldAcc} = MapFoldFun(TimeSeriesName, MetricName, InnerFoldAcc),
            {InnerMapAcc#{MetricName => MappedValue}, UpdatedInnerFoldAcc}
        end, {#{}, OuterFoldAcc}, MetricNames),
        {OuterMapAcc#{TimeSeriesName => InnerMapResult}, InnerFoldResult}
    end, {#{}, InitialAcc}, Layout).