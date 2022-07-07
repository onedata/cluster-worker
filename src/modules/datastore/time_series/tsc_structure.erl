%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utilities operating on the time series collection structure.
%%% @end
%%%-------------------------------------------------------------------
-module(tsc_structure).
-author("Lukasz Opiola").


%% API
-export([has/3, foreach/2, map/2, fold/3, merge_with/3]).
-export([to_layout/1, build_from_layout/2, buildfold_from_layout/3, subtract_layout/2]).


% merely wrappers for shorter specs
-type structure(ValueType) :: time_series_collection:structure(ValueType).
-type layout() :: time_series_collection:layout().


%%%===================================================================
%%% API
%%%===================================================================


-spec has(time_series:name(), time_series:metric_name(), structure(_)) -> boolean().
has(TimeSeriesName, MetricName, Structure) ->
    kv_utils:is_key([TimeSeriesName, MetricName], Structure).


-spec foreach(fun((time_series:name(), time_series:metric_name(), InputType) -> term()), structure(InputType)) ->
    ok.
foreach(ForeachFun, Structure) ->
    maps:foreach(fun(TimeSeriesName, ValuesPerMetric) ->
        maps:foreach(fun(MetricName, ValueForMetric) ->
            ForeachFun(TimeSeriesName, MetricName, ValueForMetric)
        end, ValuesPerMetric)
    end, Structure).


-spec map(fun((time_series:name(), time_series:metric_name(), InputType) -> OutputType), structure(InputType)) ->
    structure(OutputType).
map(MapFun, Structure) ->
    maps:map(fun(TimeSeriesName, ValuesPerMetric) ->
        maps:map(fun(MetricName, ValueForMetric) ->
            MapFun(TimeSeriesName, MetricName, ValueForMetric)
        end, ValuesPerMetric)
    end, Structure).


-spec fold(fun((time_series:name(), time_series:metric_name(), Type, Acc) -> Acc), Acc, structure(Type)) ->
    Acc.
fold(FoldFun, InitialAcc, Structure) ->
    maps:fold(fun(TimeSeriesName, ValuesPerMetric, OuterAcc) ->
        maps:fold(fun(MetricName, ValueForMetric, InnerAcc) ->
            FoldFun(TimeSeriesName, MetricName, ValueForMetric, InnerAcc)
        end, OuterAcc, ValuesPerMetric)
    end, InitialAcc, Structure).


-spec merge_with(fun((time_series:name(), time_series:metric_name(), Type, Type) -> Type), structure(Type), structure(Type)) ->
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


-spec build_from_layout(fun((time_series:name(), time_series:metric_name()) -> Type), layout()) -> structure(Type).
build_from_layout(BuildFun, Structure) ->
    maps:map(fun(TimeSeriesName, MetricNames) ->
        maps_utils:generate_from_list(fun(MetricName) ->
            {MetricName, BuildFun(TimeSeriesName, MetricName)}
        end, MetricNames)
    end, Structure).


-spec buildfold_from_layout(fun((time_series:name(), time_series:metric_name(), Acc) -> {Type, Acc}), Acc, layout()) ->
    {structure(Type), Acc}.
buildfold_from_layout(BuildFoldFun, InitialFoldAcc, Layout) ->
    maps:fold(fun(TimeSeriesName, MetricNames, {StructureAcc, OuterFoldAcc}) ->
        {InnerMapResult, InnerFoldResult} = lists:foldl(fun(MetricName, {PerMetricAcc, InnerFoldAcc}) ->
            {MappedValue, UpdatedInnerFoldAcc} = BuildFoldFun(TimeSeriesName, MetricName, InnerFoldAcc),
            {PerMetricAcc#{MetricName => MappedValue}, UpdatedInnerFoldAcc}
        end, {#{}, OuterFoldAcc}, MetricNames),
        {StructureAcc#{TimeSeriesName => InnerMapResult}, InnerFoldResult}
    end, {#{}, InitialFoldAcc}, Layout).


-spec subtract_layout(layout(), layout()) -> layout().
subtract_layout(Minuend, Subtrahend) ->
    maps:filtermap(fun(MinuendTimeSeriesName, MinuendMetricNames) ->
        case maps:find(MinuendTimeSeriesName, Subtrahend) of
            error ->
                {true, MinuendMetricNames};
            {ok, SubtrahendMetricNames} ->
                case lists_utils:subtract(MinuendMetricNames, SubtrahendMetricNames) of
                    [] ->
                        false;
                    MetricNamesDifference ->
                        {true, MetricNamesDifference}
                end
        end
    end, Minuend).
