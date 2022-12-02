%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module operating on time_series_collection metric singe window.
%%% @end
%%%-------------------------------------------------------------------
-module(ts_window).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_time_series.hrl").


%% API
-export([aggregate/3, consolidate_measurement_timestamp/2, to_info/4, info_to_json/1]).


-type timestamp_seconds() :: time_series:time_seconds().
-type value() :: number().
-type window_id() :: timestamp_seconds().
-type aggregated_measurements() :: value() | {ValuesCount :: non_neg_integer(), ValuesSum :: value()}.
-type window() :: #window{}.
-type window_info() :: #window_info{}.
-type measurement() :: {timestamp_seconds(), value()}.

-export_type([timestamp_seconds/0, value/0, window_id/0, aggregated_measurements/0, 
    window/0, window_info/0, measurement/0]).


%%%===================================================================
%%% API
%%%===================================================================

-spec aggregate(window() | undefined, measurement(), metric_config:aggregator()) -> window().
aggregate(undefined, {_NewTimestamp, NewValue}, avg) ->
    #window{aggregated_measurements = {1, NewValue}};

aggregate(undefined, {_NewTimestamp, NewValue}, _) ->
    #window{aggregated_measurements = NewValue};

aggregate(#window{aggregated_measurements = {CurrentCount, CurrentSum}} = CurrentWindow, {_NewTimestamp, NewValue}, avg) ->
    CurrentWindow#window{aggregated_measurements = {CurrentCount + 1, CurrentSum + NewValue}};

aggregate(#window{aggregated_measurements = Aggregated} = CurrentWindow, {_NewTimestamp, NewValue}, sum) ->
    CurrentWindow#window{aggregated_measurements = Aggregated + NewValue};

aggregate(#window{aggregated_measurements = Aggregated} = CurrentWindow, {_NewTimestamp, NewValue}, max) ->
    CurrentWindow#window{aggregated_measurements = max(Aggregated, NewValue)};

aggregate(#window{aggregated_measurements = Aggregated} = CurrentWindow, {_NewTimestamp, NewValue}, min) ->
    CurrentWindow#window{aggregated_measurements = min(Aggregated, NewValue)};

aggregate(#window{last_measurement_timestamp = LastMeasurementTimestamp} = CurrentWindow, {NewTimestamp, NewValue}, last) ->
    case NewTimestamp >= LastMeasurementTimestamp of
        true -> CurrentWindow#window{aggregated_measurements = NewValue};
        _ -> CurrentWindow
    end;

aggregate(#window{first_measurement_timestamp = FirstMeasurementTimestamp} = CurrentWindow, {NewTimestamp, NewValue}, first) ->
    case NewTimestamp < FirstMeasurementTimestamp of
        true -> CurrentWindow#window{aggregated_measurements = NewValue};
        _ -> CurrentWindow
    end.


-spec consolidate_measurement_timestamp(window(), timestamp_seconds()) -> window().
consolidate_measurement_timestamp(#window{
    first_measurement_timestamp = FirstMeasurementTimestamp,
    last_measurement_timestamp = LastMeasurementTimestamp
} = Window, Current) ->
    Window#window{
        first_measurement_timestamp = case FirstMeasurementTimestamp =:= undefined orelse Current < FirstMeasurementTimestamp of
            true -> Current;
            false -> FirstMeasurementTimestamp
        end,
        last_measurement_timestamp = case LastMeasurementTimestamp =:= undefined orelse Current > LastMeasurementTimestamp of
            true -> Current;
            false -> LastMeasurementTimestamp
        end
    }.


-spec to_info(window_id(), window(), basic_info | extended_info, metric_config:aggregator()) -> window_info().
to_info(
    WindowId,
    #window{aggregated_measurements = Measurements},
    basic_info,
    Aggregator
) ->
    #window_info{timestamp = WindowId, value = calculate_window_value(Measurements, Aggregator)};

to_info(
    WindowId,
    #window{
        aggregated_measurements = Measurements,
        first_measurement_timestamp = FirstMeasurementTimestamp,
        last_measurement_timestamp = LastMeasurementTimestamp
    },
    extended_info,
    Aggregator
) ->
    #window_info{
        timestamp = WindowId,
        value = calculate_window_value(Measurements, Aggregator),
        first_measurement_timestamp = FirstMeasurementTimestamp,
        last_measurement_timestamp = LastMeasurementTimestamp
    }.


info_to_json(#window_info{
    timestamp = Timestamp,
    value = Value,
    first_measurement_timestamp = FirstMeasurementTimestamp,
    last_measurement_timestamp = LastMeasurementTimestamp
}) ->
    #{
        <<"timestamp">> => Timestamp,
        <<"value">> => Value,
        <<"firstMeasurementTimestamp">> => FirstMeasurementTimestamp,
        <<"lastMeasurementTimestamp">> => LastMeasurementTimestamp
    }.


%%=====================================================================
%% Internal functions
%%=====================================================================

-spec calculate_window_value(aggregated_measurements(), metric_config:aggregator()) -> value().
calculate_window_value({Count, Sum}, avg) ->
    Sum / Count;

calculate_window_value(Aggregated, _) ->
    Aggregated.