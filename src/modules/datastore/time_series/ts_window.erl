%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module operating on time_series_collection metric single window.
%%% @end
%%%-------------------------------------------------------------------
-module(ts_window).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_time_series.hrl").


%% API
-export([new/2, consume_measurement/3, db_encode/1, db_decode/1, to_info/4, info_to_json/1, json_to_info/1]).


-type timestamp_seconds() :: time_series:time_seconds().
-type id() :: timestamp_seconds().
-type value() :: number().
-type aggregated_measurements() :: value() | {ValuesCount :: non_neg_integer(), ValuesSum :: value()}.
-type record() :: #window{}.
-type info() :: #window_info{}.
-type measurement() :: {timestamp_seconds(), value()}.

-export_type([timestamp_seconds/0, value/0, id/0, aggregated_measurements/0, record/0, info/0, measurement/0]).


%%%===================================================================
%%% API
%%%===================================================================

-spec new(measurement(), metric_config:aggregator()) -> record().
new({Timestamp, Value}, avg) ->
    #window{
        aggregated_measurements = {1, Value},
        first_measurement_timestamp = Timestamp,
        last_measurement_timestamp = Timestamp
    };

new({Timestamp, Value}, _) ->
    #window{
        aggregated_measurements = Value,
        first_measurement_timestamp = Timestamp,
        last_measurement_timestamp = Timestamp
    }.


-spec consume_measurement(record(), measurement(), metric_config:aggregator()) -> record().
consume_measurement(#window{
    first_measurement_timestamp = FirstMeasurementTimestamp,
    last_measurement_timestamp = LastMeasurementTimestamp
} = Window, {MeasurementTimestamp, _} = Measurement, Aggregator) ->
    UpdatedWindow = aggregate_measurement(Window, Measurement, Aggregator),
    UpdatedWindow#window{
        first_measurement_timestamp = case MeasurementTimestamp < FirstMeasurementTimestamp of
            true -> MeasurementTimestamp;
            false -> FirstMeasurementTimestamp
        end,
        last_measurement_timestamp = case MeasurementTimestamp > LastMeasurementTimestamp of
            true -> MeasurementTimestamp;
            false -> LastMeasurementTimestamp
        end
    }.


-spec db_encode(record()) -> list().
db_encode(#window{
    aggregated_measurements = AggregatedMeasurements,
    first_measurement_timestamp = FirstMeasurementTimestamp,
    last_measurement_timestamp = LastMeasurementTimestamp
}) ->
    [FirstMeasurementTimestamp, LastMeasurementTimestamp | aggregated_measurements_to_json(AggregatedMeasurements)].


-spec db_decode(list()) -> record().
% Old window format
% NOTE: Decoder for old format is only to prevent decoding process from crushing.
%       Such windows will be pruned during document update process.
db_decode([ValuesCount, ValuesSum]) ->
    #window{
        aggregated_measurements = {ValuesCount, ValuesSum},
        first_measurement_timestamp = 0,
        last_measurement_timestamp = 0
    };
db_decode([Value]) ->
    #window{
        aggregated_measurements = Value,
        first_measurement_timestamp = 0,
        last_measurement_timestamp = 0
    };

% New window format
db_decode([FirstMeasurementTimestamp, LastMeasurementTimestamp | AggregatedMeasurements]) ->
    #window{
        aggregated_measurements = aggregated_measurements_from_json(AggregatedMeasurements),
        first_measurement_timestamp = FirstMeasurementTimestamp,
        last_measurement_timestamp = LastMeasurementTimestamp
    }.


-spec to_info(id(), record(), metric_config:aggregator(), basic | extended) -> info().
to_info(WindowId, Window, Aggregator, basic) ->
    #window_info{
        timestamp = WindowId,
        value = aggregated_measurements_to_value(Window#window.aggregated_measurements, Aggregator)
    };
to_info(WindowId, Window, Aggregator, extended) ->
    #window_info{
        timestamp = WindowId,
        value = aggregated_measurements_to_value(Window#window.aggregated_measurements, Aggregator),
        first_measurement_timestamp = Window#window.first_measurement_timestamp,
        last_measurement_timestamp = Window#window.last_measurement_timestamp
    }.


-spec info_to_json(info()) -> json_utils:json_term().
info_to_json(#window_info{
    timestamp = Timestamp,
    value = Value,
    first_measurement_timestamp = FirstMeasurementTimestamp,
    last_measurement_timestamp = LastMeasurementTimestamp
}) ->
    Data0 = #{
        <<"timestamp">> => Timestamp,
        <<"value">> => Value
    },
    Data1 = maps_utils:put_if_defined(Data0, <<"firstMeasurementTimestamp">>, FirstMeasurementTimestamp),
    maps_utils:put_if_defined(Data1, <<"lastMeasurementTimestamp">>, LastMeasurementTimestamp).


-spec json_to_info(json_utils:json_term()) -> info().
json_to_info(JsonData) ->
    #window_info{
        timestamp = maps:get(<<"timestamp">>, JsonData),
        value = maps:get(<<"value">>, JsonData),
        first_measurement_timestamp = utils:null_to_undefined(maps:get(<<"firstMeasurementTimestamp">>, JsonData, null)),
        last_measurement_timestamp = utils:null_to_undefined(maps:get(<<"lastMeasurementTimestamp">>, JsonData, null))
    }.


%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec aggregate_measurement(record(), measurement(), metric_config:aggregator()) -> record().
aggregate_measurement(#window{aggregated_measurements = {CurrentCount, CurrentSum}} = CurrentWindow, {_NewTimestamp, NewValue}, avg) ->
    CurrentWindow#window{aggregated_measurements = {CurrentCount + 1, CurrentSum + NewValue}};

aggregate_measurement(#window{aggregated_measurements = Aggregated} = CurrentWindow, {_NewTimestamp, NewValue}, sum) ->
    CurrentWindow#window{aggregated_measurements = Aggregated + NewValue};

aggregate_measurement(#window{aggregated_measurements = Aggregated} = CurrentWindow, {_NewTimestamp, NewValue}, max) ->
    CurrentWindow#window{aggregated_measurements = max(Aggregated, NewValue)};

aggregate_measurement(#window{aggregated_measurements = Aggregated} = CurrentWindow, {_NewTimestamp, NewValue}, min) ->
    CurrentWindow#window{aggregated_measurements = min(Aggregated, NewValue)};

aggregate_measurement(#window{last_measurement_timestamp = LastMeasurementTimestamp} = CurrentWindow, {NewTimestamp, NewValue}, last) ->
    case NewTimestamp >= LastMeasurementTimestamp of
        true -> CurrentWindow#window{aggregated_measurements = NewValue};
        _ -> CurrentWindow
    end;

aggregate_measurement(#window{first_measurement_timestamp = FirstMeasurementTimestamp} = CurrentWindow, {NewTimestamp, NewValue}, first) ->
    case NewTimestamp < FirstMeasurementTimestamp of
        true -> CurrentWindow#window{aggregated_measurements = NewValue};
        _ -> CurrentWindow
    end.


%% @private
-spec aggregated_measurements_to_value(aggregated_measurements(), metric_config:aggregator()) -> value().
aggregated_measurements_to_value({Count, Sum}, avg) ->
    Sum / Count;

aggregated_measurements_to_value(Aggregated, _) ->
    Aggregated.


%% @private
-spec aggregated_measurements_to_json(aggregated_measurements()) -> list().
aggregated_measurements_to_json({ValuesCount, ValuesSum}) ->
    [ValuesCount, ValuesSum];

aggregated_measurements_to_json(Value) ->
    [Value].


%% @private
-spec aggregated_measurements_from_json(list()) -> aggregated_measurements().
aggregated_measurements_from_json([ValuesCount, ValuesSum]) ->
    {ValuesCount, ValuesSum};

aggregated_measurements_from_json([Value]) ->
    Value.