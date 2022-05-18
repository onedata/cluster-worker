%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module providing high level functions regarding time series browse request.
%%% @end
%%%-------------------------------------------------------------------
-module(ts_browse_request).
-author("Michal Stanisz").

%% API
-export([from_json/1]).

-include("time_series/browsing.hrl").
-include_lib("ctool/include/errors.hrl").

-define(MAX_WINDOW_LIMIT, 1000).
-define(DEFAULT_WINDOW_LIMIT, 1000).

-type timestamp() :: time:millis().
-type window_limit() :: 1..?MAX_WINDOW_LIMIT.

-type layout_request() :: #time_series_get_layout_request{}.
-type slice_request() :: #time_series_get_slice_request{}.
-type record() :: layout_request() | slice_request().

-export_type([timestamp/0, window_limit/0]).
-export_type([record/0, layout_request/0, slice_request/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec from_json(json_utils:json_map()) -> record().
from_json(Data) when not is_map_key(<<"mode">>, Data) ->
    #time_series_get_layout_request{};

from_json(#{<<"mode">> := <<"layout">>}) ->
    #time_series_get_layout_request{};

from_json(Data = #{<<"mode">> := <<"slice">>}) ->
    DataSpec = #{
        required => #{
            <<"layout">> => {json, fun(RequestedLayout) ->
                try
                    maps:foreach(fun(TimeSeriesName, MetricNames) ->
                        true = is_binary(TimeSeriesName) andalso
                            is_list(MetricNames) andalso
                            lists:all(fun is_binary/1, MetricNames)
                    end, RequestedLayout),
                    true
                catch _:_ ->
                    false
                end
            end}
        },
        optional => #{
            <<"startTimestamp">> => {integer, {not_lower_than, 0}},
            <<"windowLimit">> => {integer, {between, 1, ?MAX_WINDOW_LIMIT}}
        }
    },
    SanitizedData = middleware_sanitizer:sanitize_data(Data, DataSpec),
    
    #time_series_get_slice_request{
        layout = maps:get(<<"layout">>, SanitizedData),
        start_timestamp = maps:get(<<"startTimestamp">>, SanitizedData, undefined),
        window_limit = maps:get(<<"windowLimit">>, SanitizedData, ?DEFAULT_WINDOW_LIMIT)
    };

from_json(#{<<"mode">> := _InvalidMode}) ->
    throw(?ERROR_BAD_VALUE_NOT_ALLOWED(<<"mode">>, [<<"layout">>, <<"slice">>])).
