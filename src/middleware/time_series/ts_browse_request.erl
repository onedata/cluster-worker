%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module providing time series browser middleware.
%%% @end
%%%-------------------------------------------------------------------
-module(ts_browse_request).
-author("Michal Stanisz").

%% API
-export([sanitize/1]).

-include("middleware/ts_browser.hrl").
-include_lib("ctool/include/errors.hrl").

-define(MAX_WINDOW_LIMIT, 1000).
-define(DEFAULT_WINDOW_LIMIT, 1000).

-type timestamp() :: time:millis().
-type window_limit() :: 1..?MAX_WINDOW_LIMIT.

-type layout_req() :: #time_series_get_layout_req{}.
-type slice_req() :: #time_series_get_slice_req{}.
-type req() :: layout_req() | slice_req().

-export_type([timestamp/0, window_limit/0]).
-export_type([req/0, layout_req/0, slice_req/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec sanitize(json_utils:json_map()) -> req().
sanitize(Data) when not is_map_key(<<"mode">>, Data) ->
    #time_series_get_layout_req{};

sanitize(#{<<"mode">> := <<"layout">>}) ->
    #time_series_get_layout_req{};

sanitize(Data = #{<<"mode">> := <<"slice">>}) ->
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
    
    #time_series_get_slice_req{
        layout = maps:get(<<"layout">>, SanitizedData),
        start_timestamp = maps:get(<<"startTimestamp">>, SanitizedData, undefined),
        window_limit = maps:get(<<"windowLimit">>, SanitizedData, ?DEFAULT_WINDOW_LIMIT)
    };

sanitize(#{<<"mode">> := _InvalidMode}) ->
    throw(?ERROR_BAD_VALUE_NOT_ALLOWED(<<"mode">>, [<<"layout">>, <<"slice">>])).
