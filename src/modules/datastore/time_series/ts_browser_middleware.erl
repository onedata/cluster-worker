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
-module(ts_browser_middleware).
-author("Michal Stanisz").

%% API
-export([data_spec/1]).

-define(MAX_WINDOW_LIMIT, 1000).

-include_lib("ctool/include/errors.hrl").


%%%===================================================================
%%% API
%%%===================================================================

-spec data_spec(json_utils:json_map()) -> middleware_sanitizer:data_spec().
data_spec(#{<<"mode">> := <<"layout">>}) ->
    #{};

data_spec(#{<<"mode">> := <<"slice">>}) -> #{
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
};

data_spec(_) ->
    #{}.
