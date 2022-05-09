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
-module(ts_browse_result).
-author("Michal Stanisz").

%% API
-export([to_json/1]).

-include("middleware/ts_browser.hrl").
-include_lib("ctool/include/errors.hrl").

-type layout_res() :: #time_series_layout_result{}.
-type slice_res() :: #time_series_slice_result{}.
-type res() :: layout_res() | slice_res().

-export_type([res/0, layout_res/0, slice_res/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec to_json(res()) -> json_utils:json_term().
to_json(#time_series_layout_result{layout = Layout}) ->
    Layout;

to_json(#time_series_slice_result{slice = Slice}) ->
    #{
        <<"windows">> => tsc_structure:map(fun(_TimeSeriesName, _MetricName, Windows) ->
            lists:map(fun({Timestamp, ValuesSum}) ->
                #{
                    <<"timestamp">> => Timestamp,
                    <<"value">> => ValuesSum
                }
            end, Windows)
        end, Slice)
    }.
