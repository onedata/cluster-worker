%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module providing high level functions regarding time series browse result.
%%% @end
%%%-------------------------------------------------------------------
-module(ts_browse_result).
-author("Michal Stanisz").

-behaviour(jsonable_record).

%% API
-export([to_json/1, from_json/1]).

-include("time_series/browsing.hrl").

-type layout_result() :: #time_series_layout_get_result{}.
-type slice_result() :: #time_series_slice_get_result{}.
-type record() :: layout_result() | slice_result().

-export_type([record/0, layout_result/0, slice_result/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec to_json(record()) -> json_utils:json_term().
to_json(#time_series_layout_get_result{layout = Layout}) ->
    #{<<"layout">> => Layout};

to_json(#time_series_slice_get_result{slice = Slice}) ->
    #{
        <<"slice">> => tsc_structure:map(fun(_TimeSeriesName, _MetricName, Windows) ->
            lists:map(fun ts_window:info_to_json/1, Windows)
        end, Slice)
    }.


-spec from_json(json_utils:json_term()) -> record().
from_json(#{<<"layout">> := Layout}) ->
    #time_series_layout_get_result{layout = Layout};

from_json(#{<<"slice">> := SliceJson}) ->
    #time_series_slice_get_result{slice = 
        tsc_structure:map(fun(_TimeSeriesName, _MetricName, WindowsJson) ->
            lists:map(fun ts_window:json_to_info/1, WindowsJson)
        end, SliceJson)
    }.