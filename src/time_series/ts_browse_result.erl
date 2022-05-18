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

%% API
-export([to_json/1]).

-include("time_series/browsing.hrl").
-include_lib("ctool/include/errors.hrl").

-type layout_result() :: #time_series_layout_result{}.
-type slice_result() :: #time_series_slice_result{}.
-type record() :: layout_result() | slice_result().

-export_type([record/0, layout_result/0, slice_result/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec to_json(record()) -> json_utils:json_term().
to_json(#time_series_layout_result{layout = Layout}) ->
    Layout;

to_json(#time_series_slice_result{slice = Slice}) ->
    #{
        <<"windows">> => tsc_structure:map(fun(_TimeSeriesName, _MetricName, Windows) ->
            lists:map(fun({Timestamp, Value}) ->
                #{
                    <<"timestamp">> => Timestamp,
                    <<"value">> => case Value of
                        {_ValuesCount, ValuesSum} -> ValuesSum;
                        _ -> Value
                    end
                }
            end, Windows)
        end, Slice)
    }.
