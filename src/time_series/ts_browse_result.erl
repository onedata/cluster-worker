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
-export([to_json_with_compressed_windows/1]).

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
            lists:map(fun({Timestamp, Value}) ->
                #{
                    <<"timestamp">> => Timestamp,
                    <<"value">> => case Value of
                        {Count, Aggregated} -> #{
                            %% @TODO VFS-9589 - introduce average metric aggregator
                            <<"count">> => Count,
                            <<"aggregated">> => Aggregated
                        };
                        Aggregated -> #{
                            <<"aggregated">> => Aggregated
                        }
                    end
                }
            end, Windows)
        end, Slice)
    }.


-spec from_json(json_utils:json_term()) -> record().
from_json(#{<<"layout">> := Layout}) ->
    #time_series_layout_get_result{layout = Layout};

from_json(#{<<"slice">> := SliceJson}) ->
    #time_series_slice_get_result{slice = 
        tsc_structure:map(fun(_TimeSeriesName, _MetricName, WindowsJson) ->
            lists:map(fun(#{<<"timestamp">> := Timestamp, <<"value">> := Value}) ->
                {Timestamp, case Value of
                    #{<<"count">> := Count, <<"aggregated">> := Aggregated} ->
                        {Count, Aggregated};
                    #{<<"aggregated">> := Aggregated} ->
                        Aggregated
                end}
            end, WindowsJson)
        end, SliceJson)
    }.


%%--------------------------------------------------------------------
%% @doc
%% Works like to_json/1, but applies lossy compression on the window values,
%% omitting additional information carried by some aggregators (such as measurement
%% count in case of the sum aggregator).
%% @TODO VFS-9589 - use to_json/1 after average metric aggregator is introduced
%% @end
%%--------------------------------------------------------------------
-spec to_json_with_compressed_windows(record()) -> json_utils:json_term().
to_json_with_compressed_windows(#time_series_layout_get_result{} = TSBrowseResult) ->
    to_json(TSBrowseResult);
to_json_with_compressed_windows(#time_series_slice_get_result{slice = Slice}) ->
    #{
        <<"slice">> => tsc_structure:map(fun(_TimeSeriesName, _MetricName, Windows) ->
            lists:map(fun(Window) ->
                ts_window:info_to_json(Window)
            end, Windows)
        end, Slice)
    }.