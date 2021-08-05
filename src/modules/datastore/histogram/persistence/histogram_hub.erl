%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module to histogram_persistence operating on histogram
%%% hub node that stores beginnings of each metrics.
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_hub).
-author("Michal Wrzeszcz").

-include("modules/datastore/histogram.hrl").

%% API
-export([set_time_series_map/1, get_time_series_map/1]).
%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

% TODO - uporzadkowac slownictow head/hub
-record(histogram_hub, {
    time_series_map :: histogram_api:time_series_map()
}).

-type record() :: #histogram_hub{}.

% Context used only by datastore to initialize internal structure's.
% Context provided via histogram_api module functions is used to get/save
% document instead this one.
-define(CTX, #{
    model => ?MODULE,
    memory_driver => undefined,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec set_time_series_map(histogram_api:time_series_map()) -> record().
set_time_series_map(TimeSeriesMap) ->
    #histogram_hub{time_series_map = TimeSeriesMap}.

-spec get_time_series_map(record()) -> histogram_api:time_series_map().
get_time_series_map(#histogram_hub{time_series_map = TimeSeriesMap}) ->
    TimeSeriesMap.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context needed to initialize internal structure's
%% (it is not used to get or save document).
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [DataRecordStruct]} = histogram_tail_node:get_record_struct(1),
    {record, [
        {time_series, #{string => #{string => {record, [
            {config, {record, [
                {legend, binary},
                {window_timespan, integer},
                {max_windows_count, integer},
                {aggregator, atom}
            ]}},
            {splitting_strategy, {record, [
                {max_docs_count, integer},
                {max_windows_in_head_doc, integer},
                {max_windows_in_tail_doc, integer}
            ]}},
            DataRecordStruct
        ]}}}}
    ]}.