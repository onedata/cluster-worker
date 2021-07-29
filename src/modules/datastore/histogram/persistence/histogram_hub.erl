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
-export([set_time_series/1, get_time_series/1]).
%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-record(histogram_hub, {
    time_series :: histogram_api:time_series_map()
}).

-type histogram_node() :: #histogram_hub{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec set_time_series(histogram_api:time_series_map()) -> histogram_node().
set_time_series(TimeSeries) ->
    #histogram_hub{time_series = TimeSeries}.

-spec get_time_series(histogram_node()) -> histogram_api:time_series_map().
get_time_series(#histogram_hub{time_series = TimeSeries}) ->
    TimeSeries.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    #{
        model => ?MODULE,
        memory_driver => undefined,
        disc_driver => undefined
    }.

-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [DataRecordStruct]} = histogram_tail_node:get_record_struct(1),
    {record, [
        {time_series, #{string => #{string => {record, [
            {config, {record, [
                {legend, binary},
                {window_size, integer},
                {max_windows_count, integer},
                {apply_function, atom}
            ]}},
            {doc_splitting_strategy, {record, [
                {max_docs_count, integer},
                {max_windows_in_head_doc, integer},
                {max_windows_in_tail_doc, integer}
            ]}},
            DataRecordStruct
        ]}}}}
    ]}.