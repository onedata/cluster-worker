%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module to ts_persistence operating on time series hub node
%%% that stores heads of each metric's #data_node{} records linked list
%%% (see ts_persistence module).
%%% @end
%%%-------------------------------------------------------------------
-module(ts_hub).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/time_series/common.hrl").

%% API
-export([set_time_series_collection_heads/1, get_time_series_collection_heads/1]).
%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-record(ts_hub, {
    time_series_collection_heads :: time_series_collection_heads()
}).

-type record() :: #ts_hub{}.
-type time_series_heads() :: #{ts_metric:id() => ts_metric:metric()}.
-type time_series_collection_heads() :: #{time_series_collection:time_series_id() => time_series_heads()}.

-export_type([time_series_heads/0, time_series_collection_heads/0]).

% Context used only by datastore to initialize internal structures.
% Context provided via time_series_collection module functions
% overrides it in other cases.
-define(CTX, #{
    model => ?MODULE,
    memory_driver => undefined,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec set_time_series_collection_heads(time_series_collection_heads()) -> record().
set_time_series_collection_heads(TimeSeriesCollectionHeads) ->
    #ts_hub{time_series_collection_heads = TimeSeriesCollectionHeads}.

-spec get_time_series_collection_heads(record()) -> time_series_collection_heads().
get_time_series_collection_heads(#ts_hub{time_series_collection_heads = TimeSeriesCollectionHeads}) ->
    TimeSeriesCollectionHeads.

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
    {record, [DataRecordStruct]} = ts_metric_data_node:get_record_struct(1),
    {record, [
        {time_series_collection_heads, #{string => #{string => {record, [
            {config, {record, [
                {legend, binary},
                {resolution, integer},
                {retention, integer},
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