%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module to histogram_persistence operating on histogram
%%% hub node that stores heads of each metric's #data{} records linked list
%%% (see histogram_persistence module).
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_hub).
-author("Michal Wrzeszcz").

-include("modules/datastore/metric_config.hrl").

%% API
-export([set_time_series_pack/1, get_time_series_pack/1]).
%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-record(histogram_hub, {
    time_series_pack :: histogram_time_series:time_series_pack()
}).

-type record() :: #histogram_hub{}.

% Context used only by datastore to initialize internal structure's.
% Context provided via histogram_time_series module functions
% overrides it in other cases.
-define(CTX, #{
    model => ?MODULE,
    memory_driver => undefined,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec set_time_series_pack(histogram_time_series:time_series_pack()) -> record().
set_time_series_pack(TimeSeriesPack) ->
    #histogram_hub{time_series_pack = TimeSeriesPack}.

-spec get_time_series_pack(record()) -> histogram_time_series:time_series_pack().
get_time_series_pack(#histogram_hub{time_series_pack = TimeSeriesPack}) ->
    TimeSeriesPack.

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
    {record, [DataRecordStruct]} = histogram_metric_data:get_record_struct(1),
    {record, [
        {time_series, #{string => #{string => {record, [
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