%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module to histogram_persistence operating on histogram
%%% metric data node. Each histogram metric data node is connected with
%%% singe metric. Values that exceeds histogram hub capacity for particular
%%% metric are stored in list of histogram metric data nodes
%%% (capacity of single histogram metric data node is also limited so more
%%% than one histogram metric data node may be needed - see
%%% histogram_persistence module).
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_metric_data).
-author("Michal Wrzeszcz").

%% API
-export([set_data/1, get_data/1]).
%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-record(histogram_metric_data, {
    data :: histogram_metric:data()
}).

-type record() :: #histogram_metric_data{}.

% Context used only by datastore to initialize internal structures.
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

-spec set_data(histogram_metric:data()) -> record().
set_data(Data) ->
    #histogram_metric_data{data = Data}.

-spec get_data(record()) -> histogram_metric:data().
get_data(#histogram_metric_data{data = Data}) ->
    Data.

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
    {record, [
        {data, {record, [
            {windows, {custom, json, {histogram_windows, encode, decode}}},
            {prev_record, string},
            {prev_record_timestamp, integer}
        ]}}
    ]}.