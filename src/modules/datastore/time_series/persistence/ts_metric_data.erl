%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module to ts_persistence operating on time series
%%% metric data node. Each time series metric data node is connected with
%%% singe metric. Values that exceed time series hub capacity for particular
%%% metric are stored in list of time series metric data nodes
%%% (capacity of single time series metric data node is also limited so more
%%% than one time series metric data node may be needed - see ts_persistence module).
%%% @end
%%%-------------------------------------------------------------------
-module(ts_metric_data).
-author("Michal Wrzeszcz").

%% API
-export([set/1, get/1]).
%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-record(ts_metric_data, {
    value :: ts_metric:data()
}).

-type record() :: #ts_metric_data{}.
-type key() :: datastore:key().

-export_type([key/0]).

% Context used only by datastore to initialize internal structures.
% Context provided via time_series module functions
% overrides it in other cases.
-define(CTX, #{
    model => ?MODULE,
    memory_driver => undefined,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec set(ts_metric:data()) -> record().
set(Data) ->
    #ts_metric_data{value = Data}.

-spec get(record()) -> ts_metric:data().
get(#ts_metric_data{value = Data}) ->
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
        {value, {record, [
            {windows, {custom, json, {ts_windows, encode, decode}}},
            {prev_record, string},
            {prev_record_timestamp, integer}
        ]}}
    ]}.