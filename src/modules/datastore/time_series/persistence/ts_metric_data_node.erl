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
%%% metric (ie. metric's head capacity) are stored in list of time series metric
%%% data nodes (capacity of single time series metric data node is also limited so more
%%% than one time series metric data node may be needed - see ts_persistence module).
%%% @end
%%%-------------------------------------------------------------------
-module(ts_metric_data_node).
-author("Michal Wrzeszcz").

%% API
-export([set/1, get/1]).
%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, upgrade_record/2, get_record_struct/1]).

-record(ts_metric_data_node, {
    value :: ts_metric:data_node()
}).

-type record() :: #ts_metric_data_node{}.
-type key() :: datastore:key().

-export_type([key/0]).

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

-spec set(ts_metric:data_node()) -> record().
set(Data) ->
    #ts_metric_data_node{value = Data}.

-spec get(record()) -> ts_metric:data_node().
get(#ts_metric_data_node{value = Data}) ->
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


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.


-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, DataNode}) ->
    % Update is not needed as versions differ in encoding/decoding method (fields are the same)
    {2, {?MODULE, DataNode}}.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {value, {record, [
            {windows, {custom, json, {ts_windows, db_encode, db_decode}}},
            {older_node_key, string},
            {older_node_timestamp, integer}
        ]}}
    ]};
get_record_struct(2) ->
    {record, [
        {value, {record, [
            % Change custom type to string to prevent encoding field twice
            {windows, {custom, string, {ts_windows, db_encode, db_decode}}},
            {older_node_key, string},
            {older_node_timestamp, integer}
        ]}}
    ]}.