%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module to histogram_persistence operating on histogram
%%% tail node. Each histogram tail node is connected with singe metric.
%%% Values that exceeds histogram hub capacity for particular metric
%%% are stored in list of histogram tail nodes
%%% (capacity of single histogram tail node is also limited so more
%%% than one histogram tail node may be needed).
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_tail_node).
-author("Michal Wrzeszcz").

%% API
-export([set_data/1, get_data/1]).
%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-record(histogram_tail_node, {
    data :: histogram_api:data()
}).

-type record() :: #histogram_tail_node{}.

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

-spec set_data(histogram_api:data()) -> record().
set_data(Data) ->
    #histogram_tail_node{data = Data}.

-spec get_data(record()) -> histogram_api:data().
get_data(#histogram_tail_node{data = Data}) ->
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