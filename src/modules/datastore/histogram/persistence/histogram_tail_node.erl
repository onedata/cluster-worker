%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module to histogram_persistence operating on histogram
%%% hub node that stores singe metrics values that exceeds
%%% histogram hub capacity.
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

-type histogram_node() :: #histogram_tail_node{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec set_data(histogram_api:data()) -> histogram_node().
set_data(Data) ->
    #histogram_tail_node{data = Data}.

-spec get_data(histogram_node()) -> histogram_api:data().
get_data(#histogram_tail_node{data = Data}) ->
    Data.

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
    {record, [
        {data, {record, [
            {windows, {custom, json, {histogram_windows, encode, decode}}},
            {prev_record, string},
            {prev_record_timestamp, integer}
        ]}}
    ]}.