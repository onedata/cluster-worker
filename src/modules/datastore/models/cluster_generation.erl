%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model that holds information about current cluster generation.
%%% @end
%%%-------------------------------------------------------------------
-module(cluster_generation).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([get/0, save/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type ctx() :: datastore:ctx().
-type record() :: #cluster_generation{}.
-type doc() :: datastore_doc:doc(record()).

-define(CTX, #{
    model => ?MODULE
}).

-define(KEY, <<"cluster_generation">>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns current cluster generation.
%% @end
%%--------------------------------------------------------------------
-spec get() -> {ok, node_manager:cluster_generation()} | {error, term()}.
get() ->
    case datastore_model:get(?CTX, ?KEY) of
        {ok, #document{value = #cluster_generation{generation = Generation}}} ->
            {ok, Generation};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves node management data.
%% @end
%%--------------------------------------------------------------------
-spec save(node_manager:cluster_generation()) -> {ok, doc()} | {error, term()}.
save(ClusterGeneration) ->
    datastore_model:save(?CTX, #document{
        key = ?KEY,
        value = #cluster_generation{generation = ClusterGeneration}
    }).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of model in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {generation, integer}
    ]}.
