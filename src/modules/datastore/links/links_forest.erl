%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model of forest of datastore document links trees.
%%% It maps tree ID to root nodes.
%%% @end
%%%-------------------------------------------------------------------
-module(links_forest).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).
-export([resolve_conflict/3]).

-type ctx() :: datastore:ctx().
-type doc() :: datastore_doc:doc(record()).
-type record() :: #links_forest{}.
-type id() :: datastore:key().
-type rev() :: datastore_links:link_rev().
-type trees() :: #{links_tree:id() => {links_node:id(), rev()}}.

-export_type([id/0, trees/0]).

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
    #{
        model => ?MODULE,
        memory_driver => undefined,
        disc_driver => undefined
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of model in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {model, atom},
        {key, string},
        {trees, #{string => {string, string}}}
    ]}.
%%--------------------------------------------------------------------
%% @doc
%% Provides custom resolution of remote, concurrent modification conflicts
%% of links forest. Conflict resolution is handled as follows:
%% * if forests are identical operation is ignored
%% * if forests are different for each tree selects root ID with higher
%%   revision. If at least one tree root has been selected from local forest
%%   returns Mutated as 'true' with causes change broadcast, otherwise Mutated
%%   equals 'false'.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(ctx(), doc(), doc()) -> {boolean(), doc()} | ignore.
resolve_conflict(_Ctx, #document{value = Forest}, #document{value = Forest}) ->
    ignore;
resolve_conflict(_Ctx,
    RemoteDoc = #document{value = RemoteForest},
    _LocalDoc = #document{value = LocalForest}
) ->
    #links_forest{trees = RemoteTrees} = RemoteForest,
    #links_forest{trees = LocalTrees} = LocalForest,
    RemoteTreeIds = maps:keys(RemoteTrees),
    LocalTreeIds = maps:keys(LocalTrees),
    TreeIds = lists:usort(RemoteTreeIds ++ LocalTreeIds),
    {Mutated2, MergedTrees2} = lists:foldl(fun(TreeId, {Mutated, MergedTrees}) ->
        RemoteTreeWithRev = maps:get(TreeId, RemoteTrees, undefined),
        LocalTreeWithRev = maps:get(TreeId, LocalTrees, undefined),
        case {RemoteTreeWithRev, LocalTreeWithRev} of
            {undefined, undefined} ->
                {Mutated, MergedTrees};
            {undefined, _} ->
                {true, maps:put(TreeId, LocalTreeWithRev, MergedTrees)};
            {_, undefined} ->
                {Mutated, maps:put(TreeId, RemoteTreeWithRev, MergedTrees)};
            {{_RemoteTree, RemoteRev}, {_LocalTree, LocalRev}} ->
                case datastore_rev:is_greater(RemoteRev, LocalRev) of
                    true ->
                        {Mutated, maps:put(TreeId, RemoteTreeWithRev, MergedTrees)};
                    false ->
                        {true, maps:put(TreeId, LocalTreeWithRev, MergedTrees)}
                end
        end
    end, {false, #{}}, TreeIds),
    Doc = RemoteDoc#document{value = RemoteForest#links_forest{
        trees = MergedTrees2
    }},
    {Mutated2, Doc}.

