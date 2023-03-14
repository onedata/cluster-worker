%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides CRUD functionality for datastore links.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_links_crud).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_links.hrl").

%% API
-export([add/2, get/2, delete/2]).
-export([apply/5]).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type cached_keys() :: datastore_doc_batch:cached_keys().
-type tree() :: datastore_links:tree().
-type tree_id() :: datastore_links:tree_id().
-type link() :: datastore_links:link().
-type link_name() :: datastore_links:link_name().
-type link_target() :: datastore_links:link_target().
-type link_rev() :: datastore_links:link_rev().
-type remove_pred() :: bp_tree:remove_pred().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates named link between a document and a target.
%% @end
%%--------------------------------------------------------------------
-spec add([{link_name(), {link_target(), link_rev()}}], tree()) ->
    {{ok, [link_name()]} | {error, term()}, tree()}.
add(Items, Tree) ->
    case bp_tree:insert(Items, Tree) of
        {ok, AddedKeys, Tree2} ->
            {{ok, AddedKeys}, Tree2};
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns document link by name.
%% @end
%%--------------------------------------------------------------------
-spec get(link_name(), tree()) -> {{ok, link()} | {error, term()}, tree()}.
get(LinkName, Tree) ->
    case bp_tree:find(LinkName, Tree) of
        {{ok, {LinkTarget, LinkRev}}, Tree2} ->
            {{ok, #link{
                tree_id = datastore_links:get_tree_id(Tree),
                name = LinkName,
                target = LinkTarget,
                rev = LinkRev
            }}, Tree2};
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes named link between a document and a target in provided revision.
%% @end
%%--------------------------------------------------------------------
-spec delete([{link_name(), remove_pred()}], tree()) ->
    {{ok, [link_name()]} | {error, term()}, tree()}.
delete([{FirstLink, _} | _] = Items, Tree) ->
    case bp_tree:remove(Items, Tree) of
        {ok, RemovedKeys, Tree2} ->
            {{ok, RemovedKeys}, Tree2};
        {{error, Reason}, Tree2} when
            Reason == not_found;
            Reason == predicate_not_satisfied ->
            {{ok, [FirstLink]}, Tree2};
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% This is a convenient overload used in tests only. It initializes datastore
%% documents batch and links tree, applies one of add, get, delete functions
%% and terminates the thee and the batch. Along with a result returns also
%% cached keys.
%% @end
%%--------------------------------------------------------------------
-spec apply(ctx(), key(), tree_id(), atom(), list()) ->
    {ok | {ok, link()} | {error, term()}, cached_keys()}.
apply(Ctx, Key, TreeId, Function, Args) ->
    Ref = make_ref(),
    Batch = datastore_doc_batch:init(),
    Batch2 = datastore_doc_batch:init_request(Ref, Batch),
    {Result2, Batch5} = case datastore_links:init_tree(Ctx, Key, TreeId, Batch2) of
        {ok, Tree} ->
            {Result, Tree2} = erlang:apply(?MODULE, Function, Args ++ [Tree]),
            Batch3 = datastore_links:finalize_tree_operation(Tree2),
            case Result of
                {error, Reason} ->
                    {{error, Reason}, Batch3};
                _ ->
                    Batch4 = datastore_doc_batch:apply(Batch3),
                    case datastore_doc_batch:terminate_request(Ref, Batch4) of
                        ok -> {Result, Batch4};
                        {error, Reason} -> {{error, Reason}, Batch4}
                    end
            end;
        Error ->
            {Error, Batch2}
    end,
    {Result2, datastore_doc_batch:terminate(Batch5)}.