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
-export([add/3, get/2, delete/2, delete/3]).
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

-define(REV_LENGTH,
    application:get_env(cluster_worker, datastore_links_rev_length, 16)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates named link between a document and a target.
%% @end
%%--------------------------------------------------------------------
-spec add(link_name(), link_target(), tree()) ->
    {{ok, link()} | {error, term()}, tree()}.
add(LinkName, LinkTarget, Tree) ->
    LinkRev = datastore_utils:gen_hex(?REV_LENGTH),
    case bp_tree:insert(LinkName, {LinkTarget, LinkRev}, Tree) of
        {ok, Tree2} ->
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
%% Deletes named link between a document and a target ignoring revision.
%% @end
%%--------------------------------------------------------------------
-spec delete(link_name(), tree()) -> {ok | {error, term()}, tree()}.
delete(LinkName, Tree) ->
    delete(LinkName, undefined, Tree).

%%--------------------------------------------------------------------
%% @doc
%% Deletes named link between a document and a target in provided revision.
%% @end
%%--------------------------------------------------------------------
-spec delete(link_name(), link_rev(), tree()) ->
    {ok | {error, term()}, tree()}.
delete(LinkName, LinkRev, Tree) ->
    Pred = fun
        ({_, Rev}) when LinkRev =/= undefined -> Rev =:= LinkRev;
        (_) -> true
    end,
    case bp_tree:remove(LinkName, Pred, Tree) of
        {ok, Tree2} ->
            {ok, Tree2};
        {{error, Reason}, Tree2} when
            Reason == not_found;
            Reason == predicate_not_satisfied ->
            {ok, Tree2};
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
    {ok, Tree} = datastore_links:init_tree(Ctx, Key, TreeId, Batch2),
    {Result, Tree2} = erlang:apply(?MODULE, Function, Args ++ [Tree]),
    Batch3 = datastore_links:terminate_tree(Tree2),
    {Result2, Batch5} = case Result of
        {error, Reason} ->
            {{error, Reason}, Batch3};
        _ ->
            Batch4 = datastore_doc_batch:apply(Batch3),
            case datastore_doc_batch:terminate_request(Ref, Batch4) of
                ok -> {Result, Batch4};
                {error, Reason} -> {{error, Reason}, Batch4}
            end
    end,
    {Result2, datastore_doc_batch:terminate(Batch5)}.