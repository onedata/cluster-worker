%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides the document direct access API for
%%% read operations.
%%%-------------------------------------------------------------------
-module(datastore_reader).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").

-export([get/3, exists/3]).
-export([get_links/5, get_links_trees/3]).

-type tree_id() :: datastore_links:tree_id().
-type link() :: datastore_links:link().
-type link_name() :: datastore_links:link_name().

%%%===================================================================
%%% Direct access API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns datastore document first using memory only store and fallbacking
%% to persistent store if missing.
%% @end
%%--------------------------------------------------------------------
-spec get(node(), datastore_doc:ctx(), datastore_doc:key()) -> {ok, datastore_doc:doc(datastore_doc:value())} | {error, term()}.
get(FetchNode, #{include_deleted := true} = Ctx, Key) ->
    case datastore_cache:get(Ctx, Key) of
        {ok, #document{value = undefined, deleted = true}} -> {error, not_found};
        {ok, Doc} -> {ok, Doc};
        {error, not_found} -> fetch_missing(FetchNode, Ctx, Key);
        {error, Reason2} -> {error, Reason2}
    end;
get(FetchNode, Ctx, Key) ->
    case datastore_cache:get(Ctx, Key) of
        {ok, #document{deleted = true}} -> {error, not_found};
        {ok, Doc} -> {ok, Doc};
        {error, not_found} -> fetch_missing(FetchNode, Ctx, Key);
        {error, Reason2} -> {error, Reason2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks whether datastore document exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(node(), datastore_doc:ctx(), datastore_doc:key()) -> {ok, boolean()} | {error, term()}.
exists(FetchNode, Ctx, Key) ->
    case get(FetchNode, Ctx, Key) of
        {ok, _Doc} -> {ok, true};
        {error, not_found} -> {ok, false};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns datastore document links.
%% @end
%%--------------------------------------------------------------------
-spec get_links(node(), datastore_doc:ctx(), datastore_doc:key(), tree_id(), [link_name()]) ->
    [{ok, link()} | {error, term()}].
get_links(FetchNode, Ctx, Key, TreeIds, LinkNames) ->
    try
        MemoryCtx = case {Ctx, node()} of
            {#{disc_driver := undefined}, FetchNode} -> Ctx;
            _ -> Ctx#{disc_driver => undefined, remote_driver => undefined, throw_not_found => true}
        end,
        {ok, ForestIt} = datastore_links_iter:init(MemoryCtx, Key, TreeIds),
        lists:map(fun(LinkName) ->
            {Result, _} = datastore_links:get(LinkName, ForestIt),
            Result
        end, LinkNames)
    catch
        _:_ ->
            datastore_router:execute_on_node(FetchNode,
                datastore_writer, fetch_links, [Ctx, Key, TreeIds, LinkNames])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of IDs of link trees that constitute datastore document links
%% forest.
%% @end
%%--------------------------------------------------------------------
-spec get_links_trees(node(), datastore_doc:ctx(), datastore_doc:key()) -> {ok, [tree_id()]} | {error, term()}.
get_links_trees(FetchNode, Ctx, Key) ->
    try
        MemoryCtx = case {Ctx, node()} of
            {#{disc_driver := undefined}, FetchNode} -> Ctx;
            _ -> Ctx#{disc_driver => undefined, remote_driver => undefined, throw_not_found => true}
        end,
        case datastore_links:get_links_trees(MemoryCtx, Key, undefined) of
            {{ok, TreeIds}, _} ->
                {ok, TreeIds};
            {{error, Reason}, _} ->
                {error, Reason}
        end
    catch
        _:_ ->
            datastore_router:execute_on_node(FetchNode, datastore_writer, fetch_links_trees, [Ctx, Key])
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to fetch missing document via tp process if needed.
%% @end
%%--------------------------------------------------------------------
-spec fetch_missing(node(), datastore_doc:ctx(), datastore_doc:key()) -> {ok, datastore_doc:doc(datastore_doc:value())} | {error, term()}.
fetch_missing(FetchNode, Ctx, Key) ->
    case (maps:get(disc_driver, Ctx, undefined) =/= undefined) orelse (node() =/= FetchNode) of
        true ->
            datastore_router:execute_on_node(FetchNode, datastore_writer, fetch, [Ctx, Key]);
        _ ->
            {error, not_found}
    end.