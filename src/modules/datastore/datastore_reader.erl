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
-export([histogram_get/3, infinite_log_operation/4]).

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
        {ok, ForestIt} = datastore_links_iter:init(set_direct_access_ctx(FetchNode, Ctx), Key, TreeIds),
        lists:map(fun(LinkName) ->
            {Result, _} = datastore_links:get(LinkName, ForestIt),
            Result
        end, LinkNames)
    catch
        throw:{fetch_error, not_found} ->
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
        case datastore_links:get_links_trees(set_direct_access_ctx(FetchNode, Ctx), Key, undefined) of
            {{ok, TreeIds}, _} ->
                {ok, TreeIds};
            {{error, Reason}, _} ->
                {error, Reason}
        end
    catch
        throw:{fetch_error, not_found} ->
            datastore_router:execute_on_node(FetchNode, datastore_writer, fetch_links_trees, [Ctx, Key])
    end.


-spec histogram_get(node(), datastore_doc:ctx(), list()) ->
    ok | {ok, [histogram_windows:window()] | histogram_time_series:windows_map()} | {error, term()}.
histogram_get(FetchNode, Ctx, Args) ->
    try
        GetResult = case Args of
            [Id, Options] ->
                histogram_time_series:get(set_direct_access_ctx(FetchNode, Ctx), Id, Options, undefined);
            [Id, RequestedMetrics, Options] ->
                histogram_time_series:get(set_direct_access_ctx(FetchNode, Ctx), Id, RequestedMetrics, Options, undefined)
        end,
        case GetResult of
            {{ok, Result}, _} ->
                {ok, Result};
            {{error, Reason}, _} ->
                {error, Reason}
        end
    catch
        throw:{fetch_error, not_found} ->
            datastore_router:execute_on_node(FetchNode, datastore_writer, histogram_operation, [Ctx, get, Args])
    end.


-spec infinite_log_operation(node(), datastore_doc:ctx(), atom(), list()) -> 
    {ok, infinite_log_browser:listing_result()} | {error, term()}.
infinite_log_operation(FetchNode, Ctx, list, [Key, Opts]) ->
    Fallback = fun() ->
        datastore_router:execute_on_node(FetchNode, datastore_writer, infinite_log_operation, [Ctx, list, [Key, Opts]])
    end,
    try
        case infinite_log:list(set_direct_access_ctx(FetchNode, Ctx), Key, Opts, readonly, undefined) of
            {{ok, Result}, _} ->
                {ok, Result};
            {{error, update_required}, _} ->
                Fallback();
            {{error, Reason}, _} ->
                {error, Reason}
        end
    catch
        throw:{fetch_error, not_found} ->
            Fallback()
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


%% @private
-spec set_direct_access_ctx(node(), datastore_doc:ctx()) -> datastore_doc:ctx().
set_direct_access_ctx(FetchNode, #{disc_driver := undefined} = Ctx) when FetchNode =:= node() -> 
    Ctx;
set_direct_access_ctx(_FetchNode, Ctx) ->
    Ctx#{disc_driver => undefined, remote_driver => undefined, throw_not_found => true}.
