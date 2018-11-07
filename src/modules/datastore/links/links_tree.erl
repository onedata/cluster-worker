%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides {@link bp_tree_store} behaviour implementation.
%%% It is responsible for storing and retrieving B+ tree nodes.
%%% @end
%%%-------------------------------------------------------------------
-module(links_tree).
-author("Krzysztof Trzepla").

-behaviour(bp_tree_store).

-include("modules/datastore/datastore_models.hrl").
-include("global_definitions.hrl").

%% API
-export([get_tree_id/1]).

%% bp_tree_store callbacks
-export([init/1, set_root_id/2, unset_root_id/1, get_root_id/1, create_node/2,
    get_node/2, update_node/3, delete_node/2, terminate/1, update_batch/2]).

-record(state, {
    ctx :: ctx(),
    key :: datastore:key(),
    forest_id :: datastore_links:forest_id(),
    tree_id :: id(),
    batch :: batch()
}).

-type id() :: binary().
-type node_id() :: links_node:id().
-type links_node() :: links_node:links_node().
-type trees() :: links_forest:trees().
-type ctx() :: datastore_links:ctx().
-type batch() :: undefined | datastore_doc_batch:batch().
-type state() :: #state{}.

-export_type([id/0]).

% Default time in seconds for document (saved in memory only) to expire after
% delete
-define(MEMORY_EXPIRY, 5).
% Default time in seconds for document (saved to disk) to expire after delete
% (one year)
-define(DISK_EXPIRY, 31536000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns tree ID.
%% @end
%%--------------------------------------------------------------------
-spec get_tree_id(state()) -> id().
get_tree_id(#state{tree_id = TreeId}) ->
    TreeId.

%%%===================================================================
%%% bp_tree_store callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Implementation of {@link bp_tree_store:init/2} callback.
%% @end
%%--------------------------------------------------------------------
-spec init(bp_tree_store:args()) -> {ok, state()}.
init([Ctx, Key, TreeId, Batch]) ->
    {ok, #state{
        ctx = Ctx,
        key = Key,
        forest_id = datastore_links:get_forest_id(Key),
        tree_id = TreeId,
        batch = Batch
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Implementation of {@link bp_tree_store:set_root_id/2} callback.
%% @end
%%--------------------------------------------------------------------
-spec set_root_id(node_id(), state()) -> {ok | {error, term()}, state()}.
set_root_id(NodeId, State = #state{
    ctx = Ctx, key = Key, forest_id = ForestId, tree_id = TreeId, batch = Batch
}) ->
    Diff = fun(Forest = #links_forest{trees = Trees}) ->
        {ok, Forest#links_forest{trees = set_root_id(TreeId, NodeId, Trees)}}
    end,
    Default = #document{
        key = ForestId,
        value = #links_forest{
            model = maps:get(model, Ctx),
            key = Key,
            trees = #{TreeId => {NodeId, datastore_utils:gen_rev(1)}}
        }
    },
    case datastore_doc:update(Ctx, ForestId, Diff, Default, Batch) of
        {{ok, _}, Batch2} ->
            {ok, State#state{batch = Batch2}};
        {{error, Reason}, Batch2} ->
            {{error, Reason}, State#state{batch = Batch2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Implementation of {@link bp_tree_store:unset_root_id/1} callback.
%% @end
%%--------------------------------------------------------------------
-spec unset_root_id(state()) -> {ok | {error, term()}, state()}.
unset_root_id(State = #state{
    ctx = Ctx, forest_id = ForestId, tree_id = TreeId, batch = Batch
}) ->
    Diff = fun(Forest = #links_forest{trees = Trees}) ->
        {ok, Forest#links_forest{trees = set_root_id(TreeId, <<>>, Trees)}}
    end,
    case datastore_doc:update(Ctx, ForestId, Diff, Batch) of
        {{ok, _}, Batch2} ->
            {ok, State#state{batch = Batch2}};
        {{error, Reason}, Batch2} ->
            {{error, Reason}, State#state{batch = Batch2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Implementation of {@link bp_tree_store:get_root_id/1} callback.
%% @end
%%--------------------------------------------------------------------
-spec get_root_id(state()) ->
    {{ok, node_id()} | {error, term()}, state()}.
get_root_id(State = #state{
    ctx = Ctx, forest_id = ForestId, tree_id = TreeId, batch = Batch
}) ->
    Ctx2 = set_remote_driver_ctx(Ctx, State),
    case datastore_doc:fetch(Ctx2, ForestId, Batch, true) of
        {{ok, #document{value = #links_forest{trees = Trees}}}, Batch2} ->
            {get_root_id(TreeId, Trees), State#state{batch = Batch2}};
        {{error, Reason}, Batch2} ->
            {{error, Reason}, State#state{batch = Batch2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Implementation of {@link bp_tree_store:create_node/2} callback.
%% @end
%%--------------------------------------------------------------------
-spec create_node(links_node(), state()) ->
    {{ok, node_id()} | {error, term()}, state()}.
create_node(Node, State = #state{ctx = Ctx, key = Key, batch = Batch}) ->
    NodeId = datastore_utils:gen_key(),
    Ctx2 = Ctx#{generated_key => true},
    Doc = #document{
        key = NodeId,
        value = #links_node{
            model = maps:get(model, Ctx),
            key = Key,
            node = Node
        }
    },
    case datastore_doc:save(Ctx2, NodeId, Doc, Batch) of
        {{ok, _}, Batch2} ->
            {{ok, NodeId}, State#state{batch = Batch2}};
        {{error, Reason}, Batch2} ->
            {{error, Reason}, State#state{batch = Batch2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Implementation of {@link bp_tree_store:get_node/2} callback.
%% @end
%%--------------------------------------------------------------------
-spec get_node(node_id(), state()) ->
    {{ok, links_node()} | {error, term()}, state()}.
get_node(NodeId, State = #state{ctx = Ctx, batch = Batch, tree_id = TreeID}) ->
    LocalTreeID = maps:get(local_links_tree_id, Ctx, undefined),
    Ctx2 = set_remote_driver_ctx(Ctx, State),
    Ctx3 = case {Batch, LocalTreeID} of
        {undefined, _} ->
            Ctx2#{include_deleted => true};
        {_, undefined} ->
            Ctx2;
        {_, TreeID} ->
            Ctx2;
        _ ->
            Ctx2#{include_deleted => true}
    end,
    case datastore_doc:fetch(Ctx3, NodeId, Batch, true) of
        {{ok, #document{value = #links_node{node = Node}}}, Batch2} ->
            {{ok, Node}, State#state{batch = Batch2}};
        {{error, Reason}, Batch2} ->
            {{error, Reason}, State#state{batch = Batch2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Implementation of {@link bp_tree_store:update_node/3} callback.
%% @end
%%--------------------------------------------------------------------
-spec update_node(node_id(), links_node(), state()) ->
    {ok | {error, term()}, state()}.
update_node(NodeId, Node, State = #state{
    ctx = Ctx, batch = Batch
}) ->
    Diff = fun(LinksNode = #links_node{}) ->
        {ok, LinksNode#links_node{
            model = maps:get(model, Ctx),
            node = Node
        }}
    end,
    case datastore_doc:update(Ctx, NodeId, Diff, Batch) of
        {{ok, _}, Batch2} ->
            {ok, State#state{batch = Batch2}};
        {{error, Reason}, Batch2} ->
            {{error, Reason}, State#state{batch = Batch2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Implementation of {@link bp_tree_store:delete_node/2} callback.
%% @end
%%--------------------------------------------------------------------
-spec delete_node(node_id(), state()) -> {ok | {error, term()}, state()}.
delete_node(NodeId, State = #state{ctx = #{disc_driver := undefined} = Ctx,
    batch = Batch}) ->
    Expiry = application:get_env(?CLUSTER_WORKER_APP_NAME,
        link_memory_expiry, ?MEMORY_EXPIRY),
    Ctx2 = datastore_utils:set_expiry(Ctx, Expiry),
    case datastore_doc:delete(Ctx2, NodeId, Batch) of
        {ok, Batch2} ->
            {ok, State#state{batch = Batch2}};
        {{error, Reason}, Batch2} ->
            {{error, Reason}, State#state{batch = Batch2}}
    end;
delete_node(NodeId, State = #state{ctx = #{disc_driver_ctx := DiscCtx} = Ctx,
    batch = Batch}) ->
    Expiry = application:get_env(?CLUSTER_WORKER_APP_NAME,
        link_disk_expiry, ?DISK_EXPIRY),

    Ctx2 = Ctx#{disc_driver_ctx => datastore_utils:set_expiry(DiscCtx, Expiry)},
    case datastore_doc:delete(Ctx2, NodeId, Batch) of
        {ok, Batch2} ->
            {ok, State#state{batch = Batch2}};
        {{error, Reason}, Batch2} ->
            {{error, Reason}, State#state{batch = Batch2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Implementation of {@link bp_tree_store:terminate/1} callback.
%% @end
%%--------------------------------------------------------------------
-spec terminate(state()) -> batch().
terminate(#state{batch = Batch}) ->
    Batch.

%%--------------------------------------------------------------------
%% @doc
%% Function used to update batch field inside state record.
%% @end
%%--------------------------------------------------------------------
-spec update_batch(fun((batch()) -> batch()), state()) -> state().
update_batch(Fun, State = #state{batch = Batch}) ->
    State#state{batch = Fun(Batch)}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Selects root of a tree by ID.
%% @end
%%--------------------------------------------------------------------
-spec get_root_id(id(), trees()) -> {ok, node_id()} | {error, not_found}.
get_root_id(TreeId, Trees) ->
    case maps:find(TreeId, Trees) of
        {ok, {<<>>, _}} -> {error, not_found};
        {ok, {RootId, _}} -> {ok, RootId};
        error -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets root of a tree associated with ID.
%% @end
%%--------------------------------------------------------------------
-spec set_root_id(id(), node_id(), trees()) -> trees().
set_root_id(TreeId, NodeId, Trees) ->
    {Generation, _Hash} = case maps:find(TreeId, Trees) of
        {ok, {_, Rev}} -> datastore_utils:parse_rev(Rev);
        error -> {0, <<>>}
    end,
    maps:put(TreeId, {NodeId, datastore_utils:gen_rev(Generation + 1)}, Trees).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets remote driver context.
%% @end
%%--------------------------------------------------------------------
-spec set_remote_driver_ctx(ctx(), state()) -> ctx().
set_remote_driver_ctx(Ctx = #{remote_driver := undefined}, _State) ->
    Ctx;
set_remote_driver_ctx(Ctx, #state{tree_id = ?MODEL_ALL_TREE_ID}) ->
    Ctx;
set_remote_driver_ctx(Ctx, #state{key = Key, tree_id = TreeId}) ->
    Ctx#{remote_driver_ctx => #{
        model => maps:get(model, Ctx),
        routing_key => Key,
        source_ids => [TreeId]
    }}.
