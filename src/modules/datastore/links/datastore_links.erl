%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides API for datastore document links management.
%%%
%%% Each datastore key can be associated with a one or many named links
%%% that point to some target/targets (#link{}). Links are grouped into trees
%%% and internally stored as tree nodes (#links_node{}). A set of trees creates
%%% a forest which holds pointers to the tree roots (#links_forest{}). In order
%%% to avoid synchronization conflicts between trees, links masks are introduced
%%% (#links_mask{}). Links mask holds a list of links in given revisions that
%%% should be excluded when get or fold operation is executed for a given tree.
%%% Links masks are arranged in a linked list form for given tree and links mask
%%% root (#links_mask_root{}) holds pointers to heads and tails of each links
%%% mask list.
%%%
%%% Links trees are represented by B+ trees. Tree nodes management functions
%%% are provided by {@link links_tree} module which implements
%%% {@link bp_tree_store} behaviour.
%%%
%%% From the API perspective there are two key objects: tree and forest iterator.
%%% First one represents a single tree and can be created with
%%% {@link init_tree/3} or {@link init_tree/4} functions. Second one represents
%%% a collection of trees and can be created with
%%% {@link datastore_links_iter:init/3} or {@link datastore_links_iter:init/4}
%%% functions. Both of them use {@link datastore_doc_batch} as a local cache.
%%% In order to retrieve datastore documents batch, terminate function should be
%%% called on both of the objects. For more information about documents batch
%%% checkout {@link datastore_doc_batch} module.
%%%
%%% NOTE! Functions provided by this module are thread safe. In order to achieve
%%% consistency and atomicity they should by called from serialization process
%%% e.g. {@link datastore_writer}.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_links).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_links.hrl").
-include_lib("bp_tree/include/bp_tree.hrl").

%% API
-export([get_forest_id/1, get_mask_root_id/1, get_tree_id/1]).
-export([init_tree/3, init_tree/4, init_tree/5, terminate_tree/1]).
-export([add/2, get/2, delete/2, mark_deleted/3]).
-export([fold/4]).
-export([get_links_trees/3]).

-type ctx() :: datastore_cache:ctx().
-type key() :: datastore:key().
-type tree_id() :: links_tree:id().
-type tree_ids() :: all | tree_id() | [tree_id()].
-type tree() :: bp_tree:tree().
-type forest_id() :: links_forest:id().
-type batch() :: undefined | datastore_doc_batch:batch().
-type link() :: #link{}.
-type link_name() :: binary() | integer().
-type link_target() :: binary() | integer().
-type link_rev() :: undefined | binary().
-type remove_pred() :: bp_tree:remove_pred().
-type mask() :: datastore_links_mask:mask().
-type forest_it() :: datastore_links_iter:forest_it().
-type fold_fun() :: datastore_links_iter:fold_fun().
-type fold_acc() :: datastore_links_iter:fold_acc().
-type fold_opts() :: datastore_links_iter:fold_opts().

-export_type([ctx/0, tree_id/0, tree_ids/0, forest_id/0]).
-export_type([link_name/0, link_target/0, link_rev/0, link/0]).
-export_type([forest_it/0, fold_fun/0, fold_acc/0, fold_opts/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns links forest ID.
%% @end
%%--------------------------------------------------------------------
-spec get_forest_id(key()) -> forest_id().
get_forest_id(Key) ->
    datastore_utils:gen_key(<<"links_forest">>, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns links mask pointer ID.
%% @end
%%--------------------------------------------------------------------
-spec get_mask_root_id(key()) -> link_mask_ptr:id().
get_mask_root_id(Key) ->
    datastore_utils:gen_key(<<"links_mask">>, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns links tree ID.
%% @end
%%--------------------------------------------------------------------
-spec get_tree_id(tree()) -> tree_id().
get_tree_id(#bp_tree{store_state = State}) ->
    links_tree:get_tree_id(State).

%%--------------------------------------------------------------------
%% @equiv init_tree(Ctx, Key, TreeId, undefined)
%% @end
%%--------------------------------------------------------------------
-spec init_tree(ctx(), key(), tree_id()) -> {ok, tree()} | {error, term()}.
init_tree(Ctx, Key, TreeId) ->
    init_tree(Ctx, Key, TreeId, undefined).

%%--------------------------------------------------------------------
%% @equiv init_tree(Ctx, Key, TreeId, Batch, false)
%% @end
%%--------------------------------------------------------------------
-spec init_tree(ctx(), key(), tree_id(), batch()) ->
    {ok, tree()} | {error, term()}.
init_tree(Ctx, Key, TreeId, Batch) ->
    init_tree(Ctx, Key, TreeId, Batch, false).

%%--------------------------------------------------------------------
%% @doc
%% Initializes links tree.
%% @end
%%--------------------------------------------------------------------
-spec init_tree(ctx(), key(), tree_id(), batch(), boolean()) ->
    {ok, tree()} | {error, term()}.
init_tree(Ctx, Key, TreeId, Batch, ReadOnly) ->
    bp_tree:init([
        {order, application:get_env(?CLUSTER_WORKER_APP_NAME,
            datastore_links_tree_order, 50)},
        {store_module, links_tree},
        {store_args, [Ctx, Key, TreeId, Batch]},
        {read_only, ReadOnly}
    ]).

%%--------------------------------------------------------------------
%% @doc
%% Clean up links tree. Returns documents batch.
%% @end
%%--------------------------------------------------------------------
-spec terminate_tree(tree()) -> batch().
terminate_tree(Tree) ->
    bp_tree:terminate(Tree).

%%--------------------------------------------------------------------
%% @doc
%% Creates named link between a document and a target.
%% @end
%%--------------------------------------------------------------------
-spec add([{link_name(), {link_target(), link_rev()}}], tree()) ->
    {{ok, [link_name()]} | {error, term()}, tree()}.
add(Items, Tree) ->
    datastore_links_crud:add(Items, Tree).

%%--------------------------------------------------------------------
%% @doc
%% Returns document link by name.
%% @end
%%--------------------------------------------------------------------
-spec get(link_name(), forest_it()) ->
    {{ok, [link()]} | {error, term()}, forest_it()}.
get(LinkName, ForestIt) ->
    datastore_links_iter:get(LinkName, ForestIt).

%%--------------------------------------------------------------------
%% @doc
%% Removes document link by name and revision.
%% @end
%%--------------------------------------------------------------------
-spec delete([{link_name(), remove_pred()}], tree()) ->
    {{ok, [link_name()]} | {error, term()}, tree()}.
delete(Items, Tree) ->
    datastore_links_crud:delete(Items, Tree).

%%--------------------------------------------------------------------
%% @doc
%% Marks document link given by name and revision as deleted.
%% @end
%%--------------------------------------------------------------------
-spec mark_deleted(link_name(), link_rev(), mask()) ->
    {ok | {error, term()}, mask()}.
mark_deleted(LinkName, LinkRev, Mask) ->
    datastore_links_mask:mark_deleted(LinkName, LinkRev, Mask).

%%--------------------------------------------------------------------
%% @doc
%% Calls Fun(Link, Acc) for each link in a link tree forest in increasing order
%% of link names.
%% @end
%%--------------------------------------------------------------------
-spec fold(fold_fun(), fold_acc(), forest_it(), fold_opts()) ->
    {{ok, fold_acc()} | {{ok, fold_acc()}, datastore_links_iter:token()} |
    {error, term()}, forest_it()}.
fold(Fun, Acc, ForestIt, Opts) ->
    datastore_links_iter:fold(Fun, Acc, ForestIt, Opts).

%%--------------------------------------------------------------------
%% @doc
%% Returns IDs of all trees in a links tree forest.
%% @end
%%--------------------------------------------------------------------
-spec get_links_trees(ctx(), key(), batch()) ->
    {{ok, [tree_id()]} | {error, term()}, batch()}.
get_links_trees(Ctx, Key, Batch) ->
    ForestId = get_forest_id(Key),
    case datastore_doc:fetch(Ctx, ForestId, Batch) of
        {{ok, #document{value = #links_forest{trees = Trees}}}, Batch2} ->
            {{ok, maps:keys(Trees)}, Batch2};
        {{error, Reason}, Batch2} ->
            {{error, Reason}, Batch2}
    end.