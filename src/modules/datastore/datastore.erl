%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides datastore API.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([create/3, save/3, update/3, update/4, create_backup/2]).
-export([get/2, exists/2]).
-export([delete/3, delete_all/2]).
-export([add_links/4, check_and_add_links/5, get_links/4, delete_links/4, mark_links_deleted/4]).
-export([fold_links/6, get_links_trees/2]).

-type ctx() :: #{model := datastore_model:model(),
                 mutator => datastore_doc:mutator(),
                 scope => datastore_doc:scope(),
                 routing => local | global,
                 generated_key => boolean(),
                 routing_key => key(), % key used to route request to tp process and to choose node
                 fold_enabled => boolean(),
                 secure_fold_enabled => boolean(), % similar to fold_enabled but guarantees that fold links
                                                   % are added/deleted in the same order as create/delete
                                                   % operations that triggered link's adding/deletion
                                                   % TOOD VFS-5971 - does not support save and update with default
                 local_fold => boolean(), % Fold links are added using local routing
                 local_fold_node => node(), % Node used to generate key for local fold
                 sync_enabled => boolean(),
                 sync_change => boolean(), % should set to 'true' for save
                                           % of remote change
                 hooks_disabled => boolean(),
                 local_links_tree_id => tree_id(),
                 volatile => boolean(),
                 mutator_pid => pid(),
                 memory_driver => memory_driver(),
                 memory_driver_ctx => memory_driver_ctx(),

                 ha_enabled => boolean(),
                 % Memory copies external API (to be set by model)
                 memory_copies => all | none, % number of nodes used for memory copies
                 % Memory copies internal API (to be set by datastore modules)
                 memory_copies_nodes => [node()], % nodes to be used for memory copies (set by datastore_router)
                 failed_nodes => [node()], % failed nodes connected to particular request (set by datastore_router)
                 failed_master => boolean(), % is master for request down (set by datastore_router)

                 disc_driver => disc_driver(),
                 disc_driver_ctx => disc_driver_ctx(),
                 remote_driver => remote_driver(),
                 remote_driver_ctx => remote_driver_ctx(),
                 include_deleted => boolean(),
                 expiry => cberl:expiry(),
                        % Specify the expiration time. For disc/cached models
                        % This is either an absolute Unix timestamp or
                        % a relative offset from now, in seconds.
                        % If the value of this number is greater than 
                        % the value of thirty days in seconds, 
                        % then it is a Unix timestamp. For memory models
                        % it is always a relative offset from now.
                 throw_not_found => boolean(),
                 direct_disc_fallback => boolean()
}.
-type memory_driver() :: undefined | ets_driver | mnesia_driver.
-type memory_driver_ctx() :: ets_driver:ctx() | mnesia_driver:ctx().
-type disc_driver() :: undefined | couchbase_driver.
-type disc_driver_ctx() :: couchbase_driver:ctx().
-type remote_driver() :: remote_driver | atom().
-type remote_driver_ctx() :: remote_driver:ctx().
-type driver() :: memory_driver() | disc_driver() | remote_driver().
-type driver_ctx() :: memory_driver_ctx() | disc_driver_ctx() |
                      remote_driver_ctx().

-export_type([ctx/0]).
-export_type([memory_driver/0, disc_driver/0, remote_driver/0, driver/0]).
-export_type([memory_driver_ctx/0, disc_driver_ctx/0, remote_driver_ctx/0,
    driver_ctx/0]).

-type key() :: datastore_doc:key().
-type value() :: datastore_doc:value().
-type doc() :: datastore_doc:doc(value()).
-type diff() :: datastore_doc:diff(value()).
-type pred() :: datastore_doc:pred(value()).

-export_type([key/0, value/0, doc/0, diff/0, pred/0]).

-type link() :: datastore_links:link().
-type link_name() :: datastore_links:link_name().
-type link_target() :: datastore_links:link_target().
-type link_rev() :: datastore_links:link_rev().
-type tree_id() :: datastore_links:tree_id().
-type tree_ids() :: datastore_links:tree_ids().
-type fold_fun(Sub) :: fun((Sub, fold_acc()) ->
    {ok | stop, fold_acc()} | {error, term()}).
-type fold_acc() :: datastore_links:fold_acc().
-type fold_opts() :: datastore_links:fold_opts().

-export_type([link/0, link_name/0, link_target/0, link_rev/0, tree_id/0,
    tree_ids/0, fold_fun/1, fold_acc/0, fold_opts/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates document in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec create(ctx(), key(), doc()) -> {ok, doc()} | {error, term()}.
create(Ctx, Key, Doc = #document{}) ->
    datastore_hooks:wrap(Ctx, Key, create, [Doc], fun
        (Function, Args) -> datastore_router:route(Function, Args)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Saves document in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), key(), doc()) -> {ok, doc()} | {error, term()}.
save(Ctx, Key, Doc = #document{}) ->
    datastore_hooks:wrap(Ctx, Key, save, [Doc], fun
        (Function, Args) -> datastore_router:route(Function, Args)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Updates document in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff()) -> {ok, doc()} | {error, term()}.
update(Ctx, Key, Diff) ->
    datastore_hooks:wrap(Ctx, Key, update, [Diff], fun(Function, Args) ->
        datastore_router:route(Function, Args)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Updates document in a datastore. If document is missing, creates default one.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff(), doc()) -> {ok, doc()} | {error, term()}.
update(Ctx, Key, Diff, Default) ->
    datastore_hooks:wrap(Ctx, Key, update, [Diff, Default], fun
        (Function, Args) -> datastore_router:route(Function, Args)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Creates backup on slave node.
%% @end
%%--------------------------------------------------------------------
-spec create_backup(ctx(), key()) -> {ok, doc()} | {error, term()}.
create_backup(Ctx, Key) ->
    datastore_hooks:wrap(Ctx, Key, create_backup, [], fun(Function, Args) ->
        datastore_router:route(Function, Args)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns document from a datastore.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), key()) -> {ok, doc()} | {error, term()}.
get(Ctx, Key) ->
    datastore_hooks:wrap(Ctx, Key, get, [], fun(Function, Args) ->
        datastore_router:route(Function, Args)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether document exists in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec exists(ctx(), key()) -> {ok, boolean()} | {error, term()}.
exists(Ctx, Key) ->
    datastore_hooks:wrap(Ctx, Key, exists, [], fun(Function, Args) ->
        datastore_router:route(Function, Args)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Removes document from a datastore if a predicate is satisfied.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(), key(), pred()) -> ok | {error, term()}.
delete(Ctx, Key, Pred) ->
    datastore_hooks:wrap(Ctx, Key, delete, [Pred], fun
        (Function, Args) -> datastore_router:route(Function, Args)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Removes all documents from a model.
%% @end
%%--------------------------------------------------------------------
-spec delete_all(ctx(), key()) -> ok | {error, term()}.
delete_all(#{disc_driver := DD}, _Key) when DD =/= undefined ->
  {error, not_supported};
delete_all(#{
  memory_driver := ets_driver,
  memory_driver_ctx := MemoryCtx
} = Ctx, Key) ->
  datastore_hooks:wrap(Ctx, Key, delete_all, [], fun
    (_Function, _Args) -> ets_driver:delete_all(MemoryCtx)
  end);
delete_all(_, _) ->
  {error, not_supported}.

%%--------------------------------------------------------------------
%% @doc
%% Creates named links between a document and targets.
%% @end
%%--------------------------------------------------------------------
-spec add_links(ctx(), key(), tree_id(), [{link_name(), link_target()}]) ->
    [{ok, link()} | {error, term()}].
add_links(Ctx, Key, TreeId, Links) ->
    datastore_hooks:wrap(Ctx, Key, add_links, [TreeId, Links], fun
        (Function, Args) -> datastore_router:route(Function, Args)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Creates named links between a document and targets. Checks if links do not exist in selected trees.
%% @end
%%--------------------------------------------------------------------
-spec check_and_add_links(ctx(), key(), tree_id(), tree_ids(), [{link_name(), link_target()}]) ->
    [{ok, link()} | {error, term()}].
check_and_add_links(Ctx, Key, TreeId, CheckTrees, Links) ->
    datastore_hooks:wrap(Ctx, Key, check_and_add_links, [TreeId, CheckTrees, Links], fun
        (Function, Args) -> datastore_router:route(Function, Args)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns document links by names.
%% @end
%%--------------------------------------------------------------------
-spec get_links(ctx(), key(), tree_ids(), [link_name()]) ->
    [{ok, [link()]} | {error, term()}].
get_links(Ctx, Key, TreeIds, LinkNames) ->
    datastore_hooks:wrap(Ctx, Key, get_links, [TreeIds, LinkNames], fun
        (Function, Args) -> datastore_router:route(Function, Args)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Removes document links by names and optional revisions.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(ctx(), key(), tree_id(),
    [link_name() | {link_name(), link_rev()}]) -> [ok | {error, term()}].
delete_links(Ctx, Key, TreeId, Links) ->
    Links2 = lists:map(fun
        ({LinkName, LinkRev}) -> {LinkName, LinkRev};
        (LinkName) -> {LinkName, undefined}
    end, Links),
    datastore_hooks:wrap(Ctx, Key, delete_links, [TreeId, Links2], fun
        (Function, Args) -> datastore_router:route(Function, Args)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks document links in provided revisions as deleted.
%% @end
%%--------------------------------------------------------------------
-spec mark_links_deleted(ctx(), key(), tree_id(),
    [link_name() | {link_name(), link_rev()}]) -> [ok | {error, term()}].
mark_links_deleted(Ctx, Key, TreeId, Links) ->
    Links2 = lists:map(fun
        ({LinkName, LinkRev}) -> {LinkName, LinkRev};
        (LinkName) -> {LinkName, undefined}
    end, Links),
    datastore_hooks:wrap(Ctx, Key, mark_links_deleted, [TreeId, Links2], fun
        (Function, Args) -> datastore_router:route(Function, Args)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Calls Fun(Link, Acc) for each document link.
%% @end
%%--------------------------------------------------------------------
-spec fold_links(ctx(), key(), tree_ids(), fold_fun(link()), fold_acc(),
    fold_opts()) -> {ok, fold_acc()} |
    {{ok, fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_links(Ctx, Key, TreeIds, Fun, Acc, Opts) ->
    datastore_hooks:wrap(Ctx, Key, fold_links, [TreeIds, Fun, Acc, Opts],
        fun(Function, Args) ->
            datastore_router:route(Function, Args)
        end
    ).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of IDs of link trees that constitute document links forest.
%% @end
%%--------------------------------------------------------------------
-spec get_links_trees(ctx(), key()) -> {ok, [tree_id()]} | {error, term()}.
get_links_trees(Ctx, Key) ->
    datastore_hooks:wrap(Ctx, Key, get_links_trees, [], fun
        (Function, Args) -> datastore_router:route(Function, Args)
    end).