%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides datastore model API.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_model).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_links.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("global_definitions.hrl").


%% API
-export([init/1, get_unique_key/2]).
-export([create/2, save/2, update/3, update/4]).
-export([get/2, exists/2]).
-export([delete/2, delete/3, delete_all/1]).
-export([fold/3, fold/5, fold_keys/3]).
-export([add_links/4, check_and_add_links/5, get_links/4, delete_links/4, mark_links_deleted/4]).
-export([fold_links/6]).
-export([get_links_trees/2]).
%% for rpc
-export([datastore_apply_all_subtrees/4]).

-type model() :: module().
-type record() :: tuple().
-type record_struct() :: datastore_json:record_struct().
-type record_version() :: datastore_versions:record_version().
-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type pred() :: datastore_doc:pred(record()).
-type link_name() :: datastore:link_name().
-type link_target() :: datastore:link_target().
-type link_rev() :: datastore:link_rev().
-type link() :: datastore:link().
-type tree_id() :: datastore:tree_id().
-type tree_ids() :: datastore:tree_ids().
-type fold_fun(Sub) :: datastore:fold_fun(Sub).
-type fold_acc() :: datastore:fold_acc().
-type fold_opts() :: datastore:fold_opts().
-type one_or_many(Type) :: Type | [Type].

-export_type([model/0, record/0, record_struct/0, record_version/0,
    key/0, ctx/0, diff/0, fold_opts/0, tree_ids/0]).

% Default time in seconds for document to expire after delete (one year)
-define(EXPIRY, 31536000).

%%--------------------------------------------------------------------
%% @doc
%% Initializes memory driver of a datastore model.
%% @end
%%--------------------------------------------------------------------
-spec init(ctx()) -> ok | {error, term()}.
init(#{memory_driver := undefined}) ->
    ok;
init(#{
    memory_driver := Driver,
    memory_driver_ctx := Ctx,
    memory_driver_opts := Opts
}) ->
    lists:foldl(fun
        (Ctx2, ok) ->
            Driver:init(Ctx2, Opts);
        (_, Error) ->
            Error
    end, ok, datastore_multiplier:get_names(Ctx));
init(Ctx) ->
    init(datastore_model_default:set_defaults(Ctx)).

%%--------------------------------------------------------------------
%% @doc
%% Returns key that is unique between different models.
%% @end
%%--------------------------------------------------------------------
-spec get_unique_key(ctx(), key()) -> key().
% TODO - VFS-3975 - allow routing via generic key without model
% problem with links application that need such routing key
get_unique_key(#{model := Model}, Key) ->
    datastore_key:build_adjacent(atom_to_binary(Model, utf8), Key).

%%--------------------------------------------------------------------
%% @doc
%% Creates model document in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec create(ctx(), doc()) -> {ok, doc()} | {error, term()}.
create(Ctx, Doc = #document{key = undefined}) ->
    save(Ctx, Doc);
create(Ctx, Doc = #document{key = Key}) ->
    Result = datastore_apply(Ctx, Key, fun datastore:create/3, create, [Doc]),
    add_fold_link(Ctx, Key, Result).

%%--------------------------------------------------------------------
%% @doc
%% Saves model document in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), doc()) -> {ok, doc()} | {error, term()}.
save(Ctx, Doc = #document{key = undefined}) ->
    Ctx2 = Ctx#{generated_key => true},
    Doc2 = Doc#document{key = datastore_key:new()},
    save(Ctx2, Doc2);
save(Ctx, Doc = #document{key = Key}) ->
    Result = datastore_apply(Ctx, Key, fun datastore:save/3, save, [Doc]),
    add_fold_link(Ctx, Key, Result).

%%--------------------------------------------------------------------
%% @doc
%% Updates model document in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff()) -> {ok, doc()} | {error, term()}.
update(Ctx, Key, Diff) ->
    datastore_apply(Ctx, Key, fun datastore:update/3, update, [Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Updates model document in a datastore. If document is missing,
%% creates default one.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff(), record() | doc()) ->
    {ok, doc()} | {error, term()}.
update(Ctx, Key, Diff, Default = #document{}) ->
    Result = datastore_apply(Ctx, Key, fun datastore:update/4, update, [
        Diff, Default
    ]),
    add_fold_link(Ctx, Key, Result);
update(Ctx, Key, Diff, Default) ->
    update(Ctx, Key, Diff, #document{key = Key, value = Default}).

%%--------------------------------------------------------------------
%% @doc
%% Returns model document from a datastore.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), key()) -> {ok, doc()} | {error, term()}.
get(Ctx, Key) ->
    datastore_apply(Ctx, Key, fun datastore:get/2, get, []).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether model document exists in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec exists(ctx(), key()) -> {ok, boolean()} | {error, term()}.
exists(Ctx, Key) ->
    datastore_apply(Ctx, Key, fun datastore:exists/2, exists, []).

%%--------------------------------------------------------------------
%% @doc
%% Removes model document from a datastore.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(), key()) -> ok | {error, term()}.
delete(Ctx, Key) ->
    delete(Ctx, Key, fun(_) -> true end).

%%--------------------------------------------------------------------
%% @doc
%% Removes model document from a datastore if a predicate is satisfied.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(), key(), pred()) -> ok | {error, term()}.
delete(#{disc_driver := undefined} = Ctx, Key, Pred) ->
    Result = datastore_apply(Ctx, Key, fun datastore:delete/3, delete, [Pred]),
    delete_all_links(Ctx, Key, Result),
    delete_fold_link(Ctx, Key, Result);
delete(Ctx, Key, Pred) ->
    Expiry = application:get_env(?CLUSTER_WORKER_APP_NAME,
        document_expiry, ?EXPIRY),
    CtxWithExpiry = couchbase_driver:set_expiry(Ctx, Expiry),
    Result = datastore_apply(CtxWithExpiry, Key, fun datastore:delete/3, delete, [Pred]),
    delete_all_links(Ctx, Key, Result),
    delete_fold_link(Ctx, Key, Result).

%%--------------------------------------------------------------------
%% @doc
%% Removes all model documents from a datastore.
%% WARNING: should be used only on models that save all documents
%% with volatile flag.
%% @end
%%--------------------------------------------------------------------
-spec delete_all(ctx()) -> ok | {error, term()}.
delete_all(Ctx) ->
    % The Key is used for request routing, but the request does not concern any
    % specific Key - hence a constant <<"delete_all">> is used
    datastore_apply_all(Ctx, <<"delete_all">>, fun datastore:delete_all/2, delete_all, []).

%%--------------------------------------------------------------------
%% @doc
%% Calls Fun(Doc, Acc) for each model document in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec fold(ctx(), fold_fun(doc()), fold_acc()) ->
    {ok, fold_acc()} | {error, term()}.
fold(Ctx, Fun, Acc) ->
    fold(Ctx, Fun, fun(Key, Acc2) ->
        {ok, [Key | Acc2]}
    end, true, Acc).

%%--------------------------------------------------------------------
%% @doc
%% Calls Fun(Doc, Acc) for each model document in a datastore. Filters keys before.
%% @end
%%--------------------------------------------------------------------
-spec fold(ctx(), fold_fun(doc()), fold_fun(key()), boolean(), fold_acc()) ->
    {ok, fold_acc()} | {error, term()}.
fold(Ctx0 = #{model := Model, fold_enabled := true}, Fun, KeyFilter, ReverseKeys, Acc) ->
    Ctx = case Ctx0 of
        #{local_fold := true} -> Ctx0#{routing => local};
        _ -> Ctx0
    end,
    ModelKey = atom_to_binary(Model, utf8),
    LinksAns = fold_links(Ctx, ModelKey, ?MODEL_ALL_TREE_ID, fun(#link{name = Key}, Acc2) ->
        KeyFilter(Key, Acc2)
    end, [], #{}),

    case LinksAns of
        {ok, List} ->
            List2 = case ReverseKeys of
                true ->
                    lists:reverse(List);
                _ ->
                    List
            end,
            fold_internal(List2, Acc, Ctx, Fun);
        _ ->
            LinksAns
    end;
fold(_Ctx, _Fun, _KeyFilter, _ReverseKeys, _Acc) ->
    {error, not_supported}.

%%--------------------------------------------------------------------
%% @doc
%% Calls Fun(Key, Acc) for each model document key in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec fold_keys(ctx(), fold_fun(key()), fold_acc()) ->
    {ok, fold_acc()} | {error, term()}.
fold_keys(Ctx0 = #{model := Model, fold_enabled := true}, Fun, Acc) ->
    Ctx = case Ctx0 of
        #{local_fold := true} -> Ctx0#{routing => local};
        _ -> Ctx0
    end,
    ModelKey = atom_to_binary(Model, utf8),
    fold_links(Ctx, ModelKey, ?MODEL_ALL_TREE_ID, fun(#link{name = Key}, Acc2) ->
        Fun(Key, Acc2)
    end, Acc, #{});
fold_keys(_Ctx, _Fun, _Acc) ->
    {error, not_supported}.

%%--------------------------------------------------------------------
%% @doc
%% Creates named links between a model document and targets.
%% @end
%%--------------------------------------------------------------------
-spec add_links(ctx(), key(), tree_id(),
    one_or_many({link_name(), link_target()})) ->
    one_or_many({ok, link()} | {error, term()}).
add_links(Ctx, Key, TreeId, Links) when is_list(Links) ->
    datastore_apply(Ctx, Key, fun datastore:add_links/4, add_links, [TreeId, Links]);
add_links(Ctx, Key, TreeId, Link) ->
    hd_if_list(add_links(Ctx, Key, TreeId, [Link])).

%%--------------------------------------------------------------------
%% @doc
%% Creates named links between a model document and targets. Checks if links do not exist in selected trees.
%% @end
%%--------------------------------------------------------------------
-spec check_and_add_links(ctx(), key(), tree_id(), tree_ids(),
    one_or_many({link_name(), link_target()})) ->
    one_or_many({ok, link()} | {error, term()}).
check_and_add_links(Ctx, Key, TreeId, CheckTrees, Links) when is_list(Links) ->
    datastore_apply(Ctx, Key, fun datastore:check_and_add_links/5, check_and_add_links, [TreeId, CheckTrees, Links]);
check_and_add_links(Ctx, Key, TreeId, CheckTrees, Link) ->
    hd_if_list(check_and_add_links(Ctx, Key, TreeId, CheckTrees, [Link])).

%%--------------------------------------------------------------------
%% @doc
%% Returns model document links by names.
%% @end
%%--------------------------------------------------------------------
-spec get_links(ctx(), key(), tree_ids(), one_or_many(link_name())) ->
    one_or_many({ok, [link()]} | {error, term()}).
get_links(Ctx, Key, TreeIds, LinkNames) when is_list(LinkNames) ->
    datastore_apply(Ctx, Key, fun datastore:get_links/4, get_links, [TreeIds, LinkNames]);
get_links(Ctx, Key, TreeIds, LinkName) ->
    hd_if_list(get_links(Ctx, Key, TreeIds, [LinkName])).

%%--------------------------------------------------------------------
%% @doc
%% Removes model document links by names and optional revisions.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(ctx(), key(), tree_id(),
    one_or_many(link_name() | {link_name(), link_rev()})) ->
    one_or_many(ok | {error, term()}).
delete_links(Ctx, Key, TreeId, Links) when is_list(Links) ->
    datastore_apply(Ctx, Key, fun datastore:delete_links/4, delete_links, [TreeId, Links]);
delete_links(Ctx, Key, TreeId, Link) ->
    hd_if_list(delete_links(Ctx, Key, TreeId, [Link])).

%%--------------------------------------------------------------------
%% @doc
%% Marks model document links in provided revisions as deleted.
%% @end
%%--------------------------------------------------------------------
-spec mark_links_deleted(ctx(), key(), tree_id(),
    one_or_many(link_name() | {link_name(), link_rev()})) ->
    one_or_many(ok | {error, term()}).
mark_links_deleted(Ctx, Key, TreeId, Links) when is_list(Links) ->
    datastore_apply(Ctx, Key, fun datastore:mark_links_deleted/4, mark_links_deleted, [
        TreeId, Links
    ]);
mark_links_deleted(Ctx, Key, TreeId, Link) ->
    hd_if_list(mark_links_deleted(Ctx, Key, TreeId, [Link])).

%%--------------------------------------------------------------------
%% @doc
%% Calls Fun(Link, Acc) for each model document link.
%% @end
%%--------------------------------------------------------------------
-spec fold_links(ctx(), key(), tree_ids(), fold_fun(link()), fold_acc(),
    fold_opts()) -> {ok, fold_acc()} |
    {{ok, fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_links(Ctx, Key, TreeIds, Fun, Acc, Opts) ->
    datastore_apply(Ctx, Key, fun datastore:fold_links/6, fold_links, [
        TreeIds, Fun, Acc, Opts
    ]).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of IDs of link trees that constitute model document links
%% forest.
%% @end
%%--------------------------------------------------------------------
-spec get_links_trees(ctx(), key()) -> {ok, [tree_id()]} | {error, term()}.
get_links_trees(Ctx, Key) ->
    datastore_apply(Ctx, Key, fun datastore:get_links_trees/2, get_links_trees, []).

%%--------------------------------------------------------------------
%% @doc
%% Executes call on all tp subtrees.
%% @end
%%--------------------------------------------------------------------
-spec datastore_apply_all_subtrees(ctx(), fun(), key(), list()) -> term().
datastore_apply_all_subtrees(Ctx, Fun, UniqueKey, Args) ->
    lists:foldl(fun
        (Ctx2, ok) ->
            erlang:apply(Fun, [Ctx2, UniqueKey | Args]);
        (Ctx2, {ok, _}) ->
            erlang:apply(Fun, [Ctx2, UniqueKey | Args]);
        (_, Error) ->
            Error
    end, ok, datastore_multiplier:get_names(Ctx)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fills context with default parameters, generates unique key and forwards
%% function call to the {@link datastore} module.
%% @end
%%--------------------------------------------------------------------
-spec datastore_apply(ctx(), key(), fun(), atom(), list()) -> term().
datastore_apply(Ctx0, Key, Fun, _FunName, Args) ->
    UniqueKey = get_unique_key(Ctx0, Key),
    Ctx = datastore_model_default:set_defaults(UniqueKey, Ctx0),
    erlang:apply(Fun, [Ctx, UniqueKey | Args]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fills context with default parameters, generates unique key and forwards
%% function call to the {@link datastore} module.
%% Executes call with all multiplies contexts.
%% @end
%%--------------------------------------------------------------------
-spec datastore_apply_all(ctx(), key(), fun(), atom(), list()) -> term().
datastore_apply_all(Ctx0, Key, Fun, _FunName, Args) ->
    Ctx = datastore_model_default:set_defaults(Ctx0),
    UniqueKey = get_unique_key(Ctx, Key),
    Routing = maps:get(routing, Ctx, global),

    case Routing of
        global ->
            lists:foldl(fun
                (Node, ok) ->
                    rpc:call(Node, ?MODULE, datastore_apply_all_subtrees,
                        [Ctx, Fun, UniqueKey, Args]);
                (Node, {ok, _}) ->
                    rpc:call(Node, ?MODULE, datastore_apply_all_subtrees,
                        [Ctx, Fun, UniqueKey, Args]);
                (_, Error) ->
                    Error
            end, ok, consistent_hashing:get_all_nodes());
        _ ->
            datastore_apply_all_subtrees(Ctx, Fun, UniqueKey, Args)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds link that is used for model folding.
%% @end
%%--------------------------------------------------------------------
-spec add_fold_link(ctx(), key(), {ok, doc()} | {error, term()}) ->
    {ok, doc()} | {error, term()}.
add_fold_link(Ctx = #{model := Model, fold_enabled := true}, Key, {ok, Doc}) ->
    Ctx2 = Ctx#{sync_enabled => false},
    Ctx3 = case Ctx of
        #{local_fold := true} -> Ctx2#{routing => local};
        _ -> Ctx2
    end,
    ModelKey = atom_to_binary(Model, utf8),
    case add_links(Ctx3, ModelKey, ?MODEL_ALL_TREE_ID, [{Key, <<>>}]) of
        [{ok, #link{}}] -> {ok, Doc};
        [{error, already_exists}] -> {ok, Doc};
        [{error, Reason}] -> {error, Reason}
    end;
add_fold_link(_Ctx, _Key, Result) ->
    Result.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes link that is used for model folding.
%% @end
%%--------------------------------------------------------------------
-spec delete_fold_link(ctx(), key(), ok | {error, term()}) ->
    ok | {error, term()}.
delete_fold_link(Ctx = #{model := Model, fold_enabled := true}, Key, ok) ->
    Ctx2 = Ctx#{sync_enabled => false},
    Ctx3 = case Ctx of
        #{local_fold := true} -> Ctx2#{routing => local};
        _ -> Ctx2
    end,
    ModelKey = atom_to_binary(Model, utf8),
    case delete_links(Ctx3, ModelKey, ?MODEL_ALL_TREE_ID, [Key]) of
        [ok] -> ok;
        [{error, Reason}] -> {error, Reason}
    end;
delete_fold_link(_Ctx, _Key, Result) ->
    Result.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes all links that are associated with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete_all_links(ctx(), key(), ok | {error, term()}) ->
    ok | {error, term()}.
delete_all_links(Ctx, Key, ok) ->
    Result = fold_links(Ctx, Key, all, fun
        (#link{tree_id = TreeId, name = Name, rev = Rev}, Acc) ->
            Links = maps:get(TreeId, Acc, []),
            {ok, maps:put(TreeId, [{Name, Rev} | Links], Acc)}
    end, #{}, #{}),
    case Result of
        {ok, Trees} ->
            maps:fold(fun
                (TreeId, Links, ok) ->
                    Deleted = case {maps:find(local_links_tree_id, Ctx),
                        maps:find(sync_enabled, Ctx)} of
                        {{ok, LocalTreeId}, _} when LocalTreeId == TreeId ->
                            delete_links(Ctx, Key, TreeId, Links);
                        {_, {ok, true}} ->
                            mark_links_deleted(Ctx, Key, TreeId, Links);
                        _ ->
                            delete_links(Ctx, Key, TreeId, Links)
                    end,
                    Deleted2 = lists:filter(fun
                        ({error, _}) -> true;
                        (ok) -> false
                    end, Deleted),
                    case Deleted2 of
                        [] -> ok;
                        _ -> {error, Deleted2}
                    end;
                (_, _, {error, Reason}) ->
                    {error, Reason}
            end, ok, Trees);
        {error, Reason} ->
            {error, Reason}
    end;
delete_all_links(_Ctx, _Key, Result) ->
    Result.

%%--------------------------------------------------------------------
%% @private
%%--------------------------------------------------------------------
-spec hd_if_list(term()) -> term().
hd_if_list(List) when is_list(List) ->
    hd(List);
hd_if_list(Term) ->
    Term.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls Fun(Doc, Acc) for each key.
%% @end
%%--------------------------------------------------------------------
-spec fold_internal([key()], fold_acc(), ctx(), fold_fun(doc())) ->
    {ok, fold_acc()} | {error, term()}.
fold_internal([], Acc, _Ctx, _Fun) ->
    {ok, Acc};
fold_internal([Key | Tail], Acc, Ctx, Fun) ->
    case get(Ctx, Key) of
        {ok, Doc} ->
            case Fun(Doc, Acc) of
                {ok, Acc2} ->
                    fold_internal(Tail, Acc2, Ctx, Fun);
                {stop, Acc2} ->
                    {ok, Acc2};
                {error, _} = Error ->
                    Error
            end;
        {error, not_found} -> fold_internal(Tail, Acc, Ctx, Fun);
        {error, Reason} -> {error, Reason}
    end.