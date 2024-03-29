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
-export([init/1, get_unique_key/2, get_generic_key/2]).
-export([datastore_apply/4]).
-export([create/2, save/2, save_with_routing_key/2, update/3, update/4]).
-export([get/2, exists/2]).
-export([delete/2, delete/3, delete_all/1]).
-export([fold/3, fold/5, fold_keys/3, local_fold_all_nodes/3]).
-export([add_links/4, check_and_add_links/5, get_links/4, delete_links/4, mark_links_deleted/4]).
-export([fold_links/6]).
-export([get_links_trees/2]).
-export([ensure_forest_in_changes/3]).
-export([fold_memory_keys/2]).
-export([set_expiry/2, ensure_expiry_set_on_delete/1]).
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
% NOTE: datastore:key() in following functions is UniqueKey used internally by datastore (used also for routing)
%       so it differs from key included in document (UniqueKey is generated using key from document)
-type memory_fold_fun() :: fun(({model(), datastore:key(), datastore:doc()} | end_of_memory, Acc :: term()) ->
    {ok | stop, NewAcc :: term()}).
-type driver_fold_fun() :: fun((datastore:key(), datastore:doc(), Acc :: term()) -> {ok | stop, NewAcc :: term()}).

-export_type([model/0, record/0, record_struct/0, record_version/0,
    key/0, ctx/0, diff/0, fold_opts/0, tree_ids/0, driver_fold_fun/0]).

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
-spec get_unique_key(model(), key()) -> key().
% TODO - VFS-3975 - allow routing via generic key without model
% problem with links application that need such routing key
get_unique_key(Model, Key) ->
    datastore_key:build_adjacent(atom_to_binary(Model, utf8), Key).

-spec get_generic_key(model(), key()) -> key() | undefined.
get_generic_key(Model, Key) ->
    datastore_key:remove_extension(atom_to_binary(Model, utf8), Key).

%%--------------------------------------------------------------------
%% @doc
%% Fills context with default parameters, generates unique key and forwards
%% function call to the {@link datastore} module.
%% @end
%%--------------------------------------------------------------------
-spec datastore_apply(ctx(), key(), fun(), list()) -> term().
datastore_apply(#{model := Model} = Ctx0, Key, Fun, Args) ->
    UniqueKey = get_unique_key(Model, Key),
    Ctx = datastore_model_default:set_defaults(UniqueKey, Ctx0),
    erlang:apply(Fun, [Ctx, UniqueKey | Args]).

%%--------------------------------------------------------------------
%% @doc
%% Creates model document in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec create(ctx(), doc()) -> {ok, doc()} | {error, term()}.
create(#{secure_fold_enabled := true} = Ctx, Doc) ->
    UpdatedCtx = maps:remove(secure_fold_enabled, Ctx#{fold_enabled => true}),
    fold_critical_section(Ctx, fun() -> create(UpdatedCtx, Doc) end);
create(Ctx, Doc = #document{key = undefined}) ->
    save(Ctx, Doc);
create(Ctx, Doc = #document{key = Key}) ->
    Result = datastore_apply(Ctx, Key, fun datastore:create/3, [Doc]),
    add_fold_link(Ctx, Key, Result).

%%--------------------------------------------------------------------
%% @doc
%% Saves model document in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), doc()) -> {ok, doc()} | {error, term()}.
save(#{secure_fold_enabled := true}, _Doc) ->
    {error, not_supported};
save(Ctx, Doc = #document{key = undefined}) ->
    Ctx2 = Ctx#{generated_key => true},
    Doc2 = Doc#document{key = datastore_key:new()},
    save(Ctx2, Doc2);
save(Ctx, Doc = #document{key = Key}) ->
    Result = datastore_apply(Ctx, Key, fun datastore:save/3, [Doc]),
    add_fold_link(Ctx, Key, Result).

%%--------------------------------------------------------------------
%% @doc
%% Saves model document with given routing key in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec save_with_routing_key(ctx(), doc()) -> {ok, doc()} | {error, term()}.
save_with_routing_key(#{routing_key := RoutingKey} = Ctx, Doc = #document{key = Key}) ->
    Ctx1 = datastore_model_default:set_defaults(Ctx),
    Ctx2 = datastore_multiplier:extend_name(RoutingKey, Ctx1),
    datastore_router:route(save, [Ctx2, Key, Doc]).

%%--------------------------------------------------------------------
%% @doc
%% Updates model document in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff()) -> {ok, doc()} | {error, term()}.
update(Ctx, Key, Diff) ->
    datastore_apply(Ctx, Key, fun datastore:update/3, [Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Updates model document in a datastore. If document is missing,
%% creates default one.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff(), record() | doc()) ->
    {ok, doc()} | {error, term()}.
update(#{secure_fold_enabled := true}, _Key, _Diff, _Default = #document{}) ->
    {error, not_supported};
update(Ctx, Key, Diff, Default = #document{}) ->
    Result = datastore_apply(Ctx, Key, fun datastore:update/4, [
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
    datastore_apply(Ctx, Key, fun datastore:get/2, []).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether model document exists in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec exists(ctx(), key()) -> {ok, boolean()} | {error, term()}.
exists(Ctx, Key) ->
    datastore_apply(Ctx, Key, fun datastore:exists/2, []).

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
delete(#{secure_fold_enabled := true} = Ctx, Key, Pred) ->
    UpdatedCtx = maps:remove(secure_fold_enabled, Ctx#{fold_enabled => true}),
    fold_critical_section(Ctx, fun() -> delete(UpdatedCtx, Key, Pred) end);
delete(Ctx, Key, Pred) ->
    Result = datastore_apply(ensure_expiry_set_on_delete(Ctx), Key, fun datastore:delete/3, [Pred]),
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
fold(Ctx0 = #{fold_enabled := true}, Fun, KeyFilter, ReverseKeys, Acc) ->
    {Ctx, ModelKey} = get_fold_ctx_and_key(Ctx0),
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
fold(Ctx = #{secure_fold_enabled := true}, Fun, KeyFilter, ReverseKeys, Acc) ->
    fold(Ctx#{fold_enabled => true}, Fun, KeyFilter, ReverseKeys, Acc);
fold(_Ctx, _Fun, _KeyFilter, _ReverseKeys, _Acc) ->
    {error, not_supported}.

%%--------------------------------------------------------------------
%% @doc
%% Calls Fun(Key, Acc) for each model document key in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec fold_keys(ctx(), fold_fun(key()), fold_acc()) ->
    {ok, fold_acc()} | {error, term()}.
fold_keys(Ctx0 = #{fold_enabled := true}, Fun, Acc) ->
    {Ctx, ModelKey} = get_fold_ctx_and_key(Ctx0),
    fold_links(Ctx, ModelKey, ?MODEL_ALL_TREE_ID, fun(#link{name = Key}, Acc2) ->
        Fun(Key, Acc2)
    end, Acc, #{});
fold_keys(Ctx = #{secure_fold_enabled := true}, Fun, Acc) ->
    fold(Ctx#{fold_enabled => true}, Fun, Acc);
fold_keys(_Ctx, _Fun, _Acc) ->
    {error, not_supported}.

%%--------------------------------------------------------------------
%% @doc
%% Performs fold on each node. Function is intended to work for models with
%% local_fold set on true (nodes store separate lists of documents managed by them).
%% @end
%%--------------------------------------------------------------------
-spec local_fold_all_nodes(ctx(), fold_fun(doc()), fold_acc()) -> {ok, fold_acc()} | {error, term()}.
local_fold_all_nodes(Ctx = #{local_fold := true}, Fun, InitialAcc) ->
    Nodes = consistent_hashing:get_all_nodes(),
    lists:foldl(fun
        (Node, {ok, Acc}) -> rpc:call(Node, datastore_model, fold, [Ctx, Fun, Acc]);
        (_Node, Error) -> Error
    end, {ok, InitialAcc}, Nodes);
local_fold_all_nodes(_Ctx, _Fun, _Acc) ->
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
    datastore_apply(Ctx, Key, fun datastore:add_links/4, [TreeId, Links]);
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
    datastore_apply(Ctx, Key, fun datastore:check_and_add_links/5, [TreeId, CheckTrees, Links]);
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
    datastore_apply(Ctx, Key, fun datastore:get_links/4, [TreeIds, LinkNames]);
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
    datastore_apply(Ctx, Key, fun datastore:delete_links/4, [TreeId, Links]);
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
    datastore_apply(Ctx, Key, fun datastore:mark_links_deleted/4, [
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
    datastore_apply(Ctx, Key, fun datastore:fold_links/6, [
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
    datastore_apply(Ctx, Key, fun datastore:get_links_trees/2, []).

-spec ensure_forest_in_changes(ctx(), key(), tree_id()) -> ok | {error, term()}.
ensure_forest_in_changes(Ctx, Key, TreeId) ->
    datastore_apply(Ctx, Key, fun datastore:ensure_forest_in_changes/3, [TreeId]).

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

-spec fold_memory_keys(memory_fold_fun(), term()) -> term().
fold_memory_keys(Fun, Acc0) ->
    Models = datastore_config:get_models(),
    FoldlAns = lists:foldl(fun
        (Model, {ok, Acc}) ->
            case fold_memory_keys(Model, Fun, Acc) of
                {error, not_supported} -> {ok, Acc}; % Ignore - it is not memory_only model
                Acc2 -> Acc2
            end;
        (_, {stop, Acc}) ->
            {stop, Acc}
    end, {ok, Acc0}, Models),
    case FoldlAns of
        {ok, Acc3} ->
            {_, FinalAcc} = Fun(end_of_memory, Acc3),
            FinalAcc;
        {stop, Acc3} ->
            Acc3
    end.

-spec get_fold_ctx_and_key(ctx()) -> {ctx(), key()}.
get_fold_ctx_and_key(#{model := Model} = Ctx) ->
    case Ctx of
        #{local_fold := true} ->
            NodeModelKey = <<(atom_to_binary(Model, utf8))/binary, "###", (atom_to_binary(node(), utf8))/binary>>,
            {Ctx#{routing => local}, NodeModelKey};
        _ ->
            {Ctx, atom_to_binary(Model, utf8)}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets expiry field in context.
%% Couchbase treats numbers smaller or equal to 2592000 as document's time to live
%% and greater values as timestamp of deletion
%% Translate expiry time to couchbase format
%% @end
%%--------------------------------------------------------------------
-spec set_expiry(ctx(), non_neg_integer()) -> ctx().
set_expiry(Ctx, Expiry) ->
    couchbase_driver:set_expiry(Ctx, Expiry).


-spec ensure_expiry_set_on_delete(ctx() | datastore:disc_driver_ctx()) -> ctx() | datastore:disc_driver_ctx().
ensure_expiry_set_on_delete(Ctx = #{disc_driver := undefined}) ->
    Ctx;
ensure_expiry_set_on_delete(Ctx) ->
    case Ctx of
        #{expiry := _} ->
            Ctx;
        #{sync_enabled := true, disc_driver_ctx := DriverCtx} ->
            UpdatedCtx = #{expiry := Expiry} =
                set_expiry(Ctx, application:get_env(?CLUSTER_WORKER_APP_NAME, document_expiry, ?EXPIRY)),
            UpdatedCtx#{disc_driver_ctx := set_expiry(DriverCtx, Expiry)};
        #{sync_enabled := true} ->
            set_expiry(Ctx, application:get_env(?CLUSTER_WORKER_APP_NAME, document_expiry, ?EXPIRY));
        #{disc_driver_ctx := DriverCtx} ->
            UpdatedCtx = #{expiry := Expiry} = set_expiry(Ctx, 1),
            UpdatedCtx#{disc_driver_ctx := set_expiry(DriverCtx, Expiry)};
        _ ->
            set_expiry(Ctx, 1)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fills context with default parameters, generates unique key and forwards
%% function call to the {@link datastore} module.
%% Executes call with all multiplies contexts.
%% @end
%%--------------------------------------------------------------------
-spec datastore_apply_all(ctx(), key(), fun(), atom(), list()) -> term().
datastore_apply_all(#{model := Model} = Ctx0, Key, Fun, _FunName, Args) ->
    Ctx = datastore_model_default:set_defaults(Ctx0),
    UniqueKey = get_unique_key(Model, Key),
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
                (Node, {error, nodedown}) ->
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
add_fold_link(Ctx = #{fold_enabled := true}, Key, {ok, Doc}) ->
    Ctx2 = Ctx#{sync_enabled => false},
    {Ctx3, ModelKey} = get_fold_ctx_and_key(Ctx2),
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
delete_fold_link(Ctx = #{fold_enabled := true}, Key, ok) ->
    Ctx2 = Ctx#{sync_enabled => false},
    {Ctx3, ModelKey} = get_fold_ctx_and_key(Ctx2),
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

%% @private
-spec fold_memory_keys(model() | ctx(), memory_fold_fun(), term()) -> {ok | stop, term()} | {error, not_supported}.
fold_memory_keys(#{disc_driver := DD}, _Fun, _Acc0) when DD =/= undefined ->
    {error, not_supported};
fold_memory_keys(#{memory_driver := undefined}, _Fun, _Acc0) ->
    {error, not_supported};
fold_memory_keys(#{
    model := Model,
    memory_driver := Driver,
    memory_driver_ctx := Ctx
}, Fun, Acc0) ->
    FoldlFun = fun(Key, Doc, Acc) ->
        Fun({Model, Key, Doc}, Acc)
    end,

    lists:foldl(fun
        (Ctx2, {ok, Acc}) -> Driver:fold(Ctx2, FoldlFun, Acc);
        (_, {stop, Acc}) -> {stop, Acc}
    end, {ok, Acc0}, datastore_multiplier:get_names(Ctx));
fold_memory_keys(Model, Fun, Acc0) ->
    fold_memory_keys(datastore_model_default:get_ctx(Model), Fun, Acc0).

%% @private
-spec fold_critical_section(ctx(), fun (() -> Result :: term())) -> Result :: term().
fold_critical_section(#{model := Model}, Fun) ->
    critical_section:run([model_fold, Model], Fun).