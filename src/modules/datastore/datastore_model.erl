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
-include("exometer_utils.hrl").
-include("global_definitions.hrl").


%% API
-export([init/1, get_unique_key/2]).
-export([create/2, save/2, update/3, update/4]).
-export([get/2, exists/2]).
-export([delete/2, delete/3]).
-export([fold/3, fold_keys/3]).
-export([add_links/4, get_links/4, delete_links/4, mark_links_deleted/4]).
-export([fold_links/6]).
-export([get_links_trees/2]).
-export([init_counters/0, init_report/0]).

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

-export_type([model/0, record/0, record_struct/0, record_version/0]).

-define(EXOMETER_HISTOGRAM_COUNTERS,
    [save, update, create, create_or_update, get, delete, exists, add_links, 
        set_links, create_link, delete_links, fetch_link, foreach_link, 
        mark_links_deleted, get_links, fold_links, get_links_trees
    ]).

-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE,
  list_to_atom(atom_to_list(Param) ++ "_time"))).
-define(EXOMETER_DEFAULT_TIME_SPAN, 600000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes all counters.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
    TimeSpan = application:get_env(?CLUSTER_WORKER_APP_NAME,
        exometer_datastore_time_span, ?EXOMETER_DEFAULT_TIME_SPAN),
    Counters = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), histogram, TimeSpan}
    end, ?EXOMETER_HISTOGRAM_COUNTERS),
    ?init_counters(Counters).

%%--------------------------------------------------------------------
%% @doc
%% Subscribe for reports for all parameters.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    Reports = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), [min, max, median, mean, n]}
    end, ?EXOMETER_HISTOGRAM_COUNTERS),
    ?init_reports(Reports).

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
    datastore_utils:gen_key(atom_to_binary(Model, utf8), Key).

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
    Doc2 = Doc#document{key = datastore_utils:gen_key()},
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
delete(Ctx, Key, Pred) ->
    Result = datastore_apply(Ctx, Key, fun datastore:delete/3, delete, [Pred]),
    delete_all_links(Ctx, Key, Result),
    delete_fold_link(Ctx, Key, Result).

%%--------------------------------------------------------------------
%% @doc
%% Calls Fun(Doc, Acc) for each model document in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec fold(ctx(), fold_fun(doc()), fold_acc()) ->
    {ok, fold_acc()} | {error, term()}.
fold(Ctx = #{model := Model, fold_enabled := true}, Fun, Acc) ->
    ModelKey = atom_to_binary(Model, utf8),
    fold_links(Ctx, ModelKey, ?MODEL_ALL_TREE_ID, fun(#link{name = Key}, Acc2) ->
        case get(Ctx, Key) of
            {ok, Doc} -> Fun(Doc, Acc2);
            {error, not_found} -> {ok, Acc2};
            {error, Reason} -> {error, Reason}
        end
    end, Acc, #{});
fold(_Ctx, _Fun, _Acc) ->
    {error, not_supported}.

%%--------------------------------------------------------------------
%% @doc
%% Calls Fun(Key, Acc) for each model document key in a datastore.
%% @end
%%--------------------------------------------------------------------
-spec fold_keys(ctx(), fold_fun(key()), fold_acc()) ->
    {ok, fold_acc()} | {error, term()}.
fold_keys(Ctx = #{model := Model, fold_enabled := true}, Fun, Acc) ->
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
    fold_opts()) -> {ok, fold_acc()} | {error, term()}.
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
datastore_apply(Ctx0, Key, Fun, FunName, Args) ->
    Before = os:timestamp(),
    Ctx = datastore_model_default:set_defaults(Ctx0),
    UniqueKey = get_unique_key(Ctx, Key),
    Ctx2 = datastore_multiplier:extend_name(UniqueKey, Ctx),
	Ans = erlang: apply(Fun, [Ctx2, UniqueKey | Args]),
    After = os:timestamp(),
	?update_counter(?EXOMETER_NAME(FunName), timer:now_diff(After, Before)),
    Ans.


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
    ModelKey = atom_to_binary(Model, utf8),
    case add_links(Ctx2, ModelKey, ?MODEL_ALL_TREE_ID, [{Key, <<>>}]) of
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
    ModelKey = atom_to_binary(Model, utf8),
    case delete_links(Ctx2, ModelKey, ?MODEL_ALL_TREE_ID, [Key]) of
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
                    Deleted = case maps:find(local_links_tree_id, Ctx) of
                        {ok, LocalTreeId} when LocalTreeId == TreeId ->
                            delete_links(Ctx, Key, TreeId, Links);
                        _ ->
                            mark_links_deleted(Ctx, Key, TreeId, Links)
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