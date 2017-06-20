%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Main datastore API implementation.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore).
-author("Rafal Slota").

-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include("elements/task_manager/task_manager.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").


%% #document types
-type uuid() :: binary().
% TODO - exclude atom (possible crash because of to large number of atoms usage) or make apropriate WARNING
-type key() :: undefined | uuid() | atom() | integer().
-type ext_key() :: key() | term().
-type document() :: #document{}.
-type rev() ::  couchbase_driver:rev().
-type seq() ::  couchbase_changes:seq().
-type scope() :: binary().
-type mutator() :: binary().
-type value() :: term().
-type document_diff() :: #{term() => term()} | fun((OldValue :: value()) ->
    {ok, NewValue :: value()} | {error, Reason :: term()}).
-type bucket() :: atom() | binary().
-type ctx() :: datastore_context:ctx().
-type option() :: ignore_links.

-export_type([rev/0, seq/0, scope/0, mutator/0]).
-export_type([uuid/0, key/0, ext_key/0, value/0, document/0, document_diff/0,
    bucket/0, ctx/0, option/0]).

-type memory_driver() :: undefined | ets_driver | mnesia_driver.
-type memory_driver_ctx() :: ets_driver:ctx() | mnesia_driver:ctx().
-type disc_driver() :: undefined | couchbase_driver.
-type disc_driver_ctx() :: couchbase_driver:ctx().
-type driver() :: memory_driver() | disc_driver().
-type driver_ctx() :: memory_driver_ctx() | disc_driver_ctx().

-export_type([memory_driver/0, disc_driver/0, driver/0]).
-export_type([memory_driver_ctx/0, disc_driver_ctx/0, driver_ctx/0]).

%% Error types
-type generic_error() :: {error, Reason :: term()}.
-type not_found_error(ObjectType) :: {error, {not_found, ObjectType}}.
-type update_error() :: not_found_error(model_behaviour:model_type() | model_behaviour:model_config()) | generic_error().
-type create_error() :: generic_error() | {error, already_exists}.
-type get_error() :: not_found_error(term()) | generic_error().
-type link_error() :: generic_error() | {error, link_not_found}.

-export_type([generic_error/0, not_found_error/1, update_error/0, create_error/0, get_error/0, link_error/0]).

%% API utility types
-type db_node() :: {Host :: binary(), Port :: integer()}.
-type store_level() :: ?DISK_ONLY_LEVEL | ?LOCAL_ONLY_LEVEL | ?GLOBAL_ONLY_LEVEL
    | ?LOCALLY_CACHED_LEVEL | ?GLOBALLY_CACHED_LEVEL | ?DIRECT_DISK_LEVEL.
-type delete_predicate() :: fun(() -> boolean()).
-type list_fun() :: fun((Obj :: term(), AccIn :: term()) -> {next, Acc :: term()} | {abort, Acc :: term()}).
-type list_keys_fun() :: fun((Key :: ext_key(), AccIn :: term()) -> {next, Acc :: term()} | {abort, Acc :: term()}).
-type exists_return() :: boolean() | no_return().

-export_type([db_node/0, store_level/0, delete_predicate/0, list_fun/0,
    exists_return/0]).

%% Links' types
-type link_version() :: non_neg_integer().
-type link_final_target() :: {links_utils:scope(), links_utils:vhash(), ext_key(), model_behaviour:model_type() | model_behaviour:model_config()}.
-type normalized_link_target() :: {link_version(), [link_final_target()]}.
-type simple_link_target() :: {ext_key(), model_behaviour:model_type() | model_behaviour:model_config()}.
-type link_target() :: #document{} | normalized_link_target() | link_final_target() | simple_link_target().
-type link_name() :: atom() | binary() | {scoped_link, atom() | binary(), links_utils:scope(), links_utils:vhash()}.
-type link_spec() :: {link_name(), link_target()}.
-type normalized_link_spec() :: {link_name(), normalized_link_target()}.

-export_type([link_target/0, link_name/0, link_spec/0, normalized_link_spec/0, normalized_link_target/0,
    link_final_target/0, link_version/0]).

-define(LISTING_ROOT, <<"listing_root">>).

%% API
-export([save/2, update/3, create/2, create_or_update/3,
    get/2, list/3, list_keys/3, del_list_info/3,
    delete/2, delete/3, delete/4, exists/2]).
-export([fetch_link/3, add_links/3, create_link/3, delete_links/3,
    foreach_link/4, fetch_link_target/3, link_walk/4, set_links/3]).
-export([fetch_full_link/3]).
-export([ensure_state_loaded/0, cluster_initialized/0, healthcheck/0,
    initialize_state/1, check_and_initialize_state/2]).
-export([normalize_link_target/2]).
-export([initialize_minimal_env/0, initialize_minimal_env/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves given #document.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), Document :: datastore:document()) ->
    {ok, ext_key()} | generic_error().
save(Ctx, #document{} = Document) ->
    {Ctx2, Document2} = maybe_gen_uuid(Ctx, Document),
    Ans = exec_driver(Ctx2, save, [Document2]),
    add_to_list(Ctx, Ans).

%%--------------------------------------------------------------------
%% @doc
%% Updates given by key document by replacing given fields with new values.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), Key :: ext_key(), Diff :: datastore:document_diff()) ->
    {ok, ext_key()} | datastore:update_error().
update(Ctx, Key, Diff) ->
    exec_driver(Ctx, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Creates new #document.
%% @end
%%--------------------------------------------------------------------
-spec create(ctx(), Document :: datastore:document()) ->
    {ok, ext_key()} | datastore:create_error().
create(Ctx, #document{key = undefined} = Document) ->
    save(Ctx, Document);
create(Ctx, #document{} = Document) ->
    Ans = exec_driver(Ctx, create, [Document]),
    add_to_list(Ctx, Ans).

%%--------------------------------------------------------------------
%% @doc
%% Updates given document by replacing given fields with new values or
%% creates new one if not exists.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(ctx(), Document :: datastore:document(),
    Diff :: datastore:document_diff()) -> {ok, ext_key()} | datastore:generic_error().
create_or_update(Ctx, #document{} = Document, Diff) ->
    Ans = exec_driver(Ctx, create_or_update, [Document, Diff]),
    add_to_list(Ctx, Ans).

%%--------------------------------------------------------------------
%% @doc
%% Gets #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(),
    Key :: ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Ctx, Key) ->
    exec_driver(Ctx, get, [Key]).


%%--------------------------------------------------------------------
%% @doc
%% Executes given function for each model's record. After each record function may interrupt operation.
%% @end
%%--------------------------------------------------------------------
-spec list(ctx(), Fun :: list_fun(), AccIn :: term()) ->
    {ok, Handle :: term()} | generic_error() | no_return().
list(#{list_enabled := false, model_name := MN}, _Fun, _AccIn) ->
    throw({not_supported, {model, MN}});
list(Ctx, Fun, AccIn0) ->
    try
        {ok, ForeachAns} = foreach_link(datastore_context:override(links_tree, true, Ctx),
            ?LISTING_ROOT, fun(Key, _, AccIn) ->
                case get(Ctx, Key) of
                    {ok, Doc} ->
                        case Fun(Doc, AccIn) of
                            {next, NewAcc} ->
                                NewAcc;
                            {abort, NewAcc} ->
                                % TODO - change when abort in foreach is possible
                                throw({aborted, NewAcc})
                        end;
                    {error, {not_found, _}} ->
                        AccIn
                end
            end, AccIn0),
        {abort, EndAcc} = Fun('$end_of_table', ForeachAns),
        {ok, EndAcc}
    catch
        throw:{aborted, FinalAcc} ->
            {ok, FinalAcc};
        _:Reason ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Executes given function for each model's key. After each record function may interrupt operation.
%% @end
%%--------------------------------------------------------------------
-spec list_keys(ctx(), Fun :: list_keys_fun(), AccIn :: term()) ->
    {ok, Handle :: term()} | generic_error() | no_return().
list_keys(#{list_enabled := false, model_name := MN}, _Fun, _AccIn) ->
    throw({not_supported, {model, MN}});
list_keys(Ctx, Fun, AccIn0) ->
    try
        {ok, ForeachAns} = foreach_link(datastore_context:override(links_tree, true, Ctx),
            ?LISTING_ROOT, fun(Key, _, AccIn) ->
                case Fun(Key, AccIn) of
                    {next, NewAcc} ->
                        NewAcc;
                    {abort, NewAcc} ->
                        % TODO - change when abort in foreach is possible
                        throw({aborted, NewAcc})
                end
            end, AccIn0),
        {abort, EndAcc} = Fun('$end_of_table', ForeachAns),
        {ok, EndAcc}
    catch
        throw:{aborted, FinalAcc} ->
            {ok, FinalAcc};
        _:Reason ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(),
    Key :: ext_key(), Pred :: delete_predicate()) -> ok | generic_error().
delete(Ctx, Key, Pred) ->
    delete(Ctx, Key, Pred, []).

%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% You can specify 'ignore_links' option, if links should not be deleted with the document.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(),
    Key :: ext_key(), Pred :: delete_predicate(), Options :: [option()]) -> ok | generic_error().
delete(Ctx, Key, Pred, Opts) ->
    Ans = case exec_driver(Ctx, delete, [Key, Pred]) of
        ok ->
            % TODO - analyse if we should do it by default
            case lists:member(ignore_links, Opts) of
                true -> ok;
                false ->
                    % TODO - make link del asynch when tests will be able to handle it
                    %%             spawn(fun() -> catch delete_links(Level, Key, ModelConfig, all) end),
                    catch delete_links(
                        datastore_context:override(links_tree, true, Ctx),
                        Key, all)
            end,
            ok;
        {error, Reason} ->
            {error, Reason}
    end,
    del_from_list(Ctx, Key, Ans).


%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(),
    Key :: ext_key()) -> ok | generic_error().
delete(Ctx, Key) ->
    delete(Ctx, Key, ?PRED_ALWAYS).


%%--------------------------------------------------------------------
%% @doc
%% Checks if #document with given key exists. This method shall not be used with
%% multiple drivers at once - use *_only levels.
%% @end
%%--------------------------------------------------------------------
-spec exists(ctx(),
    Key :: ext_key()) -> {ok, boolean()} | generic_error().
exists(Ctx, Key) ->
    exec_driver(Ctx, exists, [Key]).


%%--------------------------------------------------------------------
%% @doc
%% Adds given links to the document with given key. Allows for link duplication when model is configured this way.
%% @end
%%--------------------------------------------------------------------
-spec add_links(ctx(), ext_key() | document(), link_spec() | [link_spec()]) ->
    ok | generic_error().
add_links(Ctx, #document{key = Key, scope = Scope}, Links) ->
    add_links(datastore_context:override(scope, Scope, Ctx), Key, Links);
add_links(Ctx, Key, {_LinkName, _LinkTarget} = LinkSpec) ->
    add_links(Ctx, Key, [LinkSpec]);
add_links(#{link_duplication := LD} = Ctx, Key, Links) when is_list(Links) ->
    NormalizedLinks = normalize_link_target(Ctx, Links),
    Method = case LD of
        true -> add_links;
        false -> set_links
    end,
    exec_driver(Ctx, Method, [Key, NormalizedLinks]).


%%--------------------------------------------------------------------
%% @doc
%% Sets given links to the document with given key.
%% @end
%%--------------------------------------------------------------------
-spec set_links(ctx(), ext_key() | document(), link_spec() | [link_spec()]) ->
    ok | generic_error().
set_links(Ctx, #document{key = Key, scope = Scope}, Links) ->
    set_links(datastore_context:override(scope, Scope, Ctx), Key, Links);
set_links(Ctx, Key, {_LinkName, _LinkTarget} = LinkSpec) ->
    set_links(Ctx, Key, [LinkSpec]);
set_links(Ctx, Key, Links) when is_list(Links) ->
    NormalizedLinks = normalize_link_target(Ctx, Links),
    exec_driver(Ctx, set_links, [Key, NormalizedLinks]).


%%--------------------------------------------------------------------
%% @doc
%% Adds given links to the document with given key if link does not exist.
%% @end
%%--------------------------------------------------------------------
-spec create_link(ctx(), ext_key() | document(), link_spec()) ->
    ok | create_error().
create_link(Ctx, #document{key = Key, scope = Scope}, Link) ->
    create_link(datastore_context:override(scope, Scope, Ctx), Key, Link);
create_link(Ctx, Key, Link) ->
    exec_driver(Ctx, create_link, [Key, normalize_link_target(Ctx, Link)]).


%%--------------------------------------------------------------------
%% @doc
%% Removes links from the document with given key. There is special link name 'all' which removes all links.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(ctx(), ext_key() | document(),
    link_name() | [link_name()] | all) -> ok | generic_error().
delete_links(Ctx, #document{key = Key, scope = Scope}, LinkNames) ->
    delete_links(datastore_context:override(scope, Scope, Ctx), Key, LinkNames);
delete_links(Ctx, Key, LinkNames) when is_list(LinkNames); LinkNames =:= all ->
    exec_driver(Ctx, delete_links, [Key, LinkNames]);
delete_links(Ctx, Key, LinkName) ->
    delete_links(Ctx, Key, [LinkName]).


%%--------------------------------------------------------------------
%% @doc
%% Gets specified link from the document given by key.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(ctx(), ext_key() | document(), link_name()) ->
    {ok, simple_link_target()} | link_error().
fetch_link(Ctx, #document{key = Key}, LinkName) ->
    fetch_link(Ctx, Key, LinkName);
fetch_link(#{link_replica_scope := LRS} = Ctx, Key, LinkName) ->
    {RawLinkName, RequestedScope, VHash} = links_utils:unpack_link_scope(undefined, LinkName),
    case fetch_full_link(Ctx, Key, RawLinkName) of
        {ok, {_Version, Targets = [H | _]}} ->
            case RequestedScope of
                undefined ->
                    {_, _, TargetKey, TargetModel} =
                        case links_utils:select_scope_related_link(RawLinkName, RequestedScope, VHash, Targets) of
                            undefined ->
                                case lists:filter(
                                    fun
                                        ({Scope, _, _, _}) ->
                                            Scope == links_utils:get_scopes(
                                                LRS, undefined)
                                    end, Targets) of
                                    [] -> H;
                                    [L | _] ->
                                        L
                                end;
                            ScopeRelated ->
                                ScopeRelated
                        end,
                    {ok, {TargetKey, TargetModel}};
                _ ->
                    case links_utils:select_scope_related_link(RawLinkName, RequestedScope, VHash, Targets) of
                        undefined ->
                            {error, link_not_found};
                        {_, _, TargetKey, TargetModel} ->
                            {ok, {TargetKey, TargetModel}}
                    end
            end;
        Other ->
            Other
    end.


%%--------------------------------------------------------------------
%% @doc
%% Gets specified link from the document given by key.
%% @end
%%--------------------------------------------------------------------
-spec fetch_full_link(ctx(), ext_key() | document(), link_name()) ->
    {ok, normalized_link_target()} | generic_error().
fetch_full_link(Ctx, #document{key = Key}, LinkName) ->
    fetch_full_link(Ctx, Key, LinkName);
fetch_full_link(Ctx, Key, LinkName) ->
    exec_driver(Ctx, fetch_link, [Key, LinkName]).


%%--------------------------------------------------------------------
%% @doc
%% Gets document pointed by given link of document given by key.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link_target(ctx(), ext_key() | document(), link_name()) ->
    {ok, document()} | generic_error().
fetch_link_target(Ctx, #document{key = Key}, LinkName) ->
    fetch_link_target(Ctx, Key, LinkName);
fetch_link_target(Ctx, Key, LinkName) ->
    case fetch_link(Ctx, Key, LinkName) of
        {ok, _Target = {TargetKey, TargetModel}} ->
            TargetModel:get(TargetKey);
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Executes given function for each link of the document given by key - similar to 'foldl'.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(ctx(), ext_key() | document(),
    fun((link_name(), link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | link_error().
foreach_link(Ctx, #document{key = Key}, Fun, AccIn) ->
    foreach_link(Ctx, Key, Fun, AccIn);
foreach_link(Ctx, Key, Fun, AccIn) ->
    exec_driver(Ctx, foreach_link, [Key, Fun, AccIn]).


%%--------------------------------------------------------------------
%% @doc
%% "Walks" from link to link and fetches either all encountered documents
%% (for Mode == get_all - not yet implemented),
%% or just last document (for Mode == get_leaf). Starts on the document given by key.
%% In case of Mode == get_leaf, list of all link's uuids is also returned.
%% @end
%%--------------------------------------------------------------------
-spec link_walk(ctx(), Key :: ext_key() | document(), [link_name()], get_leaf | get_all) ->
    {ok, {document(), [ext_key()]} | [document()]} | link_error() | get_error().
link_walk(Ctx, #document{key = StartKey}, LinkNames, Mode) ->
    link_walk(Ctx, StartKey, LinkNames, Mode);
link_walk(Ctx, Key, R, Mode) ->
    link_walk7(Ctx, Key, R, [], Mode).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback healthcheck/1.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, Reason :: term()}.
healthcheck() ->
    case ets:info(?LOCAL_STATE) of
        undefined ->
            {error, {no_ets, ?LOCAL_STATE}};
        _ -> ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Initializes minimal environment to run test.
%% @end
%%--------------------------------------------------------------------
-spec initialize_minimal_env() -> ok.
initialize_minimal_env() ->
    initialize_minimal_env([]).

%%--------------------------------------------------------------------
%% @doc
%% Initializes minimal environment to run test.
%% @end
%%--------------------------------------------------------------------
-spec initialize_minimal_env([datastore:db_node()]) -> ok.
initialize_minimal_env(DBNodes) ->
    hackney:start(),
    couchbeam_sup:start_link(),
        catch ets:new(datastore_worker, [named_table, public, set, {read_concurrency, true}]),
    datastore:ensure_state_loaded(node()),
    {ok, _} = datastore_worker:init(DBNodes),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% "Walks" from link to link and fetches either all encountered documents
%% (for Mode == get_all - not yet implemented),
%% or just last document (for Mode == get_leaf). Starts on the document given by key.
%% In case of Mode == get_leaf, list of all link's uuids is also returned.
%% @end
%%--------------------------------------------------------------------
-spec link_walk7(ctx(), Key :: ext_key(), [link_name()], Acc :: [ext_key()],
    get_leaf | get_all) ->
    {ok, {document(), [ext_key()]} | [document()]} | link_error() | get_error().
link_walk7(_Ctx, _Key, _Links, _Acc, get_all) ->
    erlang:error(not_inplemented);
link_walk7(Ctx, Key, [LastLink], Acc, get_leaf) ->
    case fetch_link_target(Ctx, Key, LastLink) of
        {ok, #document{key = LastKey} = Leaf} ->
            {ok, {Leaf, lists:reverse([LastKey | Acc])}};
        {error, Reason} ->
            {error, Reason}
    end;
link_walk7(Ctx, Key, [NextLink | R], Acc, get_leaf) ->
    case fetch_link(Ctx, Key, NextLink) of
        {ok, {TargetKey, TargetMod}} ->
            % TODO - links refactoring - what we need inside link_walk?
            link_walk7(model:create_datastore_context(link_walk, [Key], TargetMod),
                TargetKey, R, [TargetKey | Acc], get_leaf);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Normalize targets of links.
%% @end
%%--------------------------------------------------------------------
% TODO - spec
%%-spec normalize_link_target(ctx(), [link_spec()]) ->
%%    [{link_name(), normalized_link_target()}].
normalize_link_target(_, {_LinkName, {_Version, [{_ScopeId, _VHash, _TargetKey, ModelName} | _]}} = ValidLink) when is_atom(ModelName) ->
    ValidLink;
normalize_link_target(_, []) ->
    [];
normalize_link_target(Ctx, [{_, {V, []}} | R]) when is_integer(V) ->
    normalize_link_target(Ctx, R);
normalize_link_target(Ctx, [Link | R]) ->
    [normalize_link_target(Ctx, Link) | normalize_link_target(Ctx, R)];
normalize_link_target(#{link_replica_scope := LRS} = Ctx,
    {LinkName, #document{key = TargetKey} = Doc}) ->
    normalize_link_target(Ctx, {LinkName, {links_utils:get_scopes(LRS, undefined), links_utils:gen_vhash(), TargetKey, model_name(Doc)}});
normalize_link_target(#{link_replica_scope := LRS} = Ctx,
    {LinkName, {TargetKey, ModelName}}) when is_atom(ModelName) ->
    normalize_link_target(Ctx, {LinkName, {links_utils:get_scopes(LRS, undefined), links_utils:gen_vhash(), TargetKey, ModelName}});
normalize_link_target(Ctx, {LinkName, {_ScopeId, _VHash, _TargetKey, _ModelName} = Target}) ->
    normalize_link_target(Ctx, {LinkName, {1, [Target]}}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% For given #document generates key if undefined and returns updated #document structure.
%% @end
%%--------------------------------------------------------------------
-spec maybe_gen_uuid(ctx(), document()) -> {ctx(), document()}.
maybe_gen_uuid(Ctx, #document{key = undefined} = Doc) ->
    {datastore_context:override(generated_uuid, true, Ctx),
        Doc#document{key = datastore_utils:gen_uuid()}};
maybe_gen_uuid(Ctx, #document{} = Doc) ->
    {Ctx, Doc}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets model name for given document/record.
%% @end
%%--------------------------------------------------------------------
-spec model_name(ctx() | tuple() | document() | model_behaviour:model_config()
    | model_behaviour:model_type()) -> model_behaviour:model_type().
model_name(#document{value = Record}) ->
    model_name(Record);
model_name(#model_config{name = ModelName}) ->
    ModelName;
model_name(Record) when is_tuple(Record) ->
    element(1, Record);
model_name(ModelName) when is_atom(ModelName) ->
    ModelName;
model_name(#{model_name := ModelName} = _Ctx) ->
    ModelName.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets model config for given document/record/model name.
%% @end
%%--------------------------------------------------------------------
-spec model_config(tuple() | document() | model_behaviour:model_type()) -> model_behaviour:model_config().
model_config(#model_config{} = MC) ->
    MC;
model_config(ModelNameOrConfig) ->
    ModelName = model_name(ModelNameOrConfig),
    ModelName:model_init().


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs all pre-hooks for given model, method and context.
%% @end
%%--------------------------------------------------------------------
-spec run_prehooks(ctx(),
    Method :: model_behaviour:model_action(), Context :: term()) ->
    ok | {error, Reason :: term()}.
% TODO - new concept for hooks
run_prehooks(#{level := Level, hooks_config := HooksConfig} = Ctx,
    Method, Args) ->
    case HooksConfig of
        run_hooks ->
           ModelName = model_name(Ctx),
           Hooked = ets:lookup(?LOCAL_STATE, {ModelName, Method}),
           HooksRes =
               lists:map(
                   fun({_, HookedModule}) ->
                       HookedModule:before(ModelName, Method, Level, Args)
                   end, Hooked),
           case [Filtered || Filtered <- HooksRes, Filtered /= ok] of
               [] -> ok;
               [Interrupt | _] ->
                   Interrupt
           end;
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs asynchronously all post-hooks for given model, method, context and
%% return value. Returns given return value.
%% @end
%%--------------------------------------------------------------------
-spec run_posthooks(ctx(),
    Model :: model_behaviour:model_action(),
    Context :: term(), ReturnValue) -> ReturnValue when ReturnValue :: term().
run_posthooks(#{level := Level, hooks_config := HooksConfig} = Ctx,
    Method, Args, Return) ->
    case HooksConfig of
        run_hooks ->
            ModelName = model_name(Ctx),
            Hooked = ets:lookup(?LOCAL_STATE, {ModelName, Method}),
            lists:foreach(
                fun({_, HookedModule}) ->
                    spawn(fun() ->
                        HookedModule:'after'(ModelName, Method, Level, Args, Return) end)
                end, Hooked),
            Return;
        _ ->
            Return
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes local (node scope) datastore state with given models.
%% Returns initialized configuration.
%% @end
%%--------------------------------------------------------------------
-spec load_local_state(Models :: [model_behaviour:model_type() | model_behaviour:model_config()]) ->
    [model_behaviour:model_config()].
load_local_state(Models) ->
    catch ets:new(?LOCAL_STATE, [named_table, public, bag]),
        catch ets:new(monitoring_ets, [named_table, public, set]),
    lists:map(
        fun(ModelName) ->
            Config = #model_config{hooks = Hooks} = model_config(ModelName),
            ok = model:validate_model_config(Config),
            lists:foreach(
                fun(Hook) ->
                    ets:insert(?LOCAL_STATE, {Hook, ModelName})
                end, Hooks),
            ets:insert(?LOCAL_STATE, {ModelName, state_loaded}),
            Config
        end, Models).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs init_bucket/1 for each datastore driver using given models'.
%% @end
%%--------------------------------------------------------------------
-spec init_drivers(Configs :: [model_behaviour:model_config()], NodeToSync :: node()) ->
    ok | no_return().
init_drivers(Configs, _NodeToSync) ->
    lists:foreach(
        fun(Config)->
            ok = apply(?LOCAL_SLAVE_DRIVER, init,
                [model:make_memory_ctx(Config, true), []]),
            ok = apply(?GLOBAL_SLAVE_DRIVER, init,
                [model:make_memory_ctx(Config, false), []])
        end, Configs).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% If needed - loads local state, initializes datastore drivers and fetches active config.
%% @end
%%--------------------------------------------------------------------
-spec ensure_state_loaded() -> ok | {error, Reason :: term()}.
ensure_state_loaded() ->
    {ok, NodeToSync} = gen_server2:call({global, ?CLUSTER_MANAGER}, get_node_to_sync),
    ensure_state_loaded(NodeToSync),
    ?info("Datastore synchronized").

%%--------------------------------------------------------------------
%% @private
%% @doc
%% If needed - loads local state, initializes datastore drivers and fetches active config
%% from NodeToSync if needed.
%% @end
%%--------------------------------------------------------------------
-spec ensure_state_loaded(NodeToSync :: node()) -> ok | {error, Reason :: term()}.
ensure_state_loaded(NodeToSync) ->
    case ets:info(?LOCAL_STATE) of
        undefined ->
            initialize_state(NodeToSync);
        _ -> ok
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Loads local state, initializes datastore drivers and fetches active config
%% from NodeToSync if needed.
%% @end
%%--------------------------------------------------------------------
-spec initialize_state(NodeToSync :: node()) -> ok | {error, Reason :: term()}.
initialize_state(NodeToSync) ->
    Models = datastore_config:models(),
    initialize_state(NodeToSync, Models).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Loads local state, initializes datastore drivers and fetches active config
%% from NodeToSync if needed.
%% @end
%%--------------------------------------------------------------------
-spec initialize_state(NodeToSync :: node(),
    Models :: [model_behaviour:model_type()]) -> ok | {error, Reason :: term()}.
initialize_state(NodeToSync, Models) ->
    try
        Configs = load_local_state(Models),
        init_drivers(Configs, NodeToSync)
    catch
        Type:Reason ->
            ?error_stacktrace("Cannot initialize datastore local state due to"
            " ~p: ~p", [Type, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Loads local state, initializes datastore drivers and fetches active config
%% from NodeToSync if needed. First, checks if models exist.
%% @end
%%--------------------------------------------------------------------
-spec check_and_initialize_state(NodeToSync :: node(),
    Models :: [model_behaviour:model_type()]) -> ok | {error, Reason :: term()}.
check_and_initialize_state(NodeToSync, Models) ->
    FilteredModels = lists:filter(fun(ModelName) ->
        case ets:lookup(?LOCAL_STATE, ModelName) of
            [{ModelName, state_loaded}] -> false;
            _ -> true
        end
    end, Models),
    initialize_state(NodeToSync, FilteredModels).

%%--------------------------------------------------------------------
%% @doc
%% Ends datastore initialization after whole cluster is set-up.
%% @end
%%--------------------------------------------------------------------
-spec cluster_initialized() -> ok.
cluster_initialized() ->
    {ok, MaxNum} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_max_memory_proc_number),
    tp:set_processes_limit(MaxNum).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes given model action on given driver(s).
%% @end
%%--------------------------------------------------------------------
-spec exec_driver(ctx(), Method :: store_driver_behaviour:driver_action(),
    [term()]) -> ok | {ok, term()} | generic_error().
exec_driver(#{driver := Driver} = Ctx, Method, Args) ->
    Return =
        case run_prehooks(Ctx, Method, Args) of
            ok ->
                FinalArgs = [Method, Ctx, Args],
                erlang:apply(Driver, call, FinalArgs);
            {error, Reason} ->
                {error, Reason}
        end,
    run_posthooks(Ctx, Method, Args, Return).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if model can be listed and adds information needed for listing.
%% @end
%%--------------------------------------------------------------------
-spec add_to_list(ctx(), Ans :: {ok, term()} | generic_error()) ->
    FinalAns :: {ok, term()} | generic_error().
add_to_list(#{list_enabled := false}, Ans) ->
    Ans;
add_to_list(#{list_enabled := {true, return_errors}} = Ctz, Ans) ->
    add_list_info(Ctz, Ans);
add_to_list(Ctz, Ans) ->
    spawn(fun() -> add_list_info(Ctz, Ans) end),
    Ans.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if model can be listed and deletes not used information.
%% @end
%%--------------------------------------------------------------------
-spec del_from_list(ctx(), Key :: ext_key(),
    Ans :: {ok, term()} | generic_error()) ->
    FinalAns :: {ok, term()} | generic_error().
del_from_list(#{list_enabled := false}, _Key, Ans) ->
    Ans;
del_from_list(#{list_enabled := {true, return_errors}} = Ctx, Key, Ans) ->
    del_list_info(Ctx, Key, Ans);
del_from_list(Ctx, Key, Ans) ->
    spawn(fun() -> del_list_info(Ctx, Key, Ans) end),
    Ans.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds information needed for listing.
%% @end
%%--------------------------------------------------------------------
-spec add_list_info(ctx(), Ans :: {ok, term()} | generic_error()) ->
    FinalAns :: {ok, term()} | generic_error().
add_list_info(#{model_name := MN} = Ctx, {ok, Key} = Ans) ->
    Ctx2 = datastore_context:override(links_tree, true, Ctx),
    Ctx3 = datastore_context:override(volatile, false, Ctx2),
    case add_links(Ctx3, ?LISTING_ROOT, [{Key, {Key, MN}}]) of
        ok ->
            Ans;
        Error ->
            Error
    end;
add_list_info(_Ctx, Ans) ->
    Ans.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes not used listing information.
%% @end
%%--------------------------------------------------------------------
-spec del_list_info(ctx(), Key :: ext_key(),
    Ans :: {ok, term()} | generic_error()) ->
    FinalAns :: {ok, term()} | generic_error().
del_list_info(Ctx, Key, ok) ->
    Ctx2 = datastore_context:override(links_tree, true, Ctx),
    Ctx3 = datastore_context:override(volatile, false, Ctx2),
    delete_links(Ctx3, ?LISTING_ROOT, Key);
del_list_info(_Ctx, _Key, Ans) ->
    Ans.