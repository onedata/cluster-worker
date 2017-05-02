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
-type doc() :: #document2{}.
-type rev() ::  couchbase_driver:rev().
-type seq() ::  couchbase_changes:seq().
-type scope() :: binary().
-type mutator() :: binary().
-type value() :: term().
-type document_diff() :: #{term() => term()} | fun((OldValue :: value()) ->
    {ok, NewValue :: value()} | {error, Reason :: term()}).
-type bucket() :: atom() | binary().
-type opt_ctx() :: datastore_context:ctx().
-type option() :: ignore_links.

-export_type([doc/0, rev/0, seq/0, scope/0, mutator/0]).
-export_type([uuid/0, key/0, ext_key/0, value/0, document/0, document_diff/0,
    bucket/0, opt_ctx/0, option/0]).

-type memory_driver() :: ets_driver | mnesia_driver.
-type memory_driver_ctx() :: ets_driver:ctx() | mnesia_driver:ctx().
-type disc_driver() :: couchbase_driver.
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
-type store_level() :: ?DISK_ONLY_LEVEL | ?LOCAL_ONLY_LEVEL | ?GLOBAL_ONLY_LEVEL | ?LOCALLY_CACHED_LEVEL | ?GLOBALLY_CACHED_LEVEL.
-type delete_predicate() :: fun(() -> boolean()).
-type list_fun() :: fun((Obj :: term(), AccIn :: term()) -> {next, Acc :: term()} | {abort, Acc :: term()}).
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

%% API
-export([save/2, update/3, create/2, create_or_update/3,
    get/2, list/3, list/4, list_dirty/3, delete/2, delete/3, delete/4, exists/2]).
-export([fetch_link/3, add_links/3, create_link/3, delete_links/3,
    foreach_link/4, fetch_link_target/3, link_walk/4, set_links/3]).
-export([fetch_full_link/3, exists_link_doc/3, get_link_doc/4]).
-export([configs_per_bucket/1, ensure_state_loaded/0, cluster_initialized/0, healthcheck/0, level_to_driver/1,
    driver_to_module/1, initialize_state/1]).
-export([run_transaction/1, run_transaction/2, normalize_link_target/2]).
-export([initialize_minimal_env/0, initialize_minimal_env/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves given #document.
%% @end
%%--------------------------------------------------------------------
-spec save(opt_ctx(), Document :: datastore:document()) ->
    {ok, ext_key()} | generic_error().
save(Ctx, #document{} = Document) ->
    exec_driver(Ctx, save, [maybe_gen_uuid(Document)]).

%%--------------------------------------------------------------------
%% @doc
%% Updates given by key document by replacing given fields with new values.
%% @end
%%--------------------------------------------------------------------
-spec update(opt_ctx(), Key :: ext_key(), Diff :: datastore:document_diff()) ->
    {ok, ext_key()} | datastore:update_error().
update(Ctx, Key, Diff) ->
    exec_driver(Ctx, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Creates new #document.
%% @end
%%--------------------------------------------------------------------
-spec create(opt_ctx(), Document :: datastore:document()) ->
    {ok, ext_key()} | datastore:create_error().
create(Ctx, #document{} = Document) ->
    exec_driver(Ctx, create, [maybe_gen_uuid(Document)]).

%%--------------------------------------------------------------------
%% @doc
%% Updates given document by replacing given fields with new values or
%% creates new one if not exists.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(opt_ctx(), Document :: datastore:document(),
    Diff :: datastore:document_diff()) -> {ok, ext_key()} | datastore:create_error().
create_or_update(Ctx, #document{} = Document, Diff) ->
    exec_driver(Ctx, create_or_update, [Document, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Gets #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec get(opt_ctx(),
    Key :: ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Ctx, Key) ->
    exec_driver(Ctx, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Dirty alternative of list/4.
%% Executes given function for each model's record. After each record function may interrupt operation.
%% @end
%%--------------------------------------------------------------------
-spec list_dirty(opt_ctx(), Fun :: list_fun(), AccIn :: term()) ->
    {ok, Handle :: term()} | generic_error() | no_return().
list_dirty(Ctx, Fun, AccIn) ->
    list(Ctx, Fun, AccIn, [{mode, dirty}]).


%%--------------------------------------------------------------------
%% @doc
%% Executes given function for each model's record. After each record function may interrupt operation.
%% @end
%%--------------------------------------------------------------------
-spec list(opt_ctx(), Fun :: list_fun(), AccIn :: term()) ->
    {ok, Handle :: term()} | generic_error() | no_return().
list(Ctx, Fun, AccIn) ->
    list(Ctx, Fun, AccIn, [{mode, transaction}]).


%%--------------------------------------------------------------------
%% @doc
%% Executes given function for each model's record. After each record function may interrupt operation.
%% @end
%%--------------------------------------------------------------------
-spec list(opt_ctx(),
    Fun :: list_fun(), AccIn :: term(),
    Opts :: store_driver_behaviour:list_options()) ->
    {ok, Handle :: term()} | generic_error() | no_return().
list(Ctx, Fun, AccIn, Mode) ->
    exec_driver(Ctx, list, [Fun, AccIn, Mode]).


%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete(opt_ctx(),
    Key :: ext_key(), Pred :: delete_predicate()) -> ok | generic_error().
delete(Ctx, Key, Pred) ->
    delete(Ctx, Key, Pred, []).

%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% You can specify 'ignore_links' option, if links should not be deleted with the document.
%% @end
%%--------------------------------------------------------------------
-spec delete(opt_ctx(),
    Key :: ext_key(), Pred :: delete_predicate(), Options :: [option()]) -> ok | generic_error().
delete(Ctx, Key, Pred, Opts) ->
    case exec_driver(Ctx, delete, [Key, Pred]) of
        ok ->
            % TODO - analyse if we should do it by default
            case lists:member(ignore_links, Opts) of
                true -> ok;
                false ->
                    % TODO - make link del asynch when tests will be able to handle it
                    %%             spawn(fun() -> catch delete_links(Level, Key, ModelConfig, all) end),
                    catch delete_links(Ctx, Key, all)
            end,
            ok;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete(opt_ctx(),
    Key :: ext_key()) -> ok | generic_error().
delete(Ctx, Key) ->
    delete(Ctx, Key, ?PRED_ALWAYS).


%%--------------------------------------------------------------------
%% @doc
%% Checks if #document with given key exists. This method shall not be used with
%% multiple drivers at once - use *_only levels.
%% @end
%%--------------------------------------------------------------------
-spec exists(opt_ctx(),
    Key :: ext_key()) -> {ok, boolean()} | generic_error().
exists(Ctx, Key) ->
    exec_driver(Ctx, exists, [Key]).


%%--------------------------------------------------------------------
%% @doc
%% Adds given links to the document with given key. Allows for link duplication when model is configured this way.
%% @end
%%--------------------------------------------------------------------
-spec add_links(opt_ctx(), ext_key() | document(), link_spec() | [link_spec()]) ->
    ok | generic_error().
add_links(Ctx, #document{key = Key}, Links) ->
    add_links(Ctx, Key, Links);
add_links(Ctx, Key, {_LinkName, _LinkTarget} = LinkSpec) ->
    add_links(Ctx, Key, [LinkSpec]);
add_links(Ctx, Key, Links) when is_list(Links) ->
    NormalizedLinks = normalize_link_target(Ctx, Links),
    Method = case datastore_context:get_link_duplication(Ctx) of
        true -> add_links;
        false -> set_links
    end,
    exec_driver(Ctx, Method, [Key, NormalizedLinks]).


%%--------------------------------------------------------------------
%% @doc
%% Sets given links to the document with given key.
%% @end
%%--------------------------------------------------------------------
-spec set_links(opt_ctx(), ext_key() | document(), link_spec() | [link_spec()]) ->
    ok | generic_error().
set_links(Ctx, #document{key = Key}, Links) ->
    set_links(Ctx, Key, Links);
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
-spec create_link(opt_ctx(), ext_key() | document(), link_spec()) ->
    ok | create_error().
create_link(Ctx, #document{key = Key}, Link) ->
    create_link(Ctx, Key, Link);
create_link(Ctx, Key, Link) ->
    exec_driver(Ctx, create_link, [Key, normalize_link_target(Ctx, Link)]).


%%--------------------------------------------------------------------
%% @doc
%% Removes links from the document with given key. There is special link name 'all' which removes all links.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(opt_ctx(), ext_key() | document(),
    link_name() | [link_name()] | all) -> ok | generic_error().
delete_links(Ctx, #document{key = Key}, LinkNames) ->
    delete_links(Ctx, Key, LinkNames);
delete_links(Ctx, Key, LinkNames) when is_list(LinkNames); LinkNames =:= all ->
    exec_driver(Ctx, delete_links, [Key, LinkNames]);
delete_links(Ctx, Key, LinkName) ->
    delete_links(Ctx, Key, [LinkName]).


%%--------------------------------------------------------------------
%% @doc
%% Gets specified link from the document given by key.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(opt_ctx(), ext_key() | document(), link_name()) ->
    {ok, simple_link_target()} | link_error().
fetch_link(Ctx, #document{key = Key}, LinkName) ->
    fetch_link(Ctx, Key, LinkName);
fetch_link(Ctx, Key, LinkName) ->
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
                                                datastore_context:get_link_replica_scope(Ctx), undefined)
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
-spec fetch_full_link(opt_ctx(), ext_key() | document(), link_name()) ->
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
-spec fetch_link_target(opt_ctx(), ext_key() | document(), link_name()) ->
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
-spec foreach_link(opt_ctx(), ext_key() | document(),
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
-spec link_walk(opt_ctx(), Key :: ext_key() | document(), [link_name()], get_leaf | get_all) ->
    {ok, {document(), [ext_key()]} | [document()]} | link_error() | get_error().
link_walk(Ctx, #document{key = StartKey}, LinkNames, Mode) ->
    link_walk(Ctx, StartKey, LinkNames, Mode);
link_walk(Ctx, Key, R, Mode) ->
    link_walk7(Ctx, Key, R, [], Mode).


%%--------------------------------------------------------------------
%% @doc
%% Checks if document that describes links from scope exists.
%% @end
%%--------------------------------------------------------------------
-spec exists_link_doc(opt_ctx(), ext_key(), links_utils:scope()) ->
    {ok, boolean()} | datastore:generic_error().
exists_link_doc(Ctx, #document{key = Key}, Scope) ->
    exists_link_doc(Ctx, Key, Scope);
exists_link_doc(Ctx, Key, Scope) ->
    exec_driver(Ctx, exists_link_doc, [Key, Scope]).

%%--------------------------------------------------------------------
%% @doc
%% Gets link document
%% @end
%%--------------------------------------------------------------------
-spec get_link_doc(opt_ctx(), binary(), DocKey :: datastore:ext_key(),
    MainDocKey :: datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:generic_error().
get_link_doc(Ctx, BucketOverride, DocKey, MainDocKey) ->
    exec_driver(Ctx, get_link_doc, [BucketOverride, DocKey, MainDocKey]).

%%--------------------------------------------------------------------
%% @doc
%% Runs given function within locked ResourceId. This function makes sure that 2 funs with same ResourceId won't
%% run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec run_transaction(ResourceId :: binary(), fun(() -> Result)) -> Result
    when Result :: term().
run_transaction(ResourceId, Fun) ->
    ?GLOBAL_SLAVE_DRIVER:run_transation(ResourceId, Fun).

%%--------------------------------------------------------------------
%% @doc
%% Runs given function within transaction.
%% @end
%%--------------------------------------------------------------------
-spec run_transaction(fun(() -> Result)) -> Result
    when Result :: term().
run_transaction(Fun) ->
    ?GLOBAL_SLAVE_DRIVER:run_transation(Fun).

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

%%--------------------------------------------------------------------
%% @doc
%% Translates datasotre's driver name to handler module.
%% @end
%%--------------------------------------------------------------------
-spec driver_to_module(atom()) -> atom().
driver_to_module(?PERSISTENCE_DRIVER) ->
    {ok, DriverModule} = application:get_env(?CLUSTER_WORKER_APP_NAME, ?PERSISTENCE_DRIVER),
    DriverModule;
driver_to_module(Driver) ->
    Driver.


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
-spec link_walk7(opt_ctx(), Key :: ext_key(), [link_name()], Acc :: [ext_key()],
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
            link_walk7(model:create_datastore_context(link_walk, TargetMod), TargetKey, R, [TargetKey | Acc], get_leaf);
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
%%-spec normalize_link_target(opt_ctx(), [link_spec()]) ->
%%    [{link_name(), normalized_link_target()}].
normalize_link_target(_, {_LinkName, {_Version, [{_ScopeId, _VHash, _TargetKey, ModelName} | _]}} = ValidLink) when is_atom(ModelName) ->
    ValidLink;
normalize_link_target(_, []) ->
    [];
normalize_link_target(Ctx, [{_, {V, []}} | R]) when is_integer(V) ->
    normalize_link_target(Ctx, R);
normalize_link_target(Ctx, [Link | R]) ->
    [normalize_link_target(Ctx, Link) | normalize_link_target(Ctx, R)];
normalize_link_target(Ctx, {LinkName, #document{key = TargetKey} = Doc}) ->
    MScope = datastore_context:get_link_replica_scope(Ctx),
    normalize_link_target(Ctx, {LinkName, {links_utils:get_scopes(MScope, undefined), links_utils:gen_vhash(), TargetKey, model_name(Doc)}});
normalize_link_target(Ctx, {LinkName, {TargetKey, ModelName}}) when is_atom(ModelName) ->
    MScope = datastore_context:get_link_replica_scope(Ctx),
    normalize_link_target(Ctx, {LinkName, {links_utils:get_scopes(MScope, undefined), links_utils:gen_vhash(), TargetKey, ModelName}});
normalize_link_target(Ctx, {LinkName, {_ScopeId, _VHash, _TargetKey, _ModelName} = Target}) ->
    normalize_link_target(Ctx, {LinkName, {1, [Target]}}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% For given #document generates key if undefined and returns updated #document structure.
%% @end
%%--------------------------------------------------------------------
-spec maybe_gen_uuid(document()) -> document().
maybe_gen_uuid(#document{key = undefined} = Doc) ->
    Doc#document{key = datastore_utils:gen_uuid(), generated_uuid = true};
maybe_gen_uuid(#document{} = Doc) ->
    Doc.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets model name for given document/record.
%% @end
%%--------------------------------------------------------------------
-spec model_name(opt_ctx() | tuple() | document() | model_behaviour:model_config()
    | model_behaviour:model_type()) -> model_behaviour:model_type().
model_name(#document{value = Record}) ->
    model_name(Record);
model_name(#model_config{name = ModelName}) ->
    ModelName;
model_name(Record) when is_tuple(Record) ->
    element(1, Record);
model_name(ModelName) when is_atom(ModelName) ->
    ModelName;
model_name(OptCtx) ->
    datastore_context:get_model_name(OptCtx).


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
-spec run_prehooks(opt_ctx(),
    Method :: model_behaviour:model_action(), Context :: term()) ->
    ok | {error, Reason :: term()}.
% TODO - new concept for hooks
run_prehooks(Ctx, Method, Args) ->
    HooksConfig = datastore_context:get_hooks_config(Ctx),
    case HooksConfig of
        run_hooks ->
           ModelName = model_name(Ctx),
           Level = datastore_context:get_level(Ctx),
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
-spec run_posthooks(opt_ctx(),
    Model :: model_behaviour:model_action(),
    Context :: term(), ReturnValue) -> ReturnValue when ReturnValue :: term().
run_posthooks(Ctx, Method, Args, Return) ->
    HooksConfig = datastore_context:get_hooks_config(Ctx),
    case HooksConfig of
        run_hooks ->
            ModelName = model_name(Ctx),
            Level = datastore_context:get_level(Ctx),
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
    lists:map(
        fun(ModelName) ->
            Config = #model_config{hooks = Hooks} = model_config(ModelName),
            ok = model:validate_model_config(Config),
            lists:foreach(
                fun(Hook) ->
                    ets:insert(?LOCAL_STATE, {Hook, ModelName})
                end, Hooks),
            Config
        end, Models).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes information about caches consistency.
%% @end
%%--------------------------------------------------------------------
-spec init_caches_consistency(Models :: [model_behaviour:model_type() | model_behaviour:model_config()]) -> ok.
init_caches_consistency(Models) ->
    lists:foreach(fun(ModelName) ->
        #model_config{store_level = SL} = ModelConfig = model_config(ModelName),
        {Check, InfoLevel} = case SL of
            ?GLOBALLY_CACHED_LEVEL -> {true, ?GLOBAL_ONLY_LEVEL};
            ?LOCALLY_CACHED_LEVEL -> {true, ?LOCAL_ONLY_LEVEL};
            _ -> {false, SL}
        end,
        case Check of
            true ->
                % TODO - fix performance issue!!!
                case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_proc_terminate_clear_memory) of
                    {ok, true} ->
                        ok;
                    _ ->
                        case erlang:apply(datastore:driver_to_module(?PERSISTENCE_DRIVER), is_model_empty, [ModelConfig]) of
                            {ok, true} ->
                                caches_controller:init_consistency_info(InfoLevel, ModelName);
                            _ ->
                                ok
                        end
                end;
            _ ->
                ok
        end
    end, Models).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Organizes given models into #{bucket -> [model]} map.
%% @end
%%--------------------------------------------------------------------
-spec configs_per_bucket(Configs :: [model_behaviour:model_config()]) ->
    #{bucket() => [model_behaviour:model_config()]}.
configs_per_bucket(Configs) ->
    lists:foldl(
        fun(#model_config{bucket = Bucket} = ModelConfig, Acc) ->
            maps:put(Bucket, [ModelConfig | maps:get(Bucket, Acc, [])], Acc)
        end, #{}, Configs).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs init_bucket/1 for each datastore driver using given models'.
%% @end
%%--------------------------------------------------------------------
-spec init_drivers(Configs :: [model_behaviour:model_config()], NodeToSync :: node()) ->
    ok | no_return().
init_drivers(Configs, NodeToSync) ->
    lists:foreach(
        fun({Bucket, Models}) ->
            ok = apply(driver_to_module(?PERSISTENCE_DRIVER), init_bucket, [Bucket, Models, NodeToSync]),
            ok = apply(?LOCAL_SLAVE_DRIVER, init_bucket, [Bucket, Models, NodeToSync]),
            ok = apply(?GLOBAL_SLAVE_DRIVER, init_bucket, [Bucket, Models, NodeToSync])
        end, maps:to_list(configs_per_bucket(Configs))).

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
    try
        Models = datastore_config:models(),
        Configs = load_local_state(Models),
        init_drivers(Configs, NodeToSync)
    catch
        Type:Reason ->
            ?error_stacktrace("Cannot initialize datastore local state due to"
            " ~p: ~p", [Type, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Ends datastore initialization after whole cluster is set-up.
%% @end
%%--------------------------------------------------------------------
-spec cluster_initialized() -> ok.
cluster_initialized() ->
    {ok, MaxNum} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_max_memory_proc_number),
    tp:set_processes_limit(MaxNum),
    Models = datastore_config:models(),
    init_caches_consistency(Models).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Translates store level into list of drivers.
%% @end
%%--------------------------------------------------------------------
-spec level_to_driver(Level :: store_level()) -> Driver :: atom() | [atom()].
level_to_driver(?DISK_ONLY_LEVEL) ->
    ?PERSISTENCE_DRIVER;
level_to_driver(_) ->
    ?MEMORY_DRIVER.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes given model action on given driver(s).
%% @end
%%--------------------------------------------------------------------
-spec exec_driver(opt_ctx(), Method :: store_driver_behaviour:driver_action(),
    [term()]) -> ok | {ok, term()} | generic_error().
exec_driver(OptCtx, Method, Args) ->
    Driver = datastore_context:get_driver(OptCtx),
    Return =
        case run_prehooks(OptCtx, Method, Args) of
            ok ->
                {FinalMethod, FinalArgs} = final_method_with_args(Driver, OptCtx, Method, Args),
                erlang:apply(driver_to_module(Driver), FinalMethod, FinalArgs);
            {error, Reason} ->
                {error, Reason}
        end,
    run_posthooks(OptCtx, Method, Args, Return).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Provides method to be called for the driver and its args.
%% @end
%%--------------------------------------------------------------------
-spec final_method_with_args(Driver :: atom(), opt_ctx(),
    Method :: store_driver_behaviour:driver_action(),
    Args :: [term()]) -> {Fun :: atom(), FinalArgs :: [term()]}.
final_method_with_args(?PERSISTENCE_DRIVER, OptCtx, Method, Args) ->
    DriverContext = datastore_context:get_driver_context(OptCtx),
    {Method, [DriverContext | Args]};
final_method_with_args(_Driver, OptCtx, Method, Args) ->
    DriverContext = datastore_context:get_driver_context(OptCtx),
    case datastore_context:get_resolve_conflicts(OptCtx) of
        doc ->
            {call, [force_save, DriverContext, add_bucket(OptCtx, Args)]};
        {links, DocKey} ->
            {call, [force_link_save, DriverContext, add_bucket(OptCtx, Args) ++ [DocKey]]};
        _ ->
            {call, [Method, DriverContext, Args]}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds information about bucket to calls.
%% @end
%%--------------------------------------------------------------------
-spec add_bucket(opt_ctx(), Args :: [term()]) -> NewArgs :: [term()].
% TODO - probably not used option
add_bucket(OptCtx, Args) ->
    case datastore_context:get_bucket(OptCtx) of
        default -> Args;
        Bucket -> [Bucket | Args]
    end.