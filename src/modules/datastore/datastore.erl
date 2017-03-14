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
-type value() :: term().
-type document_diff() :: #{term() => term()} | fun((OldValue :: value()) ->
    {ok, NewValue :: value()} | {error, Reason :: term()}).
-type bucket() :: atom() | binary().
-type option() :: ignore_links.

-export_type([uuid/0, key/0, ext_key/0, value/0, document/0, document_diff/0, bucket/0, option/0]).

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

%% Types for auxiliary caches
-type aux_cache_key() :: {term(), key()}.
-type aux_cache_handle() :: aux_cache_key() | '$end_of_table'.
-type aux_cache_level() :: ?LOCALLY_CACHED_LEVEL | ?GLOBALLY_CACHED_LEVEL.
-type aux_iterator_fun() :: fun((aux_cache_key()) -> aux_cache_handle()).
-type aux_cache_access_context() :: transaction | async_dirty.
-type aux_cache_config() :: #aux_cache_config{}.

-export_type([aux_cache_level/0, aux_cache_key/0, aux_cache_handle/0,
    aux_iterator_fun/0, aux_cache_access_context/0, aux_cache_config/0]).

%% API
-export([save/2, update/4, create/2, create_or_update/3,
    get/3, list/4, list_dirty/4, list_ordered/5, delete/4, delete/3, delete/5, exists/3]).
-export([fetch_link/3, fetch_link/4, add_links/3, add_links/4, create_link/3, delete_links/3, delete_links/4,
    foreach_link/4, foreach_link/5, fetch_link_target/3, fetch_link_target/4,
    link_walk/4, link_walk/5, set_links/3, set_links/4]).
-export([fetch_full_link/3, fetch_full_link/4, exists_link_doc/3, exists_link_doc/4]).
-export([configs_per_bucket/1, ensure_state_loaded/0, cluster_initialized/0, healthcheck/0, level_to_driver/1,
    driver_to_module/1, initialize_state/1]).
-export([run_transaction/1, run_transaction/3, normalize_link_target/2, run_posthooks/5, models_with_aux_caches/0]).
-export([initialize_minimal_env/0, initialize_minimal_env/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves given #document.
%% @end
%%--------------------------------------------------------------------
-spec save(Level :: store_level(), Document :: datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(Level, #document{} = Document) ->
    exec_driver(model_config(Document), Level, save, [maybe_gen_uuid(Document)]).

%%--------------------------------------------------------------------
%% @doc
%% Updates given by key document by replacing given fields with new values.
%% @end
%%--------------------------------------------------------------------
-spec update(Level :: store_level(), ModelNameOrConfig :: model_behaviour:model_type() | model_behaviour:model_config(),
    Key :: datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Level, ModelNameOrConfig, Key, Diff) ->
    exec_driver(model_config(ModelNameOrConfig), Level, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Creates new #document.
%% @end
%%--------------------------------------------------------------------
-spec create(Level :: store_level(), Document :: datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(Level, #document{} = Document) ->
    exec_driver(model_config(Document), Level, create, [maybe_gen_uuid(Document)]).

%%--------------------------------------------------------------------
%% @doc
%% Updates given document by replacing given fields with new values or
%% creates new one if not exists.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(Level :: store_level(), Document :: datastore:document(),
    Diff :: datastore:document_diff()) -> {ok, datastore:ext_key()} | datastore:create_error().
create_or_update(Level, #document{} = Document, Diff) ->
    exec_driver(model_config(Document), Level, create_or_update, [Document, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Gets #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec get(Level :: store_level(), ModelNameOrConfig :: model_behaviour:model_type() | model_behaviour:model_config(),
    Key :: datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Level, ModelNameOrConfig, Key) ->
    exec_driver(model_config(ModelNameOrConfig), Level, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Dirty alternative of list/4.
%% Executes given function for each model's record. After each record function may interrupt operation.
%% @end
%%--------------------------------------------------------------------
-spec list_dirty(Level :: store_level(), ModelNameOrConfig :: model_behaviour:model_type() | model_behaviour:model_config(), Fun :: list_fun(), AccIn :: term()) ->
    {ok, Handle :: term()} | datastore:generic_error() | no_return().
list_dirty(Level, ModelNameOrConfig, Fun, AccIn) ->
    list(Level, ModelNameOrConfig, Fun, AccIn, [{mode, dirty}]).

%%--------------------------------------------------------------------
%% @doc
%% Executes given function for each model's record. Records are traversed according to
%% order in auxiliary cache connected with Field.
%% @end
%%--------------------------------------------------------------------
-spec list_ordered(Level :: store_level(), ModelName :: model_behaviour:model_type(),
    Fun :: list_fun(), Field :: atom(), AccIn :: term()) ->
    {ok, Handle :: term()} | datastore:generic_error() | no_return().
list_ordered(Level, ModelName, Fun, Field, AccIn) ->
    list(Level, ModelName, Fun, AccIn, [{mode, {ordered, Field}}]).


%%--------------------------------------------------------------------
%% @doc
%% Executes given function for each model's record. After each record function may interrupt operation.
%% @end
%%--------------------------------------------------------------------
-spec list(Level :: store_level(), ModelNameOrConfig :: model_behaviour:model_type() | model_behaviour:model_config(), Fun :: list_fun(), AccIn :: term()) ->
    {ok, Handle :: term()} | datastore:generic_error() | no_return().
list(Level, ModelNameOrConfig, Fun, AccIn) ->
    list(Level, ModelNameOrConfig, Fun, AccIn, [{mode, transaction}]).


%%--------------------------------------------------------------------
%% @doc
%% Executes given function for each model's record. After each record function may interrupt operation.
%% @end
%%--------------------------------------------------------------------
-spec list(Level :: store_level(), ModelNameOrConfig :: model_behaviour:model_type() | model_behaviour:model_config(),
    Fun :: list_fun(), AccIn :: term(),
    Opts :: store_driver_behaviour:list_options()) ->
    {ok, Handle :: term()} | datastore:generic_error() | no_return().
list(Level, ModelNameOrConfig, Fun, AccIn, Mode) ->
    MC = model_config(ModelNameOrConfig),
    exec_driver(MC#model_config{store_level = Level}, Level, list, [Fun, AccIn, Mode]).


%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete(Level :: store_level(), ModelNameOrConfig :: model_behaviour:model_type() | model_behaviour:model_config(),
    Key :: datastore:ext_key(), Pred :: delete_predicate()) -> ok | datastore:generic_error().
delete(Level, ModelNameOrConfig, Key, Pred) ->
    delete(Level, ModelNameOrConfig, Key, Pred, []).

%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% You can specify 'ignore_links' option, if links should not be deleted with the document.
%% @end
%%--------------------------------------------------------------------
-spec delete(Level :: store_level(), ModelNameOrConfig :: model_behaviour:model_type() | model_behaviour:model_config(),
    Key :: datastore:ext_key(), Pred :: delete_predicate(), Options :: [option()]) -> ok | datastore:generic_error().
delete(Level, ModelNameOrConfig, Key, Pred, Opts) ->
    ModelConfig = model_config(ModelNameOrConfig),
    case exec_driver(ModelConfig, Level, delete, [Key, Pred]) of
        ok ->
            % TODO - analyse if we should do it by default
            case lists:member(ignore_links, Opts) of
                true -> ok;
                false ->
                    % TODO - make link del asynch when tests will be able to handle it
                    %%             spawn(fun() -> catch delete_links(Level, Key, ModelConfig, all) end),
                        catch delete_links(Level, Key, ModelConfig, all)
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
-spec delete(Level :: store_level(), ModelNameOrConfig :: model_behaviour:model_type() | model_behaviour:model_config(),
    Key :: datastore:ext_key()) -> ok | datastore:generic_error().
delete(Level, ModelNameOrConfig, Key) ->
    delete(Level, ModelNameOrConfig, Key, ?PRED_ALWAYS).


%%--------------------------------------------------------------------
%% @doc
%% Checks if #document with given key exists. This method shall not be used with
%% multiple drivers at once - use *_only levels.
%% @end
%%--------------------------------------------------------------------
-spec exists(Level :: store_level(), ModelNameOrConfig :: model_behaviour:model_type() | model_behaviour:model_config(),
    Key :: datastore:ext_key()) -> {ok, boolean()} | datastore:generic_error().
exists(Level, ModelNameOrConfig, Key) ->
    exec_driver(model_config(ModelNameOrConfig), Level, exists, [Key]).


%%--------------------------------------------------------------------
%% @doc
%% Adds links to given document.
%% @end
%%--------------------------------------------------------------------
-spec add_links(Level :: store_level(), document(), link_spec() | [link_spec()]) -> ok | generic_error().
add_links(Level, #document{key = Key} = Doc, Links) ->
    add_links(Level, Key, model_config(Doc), Links).


%%--------------------------------------------------------------------
%% @doc
%% Adds given links to the document with given key. Allows for link duplication when model is configured this way.
%% @end
%%--------------------------------------------------------------------
-spec add_links(Level :: store_level(), ext_key(), model_behaviour:model_type() | model_behaviour:model_config(), link_spec() | [link_spec()]) ->
    ok | generic_error().
add_links(Level, Key, ModelNameOrConfig, {_LinkName, _LinkTarget} = LinkSpec) ->
    add_links(Level, Key, ModelNameOrConfig, [LinkSpec]);
add_links(Level, Key, ModelNameOrConfig, Links) when is_list(Links) ->
    ModelConfig = #model_config{link_duplication = LinkDuplication} = model_config(ModelNameOrConfig),
    NormalizedLinks = normalize_link_target(ModelConfig, Links),
    Method = case LinkDuplication of
        true -> add_links;
        false -> set_links
    end,
    exec_driver(ModelConfig, Level, Method, [Key, NormalizedLinks]).

%%--------------------------------------------------------------------
%% @doc
%% Sets links to given document. Always replaces existing links with the same name.
%% @end
%%--------------------------------------------------------------------
-spec set_links(Level :: store_level(), document(), link_spec() | [link_spec()]) -> ok | generic_error().
set_links(Level, #document{key = Key} = Doc, Links) ->
    set_links(Level, Key, model_config(Doc), Links).


%%--------------------------------------------------------------------
%% @doc
%% Sets given links to the document with given key.
%% @end
%%--------------------------------------------------------------------
-spec set_links(Level :: store_level(), ext_key(), model_behaviour:model_type() | model_behaviour:model_config(), link_spec() | [link_spec()]) ->
    ok | generic_error().
set_links(Level, Key, ModelNameOrConfig, {_LinkName, _LinkTarget} = LinkSpec) ->
    set_links(Level, Key, ModelNameOrConfig, [LinkSpec]);
set_links(Level, Key, ModelNameOrConfig, Links) when is_list(Links) ->
    ModelConfig = #model_config{} = model_config(ModelNameOrConfig),
    NormalizedLinks = normalize_link_target(ModelConfig, Links),
    exec_driver(ModelConfig, Level, set_links, [Key, NormalizedLinks]).


%%--------------------------------------------------------------------
%% @doc
%% Creates links to given document if link does not exist.
%% @end
%%--------------------------------------------------------------------
-spec create_link(Level :: store_level(), document(), link_spec()) -> ok | create_error().
create_link(Level, #document{key = Key} = Doc, Link) ->
    create_link(Level, Key, model_name(Doc), Link).


%%--------------------------------------------------------------------
%% @doc
%% Adds given links to the document with given key if link does not exist.
%% @end
%%--------------------------------------------------------------------
-spec create_link(Level :: store_level(), ext_key(), model_behaviour:model_type() | model_behaviour:model_config(), link_spec()) ->
    ok | create_error().
create_link(Level, Key, ModelNameOrConfig, Link) ->
    ModelConfig = model_config(ModelNameOrConfig),
    exec_driver(ModelConfig, Level, create_link, [Key, normalize_link_target(ModelConfig, Link)]).

%%--------------------------------------------------------------------
%% @doc
%% Removes links from given document. There is special link name 'all' which removes all links.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(Level :: store_level(), document(), link_name() | [link_name()] | all) -> ok | generic_error().
delete_links(Level, #document{key = Key} = Doc, LinkNames) ->
    delete_links(Level, Key, model_name(Doc), LinkNames).


%%--------------------------------------------------------------------
%% @doc
%% Removes links from the document with given key. There is special link name 'all' which removes all links.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(Level :: store_level(), ext_key(), model_behaviour:model_type() | model_behaviour:model_config(),
    link_name() | [link_name()] | all) -> ok | generic_error().
delete_links(Level, Key, ModelNameOrConfig, LinkNames) when is_list(LinkNames); LinkNames =:= all ->
    exec_driver(model_config(ModelNameOrConfig), Level, delete_links, [Key, LinkNames]);
delete_links(Level, Key, ModelNameOrConfig, LinkName) ->
    delete_links(Level, Key, ModelNameOrConfig, [LinkName]).


%%--------------------------------------------------------------------
%% @doc
%% Gets specified link from given document.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(Level :: store_level(), document(), link_name()) -> {ok, normalized_link_target()} | link_error().
fetch_link(Level, #document{key = Key} = Doc, LinkName) ->
    fetch_link(Level, Key, model_config(Doc), LinkName).


%%--------------------------------------------------------------------
%% @doc
%% Gets specified link from the document given by key.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(Level :: store_level(), ext_key(), model_behaviour:model_type() | model_behaviour:model_config(), link_name()) ->
    {ok, simple_link_target()} | link_error().
fetch_link(Level, Key, ModelNameOrConfig, LinkName) ->
    ModelName = model_name(ModelNameOrConfig),
    {RawLinkName, RequestedScope, VHash} = links_utils:unpack_link_scope(ModelName, LinkName),
    case fetch_full_link(Level, Key, ModelNameOrConfig, RawLinkName) of
        {ok, {_Version, Targets = [H | _]}} ->
            case RequestedScope of
                undefined ->
                    {_, _, TargetKey, TargetModel} =
                        case links_utils:select_scope_related_link(RawLinkName, RequestedScope, VHash, Targets) of
                            undefined ->
                                #model_config{link_replica_scope = MScope} = model_config(ModelNameOrConfig),
                                case lists:filter(
                                    fun
                                        ({Scope, _, _, _}) ->
                                            Scope == links_utils:get_scopes(MScope, undefined)
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
%% Gets specified link from given document.
%% @end
%%--------------------------------------------------------------------
-spec fetch_full_link(Level :: store_level(), document(), link_name()) -> {ok, normalized_link_target()} | link_error().
fetch_full_link(Level, #document{key = Key} = Doc, LinkName) ->
    fetch_full_link(Level, Key, model_name(Doc), LinkName).


%%--------------------------------------------------------------------
%% @doc
%% Gets specified link from the document given by key.
%% @end
%%--------------------------------------------------------------------
-spec fetch_full_link(Level :: store_level(), ext_key(), model_behaviour:model_type() | model_behaviour:model_config(), link_name()) ->
    {ok, normalized_link_target()} | generic_error().
fetch_full_link(Level, Key, ModelNameOrConfig, LinkName) ->
    ModelConfig = model_config(ModelNameOrConfig),
    exec_driver(ModelConfig, Level, fetch_link, [Key, LinkName]).


%%--------------------------------------------------------------------
%% @doc
%% Gets document pointed by given link of given document.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link_target(Level :: store_level(), document(), link_name()) -> {ok, document()} | generic_error().
fetch_link_target(Level, #document{key = Key} = Doc, LinkName) ->
    fetch_link_target(Level, Key, model_config(Doc), LinkName).


%%--------------------------------------------------------------------
%% @doc
%% Gets document pointed by given link of document given by key.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link_target(Level :: store_level(), ext_key(), model_behaviour:model_type() | model_behaviour:model_config(), link_name()) ->
    {ok, document()} | generic_error().
fetch_link_target(Level, Key, ModelNameOrConfig, LinkName) ->
    case fetch_link(Level, Key, ModelNameOrConfig, LinkName) of
        {ok, _Target = {TargetKey, TargetModel}} ->
            TargetModel:get(TargetKey);
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Executes given function for each link of given document - similar to 'foldl'.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(Level :: store_level(), document(), fun((link_name(), link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | link_error().
foreach_link(Level, #document{key = Key} = Doc, Fun, AccIn) ->
    foreach_link(Level, Key, model_config(Doc), Fun, AccIn).


%%--------------------------------------------------------------------
%% @doc
%% Executes given function for each link of the document given by key - similar to 'foldl'.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(Level :: store_level(), Key :: ext_key(), ModelNameOrConfig :: model_behaviour:model_type() | model_behaviour:model_config(),
    fun((link_name(), link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | link_error().
foreach_link(Level, Key, ModelNameOrConfig, Fun, AccIn) ->
    exec_driver(model_config(ModelNameOrConfig), Level, foreach_link, [Key, Fun, AccIn]).

%%--------------------------------------------------------------------
%% @doc
%% "Walks" from link to link and fetches either all encountered documents (for Mode == get_all - not yet implemted),
%% or just last document (for Mode == get_leaf). Starts on given document.
%% @end
%%--------------------------------------------------------------------
-spec link_walk(Level :: store_level(), document(), [link_name()], get_leaf | get_all) ->
    {ok, document() | [document()]} | link_error() | get_error().
link_walk(Level, #document{key = StartKey} = StartDoc, LinkNames, Mode) when is_atom(Mode), is_list(LinkNames) ->
    link_walk(Level, StartKey, model_name(StartDoc), LinkNames, Mode).


%%--------------------------------------------------------------------
%% @doc
%% "Walks" from link to link and fetches either all encountered documents (for Mode == get_all - not yet implemted),
%% or just last document (for Mode == get_leaf). Starts on the document given by key.
%% In case of Mode == get_leaf, list of all link's uuids is also returned.
%% @end
%%--------------------------------------------------------------------
-spec link_walk(Level :: store_level(), Key :: ext_key(), ModelNameOrConfig :: model_behaviour:model_type() | model_behaviour:model_config(), [link_name()], get_leaf | get_all) ->
    {ok, {document(), [ext_key()]} | [document()]} | link_error() | get_error().
link_walk(Level, Key, ModelNameOrConfig, R, Mode) ->
    link_walk7(Level, Key, ModelNameOrConfig, R, [], Mode).


%%--------------------------------------------------------------------
%% @doc
%% Checks if document that describes links from scope exists.
%% @end
%%--------------------------------------------------------------------
-spec exists_link_doc(store_level(), document(), links_utils:scope()) ->
    {ok, boolean()} | datastore:generic_error().
exists_link_doc(Level, #document{key = Key} = Doc, Scope) ->
    exists_link_doc(Level, Key, model_name(Doc), Scope).

%%--------------------------------------------------------------------
%% @doc
%% Checks if document that describes links from scope exists.
%% @end
%%--------------------------------------------------------------------
-spec exists_link_doc(store_level(), datastore:ext_key(), model_behaviour:model_type() | model_behaviour:model_config(), links_utils:scope()) ->
    {ok, boolean()} | datastore:generic_error().
exists_link_doc(Level, Key, ModelNameOrConfig, Scope) ->
    exec_driver(model_config(ModelNameOrConfig), Level, exists_link_doc, [Key, Scope]).

%%--------------------------------------------------------------------
%% @doc
%% Runs given function within locked ResourceId. This function makes sure that 2 funs with same ResourceId won't
%% run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec run_transaction(ModelNameOrConfig :: model_behaviour:model_type() | model_behaviour:model_config(), ResourceId :: binary(), fun(() -> Result)) -> Result
    when Result :: term().
run_transaction(ModelNameOrConfig, ResourceId, Fun) ->
    ModelConfig = model_config(ModelNameOrConfig),
    ?GLOBAL_SLAVE_DRIVER:run_transation(ModelConfig, ResourceId, Fun).

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


%%--------------------------------------------------------------------
%% @doc
%% Returns list of models which have auxiliary caches created.
%% @end
%%--------------------------------------------------------------------
-spec models_with_aux_caches() -> [model_behaviour:model_type()].
models_with_aux_caches() ->
    datastore_config:models_with_aux_caches().


%%%===================================================================
%%% Internal functions
%%%===================================================================

link_walk7(_Level, _Key, _ModelNameOrConfig, _Links, _Acc, get_all) ->
    erlang:error(not_inplemented);
link_walk7(Level, Key, ModelNameOrConfig, [LastLink], Acc, get_leaf) ->
    case fetch_link_target(Level, Key, ModelNameOrConfig, LastLink) of
        {ok, #document{key = LastKey} = Leaf} ->
            {ok, {Leaf, lists:reverse([LastKey | Acc])}};
        {error, Reason} ->
            {error, Reason}
    end;
link_walk7(Level, Key, ModelNameOrConfig, [NextLink | R], Acc, get_leaf) ->
    case fetch_link(Level, Key, ModelNameOrConfig, NextLink) of
        {ok, {TargetKey, TargetMod}} ->
            link_walk7(Level, TargetKey, TargetMod, R, [TargetKey | Acc], get_leaf);
        {error, Reason} ->
            {error, Reason}
    end.

normalize_link_target(_, {_LinkName, {_Version, [{_ScopeId, _VHash, _TargetKey, ModelName} | _]}} = ValidLink) when is_atom(ModelName) ->
    ValidLink;
normalize_link_target(_ModelConfig, []) ->
    [];
normalize_link_target(ModelConfig, [{_, {V, []}} | R]) when is_integer(V) ->
    normalize_link_target(ModelConfig, R);
normalize_link_target(ModelConfig, [Link | R]) ->
    [normalize_link_target(ModelConfig, Link) | normalize_link_target(ModelConfig, R)];
normalize_link_target(ModelConfig = #model_config{link_replica_scope = MScope}, {LinkName, #document{key = TargetKey} = Doc}) ->
    normalize_link_target(ModelConfig, {LinkName, {links_utils:get_scopes(MScope, undefined), links_utils:gen_vhash(), TargetKey, model_name(Doc)}});
normalize_link_target(ModelConfig = #model_config{link_replica_scope = MScope}, {LinkName, {TargetKey, ModelName}}) when is_atom(ModelName) ->
    normalize_link_target(ModelConfig, {LinkName, {links_utils:get_scopes(MScope, undefined), links_utils:gen_vhash(), TargetKey, ModelName}});
normalize_link_target(ModelConfig, {LinkName, {_ScopeId, _VHash, _TargetKey, _ModelName} = Target}) ->
    normalize_link_target(ModelConfig, {LinkName, {1, [Target]}}).


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
-spec model_name(tuple() | document() | model_behaviour:model_config() | model_behaviour:model_type()) -> model_behaviour:model_type().
model_name(#document{value = Record}) ->
    model_name(Record);
model_name(#model_config{name = ModelName}) ->
    ModelName;
model_name(Record) when is_tuple(Record) ->
    element(1, Record);
model_name(ModelName) when is_atom(ModelName) ->
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
-spec run_prehooks(Config :: model_behaviour:model_config(),
    Method :: model_behaviour:model_action(), Level :: store_level(), Context :: term()) ->
    ok | {ok, term()} | {task, task_manager:task()} | {tasks, [task_manager:task()]} | {error, Reason :: term()}.
% TODO - check for errors before accepting task
run_prehooks(#model_config{name = ModelName}, Method, Level, Context) ->
    Hooked = ets:lookup(?LOCAL_STATE, {ModelName, Method}),
    HooksRes =
        lists:map(
            fun({_, HookedModule}) ->
                HookedModule:before(ModelName, Method, Level, Context)
            end, Hooked),
    case [Filtered || Filtered <- HooksRes, Filtered /= ok] of
        [] -> ok;
        [Interrupt | _] ->
            Interrupt
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs asynchronously all post-hooks for given model, method, context and
%% return value. Returns given return value.
%% @end
%%--------------------------------------------------------------------
-spec run_posthooks(Config :: model_behaviour:model_config(),
    Model :: model_behaviour:model_action(), Level :: store_level(),
    Context :: term(), ReturnValue) -> ReturnValue when ReturnValue :: term().
run_posthooks(#model_config{name = ModelName} = ModelConfig, Method, Level, Context, Return) ->
    Hooked = ets:lookup(?LOCAL_STATE, {ModelName, Method}),
    LinksContext = links_utils:get_context_to_propagate(ModelConfig),
    lists:foreach(
        fun({_, HookedModule}) ->
            spawn(fun() ->
                case HookedModule of
                    ModelName ->
                        links_utils:apply_context(LinksContext);
                    _ ->
                        ok
                end,
                HookedModule:'after'(ModelName, Method, Level, Context, Return) end)
        end, Hooked),
    Return.

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
            ok = validate_model_config(Config),
            lists:foreach(
                fun(Hook) ->
                    ets:insert(?LOCAL_STATE, {Hook, ModelName})
                end, Hooks),
            Config
        end, Models).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Validates model's configuration.
%% @end
%%--------------------------------------------------------------------
-spec validate_model_config(model_behaviour:model_config()) -> ok | no_return().
validate_model_config(#model_config{version = CurrentVersion, name = ModelName, store_level = StoreLevel}) ->
    case lists:member(?PERSISTENCE_DRIVER, lists:flatten([level_to_driver(StoreLevel)])) of
        false -> ok;
        true ->
            case lists:member({record_struct, 1}, ModelName:module_info(exports)) of
                true ->
                    try
                        %% Check all versions up to CurrentVersion
                        [datastore_json:validate_struct(ModelName:record_struct(Version))
                            || Version <- lists:seq(1, CurrentVersion)],
                        HasUpdater = lists:member({record_upgrade, 2}, ModelName:module_info(exports))
                            orelse CurrentVersion == 1,
                        case HasUpdater of
                            true -> ok;
                            false ->
                                error({no_record_updater, CurrentVersion, ModelName})
                        end,
                        ok
                    catch
                        _:Reason ->
                            ?error_stacktrace("Unable to validate record version for model ~p due to ~p", [ModelName, Reason]),
                            error({invalid_record_version, CurrentVersion, ModelName})
                    end;
                false ->
                    error({no_struct_def, ModelName})
            end
    end.

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
            ok = apply(?GLOBAL_SLAVE_DRIVER, init_bucket, [Bucket, Models, NodeToSync]),
            init_auxiliary_caches(Models, NodeToSync)
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
%% Initialize auxiliary tables for given Models
%% @end
%%--------------------------------------------------------------------
-spec init_auxiliary_caches([#model_config{}], NodeToSync :: node()) -> ok.
init_auxiliary_caches(Models, NodeToSync) ->
    lists:foreach(
        fun
            (#model_config{auxiliary_caches=AuxCaches}) when map_size(AuxCaches) == 0 ->
                ok;
            (M = #model_config{auxiliary_caches=AuxCaches}) ->
                lists:foreach(fun({StoreLevel, Fields}) ->
                    Driver = level_to_driver(StoreLevel),
                    Driver:create_auxiliary_caches(M, Fields, NodeToSync)
                end, maps:to_list(invert_aux_cache_config(AuxCaches)))
        end, Models),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Inverts map. key becomes values and  values become keys.
%% In new map each key's (old value) value is list of keys associated with
%% these value in original map.
%% e.g.
%% M = #{ k1 => v1, k2 => v1, k3 => v3 }
%% invert(M) = #{ v1 => [k1, k2], v2 => [k3] }
%% @end
%%--------------------------------------------------------------------
-spec invert_aux_cache_config(#{}) -> #{term() => [term()]}.
invert_aux_cache_config(Map) ->
    Inverted = maps:fold(fun(K, #aux_cache_config{level =Field}, AccIn) ->
        CurrentValue = maps:get(Field, AccIn, []),
        maps:put(Field, [K | CurrentValue], AccIn) %todo test
    end, #{}, Map),
    Inverted.

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
-spec exec_driver(model_behaviour:model_type() | model_behaviour:model_config(),
    Level :: store_level(), Method :: store_driver_behaviour:driver_action(),
    [term()]) -> ok | {ok, term()} | {error, term()} | term().
exec_driver(ModelNameOrConfig, Level, Method, Args) ->
    ModelConfig0 = model_config(ModelNameOrConfig),
    ModelConfig = ModelConfig0#model_config{store_level = Level,
        link_store_level = Level},
    Driver = level_to_driver(Level),
    Return =
        case run_prehooks(ModelConfig, Method, Level, Args) of
            ok ->
                FullArgs = [ModelConfig | Args],
                erlang:apply(driver_to_module(Driver), Method, FullArgs);
            {ok, Value} ->
                {ok, Value};
            {task, _Task} ->
                {error, prehook_ans_not_supported};
            {tasks, _Task} ->
                {error, prehook_ans_not_supported};
            {error, Reason} ->
                {error, Reason}
        end,
    run_posthooks(ModelConfig, Method, Level, Args, Return).
