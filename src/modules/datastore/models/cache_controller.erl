%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model that is used to control memory utilization by caches.
%%% @end
%%%-------------------------------------------------------------------
-module(cache_controller).
-author("Michal Wrzeszcz").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_internal_model.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

%% model_behaviour callbacks and API
-export([save/1, get/1, list/0, list/1, exists/1, delete/1, delete/2, update/2, create/1,
    save/2, get/2, list/3, list_dirty/3, exists/2, delete/3, update/3, create/2,
    create_or_update/2, create_or_update/3, model_init/0, 'after'/5, before/4,
    list_docs_to_be_dumped/1, choose_action/5, choose_action/6, check_get/3,
    check_fetch/3, check_disk_read/4, restore_from_disk/4, link_cache_key/3]).


%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback save/1 but allows
%% choice of store level.
%% @end
%%--------------------------------------------------------------------
-spec save(Level :: datastore:store_level(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(Level, Document) ->
    datastore:save(Level, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback update/2 but allows
%% choice of store level.
%% @end
%%--------------------------------------------------------------------
-spec update(Level :: datastore:store_level(), datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Level, Key, Diff) ->
    datastore:update(Level, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback create/1 but allows
%% choice of store level.
%% @end
%%--------------------------------------------------------------------
-spec create(Level :: datastore:store_level(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(Level, Document) ->
    datastore:create(Level, Document).

%%--------------------------------------------------------------------
%% @doc
%% Updates given document by replacing given fields with new values or
%% creates new one if not exists.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(Document :: datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create_or_update(Document, Diff) ->
    datastore:create_or_update(?STORE_LEVEL, Document, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Updates given document by replacing given fields with new values or
%% creates new one if not exists.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(Level :: datastore:store_level(), Document :: datastore:document(),
    Diff :: datastore:document_diff()) -> {ok, datastore:ext_key()} | datastore:create_error().
create_or_update(Level, Document, Diff) ->
    datastore:create_or_update(Level, Document, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback get/1 but allows
%% choice of store level.
%% @end
%%--------------------------------------------------------------------
-spec get(Level :: datastore:store_level(), datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Level, Key) ->
    datastore:get(Level, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(?STORE_LEVEL, ?MODEL_NAME, ?GET_ALL, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records at chosen store level.
%% @end
%%--------------------------------------------------------------------
-spec list(Level :: datastore:store_level()) -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list(Level) ->
    datastore:list(Level, ?MODEL_NAME, ?GET_ALL, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of records filtered with provided function.
%% @end
%%--------------------------------------------------------------------
-spec list(Level :: datastore:store_level(), Filter :: datastore:list_fun(), Acc :: term()) ->
    {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list(Level, Filter, Acc) ->
    datastore:list(Level, ?MODEL_NAME, Filter, Acc).

%%--------------------------------------------------------------------
%% @doc
%% Dirty alternative of list/3.
%% Returns list of records filtered with provided function.
%% @end
%%--------------------------------------------------------------------
-spec list_dirty(Level :: datastore:store_level(), Filter :: datastore:list_fun(), Acc :: term()) ->
    {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list_dirty(Level, Filter, Acc) ->
    datastore:list_dirty(Level, ?MODEL_NAME, Filter, Acc).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of records not persisted.
%% @end
%%--------------------------------------------------------------------
-spec list_docs_to_be_dumped(Level :: datastore:store_level()) ->
    {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list_docs_to_be_dumped(Level) ->
    Filter = fun
        ('$end_of_table', Acc) ->
            {abort, Acc};
        (#document{key = Uuid, value = #cache_controller{last_user = non, action = to_be_del}}, Acc) ->
            {next, [Uuid | Acc]};
        (#document{value = #cache_controller{last_user = non}}, Acc) ->
            {next, Acc};
        % TODO check how old is clear and clear if possible
        (#document{value = #cache_controller{action = cleared}}, Acc) ->
            {next, Acc};
        (#document{key = Uuid}, Acc) ->
            {next, [Uuid | Acc]}
    end,
    datastore:list(Level, ?MODEL_NAME, Filter, []).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback delete/1 but allows
%% choice of store level.
%% @end
%%--------------------------------------------------------------------
-spec delete(Level :: datastore:store_level(), datastore:ext_key()) ->
    ok | datastore:generic_error().
delete(Level, Key) ->
    datastore:delete(Level, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:store_level(), datastore:ext_key(), datastore:delete_predicate()) ->
    ok | datastore:generic_error().
delete(Level, Key, Pred) ->
    datastore:delete(Level, ?MODULE, Key, Pred).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback exists/1 but allows
%% choice of store level.
%% @end
%%--------------------------------------------------------------------
-spec exists(Level :: datastore:store_level(), datastore:ext_key()) -> datastore:exists_return().
exists(Level, Key) ->
    ?RESPONSE(datastore:exists(Level, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    % TODO - check if transactions are realy needed
%%     ?MODEL_CONFIG(cc_bucket, get_hooks_config(),
%%         ?DEFAULT_STORE_LEVEL, ?DEFAULT_STORE_LEVEL, false).
    ?MODEL_CONFIG(cc_bucket, get_hooks_config(), ?GLOBAL_ONLY_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok | datastore:generic_error().
'after'(ModelName, get, disk_only, [Key], {ok, Doc}) ->
    Level2 = caches_controller:cache_to_datastore_level(ModelName),
    update_usage_info(Key, ModelName, Doc, Level2);
%%'after'(ModelName, get, Level, [Key], {ok, _}) ->
%%    update_usage_info(Key, ModelName, Level);
%%'after'(ModelName, exists, disk_only, [Key], {ok, true}) ->
%%    Level2 = caches_controller:cache_to_datastore_level(ModelName),
%%    update_usage_info(Key, ModelName, Level2);
%%'after'(ModelName, exists, Level, [Key], {ok, true}) ->
%%    update_usage_info(Key, ModelName, Level);
'after'(ModelName, fetch_link, disk_only, [Key, LinkName], {ok, Doc}) ->
    Level2 = caches_controller:cache_to_datastore_level(ModelName),
    update_usage_info(link_cache_key(ModelName, Key, LinkName), ModelName, Doc, Level2);
%%'after'(ModelName, fetch_link, Level, [Key, LinkName], {ok, _}) ->
%%    update_usage_info({Key, LinkName, cache_controller_link_key}, ModelName, Level);
'after'(_ModelName, save, disk_only, _Context, _ReturnValue) ->
    ok;
'after'(ModelName, save, Level, [#document{generated_uuid = true}], {ok, K}) ->
    CCCUuid = caches_controller:get_cache_uuid(K, ModelName),
    caches_controller:init_consistency_info(Level, CCCUuid);
'after'(_ModelName, create, disk_only, _Context, _ReturnValue) ->
    ok;
'after'(ModelName, create, Level, _Context, {ok, K}) ->
    CCCUuid = caches_controller:get_cache_uuid(K, ModelName),
    caches_controller:init_consistency_info(Level, CCCUuid);
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) ->
    ok | {task, task_manager:task()} | {tasks, [task_manager:task()]} | datastore:generic_error().
before(ModelName, Method, Level, Context) ->
    Level2 = caches_controller:cache_to_datastore_level(ModelName),
    before(ModelName, Method, Level, Context, Level2).
before(ModelName, save, disk_only, [Doc] = Args, Level2) ->
    start_disk_op(Doc#document.key, ModelName, save, Args, Level2);
before(ModelName, create_or_update, disk_only, [Doc, _Diff] = Args, Level2) ->
    start_disk_op(Doc#document.key, ModelName, create_or_update, Args, Level2);
before(ModelName, update, disk_only, [Key, _Diff] = Args, Level2) ->
    start_disk_op(Key, ModelName, update, Args, Level2);
before(ModelName, create, Level, [#document{generated_uuid = false} = Doc], Level) ->
    check_create(Doc#document.key, ModelName, Level);
before(ModelName, create, disk_only, [Doc] = Args, Level2) ->
    start_disk_op(Doc#document.key, ModelName, create, Args, Level2);
before(ModelName, delete, Level, [Key, _Pred], Level) ->
    before_del(Key, ModelName, Level, delete);
before(ModelName, delete, disk_only, [Key, _Pred] = Args, Level2) ->
    start_disk_op(Key, ModelName, delete, Args, Level2);
before(ModelName, get, disk_only, [Key], Level2) ->
    check_get(Key, ModelName, Level2);
before(ModelName, exists, disk_only, [Key], Level2) ->
    check_exists(Key, ModelName, Level2);
before(ModelName, fetch_link, disk_only, [Key, LinkName], Level2) ->
    check_fetch(link_cache_key(ModelName, Key, LinkName), ModelName, Level2);
before(ModelName, add_links, disk_only, [Key, Links], Level2) ->
    Tasks = lists:foldl(fun({LN, _}, Acc) ->
        [start_disk_op(link_cache_key(ModelName, Key, LN), ModelName, add_links, [Key, [LN]], Level2, false) | Acc]
    end, [], Links),
    {ok, SleepTime} = application:get_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms),
    timer:sleep(SleepTime),
    {tasks, Tasks};
before(ModelName, set_links, disk_only, [Key, Links], Level2) ->
    Tasks = lists:foldl(fun({LN, _}, Acc) ->
        [start_disk_op(link_cache_key(ModelName, Key, LN), ModelName, set_links, [Key, [LN]], Level2, false) | Acc]
    end, [], Links),
    {ok, SleepTime} = application:get_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms),
    timer:sleep(SleepTime),
    {tasks, Tasks};
before(ModelName, create_link, Level, [Key, {LinkName, _}], Level) ->
    check_link_create(Key, LinkName, ModelName, Level);
before(ModelName, create_link, disk_only, [Key, {LinkName, _}] = Args, Level2) ->
    start_disk_op(link_cache_key(ModelName, Key, LinkName), ModelName, create_link, Args, Level2);
before(ModelName, delete_links, Level, [Key, Links], Level) ->
    lists:foldl(fun(Link, Acc) ->
        Ans = before_del(link_cache_key(ModelName, Key, Link), ModelName, Level, Link),
        case Ans of
            ok ->
                Acc;
            _ ->
                Ans
        end
    end, ok, Links);
before(ModelName, delete_links, disk_only, [Key, Links], Level2) ->
    Tasks = lists:foldl(fun(Link, Acc) ->
        [start_disk_op(link_cache_key(ModelName, Key, Link), ModelName, delete_links, [Key, [Link]], Level2, false) | Acc]
    end, [], Links),
    {ok, SleepTime} = application:get_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms),
    timer:sleep(SleepTime),
    {tasks, Tasks};
before(_ModelName, _Method, _Level, _Context, _Level2) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates cache key for link entry based on link's name and document's key.
%% @end
%%--------------------------------------------------------------------
-spec link_cache_key(model_behaviour:model_type(), datastore:ext_key(), datastore:link_name()) ->
    {datastore:ext_key(), datastore:link_name(), cache_controller_link_key}.
link_cache_key(ModelName, Key, Link) ->
    {LinkRawName, _, _} = links_utils:unpack_link_scope(ModelName, Link),
    {Key, LinkRawName, cache_controller_link_key}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Provides hooks configuration.
%% @end
%%--------------------------------------------------------------------
-spec get_hooks_config() -> list().
get_hooks_config() ->
    caches_controller:get_hooks_config(datastore_config:global_caches() ++ datastore_config:local_caches()).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates information about usage of a document.
%% @end
%%--------------------------------------------------------------------
-spec update_usage_info(Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name(), cache_controller_link_key},
    ModelName :: model_behaviour:model_type(), Level :: datastore:store_level()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
update_usage_info(Key, ModelName, Level) ->
    Uuid = caches_controller:get_cache_uuid(Key, ModelName),
    UpdateFun = fun(Record) ->
        {ok, Record#cache_controller{timestamp = os:timestamp()}}
    end,
    TS = os:timestamp(),
    V = #cache_controller{timestamp = TS, last_action_time = TS},
    Doc = #document{key = Uuid, value = V},
    create_or_update(Level, Doc, UpdateFun).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates information about usage of a document and saves doc to memory.
%% @end
%%--------------------------------------------------------------------
-spec update_usage_info(Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name(), cache_controller_link_key},
    ModelName :: model_behaviour:model_type(), Doc :: datastore:document(), Level :: datastore:store_level()) ->
    boolean() | datastore:generic_error().
update_usage_info(Key, ModelName, Doc, Level) ->
    update_usage_info(Key, ModelName, Level),
    restore_from_disk(Key, ModelName, Doc, Level).

%%--------------------------------------------------------------------
%% @doc
%% Reads from disk and saves to memory.
%% @end
%%--------------------------------------------------------------------
-spec restore_from_disk(Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name(), cache_controller_link_key},
    ModelName :: model_behaviour:model_type(), Doc :: datastore:document(), Level :: datastore:store_level()) ->
    boolean() | datastore:generic_error().
restore_from_disk({Key, LinkName, cache_controller_link_key}, ModelName, Doc, Level) ->
    ModelConfig = ModelName:model_init(),
    FullArgs = [ModelConfig, Key, {LinkName, Doc}],
    CCCUuid = caches_controller:get_cache_uuid(Key, ModelName),
    caches_controller:consistency_info_lock(CCCUuid, LinkName,
        fun() ->
            case erlang:apply(datastore:level_to_driver(Level), create_link, FullArgs) of
                ok ->
                    caches_controller:save_consistency_restored_info(Level, CCCUuid, LinkName);
                {error, already_exists} ->
                    caches_controller:save_consistency_restored_info(Level, CCCUuid, LinkName);
                Error ->
                    Error
            end
        end);
restore_from_disk(Key, ModelName, Doc, Level) ->
    ModelConfig = ModelName:model_init(),
    FullArgs = [ModelConfig, Doc],
    caches_controller:consistency_info_lock(ModelName, Key,
        fun() ->
            case erlang:apply(datastore:level_to_driver(Level), create, FullArgs) of
                {ok, _} ->
                    caches_controller:save_consistency_restored_info(Level, ModelName, Key);
                {error, already_exists} ->
                    caches_controller:save_consistency_restored_info(Level, ModelName, Key);
                Error ->
                    Error
            end
        end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if get operation should be performed.
%% @end
%%--------------------------------------------------------------------
-spec check_get(Key :: datastore:ext_key(), ModelName :: model_behaviour:model_type(),
    Level :: datastore:store_level()) -> ok | {error, {not_found, model_behaviour:model_type()}}.
check_get(Key, ModelName, Level) ->
    case caches_controller:check_cache_consistency(Level, ModelName) of
        {ok, _, _} ->
            {error, {not_found, ModelName}};
        {monitored, ClearedList, _, _} ->
            case lists:member(Key, ClearedList) of
                true ->
                    check_disk_read(Key, ModelName, Level, {error, {not_found, ModelName}});
                _ ->
                    {error, {not_found, ModelName}}
            end;
        _ ->
            check_disk_read(Key, ModelName, Level, {error, {not_found, ModelName}})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if exists operation should be performed.
%% @end
%%--------------------------------------------------------------------
-spec check_exists(Key :: datastore:ext_key(), ModelName :: model_behaviour:model_type(),
    Level :: datastore:store_level()) -> ok | {ok, false}.
check_exists(Key, ModelName, Level) ->
    case caches_controller:check_cache_consistency(Level, ModelName) of
        {ok, _, _} ->
            {ok, false};
        {monitored, ClearedList, _, _} ->
            case lists:member(Key, ClearedList) of
                true ->
                    check_disk_read(Key, ModelName, Level, {ok, false});
                _ ->
                    {ok, false}
            end;
        _ ->
            check_disk_read(Key, ModelName, Level, {ok, false})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if fetch operation should be performed.
%% @end
%%--------------------------------------------------------------------
-spec check_fetch(Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name(), check_disk_read},
    ModelName :: model_behaviour:model_type(), Level :: datastore:store_level()) -> ok | {error, link_not_found}.
check_fetch({Key, LinkName, cache_controller_link_key} = CacheKey, ModelName, Level) ->
    CCCUuid = caches_controller:get_cache_uuid(Key, ModelName),
    case caches_controller:check_cache_consistency(Level, CCCUuid) of
        {ok, _, _} ->
            {error, link_not_found};
        {monitored, ClearedList, _, _} ->
            case lists:member(LinkName, ClearedList) of
                true ->
                    check_disk_fetch(CacheKey, ModelName, Level, {error, link_not_found});
                _ ->
                    {error, link_not_found}
            end;
        _ ->
            check_disk_fetch(CacheKey, ModelName, Level, {error, link_not_found})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if operation on disk should be performed.
%% @end
%%--------------------------------------------------------------------
-spec check_disk_read(Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name(), check_disk_read},
    ModelName :: model_behaviour:model_type(), Level :: datastore:store_level(),
    ErrorAns :: term()) -> term().
check_disk_read(Key, ModelName, Level, ErrorAns) ->
    Uuid = caches_controller:get_cache_uuid(Key, ModelName),
    case get(Level, Uuid) of
        {ok, Doc} ->
            Value = Doc#document.value,
            case Value#cache_controller.action of
                non -> ok;
                cleared -> ok;
                _ -> ErrorAns
            end;
        {error, {not_found, _}} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if operation on disk should be performed.
%% @end
%%--------------------------------------------------------------------
-spec check_disk_fetch(Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name(), check_disk_read},
    ModelName :: model_behaviour:model_type(), Level :: datastore:store_level(),
    ErrorAns :: term()) -> term().
check_disk_fetch({DocKey, RawLinkName, _} = CacheKey, ModelName, Level, ErrorAns) ->
    Uuid = caches_controller:get_cache_uuid(CacheKey, ModelName),
    DiskDriver = datastore:driver_to_module(datastore:level_to_driver(?DISK_ONLY_LEVEL)),
    ModelConfig = ModelName:model_init(),

    ExcludedFetch = fun(ExcludedLinkName) ->
        case DiskDriver:fetch_link(ModelConfig, DocKey, RawLinkName) of
            {ok, {V, LinkTargets}}  ->
                {RawLinkName0, Scope, _} = links_utils:unpack_link_scope(ModelName, ExcludedLinkName),
                ExcludedTarget = links_utils:select_scope_related_link(RawLinkName0, Scope, undefined, LinkTargets),
                ExcludedTargets = case ExcludedTarget of
                    undefined ->
                        [];
                    _ ->
                        [ExcludedTarget]
                end,
                {ok, {V, LinkTargets -- ExcludedTargets}};
            OtherRes ->
                OtherRes
        end
    end,

    case get(Level, Uuid) of
        {ok, Doc} ->
            Value = Doc#document.value,
            ActionData = Value#cache_controller.action_data,
            case Value#cache_controller.action of
                non -> ok;
                cleared -> ok;
                to_be_del ->
                    ExcludedFetch(ActionData);
                delete_links ->
                    ExcludedFetch(ActionData);
                _ -> ErrorAns
            end;
        {error, {not_found, _}} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Delates info about dumping of cache to disk.
%% @end
%%--------------------------------------------------------------------
-spec delete_dump_info(Uuid :: binary(), Owner :: list(), Level :: datastore:store_level()) ->
    ok | datastore:generic_error().
delete_dump_info(Uuid, Owner, Level) ->
    {CCCUuid, ClearName} = case caches_controller:decode_uuid(Uuid) of
        {ModelName, {Key, Link, cache_controller_link_key}} ->
            {caches_controller:get_cache_uuid(Key, ModelName), Link};
        {ModelName, Key} ->
            {ModelName, Key}
    end,
    caches_controller:consistency_info_lock(CCCUuid, ClearName,
        fun() ->
            Pred = fun() ->
                {LastUser, Action} = case get(Level, Uuid) of
                               {ok, #document{value = #cache_controller{last_user = LU, action = A}}} ->
                                   {LU, A};
                               {error, {not_found, _}} ->
                                   {non, non}
                           end,

                Ans1 = case {LastUser, Action} of
                    {_, to_be_del} ->
                        false;
                    {Owner, _} ->
                        true;
                    {non, _} ->
                        true;
                    _ ->
                        false
                end,
                case Ans1 of
                    true ->
                        caches_controller:save_consistency_restored_info(Level, CCCUuid, ClearName);
                    _ ->
                        Ans1
                end
            end,
            delete(Level, Uuid, Pred)
        end).
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves dump information after disk operation.
%% @end
%%--------------------------------------------------------------------
-spec end_disk_op(Uuid :: binary(), Owner :: list(), ModelName :: model_behaviour:model_type(),
    Op :: atom(), Level :: datastore:store_level()) -> ok.
end_disk_op(Uuid, Owner, _ModelName, Op, Level) ->
    try
        case Op of
            delete ->
                delete_dump_info(Uuid, Owner, Level);
            delete_links ->
                delete_dump_info(Uuid, Owner, Level);
            _ ->
                UpdateFun = fun
                    (#cache_controller{last_user = LastUser, action = A} = Record) ->
                        case {LastUser, A} of
                            {Owner, to_be_del} ->
                                {ok, Record#cache_controller{last_user = non,
                                    last_action_time = os:timestamp()}};
                            {Owner, _} ->
                                {ok, Record#cache_controller{last_user = non, action = non,
                                    last_action_time = os:timestamp()}};
                            _ ->
                                throw(user_changed)
                        end
                end,
                update(Level, Uuid, UpdateFun)
        end,
        ok
    catch
        throw:user_changed ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Choose action that should be done.
%% @end
%%--------------------------------------------------------------------
-spec choose_action(Op :: atom(), Level :: datastore:store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name(), cache_controller_link_key}, Uuid :: binary()) ->
    ok | {ok, non} | {ok, NewMethod, NewArgs} | {get_error, Error} | {fetch_error, Error} when
    NewMethod :: atom(), NewArgs :: term(), Error :: datastore:generic_error().
choose_action(Op, Level, ModelName, Key, Uuid) ->
    choose_action(Op, Level, ModelName, Key, Uuid, false).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Choose action that should be done.
%% @end
%%--------------------------------------------------------------------
-spec choose_action(Op :: atom(), Level :: datastore:store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name(), cache_controller_link_key},
    Uuid :: binary(), Flush :: boolean()) ->
    ok | {ok, non} | {ok, NewMethod, NewArgs} | {get_error, Error} | {fetch_error, Error} when
    NewMethod :: atom(), NewArgs :: term(), Error :: datastore:generic_error().
choose_action(Op, Level, ModelName, {Key, Link, cache_controller_link_key}, Uuid, Flush) ->
    % check for create/delete race
    ModelConfig = ModelName:model_init(),
    case Op of
        delete_links ->
            case erlang:apply(datastore:driver_to_module(datastore:level_to_driver(Level)),
                fetch_link, [ModelConfig, Key, Link]) of
                {ok, SavedValue} ->
                    {ok, set_links, [Key, [{Link, SavedValue}]]};
                {error, link_not_found} ->
                    case get(Level, Uuid) of
                        {ok, Doc} ->
                            Value = Doc#document.value,
                            case Value#cache_controller.action of
                                cleared ->
                                    {ok, non};
                                non ->
                                    {ok, non};
                                _ ->
                                    ok
                            end;
                        {error, {not_found, _}} ->
                            ok
                    end;
                FetchError ->
                    {fetch_error, FetchError}
            end;
        _ ->
            case erlang:apply(datastore:driver_to_module(datastore:level_to_driver(Level)),
                fetch_link, [ModelConfig, Key, Link]) of
                {ok, SavedValue} ->
                    case Flush of
                        true ->
                            UpdateFun = fun(LinkValue) ->
                                case LinkValue of
                                    SavedValue ->
                                        {error, already_updated};
                                    _ ->
                                        {ok, SavedValue}
                                end
                                        end,
                            case get(Level, Uuid) of
                                {ok, Doc} ->
                                    Value = Doc#document.value,
                                    case Value#cache_controller.action of
                                        cleared ->
                                            {ok, create_or_update_link, [Key, {Link, SavedValue}, UpdateFun]};
                                        non ->
                                            {ok, create_or_update_link, [Key, {Link, SavedValue}, UpdateFun]};
                                        _ ->
                                            {ok, set_links, [Key, [{Link, SavedValue}]]}
                                    end;
                                {error, {not_found, _}} ->
                                    {ok, create_or_update_link, [Key, {Link, SavedValue}, UpdateFun]}
                            end;
                        _ ->
                            {ok, set_links, [Key, [{Link, SavedValue}]]}
                    end;
                {error, link_not_found} ->
                    case get(Level, Uuid) of
                        {ok, Doc} ->
                            Value = Doc#document.value,
                            case Value#cache_controller.action of
                                cleared ->
                                    {ok, non};
                                non ->
                                    {ok, non};
                                _ ->
                                    {ok, delete_links, [Key, [Value#cache_controller.action_data]]}
                            end;
                        {error, {not_found, _}} ->
                            case Flush of
                                true ->
                                    {ok, non};
                                _ ->
                                    {ok, delete_links, [Key, [Link]]}
                            end
                    end;
                FetchError ->
                    {fetch_error, FetchError}
            end
    end;
choose_action(Op, Level, ModelName, Key, Uuid, Flush) ->
    % check for create/delete race
    ModelConfig = ModelName:model_init(),
    case Op of
        delete ->
            case erlang:apply(datastore:driver_to_module(datastore:level_to_driver(Level)),
                get, [ModelConfig, Key]) of
                {ok, SavedValue} ->
                    {ok, save, [SavedValue]};
                {error, {not_found, _}} ->
                    ok;
                GetError ->
                    {get_error, GetError}
            end;
        _ ->
            case erlang:apply(datastore:driver_to_module(datastore:level_to_driver(Level)),
                get, [ModelConfig, Key]) of
                {ok, #document{value = SavedValue} = SavedDoc} ->
                    case Flush of
                        true ->
                            UpdateFun = fun(Record) ->
                                case Record of
                                    SavedValue ->
                                        {error, already_updated};
                                    _ ->
                                        {ok, SavedValue}
                                end
                                        end,
                            case get(Level, Uuid) of
                                {ok, Doc} ->
                                    Value = Doc#document.value,
                                    case Value#cache_controller.action of
                                        cleared ->
                                            {ok, create_or_update, [SavedDoc, UpdateFun]};
                                        non ->
                                            {ok, create_or_update, [SavedDoc, UpdateFun]};
                                        _ ->
                                            {ok, save, [SavedDoc]}
                                    end;
                                {error, {not_found, _}} ->
                                    {ok, create_or_update, [SavedDoc, UpdateFun]}
                            end;
                        _ ->
                            {ok, save, [SavedDoc]}
                    end;
                {error, {not_found, _}} ->
                    case get(Level, Uuid) of
                        {ok, Doc} ->
                            Value = Doc#document.value,
                            case Value#cache_controller.action of
                                cleared ->
                                    {ok, non};
                                non ->
                                    {ok, non};
                                _ ->
                                    {ok, delete, [Key, ?PRED_ALWAYS]}
                            end;
                        {error, {not_found, _}} ->
                            case Flush of
                                true ->
                                    {ok, non};
                                _ ->
                                    {ok, delete, [Key, ?PRED_ALWAYS]}
                            end
                    end;
                GetError ->
                    {get_error, GetError}
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if action should proceed (clearing memory was done).
%% @end
%%--------------------------------------------------------------------
-spec check_action_after_clear(Op :: atom(), Level :: datastore:store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name(), cache_controller_link_key}) -> ok | no_return().
check_action_after_clear(Op, Level, ModelName, {Key, Link, cache_controller_link_key}) ->
    case Op of
        delete_links ->
            ok;
        _ ->
            ModelConfig = ModelName:model_init(),
            case erlang:apply(datastore:driver_to_module(datastore:level_to_driver(Level)),
                fetch_link, [ModelConfig, Key, Link]) of
                {ok, _} ->
                    ok;
                {error, link_not_found} ->
                    throw(cleared);
                FetchError ->
                    throw({fetch_error, FetchError})
            end
    end;
check_action_after_clear(Op, Level, ModelName, Key) ->
    % check for create/delete race
    ModelConfig = ModelName:model_init(),
    case Op of
        delete ->
            ok;
        _ ->
            case erlang:apply(datastore:driver_to_module(datastore:level_to_driver(Level)),
                get, [ModelConfig, Key]) of
                {ok, _} ->
                    ok;
                {error, {not_found, _}} ->
                    throw(cleared);
                GetError ->
                    throw({get_error, GetError})
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves dump information about disk operation and decides if it should be done.
%% @end
%%--------------------------------------------------------------------
-spec start_disk_op(Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name(), check_disk_read},
    ModelName :: model_behaviour:model_type(), Op :: atom(), Args :: list(), Level :: datastore:store_level()) ->
    ok | {task, task_manager:task()} | {error, Error} when
    Error :: not_last_user | preparing_disk_op_failed.
start_disk_op(Key, ModelName, Op, Args, Level) ->
    start_disk_op(Key, ModelName, Op, Args, Level, true).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves dump information about disk operation and decides if it should be done.
%% @end
%%--------------------------------------------------------------------
-spec start_disk_op(Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name(), check_disk_read},
    ModelName :: model_behaviour:model_type(), Op :: atom(), Args :: list(),
    Level :: datastore:store_level(), Sleep :: boolean()) -> ok | {task, task_manager:task()} | {error, Error} when
    Error :: not_last_user | preparing_disk_op_failed.
start_disk_op(Key, ModelName, Op, Args, Level, Sleep) ->
    try
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),
        Pid = pid_to_list(self()),

        UpdateFun = fun(Record) ->
            case Record#cache_controller.action of
                cleared ->
                    ok = check_action_after_clear(Op, Level, ModelName, Key),
                    {ok, Record#cache_controller{last_user = Pid, timestamp = os:timestamp(), action = Op}};
                _ ->
                    {ok, Record#cache_controller{last_user = Pid, timestamp = os:timestamp(), action = Op}}
            end
        end,
        % TODO - not transactional updates in local store - add transactional create and update on ets
        TS = os:timestamp(),
        V = #cache_controller{last_user = Pid, timestamp = TS, action = Op, last_action_time = TS},
        Doc = #document{key = Uuid, value = V},
        create_or_update(Level, Doc, UpdateFun),

        case Sleep of
            true ->
                {ok, SleepTime} = application:get_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms),
                timer:sleep(SleepTime);
            _ ->
                ok
        end,

        ModelConfig = ModelName:model_init(),
        LinksContext = links_utils:get_context_to_propagate(ModelConfig),

        Task = fun() ->
            {LastUser, LAT, LACT} = case get(Level, Uuid) of
                {ok, Doc2} ->
                    Value = Doc2#document.value,
                    {Value#cache_controller.last_user, Value#cache_controller.last_action_time,
                        Value#cache_controller.action};
                {error, {not_found, _}} ->
                    {Pid, 0, non}
            end,
            ToDo0 = case {LACT, LastUser} of
                {cleared, _} ->
                    {ok, non};
                {_, ToUpdate} when ToUpdate =:= Pid; ToUpdate =:= non ->
                    ok;
                _ ->
                    {ok, ForceTime} = application:get_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms),
                    case timer:now_diff(os:timestamp(), LAT) >= 1000 * ForceTime of
                        true ->
                            UpdateFun2 = fun(Record) ->
                                {ok, Record#cache_controller{last_action_time = os:timestamp()}}
                                         end,
                            update(Level, Uuid, UpdateFun2),
                            ok;
                        _ ->
                            {error, not_last_user}
                    end
            end,
            Ans = case ToDo0 of
                      ok ->
                          links_utils:apply_context(LinksContext),
                          critical_section:run({?MODULE, start_disk_op, Uuid},
                              fun() ->
                                  ToDo = choose_action(Op, Level, ModelName, Key, Uuid),

                                  case ToDo of
                                      {ok, NewMethod, NewArgs} ->
                                          FullArgs = [ModelConfig | NewArgs],
                                          CallAns = erlang:apply(datastore:driver_to_module(?PERSISTENCE_DRIVER),
                                              NewMethod, FullArgs),
                                          catch datastore:run_posthooks(ModelConfig, NewMethod,
                                              datastore:driver_to_level(?PERSISTENCE_DRIVER), NewArgs, CallAns),
                                          {op_change, NewMethod, CallAns};
                                      ok ->
                                          FullArgs = [ModelConfig | Args],
                                          CallAns = erlang:apply(datastore:driver_to_module(?PERSISTENCE_DRIVER),
                                              Op, FullArgs),
                                          catch datastore:run_posthooks(ModelConfig, Op,
                                              datastore:driver_to_level(?PERSISTENCE_DRIVER), Args, CallAns),
                                          CallAns;
                                      {ok, non} ->
                                          ok;
                                      Other ->
                                          Other
                                  end
                              end
                          );
                      _ ->
                          ToDo0
            end,

            ok = case Ans of
                ok ->
                    end_disk_op(Uuid, Pid, ModelName, Op, Level);
                {ok, _} ->
                    end_disk_op(Uuid, Pid, ModelName, Op, Level);
                {error, not_last_user} -> ok;
                {op_change, NewOp, ok} ->
                    end_disk_op(Uuid, Pid, ModelName, NewOp, Level);
                {op_change, NewOp, {ok, _}} ->
                    end_disk_op(Uuid, Pid, ModelName, NewOp, Level);
                WrongAns -> WrongAns
            end
        end,
        {task, Task}
    catch
        throw:cleared ->
            % Do not log - such race may happen and it ends disk operation
            {error, cleared};
        E1:E2 ->
            ?error_stacktrace("Error in cache_controller start_disk_op. Args: ~p. Error: ~p:~p.",
                [{Key, ModelName, Op, Level}, E1, E2]),
            {error, preparing_disk_op_failed}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Marks document before delete operation.
%% @end
%%--------------------------------------------------------------------
-spec before_del(Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name(), check_disk_read},
    ModelName :: model_behaviour:model_type(), Level :: datastore:store_level(), Op :: {atom(), term()} | atom()) ->
    ok | {error, preparing_op_failed}.
before_del(Key, ModelName, Level, Op) ->
    try
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),

        UpdateFun = fun(Record) ->
            {ok, Record#cache_controller{action = to_be_del, action_data = Op}}
        end,
        % TODO - not transactional updates in local store - add transactional create and update on ets
        V = #cache_controller{action = to_be_del},
        Doc = #document{key = Uuid, value = V},
        {ok, _} = create_or_update(Level, Doc, UpdateFun),
        ok
    catch
        E1:E2 ->
            ?error_stacktrace("Error in cache_controller before_del. Args: ~p. Error: ~p:~p.",
                [{Key, ModelName, Level}, E1, E2]),
            {error, preparing_op_failed}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if document may be created.
%% @end
%%--------------------------------------------------------------------
-spec check_create(Key :: datastore:ext_key(), ModelName :: model_behaviour:model_type(),
    Level :: datastore:store_level()) -> ok | datastore:generic_error().
check_create(Key, ModelName, Level) ->
    Proceed = case caches_controller:check_cache_consistency(Level, ModelName) of
        {ok, _, _} ->
            false;
        {monitored, ClearedList, _, _} ->
            lists:member(Key, ClearedList);
        _ ->
            true
    end,

    case Proceed of
        true ->
            Uuid = caches_controller:get_cache_uuid(Key, ModelName),
            Check = case get(Level, Uuid) of
                        {ok, Doc2} ->
                            Value = Doc2#document.value,
                            Action = Value#cache_controller.action,
                            not ((Action =:= delete) orelse (Action =:= to_be_del));
                        {error, {not_found, _}} ->
                            true
                    end,
            case Check of
                true ->
                    case erlang:apply(datastore:driver_to_module(datastore:level_to_driver(disk_only)),
                        exists, [ModelName:model_init(), Key]) of
                        {ok, false} ->
                            ok;
                        {ok, true} ->
                            {error, already_exists};
                        Other ->
                            Other
                    end;
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if link may be created.
%% @end
%%--------------------------------------------------------------------
-spec check_link_create(Key :: datastore:ext_key(), LinkName :: datastore:link_name(),
    ModelName :: model_behaviour:model_type(), Level :: datastore:store_level()) ->
    ok | datastore:generic_error().
check_link_create(Key, LinkName, ModelName, Level) ->
    CCCUuid = caches_controller:get_cache_uuid(Key, ModelName),
    Proceed = case caches_controller:check_cache_consistency(Level, CCCUuid) of
                  {ok, _, _} ->
                      false;
                  {monitored, ClearedList, _, _} ->
                      lists:member(LinkName, ClearedList);
                  _ ->
                      true
              end,

    case Proceed of
        true ->
            Uuid = caches_controller:get_cache_uuid(link_cache_key(ModelName, Key, LinkName), ModelName),
            Check = case get(Level, Uuid) of
                        {ok, Doc2} ->
                            Value = Doc2#document.value,
                            Action = Value#cache_controller.action,
                            not ((Action =:= delete_links) orelse (Action =:= to_be_del));
                        {error, {not_found, _}} ->
                            true
                    end,
            case Check of
                true ->
                    case erlang:apply(datastore:driver_to_module(datastore:level_to_driver(disk_only)),
                        fetch_link, [ModelName:model_init(), Key, LinkName]) of
                        {error, link_not_found} ->
                            ok;
                        {ok, _} ->
                            {error, already_exists};
                        Other ->
                            Other
                    end;
                _ ->
                    ok
            end;
        _ ->
            ok
    end.
