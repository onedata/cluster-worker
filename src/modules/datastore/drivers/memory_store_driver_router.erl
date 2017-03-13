%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc High Level Mnesia database driver.
%%% @end
%%%-------------------------------------------------------------------
-module(memory_store_driver_router).
-author("Rafal Slota").
-behaviour(store_driver_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% store_driver_behaviour callbacks
-export([save/2, update/3, create/2, create_or_update/3, exists/2, get/2, list/4, delete/3, is_model_empty/1]).
-export([add_links/3, add_links/4, set_links/3, create_link/3, delete_links/3, delete_links/4, fetch_link/3, foreach_link/4]).
-export([run_transation/1, run_transation/2, run_transation/3]).
-export([clear/2, clear_links/2, force_save/2, force_link_save/3, force_link_save/4]).

% TODO - internal link implementation - delete from datastore API
-export([get_link_doc/4, exists_link_doc/3]).

%% auxiliary ordered_store behaviour
-export([create_auxiliary_caches/3, aux_delete/3, aux_save/3, aux_update/3,
    aux_create/3, aux_first/2, aux_next/3]).

%% for rpc
-export([direct_call_internal/5, direct_link_call_internal/5, foreach_link_internal/4]).

%%%===================================================================
%%% store_driver_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback save/2.
%% @end
%%--------------------------------------------------------------------
-spec save(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(ModelConfig, Document) ->
    deletage_call(save, ModelConfig, Document#document.key, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(model_behaviour:model_config(), datastore:ext_key(),
    Diff :: datastore:document_diff()) -> {ok, datastore:ext_key()} | datastore:update_error().
update(ModelConfig, Key, Diff) ->
    deletage_call(update, ModelConfig, Key, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create/2.
%% @end
%%--------------------------------------------------------------------
-spec create(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(ModelConfig, Document) ->
    deletage_call(create, ModelConfig, Document#document.key, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_or_update/2.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(model_behaviour:model_config(), datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create_or_update(ModelConfig, Document, Diff) ->
    deletage_call(create_or_update, ModelConfig, Document#document.key, [Document, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get(#model_config{store_level = ?GLOBAL_ONLY_LEVEL} = ModelConfig, Key) ->
    direct_call(get, ModelConfig, Key, [Key]);
get(#model_config{name = MN} = ModelConfig, Key) ->
    direct_call(get, ModelConfig, Key, [Key], {error, {not_found, MN}}).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback list/4.
%% @end
%%--------------------------------------------------------------------
-spec list(model_behaviour:model_config(),
    Fun :: datastore:list_fun(), AccIn :: term(), Opts :: store_driver_behaviour:list_options()) ->
    {ok, Handle :: term()} | datastore:generic_error() | no_return().
% TODO - implementation base on old implementation from datastore - refactor when local cache is finished
list(#model_config{store_level = ?GLOBAL_ONLY_LEVEL} = ModelConfig, Fun, AccIn, Opts) ->
    Nodes = consistent_hasing:get_all_nodes(),

    % TODO - use user function and catch last element not to save all docs in memory
    HelperFun = fun
        (#document{} = Document, Acc) ->
            {next, [Document | Acc]};
        (_, Acc) ->
            {abort, Acc}
    end,

    ListAns = lists:foldl(fun
        (N, {ok, Acc}) ->
            rpc:call(N, get_slave_driver(false, ModelConfig), list, [ModelConfig, HelperFun, Acc, Opts]);
        (_, Error) ->
            Error
    end, {ok, []}, Nodes),

    case ListAns of
        {ok, LA} ->
            execute_list_fun(Fun, LA, AccIn);
        Other ->
            Other
    end;
list(#model_config{name = MN} = ModelConfig, Fun, AccIn, Opts) ->
    Nodes = consistent_hasing:get_all_nodes(),

    HelperFun = fun
        (#document{key = Key} = Document, Acc) ->
            {next, maps:put(Key, Document, Acc)};
        (_, Acc) ->
            {abort, Acc}
    end,

    % TODO - one consistency check (after migration from counter to times)
    try
        ConsistencyCheck1 = caches_controller:check_cache_consistency(?GLOBAL_ONLY_LEVEL, MN),
        MemMapAns = lists:foldl(fun
            (N, {ok, Acc}) ->
                rpc:call(N, get_slave_driver(false, ModelConfig), list, [ModelConfig, HelperFun, Acc, Opts]);
            (_, Error) ->
                throw(Error)
        end, {ok, #{}}, Nodes),

        {ok, MemMap} = MemMapAns,
        FirstPhaseAns = case ConsistencyCheck1 of
            {ok, ClearCounter, _} ->
                case caches_controller:check_cache_consistency(?GLOBAL_ONLY_LEVEL, MN) of
                    {ok, ClearCounter, _} ->
                        {ok, maps:values(MemMap)};
                    _ ->
                        restore
                end;
            _ ->
                restore
        end,

        case FirstPhaseAns of
            {ok, List} ->
                execute_list_fun(Fun, List, AccIn);
            restore ->
                Pid = self(),
                caches_controller:begin_consistency_restoring(?GLOBAL_ONLY_LEVEL, MN, Pid),

                HelperFun2 = fun
                    (#document{key = Key}, Acc) ->
                        {next, [Key | Acc]};
                    (_, Acc) ->
                        {abort, Acc}
                end,

                case couchdb_datastore_driver:list(ModelConfig, HelperFun2, [], Opts) of
                    {ok, DiskList} ->
                        DiskOnlyKeys = DiskList -- maps:keys(MemMap),
                        DiskOnlyValues = lists:foldl(fun(K, Acc) ->
                            case deletage_call(get, ModelConfig, K, [K]) of
                                {ok, D} -> [D | Acc];
                                {error, {not_found, _}} -> Acc;
                                Err ->
                                    throw({restore_from_disk_error, Err})
                            end
                        end, [], DiskOnlyKeys),

                        caches_controller:end_consistency_restoring(?GLOBAL_ONLY_LEVEL, MN, Pid),
                        execute_list_fun(Fun, maps:values(MemMap) ++ DiskOnlyValues, AccIn);
                    Other ->
                        throw({restore_from_disk_error, Other})
                end
        end
    catch
        throw:{restore_from_disk_error, Err} ->
            caches_controller:end_consistency_restoring(?GLOBAL_ONLY_LEVEL, MN, self(), not_monitored),
            ?error("Listing error ~p", [{restore_from_disk_error, Err}]),
            {error, {restore_from_disk, Err}};
        throw:Error ->
            ?error("Listing error ~p", [Error]),
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback is_model_empty/1.
%% @end
%%--------------------------------------------------------------------
-spec is_model_empty(model_behaviour:model_config()) -> no_return().
is_model_empty(_ModelConfig) ->
    error(not_supported).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback add_links/3.
%% @end
%%--------------------------------------------------------------------
-spec add_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()]) ->
    ok | datastore:generic_error().
add_links(ModelConfig, Key, Links) ->
    deletage_link_call(add_links, ModelConfig, Key, [Key, Links]).

%%--------------------------------------------------------------------
%% @doc
%% Add links with override of link replica scope.
%% @end
%%--------------------------------------------------------------------
-spec add_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()],
    ReplicaScopeOverride :: links_utils:link_replica_scope()) -> ok | datastore:generic_error().
add_links(ModelConfig, Key, Links, LinkReplicaScope) ->
    deletage_link_call(add_links, ModelConfig, Key, [Key, Links, LinkReplicaScope]).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback set_links/3.
%% @end
%%--------------------------------------------------------------------
-spec set_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()]) ->
    ok | datastore:generic_error().
set_links(ModelConfig, Key, Links) ->
    deletage_link_call(set_links, ModelConfig, Key, [Key, Links]).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_link/3.
%% @end
%%--------------------------------------------------------------------
-spec create_link(model_behaviour:model_config(), datastore:ext_key(), datastore:normalized_link_spec()) ->
    ok | datastore:create_error().
create_link(ModelConfig, Key, Link) ->
    deletage_link_call(create_link, ModelConfig, Key, [Key, Link]).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete_links/3.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:link_name()] | all) ->
    ok | datastore:generic_error().
delete_links(ModelConfig, Key, LinkNames) ->
    delete_links(ModelConfig, Key, LinkNames, ?PRED_ALWAYS).

%%--------------------------------------------------------------------
%% @doc
%% Simmilar to {@link store_driver_behaviour} callback delete_links/3 witch delete predicate.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:link_name()] | all,
    datastore:delete_predicate()) -> ok | datastore:generic_error().
delete_links(ModelConfig, Key, Links, Pred) ->
    deletage_link_call(delete_links, ModelConfig, Key, [Key, Links, Pred]).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback fetch_link/3.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(model_behaviour:model_config(), datastore:ext_key(), datastore:link_name()) ->
    {ok, datastore:link_target()} | datastore:link_error().
fetch_link(#model_config{link_store_level = ?GLOBAL_ONLY_LEVEL} = ModelConfig, Key, LinkName) ->
    direct_link_call(fetch_link, ModelConfig, Key, [Key, LinkName]);
fetch_link(ModelConfig, Key, LinkName) ->
    direct_link_call(fetch_link, ModelConfig, Key, [Key, LinkName], {error, link_not_found}).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback foreach_link/4.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(model_behaviour:model_config(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error().
% TODO - implementation base on old implementation from datastore - refactor when local cache is finished
foreach_link(#model_config{link_store_level = ?GLOBAL_ONLY_LEVEL} = ModelConfig, Key, Fun, AccIn) ->
    direct_call(foreach_link, ModelConfig, Key, [Key, Fun, AccIn]);
foreach_link(#model_config{name = ModelName} = MC, Key, Fun, AccIn) ->
    CHKey = get_hashing_key(ModelName, Key),
    Node = consistent_hasing:get_node(CHKey),

    rpc:call(Node, ?MODULE, foreach_link_internal, [MC, Key, Fun, AccIn]).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} foreach_link/4 internal implementation
%% (to be executed at node that hosts links process).
%% @end
%%--------------------------------------------------------------------
-spec foreach_link_internal(model_behaviour:model_config(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error().
foreach_link_internal(#model_config{name = ModelName} = MC, Key, Fun, AccIn) ->
    CCCUuid = caches_controller:get_cache_uuid(Key, ModelName),
    % TODO - one consistency check (after migration from counter to times)
    SlaveDriver = get_slave_driver(true, MC),
    case caches_controller:check_cache_consistency_direct(SlaveDriver, CCCUuid, ModelName) of
        {ok, ClearingCounter, _} ->
            % TODO - foreach link returns error when doc is not found, and should be
            RPCAns = apply(SlaveDriver, foreach_link, [MC, Key, Fun, AccIn]),
            % TODO - check directly in SLAVE_DRIVER
            case caches_controller:check_cache_consistency_direct(SlaveDriver, CCCUuid) of
                {ok, ClearingCounter, _} ->
                    RPCAns;
                _ ->
                    deletage_link_call(foreach_link, MC, Key, [Key, Fun, AccIn])
            end;
        _ ->
            deletage_link_call(foreach_link, MC, Key, [Key, Fun, AccIn])
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete/2.
%% @end
%%--------------------------------------------------------------------
-spec delete(model_behaviour:model_config(), datastore:ext_key(), datastore:delete_predicate()) ->
    ok | datastore:generic_error().
delete(ModelConfig, Key, Pred) ->
    deletage_call(delete, ModelConfig, Key, [Key, Pred]).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, boolean()} | datastore:generic_error().
exists(#model_config{link_store_level = ?GLOBAL_ONLY_LEVEL} = ModelConfig, Key) ->
    direct_call(exists, ModelConfig, Key, [Key]);
exists(ModelConfig, Key) ->
    direct_call(exists, ModelConfig, Key, [Key], {ok, false}).

%%--------------------------------------------------------------------
%% @doc
%% Checks if document that describes links from scope exists.
%% @end
%%--------------------------------------------------------------------
-spec exists_link_doc(model_behaviour:model_config(), datastore:ext_key(), links_utils:scope()) ->
    {ok, boolean()} | datastore:generic_error().
exists_link_doc(#model_config{link_store_level = ?GLOBAL_ONLY_LEVEL} = ModelConfig, DocKey, Scope) ->
    direct_link_call(exists_link_doc, ModelConfig, DocKey, [DocKey, Scope]);
exists_link_doc(ModelConfig, DocKey, Scope) ->
    direct_link_call(exists_link_doc, ModelConfig, DocKey, [DocKey, Scope], {ok, false}).

%%--------------------------------------------------------------------
%% @doc
%% Gets link document. Allows override bucket.
%% @end
%%--------------------------------------------------------------------
-spec get_link_doc(model_behaviour:model_config(), binary(), DocKey :: datastore:ext_key(),
    MainDocKey :: datastore:ext_key()) -> {ok, datastore:document()} | datastore:generic_error().
get_link_doc(#model_config{link_store_level = ?GLOBAL_ONLY_LEVEL} = ModelConfig, BucketOverride, DocKey, MainDocKey) ->
    direct_link_call(get_link_doc, ModelConfig, MainDocKey, [BucketOverride, DocKey]);
get_link_doc(#model_config{name = ModelName} = ModelConfig, BucketOverride, DocKey, MainDocKey) ->
    direct_link_call(get_link_doc, ModelConfig, MainDocKey, [BucketOverride, DocKey], {error, {not_found, ModelName}}).


%%--------------------------------------------------------------------
%% @doc
%% Runs given function within locked ResourceId. This function makes sure that 2 funs with same ResourceId won't
%% run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec run_transation(model_behaviour:model_config(), ResourceId :: binary(), fun(() -> Result)) -> Result
    when Result :: term().
run_transation(ModelConfig, ResourceID, Fun) ->
    ?GLOBAL_SLAVE_DRIVER:run_transation(ModelConfig, ResourceID, Fun).


%%--------------------------------------------------------------------
%% @doc
%% Runs given function within locked ResourceId. This function makes sure that 2 funs with same ResourceId won't
%% run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec run_transation(ResourceId :: binary(), fun(() -> Result)) -> Result
    when Result :: term().
run_transation(ResourceID, Fun) ->
    ?GLOBAL_SLAVE_DRIVER:run_transation(ResourceID, Fun).


%%--------------------------------------------------------------------
%% @doc
%% Runs given function within transaction.
%% @end
%%--------------------------------------------------------------------
-spec run_transation(fun(() -> Result)) -> Result
    when Result :: term().
run_transation(Fun) ->
    ?GLOBAL_SLAVE_DRIVER:run_transation(Fun).


%%--------------------------------------------------------------------
%% @doc
%% Clears document from memory.
%% @end
%%--------------------------------------------------------------------
-spec clear(MC :: model_behaviour:model_config(),
    Key :: datastore:ext_key()) -> ok | datastore:generic_error().
clear(ModelConfig, Key) ->
    execute(ModelConfig, Key, false, {clear, []}, [100]).

%%--------------------------------------------------------------------
%% @doc
%% Clears document from memory.
%% @end
%%--------------------------------------------------------------------
-spec clear_links(MC :: model_behaviour:model_config(),
    Key :: datastore:ext_key()) -> ok | datastore:generic_error().
clear_links(ModelConfig, Key) ->
    execute(ModelConfig, Key, true, {clear, []}, [100]).

%%--------------------------------------------------------------------
%% @doc
%% Inserts given document to database while preserving revision number.
%% Used only for document replication.
%% @end
%%--------------------------------------------------------------------
-spec force_save(model_behaviour:model_config(), datastore:document()) ->
    ok | datastore:generic_error().
% TODO - return doc for dbsync
force_save(ModelConfig, #document{key = Key} = ToSave) ->
    deletage_call(force_save, ModelConfig, Key, [ToSave]).

%%--------------------------------------------------------------------
%% @doc
%% Inserts given document to database while preserving revision number.
%% Used only for document replication.
%% @end
%%--------------------------------------------------------------------
-spec force_link_save(model_behaviour:model_config(), datastore:document(),
    MainDocKey :: datastore:ext_key()) -> ok | datastore:generic_error().
% TODO - use main doc key
force_link_save(ModelConfig, #document{key = Key} = ToSave, MainDocKey) ->
    deletage_link_call(force_save, ModelConfig, MainDocKey, [ToSave]).

%%--------------------------------------------------------------------
%% @doc
%% Inserts given document to database while preserving revision number.
%% Used only for document replication.
%% @end
%%--------------------------------------------------------------------
-spec force_link_save(model_behaviour:model_config(), binary(), datastore:document(),
    MainDocKey :: datastore:ext_key()) -> ok | datastore:generic_error().
force_link_save(ModelConfig, BucketOverride, ToSave, MainDocKey) ->
    deletage_link_call(force_save, ModelConfig, MainDocKey, [BucketOverride, ToSave]).

%%%===================================================================
%%% auxiliary_cache_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback
%% create_auxiliary_caches/3.
%% @end
%%--------------------------------------------------------------------
-spec create_auxiliary_caches(
    model_behaviour:model_config(), Fields :: [atom()], NodeToSync :: node()) ->
    ok | datastore:generic_error() | no_return().
create_auxiliary_caches(ModelConfig, Fields, NodeToSync) ->
    SlaveDriver = get_slave_driver(false, ModelConfig),
    SlaveDriver:create_auxiliary_caches(ModelConfig, Fields, NodeToSync).


%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback aux_first/2.
%% @end
%%--------------------------------------------------------------------
-spec aux_first(model_behaviour:model_config(), Field :: atom()) ->
    datastore:aux_cache_handle().
aux_first(ModelConfig, Field) ->
    direct_local_call(aux_first, ModelConfig, [Field]).


%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback next/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_next(ModelConfig :: model_behaviour:model_config(), Field :: atom(),
    Handle :: datastore:aux_cache_handle()) -> datastore:aux_cache_handle().
aux_next(_, _, '$end_of_table') -> '$end_of_table';
aux_next(ModelConfig, Field, Handle) ->
    direct_local_call(aux_next, ModelConfig, [Field, Handle]).


%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback delete/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_delete(ModelConfig :: model_behaviour:model_config(), Field :: atom(),
    Key :: datastore:ext_key()) -> ok.
aux_delete(ModelConfig, Field, [Key]) ->
    direct_local_call(aux_delete, ModelConfig, [Field, [Key]]).

%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback aux_save/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_save(ModelConfig :: model_behaviour:model_config(), Field :: atom(),
    Args :: [term()]) -> ok.
aux_save(ModelConfig, Field, [Key, Doc]) ->
    direct_local_call(aux_save, ModelConfig, [Field, [Key, Doc]]).

%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback aux_update/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_update(ModelConfig :: model_behaviour:model_config(), Field :: atom(),
    Args :: [term()]) -> ok.
aux_update(ModelConfig, Field, [Key, Level]) ->
    direct_local_call(aux_update, ModelConfig, [Field, [Key, Level]]).

%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback aux_create/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_create(Model :: model_behaviour:model_config(), Field :: atom(),
    Args :: [term()]) -> ok.
aux_create(ModelConfig, Field, [Key, Doc]) ->
    direct_local_call(aux_create, ModelConfig, [Field, [Key, Doc]]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Delegates call to appropriate process.
%% @end
%%--------------------------------------------------------------------
-spec deletage_call(Op :: atom(), MC :: model_behaviour:model_config(),
    Key :: datastore:ext_key(), Args :: list()) -> term().
deletage_call(Op, MC, Key, Args) ->
    execute(MC, Key, false, {Op, Args}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Delegates call to appropriate link handling process.
%% @end
%%--------------------------------------------------------------------
-spec deletage_link_call(Op :: atom(), MC :: model_behaviour:model_config(),
    Key :: datastore:ext_key(), Args :: list()) -> term().
deletage_link_call(Op, MC, Key, Args) ->
    execute(MC, Key, true, {Op, Args}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_call(Op :: atom(), MC :: model_behaviour:model_config(),
    Key :: datastore:ext_key(), Args :: list()) -> term().
direct_call(Op, #model_config{name = ModelName} = MC, Key, Args) ->
    CHKey = get_hashing_key(ModelName, Key),
    Node = consistent_hasing:get_node(CHKey),

    rpc:call(Node, get_slave_driver(false, MC), Op, [MC | Args]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_call(Op :: atom(), MC :: model_behaviour:model_config(),
    Key :: datastore:ext_key(), Args :: list(), CheckAns :: term()) -> term().
direct_call(Op, #model_config{name = ModelName} = MC, Key, Args, CheckAns) ->
    CHKey = get_hashing_key(ModelName, Key),
    Node = consistent_hasing:get_node(CHKey),

    rpc:call(Node, ?MODULE, direct_call_internal, [Op, MC, Key, Args, CheckAns]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation (to be executed appropriate node).
%% @end
%%--------------------------------------------------------------------
-spec direct_call_internal(Op :: atom(), MC :: model_behaviour:model_config(),
    Key :: datastore:ext_key(), Args :: list(), CheckAns :: term()) -> term().
direct_call_internal(Op, #model_config{name = ModelName} = MC, Key, Args, CheckAns) ->
    SlaveDriver = get_slave_driver(false, MC),
    case apply(SlaveDriver, Op, [MC | Args]) of
        CheckAns ->
            % TODO - better consistency info management for models
            case caches_controller:check_cache_consistency_direct(SlaveDriver, ModelName) of
                {ok, _, _} ->
                    CheckAns; % TODO - check time (if consistency wasn't restored a moment ago)
                % TODO - simplify monitoring
                {monitored, ClearedList, _, _} ->
                    case lists:member(Key, ClearedList) of
                        true ->
                            deletage_call(Op, MC, Key, Args);
                        _ ->
                            CheckAns
                    end;
                _ ->
                    deletage_call(Op, MC, Key, Args)
            end;
        RPCAns ->
            RPCAns
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_link_call(Op :: atom(), MC :: model_behaviour:model_config(),
    Key :: datastore:ext_key(), Args :: list()) -> term().
direct_link_call(Op, #model_config{name = ModelName} = MC, Key, Args) ->
    CHKey = get_hashing_key(ModelName, Key),
    Node = consistent_hasing:get_node(CHKey),

    rpc:call(Node, get_slave_driver(true, MC), Op, [MC | Args]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_link_call(Op :: atom(), MC :: model_behaviour:model_config(),
    Key :: datastore:ext_key(), Args :: list(), CheckAns :: term()) -> term().
% TODO - link_utils return appripriate error when link doc not found.
direct_link_call(Op, #model_config{name = ModelName} = MC, Key, Args, CheckAns) ->
    CHKey = get_hashing_key(ModelName, Key),
    Node = consistent_hasing:get_node(CHKey),

    rpc:call(Node, ?MODULE, direct_link_call_internal, [Op, MC, Key, Args, CheckAns]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_link_call_internal(Op :: atom(), MC :: model_behaviour:model_config(),
    Key :: datastore:ext_key(), Args :: list(), CheckAns :: term()) -> term().
direct_link_call_internal(Op, #model_config{name = ModelName} = MC, Key, Args, CheckAns) ->
    SlaveDriver = get_slave_driver(true, MC),
    case apply(SlaveDriver, Op, [MC | Args]) of
        CheckAns ->
            CCCUuid = caches_controller:get_cache_uuid(Key, ModelName),
            case caches_controller:check_cache_consistency_direct(SlaveDriver, CCCUuid, ModelName) of
                {ok, _, _} ->
                    CheckAns; % TODO - check time (if consistency wasn't restored a moment ago)
                _ ->
                    deletage_link_call(Op, MC, Key, Args)
            end;
        RPCAns2 ->
            RPCAns2
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at local node.
%% @end
%%--------------------------------------------------------------------
-spec direct_local_call(Op :: atom(), MC :: model_behaviour:model_config(), Args :: list()) -> term().
direct_local_call(Op, MC, Args) ->
    apply(get_slave_driver(false, MC), Op, [MC | Args]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation in appropriate process.
%% @end
%%--------------------------------------------------------------------
-spec execute(MC :: model_behaviour:model_config(), Key :: datastore:ext_key(),
    Link :: boolean(), {Op :: atom(), Args :: list()}) -> term().
execute(MC, Key, Link, Msg) ->
    execute(MC, Key, Link, Msg, []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation in appropriate process.
%% @end
%%--------------------------------------------------------------------
-spec execute(MC :: model_behaviour:model_config(), Key :: datastore:ext_key(),
    Link :: boolean(), {Op :: atom(), Args :: list()}, InitExtension :: list()) -> term().
% TODO - allow node specification (for fallbacks from direct operations)
execute(#model_config{name = ModelName} = MC, Key, Link, Msg, InitExtension) ->
    TPMod = memory_store_driver,

    Persist = case MC#model_config.store_level of
        ?GLOBALLY_CACHED_LEVEL ->
            couchdb_datastore_driver;
        _ ->
            undefined
    end,

    TMInit = [get_slave_driver(Link, MC), MC, Key, Persist, Link],
    TPKey = {ModelName, Key, Link},

    CHKey = get_hashing_key(ModelName, Key),
    Node = consistent_hasing:get_node(CHKey),
    rpc:call(Node, tp, run_sync, [TPMod, TMInit, TPKey, Msg | InitExtension]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Execute list fun on local memory.
%% @end
%%--------------------------------------------------------------------
-spec execute_list_fun(Fun :: datastore:list_fun(), List :: list(), AccIn :: term()) ->
    {ok, term()} | datastore:generic_error().
execute_list_fun(Fun, List, AccIn) ->
    try
        AccOut =
            lists:foldr(fun(Doc, OAcc) ->
                case Fun(Doc, OAcc) of
                    {next, NAcc} ->
                        NAcc;
                    {abort, NAcc} ->
                        throw({abort, NAcc})
                end
            end, AccIn, List),
        {abort, NAcc2} = Fun('$end_of_table', AccOut),
        {ok, NAcc2}
    catch
        {abort, AccOut0} ->
            {ok, AccOut0};
        _:Reason ->
            {error, Reason}
    end.

get_slave_driver(true, #model_config{link_store_level = ?GLOBAL_ONLY_LEVEL}) ->
    ?GLOBAL_SLAVE_DRIVER;
get_slave_driver(true, #model_config{link_store_level = ?GLOBALLY_CACHED_LEVEL}) ->
    ?GLOBAL_SLAVE_DRIVER;
get_slave_driver(true, _) ->
    ?LOCAL_SLAVE_DRIVER;
get_slave_driver(_, #model_config{store_level = ?GLOBAL_ONLY_LEVEL}) ->
    ?GLOBAL_SLAVE_DRIVER;
get_slave_driver(_, #model_config{store_level = ?GLOBALLY_CACHED_LEVEL}) ->
    ?GLOBAL_SLAVE_DRIVER;
get_slave_driver(_, _) ->
    ?LOCAL_SLAVE_DRIVER.