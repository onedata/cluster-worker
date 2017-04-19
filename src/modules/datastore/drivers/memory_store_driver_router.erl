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

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-type opt_ctx() :: datastore_context:ctx().

%% API
-export([call/3, get_default_context/2]).

%% for apply
-export([list/4, get/2, exists/2, fetch_link/3, foreach_link/4,
    exists_link_doc/3, get_link_doc/4]).
%% for rpc
-export([direct_call_internal/5, direct_link_call_internal/5, foreach_link_internal/4]).

-define(EXTENDED_ROUTING, [list, get, exists, fetch_link, foreach_link,
    exists_link_doc, get_link_doc]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Routes call to appropriate node/tp process.
%% @end
%%--------------------------------------------------------------------
-spec call(Function :: atom(), opt_ctx(), Args :: [term()]) ->
    term().
% TODO - refactor force_link_save
call(force_link_save, OptCtx, [ToSave, MainDocKey]) ->
    deletage_call(force_link_save, OptCtx, MainDocKey, [ToSave]);
call(force_link_save, OptCtx, [BucketOverride, ToSave, MainDocKey]) ->
    deletage_call(force_link_save, OptCtx, MainDocKey, [BucketOverride, ToSave]);
% delegate document changes to tp
call(Method, OptCtx, [#document{key = Key}] = Args) ->
    deletage_call(Method, OptCtx, Key, Args);
call(clear, OptCtx, [Key]) ->
    execute(OptCtx, Key, false, {clear, []}, [100]);
call(clear_links, OptCtx, [Key]) ->
    execute(OptCtx, Key, true, {clear, []}, [100]);
call(Method, OptCtx, Args) ->
    case lists:member(Method, ?EXTENDED_ROUTING) of
        true ->
            apply(?MODULE, Method, [OptCtx | Args]);
        _ ->
            [Key | _] = Args,
            IsLinkOp = model:is_link_op(Method),
            case IsLinkOp of
                false ->
                    deletage_call(Method, OptCtx, Key, Args);
                _ ->
                    deletage_link_call(Method, OptCtx, Key, Args)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets default config user by memory store driver.
%% @end
%%--------------------------------------------------------------------
-spec get_default_context(Operation :: atom(), model_behaviour:model_config()) ->
    opt_ctx().
get_default_context(_Op, Config) ->
    Config.

%%%===================================================================
%%% Extended routing functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(opt_ctx(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get(OptCtx, Key) ->
    case get_level(OptCtx, false) of
        ?GLOBAL_ONLY_LEVEL ->
            direct_call(get, OptCtx, Key, [Key]);
        ?LOCAL_ONLY_LEVEL ->
            direct_call(get, OptCtx, Key, [Key]);
        _ ->
            MN = get_model_name(OptCtx),
            direct_call(get, OptCtx, Key, [Key], {error, {not_found, MN}})
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(opt_ctx(), datastore:ext_key()) ->
    {ok, boolean()} | datastore:generic_error().
exists(OptCtx, Key) ->
    case get_level(OptCtx, false) of
        ?GLOBAL_ONLY_LEVEL ->
            direct_call(exists, OptCtx, Key, [Key]);
        ?LOCAL_ONLY_LEVEL ->
            direct_call(exists, OptCtx, Key, [Key]);
        _ ->
            direct_call(exists, OptCtx, Key, [Key], {ok, false})
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback list/4.
%% @end
%%--------------------------------------------------------------------
-spec list(opt_ctx(),
    Fun :: datastore:list_fun(), AccIn :: term(), Opts :: store_driver_behaviour:list_options()) ->
    {ok, Handle :: term()} | datastore:generic_error() | no_return().
% TODO - implementation base on old implementation from datastore - refactor when local cache is finished
list(OptCtx, Fun, AccIn, Opts) ->
    case get_level(OptCtx, false) of
        ?GLOBAL_ONLY_LEVEL ->
            Nodes = consistent_hasing:get_all_nodes(),

            % TODO - use user function and catch last element not to save all docs in memory
            HelperFun = fun
                (#document{} = Document, Acc) ->
                    {next, [Document | Acc]};
                (_, Acc) ->
                    {abort, Acc}
            end,
            SlaveDriver = get_slave_driver(false, OptCtx),

            ListAns = lists:foldl(fun
                (N, {ok, Acc}) ->
                    rpc:call(N, SlaveDriver, list, [OptCtx, HelperFun, Acc, Opts]);
                (_, Error) ->
                    Error
            end, {ok, []}, Nodes),

            case ListAns of
                {ok, LA} ->
                    execute_list_fun(Fun, LA, AccIn);
                Other ->
                    Other
            end;
        ?LOCAL_ONLY_LEVEL ->
            SlaveDriver = get_slave_driver(false, OptCtx),
            SlaveDriver:list(OptCtx, Fun, AccIn, Opts);
        Level ->
            MN = get_model_name(OptCtx),
            Nodes = case Level of
                ?LOCALLY_CACHED_LEVEL ->
                    [node()];
                _ ->
                    consistent_hasing:get_all_nodes()
            end,

            HelperFun = fun
                (#document{key = Key} = Document, Acc) ->
                    {next, maps:put(Key, Document, Acc)};
                (_, Acc) ->
                    {abort, Acc}
            end,

            % TODO - one consistency check (after migration from counter to times)
            try
                ConsistencyCheck1 = caches_controller:check_cache_consistency(
                    memory_store_driver:main_level(Level), MN),
                MemMapAns = lists:foldl(fun
                    (N, {ok, Acc}) ->
                        rpc:call(N, get_slave_driver(false, OptCtx), list, [OptCtx, HelperFun, Acc, Opts]);
                    (_, Error) ->
                        throw(Error)
                end, {ok, #{}}, Nodes),

                {ok, MemMap} = MemMapAns,
                FirstPhaseAns = case ConsistencyCheck1 of
                    {ok, ClearCounter, _} ->
                        case caches_controller:check_cache_consistency(
                            memory_store_driver:main_level(Level), MN) of
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
                        caches_controller:begin_consistency_restoring(
                            memory_store_driver:main_level(Level), MN, Pid),

                        HelperFun2 = fun
                            (#document{key = Key}, Acc) ->
                                {next, [Key | Acc]};
                            (_, Acc) ->
                                {abort, Acc}
                        end,

                        case couchdb_datastore_driver:list(OptCtx, HelperFun2, [], Opts) of
                            {ok, DiskList} ->
                                DiskOnlyKeys = DiskList -- maps:keys(MemMap),
                                DiskOnlyValues = lists:foldl(fun(K, Acc) ->
                                    case deletage_call(get, OptCtx, K, [K]) of
                                        {ok, D} -> [D | Acc];
                                        {error, {not_found, _}} -> Acc;
                                        Err ->
                                            throw({restore_from_disk_error, Err})
                                    end
                                end, [], DiskOnlyKeys),

                                caches_controller:end_consistency_restoring(
                                    memory_store_driver:main_level(Level), MN, Pid),
                                execute_list_fun(Fun, maps:values(MemMap) ++ DiskOnlyValues, AccIn);
                            Other ->
                                throw({restore_from_disk_error, Other})
                        end
                end
            catch
                throw:{restore_from_disk_error, Err} ->
                    caches_controller:end_consistency_restoring(
                        memory_store_driver:main_level(Level), MN, self(), not_monitored),
                    ?error("Listing error ~p", [{restore_from_disk_error, Err}]),
                    {error, {restore_from_disk, Err}};
                throw:Error ->
                    ?error("Listing error ~p", [Error]),
                    Error
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback fetch_link/3.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(opt_ctx(), datastore:ext_key(), datastore:link_name()) ->
    {ok, datastore:link_target()} | datastore:link_error().
fetch_link(OptCtx, Key, LinkName) ->
    case get_level(OptCtx, true) of
        ?GLOBAL_ONLY_LEVEL ->
            direct_link_call(fetch_link, OptCtx, Key, [Key, LinkName]);
        ?LOCAL_ONLY_LEVEL ->
            direct_link_call(fetch_link, OptCtx, Key, [Key, LinkName]);
        _ ->
            direct_link_call(fetch_link, OptCtx, Key, [Key, LinkName],
                {error, link_not_found})
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback foreach_link/4.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(opt_ctx(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error().
% TODO - implementation base on old implementation from datastore - refactor when local cache is finished
foreach_link(OptCtx, Key, Fun, AccIn) ->
    case get_level(OptCtx, true) of
        ?GLOBAL_ONLY_LEVEL ->
            direct_link_call(foreach_link, OptCtx, Key, [Key, Fun, AccIn]);
        ?LOCAL_ONLY_LEVEL ->
            direct_link_call(foreach_link, OptCtx, Key, [Key, Fun, AccIn]);
        _ ->
            Node = get_hashing_node(OptCtx, Key, true),
            rpc:call(Node, ?MODULE, foreach_link_internal,
                [OptCtx, Key, Fun, AccIn])
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} foreach_link/4 internal implementation
%% (to be executed at node that hosts links process).
%% @end
%%--------------------------------------------------------------------
-spec foreach_link_internal(opt_ctx(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error().
foreach_link_internal(OptCtx, Key, Fun, AccIn) ->
    ModelName = get_model_name(OptCtx),
    CCCUuid = caches_controller:get_cache_uuid(Key, ModelName),
    % TODO - one consistency check (after migration from counter to times)
    SlaveDriver = get_slave_driver(true, OptCtx),
    case caches_controller:check_cache_consistency_direct(SlaveDriver, CCCUuid, ModelName) of
        {ok, ClearingCounter, _} ->
            % TODO - foreach link returns error when doc is not found, and should be
            RPCAns = apply(SlaveDriver, foreach_link, [OptCtx, Key, Fun, AccIn]),
            % TODO - check directly in SLAVE_DRIVER
            case caches_controller:check_cache_consistency_direct(SlaveDriver, CCCUuid) of
                {ok, ClearingCounter, _} ->
                    RPCAns;
                _ ->
                    deletage_link_call(foreach_link, OptCtx, Key, [Key, Fun, AccIn])
            end;
        _ ->
            deletage_link_call(foreach_link, OptCtx, Key, [Key, Fun, AccIn])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if document that describes links from scope exists.
%% @end
%%--------------------------------------------------------------------
-spec exists_link_doc(opt_ctx(), datastore:ext_key(), links_utils:scope()) ->
    {ok, boolean()} | datastore:generic_error().
exists_link_doc(OptCtx, DocKey, Scope) ->
    case get_level(OptCtx, true) of
        ?GLOBAL_ONLY_LEVEL ->
            direct_link_call(exists_link_doc, OptCtx, DocKey, [DocKey, Scope]);
        ?LOCAL_ONLY_LEVEL ->
            direct_link_call(exists_link_doc, OptCtx, DocKey, [DocKey, Scope]);
        _ ->
            direct_link_call(exists_link_doc, OptCtx, DocKey, [DocKey, Scope],
                {ok, false})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets link document. Allows override bucket.
%% @end
%%--------------------------------------------------------------------
-spec get_link_doc(opt_ctx(), binary(), DocKey :: datastore:ext_key(),
    MainDocKey :: datastore:ext_key()) -> {ok, datastore:document()} | datastore:generic_error().
get_link_doc(OptCtx, BucketOverride, DocKey, MainDocKey) ->
    case get_level(OptCtx, true) of
        ?GLOBAL_ONLY_LEVEL ->
            direct_link_call(get_link_doc, OptCtx, MainDocKey, [BucketOverride, DocKey]);
        ?LOCAL_ONLY_LEVEL ->
            direct_link_call(get_link_doc, OptCtx, MainDocKey, [BucketOverride, DocKey]);
        _ ->
            ModelName = get_model_name(OptCtx),
            direct_link_call(get_link_doc, OptCtx, MainDocKey, [BucketOverride, DocKey],
                {error, {not_found, ModelName}})
    end.

%%%===================================================================
%%% Routing functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Delegates call to appropriate process.
%% @end
%%--------------------------------------------------------------------
-spec deletage_call(Op :: atom(), opt_ctx(),
    Key :: datastore:ext_key(), Args :: list()) -> term().
deletage_call(Op, OptCtx, Key, Args) ->
    execute(OptCtx, Key, false, {Op, Args}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Delegates call to appropriate link handling process.
%% @end
%%--------------------------------------------------------------------
-spec deletage_link_call(Op :: atom(), opt_ctx(),
    Key :: datastore:ext_key(), Args :: list()) -> term().
deletage_link_call(Op, OptCtx, Key, Args) ->
    execute(OptCtx, Key, true, {Op, Args}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_call(Op :: atom(), opt_ctx(),
    Key :: datastore:ext_key(), Args :: list()) -> term().
direct_call(Op, OptCtx, Key, Args) ->
    Node = get_hashing_node(OptCtx, Key, false),

    rpc:call(Node, get_slave_driver(false, OptCtx), Op, [OptCtx | Args]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_call(Op :: atom(), opt_ctx(),
    Key :: datastore:ext_key(), Args :: list(), CheckAns :: term()) -> term().
direct_call(Op, OptCtx, Key, Args, CheckAns) ->
    Node = get_hashing_node(OptCtx, Key, false),

    rpc:call(Node, ?MODULE, direct_call_internal, [Op, OptCtx, Key, Args, CheckAns]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation (to be executed appropriate node).
%% @end
%%--------------------------------------------------------------------
-spec direct_call_internal(Op :: atom(), opt_ctx(),
    Key :: datastore:ext_key(), Args :: list(), CheckAns :: term()) -> term().
direct_call_internal(Op, OptCtx, Key, Args, CheckAns) ->
    SlaveDriver = get_slave_driver(false, OptCtx),
    ModelName = get_model_name(OptCtx),
    case apply(SlaveDriver, Op, [OptCtx | Args]) of
        CheckAns ->
            % TODO - better consistency info management for models
            case caches_controller:check_cache_consistency_direct(SlaveDriver, ModelName) of
                {ok, _, _} ->
                    CheckAns; % TODO - check time (if consistency wasn't restored a moment ago)
                % TODO - simplify monitoring
                {monitored, ClearedList, _, _} ->
                    case lists:member(Key, ClearedList) of
                        true ->
                            deletage_call(Op, OptCtx, Key, Args);
                        _ ->
                            CheckAns
                    end;
                _ ->
                    deletage_call(Op, OptCtx, Key, Args)
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
-spec direct_link_call(Op :: atom(), opt_ctx(),
    Key :: datastore:ext_key(), Args :: list()) -> term().
direct_link_call(Op, OptCtx, Key, Args) ->
    Node = get_hashing_node(OptCtx, Key, true),

    rpc:call(Node, get_slave_driver(true, OptCtx), Op, [OptCtx | Args]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_link_call(Op :: atom(), opt_ctx(),
    Key :: datastore:ext_key(), Args :: list(), CheckAns :: term()) -> term().
% TODO - link_utils return appripriate error when link doc not found.
direct_link_call(Op, OptCtx, Key, Args, CheckAns) ->
    Node = get_hashing_node(OptCtx, Key, true),

    rpc:call(Node, ?MODULE, direct_link_call_internal, [Op, OptCtx, Key, Args, CheckAns]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_link_call_internal(Op :: atom(), opt_ctx(),
    Key :: datastore:ext_key(), Args :: list(), CheckAns :: term()) -> term().
direct_link_call_internal(Op, OptCtx, Key, Args, CheckAns) ->
    SlaveDriver = get_slave_driver(true, OptCtx),
    case apply(SlaveDriver, Op, [OptCtx | Args]) of
        CheckAns ->
            ModelName = get_model_name(OptCtx),
            CCCUuid = caches_controller:get_cache_uuid(Key, ModelName),
            case caches_controller:check_cache_consistency_direct(SlaveDriver, CCCUuid, ModelName) of
                {ok, _, _} ->
                    CheckAns; % TODO - check time (if consistency wasn't restored a moment ago)
                _ ->
                    deletage_link_call(Op, OptCtx, Key, Args)
            end;
        RPCAns2 ->
            RPCAns2
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation in appropriate process.
%% @end
%%--------------------------------------------------------------------
-spec execute(opt_ctx(), Key :: datastore:ext_key(),
    Link :: boolean(), {Op :: atom(), Args :: list()}) -> term().
execute(OptCtx, Key, Link, Msg) ->
    execute(OptCtx, Key, Link, Msg, []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation in appropriate process.
%% @end
%%--------------------------------------------------------------------
-spec execute(opt_ctx(), Key :: datastore:ext_key(),
    Link :: boolean(), {Op :: atom(), Args :: list()}, InitExtension :: list()) -> term().
% TODO - allow node specification (for fallbacks from direct operations)
execute(OptCtx, Key, Link, Msg, InitExtension) ->
    TPMod = memory_store_driver,

    Persist = get_persistance_driver(OptCtx),

    SD = get_slave_driver(Link, OptCtx),
    TMInit = [SD, OptCtx, Key, Persist, Link],
    TPKey = {get_model_name(OptCtx), Key, Link, SD},

    Node = get_hashing_node(OptCtx, Key, Link),
    rpc:call(Node, tp, run_sync, [TPMod, TMInit, TPKey, Msg | InitExtension]).

%%%===================================================================
%%% Context functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Gets model_name from config.
%% @end
%%--------------------------------------------------------------------
-spec get_model_name(opt_ctx()) ->
    model_behaviour:model_type().
get_model_name(#model_config{name = Name} = _OptCtx) ->
    Name.

%%--------------------------------------------------------------------
%% @doc
%% Gets model_name from config.
%% @end
%%--------------------------------------------------------------------
-spec get_level(opt_ctx(), LinkOp :: boolean()) ->
    datastore:store_level().
% TODO - delete second arg
get_level(#model_config{store_level = Level} = _OptCtx, false) ->
    Level;
get_level(#model_config{link_store_level = Level} = _OptCtx, _LinkOp) ->
    Level.

%%--------------------------------------------------------------------
%% @doc
%% Gets persistance driver from config.
%% @end
%%--------------------------------------------------------------------
-spec get_persistance_driver(opt_ctx()) ->
    atom().
get_persistance_driver(#model_config{store_level = Level} = _OptCtx) ->
    case Level of
        ?GLOBALLY_CACHED_LEVEL ->
            couchdb_datastore_driver;
        ?LOCALLY_CACHED_LEVEL ->
            couchdb_datastore_driver;
        _ ->
            undefined
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets key for consistent hashing algorithm.
%% @end
%%--------------------------------------------------------------------
-spec get_hashing_node(opt_ctx(), Key :: datastore:ext_key(),
    Link :: boolean()) -> term().
% TODO - delete third arg
get_hashing_node(OptCtx, Key, Link) ->
    case get_level(OptCtx, Link) of
        ?LOCAL_ONLY_LEVEL ->
            node();
        ?LOCALLY_CACHED_LEVEL ->
            node();
        _ ->
            MN = get_model_name(OptCtx),
            consistent_hasing:get_node({MN, Key})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns slave driver for model.
%% @end
%%--------------------------------------------------------------------
-spec get_slave_driver(Link :: boolean(), opt_ctx()) -> atom().
get_slave_driver(Link, OptCtx) ->
    case get_level(OptCtx, Link) of
        ?GLOBAL_ONLY_LEVEL ->
            ?GLOBAL_SLAVE_DRIVER;
        ?GLOBALLY_CACHED_LEVEL ->
            ?GLOBAL_SLAVE_DRIVER;
        _ ->
            ?LOCAL_SLAVE_DRIVER
    end.