%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Driver for operations on Mnesia (for internal use by high level driver).
%%% @end
%%%-------------------------------------------------------------------
-module(mnesia_cache_driver).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% store_driver_behaviour callbacks
-export([init_driver/1, init_bucket/3, healthcheck/1]).
%% TODO Add non_transactional updates (each update creates tmp ets!)
-export([save/2, update/3, create/2, create_or_update/3, exists/2, get/2, list/4, delete/3, is_model_empty/1]).
-export([add_links/3, set_links/3, create_link/3, delete_links/4, fetch_link/3, foreach_link/4]).
-export([run_transation/1, run_transation/2, run_transation/3]).

%% TODO zmienic na wywolania do memory_store_driver ktory pisze bezposrednio tutaj
-export([save_link_doc/2, get_link_doc/2, get_link_doc/3, delete_link_doc/2, exists_link_doc/3]).

%% auxiliary ordered_store behaviour
-export([create_auxiliary_caches/3, aux_delete/3, aux_save/3, aux_update/3,
    aux_create/3, aux_first/2, aux_next/3]).

%% Batch size for list operation
-define(LIST_BATCH_SIZE, 100).

% TODO - which type?
-define(SAVE_ACTIVITY_TYPE, async_dirty).

%%%===================================================================
%%% store_driver_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_driver/1.
%% @end
%%--------------------------------------------------------------------
-spec init_driver(worker_host:plugin_state()) -> {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init_driver(State) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/2.
%% @end
%%--------------------------------------------------------------------
-spec init_bucket(Bucket :: datastore:bucket(), Models :: [model_behaviour:model_config()], NodeToSync :: node()) -> ok.
% TODO - inicjalizowac tak zeby przechowywac tez numery rewizji (albo ostatni numer revizji - pytanie co wystarczy)
init_bucket(_BucketName, Models, NodeToSync) ->
    Node = node(),

    case NodeToSync == Node of
        true ->
            ok;
        _ ->
            % TODO - umozliwic replikacje na wszystkie nody innych wybranych tabel (zadko pisane, czesto czytane np user)
            LockMN = lock,
            LockTable = table_name(LockMN),
            LockLinkTable = links_table_name(LockMN),
            LockTransactionTable = transaction_table_name(LockMN),
            LockTables = [LockTable, LockLinkTable, LockTransactionTable],
            wait_for_tables(NodeToSync, LockTables),

            case rpc:call(NodeToSync, mnesia, change_config, [extra_db_nodes, [Node]]) of
                {ok, [Node]} ->
                    ok;
                {error, Reason} ->
                    ?error("Cannot expand mnesia cluster on node ~p due to ~p", [node(), Reason]),
                    throw(Reason)
            end
    end,

    lists:foreach( %% model
        fun(#model_config{name = ModelName, fields = Fields}) when ModelName =:= lock ->
            Table = table_name(ModelName),
            LinkTable = links_table_name(ModelName),
            TransactionTable = transaction_table_name(ModelName),
            case NodeToSync == Node of
                true -> %% No mnesia nodes -> create new table
                    create_table(Table, ModelName, [key | Fields], [Node]),
                    create_table(LinkTable, links, [key | record_info(fields, links)], [Node]),
                    create_table(TransactionTable, ModelName, [key | Fields], [Node]);
                _ -> %% there is at least one mnesia node -> join cluster
                    expand_table(Table, Node, NodeToSync),
                    expand_table(LinkTable, Node, NodeToSync),
                    expand_table(TransactionTable, Node, NodeToSync)
            end;
            (#model_config{name = ModelName, fields = Fields}) ->
                Table = table_name(ModelName),
                LinkTable = links_table_name(ModelName),
                TransactionTable = transaction_table_name(ModelName),
                create_table(Table, ModelName, [key | Fields], [Node]),
                create_table(LinkTable, links, [key | record_info(fields, links)], [Node]),
                create_table(TransactionTable, ModelName, [key | Fields], [Node])
        end, Models),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback save/2.
%% @end
%%--------------------------------------------------------------------
-spec save(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(ModelConfig, #document{key = Key, value = Value} = _Document) ->
    mnesia_run(?SAVE_ACTIVITY_TYPE, fun(_TrxType) ->
        ok = mnesia:write(table_name(ModelConfig), inject_key(Key, Value), write)
    end),
    {ok, Key}.

%%--------------------------------------------------------------------
%% @doc
%% Saves document that describes links, not using transactions (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec save_link_doc(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save_link_doc(ModelConfig, #document{key = Key, value = Value}) ->
    mnesia_run(?SAVE_ACTIVITY_TYPE, fun(_TrxType) ->
        ok = mnesia:write(links_table_name(ModelConfig), inject_key(Key, Value), write)
    end),
    {ok, Key}.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(model_behaviour:model_config(), datastore:ext_key(),
    Diff :: datastore:document_diff()) -> {ok, datastore:ext_key()} | datastore:update_error().
update(#model_config{name = ModelName} = ModelConfig, Key, Diff) ->
    mnesia_run(?SAVE_ACTIVITY_TYPE, fun(_TrxType) ->
        case mnesia:read(table_name(ModelConfig), Key, write) of
            [] ->
                {error, {not_found, ModelName}};
            [Value] ->
                case memory_store_driver_docs:update(strip_key(Value), Diff) of
                    {ok, NewValue} ->
                        ok = mnesia:write(table_name(ModelConfig), inject_key(Key, NewValue), write),
                        {ok, Key};
                    {error, Reason} ->
                        {error, Reason}
                end
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create/2.
%% @end
%%--------------------------------------------------------------------
-spec create(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(ModelConfig, #document{key = Key, value = Value}) ->
    mnesia_run(?SAVE_ACTIVITY_TYPE, fun(_TrxType) ->
        case mnesia:read(table_name(ModelConfig), Key) of
            [] ->
                ok = mnesia:write(table_name(ModelConfig), inject_key(Key, Value), write),
                {ok, Key};
            [_Record] ->
                {error, already_exists}
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_or_update/2.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(model_behaviour:model_config(), datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create_or_update(ModelConfig, #document{key = Key, value = Value}, Diff) ->
    mnesia_run(?SAVE_ACTIVITY_TYPE, fun(_TrxType) ->
        case mnesia:read(table_name(ModelConfig), Key, write) of
            [] ->
                ok = mnesia:write(table_name(ModelConfig), inject_key(Key, Value), write),
                {ok, Key};
            [OldValue] ->
                case memory_store_driver_docs:update(strip_key(OldValue), Diff) of
                    {ok, NewValue} ->
                        ok = mnesia:write(table_name(ModelConfig), inject_key(Key, NewValue), write),
                        {ok, Key};
                    {error, Reason} ->
                        {error, Reason}
                end
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get(#model_config{name = ModelName} = ModelConfig, Key) ->
    TmpAns = case mnesia:is_transaction() of
        true ->
            log(brief, "transaction -> ~p:get(~p)", [ModelName, Key]),
            mnesia:read(table_name(ModelConfig), Key);
        _ ->
            log(brief, "dirty -> ~p:get(~p)", [ModelName, Key]),
            mnesia:activity(ets, fun() ->
                mnesia:read(table_name(ModelConfig), Key)
            end)
    end,
    case TmpAns of
        [] -> {error, {not_found, ModelName}};
        [Value] -> {ok, #document{key = Key, value = strip_key(Value)}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets document that describes links (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec get_link_doc(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get_link_doc(#model_config{name = ModelName} = ModelConfig, Key) ->
    % TODO - sprawdzic is_transaction - jak dziala w innych typach activity (trzeba by zrobic cos ala is_activity?)
    TmpAns = case mnesia:is_transaction() of
        true ->
            log(brief, "transaction -> ~p:get_link_doc(~p)", [ModelName, Key]),
            mnesia:read(links_table_name(ModelConfig), Key);
        _ ->
            log(brief, "dirty -> ~p:get_link_doc(~p)", [ModelName, Key]),
            mnesia:activity(ets, fun() ->
                mnesia:read(links_table_name(ModelConfig), Key)
            end)
    end,
    case TmpAns of
        [] -> {error, {not_found, ModelName}};
        [Value] -> {ok, #document{key = Key, value = strip_key(Value)}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets document that describes links (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec get_link_doc(model_behaviour:model_config(), binary(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get_link_doc(ModelConfig, _BucketOverride, Key) ->
    get_link_doc(ModelConfig, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks if document that describes links from scope exists.
%% @end
%%--------------------------------------------------------------------
-spec exists_link_doc(model_behaviour:model_config(), datastore:ext_key(), links_utils:scope()) ->
    {ok, boolean()} | datastore:generic_error().
exists_link_doc(#model_config{name = ModelName} = ModelConfig, DocKey, Scope) ->
    Key = links_utils:links_doc_key(DocKey, Scope),
    LNT = links_table_name(ModelConfig),
    TmpAns = case mnesia:is_transaction() of
        true ->
            log(brief, "transaction -> ~p:exists_link_doc(~p)", [ModelName, Key]),
            mnesia:read(LNT, Key);
        _ ->
            log(brief, "dirty -> ~p:exists_link_doc(~p)", [ModelName, Key]),
            mnesia:activity(ets, fun() ->
                mnesia:read(LNT, Key)
            end)
    end,
    case TmpAns of
        [] -> {ok, false};
        [_Record] -> {ok, true}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback list/4.
%% @end
%%--------------------------------------------------------------------
-spec list(model_behaviour:model_config(),
    Fun :: datastore:list_fun(), AccIn :: term(), Opts :: store_driver_behaviour:list_options()) ->
    {ok, Handle :: term()} | datastore:generic_error() | no_return().
list(#model_config{} = ModelConfig, Fun, AccIn, Opts) ->
    case proplists:get_value(mode, Opts, undefined) of
        dirty -> list_dirty(ModelConfig, Fun, AccIn);
        {ordered, Field} ->
            list_ordered(ModelConfig, Fun, AccIn, Field);
        _ -> list(ModelConfig, Fun, AccIn)
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
    mnesia_run(?SAVE_ACTIVITY_TYPE, fun(_TrxType) ->
        links_utils:save_links_maps(memory_store_driver_links, ModelConfig, Key, Links, add)
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback set_links/3.
%% @end
%%--------------------------------------------------------------------
-spec set_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()]) ->
    ok | datastore:generic_error().
set_links(ModelConfig, Key, Links) ->
    mnesia_run(?SAVE_ACTIVITY_TYPE, fun(_TrxType) ->
        links_utils:save_links_maps(memory_store_driver_links, ModelConfig, Key, Links, set)
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_link/3.
%% @end
%%--------------------------------------------------------------------
-spec create_link(model_behaviour:model_config(), datastore:ext_key(), datastore:normalized_link_spec()) ->
    ok | datastore:create_error().
create_link(ModelConfig, Key, Link) ->
    mnesia_run(?SAVE_ACTIVITY_TYPE, fun(_TrxType) ->
        links_utils:create_link_in_map(memory_store_driver_links, ModelConfig, Key, Link)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Simmilar to {@link store_driver_behaviour} callback delete_links/3 witch delete predicate.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:link_name()] | all,
    datastore:delete_predicate()) -> ok | datastore:generic_error().
delete_links(ModelConfig, Key, all, Pred) ->
    mnesia_run(?SAVE_ACTIVITY_TYPE, fun(_TrxType) ->
        case Pred() of
            true ->
                ok = links_utils:delete_links(memory_store_driver_links, ModelConfig, Key);
            false ->
                ok
        end
    end);
delete_links(ModelConfig, Key, Links, Pred) ->
    mnesia_run(?SAVE_ACTIVITY_TYPE, fun(_TrxType) ->
        case Pred() of
            true ->
                ok = links_utils:delete_links_from_maps(memory_store_driver_links, ModelConfig, Key, Links);
            false ->
                ok
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback fetch_link/3.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(model_behaviour:model_config(), datastore:ext_key(), datastore:link_name()) ->
    {ok, datastore:link_target()} | datastore:link_error().
fetch_link(#model_config{} = ModelConfig, Key, LinkName) ->
    Driver = get_mcd_driver(),
    links_utils:fetch_link(Driver, ModelConfig, LinkName, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback foreach_link/4.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(model_behaviour:model_config(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error().
foreach_link(#model_config{} = ModelConfig, Key, Fun, AccIn) ->
    Driver = get_mcd_driver(),
    links_utils:foreach_link(Driver, ModelConfig, Key, Fun, AccIn).


%%--------------------------------------------------------------------
%% @doc
%% Internal helper - accumulator for list/3.
%% @end
%%--------------------------------------------------------------------
-spec list_next([term()] | '$end_of_table', term(), datastore:list_fun(), term()) ->
    {ok, Acc :: term()} | datastore:generic_error().
list_next([Obj | R], Handle, Fun, AccIn) ->
    Doc = #document{key = get_key(Obj), value = strip_key(Obj)},
    case Fun(Doc, AccIn) of
        {next, NewAcc} ->
            list_next(R, Handle, Fun, NewAcc);
        {abort, NewAcc} ->
            {ok, NewAcc}
    end;
list_next('$end_of_table' = EoT, Handle, Fun, AccIn) ->
    case Fun(EoT, AccIn) of
        {next, NewAcc} ->
            list_next(EoT, Handle, Fun, NewAcc);
        {abort, NewAcc} ->
            {ok, NewAcc}
    end;
list_next([], Handle, Fun, AccIn) ->
    case mnesia:select(Handle) of
        {Objects, NewHandle} ->
            list_next(Objects, NewHandle, Fun, AccIn);
        '$end_of_table' ->
            list_next('$end_of_table', undefined, Fun, AccIn)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete/2.
%% @end
%%--------------------------------------------------------------------
-spec delete(model_behaviour:model_config(), datastore:ext_key(), datastore:delete_predicate()) ->
    ok | datastore:generic_error().
% TODO - delete Pred - it is checked in proc
delete(ModelConfig, Key, Pred) ->
    mnesia_run(?SAVE_ACTIVITY_TYPE, fun(_TrxType) ->
        case Pred() of
            true ->
                ok = mnesia:delete(table_name(ModelConfig), Key, write);
            false ->
                ok
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Deletes document that describes links, not using transactions (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec delete_link_doc(model_behaviour:model_config(), datastore:ext_key()) ->
    ok | datastore:generic_error().
delete_link_doc(ModelConfig, #document{key = Key}) ->
    delete_link_doc(ModelConfig, Key);
delete_link_doc(#model_config{} = ModelConfig, Key) ->
    mnesia_run(?SAVE_ACTIVITY_TYPE, fun(_TrxType) ->
        mnesia:delete(links_table_name(ModelConfig), Key, write)
    end).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, boolean()} | datastore:generic_error().
exists(#model_config{name = ModelName} = ModelConfig, Key) ->
    TmpAns = case mnesia:is_transaction() of
        true ->
            log(brief, "transaction -> ~p:exists(~p)", [ModelName, Key]),
            mnesia:read(table_name(ModelConfig), Key);
        _ ->
            log(brief, "dirty -> ~p:exists(~p)", [ModelName, Key]),
            mnesia:activity(ets, fun() ->
                mnesia:read(table_name(ModelConfig), Key)
            end)
    end,
    case TmpAns of
        [] -> {ok, false};
        [_Record] -> {ok, true}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback healthcheck/1.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck(WorkerState :: term()) -> ok | {error, Reason :: term()}.
healthcheck(State) ->
    maps:fold(
        fun
            (_, #model_config{name = ModelName}, ok) ->
                case mnesia:table_info(table_name(ModelName), where_to_write) of
                    Nodes when is_list(Nodes) ->
                        case lists:member(node(), Nodes) of
                            true -> ok;
                            false ->
                                {error, {no_active_mnesia_table, table_name(ModelName)}}
                        end;
                    {error, Error} -> {error, Error};
                    Error -> {error, Error}
                end;
            (_, _, Acc) -> Acc
        end, ok, State).


%%--------------------------------------------------------------------
%% @doc
%% Runs given function within locked ResourceId. This function makes sure that 2 funs with same ResourceId won't
%% run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec run_transation(model_behaviour:model_config(), ResourceId :: binary(), fun(() -> Result)) -> Result
    when Result :: term().
run_transation(#model_config{name = _ModelName}, ResourceID, Fun) ->
    % TODO - configure per model
    ModelName = lock,
    mnesia_run(sync_transaction,
        fun(TrxType) ->
            log(normal, "~p -> ~p:run_transation(~p)", [TrxType, ModelName, ResourceID]),
            Nodes = lists:usort(mnesia:table_info(table_name(ModelName), where_to_write)),
            case mnesia:lock({global, ResourceID, Nodes}, write) of
                ok ->
                    Fun();
                Nodes0 ->
                    case lists:usort(Nodes0) of
                        Nodes ->
                            Fun();
                        LessNodes ->
                            {error, {lock_error, Nodes -- LessNodes}}
                    end
            end
        end).

%%--------------------------------------------------------------------
%% @doc
%% Runs given function within locked ResourceId. This function makes sure that 2 funs with same ResourceId won't
%% run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec run_transation(ResourceId :: binary(), fun(() -> Result)) -> Result
    when Result :: term().
run_transation(ResourceID, Fun) ->
    mnesia_run(sync_transaction,
        fun(TrxType) ->
            log(normal, "~p -> run_transation(~p)", [TrxType, ResourceID]),
            Nodes = lists:usort(mnesia:table_info(table_name(lock), where_to_write)),
            case mnesia:lock({global, ResourceID, Nodes}, write) of
                ok ->
                    Fun();
                Nodes0 ->
                    case lists:usort(Nodes0) of
                        Nodes ->
                            Fun();
                        LessNodes ->
                            {error, {lock_error, Nodes -- LessNodes}}
                    end
            end
        end).

%%--------------------------------------------------------------------
%% @doc
%% Runs given function within transaction.
%% @end
%%--------------------------------------------------------------------
-spec run_transation(fun(() -> Result)) -> Result
    when Result :: term().
run_transation(Fun) ->
    NewFun = fun(TrxType) ->
        log(normal, "~p ->run_transation", [TrxType]),
        Fun()
    end,
    mnesia_run(sync_transaction, NewFun).


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
create_auxiliary_caches(#model_config{name = MN} = ModelConfig, Fields, NodeToSync) ->
    Node = node(),
    lists:foreach(fun(Field) ->
        TabName = aux_table_name(ModelConfig, Field),
        case {NodeToSync == Node, MN} of
            {false, lock} ->
                % TODO active waiting
                wait_for_tables(NodeToSync, [TabName]),
                expand_table(TabName, Node, NodeToSync);
            _ ->
                create_table(TabName, auxiliary_cache_entry, [], [Node], ordered_set)
        end
    end, Fields),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback aux_first/2.
%% @end
%%--------------------------------------------------------------------
-spec aux_first(model_behaviour:model_config(), Field :: atom()) ->
    datastore:aux_cache_handle().
aux_first(#model_config{}=ModelConfig, Field) ->
    AuxTableName = aux_table_name(ModelConfig, Field),
    Action = fun(TrxType) ->
        log(brief, "~p -> aux_first(~p, ~p)", [TrxType, ModelConfig, Field]),
        mnesia:first(AuxTableName)
    end,
    mnesia_run(aux_cache_context(ModelConfig, Field), Action).


%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback next/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_next(ModelConfig :: model_behaviour:model_config(), Field :: atom(),
    Handle :: datastore:aux_cache_handle()) -> datastore:aux_cache_handle().
aux_next(_, _, '$end_of_table') -> '$end_of_table';
aux_next(#model_config{}=ModelConfig, Field, Handle) ->
    AuxTableName = aux_table_name(ModelConfig, Field),
    Action = fun(TrxType) ->
        log(brief, "~p -> aux_next(~p, ~p, ~p)", [TrxType, ModelConfig, Field, Handle]),
        mnesia:next(AuxTableName, Handle)
    end,
    mnesia_run(aux_cache_context(ModelConfig, Field), Action).


%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback delete/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_delete(ModelConfig :: model_behaviour:model_config(), Field :: atom(),
    Key :: datastore:ext_key()) -> ok.
aux_delete(ModelConfig, Field, [Key]) ->
    AuxTableName = aux_table_name(ModelConfig, Field),
    Action = fun(TrxType) ->
        log(brief, "~p -> aux_delete(~p, ~p, ~p)", [TrxType, ModelConfig, Field, Key]),
        Selected = mnesia:select(AuxTableName, ets:fun2ms(
            fun(#auxiliary_cache_entry{key={_, K}} = R) when K == Key -> R end)),
        lists:foreach(fun(#auxiliary_cache_entry{key=K}) ->
            mnesia:delete({AuxTableName, K})
        end, Selected)
    end,
    ok = mnesia_run(aux_cache_context(ModelConfig, Field), Action).

%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback aux_save/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_save(ModelConfig :: model_behaviour:model_config(), Field :: atom(),
    Args :: [term()]) -> ok.
aux_save(ModelConfig, Field, [Key, Doc]) ->
    AuxTableName = aux_table_name(ModelConfig, Field),
    CurrentFieldValue = datastore_utils:get_field_value(Doc, Field),
    Action = case is_aux_field_value_updated(ModelConfig, Field, Key, CurrentFieldValue) of
        {true, AuxKey} ->
            fun(TrxType) ->
                log(brief, "~p -> aux_save(~p, ~p, ~p)", [TrxType, ModelConfig, Field, Key]),
                ok = mnesia:delete({AuxTableName, AuxKey}),
                ok = mnesia:write(AuxTableName, #auxiliary_cache_entry{key={CurrentFieldValue, Key}}, write)
            end;
        true ->
            fun(TrxType) ->
                log(brief, "~p -> aux_save(~p, ~p, ~p)", [TrxType, ModelConfig, Field, Key]),
                mnesia:write(AuxTableName, #auxiliary_cache_entry{key={CurrentFieldValue, Key}}, write)
            end;
        _ -> ok
    end,
    ok = mnesia_run(aux_cache_context(ModelConfig, Field), Action).

%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback aux_update/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_update(ModelConfig :: model_behaviour:model_config(), Field :: atom(),
    Args :: [term()]) -> ok.
aux_update(ModelConfig = #model_config{name=ModelName}, Field, [Key, Level]) ->
    {ok, Doc} = datastore:get(Level, ModelName, Key),
    aux_save(ModelConfig, Field, [Key, Doc]).

%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback aux_create/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_create(Model :: model_behaviour:model_config(), Field :: atom(),
    Args :: [term()]) -> ok.
aux_create(ModelConfig, Field, [Key, Doc]) ->
    AuxTableName = aux_table_name(ModelConfig, Field),
    CurrentFieldValue = datastore_utils:get_field_value(Doc, Field),
    Action = fun(TrxType) ->
        log(brief, "~p -> aux_create(~p, ~p, ~p)", [TrxType, ModelConfig, Field, Key]),
        mnesia:write(AuxTableName, #auxiliary_cache_entry{key={CurrentFieldValue, Key}}, write)
    end,
    ok = mnesia_run(aux_cache_context(ModelConfig, Field), Action).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets Mnesia table name for given model.
%% @end
%%--------------------------------------------------------------------
-spec table_name(model_behaviour:model_config() | atom()) -> atom().
table_name(#model_config{name = ModelName}) ->
    table_name(ModelName);
table_name(TN) when is_atom(TN) ->
    TabName = extend_table_name_with_node(TN),
    binary_to_atom(<<"dc_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Extends table name with node name.
%% @end
%%--------------------------------------------------------------------
-spec extend_table_name_with_node(atom()) -> atom().
extend_table_name_with_node(TabName) when TabName =:= lock ->
    TabName;
extend_table_name_with_node(TabName) ->
    list_to_atom(atom_to_list(TabName) ++ atom_to_list(node())).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets Mnesia auxiliary table name for given model and field.
%% @end
%%--------------------------------------------------------------------
-spec aux_table_name(model_behaviour:model_config() | atom(), atom()) -> atom().
aux_table_name(#model_config{name = ModelName}, Field) ->
    aux_table_name(ModelName, Field);
aux_table_name(TabName, Field) when is_atom(TabName) and is_atom(Field) ->
    binary_to_atom(<<(atom_to_binary(table_name(TabName), utf8))/binary, "_",
        (atom_to_binary(Field, utf8))/binary>>, utf8).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets Mnesia links table name for given model.
%% @end
%%--------------------------------------------------------------------
-spec links_table_name(model_behaviour:model_config() | atom()) -> atom().
links_table_name(#model_config{name = ModelName}) ->
    links_table_name(ModelName);
links_table_name(TN) when is_atom(TN) ->
    TabName = extend_table_name_with_node(TN),
    binary_to_atom(<<"dc_links_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets Mnesia transaction table name for given model.
%% @end
%%--------------------------------------------------------------------
-spec transaction_table_name(atom()) -> atom().
transaction_table_name(TN) when is_atom(TN) ->
    TabName = extend_table_name_with_node(TN),
    binary_to_atom(<<"dc_transaction_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Create Mnesia table of default type 'set'.
%% RamCopiesNodes is list of nodes where the table is supposed to have
%% RAM copies
%% @end
%%--------------------------------------------------------------------
-spec create_table(TabName :: atom(), RecordName :: atom(),
    Attributes :: [atom()], RamCopiesNodes :: [atom()]) -> atom().
create_table(TabName, RecordName, Attributes, RamCopiesNodes) ->
    create_table(TabName, RecordName, Attributes, RamCopiesNodes, set).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Create Mnesia table with default majority parameter value set to true.
%% @end
%%--------------------------------------------------------------------
-spec create_table(TabName :: atom(), RecordName :: atom(),
    Attributes :: [atom()], RamCopiesNodes :: [atom()], Type :: atom()) -> atom().
create_table(TabName, RecordName, Attributes, RamCopiesNodes, Type) ->
    create_table(TabName, RecordName, Attributes, RamCopiesNodes, Type, false).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Create Mnesia table of type Type.
%% RamCopiesNodes is list of nodes where the table is supposed to have
%% RAM copies
%% @end
%%--------------------------------------------------------------------
-spec create_table(TabName :: atom(), RecordName :: atom(),
    Attributes :: [atom()], RamCopiesNodes :: [atom()], Type :: atom(), Majority :: boolean()) -> ok.
create_table(TabName, RecordName, Attributes, RamCopiesNodes, Type, Majority) ->
    AttributesArg = case Attributes of
        [] -> [];
        [key] -> [];
        _ -> [{attributes, Attributes}]
    end,

    Ans = case mnesia:create_table(TabName, [
            {record_name, RecordName},
            {ram_copies, RamCopiesNodes},
            {type, Type},
            {majority, Majority} | AttributesArg
        ]) of

        {atomic, ok} ->
            ok;
        {aborted, {already_exists, TabName}} ->
            ok;
        {aborted, Reason} ->
            ?error("Cannot init mnesia cluster (table ~p) on node ~p due to ~p", [TabName, node(), Reason]),
            throw(Reason)
    end,
    ?info("Creating mnesia table: ~p, result: ~p", [TabName, Ans]).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Expand mnesia table
%% @end
%%--------------------------------------------------------------------
-spec expand_table(TabName :: atom(), Node :: atom(), NodeToSync :: atom()) -> atom().
expand_table(TabName, Node, NodeToSync) ->
    case rpc:call(NodeToSync, mnesia, add_table_copy, [TabName, Node, ram_copies]) of
        {atomic, ok} ->
            ?info("Expanding mnesia cluster (table ~p) from ~p to ~p", [TabName, NodeToSync, node()]);
        {aborted, Reason} ->
            ?error("Cannot replicate mnesia table ~p to node ~p due to: ~p", [TabName, node(), Reason])
    end,
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Inserts given key as second element of given tuple.
%% @end
%%--------------------------------------------------------------------
-spec inject_key(Key :: datastore:ext_key(), Tuple :: tuple()) -> NewTuple :: tuple().
inject_key(Key, Tuple) when is_tuple(Tuple) ->
    [RecordName | Fields] = tuple_to_list(Tuple),
    list_to_tuple([RecordName | [Key | Fields]]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Strips second element of given tuple (reverses inject_key/2).
%% @end
%%--------------------------------------------------------------------
-spec strip_key(Tuple :: tuple()) -> NewTuple :: tuple().
strip_key(Tuple) when is_tuple(Tuple) ->
    [RecordName, _Key | Fields] = tuple_to_list(Tuple),
    list_to_tuple([RecordName | Fields]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns key of a tuple.
%% @end
%%--------------------------------------------------------------------
-spec get_key(Tuple :: tuple()) -> Key :: term().
get_key(Tuple) when is_tuple(Tuple) ->
    [_RecordName, Key | _Fields] = tuple_to_list(Tuple),
    Key.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convenience function for executing given Mnesia's transaction-like function and normalizing Result.
%% Available methods: sync_dirty, async_dirty, sync_transaction, transaction.
%% @end
%%--------------------------------------------------------------------
-spec mnesia_run(Method :: atom(), Fun :: fun((atom()) -> term())) -> term().
% TODO - refactor - checking transaction probably not needed; maybe delete arg from FUN?
% TODO ta funkcja moze odpowiadac za zla wydajnosc - moze wystarczy activity ets
mnesia_run(Method, Fun) when Method =:= sync_dirty; Method =:= async_dirty ->
    case mnesia:is_transaction() of
        true ->
            Fun(Method);
        _ ->
            try mnesia:Method(fun() -> Fun(Method) end) of
                Result ->
                    Result
            catch
                _:Reason ->
                    ?error_stacktrace("mnesia_run error ~p", [Reason]),
                    {error, Reason}
            end
    end;
mnesia_run(Method, Fun) when Method =:= sync_transaction; Method =:= transaction ->
    case mnesia:is_transaction() of
        true ->
            Fun(Method);
        _ ->
            case mnesia:Method(fun() -> Fun(Method) end) of
                {atomic, Result} ->
                    Result;
                {aborted, Reason} ->
                    {error, Reason}
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Logs mnesia cache driver operation if logging type equals 'normal' or match
%% the settings.
%% @end
%%--------------------------------------------------------------------
-spec log(Type :: brief | verbose | normal, Format :: string(), Args :: list()) -> ok.
log(normal, Format, Args) ->
    do_log(Format, Args);
log(Type, Format, Args) ->
    case application:get_env(?CLUSTER_WORKER_APP_NAME, mnesia_cache_driver_log_type) of
        {ok, Type} -> do_log(Format, Args);
        _ -> ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Logs mnesia cache driver operation on given level.
%% @end
%%--------------------------------------------------------------------
-spec do_log(Format :: string(), Args :: list()) -> ok.
do_log(Format, Args) ->
    LogLevel = application:get_env(?CLUSTER_WORKER_APP_NAME, mnesia_cache_driver_log_level, 0),
    ?do_log(LogLevel, "[~p] " ++ Format, [?MODULE | Args], false).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Helper function for list/4
%% @end
%%--------------------------------------------------------------------
-spec list(model_behaviour:model_config(), Fun :: datastore:list_fun(),
    AccIn :: term()) -> {ok, Acc :: term()} | datastore:generic_error() | no_return().
list(#model_config{name=ModelName} = ModelConfig, Fun, AccIn) ->
    SelectAll = [{'_', [], ['$_']}],
    ToExec = fun(TrxType) ->
        log(brief, "~p -> ~p:list()", [TrxType, ModelName]),
        case mnesia:select(table_name(ModelConfig), SelectAll, ?LIST_BATCH_SIZE, none) of
            {Obj, Handle} ->
                list_next(Obj, Handle, Fun, AccIn);
            '$end_of_table' ->
                list_next('$end_of_table', undefined, Fun, AccIn)
        end
    end,
    case mnesia:is_transaction() of
        true ->
            ToExec(transaction);
        _ ->
            mnesia_run(async_dirty, ToExec)
end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Dirty alternative of list/3
%% @end
%%--------------------------------------------------------------------
-spec list_dirty(model_behaviour:model_config(), Fun :: datastore:list_fun(),
    AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:generic_error() | no_return().
list_dirty(#model_config{} = ModelConfig, Fun, AccIn) ->
    Table = table_name(ModelConfig),
    First = mnesia:dirty_first(Table),
    list_dirty_next(Table, First, Fun, AccIn).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function acts similar to list_dirty/4 but records are traversed in order
%% by field Field, basing on auxiliary cache connected with that field
%% @end
%%--------------------------------------------------------------------
-spec list_ordered(model_behaviour:model_config(), Fun :: datastore:list_fun(),
    AccIn :: term(), Field :: atom()) ->
    {ok, Acc :: term()} | datastore:generic_error() | no_return().
list_ordered(#model_config{auxiliary_caches = AuxCaches} = ModelConfig, Fun, AccIn, Field) ->
    AuxCacheLevel = datastore_utils:get_aux_cache_level(AuxCaches, Field),
    AuxDriver = datastore:level_to_driver(AuxCacheLevel),
    First = AuxDriver:aux_first(ModelConfig, Field),
    IteratorFun = fun(Handle) -> AuxDriver:aux_next(ModelConfig, Field,  Handle) end,
    TableName = table_name(ModelConfig),
    list_ordered_next(TableName, First, Fun , IteratorFun, AccIn).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Accumulator helper function for dirty_list
%% @end
%%--------------------------------------------------------------------
-spec list_dirty_next(atom(), atom(), Fun :: datastore:list_fun(), AccIn :: term())
 -> {ok, Acc :: term()} | datastore:generic_error().
list_dirty_next(_Table, '$end_of_table' = EoT, Fun, AccIn) ->
    {abort, NewAcc} = Fun(EoT, AccIn),
    {ok, NewAcc};
list_dirty_next(Table, CurrentKey, Fun, AccIn) ->
    [Obj] = mnesia:dirty_read(Table, CurrentKey),
    Doc = #document{key = get_key(Obj), value = strip_key(Obj)},
    case Fun(Doc, AccIn) of
        {next, NewAcc} ->
            Next = mnesia:dirty_next(Table, CurrentKey),
            list_dirty_next(Table, Next, Fun, NewAcc);
        {abort, NewAcc} ->
            {ok, NewAcc}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Accumulator helper function for list_ordered
%% @end
%%--------------------------------------------------------------------
-spec list_ordered_next(atom(), term(), Fun :: datastore:list_fun(),
    IteratorFun :: datastore:aux_iterator_fun(), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:generic_error().
list_ordered_next(_Table, '$end_of_table' = EoT, Fun, _IteratorFun, AccIn) ->
    {abort, NewAcc} = Fun(EoT, AccIn),
    {ok, NewAcc};
list_ordered_next(Table, CurrentKey, Fun, IteratorFun, AccIn) ->
    [Obj] = mnesia:dirty_read(Table, datastore_utils:aux_key_to_key(CurrentKey)),
    Doc = #document{key = get_key(Obj), value = strip_key(Obj)},
    case Fun(Doc, AccIn) of
        {next, NewAcc} ->
            Next = IteratorFun(CurrentKey),
            list_ordered_next(Table, Next, Fun, IteratorFun, NewAcc);
        {abort, NewAcc} ->
            {ok, NewAcc}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Determines whether value of field Field has changed. Old value is checked
%% in auxiliary cache table.
%% If it exists and is different than current value
%% tuple {true, {OldFieldValue, Key}} is returned.
%% If it doesn't exist true is returned.
%% If it exists and it's value hasn't changed, false is returned.
%% @end
%%%--------------------------------------------------------------------
-spec is_aux_field_value_updated(
    ModelConfig :: model_behaviour:model_config(),
    Field :: atom(), datastore:ext_key(), term()) ->
    boolean() | {true, OldAuxKey :: {term(), datastore:ext_key()}}.
is_aux_field_value_updated(ModelConfig, Field, Key, CurrentFieldValue) ->
    case aux_get(ModelConfig, Field, Key) of
        [] -> true;
        [#auxiliary_cache_entry{key={CurrentFieldValue, Key}}] -> false;
        [#auxiliary_cache_entry{key=AuxKey}] -> {true, AuxKey}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns entry from auxiliary table of Model connected with Field,
%% matching the Key.
%% @end
%%--------------------------------------------------------------------
-spec aux_get(ModelConfig :: model_behaviour:model_config(), Field :: atom(),
    Key :: datastore:ext_key()) -> [#auxiliary_cache_entry{}].
aux_get(ModelConfig, Field, Key) ->
    AuxTableName = aux_table_name(ModelConfig, Field),
    Action = fun(TrxType) ->
        log(brief, "~p -> aux_get(~p, ~p, ~p)", [TrxType, ModelConfig, Field, Key]),
        mnesia:select(AuxTableName, ets:fun2ms(
            fun(#auxiliary_cache_entry{key={_, K}} = R) when K == Key -> R end))
    end,
    mnesia_run(aux_cache_context(ModelConfig, Field), Action).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns access context defined for auxiliary cache associated with
%% given field.
%% @end
%%--------------------------------------------------------------------
-spec aux_cache_context(model_behaviour:model_config(), atom()) ->
    datastore:aux_cache_access_context().
aux_cache_context(#model_config{auxiliary_caches = AuxCaches}, Field) ->
    #aux_cache_config{context = Context} = maps:get(Field, AuxCaches),
    Context.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Waits for mnesia tables.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_tables(NodeToSync :: node(), Tables :: list()) ->
    ok | {timeout, BadTabList :: list()} | {error, term()}.
wait_for_tables(NodeToSync, Tables) ->
    ok = lists:foldl(fun
        (_, ok) ->
            ok;
        (_, _) ->
            rpc:call(NodeToSync, mnesia, wait_for_tables, [Tables, ?MNESIA_WAIT_TIMEOUT])
    end, start, lists:seq(1, ?MNESIA_WAIT_REPEATS)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns memory driver to be used.
%% @end
%%--------------------------------------------------------------------
-spec get_mcd_driver() -> atom().
get_mcd_driver() ->
    case get(mcd_driver) of
        undefined -> ?MODULE;
        D -> D
    end.