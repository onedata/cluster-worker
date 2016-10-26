%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Mnesia database driver.
%%% @end
%%%-------------------------------------------------------------------
-module(mnesia_cache_driver).
-author("Rafal Slota").
-behaviour(store_driver_behaviour).

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
-export([add_links/3, set_links/3, create_link/3, delete_links/3, delete_links/4, fetch_link/3, foreach_link/4]).
-export([run_transation/1, run_transation/2, run_transation/3]).

-export([save_link_doc/2, get_link_doc/2, delete_link_doc/2, exists_link_doc/3]).

%% auxiliary ordered_store behaviour
-export([create_auxiliary_caches/3, aux_delete/3, aux_save/3, aux_update/3,
    aux_create/3, aux_first/2, list_ordered/4, aux_next/3]).

%% Batch size for list operation
-define(LIST_BATCH_SIZE, 100).


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
init_bucket(_BucketName, Models, NodeToSync) ->
    Node = node(),
    lists:foreach( %% model
        fun(#model_config{name = ModelName, fields = Fields}) ->
            Table = table_name(ModelName),
            LinkTable = links_table_name(ModelName),
            TransactionTable = transaction_table_name(ModelName),
            case NodeToSync == Node of
                true -> %% No mnesia nodes -> create new table
                    create_table(Table, ModelName, [key | Fields], [Node]),
                    create_table(LinkTable, links, [key | record_info(fields, links)], [Node]),
                    create_table(TransactionTable, ModelName, [key | Fields], [Node]);
                _ -> %% there is at least one mnesia node -> join cluster
                    Tables = [table_name(MName) || MName <- datastore_config:models()] ++
                        [links_table_name(MName) || MName <- datastore_config:models()] ++
                        [transaction_table_name(MName) || MName <- datastore_config:models()],
                    ok = rpc:call(NodeToSync, mnesia, wait_for_tables, [Tables, ?MNESIA_WAIT_TIMEOUT]),
                    expand_table(Table, Node, NodeToSync),
                    expand_table(LinkTable, Node, NodeToSync),
                    expand_table(TransactionTable, Node, NodeToSync)
            end
        end, Models),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback save/2.
%% @end
%%--------------------------------------------------------------------
-spec save(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(#model_config{name = ModelName} = ModelConfig, #document{key = Key, value = Value} = _Document) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun(TrxType) ->
        log(brief, "~p -> ~p:save(~p)", [TrxType, ModelName, Key]),
        log(verbose, "~p -> ~p:save(~p, ~p)", [TrxType, ModelName, Key, Value]),
        ok = mnesia:write(table_name(ModelConfig), inject_key(Key, Value), write),
        {ok, Key}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Saves document that describes links, not using transactions (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec save_link_doc(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save_link_doc(ModelConfig, #document{key = Key, value = Value} = _Document) ->
    ok = mnesia:write(links_table_name(ModelConfig), inject_key(Key, Value), write),
    {ok, Key}.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(model_behaviour:model_config(), datastore:ext_key(),
    Diff :: datastore:document_diff()) -> {ok, datastore:ext_key()} | datastore:update_error().
update(#model_config{name = ModelName} = ModelConfig, Key, Diff) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun(TrxType) ->
        log(brief, "~p -> ~p:update(~p)", [TrxType, ModelName, Key]),
        case mnesia:read(table_name(ModelConfig), Key, write) of
            [] ->
                {error, {not_found, ModelName}};
            [Value] when is_map(Diff) ->
                NewValue = maps:merge(datastore_utils:shallow_to_map(strip_key(Value)), Diff),
                log(verbose, "~p -> ~p:update(~p, ~p)", [TrxType, ModelName, Key, NewValue]),
                ok = mnesia:write(table_name(ModelConfig),
                    inject_key(Key, datastore_utils:shallow_to_record(NewValue)), write),
                {ok, Key};
            [Value] when is_function(Diff) ->
                case Diff(strip_key(Value)) of
                    {ok, NewValue} ->
                        log(verbose, "~p -> ~p:update(~p, ~p)", [TrxType, ModelName, Key, NewValue]),
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
create(#model_config{name = ModelName} = ModelConfig, #document{key = Key, value = Value}) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun(TrxType) ->
        log(brief, "~p -> ~p:create(~p)", [TrxType, ModelName, Key]),
        log(verbose, "~p -> ~p:create(~p, ~p)", [TrxType, ModelName, Key, Value]),
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
create_or_update(#model_config{name = ModelName} = ModelConfig, #document{key = Key, value = Value}, Diff) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun(TrxType) ->
        log(brief, "~p -> ~p:create_or_update(~p)", [TrxType, ModelName, Key]),
        case mnesia:read(table_name(ModelConfig), Key, write) of
            [] ->
                log(verbose, "~p -> ~p:create_or_update(~p, ~p)", [TrxType, ModelName, Key, Value]),
                ok = mnesia:write(table_name(ModelConfig), inject_key(Key, Value), write),
                {ok, Key};
            [OldValue] when is_map(Diff) ->
                NewValue = maps:merge(datastore_utils:shallow_to_map(strip_key(OldValue)), Diff),
                log(verbose, "~p -> ~p:create_or_update(~p, ~p)", [TrxType, ModelName, Key, NewValue]),
                ok = mnesia:write(table_name(ModelConfig),
                    inject_key(Key, datastore_utils:shallow_to_record(NewValue)), write),
                {ok, Key};
            [OldValue] when is_function(Diff) ->
                case Diff(strip_key(OldValue)) of
                    {ok, NewValue} ->
                        log(verbose, "~p -> ~p:create_or_update(~p, ~p)", [TrxType, ModelName, Key, NewValue]),
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
            log(normal, "transaction -> ~p:get(~p)", [ModelName, Key]),
            mnesia:read(table_name(ModelConfig), Key);
        _ ->
            log(normal, "dirty -> ~p:get(~p)", [ModelName, Key]),
            mnesia:dirty_read(table_name(ModelConfig), Key)
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
    TmpAns = case mnesia:is_transaction() of
        true ->
            log(normal, "transaction -> ~p:get_link_doc(~p)", [ModelName, Key]),
            mnesia:read(links_table_name(ModelConfig), Key);
        _ ->
            log(normal, "dirty -> ~p:get_link_doc(~p)", [ModelName, Key]),
            mnesia:dirty_read(links_table_name(ModelConfig), Key)
    end,
    case TmpAns of
        [] -> {error, {not_found, ModelName}};
        [Value] -> {ok, #document{key = Key, value = strip_key(Value)}}
    end.

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
            log(normal, "transaction -> ~p:exists_link_doc(~p)", [ModelName, Key]),
            mnesia:read(LNT, Key);
        _ ->
            log(normal, "dirty -> ~p:exists_link_doc(~p)", [ModelName, Key]),
            mnesia:dirty_read(LNT, Key)
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
add_links(#model_config{name = ModelName} = ModelConfig, Key, Links) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun(TrxType) ->
        log(brief, "~p -> ~p:add_links(~p)", [TrxType, ModelName, Key]),
        log(verbose, "~p -> ~p:add_links(~p, ~p)", [TrxType, ModelName, Key, Links]),
        links_utils:save_links_maps(?MODULE, ModelConfig, Key, Links, add)
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback set_links/3.
%% @end
%%--------------------------------------------------------------------
-spec set_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()]) ->
    ok | datastore:generic_error().
set_links(#model_config{name = ModelName} = ModelConfig, Key, Links) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun(TrxType) ->
        log(brief, "~p -> ~p:add_links(~p)", [TrxType, ModelName, Key]),
        log(verbose, "~p -> ~p:add_links(~p, ~p)", [TrxType, ModelName, Key, Links]),
        links_utils:save_links_maps(?MODULE, ModelConfig, Key, Links, set)
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_link/3.
%% @end
%%--------------------------------------------------------------------
-spec create_link(model_behaviour:model_config(), datastore:ext_key(), datastore:normalized_link_spec()) ->
    ok | datastore:create_error().
create_link(#model_config{name = ModelName} = ModelConfig, Key, Link) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun(TrxType) ->
        log(brief, "~p -> ~p:create_link(~p)", [TrxType, ModelName, Key]),
        log(verbose, "~p -> ~p:create_link(~p, ~p)", [TrxType, ModelName, Key, Link]),
        links_utils:create_link_in_map(?MODULE, ModelConfig, Key, Link)
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete_links/3.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:link_name()] | all) ->
    ok | datastore:generic_error().
delete_links(#model_config{} = ModelConfig, Key, LinkNames) ->
    delete_links(#model_config{} = ModelConfig, Key, LinkNames, ?PRED_ALWAYS).

%%--------------------------------------------------------------------
%% @doc
%% Simmilar to {@link store_driver_behaviour} callback delete_links/3 witch delete predicate.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:link_name()] | all,
    datastore:delete_predicate()) -> ok | datastore:generic_error().
delete_links(#model_config{name = ModelName} = ModelConfig, Key, all, Pred) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun(TrxType) ->
        log(brief, "~p -> ~p:delete_links(~p)", [TrxType, ModelName, Key]),
        log(verbose, "~p -> ~p:delete_links(~p, ~p)", [TrxType, ModelName, Key, all]),
        case Pred() of
            true ->
                ok = links_utils:delete_links(?MODULE, ModelConfig, Key);
            false ->
                ok
        end
    end);
delete_links(#model_config{name = ModelName} = ModelConfig, Key, Links, Pred) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun(TrxType) ->
        log(brief, "~p -> ~p:delete_links(~p)", [TrxType, ModelName, Key]),
        log(verbose, "~p -> ~p:delete_links(~p, ~p)", [TrxType, ModelName, Key, Links]),
        case Pred() of
            true ->
                ok = links_utils:delete_links_from_maps(?MODULE, ModelConfig, Key, Links);
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
    links_utils:fetch_link(?MODULE, ModelConfig, LinkName, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback foreach_link/4.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(model_behaviour:model_config(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error().
foreach_link(#model_config{} = ModelConfig, Key, Fun, AccIn) ->
    links_utils:foreach_link(?MODULE, ModelConfig, Key, Fun, AccIn).


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
delete(#model_config{name = ModelName} = ModelConfig, Key, Pred) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun(TrxType) ->
        log(normal, "~p -> ~p:delete(~p)", [TrxType, ModelName, Key]),
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
-spec delete_link_doc(model_behaviour:model_config(), datastore:document()) ->
    ok | datastore:generic_error().
delete_link_doc(#model_config{} = ModelConfig, #document{key = Key} = _Document) ->
    mnesia:delete(links_table_name(ModelConfig), Key, write).


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
            log(normal, "transaction -> ~p:exists(~p)", [ModelName, Key]),
            mnesia:read(table_name(ModelConfig), Key);
        _ ->
            log(normal, "dirty -> ~p:exists(~p)", [ModelName, Key]),
            mnesia:dirty_read(table_name(ModelConfig), Key)
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
run_transation(#model_config{name = ModelName}, ResourceID, Fun) ->
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
create_auxiliary_caches(#model_config{}=ModelConfig, Fields, NodeToSync) ->
    Node = node(),
    lists:foreach(fun(Field) ->
        TabName = aux_table_name(ModelConfig, Field),
        case NodeToSync == Node of
            true ->
                create_table(TabName, auxiliary_cache_entry, [], [Node], ordered_set);
            _ ->
                ok = rpc:call(NodeToSync, mnesia, wait_for_tables, [[TabName], ?MNESIA_WAIT_TIMEOUT]),
                expand_table(TabName, Node, NodeToSync)
        end
    end, Fields),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback aux_first/2.
%% @end
%%--------------------------------------------------------------------
-spec aux_first(model_behaviour:model_config(), Field :: atom()) -> datastore:aux_cache_handle().
aux_first(#model_config{}=ModelConfig, Field) ->
    AuxTableName = aux_table_name(ModelConfig, Field),
    mnesia:dirty_first(AuxTableName).


%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback next/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_next(model_behaviour:model_config(), Field :: atom(),
    Handle :: datastore:aux_cache_handle()) -> datastore:aux_cache_handle().
aux_next(_, _, '$end_of_table') -> '$end_of_table';
aux_next(#model_config{}=ModelConfig, Field, Handle) ->
    AuxTableName = aux_table_name(ModelConfig, Field),
    mnesia:dirty_next(AuxTableName, Handle).


%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback delete/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_delete(Model :: model_behaviour:model_config(), Field :: atom(),
    Key :: datastore:ext_key()) -> ok.
aux_delete(ModelConfig, Field, [Key]) ->
    AuxTableName = aux_table_name(ModelConfig, Field),
    Action = fun(TrxType) ->
        log(normal, "~p -> aux_delete(~p, ~p, ~p)", [TrxType, ModelConfig, Field, Key]),
        Selected = mnesia:dirty_select(AuxTableName, ets:fun2ms(
            fun(#auxiliary_cache_entry{key={_, K}} = R) when K == Key -> R end)),
        lists:foreach(fun(#auxiliary_cache_entry{key=K}) ->
            mnesia:dirty_delete(AuxTableName, K)
        end, Selected)
    end,
    ok = mnesia_run(async_dirty, Action),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback aux_save/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_save(Model :: model_behaviour:model_config(), Field :: atom(),
    Args :: [term()]) -> ok.
aux_save(ModelConfig, Field, [Key, Doc]) ->
    AuxTableName = aux_table_name(ModelConfig, Field),
    CurrentFieldValue = datastore_utils:get_field_value(Doc, Field),
    Action = case is_aux_field_value_updated(AuxTableName, Key, CurrentFieldValue) of
        {true, AuxKey} ->
            fun(TrxType) ->
                log(normal, "~p -> aux_save(~p, ~p, ~p)", [TrxType, ModelConfig, Field, Key]),
                ok = mnesia:dirty_delete(AuxTableName, AuxKey),
                ok = mnesia:dirty_write(AuxTableName, #auxiliary_cache_entry{key={CurrentFieldValue, Key}})
            end;
        true ->
            fun(TrxType) ->
                log(normal, "~p -> aux_save(~p, ~p, ~p)", [TrxType, ModelConfig, Field, Key]),
                mnesia:dirty_write(AuxTableName, #auxiliary_cache_entry{key={CurrentFieldValue, Key}})
            end;
        _ -> ok
    end,
    mnesia_run(async_dirty, Action),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback aux_update/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_update(Model :: model_behaviour:model_config(), Field :: atom(),
    Args :: [term()]) -> ok.
aux_update(ModelConfig = #model_config{name=ModelName}, Field, [Key, Level]) ->
    Doc = datastore:get(Level, ModelName, Key),
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
        log(normal, "~p -> aux_create(~p, ~p, ~p)", [TrxType, ModelConfig, Field, Key]),
        mnesia:dirty_write(AuxTableName, #auxiliary_cache_entry{key={CurrentFieldValue, Key}})
    end,
    mnesia_run(async_dirty, Action),
    ok.

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
table_name(TabName) when is_atom(TabName) ->
    binary_to_atom(<<"dc_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).


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
links_table_name(TabName) when is_atom(TabName) ->
    binary_to_atom(<<"dc_links_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets Mnesia transaction table name for given model.
%% @end
%%--------------------------------------------------------------------
-spec transaction_table_name(atom()) -> atom().
transaction_table_name(TabName) when is_atom(TabName) ->
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
    create_table(TabName, RecordName, Attributes, RamCopiesNodes, Type, true).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Create Mnesia table of type Type.
%% RamCopiesNodes is list of nodes where the table is supposed to have
%% RAM copies
%% @end
%%--------------------------------------------------------------------
-spec create_table(TabName :: atom(), RecordName :: atom(),
    Attributes :: [atom()], RamCopiesNodes :: [atom()], Type :: atom(), Majority :: boolean()) -> atom().
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
    case rpc:call(NodeToSync, mnesia, change_config, [extra_db_nodes, [Node]]) of
        {ok, [Node]} ->
            case rpc:call(NodeToSync, mnesia, add_table_copy, [TabName, Node, ram_copies]) of
                {atomic, ok} ->
                    ?info("Expanding mnesia cluster (table ~p) from ~p to ~p", [TabName, NodeToSync, node()]);
                {aborted, Reason} ->
                    ?error("Cannot replicate mnesia table ~p to node ~p due to: ~p", [TabName, node(), Reason])
            end,
            ok;
        {error, Reason} ->
            ?error("Cannot expand mnesia cluster (table ~p) on node ~p due to ~p", [TabName, node(), Reason]),
            throw(Reason)
    end.


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
%% If transactions are enabled in #model_config{} returns given TransactionType.
%% If transactions are disabled in #model_config{} returns corresponding dirty mode.
%% @end
%%--------------------------------------------------------------------
-spec maybe_transaction(model_behaviour:model_config(), atom()) -> atom().
maybe_transaction(#model_config{transactional_global_cache = false}, TransactionType) ->
    case TransactionType of
        sync_transaction -> sync_dirty
    end;
maybe_transaction(#model_config{transactional_global_cache = true}, TransactionType) ->
    TransactionType.

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
        log(normal, "~p -> ~p:list()", [TrxType, ModelName]),
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
    AuxCacheLevel = maps:get(Field, AuxCaches),
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
-spec is_aux_field_value_updated(atom(), datastore:key(), term()) -> boolean() | {true, OldAuxKey :: {term(), datastore:key()}}.
is_aux_field_value_updated(AuxTableName, Key, CurrentFieldValue) ->
    case aux_get(AuxTableName, Key) of
        [] -> true;
        [{CurrentFieldValue, Key}] -> false;
        [#auxiliary_cache_entry{key=AuxKey}] -> {true, AuxKey}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns entry from auxiliary table of Model connected with Field,
%% matching the Key.
%% @end
%%--------------------------------------------------------------------
-spec aux_get(atom(),Key :: datastore:key()) -> [{term(), datastore:key()}].
aux_get(AuxTableName, Key) ->
    mnesia:dirty_select(AuxTableName, ets:fun2ms(
        fun(#auxiliary_cache_entry{key={_, K}} = R) when K == Key -> R end)).


