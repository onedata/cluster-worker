%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc ETS based cache implementation.
%%% @todo: https://jira.plgrid.pl/jira/browse/VFS-2588
%%% @end
%%%-------------------------------------------------------------------
-module(ets_cache_driver).
-author("Rafal Slota").
-behaviour(store_driver_behaviour).
-behaviour(auxiliary_cache_behaviour).


-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% store_driver_behaviour callbacks
-export([init_driver/1, init_bucket/3, healthcheck/1]).
-export([save/2, update/3, create/2, create_or_update/3, exists/2, get/2, list/4, delete/3, is_model_empty/1]).
-export([add_links/3, set_links/3, create_link/3, delete_links/3, fetch_link/3, foreach_link/4]).

-export([save_link_doc/2, get_link_doc/2, delete_link_doc/2, exists_link_doc/3]).

%% auxiliary_cache_behaviour
-export([create_auxiliary_caches/3, aux_delete/3, aux_save/3, aux_update/3,
    aux_create/3, aux_first/2, aux_next/3]).

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
-spec init_bucket(Bucket :: datastore:bucket(), Models :: [model_behaviour:model_config()],
    NodeToSync :: node()) -> ok.
init_bucket(_Bucket, Models, _NodeToSync) ->
    lists:foreach(
        fun(#model_config{} = ModelConfig) ->
            TableName = table_name(ModelConfig),
            case table_exists(TableName) of
                false ->
                    Ans = create_table(TableName),
                    create_table(links_table_name(ModelConfig)),
                    ?info("Creating ets table: ~p, result: ~p", [table_name(ModelConfig), Ans]);
                _ -> ok
            end
        end, Models).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback save/2.
%% @end
%%--------------------------------------------------------------------
-spec save(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(#model_config{} = ModelConfig, #document{key = Key, value = Value}) ->
    true = ets:insert(table_name(ModelConfig), {Key, Value}),
    {ok, Key}.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback update/3.
%% @end
%%--------------------------------------------------------------------
-spec update(model_behaviour:model_config(), datastore:ext_key(),
    Diff :: datastore:document_diff()) -> {ok, datastore:ext_key()} | datastore:update_error().
update(#model_config{name = ModelName} = ModelConfig, Key, Diff) ->
    case ets:lookup(table_name(ModelConfig), Key) of
        [] ->
            {error, {not_found, ModelName}};
        [{_, Value}] ->
            case memory_store_driver_docs:update(Value, Diff) of
                {ok, NewValue} ->
                    true = ets:insert(table_name(ModelConfig), {Key, NewValue}),
                    {ok, Key};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create/2.
%% @end
%%--------------------------------------------------------------------
-spec create(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(#model_config{} = ModelConfig, #document{key = Key, value = Value}) ->
    case ets:insert_new(table_name(ModelConfig), {Key, Value}) of
        false -> {error, already_exists};
        true -> {ok, Key}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_or_update/2.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(model_behaviour:model_config(), datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create_or_update(#model_config{} = ModelConfig, #document{key = Key, value = Value}, Diff) ->
    case ets:lookup(table_name(ModelConfig), Key) of
        [] ->
            case ets:insert_new(table_name(ModelConfig), {Key, Value}) of
                false -> update(ModelConfig, Key, Diff);
                true -> {ok, Key}
            end;
        [{_, OldValue}] ->
            case memory_store_driver_docs:update(OldValue, Diff) of
                {ok, NewValue} ->
                    true = ets:insert(table_name(ModelConfig), {Key, NewValue}),
                    {ok, Key};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get(#model_config{name = ModelName} = ModelConfig, Key) ->
    case ets:lookup(table_name(ModelConfig), Key) of
        [{_, Value}] ->
            {ok, #document{key = Key, value = Value}};
        [] ->
            {error, {not_found, ModelName}}
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
        {ordered, Field} ->
            list_ordered(ModelConfig, Fun, AccIn, Field);
        _ ->
            list(ModelConfig, Fun, AccIn)
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
%% {@link store_driver_behaviour} callback delete/2.
%% @end
%%--------------------------------------------------------------------
-spec delete(model_behaviour:model_config(), datastore:ext_key(), datastore:delete_predicate()) ->
    ok | datastore:generic_error().
delete(#model_config{} = ModelConfig, Key, Pred) ->
    case Pred() of
        true ->
            true = ets:delete(table_name(ModelConfig), Key),
            ok;
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, boolean()} | datastore:generic_error().
exists(#model_config{} = ModelConfig, Key) ->
    {ok, ets:member(table_name(ModelConfig), Key)}.

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
                case ets:info(table_name(ModelName)) of
                    undefined ->
                        {error, {no_ets, table_name(ModelName)}};
                    _ -> ok
                end;
            (_, _, Acc) -> Acc
        end, ok, State).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback add_links/3.
%% @end
%%--------------------------------------------------------------------
-spec add_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()]) ->
    no_return().
add_links(ModelConfig, Key, Links) ->
    links_utils:save_links_maps(?MODULE, ModelConfig, Key, Links, add).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback set_links/3.
%% @end
%%--------------------------------------------------------------------
-spec set_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()]) ->
    no_return().
set_links(ModelConfig, Key, Links) ->
    links_utils:save_links_maps(?MODULE, ModelConfig, Key, Links, set).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_link/3.
%% @end
%%--------------------------------------------------------------------
-spec create_link(model_behaviour:model_config(), datastore:ext_key(), datastore:normalized_link_spec()) ->
    no_return().
create_link(ModelConfig, Key, Link) ->
    links_utils:create_link_in_map(?MODULE, ModelConfig, Key, Link).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete_links/3.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:link_name()] | all) ->
    no_return().
delete_links(ModelConfig, Key, all) ->
    links_utils:delete_links(?MODULE, ModelConfig, Key);
delete_links(ModelConfig, Key, Links) ->
    links_utils:delete_links_from_maps(?MODULE, ModelConfig, Key, Links).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback fetch_link/3.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(model_behaviour:model_config(), datastore:ext_key(), datastore:link_name()) ->
    no_return().
fetch_link(ModelConfig, Key, LinkName) ->
    links_utils:fetch_link(?MODULE, ModelConfig, LinkName, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback foreach_link/4.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(model_behaviour:model_config(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    no_return().
foreach_link(ModelConfig, Key, Fun, AccIn) ->
    links_utils:foreach_link(?MODULE, ModelConfig, Key, Fun, AccIn).


%%--------------------------------------------------------------------
%% @doc
%% Saves document that describes links, not using transactions (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec save_link_doc(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save_link_doc(ModelConfig, #document{key = Key, value = Value} = _Document) ->
    true = ets:insert(links_table_name(ModelConfig), {Key, Value}),
    {ok, Key}.

%%--------------------------------------------------------------------
%% @doc
%% Gets document that describes links (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec get_link_doc(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get_link_doc(#model_config{name = ModelName} = ModelConfig, Key) ->
    case ets:lookup(links_table_name(ModelConfig), Key) of
        [{_, Value}] ->
            {ok, #document{key = Key, value = Value}};
        [] ->
            {error, {not_found, ModelName}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks if document that describes links from scope exists.
%% @end
%%--------------------------------------------------------------------
-spec exists_link_doc(model_behaviour:model_config(), datastore:ext_key(), links_utils:scope()) ->
    {ok, boolean()} | datastore:generic_error().
exists_link_doc(ModelConfig, DocKey, Scope) ->
    Key = links_utils:links_doc_key(DocKey, Scope),
    case ets:lookup(links_table_name(ModelConfig), Key) of
        [{_, _Value}] ->
            {ok, true};
        [] ->
            {ok, false}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes document that describes links, not using transactions (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec delete_link_doc(model_behaviour:model_config(), datastore:document()) ->
    ok | datastore:generic_error().
delete_link_doc(#model_config{} = ModelConfig, #document{key = Key} = _Document) ->
    true = ets:delete(links_table_name(ModelConfig), Key),
    ok.

%%%===================================================================
%%% auxiliary_cache_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback
%% create_auxiliary_caches/2.
%% @end
%%--------------------------------------------------------------------
-spec create_auxiliary_caches(
    model_behaviour:model_config(), Fields :: [atom()], _NodeToSync :: node()) ->
    ok | datastore:generic_error() | no_return().
create_auxiliary_caches(#model_config{}=ModelConfig, Fields, _NodeToSync) ->
    lists:foreach(fun(Field) ->
        create_table(aux_table_name(ModelConfig, Field), ordered_set)
    end, Fields).


%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback first/2.
%% @end
%%--------------------------------------------------------------------
-spec aux_first(model_behaviour:model_config(), Field :: atom()) -> datastore:aux_cache_handle().
aux_first(#model_config{}=ModelConfig, Field) ->
    ets:first(aux_table_name(ModelConfig, Field)).


%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback next/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_next(model_behaviour:model_config(), Field :: atom(),
    Handle :: datastore:aux_cache_handle()) -> datastore:aux_cache_handle().
aux_next(_, _, '$end_of_table') -> '$end_of_table';
aux_next(#model_config{}=ModelConfig, Field, Handle) ->
    ets:next(aux_table_name(ModelConfig, Field), Handle).


%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback delete/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_delete(Model :: model_behaviour:model_config(), Field :: atom(),
    Args :: [term()]) -> ok.
aux_delete(ModelConfig, Field, [Key]) ->
    AuxTableName = aux_table_name(ModelConfig, Field),
    ets:select_delete(AuxTableName,
        ets:fun2ms(fun({{_, K}, _}) when K == Key -> true end)),
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
    case is_aux_field_value_updated(AuxTableName, Key,  CurrentFieldValue) of
        {true, AuxKey} ->
            true = ets:delete(AuxTableName, AuxKey),
            ets:insert(AuxTableName, {{CurrentFieldValue, Key}, undefined});
        true -> % there is no entry in AuxTableName matching Key
            ets:insert(AuxTableName, {{CurrentFieldValue, Key}, undefined});
        _ -> ok
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link auxiliary_cache_behaviour} callback aux_update/3.
%% @end
%%--------------------------------------------------------------------
-spec aux_update(Model :: model_behaviour:model_config(), Field :: atom(),
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
    ets:insert_new(AuxTableName, {{CurrentFieldValue, Key}, undefined}),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Internal helper for list/4.
%% @end
%%--------------------------------------------------------------------
-spec list(model_behaviour:model_config(), Fun :: datastore:list_fun(), AccIn :: term()) ->
    {ok, Handle :: term()} | datastore:generic_error() | no_return().
list(#model_config{} = ModelConfig, Fun, AccIn) ->
    SelectAll = [{'_', [], ['$_']}],
    case ets:select(table_name(ModelConfig), SelectAll, ?LIST_BATCH_SIZE) of
        {Obj, Handle} ->
            list_next(Obj, Handle, Fun, AccIn);
        '$end_of_table' ->
            list_next('$end_of_table', undefined, Fun, AccIn)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Internat helper - accumulator for list/3.
%% @end
%%--------------------------------------------------------------------
-spec list_next([term()] | '$end_of_table', term(), datastore:list_fun(), term()) ->
    {ok, Acc :: term()} | datastore:generic_error().
list_next([{Key, Obj} | R], Handle, Fun, AccIn) ->
    Doc = #document{key = Key, value = Obj},
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
    case ets:select(Handle) of
        {Objects, NewHandle} ->
            list_next(Objects, NewHandle, Fun, AccIn);
        '$end_of_table' ->
            list_next('$end_of_table', undefined, Fun, AccIn)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function traverses records in order by field Field,
%% basing on auxiliary cache connected with that field
%% @end
%%--------------------------------------------------------------------
-spec list_ordered(model_behaviour:model_config(), Fun :: datastore:list_fun(),
    AccIn :: term(), Field :: atom()) ->
    {ok, Acc :: term()} | datastore:generic_error() | no_return().
list_ordered(#model_config{auxiliary_caches = AuxCaches} = ModelConfig, Fun, AccIn, Field) ->
    AuxCacheLevel = datastore_utils:get_aux_cache_level(AuxCaches, Field),
    AuxDriver = datastore:level_to_driver(AuxCacheLevel),
    First = AuxDriver:aux_first(ModelConfig, Field),
    IteratorFun = fun(Handle) -> AuxDriver:aux_next(ModelConfig, Field, Handle) end,
    TableName = table_name(ModelConfig),
    list_ordered_next(TableName, First, Fun, IteratorFun, AccIn).


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
    [{Key, Obj}] = ets:lookup(Table, datastore_utils:aux_key_to_key(CurrentKey)),
    Doc = #document{key = Key, value = Obj},
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
%% Gets ETS table name for given model.
%% @end
%%--------------------------------------------------------------------
-spec table_name(model_behaviour:model_config() | atom()) -> atom().
table_name(#model_config{name = ModelName}) ->
    table_name(ModelName);
table_name(TabName) when is_atom(TabName) ->
    binary_to_atom(<<"lc_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).

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
    binary_to_atom(<<"lc_links_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets ETS auxiliary table name for given model and field.
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
%% Checks if ETS with given table exists
%% @end
%%--------------------------------------------------------------------
-spec table_exists(TableName :: atom()) -> boolean().
table_exists(TableName) ->
    case ets:info(TableName) of
        undefined -> false;
        _ -> true
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new ETS table with given name. If table exists, does nothing.
%% @end
%%--------------------------------------------------------------------
-spec create_table(TableName :: atom()) -> ok.
create_table(TableName) ->
    create_table(TableName, set).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new ETS table with given name.
%% @end
%%--------------------------------------------------------------------
-spec create_table(TableName :: atom(), Type :: ets:type()) -> ok.
create_table(TableName, Type) ->
    Ans = (catch ets:new(TableName, [named_table, public, Type])),
    ?info("Creating ets table: ~p, result: ~p", [TableName, Ans]).


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
-spec is_aux_field_value_updated(datastore:model_config(), datastore:key(),
    term()) -> boolean() | {true, OldAuxKey :: {term(), datastore:key()}}.
is_aux_field_value_updated(AuxTableName, Key, CurrentFieldValue) ->
    case aux_get(AuxTableName, Key) of
        [] -> true;
        [{CurrentFieldValue, Key}] -> false;
        [{AuxKey, _Value}] -> {true, AuxKey}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns entry from auxiliary table of Model connected with Field,
%% matching the Key.
%% @end
%%--------------------------------------------------------------------
-spec aux_get(atom(), Key :: datastore:key()) -> [{term(), datastore:key()}].
aux_get(AuxTableName, Key) ->
    ets:select(AuxTableName,
        ets:fun2ms(fun(T = {{_, K}, _}) when K == Key -> T end)).

