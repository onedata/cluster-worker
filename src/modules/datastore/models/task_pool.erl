%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Information about tasks to be done.
%%% @end
%%%-------------------------------------------------------------------
-module(task_pool).
-author("Michał Wrzeszcz").
-behaviour(model_behaviour).

-include("elements/task_manager/task_manager.hrl").
-include("modules/datastore/datastore_internal_model.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, list/0, list/1, list_failed/1, exists/1, delete/1, delete/2, update/2, update/3,
    create/1, create/2, model_init/0, 'after'/5, before/4]).
-export([record_struct/1]).
%% API
-export([count_tasks/3]).

-define(LEVEL_OVERRIDE(Level), [{level, Level}]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {task, term},
        {task_type, atom},
        {owner, term},
        {node, atom}
    ]}.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback update/2 but allows
%% choice of task level.
%% @end
%%--------------------------------------------------------------------
-spec update(Level :: task_manager:level(), datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(?NON_LEVEL, _Key, _Diff) ->
    {ok, non};

update(Level, Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff],
        ?LEVEL_OVERRIDE(task_to_db_level(Level))).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback create/1 but allows
%% choice of task level.
%% @end
%%--------------------------------------------------------------------
-spec create(Level :: task_manager:level(), datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(?NON_LEVEL, _Document) ->
    {ok, non};

create(Level, Document) ->
    model:execute_with_default_context(?MODULE, create, [Document],
        ?LEVEL_OVERRIDE(task_to_db_level(Level))).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    model:execute_with_default_context(?MODULE, list, [?GET_ALL, []]).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list(Level :: task_manager:level()) ->
    {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list(?NON_LEVEL) ->
    {ok, []};

list(Level) ->
    model:execute_with_default_context(?MODULE, list, [?GET_ALL, []],
        ?LEVEL_OVERRIDE(task_to_db_level(Level))).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of tasks that failed.
%% @end
%%--------------------------------------------------------------------
-spec list_failed(Level :: task_manager:level()) ->
    {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list_failed(?NON_LEVEL) ->
    {ok, []};

list_failed(?NODE_LEVEL = Level) ->
    Filter = fun
        ('$end_of_table', Acc) ->
            {abort, Acc};
        (#document{value = V} = Doc, Acc) ->
            N = node(),
            case (V#task_pool.node =/= N) orelse task_manager:check_owner(V#task_pool.owner) of
                false ->
                    {next, [Doc | Acc]};
                _ ->
                    {next, Acc}
            end
    end,
    model:execute_with_default_context(?MODULE, list, [Filter, []],
        ?LEVEL_OVERRIDE(task_to_db_level(Level)));

list_failed(Level) ->
    Filter = fun
        ('$end_of_table', Acc) ->
            {abort, Acc};
        (#document{value = V} = Doc, Acc) ->
            case task_manager:is_task_alive(V) of
                false ->
                    {next, [Doc | Acc]};
                _ ->
                    {next, Acc}
            end
    end,
    model:execute_with_default_context(?MODULE, list, [Filter, []],
        ?LEVEL_OVERRIDE(task_to_db_level(Level))).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback delete/1 but allows
%% choice of task level.
%% @end
%%--------------------------------------------------------------------
-spec delete(Level :: task_manager:level(), datastore:key()) -> ok | datastore:generic_error().
delete(?NON_LEVEL, _Key) ->
    ok;

delete(Level, Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key],
        ?LEVEL_OVERRIDE(task_to_db_level(Level))).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0. 
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(task_pool_bucket, [], ?LOCAL_ONLY_LEVEL)#model_config{
        list_enabled = {true, return_errors}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5. 
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
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
    ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Counts alive and failed tasks. Stops counting after specified limit.
%% @end
%%--------------------------------------------------------------------
-spec count_tasks(Level :: task_manager:level(), Type :: atom(), Limit :: non_neg_integer()) ->
    {ok, {Failed :: non_neg_integer(), All :: non_neg_integer()}} | datastore:generic_error() | no_return().
count_tasks(Level, Type, Limit) ->
    Filter = fun
                 ('$end_of_table', {Failed, All}) ->
                     {abort, {Failed, All}};
                 (#document{value = #task_pool{task_type = T, node = N} = V}, {Failed, All}) ->
                     case T of
                         Type ->
                             IsFailed = case Level of
                                 ?NODE_LEVEL ->
                                     MyNode = node(),
                                     (MyNode =:= N) andalso not task_manager:check_owner(V#task_pool.owner);
                                 _ ->
                                     not task_manager:is_task_alive(V)
                             end,
                             NewFailed = case IsFailed of
                                 true ->
                                     Failed + 1;
                                 _ ->
                                     Failed
                             end,
                             NewAll = All + 1,
                             case NewAll >= Limit of
                                 true ->
                                     {abort, {NewFailed, NewAll}};
                                 _ ->
                                     {next, {NewFailed, NewAll}}
                             end;
                         _ ->
                             {next, {Failed, All}}
                     end
             end,
    model:execute_with_default_context(?MODULE, list, [Filter, {0,0}],
        ?LEVEL_OVERRIDE(task_to_db_level(Level))).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Translates task level to store level.
%% @end
%%--------------------------------------------------------------------
-spec task_to_db_level(Level :: task_manager:level()) -> datastore:store_level().
task_to_db_level(?NODE_LEVEL) ->
    ?LOCAL_ONLY_LEVEL;

task_to_db_level(?CLUSTER_LEVEL) ->
    ?GLOBAL_ONLY_LEVEL;

task_to_db_level(?PERSISTENT_LEVEL) ->
    ?DISK_ONLY_LEVEL.