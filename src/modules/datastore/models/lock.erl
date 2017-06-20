%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model of lock for critical section.
%%% @end
%%%-------------------------------------------------------------------
-module(lock).
-author("Mateusz Paciorek").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_internal_model.hrl").
-include_lib("ctool/include/logging.hrl").

-type queue_element() :: {Pid :: pid(), Counter :: non_neg_integer()}.
-export_type([queue_element/0]).

%% model_behaviour callbacks and API
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    create_or_update/2, model_init/0, 'after'/5, before/4]).
-export([enqueue/3, dequeue/2, current_owner/1, is_pid_alive/1]).

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
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% Updates given document by replacing given fields with new values or
%% creates new one if not exists.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(Document :: datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
create_or_update(Doc, Diff) ->
    model:execute_with_default_context(?MODULE, create_or_update, [Doc, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
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
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(lock_bucket, [], ?GLOBAL_ONLY_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok | datastore:generic_error().
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

%%--------------------------------------------------------------------
%% @doc
%% Adds Pid to queue of processes waiting for lock on Key.
%% Returns status after operation:
%% 'acquired' if lock is taken, 'wait' otherwise.
%% Lock may be taken as recursive or non-recursive.
%% @end
%%--------------------------------------------------------------------
-spec enqueue(datastore:ext_key(), pid(), boolean()) ->
    {ok, acquired | wait} | {error, already_acquired}.
enqueue(Key, Pid, Recursive) ->
    case get(Key) of
        {ok, #document{value = #lock{queue = Q}}} ->
            case has(Q, Pid) of
                true ->
                    case Recursive of
                        true ->
                            {ok, Key} = update(Key, #{queue => inc(Q)}),
                            {ok, acquired};
                        false ->
                            {error, already_acquired}
                    end;
                false ->
                    {ok, Key} = update(Key, #{queue => add(Q, Pid)}),
                    {ok, wait}
            end;
        {error, {not_found, _}} ->
            {ok, _} = create(#document{key = Key, value = #lock{queue = add([], Pid)}}),
            {ok, acquired}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes first Pid from queue of processes waiting for lock on Key.
%% Returns element that is in front of the queue after operation (if any).
%% Record is automatically deleted if queue is empty after operation.
%% @end
%%--------------------------------------------------------------------
-spec dequeue(datastore:ext_key(), pid()) ->
    {ok, pid() | empty} | {error, not_lock_owner | lock_does_not_exist}.
dequeue(Key, Pid) ->
    case get(Key) of
        {ok, #document{value = #lock{queue = Q}}} ->
            case has(Q, Pid) of
                false ->
                    {error, not_lock_owner};
                true ->
                    NewQ = dec(Q),
                    case length(NewQ) of
                        0 ->
                            delete(Key),
                            {ok, empty};
                        _ ->
                            {ok, Key} = update(Key, #{queue => NewQ}),
                            {ok, owner(NewQ)}
                    end
            end;
        {error, {not_found, _}} ->
            {error, lock_does_not_exist}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns current owner the lock.
%% @end
%%--------------------------------------------------------------------
-spec current_owner(Key :: term()) -> {ok, pid()} | {error, term()}.
current_owner(Key) ->
    case get(Key) of
        {ok, #document{value = #lock{queue = Q}}} ->
            Owner = owner(Q),
            {ok, Owner};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if owner is alive.
%% @end
%%--------------------------------------------------------------------
-spec is_pid_alive(pid()) -> boolean().
is_pid_alive(Owner) ->
    case rpc:pinfo(Owner) of
        Info when is_list(Info) ->
            true;
        _ ->
            false
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds new element with given Pid and Counter set to 1 at the end of queue.
%% @end
%%--------------------------------------------------------------------
-spec add([queue_element()], pid()) -> [queue_element()].
add(Q, Pid) ->
    Q ++ [{Pid, 1}].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given Pid has the lock.
%% @end
%%--------------------------------------------------------------------
-spec has([queue_element()], pid()) -> boolean().
has(Q, Pid) ->
    Pid =:= owner(Q).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns owner of the lock, ie. pid in the front of queue.
%% @end
%%--------------------------------------------------------------------
-spec owner([queue_element()]) -> pid().
owner([{Pid, _} | _]) ->
    Pid.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Increases counter of first element in queue.
%% @end
%%--------------------------------------------------------------------
-spec inc([queue_element()]) -> [queue_element()].
inc([{Pid, C} | T]) when C > 0 ->
    [{Pid, C + 1} | T].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Decreases counter of first element in queue.
%% @end
%%--------------------------------------------------------------------
-spec dec([queue_element()]) -> [queue_element()].
dec([{Pid, C} | T]) when C > 1 ->
    [{Pid, C - 1} | T];
dec([{_Pid, C} | T]) when C =:= 1 ->
    T.