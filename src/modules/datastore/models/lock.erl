%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model of lock for critical section.
%%% @end
%%%-------------------------------------------------------------------
-module(lock).
-author("Mateusz Paciorek").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([enqueue/3, dequeue/2, current_owner/1, is_pid_alive/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type queue_element() :: {Pid :: pid(), Counter :: non_neg_integer()}.
-export_type([queue_element/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds Pid to queue of processes waiting for lock on Key.
%% Returns status after operation: 'acquired' if lock is taken, 'wait'
%% otherwise. Lock may be taken as recursive or non-recursive.
%% @end
%%--------------------------------------------------------------------
-spec enqueue(key(), pid(), boolean()) ->
    {ok, acquired | wait} | {error, already_acquired}.
enqueue(Key, Pid, Recursive) ->
    Diff = fun(Lock = #lock{queue = Queue}) ->
        case {has(Queue, Pid), Recursive} of
            {true, true} ->
                {ok, Lock#lock{queue = inc(Queue)}};
            {true, false} ->
                {error, already_acquired};
            {false, _} ->
                {ok, Lock#lock{queue = add(Queue, Pid)}}
        end
    end,
    Default = #lock{queue = add([], Pid)},
    case datastore_model:update(?CTX, Key, Diff, Default) of
        {ok, #document{value = #lock{queue = Queue}}} ->
            case has(Queue, Pid) of
                true -> {ok, acquired};
                false -> {ok, wait}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes first Pid from queue of processes waiting for lock on Key.
%% Returns element that is in front of the queue after operation (if any).
%% Record is automatically deleted if queue is empty after operation.
%% @end
%%--------------------------------------------------------------------
-spec dequeue(key(), pid()) ->
    {ok, pid() | empty} | {error, not_lock_owner | lock_does_not_exist}.
dequeue(Key, Pid) ->
    Diff = fun(Lock = #lock{queue = Queue}) ->
        case has(Queue, Pid) of
            true -> {ok, Lock#lock{queue = dec(Queue)}};
            false -> {error, not_lock_owner}
        end
    end,
    case datastore_model:update(?CTX, Key, Diff) of
        {ok, #document{value = #lock{queue = []}}} ->
            case datastore_model:delete(?CTX, Key) of
                ok -> {ok, empty};
                {error, Reason} -> {error, Reason}
            end;
        {ok, #document{value = #lock{queue = Queue}}} ->
            {ok, owner(Queue)};
        {error, not_found} ->
            {error, lock_does_not_exist};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns current owner the lock.
%% @end
%%--------------------------------------------------------------------
-spec current_owner(key()) -> {ok, pid()} | {error, term()}.
current_owner(Key) ->
    case datastore_model:get(?CTX, Key) of
        {ok, #document{value = #lock{queue = Queue}}} -> {ok, owner(Queue)};
        {error, Reason} -> {error, Reason}
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

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.