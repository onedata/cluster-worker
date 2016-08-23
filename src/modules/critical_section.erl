%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% This module allows constructing critical sections.
%%% @end
%%%-------------------------------------------------------------------
% TODO VFS-2371 - problem with nested critical sections (change to critical sections in couchdb_datastore_driver to check error).
-module(critical_section).
-author("Mateusz Paciorek").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").

%% API
-export([run/2, lock/1, unlock/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Runs Fun in critical section locked on Key.
%% Guarantees that at most one function is running for selected Key in all
%% nodes in current cluster at any given moment.
%% @end
%%--------------------------------------------------------------------
-spec run(Key :: datastore:key(), Fun :: fun (() -> Result :: term())) ->
    Result :: term().
run(Key, Fun) ->
    ok = lock(Key),
    try
        Fun()
    after
        ok = unlock(Key)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Enqueues process for lock on given key.
%% If process is first in the queue, this function returns immediately,
%% otherwise waits for message from process releasing lock.
%% It is possible for one process to acquire same lock multiple times,
%% but it must be released the same number of times.
%% @end
%%--------------------------------------------------------------------
-spec lock(Key :: datastore:key()) -> ok.
lock(Key) ->
    case lock:enqueue(Key, self()) of
        {ok, acquired} ->
            ok;
        {ok, wait} ->
            {ok, TimeoutMinutes} = application:get_env(
                ?CLUSTER_WORKER_APP_NAME,
                lock_timeout_minutes
            ),
            receive
                {acquired, Key} ->
                    ok
            after timer:minutes(TimeoutMinutes) ->
                throw(lock_timeout)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Dequeues process from lock on given key.
%% If process has acquired this lock multiple times, counter is decreased.
%% When counter reaches zero, next waiting process receives message that
%% the lock has been successfully acquired.
%% @end
%%--------------------------------------------------------------------
-spec unlock(Key :: datastore:key()) -> ok | {error, term()}.
unlock(Key) ->
    Self = self(),
    case lock:dequeue(Key, Self) of
        {ok, empty} ->
            ok;
        {ok, Self} ->
            ok;
        {ok, Pid} ->
            Pid ! {acquired, Key},
            ok;
        Error ->
            Error
    end.