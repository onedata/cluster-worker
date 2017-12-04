%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Driver that coordinates access to memory stores.
%%% @end
%%%-------------------------------------------------------------------
-module(memory_store_driver_hub).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_doc.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
% TODO - delete second arg of terminate and third arg of modify
-export([modify/3, init/1, terminate/2, commit/2, merge_changes/2,
  commit_backoff/1, handle_committed/2]).

-record(state, {
  cached = false :: boolean(),
  master_pid :: pid(),
  state_map = #{}
}).

% Types
-type state() :: #state{}.
-type batch_key() :: {model_behaviour:model_type(), boolean(),
  datastore:store_level(), datastore:ext_key()}.
-type message_body() :: memory_store_driver:message().
-type message() :: {batch_key(), message_body()}.
-type change() :: memory_store_driver:change().

-define(DEFAULT_ERROR_SUSPENSION_TIME, timer:seconds(10)).
-define(DEFAULT_THROTTLING_DB_QUEUE_SIZE, 2000).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles all operation executed at memory store.
%% @end
%%--------------------------------------------------------------------
-spec modify(Messages :: [message()], State :: state(), datastore_doc:rev()) ->
  {Answers :: list(), {true, change()} | false, NewState :: state()}.
modify(Messages0, #state{cached = Cached,
  master_pid = Master, state_map = SM0} = State, _) ->
  Messages = split_messages(Messages0),

  {FinalAns, FinalChanges, FinalStateMap} = lists:foldl(
    fun({{_MN, Link, _Level, Key} = BatchDesc, MessagesList}, {AnsAcc, ChangesAcc, SM}) ->
      erase(),
      BatchState = case maps:get(BatchDesc, SM, undefined) of
        undefined -> memory_store_driver:new_state(Link, Key, Cached, Master);
        BS -> BS
      end,
      {A, Changes, NewState} =
        memory_store_driver:modify(MessagesList, BatchState, undefined),
      Changes2 = case {ChangesAcc, Changes} of
        {{true, C1}, {true, C2}} -> {true, C1 ++ C2};
        {{true, C1}, _} -> {true, C1};
        {_, {true, C2}} -> {true, C2};
        _ -> false
      end,
      {AnsAcc ++ A, Changes2, maps:put(BatchDesc, NewState, SM)}
  end, {[], false, SM0}, Messages),

  {FinalAns, FinalChanges, State#state{state_map = FinalStateMap}}.

%%--------------------------------------------------------------------
%% @doc
%% Initializes state of memory driver.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: list()) -> {ok, tp:init()}.
init([Cached]) ->
  {ok, #datastore_doc_init{
    data = #state{cached = Cached, master_pid = self()},
    idle_timeout = caches_controller:get_idle_timeout(),
    min_commit_delay = memory_store_driver:get_flush_min_interval(Cached),
    max_commit_delay = memory_store_driver:get_flush_max_interval(Cached)
  }}.

%%--------------------------------------------------------------------
%% @doc
%% Checks if memory store driver can be stopped.
%% @end
%%--------------------------------------------------------------------
-spec terminate(State :: state(), datastore_doc:rev()) -> ok | {error, term()}.
terminate(#state{state_map = SM}, _Rev) ->
  lists:foreach(fun(S) ->
    memory_store_driver:terminate(S, undefined)
  end, maps:values(SM)).

%%--------------------------------------------------------------------
%% @doc
%% Saves changes on persistent storage if possible.
%% @end
%%--------------------------------------------------------------------
-spec commit(Modified :: change(), State :: state()) ->
  {true | {false, change()}, datastore_doc:rev()}.
commit(ModifiedKeys, _State) ->
  memory_store_driver:commit(ModifiedKeys, memory_store_driver:new_state()).

%%--------------------------------------------------------------------
%% @doc
%% Updates state using information about last flush.
%% @end
%%--------------------------------------------------------------------
-spec merge_changes(Prev :: change(), Next :: change()) -> change().
merge_changes(Prev, Next) ->
  FilteredPrev = lists:filter(fun({Key, #{model_name := MN}}) ->
    Old = proplists:get_all_values(Key, Next),
    Found = lists:foldl(fun(#{model_name := MN2}, Acc) ->
      Acc orelse (MN =:= MN2)
    end, false, Old),
    not Found
  end, Prev),
  Next ++ FilteredPrev.

%%--------------------------------------------------------------------
%% @doc
%% Sets interval between flushes if flush fail.
%% @end
%%--------------------------------------------------------------------
-spec commit_backoff(timeout()) -> timeout().
commit_backoff(T) ->
  memory_store_driver:commit_backoff(T).

%%--------------------------------------------------------------------
%% @doc
%% Updates revision in datastore cache.
%% @end
%%--------------------------------------------------------------------
-spec handle_committed(state(), datastore_doc:rev()) -> state().
handle_committed(#state{} = State, _Rev) ->
  State.

%%%===================================================================
%%% Helper functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Splits messages list to chunks with similar batch key.
%% @end
%%--------------------------------------------------------------------
-spec split_messages([message()]) -> [{batch_key(), [message_body()]}].
split_messages([]) ->
  [];
split_messages([{BatchDesc, M} | Messages]) ->
  lists:reverse(split_messages(Messages, [], BatchDesc, [M])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Splits messages list to chunks with similar batch key.
%% @end
%%--------------------------------------------------------------------
-spec split_messages([message()], [{batch_key(), [message_body()]}],
    batch_key(), [message_body()]) -> [{batch_key(), [message_body()]}].
split_messages([], Ans, BD, List) ->
  [{BD, List} | Ans];
split_messages([{BatchDesc, M} | Messages], Ans, BatchDesc, List) ->
  split_messages(Messages, Ans, BatchDesc, List ++ [M]);
split_messages([{BatchDesc, M} | Messages], Ans, BD, List) ->
  split_messages(Messages, [{BD, List} | Ans], BatchDesc, [M]).