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
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
% TODO - delete second arg of terminate and third arg of modify
-export([modify/3, init/1, terminate/2, commit/2, merge_changes/2,
  commit_backoff/1, handle_committed/2]).

-record(state, {
  key :: datastore:ext_key(),
  cached = false :: boolean(),
  master_pid :: pid(),
  state_map :: #{}
}).

% Types
-type ctx() :: datastore_context:ctx().
-type state() :: #state{}.
-type value_doc() :: datastore:document() | undefined.
-type value_link() :: list().
-type message() :: {ctx(), {atom(), list()}}.
-type change() :: [{datastore:ext_key(), ctx()}].

-export_type([value_doc/0, value_link/0, message/0]).

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
modify(Messages0, #state{key = Key, cached = Cached,
  master_pid = Master, state_map = SM0} = State, _) ->
  Messages = split_messages(Messages0),

  {FinalAns, FinalChanges, FinalStateMap} = lists:foldl(fun({{_MN, Link, _Level} = BatchDesc, MessagesList}, {AnsAcc, ChangesAcc, SM}) ->
    BatchState = case maps:get(BatchDesc, SM) of
      undefined -> memory_store_driver:new_state(Link, Key, Cached, Master);
      BS -> BS
    end,
    {A, Changes, NewState} =
      memory_store_driver:modify(MessagesList, BatchState, undefined),
    {AnsAcc ++ A, ChangesAcc ++ Changes, maps:put(BatchDesc, NewState, SM)}
  end, {[], [], SM0}, Messages),

  {FinalAns, FinalChanges, State#state{state_map = FinalStateMap}}.

split_messages([]) ->
  [];
split_messages([{BatchDesc, M} | Messages]) ->
  lists:reverse(split_messages(Messages, [], BatchDesc, [M])).

split_messages([], Ans, BD, List) ->
  [{BD, List} | Ans];
split_messages([{BatchDesc, M} | Messages], Ans, BatchDesc, List) ->
  split_messages(Messages, Ans, BatchDesc, List ++ [M]);
split_messages([{BatchDesc, M} | Messages], Ans, BD, List) ->
  split_messages(Messages, [{BD, List} | Ans], BatchDesc, [M]).

%%--------------------------------------------------------------------
%% @doc
%% Initializes state of memory driver.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: list()) -> {ok, tp:init()}.
init([Key, Cached]) ->
  {ok, #datastore_doc_init{
    data = #state{key = Key, cached = Cached, master_pid = self()},
    idle_timeout = caches_controller:get_idle_timeout(),
    min_commit_delay = get_flush_min_interval(Cached),
    max_commit_delay = get_flush_max_interval(Cached)
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
commit(ModifiedKeys, _) ->
  ToFlush = lists:reverse(ModifiedKeys),
  AnsList = datastore_cache:flush(ToFlush),

  {_Revs, NotSaved} = lists:foldl(fun
    ({_Flush, {ok, #document{key = K, rev = R}}}, {AccRev, AccErr}) ->
      {[{K, R} | AccRev], AccErr};
    ({{Key, Ctx}, {error, etimedout}}, {AccRev, AccErr}) ->
      ?error("Cannot flush document to database - timeout"),
      {AccRev, [{Key, Ctx} | AccErr]};
    ({{Key, Ctx}, {error, timeout}}, {AccRev, AccErr}) ->
      ?error("Cannot flush document to database - timeout"),
      {AccRev, [{Key, Ctx} | AccErr]};
    ({{Key, Ctx}, Error}, {AccRev, AccErr}) ->
      ?error("Document flush to database error ~p for key ~p, context ~p",
        [Error, Key, Ctx]),
      {AccRev, [{Key, Ctx} | AccErr]}
  end, {[], []}, lists:zip(ToFlush, AnsList)),

  case NotSaved of
    [] ->
      {true, []};
    _ ->
      {{false, NotSaved}, []}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Updates state using information about last flush.
%% @end
%%--------------------------------------------------------------------
-spec merge_changes(Prev :: change(), Next :: change()) -> change().
merge_changes(Prev, Next) ->
  FilteredPrev = lists:filter(fun({Key, _Ctx}) ->
    not proplists:is_defined(Key, Next)
  end, Prev),
  Next ++ FilteredPrev.

%%--------------------------------------------------------------------
%% @doc
%% Sets interval between flushes if flush fail.
%% @end
%%--------------------------------------------------------------------
-spec commit_backoff(timeout()) -> timeout().
commit_backoff(_T) ->
  Interval = application:get_env(?CLUSTER_WORKER_APP_NAME,
    memory_store_flush_error_suspension_ms, ?DEFAULT_ERROR_SUSPENSION_TIME),

  try
    Base = application:get_env(?CLUSTER_WORKER_APP_NAME,
      throttling_delay_db_queue_size, ?DEFAULT_THROTTLING_DB_QUEUE_SIZE),
    QueueSize = lists:foldl(fun(Bucket, Acc) ->
      couchbase_pool:get_request_queue_size(Bucket) + Acc
    end, 0, couchbase_config:get_buckets()),

    min(10, max(round(QueueSize/Base), 1)) * Interval
  catch
    E1:E2 ->
      ?error_stacktrace("Cannot calculate commit backoff, error: ~p:~p",
        [E1, E2]),
      Interval
  end.

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
%% Returns min interval between successful flush operations.
%% @end
%%--------------------------------------------------------------------
-spec get_flush_min_interval(FlushDriver :: atom()) -> non_neg_integer().
get_flush_min_interval(true) ->
  {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME,
    cache_to_disk_delay_ms),
  Interval;
get_flush_min_interval(_) ->
  infinity.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns max interval between successful flush operations.
%% @end
%%--------------------------------------------------------------------
-spec get_flush_max_interval(FlushDriver :: atom()) -> non_neg_integer().
get_flush_max_interval(true) ->
  {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME,
    cache_to_disk_force_delay_ms),
  Interval;
get_flush_max_interval(_) ->
  infinity.
