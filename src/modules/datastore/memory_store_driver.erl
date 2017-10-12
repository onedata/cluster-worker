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
-module(memory_store_driver).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_doc.hrl").
-include("modules/datastore/memory_store_driver.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
% TODO - delete second arg of terminate and third arg of modify
-export([modify/3, init/1, terminate/2, commit/2, merge_changes/2,
  commit_backoff/1, handle_committed/2]).
%% Helper functions
-export([resolve_conflict/2, update_rev_if_needed/2, add_durability_to_memory/2,
  get_durability_from_memory/0, del_durability_from_memory/1,
  add_to_proc_mem/3, get_from_proc_mem/2, increment_rev/2, rev_to_info/1]).

% Types
-type ctx() :: datastore_context:ctx().
-type state() :: #state{}.
-type value_doc() :: datastore:document() | undefined.
-type value_link() :: list().
-type message() :: {ctx(), {atom(), list()}}.
-type change() :: [{datastore:ext_key(), ctx()}].

-export_type([value_doc/0, value_link/0, message/0]).

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
modify(Messages, #state{link_proc = LP, key = Key, cached = Cached,
  master_pid = Master, docs_keys = DocsKeys} = State, _) ->
  {A, Changes} = case LP of
    true ->
      memory_store_driver_links:handle_link_messages(Messages, Master);
    _ ->
      memory_store_driver_docs:handle_messages(Messages, Key, Master)
  end,

  {FinalChanges, FinalState} = case {Cached, Changes} of
    {false, [{_K, Ctx} | _]} -> {false, State#state{last_ctx = Ctx}};
    {false, _} -> {false, State};
    {_, []} -> {false, State};
    {_, [{_K, Ctx} | _]} -> {{true, Changes}, State#state{last_ctx = Ctx}}
  end,

  DocsKeys2 = DocsKeys ++ (Changes -- DocsKeys),

  {A, FinalChanges, FinalState#state{docs_keys = DocsKeys2}}.


%%--------------------------------------------------------------------
%% @doc
%% Initializes state of memory driver.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: list()) -> {ok, tp:init()}.
init([Key, LinkProc, Cached]) ->
  {ok, #datastore_doc_init{
    data = #state{key = Key, link_proc = LinkProc, cached = Cached,
      master_pid = self()},
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
terminate(#state{last_ctx = undefined}, _Rev) ->
  ok;
terminate(#state{last_ctx = Ctx, docs_keys = DocsKeys}, _Rev) ->
  datastore_cache:inactivate(Ctx, DocsKeys).

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
  {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME,
    memory_store_flush_error_suspension_ms),

  {ok, Base} = application:get_env(?CLUSTER_WORKER_APP_NAME,
    throttling_delay_db_queue_size),
  QueueSize = lists:foldl(fun(Bucket, Acc) ->
    couchbase_pool:get_request_queue_size(Bucket) + Acc
  end, 0, couchbase_config:get_buckets()),

  min(10, max(round(QueueSize/Base), 1)) * Interval.

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
%% @doc
%% Resolves conflict between two documents.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(datastore:document(), datastore:document()) ->
  Resolved :: datastore:document() | not_changed.
resolve_conflict(#document{rev = []} = _Old, ToSave) ->
  ToSave;
resolve_conflict(#document{key = Key, rev = [OldRev | _]} = _Old,
    #document{key = Key, rev = [Rev | _]} = ToSave) ->
  case is_second_higher(OldRev, Rev) of
    higher -> ToSave;
    higher_rev -> ToSave;
    _ -> not_changed
  end.

update_rev_if_needed(#document{key = Key, rev = [OldRev | _] = OldRevs} = _Old,
    #document{key = Key, rev = [Rev | _]} = New) ->
  case is_second_higher(Rev, OldRev) of
    higher -> New#document{rev = OldRevs};
    _ -> New
  end;
update_rev_if_needed(#document{key = Key, rev = [_ | _] = OldRevs} = _Old,
    #document{key = Key, rev = []} = New) ->
  New#document{rev = OldRevs};
update_rev_if_needed(_, New) ->
  New.

%%--------------------------------------------------------------------
%% @doc
%% Saves information about document durability in memory.
%% @end
%%--------------------------------------------------------------------
-spec add_durability_to_memory(datastore:ext_key(),
    Durability :: ctx()) -> ok.
add_durability_to_memory(Key, D) ->
  add_to_proc_mem(durability_cache, Key, D).

%%--------------------------------------------------------------------
%% @doc
%% Deletes information about document durability from memory.
%% @end
%%--------------------------------------------------------------------
-spec del_durability_from_memory(datastore:ext_key()) -> ok.
del_durability_from_memory(Key) ->
  Value = get_value(durability_cache),
  put(durability_cache, proplists:delete(Key, Value)),
  ok.

%%--------------------------------------------------------------------
%% @doc
%% Gets document version that was fetched from memory from process memory.
%% @end
%%--------------------------------------------------------------------
-spec get_durability_from_memory() -> [{datastore:ext_key(), ctx()}].
get_durability_from_memory() ->
  case get(durability_cache) of
    undefined -> [];
    List -> List
  end.

%%--------------------------------------------------------------------
%% @doc
%% Saves information in memory. Generic cache function for caching of information
%% needed to process single batch of messages.
%% @end
%%--------------------------------------------------------------------
-spec add_to_proc_mem(MainKey :: atom(), AdditionalKey :: datastore:ext_key(),
    ToAdd :: term()) -> ok.
add_to_proc_mem(MainKey, AdditionalKey, ToAdd) ->
  Value = get_value(MainKey),
  put(MainKey, [{AdditionalKey, ToAdd} | proplists:delete(AdditionalKey, Value)]),
  ok.

%%--------------------------------------------------------------------
%% @doc
%% Gets information from memory. Generic cache function for caching of information
%% needed to process single batch of messages.
%% @end
%%--------------------------------------------------------------------
-spec get_from_proc_mem(MainKey :: atom(), AdditionalKey :: datastore:ext_key()) ->
  term().
get_from_proc_mem(MainKey, AdditionalKey) ->
  case get(MainKey) of
    undefined -> undefined;
    List ->
      proplists:get_value(AdditionalKey, List, undefined)
  end.

%%--------------------------------------------------------------------
%% @doc
%% Gets value from process memory.
%% @end
%%--------------------------------------------------------------------
-spec increment_rev(ctx(), datastore:document()) -> datastore:document().
increment_rev(#{resolve_conflicts := true}, Doc) ->
  Doc;
increment_rev(#{persistence := false}, Doc) ->
  Doc;
increment_rev(Ctx, Doc) ->
  {Doc2, _} = couchbase_doc:set_next_rev(Ctx, Doc),
  Doc2.

%%--------------------------------------------------------------------
%% @doc
%% Converts given binary into tuple {revision_num, hash}.
%% @end
%%--------------------------------------------------------------------
-spec rev_to_info(binary()) ->
  {Num :: non_neg_integer() | binary(), Hash :: binary()}.
rev_to_info(Rev) ->
  [Num, ID] = binary:split(Rev, <<"-">>),
  {binary_to_integer(Num), ID}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Compares two revisons.
%% @end
%%--------------------------------------------------------------------
-spec is_second_higher(couchbase_doc:hash(), couchbase_doc:hash()) ->
  higher | lower | higher_rev | lower_rev.
is_second_higher(First, Second) ->
  {FirstNum, FirstId} = rev_to_info(First),
  {SecodnNum, SecondId} = rev_to_info(Second),
  case SecodnNum of
    FirstNum ->
      case SecondId > FirstId of
        true ->
          higher_rev;
        _ ->
          lower_rev
      end;
    Higher when Higher > FirstNum ->
      higher;
    _ ->
      lower
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets value from process memory.
%% @end
%%--------------------------------------------------------------------
-spec get_value(atom()) -> list().
get_value(Key) ->
  case get(Key) of
    undefined -> [];
    List -> List
  end.

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
