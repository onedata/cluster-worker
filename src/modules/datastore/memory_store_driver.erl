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
-behaviour(tp_behaviour).

-include("global_definitions.hrl").
-include("modules/tp/tp.hrl").
-include("modules/datastore/memory_store_driver.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

% TODO - add option - enable mnesia transaction log and/or replication
% of data to other nodes
% TODO - add setting of cache level - from simple ets to dumping to
% couch immediately
% TODO - check size of key and doc during modify operation

%% API
-export([modify/2, init/1, terminate/1, commit/2, merge_changes/2,
  commit_backoff/1]).
%% Helper functions
-export([driver_to_level/1, resolve_conflict/3]).

% Types
-type state() :: #state{}.
-type value_doc() :: datastore:document() | undefined | not_found.
-type value_link() :: list().
-type message() :: {atom(), list()}.
-type resolved_conflict() :: {Document :: datastore:document(),
  Bucket :: datastore:bucket(), ToDel :: false | datastore:document()}.
-type change() :: ok | to_save | {to_save, resolved_conflict()} |
  {[datastore:ext_key()], [{datastore:ext_key(), resolved_conflict()}]}.

-export_type([value_doc/0, value_link/0, message/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles all operation executed at memory store.
%% @end
%%--------------------------------------------------------------------
-spec modify(Messages :: [message()], State :: state()) ->
  {Answers :: list(), {true, change()} | false, NewState :: state()}.
modify([{clear, _}], #state{link_proc = true, key = Key, driver = Driver,
  model_config = ModelConfig} = State) ->
  Ans = memory_store_driver_links:clear(Driver, ModelConfig, Key),
  {[Ans], false, State};
modify([{clear, _}], #state{driver = Driver, model_config = ModelConfig,
  key = Key} = State) ->
  Ans = memory_store_driver_docs:clear(Driver, ModelConfig, Key),
  {[Ans], false, State};
modify(Messages, #state{link_proc = LP, current_value = CurrentValue,
  driver = Driver, flush_driver = FD,
  model_config = ModelConfig, key = Key} = State) ->
  FilteredMessages = lists:filter(fun
    ({clear, _}) -> false;
    (_) -> true
  end, Messages),
  {A, NV, Changes} = try
    case LP of
      true ->
        memory_store_driver_links:handle_link_messages(FilteredMessages,
          CurrentValue, Driver, FD, ModelConfig, Key);
      _ ->
        memory_store_driver_docs:handle_messages(FilteredMessages,
          CurrentValue, Driver, FD, ModelConfig, Key)
    end
  catch
    throw:{Reason, ThrowNV, ThrowChanges} ->
      ?error_stacktrace("Modify error for key ~p: ~p", [Key, Reason]),
      ThrowA = lists:map(fun(_) -> Reason end, FilteredMessages),
      {ThrowA, ThrowNV, ThrowChanges}
  end,

  FinalChanges = case {FD, Changes} of
    {undefined, _} -> false;
    {_, {[], []}} -> false;
    {_, ok} -> false;
    _ -> {true, Changes}
  end,
  case length(FilteredMessages) =:= length(Messages) of
    true ->
      {A, FinalChanges, State#state{current_value = NV}};
    _ ->
      {ReversedAns, []} = lists:foldl(fun
        ({clear, _}, {Acc, AnsList}) ->
          {[ok | Acc], AnsList};
        (_, {Acc, [Ans | AnsList]}) ->
          {[Ans | Acc], AnsList}
      end, {[], A}, Messages),
      {lists:reverse(ReversedAns), FinalChanges,
        State#state{current_value = NV}}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Initializes state of memory driver.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: list()) -> {ok, tp:init()}.
% TODO - add arg for save and create etc operations that get is not needed.
init([Driver, MC, Key, FD, LinkProc]) ->
  init([Driver, MC, Key, FD, LinkProc, caches_controller:get_idle_timeout()]);
init([Driver, MC, Key, FD, true = LinkProc, IdleT]) ->
  {ok, #tp_init{data = #state{driver = Driver, model_config = MC, key = Key,
    link_proc = LinkProc, flush_driver = FD, current_value = []},
    idle_timeout = IdleT, min_commit_delay = get_flush_min_interval(FD),
    max_commit_delay = get_flush_max_interval(FD)}};
init([Driver, MC, Key, FD, LinkProc, IdleT]) ->
  {ok, #tp_init{data = #state{driver = Driver, model_config = MC, key = Key,
    link_proc = LinkProc, flush_driver = FD}, idle_timeout = IdleT,
    min_commit_delay = get_flush_min_interval(FD),
    max_commit_delay = get_flush_max_interval(FD)}}.

%%--------------------------------------------------------------------
%% @doc
%% Checks if memory store driver can be stopped.
%% @end
%%--------------------------------------------------------------------
-spec terminate(State :: state()) -> ok | {error, term()}.
terminate(#state{flush_driver = undefined}) ->
  ok;
terminate(#state{link_proc = true, key = Key, driver = Driver,
  model_config = ModelConfig}) ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME,
    tp_proc_terminate_clear_memory) of
    {ok, true} ->
      memory_store_driver_links:clear(Driver, ModelConfig, Key);
    _ ->
      ok
  end,
  ok;
terminate(#state{key = Key, driver = Driver, model_config = ModelConfig}) ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME,
    tp_proc_terminate_clear_memory) of
    {ok, true} ->
      memory_store_driver_docs:clear(Driver, ModelConfig, Key);
    _ ->
      ok
  end,
  ok.

%%--------------------------------------------------------------------
%% @doc
%% Saves changes on persistent storage if possible.
%% @end
%%--------------------------------------------------------------------
-spec commit(Modified :: change(), State :: state()) -> true | {false, change()}.
commit({ModifiedKeys, []}, #state{model_config = MC, current_value = CV,
  flush_driver = Driver, link_proc = true}) ->
  ModifiedList = lists:map(fun(K) ->
    {K, proplists:get_value(K, CV)}
  end, ModifiedKeys),

  NotSaved = dump_docs(MC, Driver, ModifiedList),

  case NotSaved of
    [] ->
      true;
    _ ->
      {false, NotSaved}
  end;
commit({ModifiedKeys, ResolvedChanges}, #state{model_config = MC, current_value = CV,
  flush_driver = Driver, link_proc = true} = State) ->
  NotSaved = dump_docs(MC, Driver, ResolvedChanges),

  RevsToDel = lists:foldl(fun({K, {_Document, _Bucket, ToDel}}, Acc) ->
    case lists:member(K, NotSaved) of
      true ->
        Acc;
      _ ->
        [{K, {delete_doc_asynch, ToDel}} | Acc]
    end
  end, [], ResolvedChanges),
  NotSaved2 = dump_docs(MC, Driver, RevsToDel),

  case {NotSaved, NotSaved2} of
    {[], []} ->
      ModifiedKeys2 = lists:foldl(fun(K, Acc) ->
        V = proplists:get_value(K, CV),
        case proplists:get_value(K, ResolvedChanges) of
          {ToCheck, _, _} ->
            case check_resolved_doc(V, ToCheck) of
              true ->
                Acc;
              _ ->
                [K | Acc]
            end;
          _ ->
            [K | Acc]
        end
      end, [], ModifiedKeys),

      commit({ModifiedKeys2, []}, State);
    _ ->
      ResolvedChanges2 = lists:foldl(fun(K, Acc) ->
        [proplists:get_value(K, ResolvedChanges) | Acc]
      end, [], NotSaved),

      ResolvedChanges2_2 = lists:foldl(fun(K, Acc) ->
        [proplists:get_value(K, RevsToDel) | Acc]
      end, [], NotSaved2),
      % TODO - delete old rev even if new revision appears before successful del
      {false, {ModifiedKeys, ResolvedChanges2 ++ ResolvedChanges2_2}}
  end;
commit(ok, _State)->
  true;
commit(to_save, #state{model_config = MC, key = Key, current_value = CV,
  flush_driver = Driver})->
  Ans = case CV of
    not_found ->
      apply(Driver, delete, [MC, Key, ?PRED_ALWAYS]);
    _ ->
      case apply(Driver, save, [MC, CV]) of
        {ok, Key} -> ok;
        Other -> Other
      end
  end,

  case Ans of
    ok ->
      true;
    {error, already_exists} -> % conflict with force_save
      ?debug("Dump doc canceled for key ~p, value ~p: already exists", [Key, CV]),
      true;
    Err ->
      ?error("Dump doc error ~p for key ~p, value ~p", [Err, Key, CV]),
      {false, to_save}
  end;
commit({to_save, ResolvedConflicts} = TS, #state{model_config = MC, key = Key,
  current_value = CV, flush_driver = Driver} = State)->
  {Document, Bucket, ToDel} = ResolvedConflicts,

  SRAns = case Driver:save_revision(MC, Bucket, Document) of
    {ok, _} ->
      % TODO - what happens if first save is ok and second fails
      % Delete in new task type that starts if first try fails
      case ToDel of
        false ->
          ok;
        _ ->
          apply(Driver, delete_doc, [MC, ToDel])
      end;
    Other ->
      Other
  end,

  Ans = case SRAns of
    ok ->
      true;
    {error, already_exists} -> % conflict with force_save
      ?debug("Dump doc canceled for key ~p, value ~p: already exists", [Key, CV]),
      true;
    Err ->
      ?error("Dump doc error ~p for key ~p, value ~p", [Err, Key, CV]),
      {false, TS}
  end,

  case {Ans, check_resolved_doc(CV, Document#document{rev = undefined})} of
    {true, true} ->
      true;
    {true, _} ->
      commit(to_save, State);
    {Err2, _} ->
      Err2
  end.

%%--------------------------------------------------------------------
%% @doc
%% Updates state using information about last flush.
%% @end
%%--------------------------------------------------------------------
-spec merge_changes(Prev :: change(), Next :: change()) -> change().
merge_changes(_, {to_save, DiskValue}) ->
  {to_save, DiskValue};
merge_changes({to_save, DiskValue}, _) ->
  {to_save, DiskValue};
merge_changes(to_save, _) ->
  to_save;
merge_changes(_, to_save) ->
  to_save;
merge_changes(P, _N) when is_atom(P) ->
  P;
merge_changes({P1, P2}, {N1, N2}) ->
  PrevK = proplists:get_keys(P2),
  NewK = proplists:get_keys(N2),
  New2 = lists:foldl(fun(K, Acc) ->
    [proplists:lookup(K, P2) | Acc]
  end, N2, PrevK -- NewK),
  {N1 ++ (P1 -- N1), New2}.

%%--------------------------------------------------------------------
%% @doc
%% Sets interval between flushes if flush fail.
%% @end
%%--------------------------------------------------------------------
-spec commit_backoff(timeout()) -> timeout().
% TODO - brac pod uwage ile procesow wisi na flush
commit_backoff(_T) ->
  {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME,
    memory_store_flush_error_suspension_ms),
  Interval.

%%%===================================================================
%%% Helper functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets level for driver.
%% @end
%%--------------------------------------------------------------------
-spec driver_to_level(atom()) -> datastore:store_level().
% TODO - also local only level
driver_to_level(_Driver) ->
  ?GLOBAL_ONLY_LEVEL.

%%--------------------------------------------------------------------
%% @doc
%% Resolves conflict between new document and current document.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(model_behaviour:model_config(), atom(), datastore:document()) ->
  datastore:generic_error() | not_changed | {ResolvedDoc :: datastore:document(),
    DeleteOldRev :: datastore:document() | false}.
resolve_conflict(ModelConfig, Driver,
    #document{key = Key, rev = {RNum, [Id | _]}} = ToSave) ->

  case Driver:get_last(ModelConfig, Key) of
    {error, {not_found, _}} ->
      FinalDoc = ToSave#document{rev = {RNum, [Id]}},
      {FinalDoc, false};
    {error, not_found} ->
      FinalDoc = ToSave#document{rev = {RNum, [Id]}},
      {FinalDoc, false};
    {error, Reason} ->
      {error, Reason};
    {ok, OldDoc} ->
      resolve_docs_conflict(OldDoc, ToSave)
  end.

%%--------------------------------------------------------------------
%% @doc
%% Resolves conflict between two documents.
%% @end
%%--------------------------------------------------------------------
-spec resolve_docs_conflict(datastore:document(), datastore:document()) ->
  not_changed | {ResolvedDoc :: datastore:document(),
    DeleteOldRev :: datastore:document() | false}.
resolve_docs_conflict(#document{key = Key, rev = Rev, deleted = OldDel} = Old,
    #document{key = Key, rev = {RNum, [Id | IdsTail]}} = ToSave) ->
  {OldRNum, OldId} = rev_to_info(Rev),
  case RNum of
    OldRNum ->
      case Id > OldId of
        true ->
          case OldDel of
            true ->
              {ToSave, false};
            _ ->
              {ToSave, Old}
          end;
        false ->
          not_changed
      end;
    Higher when Higher > OldRNum ->
      NewIDs = check_revisions_list(OldId, IdsTail, OldRNum, Higher),
      {ToSave#document{rev = {RNum, [Id | NewIDs]}}, false};
    _ ->
      not_changed
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts given binary into tuple {revision_num, hash}.
%% @end
%%--------------------------------------------------------------------
-spec rev_to_info(binary()) ->
  {Num :: non_neg_integer() | binary(), Hash :: binary()}.
rev_to_info(Rev) ->
  [Num, ID] = binary:split(Rev, <<"-">>),
  {binary_to_integer(Num), ID}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if revision list provided to force_save can be written.
%% @end
%%--------------------------------------------------------------------
-spec check_revisions_list(term(), list(), integer(), integer()) -> list().
check_revisions_list(OldID, [OldID | _] = NewIDs, OldNum, NewNum) when NewNum =:= OldNum + 1 ->
  NewIDs;
check_revisions_list(_, _, _, _) ->
  [].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns min interval between successful flush operations.
%% @end
%%--------------------------------------------------------------------
-spec get_flush_min_interval(FlushDriver :: atom()) -> non_neg_integer().
get_flush_min_interval(FlushDriver) ->
  case FlushDriver of
    undefined ->
      infinity;
    _ ->
      {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        cache_to_disk_delay_ms),
      Interval
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns max interval between successful flush operations.
%% @end
%%--------------------------------------------------------------------
-spec get_flush_max_interval(FlushDriver :: atom()) -> non_neg_integer().
get_flush_max_interval(FlushDriver) ->
  case FlushDriver of
    undefined ->
      infinity;
    _ ->
      {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        cache_to_disk_force_delay_ms),
      Interval
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Dumps to disk.
%% @end
%%--------------------------------------------------------------------
-spec dump_docs(model_behaviour:model_config(), atom(), ModifiedList :: list()) ->
  [datastore:ext_key()].
dump_docs(MC, Driver, ModifiedList) ->
  Refs = lists:foldl(fun
  % TODO - handle batch delete
    ({K, not_found}, Acc) ->
      % TODO - get should not be needed before get
      RefOrError = case apply(Driver, get_link_doc, [MC, K]) of
        {ok, OldDoc} ->
          apply(Driver, delete_doc_asynch, [MC, OldDoc]);
        {error, {not_found, _}} ->
          ok;
        E ->
          {get_error, E}
      end,
      [{K, not_found, RefOrError} | Acc];
    ({K, {Document, Bucket, _ToDel} = V}, Acc) ->
      RefOrError = Driver:save_revision_asynch(MC, Bucket, Document),
      [{K, V, RefOrError} | Acc];
    ({K, {delete_doc_asynch, ToDel} = V}, Acc) ->
      RefOrError = case ToDel of
        false -> ok;
        _ -> apply(Driver, delete_doc_asynch, [MC, ToDel])
      end,
      [{K, V, RefOrError} | Acc];
    ({K, V}, Acc) ->
      % TODO - get should not be needed before save
      RefOrError = case apply(Driver, get_link_doc, [MC, K]) of
        {ok, OldDoc} ->
          apply(Driver, save_doc_asynch, [MC, OldDoc#document{value = V}]);
        {error, {not_found, _}} ->
          apply(Driver, save_doc_asynch, [MC, #document{key = K, value = V}]);
        E ->
          {get_error, E}
      end,
      [{K, V, RefOrError} | Acc]
  end, [], ModifiedList),

  lists:foldl(fun
    ({K, V, {get_error, _} = E}, Acc) ->
      ?error("Get link doc error ~p for key ~p, value ~p", [E, K, V]),
      [K | Acc];
    ({_K, _V, ok}, Acc) ->
      Acc;
    ({K, V, Ref}, Acc) ->
      case apply(Driver, asynch_response, [Ref]) of
        ok ->
          Acc;
        {ok, _} ->
          Acc;
        {error, already_exists} -> % conflict with force_save
          ?debug("Save link canceled for key ~p, value ~p: already exists", [K, V]),
          Acc;
        Err ->
          ?error("Link dump error ~p for key ~p, value ~p", [Err, K, V]),
          [K | Acc]
      end
  end, [], Refs).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if resolved doc is equal to doc voalue in memory.
%% @end
%%--------------------------------------------------------------------
-spec check_resolved_doc(value_doc() | datastore:value(), datastore:document()) ->
  boolean().
check_resolved_doc(not_found, #document{deleted = true}) ->
  true;
check_resolved_doc(#document{value = V}, #document{value = V}) ->
  true;
check_resolved_doc(V, #document{value = V}) ->
  true;
check_resolved_doc(_, _) ->
  false.