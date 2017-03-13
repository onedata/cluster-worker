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

% Types
-type state() :: #state{}.
-type value_doc() :: datastore:document() | undefined | not_found.
-type value_link() :: list().
-type message() :: {atom(), list()}.
-type change() :: ok | to_save | list().

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
    {_, []} -> false;
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
      {lists:reverse(ReversedAns), FinalChanges, State#state{current_value = NV}}
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
commit(ModifiedKeys, #state{model_config = MC, current_value = CV,
  flush_driver = Driver, link_proc = true}) ->
  ModifiedList = lists:map(fun(K) ->
    {K, proplists:get_value(K, CV)}
  end, ModifiedKeys),

  Refs = lists:foldl(fun
    % TODO - handle batch delete
    ({K, not_found}, Acc) ->
      [{K, not_found} | Acc];
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

  NotSaved = lists:foldl(fun
    ({K, V, {get_error, _} = E}, Acc) ->
      ?error("Get link doc error ~p for key ~p, value ~p", [E, K, V]),
      [K | Acc];
    % TODO - handle batch delete
    ({K, not_found}, Acc) ->
      DumpAns = case apply(Driver, get_link_doc, [MC, K]) of
        {ok, OldDoc} ->
          apply(Driver, delete_link_doc, [MC, OldDoc]);
        {error, {not_found, _}} ->
          ok;
        E ->
          E
      end,
      case DumpAns of
        ok ->
          Acc;
        Err ->
          ?error("Delete link error ~p for key ~p", [Err, K]),
          [K | Acc]
      end;
    ({K, V, Ref}, Acc) ->
      case apply(Driver, save_doc_asynch_response, [Ref]) of
        {ok, _} ->
          Acc;
        {error, already_exists} -> % conflict with force_save
          ?debug("Save link canceled for key ~p, value ~p: already exists", [K, V]),
          Acc;
        Err ->
          ?error("Save link error ~p for key ~p, value ~p", [Err, K, V]),
          [K | Acc]
      end
  end, [], Refs),

  case NotSaved of
    [] ->
      true;
    _ ->
      {false, NotSaved}
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
  end.

%%--------------------------------------------------------------------
%% @doc
%% Updates state using information about last flush.
%% @end
%%--------------------------------------------------------------------
-spec merge_changes(Prev :: change(), Next :: change()) -> change().
merge_changes(to_save, _) ->
  to_save;
merge_changes(_, to_save) ->
  to_save;
merge_changes(P, _N) when is_atom(P) ->
  P;
merge_changes(P, N) ->
  N ++ (P -- N).

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
%%% Internal functions
%%%===================================================================

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
