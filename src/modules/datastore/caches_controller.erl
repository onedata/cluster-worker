%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used by node manager to coordinate
%%% clearing of not used values cached in memory.
%%% TODO - sort cache documents by timestamp.
%%% @end
%%%-------------------------------------------------------------------
-module(caches_controller).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include("elements/task_manager/task_manager.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([clear_local_cache/1, clear_global_cache/1]).
-export([clear_cache/2, should_clear_cache/2, get_hooks_throttling_config/1, wait_for_cache_dump/0]).
-export([delete_old_keys/2]).
-export([get_cache_uuid/2, decode_uuid/1, cache_to_task_level/1]).
-export([clear/3, clear_links/3]).
-export([save_consistency_info/3, save_consistency_info_direct/3, consistency_restored/2, save_consistency_restored_info/3,
  begin_consistency_restoring/2, begin_consistency_restoring/3, end_consistency_restoring/2, end_consistency_restoring/3,
  end_consistency_restoring/4, check_cache_consistency/2, check_cache_consistency/3, check_cache_consistency_direct/2,
  check_cache_consistency_direct/3, consistency_info_lock/3, init_consistency_info/2]).
-export([throttle/1, throttle/2, throttle_del/1, throttle_del/2, get_idle_timeout/0,
  configure_throttling/0, plan_next_throttling_check/0]).
% for tests
-export([send_after/3]).

-define(CLEAR_BATCH_SIZE, 50).
-define(MNESIA_THROTTLING_KEY, mnesia_throttling).
-define(MEMORY_PROC_IDLE_KEY, throttling_idle_time).
-define(MNESIA_THROTTLING_DATA_KEY, <<"mnesia_throttling_data">>).
-define(THROTTLING_ERROR, {error, load_to_high}).

-define(BLOCK_THROTTLING, 3).
-define(LIMIT_THROTTLING, 2).
-define(CONFIG_THROTTLING, 1).
-define(NO_THROTTLING, 0).

-define(LEVEL_OVERRIDE(Level), [{level, Level}]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Limits operation performance if needed.
%% @end
%%--------------------------------------------------------------------
-spec throttle(ModelName :: model_behaviour:model_type()) -> ok | ?THROTTLING_ERROR.
throttle(ModelName) ->
  case lists:member(ModelName, datastore_config:throttled_models()) of
    true ->
      case application:get_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY) of
        {ok, V} ->
          case V of
            ok ->
              ok;
            {throttle, Time, _} ->
              timer:sleep(Time),
              ok;
            {overloaded, _} ->
              ?THROTTLING_ERROR
          end;
        _ ->
          ok
      end;
    _ ->
      ok
  end.

%%--------------------------------------------------------------------
%% @doc
%% Limits operation performance if needed.
%% @end
%%--------------------------------------------------------------------
-spec throttle(ModelName :: model_behaviour:model_type(), TmpAns) -> TmpAns | ?THROTTLING_ERROR when
  TmpAns :: term().
% TODO - delete when local cache is refactored
throttle(ModelName, ok) ->
  throttle(ModelName);
throttle(_ModelName, TmpAns) ->
  TmpAns.

%%--------------------------------------------------------------------
%% @doc
%% Limits delete operation performance if needed.
%% @end
%%--------------------------------------------------------------------
-spec throttle_del(ModelName :: model_behaviour:model_type()) -> ok | ?THROTTLING_ERROR.
throttle_del(ModelName) ->
  case lists:member(ModelName, datastore_config:throttled_models()) of
    true ->
      case application:get_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY) of
        {ok, V} ->
          case V of
            ok ->
              ok;
            {throttle, Time, true} ->
              timer:sleep(Time),
              ok;
            {throttle, _, _} ->
              ok;
            {overloaded, true} ->
              ?THROTTLING_ERROR;
            {overloaded, _} ->
              ok
          end;
        _ ->
          ok
      end;
    _ ->
      ok
  end.

%%--------------------------------------------------------------------
%% @doc
%% Limits delete operation performance if needed.
%% @end
%%--------------------------------------------------------------------
-spec throttle_del(ModelName :: model_behaviour:model_type(), TmpAns) -> TmpAns | ?THROTTLING_ERROR when
  TmpAns :: term().
% TODO - delete when local cache is refactored
throttle_del(ModelName, ok) ->
  throttle_del(ModelName);
throttle_del(_ModelName, TmpAns) ->
  TmpAns.

%%--------------------------------------------------------------------
%% @doc
%% Returns timeout after which memory store will be terminated.
%% @end
%%--------------------------------------------------------------------
-spec get_idle_timeout() -> non_neg_integer().
get_idle_timeout() ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME, ?MEMORY_PROC_IDLE_KEY) of
    {ok, IdleTimeout} ->
      IdleTimeout;
    _ ->
      {ok, Timeout} = application:get_env(?CLUSTER_WORKER_APP_NAME, memory_store_idle_timeout_ms),
      Timeout
  end.

%%--------------------------------------------------------------------
%% @doc
%% Configures throttling settings.
%% @end
%%--------------------------------------------------------------------
-spec configure_throttling() -> ok.
configure_throttling() ->
  Self = self(),
  spawn(fun() ->
    CheckInterval = try
      % TODO - use info abut failed batch saves and length of queue to pool of processes dumping to disk
      {MemAction, MemoryUsage} = verify_memory(),
      {TPAction, ProcNum} = verify_tp(),
      {DBAction, QueueSize} = verify_db(),
      Action = max(max(MemAction, TPAction), DBAction),

      case Action of
        ?BLOCK_THROTTLING ->
          application:set_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY,
            {overloaded, MemAction =:= ?NO_THROTTLING}),

          {ok, NewProcNum} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_block_memory_proc_number),
          tp:set_processes_limit(NewProcNum),

          ?info("Throttling: overload mode started, mem: ~p, tp proc num ~p, couch queue size: ~p",
            [MemoryUsage, ProcNum, QueueSize]),
          plan_next_throttling_check(true);
        _ ->
          Oldthrottling = case application:get_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY) of
            {ok, V} ->
              case V of
                {throttle, Time, _} ->
                  Time;
                Other ->
                  Other
              end;
            _ ->
              ok
          end,

          case {Action, Oldthrottling} of
            {A, {overloaded, _}} when A < ?LIMIT_THROTTLING ->
              {ok, NewProcNum} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_max_memory_proc_number),
              tp:set_processes_limit(NewProcNum);
            _ ->
              ok
          end,

          case {Action, Oldthrottling} of
            {?NO_THROTTLING, ok} ->
              ?debug("Throttling: no config needed, mem: ~p, tp proc num ~p, couch queue size: ~p",
                [MemoryUsage, ProcNum, QueueSize]),
              plan_next_throttling_check();
            {?NO_THROTTLING, _} ->
              application:unset_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY),
              ok = node_management:delete(?MNESIA_THROTTLING_DATA_KEY),
              ?info("Throttling: stop, mem: ~p, tp proc num ~p, couch queue size: ~p",
                [MemoryUsage, ProcNum, QueueSize]),
              plan_next_throttling_check();
            {?CONFIG_THROTTLING, ok} ->
              ?debug("Throttling: no config needed, mem: ~p, tp proc num ~p, couch queue size: ~p",
                [MemoryUsage, ProcNum, QueueSize]),
              plan_next_throttling_check(true);
            {?LIMIT_THROTTLING, {overloaded, true}} when MemAction > ?NO_THROTTLING ->
              application:set_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY, {overloaded, false}),
              ?info("Throttling: continue overload, mem: ~p, tp proc num ~p, couch queue size: ~p",
                [MemoryUsage, ProcNum, QueueSize]),
              plan_next_throttling_check(true);
            {?LIMIT_THROTTLING, {overloaded, _}} ->
              ?info("Throttling: continue overload, mem: ~p, tp proc num ~p, couch queue size: ~p",
                [MemoryUsage, ProcNum, QueueSize]),
              plan_next_throttling_check(true);
            _ ->
              TimeBase = case Oldthrottling of
                OT when is_integer(OT) ->
                  OT;
                _ ->
                  {ok, DefaultTimeBase} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_base_time_ms),
                  DefaultTimeBase
              end,

              {OldProcNum, OldQueueSize, OldMemory, LastInterval} =
                case node_management:get(?MNESIA_THROTTLING_DATA_KEY) of
                {ok, #document{value = #node_management{value = TD}}} ->
                  TD;
                {error, {not_found, _}} ->
                  {0, 0, 0, 0}
              end,

              TPMultip = (TPAction > 0) andalso (ProcNum > OldProcNum),
              MemoryMultip = (MemAction > 0) andalso (MemoryUsage > OldMemory),
              DumpMultip = (DBAction > 0) andalso (QueueSize > OldQueueSize),
              ThrottlingTime = case
                {TPMultip or MemoryMultip or DumpMultip,
                Action} of
                {true, _} ->
                  TB2 = 2 * TimeBase,
                  {ok, MaxTime} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_max_time_ms),
                  case TB2 > MaxTime of
                    true ->
                      MaxTime;
                    _ ->
                      TB2
                  end;
                {_, ?CONFIG_THROTTLING} ->
                  max(round(TimeBase / 2), 1);
                _ ->
                  TimeBase
              end,

              {ok, MemRatioThreshold} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_block_mem_error_ratio),
              NextCheck = plan_next_throttling_check(MemoryUsage-OldMemory, MemRatioThreshold-MemoryUsage, LastInterval),

              application:set_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY,
                {throttle, ThrottlingTime, MemAction =:= ?NO_THROTTLING}),
              {ok, _} = node_management:save(#document{key = ?MNESIA_THROTTLING_DATA_KEY,
                value = #node_management{value = {ProcNum, QueueSize, MemoryUsage, NextCheck}}}),

              ?info("Throttling: delay ~p ms used, mem: ~p, tp proc num ~p, couch queue size: ~p",
                [ThrottlingTime, MemoryUsage, ProcNum, QueueSize]),

              NextCheck
          end
      end
    catch
      E1:E2 ->
        ?error_stacktrace("Error during throttling configuration: ~p:~p", [E1, E2]),
        plan_next_throttling_check()
    end,
    caches_controller:send_after(CheckInterval, Self, {timer, configure_throttling})
  end),
  ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns time after which next throttling config should start.
%% @end
%%--------------------------------------------------------------------
-spec plan_next_throttling_check() -> non_neg_integer().
plan_next_throttling_check() ->
  plan_next_throttling_check(false).

%%--------------------------------------------------------------------
%% @doc
%% Checks if memory should be cleared.
%% @end
%%--------------------------------------------------------------------
-spec should_clear_cache(MemUsage :: float(), ErlangMemUsage :: [{atom(), non_neg_integer()}]) -> boolean().
should_clear_cache(MemUsage, ErlangMemUsage) ->
  {ok, TargetMemUse} = application:get_env(?CLUSTER_WORKER_APP_NAME, node_mem_ratio_to_clear_cache),
  {ok, TargetErlangMemUse} = application:get_env(?CLUSTER_WORKER_APP_NAME, erlang_mem_to_clear_cache_mb),
  MemToCompare = proplists:get_value(system, ErlangMemUsage, 0),
  (MemUsage >= TargetMemUse) andalso (MemToCompare >= TargetErlangMemUse * 1024 * 1024).

%%--------------------------------------------------------------------
%% @doc
%% Clears local cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_local_cache(Aggressive :: boolean()) -> ok | mem_usage_too_high | cannot_check_mem_usage.
clear_local_cache(Aggressive) ->
  clear_cache(Aggressive, locally_cached).

%%--------------------------------------------------------------------
%% @doc
%% Clears global cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_global_cache(Aggressive :: boolean()) -> ok | mem_usage_too_high | cannot_check_mem_usage.
clear_global_cache(Aggressive) ->
  clear_cache(Aggressive, globally_cached).

%%--------------------------------------------------------------------
%% @doc
%% Clears cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_cache(Aggressive :: boolean(), StoreType :: globally_cached | locally_cached) ->
  ok | mem_usage_too_high | cannot_check_mem_usage.
clear_cache(true, StoreType) ->
  clear_cache_by_time_windows(StoreType, [timer:minutes(10), 0]);

clear_cache(_, StoreType) ->
  clear_cache_by_time_windows(StoreType, [timer:hours(7*24), timer:hours(24), timer:hours(1)]).

%%--------------------------------------------------------------------
%% @doc
%% Clears cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_cache_by_time_windows(StoreType :: globally_cached | locally_cached, TimeWindows :: list()) ->
  ok | mem_usage_too_high | cannot_check_mem_usage.
clear_cache_by_time_windows(_StoreType, []) ->
  mem_usage_too_high;

clear_cache_by_time_windows(StoreType, [TimeWindow | Windows]) ->
  caches_controller:delete_old_keys(StoreType, TimeWindow),
  {ok, DumpDelay} = application:get_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms),
  {ok, AggTime} = application:get_env(?CLUSTER_WORKER_APP_NAME, datastore_pool_batch_delay),
  {ok, CTTRS} = application:get_env(?CLUSTER_WORKER_APP_NAME, clearing_time_to_refresh_stats),
  SleepTime = DumpDelay + AggTime + CTTRS,
  timer:sleep(SleepTime),
  case monitoring:get_memory_stats() of
    [{<<"mem">>, MemUsage}] ->
      ErlangMemUsage = erlang:memory(),
      case should_clear_cache(MemUsage, ErlangMemUsage) of
        true ->
          clear_cache_by_time_windows(StoreType, Windows);
        _ ->
          ok
      end;
    _ ->
      ?warning("Not able to check memory usage"),
      cannot_check_mem_usage
  end.

%%--------------------------------------------------------------------
%% @doc
%% Provides hooks configuration on the basis of models list.
%% @end
%%--------------------------------------------------------------------
-spec get_hooks_throttling_config(Models :: list()) -> list().
get_hooks_throttling_config(Models) ->
  Methods = [save, delete, update, create, create_or_update,
    add_links, set_links, create_link, delete_links],
  lists:foldl(fun(Model, Ans) ->
    ModelConfig = lists:map(fun(Method) ->
      {Model, Method}
    end, Methods),
    ModelConfig ++ Ans
  end, [], Models).

%%--------------------------------------------------------------------
%% @doc
%% Generates uuid on the basis of key and model name.
%% @end
%%--------------------------------------------------------------------
-spec get_cache_uuid(Key :: datastore:key() | {datastore:ext_key(), datastore:link_name(), cache_controller_link_key},
    ModelName :: model_behaviour:model_type()) -> binary().
get_cache_uuid(Key, ModelName) ->
  base64:encode(term_to_binary({ModelName, Key})).

%%--------------------------------------------------------------------
%% @doc
%% Decodes uuid to key and model name.
%% @end
%%--------------------------------------------------------------------
-spec decode_uuid(binary()) -> {ModelName :: model_behaviour:model_type(),
  Key :: datastore:key() | {datastore:ext_key(), datastore:link_name(), cache_controller_link_key}}.
decode_uuid(Uuid) ->
  binary_to_term(base64:decode(Uuid)).

%%--------------------------------------------------------------------
%% @doc
%% Clears old documents from memory.
%% @end
%%--------------------------------------------------------------------
-spec delete_old_keys(StoreType :: globally_cached | locally_cached, TimeWindow :: integer()) -> ok.
delete_old_keys(globally_cached, TimeWindow) ->
  delete_old_keys(global_only, datastore_config:global_caches(), TimeWindow);

delete_old_keys(locally_cached, TimeWindow) ->
  delete_old_keys(local_only, datastore_config:local_caches(), TimeWindow).

%%--------------------------------------------------------------------
%% @doc
%% Waits for dumping cache to disk
%% @end
%%--------------------------------------------------------------------
-spec wait_for_cache_dump() ->
  ok | dump_error.
% TODO - use info from tp_servers
wait_for_cache_dump() ->
  {ok, Delay} = application:get_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms),
  wait_for_cache_dump(round(Delay/1000) + 10, {0, 0}).

%%--------------------------------------------------------------------
%% @doc
%% Waits for dumping cache to disk. Arguments are number of repeats left and number of
%% docs left in caches {global, local} (to check if documents are being dumped at the moment).
%% @end
%%--------------------------------------------------------------------
-spec wait_for_cache_dump(N :: integer(), {GSize :: integer(), LSize :: integer()}) ->
  ok | dump_error.
wait_for_cache_dump(0, _) ->
  dump_error;
wait_for_cache_dump(_N, {_GSize, _LSize}) ->
  % TODO - implementation for new cache
  ok.

%%--------------------------------------------------------------------
%% @doc
%% Translates cache name to task level.
%% @end
%%--------------------------------------------------------------------
-spec cache_to_task_level(ModelName :: atom()) -> task_manager:level().
cache_to_task_level(ModelName) ->
  case ModelName of
    locally_cached_record -> ?NODE_LEVEL;
    locally_cached_sync_record -> ?NODE_LEVEL;
    _ -> ?CLUSTER_LEVEL
  end.

%%--------------------------------------------------------------------
%% @doc
%% Clears document from memory.
%% @end
%%--------------------------------------------------------------------
-spec clear(Level :: datastore:store_level(), ModelName :: atom(),
    Key :: datastore:ext_key()) -> ok | datastore:generic_error().
clear(_, ModelName, Key) ->
  ModelConfig = ModelName:model_init(),
  ?MEMORY_DRIVER:call(clear, ModelConfig, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Clears link documents from memory.
%% @end
%%--------------------------------------------------------------------
-spec clear_links(Level :: datastore:store_level(), ModelName :: atom(),
    Key :: datastore:ext_key()) -> ok | datastore:generic_error().
clear_links(_, ModelName, Key) ->
  ModelConfig = ModelName:model_init(),
  ?MEMORY_DRIVER:call(clear_links, ModelConfig, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Saves information about restoring doc to memory
%% @end
%%--------------------------------------------------------------------
-spec save_consistency_restored_info(Level :: datastore:store_level(), Key :: datastore:ext_key(),
    ClearedName :: datastore:key() | datastore:link_name()) ->
  boolean() | datastore:create_error().
save_consistency_restored_info(Level, Key, ClearedName) ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_proc_terminate_clear_memory) of
    {ok, true} ->
      true;
    _ ->
      UpdateFun = fun
        (#cache_consistency_controller{status = not_monitored}) ->
          {error, clearing_not_monitored};
        (#cache_consistency_controller{cleared_list = CL, restore_counter = RC} = Record) ->
          {ok, Record#cache_consistency_controller{cleared_list = lists:delete(ClearedName, CL),
            restore_counter = RC + 1}}
      end,

      case cache_consistency_controller:update(Level, Key, UpdateFun) of
        {ok, _} ->
          true;
        {error, clearing_not_monitored} ->
          true;
        {error, {not_found, _}} ->
          true;
        Other ->
          ?error_stacktrace("Cannot save consistency_restored_info ~p, error: ~p", [{Level, Key, ClearedName}, Other]),
          false
      end
  end.

%%--------------------------------------------------------------------
%% @doc
%% Inits information about cache consistency
%% @end
%%--------------------------------------------------------------------
-spec init_consistency_info(Level :: datastore:store_level(), Key :: datastore:ext_key()) ->
  ok | datastore:create_error().
init_consistency_info(Level, Key) ->
  Doc = #document{key = Key, value = #cache_consistency_controller{}},

  case cache_consistency_controller:create(Level, Doc) of
    {ok, _} ->
      ok;
    {error, already_exists} ->
      ok;
    Other ->
      ?error_stacktrace("Cannot init consistency_info ~p, error: ~p", [{Level, Key}, Other]),
      Other
  end.

%%--------------------------------------------------------------------
%% @doc
%% Marks that consistency will be restored
%% @end
%%--------------------------------------------------------------------
-spec begin_consistency_restoring(Level :: datastore:store_level(), Key :: datastore:ext_key()) ->
  {ok, datastore:ext_key()} | datastore:create_error().
begin_consistency_restoring(Level, Key) ->
  begin_consistency_restoring(Level, Key, self()).

%%--------------------------------------------------------------------
%% @doc
%% Marks that consistency will be restored
%% @end
%%--------------------------------------------------------------------
-spec begin_consistency_restoring(Level :: datastore:store_level(), Key :: datastore:ext_key(), Pid :: pid()) ->
  {ok, datastore:ext_key()} | datastore:create_error().
begin_consistency_restoring(Level, Key, Pid) ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_proc_terminate_clear_memory) of
    {ok, true} ->
      {ok, Key};
    _ ->
      UpdateFun = fun
                    (#cache_consistency_controller{cleared_list = [], status = {restoring, RPid},
                      restore_counter = RC} = Record) ->
                      case process_alive(RPid) of
                        true ->
                          {error, restoring_process_in_progress};
                        _ ->
                          {ok, Record#cache_consistency_controller{status = {restoring, Pid},
                            restore_counter = RC + 1}}
                      end;
                    (#cache_consistency_controller{cleared_list = [], status = ok}) ->
                      {error, consistency_ok};
                    (Record = #cache_consistency_controller{restore_counter = RC}) ->
                      {ok, Record#cache_consistency_controller{status = {restoring, Pid},
                        restore_counter = RC + 1}}
                  end,
      Doc = #document{key = Key, value = #cache_consistency_controller{status = {restoring, Pid},
        restore_counter = 1}},

      cache_consistency_controller:create_or_update(Level, Doc, UpdateFun)
  end.

%%--------------------------------------------------------------------
%% @doc
%% Marks that consistency restoring has ended
%% @end
%%--------------------------------------------------------------------
-spec end_consistency_restoring(Level :: datastore:store_level(), Key :: datastore:ext_key()) ->
  {ok, datastore:ext_key()} | datastore:create_error().
end_consistency_restoring(Level, Key) ->
  end_consistency_restoring(Level, Key, self()).

%%--------------------------------------------------------------------
%% @doc
%% Marks that consistency restoring has ended
%% @end
%%--------------------------------------------------------------------
-spec end_consistency_restoring(Level :: datastore:store_level(), Key :: datastore:ext_key(), Pid :: pid()) ->
  {ok, datastore:ext_key()} | datastore:create_error().
end_consistency_restoring(Level, Key, Pid) ->
  end_consistency_restoring(Level, Key, Pid, ok).

%%--------------------------------------------------------------------
%% @doc
%% Marks that consistency restoring has ended
%% @end
%%--------------------------------------------------------------------
-spec end_consistency_restoring(Level :: datastore:store_level(), Key :: datastore:ext_key(), Pid :: pid(),
    V :: ok | not_monitored) -> {ok, datastore:ext_key()} | datastore:create_error().
end_consistency_restoring(Level, Key, Pid, V) ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_proc_terminate_clear_memory) of
    {ok, true} ->
      {ok, Key};
    _ ->
      UpdateFun = fun
                    (#cache_consistency_controller{cleared_list = [], status = {restoring, RPid}} = Record) ->
                      case RPid of
                        Pid ->
                          {ok, Record#cache_consistency_controller{status = V}};
                        _ ->
                          {error, interupted}
                      end;
                    (#cache_consistency_controller{status = {restoring, RPid}} = Record) ->
                      case RPid of
                        Pid ->
                          {ok, Record#cache_consistency_controller{cleared_list = [], status = not_monitored}};
                        _ ->
                          {error, interupted}
                      end;
                    (_) ->
                      {error, interupted}
                  end,
      Doc = #document{key = Key, value = #cache_consistency_controller{status = not_monitored}},

      cache_consistency_controller:create_or_update(Level, Doc, UpdateFun)
  end.

%%--------------------------------------------------------------------
%% @doc
%% Marks that consistency restoring has ended
%% @end
%%--------------------------------------------------------------------
-spec consistency_restored(Driver :: atom(), Key :: datastore:ext_key()) ->
  {ok, datastore:ext_key()} | datastore:create_error().
consistency_restored(Driver, Key) ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_proc_terminate_clear_memory) of
    {ok, true} ->
      {ok, Key};
    _ ->
      UpdateFun = fun(Record) ->
          {ok, Record#cache_consistency_controller{status = ok}}
      end,
      Doc = #document{key = Key, value = #cache_consistency_controller{restore_counter = 1}},

      Driver:create_or_update(cache_consistency_controller:model_init(), Doc, UpdateFun)
  end.

%%--------------------------------------------------------------------
%% @doc
%% Checks consistency status of Key.
%% @end
%%--------------------------------------------------------------------
-spec check_cache_consistency(Level :: datastore:store_level(), Key :: datastore:ext_key()) ->
  {ok, non_neg_integer(), non_neg_integer()}
  | {monitored, [datastore:key() | datastore:link_name()], non_neg_integer(), non_neg_integer()}
  | not_monitored | no_return().
check_cache_consistency(Level, Key) ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_proc_terminate_clear_memory) of
    {ok, true} ->
      not_monitored;
    _ ->
      case cache_consistency_controller:get(Level, Key) of
        % TODO - use clear time instead of counter
        {ok, #document{value = #cache_consistency_controller{cleared_list = [], status = ok, clearing_counter = CC,
          restore_counter = RC}}} ->
          {ok, CC, RC};
        {ok, #document{value = #cache_consistency_controller{cleared_list = CL, status = ok, clearing_counter = CC,
          restore_counter = RC}}} ->
          {monitored, CL, CC, RC};
        {ok, _} ->
          not_monitored;
        {error, {not_found, _}} ->
          not_monitored
      end
  end.

%%--------------------------------------------------------------------
%% @doc
%% Checks consistency status of Key.
%% @end
%%--------------------------------------------------------------------
-spec check_cache_consistency_direct(Driver :: atom(), Key :: datastore:ext_key()) ->
  {ok, non_neg_integer(), non_neg_integer()}
  | {monitored, [datastore:key() | datastore:link_name()], non_neg_integer(), non_neg_integer()}
  | not_monitored | no_return().
check_cache_consistency_direct(Driver, Key) ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_proc_terminate_clear_memory) of
    {ok, true} ->
      not_monitored;
    _ ->
      case Driver:get(cache_consistency_controller:model_init(), Key) of
        % TODO - use clear time instead of counter
        {ok, #document{value = #cache_consistency_controller{cleared_list = [], status = ok, clearing_counter = CC,
          restore_counter = RC}}} ->
          {ok, CC, RC};
        {ok, #document{value = #cache_consistency_controller{cleared_list = CL, status = ok, clearing_counter = CC,
          restore_counter = RC}}} ->
          {monitored, CL, CC, RC};
        {ok, _} ->
          not_monitored;
        {error, {not_found, _}} ->
          not_monitored
      end
  end.

%%--------------------------------------------------------------------
%% @doc
%% Checks consistency status of Key. If consistency document is not found, seeks document for whole model to analyse.
%% @end
%%--------------------------------------------------------------------
-spec check_cache_consistency(Level :: datastore:store_level(), Key :: datastore:ext_key(),
    NotFoundCheck :: datastore:ext_key()) -> {ok, non_neg_integer(), non_neg_integer()}
  | {monitored, [datastore:key() | datastore:link_name()], non_neg_integer(), non_neg_integer()}
  | not_monitored | no_return().
% TODO - cache consistency is not save through datastore (it is part of management mechanism ad should be managed
% only by memory_store_driver), only exception - data for models
check_cache_consistency(Level, Key, NotFoundCheck) ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_proc_terminate_clear_memory) of
    {ok, true} ->
      not_monitored;
    _ ->
      case cache_consistency_controller:get(Level, Key) of
        {ok, #document{value = #cache_consistency_controller{cleared_list = [], status = ok, clearing_counter = CC,
          restore_counter = RC}}} ->
          {ok, CC, RC};
        {ok, #document{value = #cache_consistency_controller{cleared_list = CL, status = ok, clearing_counter = CC,
          restore_counter = RC}}} ->
          {monitored, CL, CC, RC};
        {ok, _} ->
          not_monitored;
        {error, {not_found, _}} ->
          ModelNode = consistent_hasing:get_node(NotFoundCheck),
          case rpc:call(ModelNode, caches_controller, check_cache_consistency, [Level, NotFoundCheck]) of
            {ok, 0, 0} ->
              init_consistency_info(Level, Key),
              {ok, 0, 0};
            _ ->
              not_monitored
          end
      end
  end.

%%--------------------------------------------------------------------
%% @doc
%% Checks consistency status of Key. If consistency document is not found, seeks document for whole model to analyse.
%% @end
%%--------------------------------------------------------------------
-spec check_cache_consistency_direct(Driver :: atom(), Key :: datastore:ext_key(),
    NotFoundCheck :: datastore:ext_key()) -> {ok, non_neg_integer(), non_neg_integer()}
| {monitored, [datastore:key() | datastore:link_name()], non_neg_integer(), non_neg_integer()}
| not_monitored | no_return().
% TODO - cache consistency is not save through datastore (it is part of management mechanism ad should be managed
% only by memory_store_driver), only exception - data for models
check_cache_consistency_direct(Driver, Key, NotFoundCheck) ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_proc_terminate_clear_memory) of
    {ok, true} ->
      not_monitored;
    _ ->
      case Driver:get(cache_consistency_controller:model_init(), Key) of
        {ok, #document{value = #cache_consistency_controller{cleared_list = [], status = ok, clearing_counter = CC,
          restore_counter = RC}}} ->
          {ok, CC, RC};
        {ok, #document{value = #cache_consistency_controller{cleared_list = CL, status = ok, clearing_counter = CC,
          restore_counter = RC}}} ->
          {monitored, CL, CC, RC};
        {ok, _} ->
          not_monitored;
        {error, {not_found, _}} ->
          % TODO - use more universal method
          #model_config{link_store_level = Level0} = NotFoundCheck:model_init(),
          Level = memory_store_driver:main_level(Level0),
          case caches_controller:check_cache_consistency(Level, NotFoundCheck) of
            {ok, 0, 0} ->
              init_consistency_info(Level, Key),
              {ok, 0, 0};
            _ ->
              not_monitored
          end
      end
  end.


%%--------------------------------------------------------------------
%% @doc
%% Critical section for consistency info.
%% @end
%%--------------------------------------------------------------------
-spec consistency_info_lock(Key :: datastore:ext_key(), ClearedName :: datastore:key() | datastore:link_name(),
    Fun :: fun(() -> term())) -> term().
consistency_info_lock(Key, ClearedName, Fun) ->
  critical_section:run([?MODULE, consistency_info, {Key, ClearedName}], Fun).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Saves information about clearing doc from memory
%% @end
%%--------------------------------------------------------------------
-spec save_consistency_info(Level :: datastore:store_level(), Key :: datastore:ext_key(),
    ClearedName :: datastore:key() | datastore:link_name()) ->
  boolean() | datastore:create_error().
save_consistency_info(Level, Key, ClearedName) ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_proc_terminate_clear_memory) of
    {ok, true} ->
      true;
    _ ->
      UpdateFun = fun
        (#cache_consistency_controller{status = not_monitored}) ->
          {error, clearing_not_monitored};
        (#cache_consistency_controller{cleared_list = CL, clearing_counter = CC} = Record) ->
          case length(CL) >= ?CLEAR_MONITOR_MAX_SIZE of
            true ->
              {ok, Record#cache_consistency_controller{cleared_list = [], status = not_monitored,
                clearing_counter = CC + 1}}
    %%        ;
    %%        _ ->
    %%          case lists:member(ClearedName, CL) of
    %%            true ->
    %%              {error, already_cleared};
    %%            _ ->
    %%              {ok, Record#cache_consistency_controller{cleared_list = [ClearedName | CL],
    %%                clearing_counter = CC + 1}}
    %%          end
          end
      end,

      Doc = #document{key = Key, value = #cache_consistency_controller{status = not_monitored, clearing_counter = 1}},
      case cache_consistency_controller:create_or_update(Level, Doc, UpdateFun) of
        {ok, _} ->
          true;
        {error, clearing_not_monitored} ->
          true;
    %%    {error, already_cleared} ->
    %%      true;
        Other ->
          ?error_stacktrace("Cannot save consistency_info ~p, error: ~p", [{Level, Key, ClearedName}, Other]),
          false
      end
  end.

%%--------------------------------------------------------------------
%% @doc
%% Saves information about clearing doc from memory
%% @end
%%--------------------------------------------------------------------
-spec save_consistency_info_direct(Driver :: atom(), Key :: datastore:ext_key(),
    ClearedName :: datastore:key() | datastore:link_name()) ->
  boolean() | datastore:create_error().
save_consistency_info_direct(Driver, Key, ClearedName) ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_proc_terminate_clear_memory) of
    {ok, true} ->
      true;
    _ ->
      case Driver:delete(cache_consistency_controller:model_init(), Key, ?PRED_ALWAYS) of
        ok ->
          true;
        Other ->
          ?error_stacktrace("Cannot save consistency_info ~p, error: ~p", [{Driver, Key, ClearedName}, Other]),
          false
      end
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Clears old documents from memory.
%% @end
%%--------------------------------------------------------------------
-spec delete_old_keys(Level :: global_only | local_only, Caches :: list(), TimeWindow :: integer()) -> ok.
% TODO - clear links
delete_old_keys(Level, Caches, _TimeWindow) ->
  lists:foreach(fun(Cache) ->
    ClearFun = fun
      ('$end_of_table', {0, _, undefined}) ->
        {abort, 0};
      ('$end_of_table', {Count, BatchNum, UuidToDel}) ->
        Master = self(),
        spawn(fun() ->
          clear(Level, Cache, UuidToDel),
          clear_links(Level, Cache, UuidToDel),
          Master ! {doc_cleared, BatchNum}
        end),
        ToCount = case Count rem ?CLEAR_BATCH_SIZE of
          0 ->
            ?CLEAR_BATCH_SIZE;
          TC ->
            TC
        end,
        count_clear_acc(ToCount, BatchNum),
        {abort, Count};
      (#document{key = Uuid}, {0, BatchNum, undefined}) ->
        {next, {1, BatchNum, Uuid}};
      (#document{key = Uuid}, {Count, BatchNum0, UuidToDel}) ->
        Master = self(),
        spawn(fun() ->
          clear(Level, Cache, UuidToDel),
          clear_links(Level, Cache, UuidToDel),
          Master ! {doc_cleared, BatchNum0}
        end),
        BatchNum = case Count rem ?CLEAR_BATCH_SIZE of
          0 ->
            count_clear_acc(?CLEAR_BATCH_SIZE, BatchNum0),
            BatchNum0 + 1;
          _ ->
            BatchNum0
        end,
        {next, {Count + 1, BatchNum, Uuid}}
    end,
    {ok, _} = list_old_keys(Level, ClearFun, Cache)
  end, Caches),

  % TODO - clear cache_consistency_controller
  ok.

 %%--------------------------------------------------------------------
%% @private
%% @doc
%% Lists keys and executes fun for each key. Reruns listing if aborted.
%% @end
%%--------------------------------------------------------------------
-spec list_old_keys(Level :: global_only | local_only, ClearFun :: datastore:list_fun(),
    ModelName :: model_behaviour:model_type()) -> {ok, term()} | datastore:generic_error() | no_return().
list_old_keys(Level, ClearFun, Model) ->
  case model:execute_with_default_context(Model, list, [ClearFun, {0, 0, undefined}],
    ?LEVEL_OVERRIDE(Level)) of
    {ok, _} = Ans ->
      Ans;
    {error, {aborted, R}} ->
      ?warning("Cache cleaning aborted: ~p", [R]),
      list_old_keys(Level, ClearFun, Model);
    Other ->
      Other
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Counts acc clearing answers.
%% @end
%%--------------------------------------------------------------------
-spec count_clear_acc(Count :: integer(), BatchNum :: integer()) -> ok.
count_clear_acc(0, _) ->
  ok;
count_clear_acc(Count, BatchNum) ->
  receive
    {doc_cleared, BatchNum} ->
      count_clear_acc(Count - 1, BatchNum)
  after timer:minutes(1) ->
    ?warning("Not cleared old cache for batch num ~p, current count ~p", [BatchNum, Count]),
    ok
  end.


%%--------------------------------------------------------------------
%% @doc
%% Checks poll of tp processes and recommends throttling action.
%% @end
%%--------------------------------------------------------------------
-spec verify_tp() ->
  {TPAction :: non_neg_integer(), ProcNum :: non_neg_integer()}.
verify_tp() ->
  {ok, StartNum} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_reduce_idle_time_memory_proc_number),
  {ok, DelayNum} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_start_memory_proc_number),
  {ok, MaxProcNum} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_max_memory_proc_number),

  ProcNum = tp:get_processes_number(),
  {ok, IdleTimeout} = application:get_env(?CLUSTER_WORKER_APP_NAME, memory_store_idle_timeout_ms),

  NewIdleTimeout = case ProcNum < StartNum of
    true ->
      ?debug("Throttling: idle timeout: ~p", [IdleTimeout]),
      IdleTimeout;
    _ ->
      {ok, MinIdleTimeout} = application:get_env(?CLUSTER_WORKER_APP_NAME, memory_store_min_idle_timeout_ms),
      NID = round(max(IdleTimeout * (DelayNum * 9 / 10 - ProcNum) / DelayNum, MinIdleTimeout)),
      ?info("Throttling: idle timeout: ~p", [NID]),
      NID
  end,
  application:set_env(?CLUSTER_WORKER_APP_NAME, ?MEMORY_PROC_IDLE_KEY, NewIdleTimeout),

  TPAction = case ProcNum of
    PN when PN >= (MaxProcNum * 4 / 5) ->
      ?BLOCK_THROTTLING;
    PN when PN >= DelayNum ->
      ?LIMIT_THROTTLING;
    DP when DP >= (DelayNum * 4 / 5) ->
      ?CONFIG_THROTTLING;
    _ ->
      ?NO_THROTTLING
  end,

  {TPAction, ProcNum}.

%%--------------------------------------------------------------------
%% @doc
%% Checks queue fo datastore_pool and recommends throttling action.
%% @end
%%--------------------------------------------------------------------
-spec verify_db() ->
  {DBAction :: non_neg_integer(), QueueSize :: non_neg_integer()}.
verify_db() ->
  {ok, Limit} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_db_queue_limit),
  {ok, Start} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_delay_db_queue_size),

  QueueSize = datastore_pool:request_queue_size(),

  DBAction = case QueueSize of
    DP when DP >= Limit ->
      ?BLOCK_THROTTLING;
    DP when DP >= Start ->
      ?LIMIT_THROTTLING;
    DP when DP >= Start/2 ->
      ?CONFIG_THROTTLING;
    _ ->
      ?NO_THROTTLING
  end,

  {DBAction, QueueSize}.

%%--------------------------------------------------------------------
%% @doc
%% Checks memory and recommends throttling action.
%% @end
%%--------------------------------------------------------------------
-spec verify_memory() ->
  {MemAction :: non_neg_integer(), MemoryUsage :: float()}.
verify_memory() ->
  {ok, MemRatioThreshold} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_block_mem_error_ratio),
  {ok, MemCleanRatioThreshold} = application:get_env(?CLUSTER_WORKER_APP_NAME, node_mem_ratio_to_clear_cache),

  MemoryUsage = case monitoring:get_memory_stats() of
                  [{<<"mem">>, MemUsage}] ->
                    MemUsage
                end,

  MemAction = case MemoryUsage of
                MU when MU >= MemRatioThreshold ->
                  ?BLOCK_THROTTLING;
                MU when MU >= (MemRatioThreshold + MemCleanRatioThreshold)/2 ->
                  ?LIMIT_THROTTLING;
                MU when MU >= MemCleanRatioThreshold ->
                  ?CONFIG_THROTTLING;
                _ ->
                  ?NO_THROTTLING
              end,

  {MemAction, MemoryUsage}.

%%--------------------------------------------------------------------
%% @doc
%% Returns time after which next throttling config should start.
%% @end
%%--------------------------------------------------------------------
-spec plan_next_throttling_check(Active :: boolean()) -> non_neg_integer().
plan_next_throttling_check(true) ->
  {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_active_check_interval_seconds),
  timer:seconds(Interval);
plan_next_throttling_check(_) ->
  {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_check_interval_seconds),
  timer:seconds(Interval).

%%--------------------------------------------------------------------
%% @doc
%% Returns time after which next throttling config should start.
%% Decreases interval if memory is growing too high.
%% @end
%%--------------------------------------------------------------------
-spec plan_next_throttling_check(MemoryChange :: float(), MemoryToStop :: float(), LastInterval :: integer()) ->
  non_neg_integer().
plan_next_throttling_check(_MemoryChange, _MemoryToStop, 0) ->
  plan_next_throttling_check(true);
plan_next_throttling_check(0.0, _MemoryToStop, _LastInterval) ->
  plan_next_throttling_check(true);
plan_next_throttling_check(MemoryChange, MemoryToStop, LastInterval) ->
  Default = plan_next_throttling_check(true),
  Corrected = round(LastInterval*MemoryToStop/MemoryChange),
  case (Corrected < Default) and (Corrected >= 0) of
    true ->
      Corrected;
    _ ->
      Default
  end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv erlang:send_after but enables mocking.
%% @end
%%--------------------------------------------------------------------
-spec send_after(CheckInterval :: non_neg_integer(), Master :: pid() | atom(), Message :: term()) -> reference().
send_after(CheckInterval, Master, Message) ->
  erlang:send_after(CheckInterval, Master, Message).

%%--------------------------------------------------------------------
%% @doc
%% Checks if process is alive.
%% @end
%%--------------------------------------------------------------------
-spec process_alive(Pid :: pid()) -> boolean().
process_alive(P) when node(P) == node() ->
  is_process_alive(P);
process_alive(P) ->
  rpc:call(node(P), erlang, is_process_alive, [P]).