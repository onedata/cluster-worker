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
-export([clear_cache/2, should_clear_cache/2, get_hooks_config/1, wait_for_cache_dump/0]).
-export([delete_old_keys/2, delete_all_keys/1]).
-export([get_cache_uuid/2, decode_uuid/1, cache_to_datastore_level/1, cache_to_task_level/1]).
-export([flush_all/2, flush/3, flush/4, clear/3, clear/4]).
-export([save_consistency_restored_info/3, begin_consistency_restoring/2, end_consistency_restoring/2,
  check_cache_consistency/2, consistency_info_lock/3, init_consistency_info/2]).
-export([throttle/1, throttle/2, throttle_del/2, configure_throttling/0, plan_next_throttling_check/0]).
% for tests
-export([send_after/3]).

-define(CLEAR_BATCH_SIZE, 50).
-define(MNESIA_THROTTLING_KEY, <<"mnesia_throttling">>).
-define(MNESIA_THROTTLING_DATA_KEY, <<"mnesia_throttling_data">>).
-define(THROTTLING_ERROR, {error, load_to_high}).

-define(BLOCK_THROTTLING, 3).
-define(LIMIT_THROTTLING, 2).
-define(CONFIG_THROTTLING, 1).
-define(NO_THROTTLING, 0).

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
      case node_management:get(?MNESIA_THROTTLING_KEY) of
        {ok, #document{value = #node_management{value = V}}} ->
          case V of
            ok ->
              ok;
            {throttle, Time, _} ->
              timer:sleep(Time),
              ok;
            {overloaded, _} ->
              ?THROTTLING_ERROR
          end;
        {error, {not_found, _}} ->
          ok;
        Error ->
          ?error("throttling error: ~p", [Error]),
          ?THROTTLING_ERROR
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
throttle(ModelName, ok) ->
  throttle(ModelName);
throttle(_ModelName, TmpAns) ->
  TmpAns.

%%--------------------------------------------------------------------
%% @doc
%% Limits delete operation performance if needed.
%% @end
%%--------------------------------------------------------------------
-spec throttle_del(ModelName :: model_behaviour:model_type(), TmpAns) -> TmpAns | ?THROTTLING_ERROR when
  TmpAns :: term().
throttle_del(ModelName, ok) ->
  case lists:member(ModelName, datastore_config:throttled_models()) of
    true ->
      case node_management:get(?MNESIA_THROTTLING_KEY) of
        {ok, #document{value = #node_management{value = V}}} ->
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
        {error, {not_found, _}} ->
          ok;
        Error ->
          ?error("throttling error: ~p", [Error]),
          ?THROTTLING_ERROR
      end;
    _ ->
      ok
  end;
throttle_del(_ModelName, TmpAns) ->
  TmpAns.

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
      {MemAction, MemoryUsage} = verify_memory(),
      {TaskAction, NewFailed, NewTasks} = verify_tasks(),
      Action = max(MemAction, TaskAction),

      case Action of
        ?BLOCK_THROTTLING ->
          {ok, _} = node_management:save(#document{key = ?MNESIA_THROTTLING_KEY,
            value = #node_management{value = {overloaded, MemAction =:= ?NO_THROTTLING}}}),
          ?info("Throttling: overload mode started, mem: ~p, failed tasks ~p, all tasks ~p",
            [MemoryUsage, NewFailed, NewTasks]),
          plan_next_throttling_check(true);
        _ ->
          Oldthrottling = case node_management:get(?MNESIA_THROTTLING_KEY) of
            {ok, #document{value = #node_management{value = V}}} ->
              case V of
                {throttle, Time, _} ->
                  Time;
                Other ->
                  Other
              end;
            {error, {not_found, _}} ->
              ok
          end,

          case {Action, Oldthrottling} of
            {?NO_THROTTLING, ok} ->
              ?debug("Throttling: no config needed, mem: ~p, failed tasks ~p, all tasks ~p",
                [MemoryUsage, NewFailed, NewTasks]),
              plan_next_throttling_check();
            {?NO_THROTTLING, _} ->
              ok = node_management:delete(?MNESIA_THROTTLING_KEY),
              ok = node_management:delete(?MNESIA_THROTTLING_DATA_KEY),
              ?info("Throttling: stop, mem: ~p, failed tasks ~p, all tasks ~p",
                [MemoryUsage, NewFailed, NewTasks]),
              plan_next_throttling_check();
            {?CONFIG_THROTTLING, ok} ->
              ?debug("Throttling: no config needed, mem: ~p, failed tasks ~p, all tasks ~p",
                [MemoryUsage, NewFailed, NewTasks]),
              plan_next_throttling_check(true);
            {?LIMIT_THROTTLING, {overloaded, true}} when MemAction > ?NO_THROTTLING ->
              {ok, _} = node_management:save(#document{key = ?MNESIA_THROTTLING_KEY,
                value = #node_management{value = {overloaded, false}}}),
              ?info("Throttling: continue overload, mem: ~p, failed tasks ~p, all tasks ~p",
                [MemoryUsage, NewFailed, NewTasks]),
              plan_next_throttling_check(true);
            {?LIMIT_THROTTLING, {overloaded, _}} ->
              ?info("Throttling: continue overload, mem: ~p, failed tasks ~p, all tasks ~p",
                [MemoryUsage, NewFailed, NewTasks]),
              plan_next_throttling_check(true);
            _ ->
              TimeBase = case Oldthrottling of
                OT when is_integer(OT) ->
                  OT;
                _ ->
                  {ok, DefaultTimeBase} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_base_time_ms),
                  DefaultTimeBase
              end,

              {OldFaild, OldTasks, OldMemory, LastInterval} = case node_management:get(?MNESIA_THROTTLING_DATA_KEY) of
                {ok, #document{value = #node_management{value = TD}}} ->
                  TD;
                {error, {not_found, _}} ->
                  {0, 0, 0, 0}
              end,

              ThrottlingTime = case
                {(NewFailed > OldFaild) or ((NewTasks + NewFailed) > (OldTasks + OldFaild)) or (MemoryUsage > OldMemory),
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

              {ok, _} = node_management:save(#document{key = ?MNESIA_THROTTLING_KEY,
                value = #node_management{value = {throttle, ThrottlingTime, MemAction =:= ?NO_THROTTLING}}}),
              {ok, _} = node_management:save(#document{key = ?MNESIA_THROTTLING_DATA_KEY,
                value = #node_management{value = {NewFailed, NewTasks, MemoryUsage, NextCheck}}}),

              ?info("Throttling: delay ~p ms used, mem: ~p, failed tasks ~p, all tasks ~p",
                [ThrottlingTime, MemoryUsage, NewFailed, NewTasks]),

              NextCheck
          end
      end
    catch
      E1:E2 ->
        ?error_stacktrace("Error during throtling configuration: ~p:~p", [E1, E2]),
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
  timer:sleep(1000), % time for system for mem info update
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
-spec get_hooks_config(Models :: list()) -> list().
get_hooks_config(Models) ->
  Methods = [save, get, exists, delete, update, create, create_or_update,
    fetch_link, add_links, set_links, create_link, delete_links],
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
%% Clears all documents from memory.
%% @end
%%--------------------------------------------------------------------
-spec delete_all_keys(StoreType :: globally_cached | locally_cached) -> ok | cleared.
delete_all_keys(globally_cached) ->
  delete_all_keys(global_only, datastore_config:global_caches());

delete_all_keys(locally_cached) ->
  delete_all_keys(local_only, datastore_config:local_caches()).

%%--------------------------------------------------------------------
%% @doc
%% Waits for dumping cache to disk
%% @end
%%--------------------------------------------------------------------
-spec wait_for_cache_dump() ->
  ok | dump_error.
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
wait_for_cache_dump(N, {GSize, LSize}) ->
  case {cache_controller:list_docs_to_be_dumped(?GLOBAL_ONLY_LEVEL),
    cache_controller:list_docs_to_be_dumped(?LOCAL_ONLY_LEVEL)} of
    {{ok, []}, {ok, []}} ->
      ok;
    {{ok, L1}, {ok, L2}} ->
      case {length(L1), length(L2)} of
        {GSize, LSize} ->
          timer:sleep(timer:seconds(1)),
          wait_for_cache_dump(N-1, {GSize, LSize});
        {GSize2, LSize2} ->
          timer:sleep(timer:seconds(1)),
          wait_for_cache_dump(N, {GSize2, LSize2})
      end;
    _ ->
      timer:sleep(timer:seconds(1)),
      wait_for_cache_dump(N-1, {GSize, LSize})
  end.

%%--------------------------------------------------------------------
%% @doc
%% Translates cache name to store level.
%% @end
%%--------------------------------------------------------------------
-spec cache_to_datastore_level(ModelName :: atom()) -> datastore:store_level().
cache_to_datastore_level(ModelName) ->
%%  case lists:member(ModelName, datastore_config:global_caches()) of
%%    true -> ?GLOBAL_ONLY_LEVEL;
%%    _ -> ?LOCAL_ONLY_LEVEL
%%  end.
  case ModelName of
    locally_cached_record -> ?LOCAL_ONLY_LEVEL;
    locally_cached_sync_record -> ?LOCAL_ONLY_LEVEL;
    _ -> ?GLOBAL_ONLY_LEVEL
  end.

%%--------------------------------------------------------------------
%% @doc
%% Translates cache name to task level.
%% @end
%%--------------------------------------------------------------------
-spec cache_to_task_level(ModelName :: atom()) -> task_manager:level().
cache_to_task_level(ModelName) ->
%%  case lists:member(ModelName, datastore_config:global_caches()) of
%%    true -> ?CLUSTER_LEVEL;
%%    _ -> ?NODE_LEVEL
%%  end.
  case ModelName of
    locally_cached_record -> ?NODE_LEVEL;
    locally_cached_sync_record -> ?NODE_LEVEL;
    _ -> ?CLUSTER_LEVEL
  end.

%%--------------------------------------------------------------------
%% @doc
%% Flushes all documents from memory to disk.
%% @end
%%--------------------------------------------------------------------
-spec flush_all(Level :: datastore:store_level(), ModelName :: atom()) -> ok.
flush_all(Level, ModelName) ->
  {ok, Keys} = cache_controller:list_docs_to_be_dumped(Level),
  lists:foreach(fun(Key) ->
    flush(Level, ModelName, Key)
  end, Keys).

%%--------------------------------------------------------------------
%% @doc
%% Flushes links from memory to disk.
%% @end
%%--------------------------------------------------------------------
-spec flush(Level :: datastore:store_level(), ModelName :: atom(),
    Key :: datastore:ext_key(), datastore:link_name() | all) -> ok | datastore:generic_error().
flush(Level, ModelName, Key, all) ->
  ModelConfig = ModelName:model_init(),
  AccFun = fun(LinkName, _, Acc) ->
    [LinkName | Acc]
  end,
  FullArgs = [ModelConfig, Key, AccFun, []],
  {ok, Links} = erlang:apply(datastore:level_to_driver(Level), foreach_link, FullArgs),
  lists:foldl(fun(Link, Acc) ->
    Ans = flush(Level, ModelName, Key, Link),
    case Ans of
      ok ->
        Acc;
      _ ->
        Ans
    end
  end, ok, Links);

flush(Level, ModelName, Key, Link) ->
  flush(Level, ModelName, cache_controller:link_cache_key(ModelName, Key, Link)).

%%--------------------------------------------------------------------
%% @doc
%% Flushes document from memory to disk.
%% @end
%%--------------------------------------------------------------------
-spec flush(Level :: datastore:store_level(), ModelName :: atom(),
    Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name(), cache_controller_link_key}) ->
  ok | datastore:generic_error().
flush(Level, ModelName, Key) ->
  ModelConfig = ModelName:model_init(),
  Uuid = get_cache_uuid(Key, ModelName),
  ToDo = cache_controller:choose_action(save, Level, ModelName, Key, Uuid, true),

  Ans = case ToDo of
          {ok, NewMethod, NewArgs} ->
            FullArgs = [ModelConfig | NewArgs],
            case erlang:apply(get_driver_module(?DISK_ONLY_LEVEL), NewMethod, FullArgs) of
              {error, already_updated} ->
                ok;
              FlushAns ->
                FlushAns
            end;
          {ok, non} ->
            ok;
          Other ->
            Other
        end,

  case Ans of
    {ok, _} ->
      ok;
    OtherAns -> OtherAns
  end.

%%--------------------------------------------------------------------
%% @doc
%% Clears document from memory.
%% @end
%%--------------------------------------------------------------------
-spec clear(Level :: datastore:store_level(), ModelName :: atom(),
    Key :: datastore:ext_key()) -> ok | datastore:generic_error().
clear(Level, ModelName, Key) ->
  ModelConfig = ModelName:model_init(),
  Uuid = get_cache_uuid(Key, ModelName),

  consistency_info_lock(ModelName, Key,
    fun() ->
      Pred = fun() ->
        case save_clear_info(Level, Uuid) of
          {ok, _} ->
            save_consistency_info(Level, ModelName, Key);
          _ ->
            false
        end
      end,
      erlang:apply(get_driver_module(Level), delete, [ModelConfig, Key, Pred])
    end).

%%--------------------------------------------------------------------
%% @doc
%% Clears links from memory.
%% @end
%%--------------------------------------------------------------------
-spec clear(Level :: datastore:store_level(), ModelName :: atom(),
    Key :: datastore:ext_key(), datastore:link_name() | all) -> ok | datastore:generic_error().
clear(Level, ModelName, Key, all) ->
  ModelConfig = ModelName:model_init(),
  AccFun = fun(LinkName, _, Acc) ->
    [LinkName | Acc]
  end,
  FullArgs = [ModelConfig, Key, AccFun, []],
  {ok, Links} = erlang:apply(datastore:level_to_driver(Level), foreach_link, FullArgs),
  lists:foldl(fun(Link, Acc) ->
    Ans = clear(Level, ModelName, Key, Link),
    case Ans of
      ok ->
        Acc;
      _ ->
        Ans
    end
  end, ok, Links);

clear(Level, ModelName, Key, Link) ->
  ModelConfig = ModelName:model_init(),
  Uuid = get_cache_uuid(cache_controller:link_cache_key(ModelName, Key, Link), ModelName),
  CCCUuid = get_cache_uuid(Key, ModelName),

  consistency_info_lock(CCCUuid, Link,
    fun() ->
      Pred = fun() ->
        case save_clear_info(Level, Uuid) of
          {ok, _} ->
            save_consistency_info(Level, CCCUuid, Link);
          _ ->
            false
        end
      end,
      erlang:apply(get_driver_module(Level), delete_links, [ModelConfig, Key, [Link], Pred])
    end).

%%--------------------------------------------------------------------
%% @doc
%% Saves information about restoring doc to memory
%% @end
%%--------------------------------------------------------------------
-spec save_consistency_restored_info(Level :: datastore:store_level(), Key :: datastore:ext_key(),
    ClearedName :: datastore:key() | datastore:link_name()) ->
  boolean() | datastore:create_error().
save_consistency_restored_info(Level, Key, ClearedName) ->
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
  Pid = self(),
  UpdateFun = fun
                (#cache_consistency_controller{cleared_list = [], status = {restoring, RPid},
                  restore_counter = RC} = Record) ->
                  case is_process_alive(RPid) of
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

  cache_consistency_controller:create_or_update(Level, Doc, UpdateFun).

%%--------------------------------------------------------------------
%% @doc
%% Marks that consistency restoring has ended
%% @end
%%--------------------------------------------------------------------
-spec end_consistency_restoring(Level :: datastore:store_level(), Key :: datastore:ext_key()) ->
  {ok, datastore:ext_key()} | datastore:create_error().
end_consistency_restoring(Level, Key) ->
  Pid = self(),
  UpdateFun = fun
                (#cache_consistency_controller{cleared_list = [], status = {restoring, RPid}} = Record) ->
                  case RPid of
                    Pid ->
                      {ok, Record#cache_consistency_controller{status = ok}};
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
  Doc = #document{key = Key, value = #cache_consistency_controller{}},

  cache_consistency_controller:create_or_update(Level, Doc, UpdateFun).

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
      not_monitored
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
%% Checks if memory clearing should be stopped.
%% @end
%%--------------------------------------------------------------------
-spec should_stop_clear_cache(MemUsage :: float(), ErlangMemUsage :: [{atom(), non_neg_integer()}]) -> boolean().
should_stop_clear_cache(MemUsage, ErlangMemUsage) ->
  {ok, TargetMemUse} = application:get_env(?CLUSTER_WORKER_APP_NAME, node_mem_ratio_to_clear_cache),
  {ok, TargetErlangMemUse} = application:get_env(?CLUSTER_WORKER_APP_NAME, erlang_mem_to_clear_cache_mb),
  {ok, StopRatio} = application:get_env(?CLUSTER_WORKER_APP_NAME, mem_clearing_ratio_to_stop),
  MemToCompare = proplists:get_value(system, ErlangMemUsage, 0),
  (MemUsage < TargetMemUse * StopRatio / 100) orelse (MemToCompare < TargetErlangMemUse * 1024 * 1024  * StopRatio / 100).

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Translates datastore level to module name.
%% @end
%%--------------------------------------------------------------------
-spec get_driver_module(Level :: datastore:store_level()) -> atom().
get_driver_module(Level) ->
  datastore:driver_to_module(datastore:level_to_driver(Level)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves information about clearing doc from memory initialized by user
%% @end
%%--------------------------------------------------------------------
-spec save_clear_info(Level :: datastore:store_level(), Uuid :: binary()) ->
  {ok, datastore:ext_key()} | datastore:create_error().
save_clear_info(Level, Uuid) ->
  TS = os:system_time(?CC_TIMEUNIT),
  UpdateFun = fun(Record) ->
    {ok, Record#cache_controller{action = cleared, last_action_time = TS}}
  end,
  V = #cache_controller{timestamp = TS, action = cleared, last_action_time = TS},
  Doc = #document{key = Uuid, value = V},

  cache_controller:create_or_update(Level, Doc, UpdateFun).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves information about clearing doc from memory initialized by high mem usage
%% @end
%%--------------------------------------------------------------------
-spec save_high_mem_clear_info(Level :: datastore:store_level(), Uuid :: binary()) ->
  {ok, datastore:ext_key()} | datastore:create_error().
save_high_mem_clear_info(Level, Uuid) ->
  TS = os:system_time(?CC_TIMEUNIT),
  UpdateFun = fun
                (#cache_controller{action = to_be_del}) ->
                  {error, document_in_use};
                (#cache_controller{last_user = non} = Record) ->
                  {ok, Record#cache_controller{action = cleared, last_action_time = TS}};
                (_) ->
                  {error, document_in_use}
              end,
  V = #cache_controller{timestamp = TS, action = cleared, last_action_time = TS},
  Doc = #document{key = Uuid, value = V},

  cache_controller:create_or_update(Level, Doc, UpdateFun).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves information about clearing doc from memory
%% @end
%%--------------------------------------------------------------------
-spec save_consistency_info(Level :: datastore:store_level(), Key :: datastore:ext_key(),
    ClearedName :: datastore:key() | datastore:link_name()) ->
  boolean() | datastore:create_error().
save_consistency_info(Level, Key, ClearedName) ->
  UpdateFun = fun
    (#cache_consistency_controller{status = not_monitored}) ->
      {error, clearing_not_monitored};
    (#cache_consistency_controller{cleared_list = CL, clearing_counter = CC} = Record) ->
      case length(CL) >= ?CLEAR_MONITOR_MAX_SIZE of
        true ->
          {ok, Record#cache_consistency_controller{cleared_list = [], status = not_monitored,
            clearing_counter = CC + 1}};
        _ ->
          case lists:member(ClearedName, CL) of
            true ->
              {error, already_cleared};
            _ ->
              {ok, Record#cache_consistency_controller{cleared_list = [ClearedName | CL],
                clearing_counter = CC + 1}}
          end
      end
  end,

  case cache_consistency_controller:update(Level, Key, UpdateFun) of
    {ok, _} ->
      true;
    {error, clearing_not_monitored} ->
      true;
    {error, already_cleared} ->
      true;
    {error, {not_found, _}} ->
      true;
    Other ->
      ?error_stacktrace("Cannot save consistency_info ~p, error: ~p", [{Level, Key, ClearedName}, Other]),
      false
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Clears old documents from memory.
%% @end
%%--------------------------------------------------------------------
-spec delete_old_keys(Level :: global_only | local_only, Caches :: list(), TimeWindow :: integer()) -> ok.
delete_old_keys(Level, Caches, TimeWindow) ->
  Now = os:system_time(?CC_TIMEUNIT),
  ClearFun = fun
    ('$end_of_table', {0, _, undefined}) ->
      {abort, 0};
    ('$end_of_table', {Count0, BatchNum, {Uuid, V}}) ->
      T = V#cache_controller.timestamp,
      U = V#cache_controller.last_user,
      Age = Now - T,
      Count = case U of
        non when Age >= 1000 * TimeWindow ->
          delete_old_key(Level, Uuid, BatchNum),
          Count0 + 1;
        _ ->
          Count0
      end,
      case Count of
        0 ->
          ok;
        _ ->
          count_clear_acc(Count rem (?CLEAR_BATCH_SIZE + 1), BatchNum)
      end,
      {abort, Count};
    (#document{key = Uuid, value = V}, {0, BatchNum, undefined}) ->
      {next, {0, BatchNum, {Uuid, V}}};
    (#document{key = NewUuid, value = NewV}, {Count0, BatchNum0, {Uuid, V}}) ->
      T = V#cache_controller.timestamp,
      U = V#cache_controller.last_user,
      Age = Now - T,
      {Stop, BatchNum, Count} = case U of
        non when Age >= 1000 * TimeWindow ->
          delete_old_key(Level, Uuid, BatchNum0),
          NewCount = Count0 + 1,
          case NewCount rem ?CLEAR_BATCH_SIZE of
            0 ->
              count_clear_acc(?CLEAR_BATCH_SIZE, BatchNum0),

              case monitoring:get_memory_stats() of
                [{<<"mem">>, MemUsage}] ->
                  ErlangMemUsage = erlang:memory(),
                  {should_stop_clear_cache(MemUsage, ErlangMemUsage), BatchNum0 + 1, NewCount};
                _ ->
                  ?warning("Not able to check memory usage"),
                  {false, BatchNum0 + 1, NewCount}
              end;
            _ ->
              {false, BatchNum0, NewCount}
          end;
        _ ->
          {false, BatchNum0, Count0}
      end,

      case Stop of
        true ->
          {abort, Count};
        _ ->
          {next, {Count, BatchNum, {NewUuid, NewV}}}
      end
  end,

  ClearAns = case TimeWindow of
    0 ->
      list_old_keys(Level, ClearFun, cache_controller);
    _ ->
      cache_controller:list_dirty(Level, ClearFun, {0, 0, undefined})
  end,

  case ClearAns of
    {ok, _} ->
      case TimeWindow of
        0 ->
          % TODO - the same for links
          lists:foreach(fun(Cache) ->
            ClearFun2 = fun
              ('$end_of_table', {0, _, undefined}) ->
                {abort, 0};
              ('$end_of_table', {Count, BatchNum, UuidToDel}) ->
                Master = self(),
                spawn(fun() ->
                  safe_delete(Level, Cache, UuidToDel),
                  Master ! {doc_cleared, BatchNum}
                end),
                count_clear_acc(Count rem (?CLEAR_BATCH_SIZE + 1), BatchNum),
                {abort, Count};
              (#document{key = Uuid}, {0, BatchNum, undefined}) ->
                {next, {1, BatchNum, Uuid}};
              (#document{key = Uuid}, {Count, BatchNum0, UuidToDel}) ->
                Master = self(),
                spawn(fun() ->
                  safe_delete(Level, Cache, UuidToDel),
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
            {ok, _} = list_old_keys(Level, ClearFun2, Cache)
          end, Caches);
        _ ->
          ok
      end;
    Other ->
      ?error("Error during cache cleaning: ~p", [Other]),
      ok
  end.

 %%--------------------------------------------------------------------
%% @private
%% @doc
%% Lists keys and executes fun for each key. Reruns listing if aborted.
%% @end
%%--------------------------------------------------------------------
-spec list_old_keys(Level :: global_only | local_only, ClearFun :: datastore:list_fun(),
    ModelName :: model_behaviour:model_type()) -> {ok, term()} | datastore:generic_error() | no_return().
list_old_keys(Level, ClearFun, Model) ->
  case datastore:list_dirty(Level, Model, ClearFun, {0, 0, undefined}) of
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
%% Deletes key with cache data.
%% @end
%%--------------------------------------------------------------------
-spec delete_old_key(Level :: global_only | local_only, Uuid :: binary(), BatchNum :: integer()) -> ok.
delete_old_key(Level, Uuid, BatchNum) ->
  Master = self(),
  spawn(fun() ->
    {ModelName, Key} = decode_uuid(Uuid),
    case safe_delete(Level, ModelName, Key) of
      ok ->
        Master ! {doc_cleared, BatchNum},
        timer:sleep(timer:seconds(2)), % allow start new operations if scheduled
        Pred = fun() ->
          CheckAns = case cache_controller:get(Level, Uuid) of
            {ok, #document{value = #cache_controller{last_user = LU, action = A}}} ->
              {LU, A};
            {error, {not_found, _}} ->
              ok
          end,

          case CheckAns of
            {_, to_be_del} ->
              false;
            {non, _} ->
              true;
            _ ->
              false
          end
        end,

        cache_controller:delete(Level, Uuid, Pred),
        case Key of
          {_, _, cache_controller_link_key} ->
            ok;
          _ ->
            CCCUuid = get_cache_uuid(Key, ModelName),
            cache_consistency_controller:delete(Level, CCCUuid)
        end;
      _ ->
        error
    end
  end),
  ok.

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
%% @private
%% @doc
%% Deletes info from memory when it is dumped to disk.
%% @end
%%--------------------------------------------------------------------
-spec safe_delete(Level :: datastore:store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:key() | {datastore:ext_key(), datastore:link_name(), cache_controller_link_key}) ->
  ok | datastore:generic_error().
safe_delete(Level, ModelName, {Key, Link, cache_controller_link_key}) ->
  try
    ModelConfig = ModelName:model_init(),
    Uuid = get_cache_uuid(cache_controller:link_cache_key(ModelName, Key, Link), ModelName),
    CCCUuid = get_cache_uuid(Key, ModelName),
    Driver = get_driver_module(Level),

    consistency_info_lock(CCCUuid, Link,
      fun() ->
        critical_section:run({cache_controller, start_disk_op, Uuid},
          fun() ->
            {ok, DiskValue} = erlang:apply(get_driver_module(?DISK_ONLY_LEVEL), fetch_link, [ModelConfig, Key, Link]),
            Pred = fun() ->
              {ok, MemValue} = erlang:apply(Driver, fetch_link, [ModelConfig, Key, Link]),
              case MemValue of
                DiskValue ->
                  case save_high_mem_clear_info(Level, Uuid) of
                    {ok, _} ->
                      save_consistency_info(Level, CCCUuid, Link);
                    _ ->
                      false
                  end;
                _ ->
                  false
              end
            end,
            erlang:apply(Driver, delete_links, [ModelConfig, Key, [Link], Pred])
          end)
      end)
  catch
    E1:E2 ->
      ?error_stacktrace("Error in cache controller safe_delete. "
      ++ "Args: ~p. Error: ~p:~p.", [{Level, ModelName, {Key, Link, cache_controller_link_key}}, E1, E2]),
      {error, safe_delete_failed}
  end;
safe_delete(Level, ModelName, Key) ->
  try
    ModelConfig = ModelName:model_init(),
    Uuid = get_cache_uuid(Key, ModelName),
    Driver = get_driver_module(Level),

    consistency_info_lock(ModelName, Key,
      fun() ->
        critical_section:run({cache_controller, start_disk_op, Uuid},
          fun() ->
            {ok, #document{value = DiskValue}} = erlang:apply(get_driver_module(?DISK_ONLY_LEVEL), get, [ModelConfig, Key]),
            Pred = fun() ->
              {ok, #document{value = MemValue}} = erlang:apply(Driver, fetch_link, [ModelConfig, Key]),
              case MemValue of
                DiskValue ->
                  case save_high_mem_clear_info(Level, Uuid) of
                    {ok, _} ->
                      save_consistency_info(Level, ModelName, Key);
                    _ ->
                      false
                  end;
                _ ->
                  false
              end
            end,
            erlang:apply(Driver, delete, [ModelConfig, Key, Pred])
          end),
        Pred =fun() ->
          case save_high_mem_clear_info(Level, Uuid) of
            {ok, _} ->
              save_consistency_info(Level, ModelName, Key);
            _ ->
              false
          end
        end,
        erlang:apply(get_driver_module(Level), delete, [ModelConfig, Key, Pred])
      end)
  catch
    E1:E2 ->
      ?error_stacktrace("Error in cache controller safe_delete. "
      ++ "Args: ~p. Error: ~p:~p.", [{Level, ModelName, Key}, E1, E2]),
      {error, safe_delete_failed}
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Clears all documents from memory.
%% @end
%%--------------------------------------------------------------------
-spec delete_all_keys(Level :: global_only | local_only, Caches :: list()) -> ok | cleared.
delete_all_keys(Level, Caches) ->
  Filter = fun
             ('$end_of_table', Acc) ->
               {abort, Acc};
             (#document{key = Uuid}, Acc) ->
               {next, [Uuid | Acc]}
           end,
  {ok, Uuids} = cache_controller:list(Level, Filter, []),
  UuidsNum = length(Uuids),
  lists:foreach(fun(Uuid) ->
    {ModelName, Key} = decode_uuid(Uuid),
    value_delete(Level, ModelName, Key),
    cache_controller:delete(Level, Uuid, ?PRED_ALWAYS)
  end, Uuids),

  ClearedNum = lists:foldl(fun(Cache, Sum) ->
    {ok, Docs} = datastore:list(Level, Cache, ?GET_ALL, []),
    DocsNum = length(Docs),
    lists:foreach(fun(Doc) ->
      value_delete(Level, Cache, Doc#document.key)
    end, Docs),
    Sum + DocsNum
  end, 0, Caches),

  case UuidsNum + ClearedNum of
    0 ->
      ok;
    _ ->
      cleared
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes info from memory.
%% @end
%%--------------------------------------------------------------------
-spec value_delete(Level :: datastore:store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:key() | {datastore:ext_key(), datastore:link_name(), cache_controller_link_key}) ->
  ok | datastore:generic_error().
value_delete(Level, ModelName, {Key, Link, cache_controller_link_key}) ->
  try
    ModelConfig = ModelName:model_init(),
    FullArgs2 = [ModelConfig, Key, [Link]],
    erlang:apply(get_driver_module(Level), delete_links, FullArgs2)
  catch
    E1:E2 ->
      ?error_stacktrace("Error in cache controller value_delete. "
      ++ "Args: ~p. Error: ~p:~p.", [{Level, ModelName, {Key, Link, cache_controller_link_key}}, E1, E2]),
      {error, delete_failed}
  end;
value_delete(Level, ModelName, Key) ->
  try
    ModelConfig = ModelName:model_init(),
    FullArgs2 = [ModelConfig, Key, ?PRED_ALWAYS],
    erlang:apply(get_driver_module(Level), delete, FullArgs2)
  catch
    E1:E2 ->
      ?error_stacktrace("Error in cache controller value_delete. "
      ++ "Args: ~p. Error: ~p:~p.", [{Level, ModelName, Key}, E1, E2]),
      {error, delete_failed}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Checks tasks poll and recommends throttling action.
%% @end
%%--------------------------------------------------------------------
-spec verify_tasks() ->
  {TaskAction :: non_neg_integer(), NewFailed :: non_neg_integer(), NewTasks :: non_neg_integer()}.
verify_tasks() ->
  {ok, FailedTasksNumThreshold} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_start_failed_tasks_number),
  {ok, PendingTasksNumThreshold} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_start_pending_tasks_number),

  {ok, {NewFailed, NewTasks}} =
    task_pool:count_tasks(?CLUSTER_LEVEL, cache_dump, FailedTasksNumThreshold+PendingTasksNumThreshold),

  TaskAction = case {NewFailed, NewTasks} of
    {NF, _} when NF >= FailedTasksNumThreshold ->
      ?BLOCK_THROTTLING;
    {NF, NT} when NT-NF >= PendingTasksNumThreshold ->
      ?BLOCK_THROTTLING;
    {NF, _} when NF >= FailedTasksNumThreshold/2 ->
      ?LIMIT_THROTTLING;
    {NF, NT} when NT-NF >= PendingTasksNumThreshold/2 ->
      ?LIMIT_THROTTLING;
    {NF, _} when NF > 0 ->
      ?CONFIG_THROTTLING;
    {_, NT} when NT > 0 ->
      ?CONFIG_THROTTLING;
    _ ->
      ?NO_THROTTLING
  end,

  {TaskAction, NewFailed, NewTasks}.

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