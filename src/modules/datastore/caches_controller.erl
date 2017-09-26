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
-export([throttle/1, throttle_get/1, throttle_del/1, throttle_del/2,
  get_idle_timeout/0, configure_throttling/0, plan_next_throttling_check/0,
  get_hooks_throttling_config/1, init_counters/0]).
% for tests
-export([send_after/3]).

-define(CLEAR_BATCH_SIZE, 50).
-define(MNESIA_THROTTLING_KEY, mnesia_throttling).
-define(READ_THROTTLING_KEY, read_throttling).
-define(MEMORY_PROC_IDLE_KEY, throttling_idle_time).
-define(MNESIA_THROTTLING_DATA_KEY, <<"mnesia_throttling_data">>).
-define(THROTTLING_ERROR, {error, load_to_high}).

-define(BLOCK_THROTTLING, 3).
-define(LIMIT_THROTTLING, 2).
-define(CONFIG_THROTTLING, 1).
-define(NO_THROTTLING, 0).

-define(LEVEL_OVERRIDE(Level), [{level, Level}]).

-define(EXOMETER_NAME, [tp_nums]).

%%%===================================================================
%%% API
%%%===================================================================

init_counters() ->
  exometer:new(?EXOMETER_NAME, histogram, [{time_span,
    application:get_env(?CLUSTER_WORKER_APP_NAME, exometer_time_span, 600000)}]),
  exometer_report:subscribe(exometer_report_lager, ?EXOMETER_NAME,
    [min, max, median, mean],
    application:get_env(?CLUSTER_WORKER_APP_NAME, exometer_logging_interval, 1000)).

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
        {ok, ok} ->
          ok;
        {ok, {throttle, Time, _}} ->
          timer:sleep(Time),
          ok;
        {ok, {overloaded, _}} ->
          ?THROTTLING_ERROR;
        _ ->
          ok
      end;
    _ ->
      ok
  end.

%%--------------------------------------------------------------------
%% @doc
%% Limits get operation performance if needed.
%% @end
%%--------------------------------------------------------------------
-spec throttle_get(ModelName :: model_behaviour:model_type()) -> ok | ?THROTTLING_ERROR.
throttle_get(ModelName) ->
  case lists:member(ModelName, datastore_config:throttled_models()) of
    true ->
      case application:get_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY) of
        {ok, ok} ->
          ok;
        {ok, {throttle, Time, _}} ->
          case application:get_env(?CLUSTER_WORKER_APP_NAME, ?READ_THROTTLING_KEY) of
            {ok, throttle} ->
              timer:sleep(Time),
              ok;
            _ ->
              ok
          end;
        {ok, {overloaded, _}} ->
          ?THROTTLING_ERROR;
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
-spec throttle_del(ModelName :: model_behaviour:model_type()) -> ok | ?THROTTLING_ERROR.
throttle_del(ModelName) ->
  case lists:member(ModelName, datastore_config:throttled_models()) of
    true ->
      case application:get_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY) of
        {ok, ok} ->
          ok;
        {ok, {throttle, Time, true}} ->
          timer:sleep(Time),
          ok;
        {ok, {throttle, _, _}} ->
          ok;
        {ok, {overloaded, true}} ->
          ?THROTTLING_ERROR;
        {ok, {overloaded, _}} ->
          ok;
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
      {DBAction, QueueSize, ReadDBAction, ReadQueueSize} = verify_db(),
      Action = max(max(MemAction, TPAction), DBAction),
      ReadAction = max(max(MemAction, TPAction), ReadDBAction),

      case Action of
        ?BLOCK_THROTTLING ->
          application:set_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY,
            {overloaded, MemAction =:= ?NO_THROTTLING}),

          {ok, NewProcNum} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_block_memory_proc_number),
          tp:set_processes_limit(NewProcNum),

          case ReadAction of
            ?NO_THROTTLING ->
              application:set_env(?CLUSTER_WORKER_APP_NAME, ?READ_THROTTLING_KEY,
                no_throttling);
            _ ->
              application:set_env(?CLUSTER_WORKER_APP_NAME, ?READ_THROTTLING_KEY,
                throttle)
          end,

          ?info("Throttling: overload mode started, mem: ~p, tp proc num ~p,
          couch queue size: ~p, couch read queue size: ~p, read action ~p",
            [MemoryUsage, ProcNum, QueueSize, ReadQueueSize, ReadAction]),
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
              ?debug("Throttling: no config needed, mem: ~p, tp proc num ~p,
              couch queue size: ~p, couch read queue size: ~p",
                [MemoryUsage, ProcNum, QueueSize, ReadQueueSize]),
              plan_next_throttling_check();
            {?NO_THROTTLING, _} ->
              application:unset_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY),
              ok = node_management:delete(?MNESIA_THROTTLING_DATA_KEY),
              ?info("Throttling: stop, mem: ~p, tp proc num ~p,
              couch queue size: ~p, couch read queue size: ~p",
                [MemoryUsage, ProcNum, QueueSize, ReadQueueSize]),
              plan_next_throttling_check();
            {?CONFIG_THROTTLING, ok} ->
              ?debug("Throttling: no config needed, mem: ~p, tp proc num ~p,
              couch queue size: ~p, couch read queue size: ~p",
                [MemoryUsage, ProcNum, QueueSize, ReadQueueSize]),
              plan_next_throttling_check(true);
            {?LIMIT_THROTTLING, {overloaded, true}} when MemAction > ?NO_THROTTLING ->
              application:set_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY, {overloaded, false}),
              ?info("Throttling: continue overload, mem: ~p, tp proc num ~p,
              couch queue size: ~p, couch read queue size: ~p",
                [MemoryUsage, ProcNum, QueueSize, ReadQueueSize]),
              plan_next_throttling_check(true);
            {?LIMIT_THROTTLING, {overloaded, _}} ->
              ?info("Throttling: continue overload, mem: ~p, tp proc num ~p,
              couch queue size: ~p, couch read queue size: ~p",
                [MemoryUsage, ProcNum, QueueSize, ReadQueueSize]),
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

              case ReadAction of
                ?NO_THROTTLING ->
                  application:set_env(?CLUSTER_WORKER_APP_NAME, ?READ_THROTTLING_KEY,
                    no_throttling);
                _ ->
                  application:set_env(?CLUSTER_WORKER_APP_NAME, ?READ_THROTTLING_KEY,
                    throttle)
              end,

              {ok, _} = node_management:save(#document{key = ?MNESIA_THROTTLING_DATA_KEY,
                value = #node_management{value = {ProcNum, QueueSize, MemoryUsage, NextCheck}}}),

              ?info("Throttling: delay ~p ms used, mem: ~p, tp proc num ~p,
              couch queue size: ~p, couch read queue size: ~p, read action ~p",
                [ThrottlingTime, MemoryUsage, ProcNum, QueueSize, ReadQueueSize, ReadAction]),

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

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
  ok = exometer:update(?EXOMETER_NAME, ProcNum),
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
  {DBAction :: non_neg_integer(), QueueSize :: non_neg_integer(),
    ReadDBAction :: non_neg_integer(), ReadQueueSize :: non_neg_integer()}.
verify_db() ->
  QueueSize = lists:foldl(fun(Bucket, Acc) ->
    couchbase_pool:get_max_worker_queue_size(Bucket) + Acc
  end, 0, couchbase_config:get_buckets()),

  ReadQueueSize = lists:foldl(fun(Bucket, Acc) ->
    couchbase_pool:get_max_worker_queue_size(Bucket, read) + Acc
  end, 0, couchbase_config:get_buckets()),

  DBAction = get_db_action(QueueSize),
  ReadDBAction = get_db_action(ReadQueueSize),

  {DBAction, QueueSize, ReadDBAction, ReadQueueSize}.

%%--------------------------------------------------------------------
%% @doc
%% Advices action on the basis of db queue length.
%% @end
%%--------------------------------------------------------------------
-spec get_db_action(QueueSize :: non_neg_integer()) -> Action :: non_neg_integer().
get_db_action(QueueSize) ->
  {ok, Limit} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_db_queue_limit),
  {ok, Start} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_delay_db_queue_size),

  case QueueSize of
    DP when DP >= Limit ->
      ?BLOCK_THROTTLING;
    DP when DP >= Start ->
      ?LIMIT_THROTTLING;
    DP when DP >= Start/2 ->
      ?CONFIG_THROTTLING;
    _ ->
      ?NO_THROTTLING
  end.

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
