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
-export([throttle/1,
  get_idle_timeout/0, configure_throttling/0, plan_next_throttling_check/0,
  get_hooks_throttling_config/1, init_counters/0, init_report/0]).
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

-define(EXOMETER_NAME(Param), [throttling_stats, Param]).

%%%===================================================================
%%% API
%%%===================================================================

init_counters() ->
  init_counter(tp),
  init_counter(db_queue).

init_counter(Name) ->
  exometer:new(?EXOMETER_NAME(Name), histogram, [{time_span,
    application:get_env(?CLUSTER_WORKER_APP_NAME, exometer_base_time_span, 600000)}]).

init_report() ->
  init_report(tp),
  init_report(db_queue).

init_report(Name) ->
  exometer_report:subscribe(exometer_report_lager, ?EXOMETER_NAME(Name),
    [min, max, median, mean, n],
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
        {ok, {throttle, Time}} ->
          timer:sleep(Time),
          ok;
        {ok, overloaded} ->
          ?THROTTLING_ERROR;
        _ ->
          ok
      end;
    _ ->
      ok
  end.

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
      {MemRation, MemUsage} = verify_memory(),
      {TPRatio, TPNum} = verify_tp(),
      {DBRatio, DBQueue} = verify_db(),

      TPMultip = application:get_env(?CLUSTER_WORKER_APP_NAME,
        throttling_tp_param_strength, 1),
      DBMultip = application:get_env(?CLUSTER_WORKER_APP_NAME,
        throttling_db_param_strength, 1),
      MemMultip = application:get_env(?CLUSTER_WORKER_APP_NAME,
        throttling_mem_param_strength, 0),

      Parameters = [{TPMultip, TPRatio}, {DBMultip, DBRatio}, {MemMultip, MemRation}],
      ThrottlingBase0 = lists:foldl(fun({Multip, Ratio}, Acc) ->
        Acc + Multip * math:pow(max(0, Ratio), 3)
      end, 0, Parameters),

      case ThrottlingBase0 of
        0.0 ->
          application:set_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY,
            ok),
          ?info("No throttling: tp num ~p, db queue ~p, mem usage ~p",
            [TPNum, DBQueue, MemUsage]),
          plan_next_throttling_check();
        _ ->
          Strength = application:get_env(?CLUSTER_WORKER_APP_NAME,
            throttling_strength, 5),

          ThrottlingBase = math:exp(-1 * Strength * ThrottlingBase0),

          case ThrottlingBase < 0.001 of
            true ->
              application:set_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY,
                overloaded),

              ?info("Throttling: overload, base ~p, tp ratio ~p, db ratio ~p, "
              "mem ratio ~p, tp num ~p, db queue ~p, mem usage ~p",
                [ThrottlingBase, TPRatio, DBRatio, MemRation,
                  TPNum, DBQueue, MemUsage]);
            _ ->
              BaseTime = application:get_env(?CLUSTER_WORKER_APP_NAME,
                throttling_base_time_ms, 2048),
              Time = round(BaseTime * (1 - ThrottlingBase)),
              application:set_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY,
                {throttle, Time}),

              ?info("Throttling: time ~p, base ~p, tp ratio ~p, db ratio ~p, "
              "mem ratio ~p, tp num ~p, db queue ~p, mem usage ~p",
                [Time, ThrottlingBase, TPRatio, DBRatio, MemRation,
                  TPNum, DBQueue, MemUsage])
          end,

          plan_next_throttling_check(true)
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
  {ok, Idle1} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_reduce_idle_time_memory_proc_number),
  {ok, Idle2} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_min_idle_time_memory_proc_number),
  {ok, DelayNum} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_start_memory_proc_number),
  {ok, MaxProcNum} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_max_memory_proc_number),

  ProcNum = tp:get_processes_number(),
  ok = exometer:update(?EXOMETER_NAME(tp), ProcNum),
  {ok, IdleTimeout} = application:get_env(?CLUSTER_WORKER_APP_NAME, memory_store_idle_timeout_ms),
  {ok, MinIdleTimeout} = application:get_env(?CLUSTER_WORKER_APP_NAME, memory_store_min_idle_timeout_ms),

  Multip0 = max(0, min(1, (ProcNum - Idle1) / (Idle2 - Idle1))),
  Multip = math:pow(Multip0, 3),
  NewIdleTimeout = round(IdleTimeout - Multip * (IdleTimeout - MinIdleTimeout)),
  application:set_env(?CLUSTER_WORKER_APP_NAME, ?MEMORY_PROC_IDLE_KEY, NewIdleTimeout),

  {(ProcNum - DelayNum) / (MaxProcNum - DelayNum), ProcNum}.

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
  ok = exometer:update(?EXOMETER_NAME(db_queue), QueueSize),

  {ok, Limit} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_db_queue_limit),
  {ok, Start} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_delay_db_queue_size),

  {(QueueSize - Start) / (Limit - Start), QueueSize}.

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

  {(MemoryUsage - MemCleanRatioThreshold) /
    (MemRatioThreshold - MemCleanRatioThreshold), MemoryUsage}.

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
%% @equiv erlang:send_after but enables mocking.
%% @end
%%--------------------------------------------------------------------
-spec send_after(CheckInterval :: non_neg_integer(), Master :: pid() | atom(), Message :: term()) -> reference().
send_after(CheckInterval, Master, Message) ->
  erlang:send_after(CheckInterval, Master, Message).
