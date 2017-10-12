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
-export([throttle/0, throttle/1, throttle_model/1,
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

%%--------------------------------------------------------------------
%% @doc
%% Initializes exometer counters used by this module.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
  init_counter(tp),
  init_counter(db_queue).

%%--------------------------------------------------------------------
%% @doc
%% Sets exometer report connected with counters used by this module.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
  init_report(tp),
  init_report(db_queue).

%%--------------------------------------------------------------------
%% @doc
%% Limits operation performance depending on model name.
%% @end
%%--------------------------------------------------------------------
-spec throttle_model(ModelName :: model_behaviour:model_type()) -> ok | ?THROTTLING_ERROR.
throttle_model(ModelName) ->
  case lists:member(ModelName, datastore_config:throttled_models()) of
    true ->
      throttle();
    _ ->
      ok
  end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv throttle(default).
%% @end
%%--------------------------------------------------------------------
-spec throttle() -> ok | ?THROTTLING_ERROR.
throttle() ->
  throttle(default).

%%--------------------------------------------------------------------
%% @doc
%% Limits operation performance for particular config.
%% @end
%%--------------------------------------------------------------------
-spec throttle(Config :: atom()) -> ok | ?THROTTLING_ERROR.
throttle(Config) ->
  case application:get_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY) of
    {ok, ConfigList} ->
      case proplists:get_value(Config, ConfigList) of
        ok ->
          ok;
        {throttle, Time} ->
          timer:sleep(Time),
          ok;
        overloaded ->
          ?THROTTLING_ERROR
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
      [TPNum, DBQueue, MemUsage] = Values = get_values_and_update_counters(),
      set_idle_time(TPNum),

      {ok, Configs} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_config),
      DefaultConfig = proplists:get_value(default, Configs),
      ConfigResult = lists:foldl(fun({ConfigName, Config}, Acc) ->
        [{ConfigName, configure_throttling(Values, Config, DefaultConfig)} | Acc]
      end, [], Configs),

      application:set_env(?CLUSTER_WORKER_APP_NAME, ?MNESIA_THROTTLING_KEY,
        ConfigResult),

      FilteredConfigResult = lists:filter(fun
        ({_, ok}) -> false;
        (_) -> true
      end, ConfigResult),

      case FilteredConfigResult of
        [] ->
          % TODO - change logs to debug
          ?info("No throttling: config: ~p, tp num ~p, db queue ~p, mem usage ~p",
            [ConfigResult, TPNum, DBQueue, MemUsage]),
          plan_next_throttling_check();
        _ ->
          ?info("Throttling config: ~p, tp num ~p, db queue ~p, mem usage ~p",
            [ConfigResult, TPNum, DBQueue, MemUsage]),
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
%% @private
%% Configures throttling settings for particular config.
%% @end
%%--------------------------------------------------------------------
-spec configure_throttling(Values :: [number()], Config :: list(),
    DefaultConfig :: list()) -> ok | overloaded | {throttle, non_neg_integer()}.
configure_throttling(Values, Config, DefaultConfig) ->
  TPMultip = get_config_value(tp_param_strength, Config, DefaultConfig),
  DBMultip = get_config_value(db_param_strength, Config, DefaultConfig),
  MemMultip = get_config_value(mem_param_strength, Config, DefaultConfig),
  Multipliers = [TPMultip, DBMultip, MemMultip],

  GetFunctions = [fun get_tp_params/2, fun get_db_params/2,
    fun get_memory_params/2],
  Parameters = lists:zip(Multipliers, lists:zip(Values, GetFunctions)),

  {ThrottlingBase0, MaxRatio} = lists:foldl(fun
    ({0, {_,_}}, {Acc, Max}) ->
      {Acc, Max};
    ({Multip, {Value, GetFun}}, {Acc, Max}) ->
      {Expected, Limit} = apply(GetFun, [Config, DefaultConfig]),
      Ratio = (Value - Expected) / (Limit - Expected),
      {Acc + Multip * math:pow(max(0, Ratio), 3), max(Ratio, Max)}
  end, {0, 0}, Parameters),

  case {ThrottlingBase0, MaxRatio >= 1.0} of
    {0.0, _} ->
      ok;
    {_, true} ->
      overloaded;
    _ ->
      Strength = get_config_value(strength, Config, DefaultConfig),
      ThrottlingBase = math:exp(-1 * Strength * ThrottlingBase0),

      BaseTime = application:get_env(?CLUSTER_WORKER_APP_NAME,
        throttling_base_time_ms, 2048),
      Time = round(BaseTime * (1 - ThrottlingBase)),
      {throttle, Time}
  end.

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Sets idle time depending on tp process number.
%% @end
%%--------------------------------------------------------------------
-spec set_idle_time(ProcNum :: non_neg_integer()) -> ok.
set_idle_time(ProcNum) ->
  {ok, Idle1} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_reduce_idle_time_memory_proc_number),
  {ok, Idle2} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_min_idle_time_memory_proc_number),

  {ok, IdleTimeout} = application:get_env(?CLUSTER_WORKER_APP_NAME, memory_store_idle_timeout_ms),
  {ok, MinIdleTimeout} = application:get_env(?CLUSTER_WORKER_APP_NAME, memory_store_min_idle_timeout_ms),

  Multip = max(0, min(1, (ProcNum - Idle1) / (Idle2 - Idle1))),
  NewIdleTimeout = round(IdleTimeout - Multip * (IdleTimeout - MinIdleTimeout)),

  % TODO - debug
  ?info("New idle time: ~p", [NewIdleTimeout]),

  application:set_env(?CLUSTER_WORKER_APP_NAME, ?MEMORY_PROC_IDLE_KEY, NewIdleTimeout).

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Gets value of parameters used to configure throttling.
%% Updates exometer counters.
%% @end
%%--------------------------------------------------------------------
-spec get_values_and_update_counters() -> [number()].
get_values_and_update_counters() ->
  ProcNum = tp:get_processes_number(),
  ok = exometer:update(?EXOMETER_NAME(tp), ProcNum),

  QueueSize = lists:foldl(fun(Bucket, Acc) ->
    couchbase_pool:get_max_worker_queue_size(Bucket) + Acc
  end, 0, couchbase_config:get_buckets()),
  ok = exometer:update(?EXOMETER_NAME(db_queue), QueueSize),

  MemoryUsage = case monitoring:get_memory_stats() of
    [{<<"mem">>, MemUsage}] ->
      MemUsage
  end,

  [ProcNum, QueueSize, MemoryUsage].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets expected value and limit for tp proceses number.
%% @end
%%--------------------------------------------------------------------
-spec get_tp_params(Config :: list(), Defaults :: list()) ->
  {Expected :: non_neg_integer(), Limit :: non_neg_integer()}.
get_tp_params(Config, Defaults) ->
  Expected = get_config_value(tp_proc_expected, Config, Defaults),
  Limit = get_config_value(tp_proc_limit, Config, Defaults),

  {Expected, Limit}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets expected value and limit for db queue.
%% @end
%%--------------------------------------------------------------------
-spec get_db_params(Config :: list(), Defaults :: list()) ->
  {Expected :: non_neg_integer(), Limit :: non_neg_integer()}.
get_db_params(Config, Defaults) ->
  Expected = get_config_value(db_queue_expected, Config, Defaults),
  Limit = get_config_value(db_queue_limit, Config, Defaults),

  {Expected, Limit}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets expected value and limit for memory usage.
%% @end
%%--------------------------------------------------------------------
-spec get_memory_params(Config :: list(), Defaults :: list()) ->
  {Expected :: non_neg_integer(), Limit :: non_neg_integer()}.
get_memory_params(Config, Defaults) ->
  Expected = get_config_value(memory_expected, Config, Defaults),
  Limit = get_config_value(memory_limit, Config, Defaults),

  {Expected, Limit}.

%%--------------------------------------------------------------------
%% @private
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets value from configuration
%% @end
%%--------------------------------------------------------------------
-spec get_config_value(Name :: atom(), Config :: list(), Defaults :: list()) ->
  term().
get_config_value(Name, Config, Defaults) ->
  case proplists:get_value(Name, Config) of
    undefined ->
      proplists:get_value(Name, Defaults);
    Value ->
      Value
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes exometer counter.
%% @end
%%--------------------------------------------------------------------
-spec init_counter(Name :: atom()) -> ok.
init_counter(Name) ->
  exometer:new(?EXOMETER_NAME(Name), histogram, [{time_span,
    application:get_env(?CLUSTER_WORKER_APP_NAME,
      exometer_throttling_time_span, 600000)}]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets exometer report connected with particular counter.
%% @end
%%--------------------------------------------------------------------
-spec init_report(Name :: atom()) -> ok.
init_report(Name) ->
  exometer_report:subscribe(exometer_report_lager, ?EXOMETER_NAME(Name),
    [min, max, median, mean, n],
    application:get_env(?CLUSTER_WORKER_APP_NAME, exometer_logging_interval, 1000)).