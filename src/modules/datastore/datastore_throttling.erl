%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for introducing delays between datastore
%%% operations or even blocking them in order to prevent overload.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_throttling).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("exometer_utils.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("elements/task_manager/task_manager.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([throttle/0, throttle/1, throttle_model/1,
    get_idle_timeout/0, configure_throttling/0, plan_next_throttling_check/0, 
    init_counters/0, init_report/0]).
% for tests
-export([send_after/3]).

-type model() :: datastore_model:model().

-define(MNESIA_THROTTLING_KEY, mnesia_throttling).
-define(MEMORY_PROC_IDLE_KEY, throttling_idle_time).
-define(THROTTLING_ERROR, {error, load_to_high}).

-define(EXOMETER_COUNTERS, [tp, db_queue_max, db_queue_sum,
    db_flush_queue, tp_size_sum]).
-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE, Param)).
-define(EXOMETER_DEFAULT_LOGGING_INTERVAL, 60000).
-define(EXOMETER_DEFAULT_DATA_POINTS_NUMBER, 10000).

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
    Size = application:get_env(?CLUSTER_WORKER_APP_NAME, 
        exometer_data_points_number, ?EXOMETER_DEFAULT_DATA_POINTS_NUMBER),
    Counters = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), uniform, [{size, Size}]}
    end, ?EXOMETER_COUNTERS),
    ?init_counters(Counters).

%%--------------------------------------------------------------------
%% @doc
%% Sets exometer report connected with counters used by this module.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
  Reports = lists:map(fun(Name) ->
    {?EXOMETER_NAME(Name), [min, max, median, mean, n]}
  end, ?EXOMETER_COUNTERS),
  ?init_reports(Reports).

%%--------------------------------------------------------------------
%% @doc
%% Limits operation performance depending on model name.
%% @end
%%--------------------------------------------------------------------
-spec throttle_model(model()) -> ok | ?THROTTLING_ERROR.
throttle_model(Model) ->
    case lists:member(Model, datastore_config:get_throttled_models()) of
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
            application:get_env(?CLUSTER_WORKER_APP_NAME,
                datastore_writer_idle_timeout, timer:seconds(30))
    end.

%%--------------------------------------------------------------------
%% @doc
%% Configures throttling settings.
%% @end
%%--------------------------------------------------------------------
-spec configure_throttling() -> ok.
configure_throttling() ->
    Self = self(),
    case application:get_env(?CLUSTER_WORKER_APP_NAME, spawn_throttling_config, false) of
        true ->
            spawn(fun() ->
                configure_throttling(Self)
            end);
        _ ->
            configure_throttling(Self)
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Configures throttling settings.
%% @end
%%--------------------------------------------------------------------
-spec configure_throttling(pid()) -> ok.
configure_throttling(SendTo) ->
    CheckInterval = try
        [TPNum, DBQueueMax, FlushQueue, DBQueueSum, TPSizesSum, MemUsage] =
            Values = get_values_and_update_counters(),
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
                log_monitoring_stats("No throttling: config: ~p, tp num ~p,
                    db queue max ~p, flush queue ~p, db queue sum ~p, tp sizes sum ~p, mem usage ~p",
                    [ConfigResult, TPNum, DBQueueMax, FlushQueue, DBQueueSum, TPSizesSum, MemUsage]),
                plan_next_throttling_check();
            _ ->
                Msg = "Throttling config: ~p, tp num ~p, db queue max ~p,
                    db queue sum ~p, flush queue ~p, tp sizes sum ~p, mem usage ~p",
                Args = [ConfigResult, TPNum, DBQueueMax, FlushQueue, DBQueueSum, TPSizesSum, MemUsage],
                log_monitoring_stats(Msg, Args),
                plan_next_throttling_check(true)
        end
    catch
        E1:E2 ->
            % Debug log only, possible during start of the system when connection to
            % database is not ready
            log_monitoring_stats("Error during throttling configuration: "
            "~p:~p, ~p", [E1, E2, erlang:get_stacktrace()]),
            plan_next_throttling_check()
    end,
    ?MODULE:send_after(CheckInterval, SendTo, {timer, configure_throttling}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns time after which next throttling config should start.
%% @end
%%--------------------------------------------------------------------
-spec plan_next_throttling_check() -> non_neg_integer().
plan_next_throttling_check() ->
    plan_next_throttling_check(false).

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
    DBMaxMultip = get_config_value(db_max_param_strength, Config, DefaultConfig),
    FlushQueueMultip = get_config_value(flush_queue_param_strength, Config, DefaultConfig),
    DBSumMultip = get_config_value(db_sum_param_strength, Config, DefaultConfig),
    TPSumMultip = get_config_value(tp_size_sum_param_strength, Config, DefaultConfig),
    MemMultip = get_config_value(mem_param_strength, Config, DefaultConfig),
    Multipliers = [TPMultip, DBMaxMultip, FlushQueueMultip, DBSumMultip,
        TPSumMultip, MemMultip],

    GetFunctions = [fun get_tp_params/2, fun get_db_max_params/2,
        fun get_flush_queue_params/2, fun get_db_sum_params/2,
        fun get_tp_sum_params/2, fun get_memory_params/2],
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

            BaseTime = get_config_value(base_time_ms, Config, DefaultConfig),
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

    log_monitoring_stats("New idle time: ~p", [NewIdleTimeout]),

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
    ok = ?update_counter(?EXOMETER_NAME(tp), ProcNum),

    {QueueSizeMax, QueueSizeSum} = lists:foldl(fun(Bucket, {Max, Sum}) ->
        {M, S} = couchbase_pool:get_worker_queue_size_stats(Bucket),
        {max(Max, M), Sum + S}
    end, {0, 0}, couchbase_config:get_buckets()),
    TPSizesSum = tp_router:get_process_size_sum(),
    FlushQueue = couchbase_config:get_flush_queue_size(),

    QueueSizeSum2 = QueueSizeSum + TPSizesSum + FlushQueue,

    ok = ?update_counter(?EXOMETER_NAME(db_queue_max), QueueSizeMax),
    ok = ?update_counter(?EXOMETER_NAME(db_queue_sum), QueueSizeSum2),
    ok = ?update_counter(?EXOMETER_NAME(db_flush_queue), QueueSizeSum2),
    ok = ?update_counter(?EXOMETER_NAME(tp_size_sum), TPSizesSum),

    MemoryUsage = case monitoring:get_memory_stats() of
        [{<<"mem">>, MemUsage}] ->
            MemUsage
    end,

    [ProcNum, QueueSizeMax, FlushQueue, QueueSizeSum2, TPSizesSum, MemoryUsage].

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
%% Gets expected value and limit for max length of db queue.
%% @end
%%--------------------------------------------------------------------
-spec get_db_max_params(Config :: list(), Defaults :: list()) ->
    {Expected :: non_neg_integer(), Limit :: non_neg_integer()}.
get_db_max_params(Config, Defaults) ->
    Expected = get_config_value(db_queue_max_expected, Config, Defaults),
    Limit = get_config_value(db_queue_max_limit, Config, Defaults),

    {Expected, Limit}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets expected value and limit for db flush queue parameter.
%% @end
%%--------------------------------------------------------------------
-spec get_flush_queue_params(Config :: list(), Defaults :: list()) ->
    {Expected :: non_neg_integer(), Limit :: non_neg_integer()}.
get_flush_queue_params(Config, Defaults) ->
    Expected = get_config_value(db_flush_queue_expected, Config, Defaults),
    Limit = get_config_value(db_flush_queue_limit, Config, Defaults),

    {Expected, Limit}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets expected value and limit for sum of length of db queues.
%% @end
%%--------------------------------------------------------------------
-spec get_db_sum_params(Config :: list(), Defaults :: list()) ->
    {Expected :: non_neg_integer(), Limit :: non_neg_integer()}.
get_db_sum_params(Config, Defaults) ->
    Expected = get_config_value(db_queue_sum_expected, Config, Defaults),
    Limit = get_config_value(db_queue_sum_limit, Config, Defaults),

    {Expected, Limit}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets expected value and limit for sum of sizes of tp processes.
%% @end
%%--------------------------------------------------------------------
-spec get_tp_sum_params(Config :: list(), Defaults :: list()) ->
    {Expected :: non_neg_integer(), Limit :: non_neg_integer()}.
get_tp_sum_params(Config, Defaults) ->
    Expected = get_config_value(tp_size_sum_expected, Config, Defaults),
    Limit = get_config_value(tp_size_sum_limit, Config, Defaults),

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
%% Log monitoring result.
%% @end
%%--------------------------------------------------------------------
-spec log_monitoring_stats(Format :: io:format(), Args :: [term()]) -> ok.
log_monitoring_stats(Format, Args) ->
    LogFile = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_log_file,
        "/tmp/throttling_monitoring.log"),
    MaxSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
        throttling_log_file_max_size, 524288000), % 500 MB
    logger:log_with_rotation(LogFile, Format, Args, MaxSize).
