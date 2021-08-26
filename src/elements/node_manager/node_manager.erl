%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @author Tomasz Lichon
%%% @author Lukasz Opiola
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is a gen_server that coordinates the
%%% life cycle of node. It starts/stops appropriate services and communicates
%%% with cluster manager.
%%% @end
%%%-------------------------------------------------------------------
-module(node_manager).
-author("Michal Wrzeszcz").
-author("Tomasz Lichon").
-author("Lukasz Opiola").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("exometer_utils.hrl").
-include("elements/node_manager/node_manager.hrl").
-include("elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("cluster_manager/include/node_management_protocol.hrl").


%% API
-export([start_link/0, stop/0, get_ip_address/0,
    modules/0, listeners/0, cluster_worker_modules/0, start_worker/2]).
-export([single_error_log/2, single_error_log/3, single_error_log/4,
    log_monitoring_stats/3]).
-export([init_report/0, init_counters/0]).
-export([are_db_and_workers_ready/0, get_cluster_status/0, get_cluster_status/1]).
-export([is_cluster_healthy/0]).
-export([get_cluster_ips/0]).
-export([reschedule_service_healthcheck/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% for tests
-export([init_workers/0, init_workers/1, upgrade_cluster/0]).


-type state() :: #state{}.
-type cluster_generation() :: non_neg_integer().
% {timer since the last analysis, pid that performs analysis}
-type last_state_analysis() :: {stopwatch:instance(), undefined | pid()}.
-export_type([cluster_generation/0, last_state_analysis/0]).


-define(EXOMETER_COUNTERS, [processes_num, memory_erlang, memory_node, cpu_node]).
-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE, Param)).
-define(EXOMETER_DEFAULT_DATA_POINTS_NUMBER, 10000).

-define(CALL_PLUGIN(Fun, Args), plugins:apply(node_manager_plugin, Fun, Args)).

% make sure the retries take more than 4 minutes, which is the time required
% for a hanging socket to exit the TIME_WAIT state and terminate
-define(PORT_CHECK_RETRIES, 41).
-define(PORT_CHECK_INTERVAL, timer:seconds(6)).

-define(DEFAULT_TERMINATE_TIMEOUT, 5000).

-define(CLUSTER_WORKER_MODULES, [
    {datastore_worker, [
        {supervisor_flags, datastore_worker:supervisor_flags()},
        {supervisor_children_spec, datastore_worker:supervisor_children_spec()}
    ], [{terminate_timeout, infinity}, {posthook, check_db_connection}]},
    {tp_router, [
        {supervisor_flags, tp_router:main_supervisor_flags()},
        {supervisor_children_spec, tp_router:main_supervisor_children_spec()}
    ], [worker_first, {posthook, init_supervisors}]}
]).

-define(HELPER_ETS, node_manager_helper_ets).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc
%% Initializes all counters.
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
%% Subscribe for reports for all parameters.
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
%% List of modules provided by cluster-worker.
%% Use in plugins when specifying custom_workers.
%% @end
%%--------------------------------------------------------------------
-spec cluster_worker_modules() -> Models :: [{atom(), [any()]}
| {atom(), [any()], [any()]} | {singleton, atom(), [any()]}].
cluster_worker_modules() -> ?CLUSTER_WORKER_MODULES.

%%--------------------------------------------------------------------
%% @doc
%% List of loaded modules.
%% @end
%%--------------------------------------------------------------------
-spec modules() -> Models :: [atom()].
modules() ->
    DefaultWorkers = cluster_worker_modules(),
    CustomWorkers = ?CALL_PLUGIN(custom_workers, []),
    lists:map(fun
        ({Module, _}) -> Module;
        ({singleton, Module, _}) -> Module;
        ({Module, _, _}) -> Module
    end, DefaultWorkers ++ CustomWorkers).

%%--------------------------------------------------------------------
%% @doc
%% List of listeners loaded by node_manager.
%% @end
%%--------------------------------------------------------------------
-spec listeners() -> Listeners :: [atom()].
listeners() ->
    ?CALL_PLUGIN(listeners, []).

%%--------------------------------------------------------------------
%% @doc
%% Starts node manager
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> Result when
    Result :: {ok, Pid}
    | ignore
    | {error, Error},
    Pid :: pid(),
    Error :: {already_started, Pid} | term().
start_link() ->
    gen_server2:start_link({local, ?NODE_MANAGER_NAME}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Stops node manager
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
    gen_server2:cast(?NODE_MANAGER_NAME, stop).


%%--------------------------------------------------------------------
%% @doc
%% @equiv single_error_log(LogKey, Log, [])
%% @end
%%--------------------------------------------------------------------
-spec single_error_log(LogKey :: atom(), Log :: string()) -> ok.
single_error_log(LogKey, Log) ->
    single_error_log(LogKey, Log, []).

%%--------------------------------------------------------------------
%% @doc
%% @equiv single_error_log(LogKey, Log, Args, 500)
%% @end
%%--------------------------------------------------------------------
-spec single_error_log(LogKey :: atom(), Log :: string(),
    Args :: [term()]) -> ok.
single_error_log(LogKey, Log, Args) ->
    single_error_log(LogKey, Log, Args, 500).

%%--------------------------------------------------------------------
%% @doc
%% Logs error. If any other process tries to log with same key,
%% the message is not logged.
%% @end
%%--------------------------------------------------------------------
-spec single_error_log(LogKey :: atom(), Log :: string(),
    Args :: [term()], FreezeTime :: non_neg_integer()) -> ok.
single_error_log(LogKey, Log, Args, FreezeTime) ->
    case ets:lookup(?HELPER_ETS, LogKey) of
        [] ->
            case ets:insert_new(?HELPER_ETS, {LogKey, ok}) of
                true ->
                    ?error(Log, Args),
                    timer:sleep(FreezeTime),
                    ets:delete(?HELPER_ETS, LogKey),
                    ok;
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Log monitoring result.
%% @end
%%--------------------------------------------------------------------
-spec log_monitoring_stats(LogFile :: string(),
    Format :: io:format(), Args :: [term()]) -> ok.
log_monitoring_stats(LogFile, Format, Args) ->
    MaxSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
        monitoring_log_file_max_size, 524288000), % 500 MB
    onedata_logger:log_with_rotation(LogFile, Format, Args, MaxSize).


%%--------------------------------------------------------------------
%% @doc
%% Starts worker node with dedicated supervisor as brother. Both entities
%% are started under MAIN_WORKER_SUPERVISOR supervision.
%% @end
%%--------------------------------------------------------------------
-spec start_worker(Module :: atom(), Args :: term()) -> ok | {error, term()}.
start_worker(Module, Args) ->
    start_worker(Module, Args, []).
%%--------------------------------------------------------------------
%% @doc
%% Starts worker node with dedicated supervisor as brother. Both entities
%% are started under MAIN_WORKER_SUPERVISOR supervision.
%% @end
%%--------------------------------------------------------------------
-spec start_worker(Module :: atom(), Args :: term(), Options :: list()) ->
    ok | {error, term()}.
start_worker(Module, Args, Options) ->
    try
        {ok, LoadMemorySize} = application:get_env(?CLUSTER_WORKER_APP_NAME, worker_load_memory_size),
        WorkerSupervisorName = ?WORKER_HOST_SUPERVISOR_NAME(Module),
        
        case lists:member(worker_first, Options) of
            true ->
                case supervisor:start_child(
                    ?MAIN_WORKER_SUPERVISOR_NAME,
                    {Module, {worker_host, start_link,
                        [Module, Args, LoadMemorySize]}, transient,
                        proplists:get_value(terminate_timeout, Options, ?DEFAULT_TERMINATE_TIMEOUT),
                        worker, [worker_host]}
                ) of
                    {ok, _} -> ok;
                    {error, {already_started, _}} ->
                        ?warning("Module ~p already started", [Module]),
                        ok
                end,
                case supervisor:start_child(
                    ?MAIN_WORKER_SUPERVISOR_NAME,
                    {WorkerSupervisorName, {worker_host_sup, start_link,
                        [WorkerSupervisorName, Args]}, transient, infinity,
                        supervisor, [worker_host_sup]}
                ) of
                    {ok, _} -> ok;
                    {error, {already_started, _}} ->
                        ?warning("Module supervisor ~p already started", [Module]),
                        ok
                end;
            _ ->
                case supervisor:start_child(
                    ?MAIN_WORKER_SUPERVISOR_NAME,
                    {WorkerSupervisorName, {worker_host_sup, start_link,
                        [WorkerSupervisorName, Args]}, transient, infinity,
                        supervisor, [worker_host_sup]}
                ) of
                    {ok, _} -> ok;
                    {error, {already_started, _}} ->
                        ?warning("Module supervisor ~p already started", [Module]),
                        ok
                end,
                case supervisor:start_child(
                    ?MAIN_WORKER_SUPERVISOR_NAME,
                    {Module, {worker_host, start_link,
                        [Module, Args, LoadMemorySize]}, transient,
                        proplists:get_value(terminate_timeout, Options, ?DEFAULT_TERMINATE_TIMEOUT),
                        worker, [worker_host]}
                ) of
                    {ok, _} -> ok;
                    {error, {already_started, _}} ->
                        ?warning("Module ~p already started", [Module]),
                        ok
                end
        end,
        ?info("Worker: ~s started", [Module]),
        ok
    catch
        _:Error:Stacktrace ->
            ?error_stacktrace("Error: ~p during start of worker: ~s", [Error, Module], Stacktrace),
            {error, Error}
    end.

-spec reschedule_service_healthcheck(node(), internal_services_manager:unique_service_name(),
    internal_services_manager:node_id(), internal_service:healthcheck_interval()) -> ok.
reschedule_service_healthcheck(Node, ServiceName, MasterNodeId, NewInterval) ->
    schedule_service_healthcheck(Node, override, make_ref(), ServiceName, MasterNodeId, NewInterval).

%% @private
-spec schedule_service_healthcheck(node() | pid(), Strategy :: override | continue,
    Generation :: reference(), internal_services_manager:unique_service_name(),
    internal_services_manager:node_id(), internal_service:healthcheck_interval()) -> ok.
schedule_service_healthcheck(NodeOrPid, Strategy, Generation, ServiceName, MasterNodeId, NewInterval) ->
    GenServerName = case NodeOrPid of
        Pid when is_pid(Pid) -> Pid;
        Node when is_atom(Node) -> {?NODE_MANAGER_NAME, Node}
    end,
    gen_server2:cast(
        GenServerName,
        {schedule_service_healthcheck, Strategy, Generation, ServiceName, MasterNodeId, NewInterval}
    ).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State}
    | {ok, State, Timeout}
    | {ok, State, hibernate}
    | {stop, Reason :: term()}
    | ignore,
    State :: state(),
    Timeout :: non_neg_integer() | infinity.
init([]) ->
    process_flag(trap_exit, true),
    try
        ?init_exometer_reporters(false),
        exometer_utils:init_exometer_counters(),
        catch ets:new(?HELPER_ETS, [named_table, public, set]),
        
        ?info("Running 'before_init' procedures."),
        ok = ?CALL_PLUGIN(before_init, []),
        
        ?info("node manager plugin initialized"),
        
        ?info("Checking if all ports are free..."),
        lists:foreach(
            fun(Module) ->
                Port = erlang:apply(Module, port, []),
                case ensure_port_free(Port) of
                    ok ->
                        ok;
                    {error, Reason} ->
                        ?error("The port ~B for ~p is not free: ~p. Terminating.",
                            [Port, Module, Reason]),
                        throw({port_in_use, Port})
                end
            end, node_manager:listeners()),
        
        ?info("Ports OK"),
        
        next_task_check(),
        erlang:send_after(datastore_throttling:plan_next_throttling_check(), self(), {timer, configure_throttling}),
        {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, memory_check_interval_seconds),
        {ok, ExometerInterval} = application:get_env(?CLUSTER_WORKER_APP_NAME, exometer_check_interval),
        erlang:send_after(ExometerInterval, self(), {timer, check_exometer}),
        erlang:send_after(timer:seconds(Interval), self(), {timer, check_memory}),
        ?info("All checks performed"),
        
        gen_server2:cast(self(), connect_to_cm),
        MonitoringState = monitoring:start(),
        
        {ok, #state{
            cm_con_status = not_connected,
            monitoring_state = MonitoringState
        }}
    catch
        _:Error:Stacktrace ->
            ?error_stacktrace("Cannot start node_manager: ~p", [Error], Stacktrace),
            {stop, cannot_start_node_manager}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: state()) -> Result when
    Result :: {reply, Reply, NewState}
    | {reply, Reply, NewState, Timeout}
    | {reply, Reply, NewState, hibernate}
    | {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason, Reply, NewState}
    | {stop, Reason, NewState},
    Reply :: cluster_status:status() | term(),
    NewState :: state(),
    Timeout :: non_neg_integer() | infinity,
    Reason :: term().

handle_call(healthcheck, _From, State) ->
    {reply, perform_healthcheck(node_manager, State), State};

handle_call({healthcheck, Component}, _From, State) ->
    {reply, perform_healthcheck(Component, State), State};

handle_call(are_db_and_workers_ready, _From, State) ->
    {reply, State#state.db_and_workers_ready, State};

handle_call(disable_task_control, _From, State) ->
    {reply, ok, State#state{task_control = false}};

handle_call(enable_task_control, _From, State) ->
    {reply, ok, State#state{task_control = true}};

handle_call(disable_throttling, _From, State) ->
    {reply, ok, State#state{throttling = false}};

handle_call(enable_throttling, _From, State) ->
    {reply, ok, State#state{throttling = true}};

%% Generic function apply in node_manager's process
handle_call({apply, M, F, A}, _From, State) ->
    {reply, apply(M, F, A), State};

handle_call(Request, From, State) ->
    ?CALL_PLUGIN(handle_call, [Request, From, State]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: state(),
    Timeout :: non_neg_integer() | infinity.
handle_cast(connect_to_cm, State) ->
    NewState = connect_to_cm(State),
    {noreply, NewState};

handle_cast(?INIT_STEP_MSG(Step), State) ->
    try
        case {Step, cluster_init_step(Step)} of
            % end of cluster init procedure
            {?CLUSTER_READY, ok} -> ok;
            % report the result to cluster manager
            {_, ok} -> report_step_result(Step, success);
            % result will be reported by the async process that is handling the step
            {_, async} -> ok
        end
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Error during cluster initialization in step ~p: ~p:~p",
                [Step, Error, Reason], Stacktrace),
            report_step_result(Step, failure)
    end,
    {noreply, State};

handle_cast(report_db_and_workers_ready, State) ->
    {noreply, State#state{db_and_workers_ready = true}};

handle_cast(report_cluster_ready, State) ->
    {noreply, State#state{cluster_ready = true}};

handle_cast(configure_throttling, #state{throttling = true} = State) ->
    try
        ok = datastore_throttling:configure_throttling()
    catch
        Error:Reason:Stacktrace ->
            ?warning_stacktrace("configure_throttling failed due to ~p:~p", [Error, Reason], Stacktrace)
    end,
    {noreply, State};

handle_cast(configure_throttling, State) ->
    {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_check_interval_seconds),
    erlang:send_after(timer:seconds(Interval), self(), {timer, configure_throttling}),
    {noreply, State};

handle_cast(check_memory, State) ->
    {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, memory_check_interval_seconds),
    erlang:send_after(timer:seconds(Interval), self(), {timer, check_memory}),
    spawn(fun() ->
        ?CALL_PLUGIN(clear_memory, [false])
    end),
    {noreply, State};

handle_cast(check_exometer, State) ->
    {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, exometer_check_interval),
    erlang:send_after(Interval, self(), {timer, check_exometer}),
    spawn(fun() ->
        ?init_exometer_reporters
    end),
    {noreply, State};

handle_cast(check_tasks, #state{task_control = TC} = State) ->
    case TC of
        true ->
            spawn(task_manager, check_and_rerun_all, []);
        _ ->
            ok
    end,
    next_task_check(),
    {noreply, State};

handle_cast(force_check_tasks, State) ->
    spawn(task_manager, check_and_rerun_all, []),
    {noreply, State};

handle_cast(do_heartbeat, #state{cm_con_status = Status} = State) ->
    Self = self(),
    spawn(fun() ->
        {NewMonState, NewLSA} = do_heartbeat(State),
        gen_server2:cast(?NODE_MANAGER_NAME, {heartbeat_state_update, {NewMonState, NewLSA}}),
        {ok, Interval} = case Status of
            registered ->
                application:get_env(?CLUSTER_WORKER_APP_NAME, heartbeat_registered_interval);
            _ ->
                application:get_env(?CLUSTER_WORKER_APP_NAME, heartbeat_interval)
        end,
        erlang:send_after(Interval, Self, {timer, do_heartbeat})
    end),
    {noreply, State};

handle_cast({heartbeat_state_update, {NewMonState, NewLSA}}, State) ->
    {noreply, State#state{monitoring_state = NewMonState, last_state_analysis = NewLSA}};

handle_cast({service_healthcheck, Generation, ServiceName, MasterNodeId, LastInterval}, State) ->
    case maps:get(ServiceName, State#state.service_healthcheck_generations) of
        Generation ->
            % run the healthcheck asynchronously to allow calling the node_manager from within the healthcheck procedure
            NodeManager = self(),
            spawn(fun() ->
                case internal_services_manager:do_healthcheck(ServiceName, MasterNodeId, LastInterval) of
                    {ok, NewInterval} ->
                        schedule_service_healthcheck(
                            NodeManager, continue, Generation, ServiceName, MasterNodeId, NewInterval
                        );
                    ignore ->
                        ok
                end
            end);
        _ ->
            % ignore as the generation has changed in the meantime -> healthchecks have been rescheduled
            ok
    end,
    {noreply, State};

handle_cast({schedule_service_healthcheck, override, Generation, ServiceName, MasterNodeId, Interval}, State) ->
    NewState = State#state{service_healthcheck_generations = maps:put(
        ServiceName, Generation, State#state.service_healthcheck_generations
    )},
    handle_cast({schedule_service_healthcheck, continue, Generation, ServiceName, MasterNodeId, Interval}, NewState);

handle_cast({schedule_service_healthcheck, continue, Generation, ServiceName, MasterNodeId, Interval}, State) ->
    erlang:send_after(Interval, ?NODE_MANAGER_NAME,
        {timer, {service_healthcheck, Generation, ServiceName, MasterNodeId, Interval}}
    ),
    {noreply, State};

handle_cast(?UPDATE_LB_ADVICES(Advices), State) ->
    NewState = update_lb_advices(State, Advices),
    {noreply, NewState};

handle_cast({update_scheduler_info, SI}, State) ->
    {noreply, State#state{scheduler_info = SI}};

handle_cast(?FORCE_STOP(ReasonMsg), State) ->
    ?critical("Received stop signal from cluster manager: ~s", [ReasonMsg]),
    ?critical("Force stopping application..."),
    init:stop(),
    {stop, normal, State};

handle_cast(?NODE_DOWN(Node), State) ->
    handle_node_status_change_async(Node, node_down, fun() ->
        ok = case ha_management:node_down(Node) of
            master -> plugins:apply(node_manager_plugin, master_node_down, [Node]);
            non_master -> ok % Failed node is not master for this node - ignore
        end
    end),
    {noreply, State};

handle_cast(?NODE_UP(Node), State) ->
    handle_node_status_change_async(Node, node_up, fun() ->
        ok = case ha_management:node_up(Node) of
            master -> plugins:apply(node_manager_plugin, master_node_up, [Node]);
            non_master -> ok % Recovered node is not master for this node - ignore
        end,
        gen_server2:cast({global, ?CLUSTER_MANAGER}, ?RECOVERY_ACKNOWLEDGED(node(), Node))
    end),
    {noreply, State};

handle_cast(?NODE_READY(Node), State) ->
    handle_node_status_change_async(Node, node_ready, fun() ->
        ok = case ha_management:node_ready(Node) of
            master -> plugins:apply(node_manager_plugin, master_node_ready, [Node]);
            non_master -> ok % Recovered node is not master for this node - ignore
        end
    end),
    {noreply, State};

handle_cast(?INITIALIZE_RECOVERY, State) ->
    initialize_recovery(),
    {noreply, State};

handle_cast(?FINALIZE_RECOVERY, State) ->
    finalize_recovery(),
    {noreply, State};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(Request, State) ->
    ?CALL_PLUGIN(handle_cast, [Request, State]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout | term(), State :: state()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: state(),
    Timeout :: non_neg_integer() | infinity.
handle_info({timer, Msg}, State) ->
    gen_server2:cast(?NODE_MANAGER_NAME, Msg),
    {noreply, State};

handle_info({nodedown, Node}, State) ->
    {ok, CMNodes} = ?CALL_PLUGIN(cm_nodes, []),
    case lists:member(Node, CMNodes) of
        false ->
            ?warning("Node manager received unexpected nodedown msg: ~p",
                [{nodedown, Node}]);
        true ->
            ok
    end,
    {noreply, State};

handle_info(Request, State) ->
    ?CALL_PLUGIN(handle_info, [Request, State]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason, State :: state()) -> Any :: term() when
    Reason :: normal
    | shutdown
    | {shutdown, term()}
    | term().
terminate(Reason, State) ->
    % TODO VFS-6339 - Unregister node during normal stop not to start HA procedures
    ?info("Shutting down ~p due to ~p", [?MODULE, Reason]),
    
    lists:foreach(fun(Module) ->
        try
            erlang:apply(Module, stop, [])
        catch
            E1:E2:Stacktrace ->
                ?warning_stacktrace("Stop failed on module ~p: ~p:~p",
                    [Module, E1, E2], Stacktrace)
        end
    end, node_manager:listeners()),
    ?info("All listeners stopped"),
    
    ?CALL_PLUGIN(terminate, [Reason, State]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term()},
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term().
code_change(OldVsn, State, Extra) ->
    ?CALL_PLUGIN(code_change, [OldVsn, State, Extra]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Connects with cluster manager and tells that the node is alive.
%% First it establishes network connection, next sends message to cluster manager.
%% @end
%%--------------------------------------------------------------------
-spec connect_to_cm(State :: state()) -> state().
connect_to_cm(State = #state{cm_con_status = connected}) ->
    % Already connected, do nothing
    State;
connect_to_cm(State = #state{cm_con_status = not_connected}) ->
    % Not connected to cluster manager, try and automatically schedule the next try
    {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, cm_connection_retry_period),
    erlang:send_after(Interval, self(), {timer, connect_to_cm}),
    case whereis(?MAIN_WORKER_SUPERVISOR_NAME) of
        undefined ->
            % Main application did not started workers supervisor
            ?debug("Workers supervisor not started, retrying in ~p ms", [Interval]),
            State#state{cm_con_status = not_connected};
        _ ->
            {ok, CMNodes} = ?CALL_PLUGIN(cm_nodes, []),
            case (catch init_net_connection(CMNodes)) of
                ok ->
                    ?info("Initializing connection to cluster manager"),
                    gen_server2:cast({global, ?CLUSTER_MANAGER}, {cm_conn_req, node()}),
                    State#state{cm_con_status = connected};
                Err ->
                    ?debug("No connection with cluster manager: ~p, retrying in ~p ms", [Err, Interval]),
                    State#state{cm_con_status = not_connected}
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Performs all cluster init operations in given step.
%% @end
%%--------------------------------------------------------------------
-spec cluster_init_step(cluster_manager_server:cluster_init_step()) -> ok | async.
cluster_init_step(?INIT_CONNECTION) ->
    ?info("Successfully connected to cluster manager"),
    ?info("Starting regular heartbeat to cluster manager"),
    gen_server2:cast(self(), do_heartbeat),
    ok;
cluster_init_step(?START_DEFAULT_WORKERS) ->
    ?info("Starting default workers..."),
    init_workers(cluster_worker_modules()),
    ?info("Default workers started successfully"),
    ok;
cluster_init_step(?PREPARE_FOR_UPGRADE) ->
    ?info("Preparing cluster for upgrade..."),
    ?CALL_PLUGIN(before_cluster_upgrade, []),
    ?info("The cluster is ready for upgrade"),
    ok;
cluster_init_step(?UPGRADE_CLUSTER) ->
    case node() == consistent_hashing:get_assigned_node(?UPGRADE_CLUSTER) of
        true ->
            spawn(fun() ->
                Result = try
                    upgrade_cluster(),
                    success
                catch Type:Reason:Stacktrace ->
                    ?error_stacktrace("Failed to upgrade cluster - ~p:~p", [
                        Type, Reason
                    ], Stacktrace),
                    failure
                end,
                report_step_result(?UPGRADE_CLUSTER, Result)
            end),
            async;
        false ->
            ok
    end;
cluster_init_step(?START_CUSTOM_WORKERS) ->
    ?info("Starting custom workers..."),
    Workers = ?CALL_PLUGIN(custom_workers, []),
    init_workers(Workers),
    ?info("Custom workers started successfully"),
    ok;
cluster_init_step(?DB_AND_WORKERS_READY) ->
    gen_server2:cast(?NODE_MANAGER_NAME, report_db_and_workers_ready),
    ?info("Database and workers ready - executing 'on_db_and_workers_ready' procedures..."),
    % the procedures require calls to node manager, hence they are processed asynchronously
    spawn(fun() ->
        Result = try
            ok = ?CALL_PLUGIN(on_db_and_workers_ready, []),
            ?info("Successfully executed 'on_db_and_workers_ready' procedures"),
            success
        catch Type:Reason:Stacktrace ->
            ?error_stacktrace("Failed to execute 'on_db_and_workers_ready' procedures - ~p:~p", [
                Type, Reason
            ], Stacktrace),
            failure
        end,
        report_step_result(?DB_AND_WORKERS_READY, Result)
    end),
    async;
cluster_init_step(?START_LISTENERS) ->
    lists:foreach(fun(Module) ->
        ok = erlang:apply(Module, start, [])
    end, node_manager:listeners());
cluster_init_step(?CLUSTER_READY) ->
    ?info("Cluster initialized successfully"),
    gen_server2:cast(?NODE_MANAGER_NAME, report_cluster_ready),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Performs necessary upgrades if cluster is not in newest generation.
%% Executed by one node in cluster.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_cluster() -> ok | no_return().
upgrade_cluster() ->
    case get_current_cluster_generation() of
        {error, _} = Error ->
            ?error("Error when retrieving current cluster generation: ~p", [Error]),
            throw(Error);
        {ok, CurrentGen} ->
            AllClusterGens = ?CALL_PLUGIN(cluster_generations, []),
            OldestUpgradableGen = ?CALL_PLUGIN(oldest_upgradable_cluster_generation, []),
            {InstalledGen, _} = lists:last(AllClusterGens),
            OldestUpgradableVersion = kv_utils:get(OldestUpgradableGen, AllClusterGens, <<>>),
            upgrade_cluster(CurrentGen, InstalledGen, {OldestUpgradableGen, OldestUpgradableVersion})
    end.

%% @private
-spec upgrade_cluster(CurrentGen :: cluster_generation(), InstalledGen :: cluster_generation(),
    {OldestKnownGen :: cluster_generation(), ReadableVersion :: binary()}) -> ok.
upgrade_cluster(CurrentGen, _InstalledGen, {OldestUpgradableGen, ReadableVersion}) when CurrentGen < OldestUpgradableGen ->
    ?critical("Cluster in too old version to be upgraded directly. Upgrade to intermediate version first."
    "~nOldest supported version: ~s", [ReadableVersion]),
    throw({error, too_old_cluster_generation});

upgrade_cluster(CurrentGen, InstalledGen, OldestUpgradableGen) when CurrentGen < InstalledGen ->
    ?info("Upgrading cluster from generation ~p to ~p...", [CurrentGen, InstalledGen]),
    {ok, NewGeneration} = ?CALL_PLUGIN(upgrade_cluster, [CurrentGen]),
    case NewGeneration > InstalledGen of
        true ->
            ?error("Cluster upgraded to too high generation ~p. Installed generation: ~p",
                [NewGeneration, InstalledGen]),
            throw({error, too_high_generation});
        false ->
            ok
    end,
    ?info("Cluster succesfully upgraded to generation ~p", [NewGeneration]),
    {ok, _} = cluster_generation:save(NewGeneration),
    upgrade_cluster(NewGeneration, InstalledGen, OldestUpgradableGen);

upgrade_cluster(CurrentGen, _InstalledGen, _OldestKnownGen) ->
    {ok, _} = cluster_generation:save(CurrentGen),
    ?info("No upgrade needed - the cluster is in newest generation").


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Performs healthcheck of given component.
%% Cluster manager connection check is only performed
%% by cluster manager during cluster initialization.
%% @end
%%--------------------------------------------------------------------
-spec perform_healthcheck(cluster_status:component(), state()) ->
    cluster_status:status() | [{module(), cluster_status:status()}].
perform_healthcheck(cluster_manager_connection, #state{cm_con_status = connected}) -> ok;
perform_healthcheck(cluster_manager_connection, _State) -> out_of_sync;
perform_healthcheck(node_manager, #state{cm_con_status = connected, cluster_ready = true}) -> ok;
perform_healthcheck(node_manager, _State) -> out_of_sync;
perform_healthcheck(dispatcher, _) -> gen_server2:call(?DISPATCHER_NAME, healthcheck);
perform_healthcheck(workers, _) ->
    lists:map(fun(WorkerName) -> {WorkerName, worker_proxy:call_direct(WorkerName, healthcheck)} end, modules());
perform_healthcheck(listeners, _) ->
    lists:map(fun(Listener) -> {Listener, apply(Listener, healthcheck, [])} end, listeners()).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Performs calls to cluster manager with heartbeat. The heartbeat message consists of
%% current monitoring data. The data is updated directly before sending.
%% The cluster manager will perform an 'update_lb_advices' cast periodically, using
%% newest node states from node managers for calculations.
%% @end
%%--------------------------------------------------------------------
-spec do_heartbeat(State :: state()) -> {monitoring:node_monitoring_state(), last_state_analysis()}.
do_heartbeat(#state{cm_con_status = connected, monitoring_state = MonState, last_state_analysis = LSA,
    scheduler_info = SchedulerInfo} = _State) ->
    NewMonState = monitoring:update(MonState),
    NewLSA = analyse_monitoring_state(NewMonState, SchedulerInfo, LSA),
    NodeState = monitoring:get_node_state(NewMonState),
    gen_server2:cast({global, ?CLUSTER_MANAGER}, {heartbeat, NodeState}),
    {NewMonState, NewLSA};

% Stop heartbeat if node_manager is not registered in cluster manager
do_heartbeat(#state{monitoring_state = MonState, last_state_analysis = LSA} = _State) ->
    {MonState, LSA}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Receives lb advices update from cluster manager and follows it to dispatcher.
%% @end
%%--------------------------------------------------------------------
-spec update_lb_advices(State :: state(), DispatcherAdvice :: load_balancing:dispatcher_lb_advice()) -> state().
update_lb_advices(State, DispatcherAdvice) ->
    gen_server2:cast(?DISPATCHER_NAME, {update_lb_advice, DispatcherAdvice}),
    State.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes network connection with cluster that contains nodes
%% given in argument.
%% @end
%%--------------------------------------------------------------------
-spec init_net_connection(Nodes :: list()) -> ok | {error, not_connected}.
init_net_connection([]) ->
    {error, not_connected};
init_net_connection([Node | Nodes]) ->
    case net_adm:ping(Node) of
        pong ->
            global:sync(),
            case global:whereis_name(?CLUSTER_MANAGER) of
                undefined ->
                    ?error("Connection to node ~p global_synch error", [Node]),
                    rpc:eval_everywhere(erlang, disconnect_node, [node()]),
                    {error, global_synch};
                _ ->
                    ?debug("Connection to node ~p initialized", [Node]),
                    erlang:monitor_node(Node, true),
                    ok
            end;
        pang ->
            ?error("Cannot connect to node ~p", [Node]),
            init_net_connection(Nodes)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Starts all workers on node.
%% @end
%%--------------------------------------------------------------------
-spec init_workers() -> ok.
init_workers() ->
    DefaultWorkers = cluster_worker_modules(),
    CustomWorkers = ?CALL_PLUGIN(custom_workers, []),
    init_workers(DefaultWorkers ++ CustomWorkers).


%%--------------------------------------------------------------------
%% @doc
%% Starts specified workers on node.
%% @end
%%--------------------------------------------------------------------
-spec init_workers(list()) -> ok.
init_workers(Workers) ->
    lists:foreach(fun
        ({Module, Args}) ->
            ok = start_worker(Module, Args);
        ({singleton, Module, Args}) ->
            case gen_server2:call({global, ?CLUSTER_MANAGER},
                {register_singleton_module, Module, node()}) of
                ok ->
                    ?info("Starting singleton module ~p", [Module]),
                    ok = start_worker(Module, Args);
                already_started ->
                    ok
            end;
        ({Module, Args, Options}) ->
            case proplists:get_value(prehook, Options) of
                undefined -> ok;
                Prehook -> ok = apply(Module, Prehook, [])
            end,
            ok = start_worker(Module, Args, Options),
            case proplists:get_value(posthook, Options) of
                undefined -> ok;
                Posthook -> ok = apply(Module, Posthook, [])
            end
    end, Workers),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Plans next tasks list checking.
%% @end
%%--------------------------------------------------------------------
-spec next_task_check() -> TimerRef :: reference().
next_task_check() ->
    {ok, IntervalMin} = application:get_env(?CLUSTER_WORKER_APP_NAME, task_checking_period_minutes),
    Interval = timer:minutes(IntervalMin),
    erlang:send_after(Interval, self(), {timer, check_tasks}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether port is free on localhost.
%% @TODO VFS-7847 Currently the listener ports are not freed correctly and after
%% a restart, they may still be not available for some time. This appears to be
%% triggered by listener healthcheck connections made by hackney, which causes
%% the listener to go into TIME_WAIT state for some duration.
%% @end
%%--------------------------------------------------------------------
-spec ensure_port_free(integer()) -> ok | {error, term()}.
ensure_port_free(Port) ->
    ensure_port_free(Port, ?PORT_CHECK_RETRIES).

%% @private
-spec ensure_port_free(integer(), integer()) -> ok | {error, term()}.
ensure_port_free(Port, AttemptsLeft) ->
    case gen_tcp:listen(Port, [{reuseaddr, true}, {ip, any}]) of
        {ok, Socket} ->
            gen_tcp:close(Socket);
        {error, Reason} ->
            case AttemptsLeft of
                1 ->
                    {error, Reason};
                _ ->
                    ?warning("Port ~B required by the application is not free, attempts left: ~B",
                        [Port, AttemptsLeft - 1]),
                    timer:sleep(?PORT_CHECK_INTERVAL),
                    ensure_port_free(Port, AttemptsLeft - 1)
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Analyse monitoring state and log result.
%% @end
%%--------------------------------------------------------------------
-spec analyse_monitoring_state(MonState :: monitoring:node_monitoring_state(),
    SchedulerInfo :: undefined | list(), last_state_analysis()) -> last_state_analysis().
analyse_monitoring_state(MonState, SchedulerInfo, {LastAnalysisTimer, LastAnalysisPid}) ->
    {CPU, Mem, PNum} = monitoring:erlang_vm_stats(MonState),
    MemInt = proplists:get_value(total, Mem, 0),
    
    {ok, MinInterval} = application:get_env(?CLUSTER_WORKER_APP_NAME, min_analysis_interval_sek),
    {ok, MaxInterval} = application:get_env(?CLUSTER_WORKER_APP_NAME, max_analysis_interval_min),
    {ok, MemThreshold} = application:get_env(?CLUSTER_WORKER_APP_NAME, node_mem_analysis_treshold),
    {ok, ProcThreshold} = application:get_env(?CLUSTER_WORKER_APP_NAME, procs_num_analysis_treshold),
    {ok, MemToStartCleaning} = application:get_env(?CLUSTER_WORKER_APP_NAME, node_mem_ratio_to_clear_cache),
    
    {ok, SchedulersMonitoring} = application:get_env(?CLUSTER_WORKER_APP_NAME, schedulers_monitoring),
    erlang:system_flag(scheduler_wall_time, SchedulersMonitoring),
    
    MemUsage = monitoring:mem_usage(MonState),
    
    ?update_counter(?EXOMETER_NAME(processes_num), PNum),
    ?update_counter(?EXOMETER_NAME(memory_erlang), MemInt),
    ?update_counter(?EXOMETER_NAME(memory_node), MemUsage),
    ?update_counter(?EXOMETER_NAME(cpu_node), CPU * 100),
    
    TimeDiff = stopwatch:read_millis(LastAnalysisTimer),
    MaxTimeExceeded = TimeDiff >= timer:minutes(MaxInterval),
    MinTimeExceeded = TimeDiff >= timer:seconds(MinInterval),
    MinThresholdExceeded = (MemInt >= MemThreshold) orelse
        (PNum >= ProcThreshold) orelse (MemUsage >= MemToStartCleaning),
    LastPidAlive = LastAnalysisPid =/= undefined andalso
        erlang:is_process_alive(LastAnalysisPid),
    case (MaxTimeExceeded orelse (MinTimeExceeded andalso MinThresholdExceeded))
        andalso not LastPidAlive of
        true ->
            Pid = spawn(fun() ->
                log_monitoring_stats("Monitoring state: ~p", [MonState]),
                log_monitoring_stats("Erlang ets mem usage: ~p", [
                    lists:reverse(lists:sort(lists:map(fun(N) ->
                        {ets:info(N, memory), ets:info(N, size), N} end, ets:all())))
                ]),
                
                % Gather processes info
                {ProcNum, {Top5M, Top5B, CS_Map, CS_Bin_Map}} =
                    get_procs_stats(),
                
                % Merge processes with similar stacktrace and find groups
                % that use a lot of memory
                MergedStacks = lists:sublist(lists:reverse(lists:sort(maps:values(CS_Map))), 5),
                
                MergedStacksMap2 = maps:map(fun(K, {M, N, K}) ->
                    {N, M, K} end, CS_Map),
                MergedStacks2 = lists:sublist(lists:reverse(lists:sort(maps:values(MergedStacksMap2))), 5),
                
                % Merge processes with similar stacktrace and find groups
                % that use a lot of memory to store binaries
                MergedStacksBin = lists:sublist(lists:reverse(lists:sort(maps:values(CS_Bin_Map))), 5),
                
                MergedStacksBinMap2 = maps:map(fun(K, {M, N, K}) ->
                    {N, M, K} end, CS_Bin_Map),
                MergedStacksBin2 = lists:sublist(lists:reverse(lists:sort(maps:values(MergedStacksBinMap2))), 5),
                
                All = erlang:registered(),
                
                TopProcesses = lists:map(
                    fun({M, {P, CS}}) ->
                        {M, CS, P, get_process_name(P, All),
                            erlang:process_info(P, current_function),
                            erlang:process_info(P, initial_call),
                            erlang:process_info(P, message_queue_len)}
                    end, lists:reverse(Top5M)),
                
                AddBL = application:get_env(?CLUSTER_WORKER_APP_NAME,
                    include_binary_list_in_monitoring_reoport, false),
                TopProcessesBin = case AddBL of
                    true ->
                        lists:map(
                            fun({M, {P, CS, BL}}) ->
                                {M, CS, P, get_process_name(P, All), BL,
                                    erlang:process_info(P, current_function),
                                    erlang:process_info(P, initial_call),
                                    erlang:process_info(P, message_queue_len)}
                            end, lists:reverse(Top5B));
                    _ ->
                        lists:map(
                            fun({M, {P, CS, _BL}}) ->
                                {M, CS, P, get_process_name(P, All),
                                    erlang:process_info(P, current_function),
                                    erlang:process_info(P, initial_call),
                                    erlang:process_info(P, message_queue_len)}
                            end, lists:reverse(Top5B))
                end,
                
                log_monitoring_stats("Erlang Procs stats:~n procs num: ~p~n single proc memory cosumption: ~p~n"
                "single proc memory cosumption (binary): ~p~n"
                "aggregated memory consumption: ~p~n"
                "simmilar procs: ~p~n"
                "aggregated memory consumption (binary): ~p~n"
                "simmilar procs (binary): ~p", [
                    ProcNum, TopProcesses, TopProcessesBin, MergedStacks,
                    MergedStacks2, MergedStacksBin, MergedStacksBin2
                ]),
                
                % Log schedulers info
                log_monitoring_stats("Schedulers basic info: all: ~p, online: ~p",
                    [erlang:system_info(schedulers), erlang:system_info(schedulers_online)]),
                NewSchedulerInfo0 = erlang:statistics(scheduler_wall_time),
                case is_list(NewSchedulerInfo0) of
                    true ->
                        NewSchedulerInfo = lists:sort(NewSchedulerInfo0),
                        gen_server2:cast(?NODE_MANAGER_NAME, {update_scheduler_info, NewSchedulerInfo}),
                        
                        log_monitoring_stats("Schedulers advanced info: ~p", [NewSchedulerInfo]),
                        case is_list(SchedulerInfo) of
                            true ->
                                Percent = lists:map(fun({{I, A0, T0}, {I, A1, T1}}) ->
                                    {I, (A1 - A0) / (T1 - T0)} end, lists:zip(SchedulerInfo, NewSchedulerInfo)),
                                {A, T} = lists:foldl(fun({{_, A0, T0}, {_, A1, T1}}, {Ai, Ti}) ->
                                    {Ai + (A1 - A0), Ti + (T1 - T0)} end, {0, 0}, lists:zip(SchedulerInfo, NewSchedulerInfo)),
                                Aggregated = A / T,
                                log_monitoring_stats("Schedulers utilization percent: ~p, aggregated: ~p", [Percent, Aggregated]);
                            _ ->
                                ok
                        end;
                    _ ->
                        ok
                end
            end),
            {stopwatch:start(), Pid};
        _ ->
            {LastAnalysisTimer, LastAnalysisPid}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets statistics for processes (processes number and processes that use
%% a lot of memory).
%% @end
%%--------------------------------------------------------------------
-spec get_procs_stats() -> {ProcNum :: non_neg_integer(),
    {Top5Mem :: [{Memory :: non_neg_integer(), Value :: term()}],
        Top5Bin :: [{MemoryBin :: non_neg_integer(), Value :: term()}],
        CS_Map :: #{}, CS_Bin_Map :: #{}}}.
get_procs_stats() ->
    Procs = erlang:processes(),
    PNum = length(Procs),
    
    {PNum, get_procs_stats(Procs, {[], [], #{}, #{}}, 0)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets statistics for processes that use a lot of memory.
%% @end
%%--------------------------------------------------------------------
-spec get_procs_stats([pid()],
    {TmpTop5Mem :: [{TmpMemory :: non_neg_integer(), TmpValue :: term()}],
        TmpTop5Bin :: [{TmpMemoryBin :: non_neg_integer(), TmpValue :: term()}],
        TmpCS_Map :: #{}, TmpCS_Bin_Map :: #{}}, Count :: non_neg_integer()) ->
    {Top5Mem :: [{Memory :: non_neg_integer(), Value :: term()}],
        Top5Bin :: [{MemoryBin :: non_neg_integer(), Value :: term()}],
        CS_Map :: #{}, CS_Bin_Map :: #{}}.
get_procs_stats([], Ans, _Count) ->
    Ans;
get_procs_stats([P | Procs], {Top5Mem, Top5Bin, CS_Map, CS_Bin_Map}, Count) ->
    CS = erlang:process_info(P, current_stacktrace),
    
    ProcMem = case erlang:process_info(P, memory) of
        {memory, M} ->
            M;
        _ ->
            0
    end,
    
    {Bin, BinList} = case erlang:process_info(P, binary) of
        {binary, BL} ->
            {get_binary_size(BL), BL};
        _ ->
            {0, []}
    end,
    
    Top5Mem2 = top_five(ProcMem, {P, CS}, Top5Mem),
    Top5Bin2 = top_five(Bin, {P, CS, BinList}, Top5Bin),
    CS_Map2 = merge_stacks(CS, ProcMem, CS_Map),
    CS_Bin_Map2 = merge_stacks(CS, Bin, CS_Bin_Map),
    
    GC_Interval = application:get_env(?CLUSTER_WORKER_APP_NAME,
        proc_stats_analysis_gc_interval, 25000),
    case Count rem GC_Interval of
        0 ->
            erlang:garbage_collect();
        _ ->
            ok
    end,
    
    get_procs_stats(Procs, {Top5Mem2, Top5Bin2, CS_Map2, CS_Bin_Map2},
        Count + 1).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sums utilization of memory of processes with similar stacktrace.
%% @end
%%--------------------------------------------------------------------
-spec top_five(Memory :: non_neg_integer(), Value :: term(),
    Acc :: [{non_neg_integer(), term()}]) ->
    NewAcc :: [{non_neg_integer(), term()}].
top_five(M, Value, []) ->
    [{M, Value}];
top_five(M, Value, [{Min, _} | Top4] = Top5) ->
    case length(Top5) < 5 of
        true -> lists:sort([{M, Value} | Top5]);
        _ ->
            case M > Min of
                true ->
                    lists:sort([{M, Value} | Top4]);
                _ ->
                    Top5
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sums utilization of memory of processes with similar stacktrace.
%% @end
%%--------------------------------------------------------------------
-spec merge_stacks(Stacktrace :: term(), Memory :: non_neg_integer(),
    Acc :: #{}) -> NewAcc :: #{}.
merge_stacks(CS, M, Map) ->
    case CS of
        {current_stacktrace, K} ->
            {M2, Num, _} = maps:get(K, Map, {0, 0, K}),
            maps:put(K, {M + M2, Num + 1, K}, Map);
        _ ->
            Map
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to find process name.
%% @end
%%--------------------------------------------------------------------
-spec get_process_name(pid(), [atom()]) -> atom().
get_process_name(P, All) ->
    lists:foldl(fun(Name, Acc) ->
        case whereis(Name) of
            P ->
                Name;
            _ ->
                Acc
        end
    end, non, All).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns sum of sizes of binaries listed by process_info.
%% @end
%%--------------------------------------------------------------------
-spec get_binary_size(list()) -> non_neg_integer().
get_binary_size(BinList) ->
    lists:foldl(fun({_, S, _}, Acc) ->
        S + Acc
    end, 0, BinList).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Log monitoring result.
%% @end
%%--------------------------------------------------------------------
-spec log_monitoring_stats(Format :: io:format(), Args :: [term()]) -> ok.
log_monitoring_stats(Format, Args) ->
    LogFile = application:get_env(?CLUSTER_WORKER_APP_NAME, monitoring_log_file,
        "/tmp/node_manager_monitoring.log"),
    
    log_monitoring_stats(LogFile, Format, Args).


%%--------------------------------------------------------------------
%% @doc
%% Returns node's IP address.
%% @end
%%--------------------------------------------------------------------
-spec get_ip_address() -> inet:ip4_address().
get_ip_address() ->
    case application:get_env(?CLUSTER_WORKER_APP_NAME, external_ip, undefined) of
        {_, _, _, _} = IP -> IP;
        IPAsList when is_list(IPAsList) ->
            {ok, IP} = inet:parse_ipv4strict_address(IPAsList),
            IP;
        undefined -> {127, 0, 0, 1}; % should be overriden by onepanel during deployment
        _ -> {127, 0, 0, 1}
    end.


-spec are_db_and_workers_ready() -> boolean().
are_db_and_workers_ready() ->
    % cache only the positive result as after the first initialization
    % this does not change anymore
    {ok, Result} = node_cache:acquire({?MODULE, ?FUNCTION_NAME}, fun() ->
        case gen_server2:call(?NODE_MANAGER_NAME, are_db_and_workers_ready) of
            true -> {ok, true, infinity};
            false -> {ok, false, 0}
        end
    end),
    Result.


%%--------------------------------------------------------------------
%% @doc
%% Fetches cluster status from cluster manager.
%% @end
%%--------------------------------------------------------------------
-spec get_cluster_status() ->
    {ok, {cluster_status:status(), [cluster_status:node_status()]}} | {error, term()}.
get_cluster_status() ->
    get_cluster_status(infinity).


-spec get_cluster_status(Timeout :: non_neg_integer() | infinity) ->
    {ok, {cluster_status:status(), [cluster_status:node_status()]}} | {error, term()}.
get_cluster_status(Timeout) ->
    gen_server2:call({global, ?CLUSTER_MANAGER}, cluster_status, Timeout).


%%--------------------------------------------------------------------
%% @doc
%% Get up to date information about IPs in the cluster.
%% @end
%%--------------------------------------------------------------------
-spec get_cluster_ips() -> [inet:ip4_address()] | no_return().
get_cluster_ips() ->
    lists:map(fun(Node) ->
        {_, _, _, _} = rpc:call(Node, ?MODULE, get_ip_address, [])
    end, consistent_hashing:get_all_nodes()).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves current cluster generation from database.
%% Defaults to newest known cluster generation.
%% @end
%%--------------------------------------------------------------------
-spec get_current_cluster_generation() -> {ok, cluster_generation()} | {error, term()}.
get_current_cluster_generation() ->
    case cluster_generation:get() of
        {ok, Generation} -> {ok, Generation};
        {error, not_found} ->
            AllGens = ?CALL_PLUGIN(cluster_generations, []),
            {InstalledGen, _} = lists:last(AllGens),
            {ok, InstalledGen};
        {error, _} = Error -> Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Reports to cluster manager that given cluster init step is finished
%% or step ended with failure.
%% @end
%%--------------------------------------------------------------------
-spec report_step_result(cluster_manager_server:cluster_init_step(), success | failure) -> ok.
report_step_result(Step, Result) ->
    gen_server2:cast({global, ?CLUSTER_MANAGER}, {cluster_init_step_report, node(), Step, Result}).

%%%===================================================================
%%% Node recovery handling
%%%===================================================================

-spec initialize_recovery() -> ok.
initialize_recovery() ->
    ?info("Starting phase 1/2 of node recovery"),
    gen_server2:cast(self(), do_heartbeat),
    cluster_init_step(?START_DEFAULT_WORKERS),
    gen_server2:cast({global, ?CLUSTER_MANAGER}, ?RECOVERY_INITIALIZED(node())),
    ?info("Phase 1/2 of node recovery finished successfully"),
    ok.

-spec finalize_recovery() -> ok.
finalize_recovery() ->
    ?info("Starting phase 2/2 of node recovery"),
    cluster_init_step(?PREPARE_FOR_UPGRADE),
    cluster_init_step(?START_CUSTOM_WORKERS),
    cluster_init_step(?START_LISTENERS),
    gen_server2:cast({global, ?CLUSTER_MANAGER}, ?RECOVERY_FINALIZED(node())),
    ?info("Phase 2/2 of node recovery finished successfully"),
    ok.

-spec handle_node_status_change_async(node(), NodeStatusNotificationType :: node_down | node_up | node_ready,
    HandlingFun :: fun(() -> ok)) -> ok.
handle_node_status_change_async(Node, NewStatus, HandlingFun) ->
    ?info("Started processing transition of node ~p to status ~p", [Node, NewStatus]),
    spawn(fun() ->
        try
            HandlingFun(),
            ?info("Finished processing transition of node ~p to status ~p", [Node, NewStatus])
        catch
            Error:Reason:Stacktrace ->
                ?error_stacktrace("Error while processing transition of node ~p to status ~p: ~p:~p",
                    [Node, NewStatus, Error, Reason], Stacktrace)
        end
    end),
    ok.

-spec is_cluster_healthy() -> boolean().
is_cluster_healthy() ->
    {_, {AppStatus, _}} = get_cluster_status(),
    case AppStatus of
        ok -> true;
        _ -> false
    end.
