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

%% Note: Workers start with order specified below. For this reason
%% all workers that need datastore_worker shall be after datastore_worker on
%% the list below.
-define(CLUSTER_WORKER_MODULES, [
    {early_init, datastore_worker, [
        {supervisor_flags, datastore_worker:supervisor_flags()},
        {supervisor_children_spec, datastore_worker:supervisor_children_spec()}
    ]},
    {early_init, tp_router, [
        {supervisor_flags, tp_router:supervisor_flags()},
        {supervisor_children_spec, tp_router:supervisor_children_spec()}
    ]},
    {dns_worker, []}
]).
-define(CLUSTER_WORKER_LISTENERS, [
    dns_listener,
    nagios_listener,
    redirector_listener
]).
% TODO - new drivers do not require two step init
-define(MODULES_HOOKS, [
    {{datastore_worker, early_init}, {datastore, ensure_state_loaded, []}},
    {{datastore_worker, init}, {datastore, cluster_initialized, []}}
]).

%% API
-export([start_link/0, stop/0, get_ip_address/0, refresh_ip_address/0,
    modules/0, listeners/0, cluster_worker_modules/0,
    cluster_worker_listeners/0, modules_hooks/0, init_report/0, init_counters/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% for tests
-export([init_workers/0]).

-define(EXOMETER_COUNTERS,
    [processes_num, memory_erlang, memory_node, cpu_node]).
-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE, Param)).
-define(EXOMETER_DEFAULT_TIME_SPAN, 600000).

%%%===================================================================
%%% API
%%%===================================================================

% TODO - dodac funkcje ktora inicjuje odpowiedni counter po tym jak zmieni sie
% zmienna srodowiskowa

%%--------------------------------------------------------------------
%% @doc
%% Initializes all counters.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
    TimeSpan = application:get_env(?CLUSTER_WORKER_APP_NAME,
        exometer_node_manager_time_span, ?EXOMETER_DEFAULT_TIME_SPAN),
    Counters = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), histogram, TimeSpan}
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
%% Use in plugins when specifying modules_with_args.
%% @end
%%--------------------------------------------------------------------
-spec cluster_worker_modules() -> Models :: [{atom(), [any()]}
    | {singleton | early_init, atom(), [any()]}].
cluster_worker_modules() -> ?CLUSTER_WORKER_MODULES.

%%--------------------------------------------------------------------
%% @doc
%% List of modules' hooks executed after module initialization (early or standard).
%% Use in plugins when specifying modules_hooks.
%% @end
%%--------------------------------------------------------------------
-spec modules_hooks() -> Hooks :: [{{Module :: atom(), early_init | init},
    {HookedModule :: atom(), Fun :: atom(), Args :: list()}}].
modules_hooks() -> ?MODULES_HOOKS.

%%--------------------------------------------------------------------
%% @doc
%% List of listeners provided by cluster-worker.
%% Use in plugins when specifying listeners.
%% @end
%%--------------------------------------------------------------------
-spec cluster_worker_listeners() -> Listeners :: [atom()].
cluster_worker_listeners() -> ?CLUSTER_WORKER_LISTENERS.

%%--------------------------------------------------------------------

%% @doc
%% List of loaded modules.
%% @end
%%--------------------------------------------------------------------
-spec modules() -> Models :: [atom()].
modules() ->
    lists:map(fun
        ({Module, _}) -> Module;
        ({singleton, Module, _}) -> Module;
        ({early_init, Module, _}) -> Module
    end, plugins:apply(node_manager_plugin, modules_with_args, [])).

%%--------------------------------------------------------------------
%% @doc
%% List of listeners loaded by node_manager.
%% @end
%%--------------------------------------------------------------------
-spec listeners() -> Listeners :: [atom()].
listeners() ->
    plugins:apply(node_manager_plugin, listeners, []).

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
%% Returns node's IP address.
%% @end
%%--------------------------------------------------------------------
get_ip_address() ->
    gen_server2:call(?NODE_MANAGER_NAME, get_ip_address).


%%--------------------------------------------------------------------
%% @doc
%% Tries to contact GR and refresh node's IP Address.
%% @end
%%--------------------------------------------------------------------
refresh_ip_address() ->
    gen_server2:cast(?NODE_MANAGER_NAME, refresh_ip_address).

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
    State :: term(),
    Timeout :: non_neg_integer() | infinity.
init([]) ->
    process_flag(trap_exit, true),
    try
        ?init_exometer_reporters(false),
        ?init_exometer_counters,
        ok = plugins:apply(node_manager_plugin, before_init, [[]]),

        ?info("Plugin initialised"),

        ?info("Checking if all ports are free..."),
        lists:foreach(
            fun(Module) ->
                Port = erlang:apply(Module, port, []),
                case check_port(Port) of
                    ok ->
                        ok;
                    {error, Reason} ->
                        ?error("The port ~B for ~p is not free: ~p. Terminating.",
                            [Port, Module, Reason]),
                        throw(ports_are_not_free)
                end
            end, node_manager:listeners()),

        ?info("Ports OK, starting listeners..."),

        lists:foreach(fun(Module) ->
            ok = erlang:apply(Module, start, []),
            ?info("Listener: ~p started", [Module])
        end, node_manager:listeners()),
        ?info("All listeners started"),

        next_task_check(),
        erlang:send_after(caches_controller:plan_next_throttling_check(), self(), {timer, configure_throttling}),
        {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, memory_check_interval_seconds),
        erlang:send_after(timer:seconds(Interval), self(), {timer, check_memory}),
        {ok, ExometerInterval} = application:get_env(?CLUSTER_WORKER_APP_NAME, exometer_check_interval_seconds),
        erlang:send_after(ExometerInterval, self(), {timer, check_exometer}),
        ?info("All checks performed"),

        gen_server2:cast(self(), connect_to_cm),

        NodeIP = plugins:apply(node_manager_plugin, check_node_ip_address, []),
        MonitoringState = monitoring:start(NodeIP),

        {ok, #state{node_ip = NodeIP,
            cm_con_status = not_connected,
            monitoring_state = MonitoringState}}
    catch
        _:Error ->
            ?error_stacktrace("Cannot start node_manager: ~p", [Error]),
            {stop, cannot_start_node_manager}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
    Result :: {reply, Reply, NewState}
    | {reply, Reply, NewState, Timeout}
    | {reply, Reply, NewState, hibernate}
    | {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason, Reply, NewState}
    | {stop, Reason, NewState},
    Reply :: nagios_handler:healthcheck_response() | term(),
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity,
    Reason :: term().

handle_call(healthcheck, _From, #state{cm_con_status = registered} = State) ->
    {reply, ok, State};
handle_call(healthcheck, _From, State) ->
    {reply, out_of_sync, State};

handle_call(get_ip_address, _From, State = #state{node_ip = IPAddress}) ->
    {reply, IPAddress, State};

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

handle_call(_Request, _From, State) ->
    plugins:apply(node_manager_plugin, handle_call_extension, [_Request, _From, State]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
handle_cast(connect_to_cm, State) ->
    NewState = connect_to_cm(State),
    {noreply, NewState};

handle_cast(cm_conn_ack, State) ->
    NewState = cm_conn_ack(State),
    {noreply, NewState};

handle_cast(cluster_init_finished, State) ->
    NewState = cluster_init_finished(State),
    {noreply, NewState};

handle_cast(configure_throttling, #state{throttling = true} = State) ->
    ok = caches_controller:configure_throttling(),
    {noreply, State};

handle_cast(configure_throttling, State) ->
    {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, throttling_check_interval_seconds),
    erlang:send_after(timer:seconds(Interval), self(), {timer, configure_throttling}),
    {noreply, State};

handle_cast(check_memory, State) ->
    {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, memory_check_interval_seconds),
    erlang:send_after(timer:seconds(Interval), self(), {timer, check_memory}),
    spawn(fun() ->
        plugins:apply(node_manager_plugin, clear_memory, [false])
    end),
    {noreply, State};

handle_cast(check_exometer, State) ->
    {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, exometer_check_interval_seconds),
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

handle_cast(do_heartbeat, State) ->
    Self = self(),
    spawn(fun() ->
        {NewMonState, NewLSA} = do_heartbeat(State),
        gen_server2:cast(?NODE_MANAGER_NAME, {heartbeat_state_update, {NewMonState, NewLSA}}),
        {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, heartbeat_interval),
        erlang:send_after(Interval, Self, {timer, do_heartbeat})
    end),
    {noreply, State};

handle_cast({heartbeat_state_update, {NewMonState, NewLSA}}, State) ->
    {noreply, State#state{monitoring_state = NewMonState, last_state_analysis = NewLSA}};

handle_cast(check_cluster_status, State) ->
    % Check if cluster is initialized - this must be done in different process
    % as healthcheck performs a gen_server call to node_manager and
    % calling it from node_manager would cause a deadlock.
    spawn(fun() ->
        {ok, Timeout} = application:get_env(
            ?CLUSTER_WORKER_APP_NAME, nagios_healthcheck_timeout),
        Status = case nagios_handler:get_cluster_status(Timeout) of
            {ok, {_AppName, ok, _NodeStatuses}} ->
                ok;
            {ok, {_AppName, GenericError, NodeStatuses}}  ->
                {error, {GenericError, NodeStatuses}};
            Error ->
                {error, Error}
        end,
        % Cast cluster status back to node manager
        gen_server2:cast(?NODE_MANAGER_NAME, {cluster_status, Status})
    end),
    {noreply, State};


handle_cast({cluster_status, _}, #state{initialized = true} = State) ->
    % Already initialized, do nothing
    {noreply, State};

handle_cast({cluster_status, CStatus}, #state{initialized = {false, TriesNum}} = State) ->
    % Not yet initialized, run after_init if cluster health is ok.
    case CStatus of
        ok ->
            % Cluster initialized, run after_init callback
            % of node_manager_plugin.
            ?info("Cluster initialized. Running 'after_init' procedures."),
            ok = plugins:apply(node_manager_plugin, after_init, [[]]),
            {noreply, State#state{initialized = true}};
        {error, Error} ->
            MaxChecksNum = application:get_env(?CLUSTER_WORKER_APP_NAME,
                cluster_status_max_checks_number, 30),
            case TriesNum < MaxChecksNum of
                true ->
                    % Cluster not yet initialized, try in a second.
                    ?debug("Cluster not initialized. Next check in a second."),
                    erlang:send_after(timer:seconds(1), self(), {timer, check_cluster_status}),
                    {noreply, State#state{initialized = {false, TriesNum + 1}}};
                _ ->
                    ?error("Stopping application. Reason: "
                        "cannot initialize cluster, status: ~p", [Error]),
                    init:stop(),
                    {stop, normal, State}
            end
    end;

handle_cast({update_lb_advices, Advices}, State) ->
    NewState = update_lb_advices(State, Advices),
    {noreply, NewState};

handle_cast({update_scheduler_info, SI}, State) ->
    {noreply, State#state{scheduler_info = SI}};

handle_cast(refresh_ip_address, #state{monitoring_state = MonState} = State) ->
    NodeIP = plugins:apply(node_manager_plugin, check_node_ip_address, []),
    NewMonState = monitoring:refresh_ip_address(NodeIP, MonState),
    {noreply, State#state{node_ip = NodeIP, monitoring_state = NewMonState}};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Request, State) ->
    plugins:apply(node_manager_plugin, handle_cast_extension, [_Request, State]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.

handle_info({timer, Msg}, State) ->
    gen_server2:cast(?NODE_MANAGER_NAME, Msg),
    {noreply, State};

handle_info({nodedown, Node}, State) ->
    {ok, CMNodes} = plugins:apply(node_manager_plugin, cm_nodes, []),
    case lists:member(Node, CMNodes) of
        false ->
            ?warning("Node manager received unexpected nodedown msg: ~p", [{nodedown, Node}]);
        true ->
            ok
        % TODO maybe node_manager should be restarted along with all workers to
        % avoid desynchronization of modules between nodes.
%%             ?error("Connection to cluster manager lost, restarting node"),
%%             throw(connection_to_cm_lost)
    end,
    {noreply, State};

handle_info(_Request, State) ->
    plugins:apply(node_manager_plugin, handle_info_extension, [_Request, State]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason, State :: term()) -> Any :: term() when
    Reason :: normal
    | shutdown
    | {shutdown, term()}
    | term().
terminate(_Reason, _State) ->
    ?info("Shutting down ~p due to ~p", [?MODULE, _Reason]),

    lists:foreach(fun(Module) ->
        erlang:apply(Module, stop, []) end, node_manager:listeners()),
    ?info("All listeners stopped"),

    plugins:apply(node_manager_plugin, on_terminate, [_Reason, _State]).

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
code_change(_OldVsn, State, _Extra) ->
    plugins:apply(node_manager_plugin, on_code_change, [_OldVsn, State, _Extra]).

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
-spec connect_to_cm(State :: #state{}) -> #state{}.
connect_to_cm(State = #state{cm_con_status = registered}) ->
    % Already registered, do nothing
    State;
connect_to_cm(State = #state{cm_con_status = connected}) ->
    % Connected, but not registered (workers did not start), check again in some time
    {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, cm_connection_retry_period),
    gen_server2:cast({global, ?CLUSTER_MANAGER}, {cm_conn_req, node()}),
    erlang:send_after(Interval, self(), {timer, connect_to_cm}),
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
            {ok, CMNodes} = plugins:apply(node_manager_plugin, cm_nodes, []),
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
%% Saves information about cluster manager connection when cluster manager answers to its request
%% @end
%%--------------------------------------------------------------------
-spec cm_conn_ack(State :: term()) -> #state{}.
cm_conn_ack(State = #state{cm_con_status = connected}) ->
    ?info("Successfully connected to cluster manager"),
    init_node(),
    ?info("Node initialized"),
    gen_server2:cast({global, ?CLUSTER_MANAGER}, {init_ok, node()}),
    {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, heartbeat_interval),
    erlang:send_after(Interval, self(), {timer, do_heartbeat}),
    self() ! {timer, check_cluster_status},
    State#state{cm_con_status = registered};
cm_conn_ack(State) ->
    % Already registered or not connected, do nothing
    ?warning("node_manager received redundant cm_conn_ack"),
    State.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Receives information that cluster has been successfully initialized.
%% @end
%%--------------------------------------------------------------------
-spec cluster_init_finished(State :: term()) -> #state{}.
cluster_init_finished(State) ->
    ?info("Cluster sucessfully initialized"),
    init_workers(),
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Performs calls to cluster manager with heartbeat. The heartbeat message consists of
%% current monitoring data. The data is updated directly before sending.
%% The cluster manager will perform an 'update_lb_advices' cast perodically, using
%% newest node states from node managers for calculations.
%% @end
%%--------------------------------------------------------------------
-spec do_heartbeat(State :: #state{}) -> {monitoring:node_monitoring_state(), {integer(), integer(), integer()}}.
do_heartbeat(#state{cm_con_status = registered, monitoring_state = MonState, last_state_analysis = LSA,
    scheduler_info = SchedulerInfo} = _State) ->
    NewMonState = monitoring:update(MonState),
    NewLSA = analyse_monitoring_state(NewMonState, SchedulerInfo, LSA),
    NodeState = monitoring:get_node_state(NewMonState),
    ?debug("Sending heartbeat to cluster manager"),
    gen_server2:cast({global, ?CLUSTER_MANAGER}, {heartbeat, NodeState}),
    {NewMonState, NewLSA};

% Stop heartbeat if node_manager is not registered in cluster manager
do_heartbeat(#state{monitoring_state = MonState, last_state_analysis = LSA} = _State) ->
    {MonState, LSA}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Receives lb advices update from cluster manager and follows it to DNS worker and dispatcher.
%% @end
%%--------------------------------------------------------------------
-spec update_lb_advices(State :: #state{}, LBAdvices) -> #state{} when
    LBAdvices :: {load_balancing:dns_lb_advice(), load_balancing:dispatcher_lb_advice()}.
update_lb_advices(State, {DNSAdvice, DispatcherAdvice}) ->
    gen_server2:cast(?DISPATCHER_NAME, {update_lb_advice, DispatcherAdvice}),
    worker_proxy:call({dns_worker, node()}, {update_lb_advice, DNSAdvice}),
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
%% Init node as worker of oneprovider cluster
%% @end
%%--------------------------------------------------------------------
-spec init_node() -> ok.
init_node() ->
    early_init_workers().

%%--------------------------------------------------------------------
%% @doc
%% Starts workers that must be initialized before init of cluster ring.
%% @end
%%--------------------------------------------------------------------
-spec early_init_workers() -> ok.
early_init_workers() ->
    Hooks = plugins:apply(node_manager_plugin, modules_hooks, []),

    lists:foreach(fun
        ({early_init, Module, Args}) ->
            ok = start_worker(Module, Args),
            case proplists:get_value({Module, early_init}, Hooks) of
                {HookedModule, Fun, HookedArgs} ->
                    ok = apply(HookedModule, Fun, HookedArgs);
                _ -> ok
            end;
        (_) ->
            ok
    end, plugins:apply(node_manager_plugin, modules_with_args, [])),
    ?info("Early init finished"),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Starts all workers on node.
%% @end
%%--------------------------------------------------------------------
-spec init_workers() -> ok.
init_workers() ->
    Hooks = plugins:apply(node_manager_plugin, modules_hooks, []),

    lists:foreach(fun(ModuleDesc) ->
        InitializedModul = case ModuleDesc of
            {early_init, Module, _Args} ->
                Module;
            {Module, Args} ->
                ok = start_worker(Module, Args),
                Module;
            {singleton, Module, Args} ->
                case gen_server2:call({global, ?CLUSTER_MANAGER}, {register_singleton_module, Module, node()}) of
                    ok ->
                        ?info("Starting singleton module ~p", [Module]),
                        ok = start_worker(Module, Args);
                    already_started ->
                        ok
                end,
                Module
        end,
        case proplists:get_value({InitializedModul, init}, Hooks) of
            {HookedModule, Fun, HookedArgs} ->
                ok = apply(HookedModule, Fun, HookedArgs);
            _ -> ok
        end
    end, plugins:apply(node_manager_plugin, modules_with_args, [])),
    ?info("All workers started"),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts worker node with dedicated supervisor as brother. Both entities
%% are started under MAIN_WORKER_SUPERVISOR supervision.
%% @end
%%--------------------------------------------------------------------
-spec start_worker(Module :: atom(), Args :: term()) -> ok | {error, term()}.
start_worker(Module, Args) ->
    try
        {ok, LoadMemorySize} = application:get_env(?CLUSTER_WORKER_APP_NAME, worker_load_memory_size),
        WorkerSupervisorName = ?WORKER_HOST_SUPERVISOR_NAME(Module),
        {ok, _} = supervisor:start_child(
            ?MAIN_WORKER_SUPERVISOR_NAME,
            {WorkerSupervisorName, {worker_host_sup, start_link, [WorkerSupervisorName, Args]}, transient, infinity, supervisor, [worker_host_sup]}
        ),
        {ok, _} = supervisor:start_child(
            ?MAIN_WORKER_SUPERVISOR_NAME,
            {Module, {worker_host, start_link, [Module, Args, LoadMemorySize]}, transient, 5000, worker, [worker_host]}
        ),
        ?info("Worker: ~s started", [Module])
    catch
        _:Error ->
            ?error_stacktrace("Error: ~p during start of worker: ~s", [Error, Module]),
            {error, Error}
    end.


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
%% @end
%%--------------------------------------------------------------------
-spec check_port(Port :: integer()) -> ok | {error, Reason :: term()}.
check_port(Port) ->
    case gen_tcp:listen(Port, [{reuseaddr, true}]) of
        {ok, Socket} ->
            gen_tcp:close(Socket);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Analyse monitoring state and log result.
%% @end
%%--------------------------------------------------------------------
-spec analyse_monitoring_state(MonState :: monitoring:node_monitoring_state(), SchedulerInfo :: undefined | list(),
    LastAnalysisTime :: erlang:timestamp()) -> erlang:timestamp().
analyse_monitoring_state(MonState, SchedulerInfo, LastAnalysisTime) ->
    ?debug("Monitoring state: ~p", [MonState]),

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

    exometer:update(?EXOMETER_NAME(processes_num), PNum),
    exometer:update(?EXOMETER_NAME(memory_erlang), MemInt),
    exometer:update(?EXOMETER_NAME(memory_node), MemUsage),
    exometer:update(?EXOMETER_NAME(cpu_node), CPU * 100),

    Now = os:timestamp(),
    TimeDiff = timer:now_diff(Now, LastAnalysisTime) div 1000,
    case (TimeDiff >= timer:minutes(MaxInterval)) orelse
        ((TimeDiff >= timer:seconds(MinInterval)) andalso
            ((MemInt >= MemThreshold) orelse (PNum >= ProcThreshold) orelse (MemUsage >= MemToStartCleaning))) of
        true ->
            spawn(fun() ->
                log_monitoring_stats("Monitoring state: ~p", [MonState]),
                log_monitoring_stats("Erlang ets mem usage: ~p", [
                    lists:reverse(lists:sort(lists:map(fun(N) ->
                        {ets:info(N, memory), ets:info(N, size), N} end, ets:all())))
                ]),

                Procs = erlang:processes(),
                SortedProcs = lists:reverse(lists:sort(lists:map(fun(P) ->
                    {erlang:process_info(P, memory), P} end, Procs))),
                MergedStacksMap = lists:foldl(fun(P, Map) ->
                    case {erlang:process_info(P, current_stacktrace), erlang:process_info(P, memory)} of
                        {{current_stacktrace, K}, {memory, M}} ->
                            {M2, Num, _} = maps:get(K, Map, {0, 0, K}),
                            maps:put(K, {M + M2, Num + 1, K}, Map);
                        _ ->
                            Map
                    end
                end, #{}, Procs),
                MergedStacks = lists:sublist(lists:reverse(lists:sort(maps:values(MergedStacksMap))), 5),

                MergedStacksMap2 = maps:map(fun(K, {M, N, K}) ->
                    {N, M, K} end, MergedStacksMap),
                MergedStacks2 = lists:sublist(lists:reverse(lists:sort(maps:values(MergedStacksMap2))), 5),

                GetName = fun(P) ->
                    All = erlang:registered(),
                    lists:foldl(fun(Name, Acc) ->
                        case whereis(Name) of
                            P ->
                                Name;
                            _ ->
                                Acc
                        end
                    end, non, All)
                end,

                TopProcesses = lists:map(
                    fun({M, P}) ->
                        {M, erlang:process_info(P, current_stacktrace), erlang:process_info(P, message_queue_len),
                            erlang:process_info(P, stack_size), erlang:process_info(P, heap_size),
                            erlang:process_info(P, total_heap_size), P, GetName(P)}
                    end, lists:sublist(SortedProcs, 5)),
                log_monitoring_stats("Erlang Procs stats:~n procs num: ~p~n single proc memory cosumption: ~p~n "
                "aggregated memory consumption: ~p~n simmilar procs: ~p", [length(Procs),
                    TopProcesses, MergedStacks, MergedStacks2
                ]),

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
            Now;
        _ ->
            LastAnalysisTime
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Analyse monitoring state and log result.
%% @end
%%--------------------------------------------------------------------
-spec log_monitoring_stats(Format :: io:format(), Args :: [term()]) -> ok.
log_monitoring_stats(Format, Args) ->
    Now = os:timestamp(),
    {Date, Time} = lager_util:format_time(lager_util:maybe_utc(
        lager_util:localtime_ms(Now))),
    LogFile = application:get_env(?CLUSTER_WORKER_APP_NAME, monitoring_log_file,
        "/tmp/node_manager_monitoring.log"),
    file:write_file(LogFile,
        io_lib:format("~n~s, ~s: " ++ Format, [Date, Time | Args]), [append]),
    ok.
