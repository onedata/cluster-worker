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
-include("elements/node_manager/node_manager.hrl").
-include("elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").

-define(CLUSTER_WORKER_MODULES, [
    {datastore_worker, []},
    {dns_worker, []}
]).
-define(CLUSTER_WORKER_LISTENERS, [
    dns_listener,
    nagios_listener,
    redirector_listener
]).

%% API
-export([start_link/0, stop/0, get_ip_address/0, refresh_ip_address/0,
    modules/0, listeners/0, cluster_worker_modules/0, cluster_worker_listeners/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% List of modules provided by cluster-worker.
%% Use in plugins when specifying modules_with_args.
%% @end
%%--------------------------------------------------------------------
-spec cluster_worker_modules() -> Models :: [{atom(), [any()]}].
cluster_worker_modules() -> ?CLUSTER_WORKER_MODULES.

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
    lists:map(fun({Module, _}) ->
        Module end, plugins:apply(node_manager_plugin, modules_with_args, [])).

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
    gen_server:start_link({local, ?NODE_MANAGER_NAME}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Stops node manager
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
    gen_server:cast(?NODE_MANAGER_NAME, stop).


%%--------------------------------------------------------------------
%% @doc
%% Returns node's IP address.
%% @end
%%--------------------------------------------------------------------
get_ip_address() ->
    gen_server:call(?NODE_MANAGER_NAME, get_ip_address).


%%--------------------------------------------------------------------
%% @doc
%% Tries to contact GR and refresh node's IP Address.
%% @end
%%--------------------------------------------------------------------
refresh_ip_address() ->
    gen_server:cast(?NODE_MANAGER_NAME, refresh_ip_address).

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
        ok = plugins:apply(node_manager_plugin, on_init, [[]]),

        ?info("Plugin initailised"),

        ?info("Checking if all ports are free..."),
        lists:foreach(
            fun(Module) ->
                case erlang:apply(Module, healthcheck, []) of
                    {error, server_not_responding} ->
                        ok;
                    _ ->
                        ?error("The port ~B for ~p is not free. Terminating.",
                            [erlang:apply(Module, port, []), Module]),
                        throw(ports_are_not_free)
                end
            end, node_manager:listeners()),

        ?info("Ports OK, starting listeners..."),

        lists:foreach(fun(Module) ->
            ok = erlang:apply(Module, start, []) end, node_manager:listeners()),
        ?info("All listeners started"),

        next_mem_check(),
        next_task_check(),
        ?info("All checks performed"),

        gen_server:cast(self(), connect_to_cm),

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

handle_call(healthcheck, _From, State = #state{cm_con_status = ConnStatus}) ->
    Reply = case ConnStatus of
                registered -> ok;
                _ -> out_of_sync
            end,
    {reply, Reply, State};

handle_call(get_ip_address, _From, State = #state{node_ip = IPAddress}) ->
    {reply, IPAddress, State};

% only for tests
handle_call(check_mem_synch, _From, State) ->
    Ans = case monitoring:get_memory_stats() of
              [{<<"mem">>, MemUsage}] ->
                  case caches_controller:should_clear_cache(MemUsage) of
                      true ->
                          free_memory(MemUsage);
                      _ ->
                          ok
                  end;
              _ ->
                  cannot_check_mem_usage
          end,
    {reply, Ans, State};

% only for tests
handle_call(clear_mem_synch, _From, State) ->
    caches_controller:delete_old_keys(locally_cached, 0),
    caches_controller:delete_old_keys(globally_cached, 0),
    {reply, ok, State};

% only for tests
handle_call(force_clear_node, _From, State) ->
    A1 = caches_controller:delete_all_keys(locally_cached),
    A2 = caches_controller:delete_all_keys(globally_cached),
    task_manager:kill_all(),
    {reply, {A1, A2}, State};

handle_call(disable_cache_control, _From, State) ->
    {reply, ok, State#state{cache_control = false}};

handle_call(enable_cache_control, _From, State) ->
    {reply, ok, State#state{cache_control = true}};

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

handle_cast(check_mem, #state{monitoring_state = MonState, cache_control = CacheControl,
    last_cache_cleaning = Last} = State) when CacheControl =:= true ->
    MemUsage = monitoring:mem_usage(MonState),
    % Check if memory cleaning of oldest docs should be started
    % even when memory utilization is low (e.g. once a day)
    NewState = case caches_controller:should_clear_cache(MemUsage) of
                   true ->
                       spawn(fun() -> free_memory(MemUsage) end),
                       State#state{last_cache_cleaning = os:timestamp()};
                   _ ->
                       Now = os:timestamp(),
                       {ok, CleaningPeriod} = application:get_env(?CLUSTER_WORKER_APP_NAME, clear_cache_max_period_ms),
                       case timer:now_diff(Now, Last) >= 1000000 * CleaningPeriod of
                           true ->
                               spawn(fun() -> free_memory() end),
                               State#state{last_cache_cleaning = Now};
                           _ ->
                               State
                       end
               end,
    next_mem_check(),
    {noreply, NewState};

handle_cast(check_mem, State) ->
    next_mem_check(),
    {noreply, State};

handle_cast(check_tasks, State) ->
    spawn(task_manager, check_and_rerun_all, []),
    next_task_check(),
    {noreply, State};

handle_cast(do_heartbeat, State) ->
    NewState = do_heartbeat(State),
    {noreply, NewState};

handle_cast({update_lb_advices, Advices}, State) ->
    NewState = update_lb_advices(State, Advices),
    {noreply, NewState};

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
    gen_server:cast(?NODE_MANAGER_NAME, Msg),
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
    gen_server:cast({global, ?CLUSTER_MANAGER}, {cm_conn_req, node()}),
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
                    gen_server:cast({global, ?CLUSTER_MANAGER}, {cm_conn_req, node()}),
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
    gen_server:cast({global, ?CLUSTER_MANAGER}, {init_ok, node()}),
    {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, heartbeat_interval),
    erlang:send_after(Interval, self(), {timer, do_heartbeat}),
    State#state{cm_con_status = registered};
cm_conn_ack(State) ->
    % Already registered or not connected, do nothing
    ?warning("node_manager received redundant cm_conn_ack"),
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
-spec do_heartbeat(State :: #state{}) -> #state{}.
do_heartbeat(#state{cm_con_status = registered, monitoring_state = MonState} = State) ->
    {ok, Interval} = application:get_env(?CLUSTER_WORKER_APP_NAME, heartbeat_interval),
    erlang:send_after(Interval, self(), {timer, do_heartbeat}),
    NewMonState = monitoring:update(MonState),
    NodeState = monitoring:get_node_state(NewMonState),
    ?debug("Sending heartbeat to cluster manager"),
    gen_server:cast({global, ?CLUSTER_MANAGER}, {heartbeat, NodeState}),
    State#state{monitoring_state = NewMonState};

% Stop heartbeat if node_manager is not registered in cluster manager
do_heartbeat(State) ->
    State.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Receives lb advices update from cluster manager and follows it to DNS worker and dispatcher.
%% @end
%%--------------------------------------------------------------------
-spec update_lb_advices(State :: #state{}, LBAdvices) -> #state{} when
    LBAdvices :: {load_balancing:dns_lb_advice(), load_balancing:dispatcher_lb_advice()}.
update_lb_advices(State, {DNSAdvice, DispatcherAdvice}) ->
    gen_server:cast(?DISPATCHER_NAME, {update_lb_advice, DispatcherAdvice}),
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
    init_workers().

%%--------------------------------------------------------------------
%% @doc
%% Starts all workers on node, and notifies
%% cluster manager about successfull init
%% @end
%%--------------------------------------------------------------------
-spec init_workers() -> ok.
init_workers() ->
    lists:foreach(fun({Module, Args}) ->
        ok = start_worker(Module, Args),
        case Module of
            datastore_worker ->
                {ok, NodeToSync} = gen_server:call({global, ?CLUSTER_MANAGER}, get_node_to_sync),
                ok = datastore:ensure_state_loaded(NodeToSync),
                ?info("Datastore synchronized");
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
            {Module, {worker_host, start_link, [Module, Args, LoadMemorySize]}, transient, 5000, worker, [worker_host]}
        ),
        {ok, _} = supervisor:start_child(
            ?MAIN_WORKER_SUPERVISOR_NAME,
            {WorkerSupervisorName, {worker_host_sup, start_link, [WorkerSupervisorName, Args]}, transient, infinity, supervisor, [worker_host_sup]}
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
%% Clears memory caches.
%% @end
%%--------------------------------------------------------------------
-spec free_memory(NodeMem :: number()) -> ok | mem_usage_too_high | cannot_check_mem_usage | {error, term()}.
free_memory(NodeMem) ->
    try
        AvgMem = gen_server:call({global, ?CLUSTER_MANAGER}, get_avg_mem_usage),
        ClearingOrder = case NodeMem >= AvgMem of
                            true ->
                                [{false, locally_cached}, {false, globally_cached}, {true, locally_cached}, {true, globally_cached}];
                            _ ->
                                [{false, globally_cached}, {false, locally_cached}, {true, globally_cached}, {true, locally_cached}]
                        end,
        ?info("Clearing memory in order: ~p", [ClearingOrder]),
        lists:foldl(fun
            ({_Aggressive, _StoreType}, ok) ->
                ok;
            ({Aggressive, StoreType}, _) ->
                Ans = caches_controller:clear_cache(NodeMem, Aggressive, StoreType),
                case Ans of
                    mem_usage_too_high ->
                        ?warning("Not able to free enough memory clearing cache ~p with param ~p", [StoreType, Aggressive]);
                    _ ->
                        ok
                end,
                Ans
        end, start, ClearingOrder)
    catch
        E1:E2 ->
            ?error_stacktrace("Error during caches cleaning ~p:~p", [E1, E2]),
            {error, E2}
    end.

free_memory() ->
    try
        ClearingOrder = [{false, globally_cached}, {false, locally_cached}],
        lists:foreach(fun
            ({Aggressive, StoreType}) ->
                caches_controller:clear_cache(100, Aggressive, StoreType)
        end, ClearingOrder)
    catch
        E1:E2 ->
            ?error_stacktrace("Error during caches cleaning ~p:~p", [E1, E2]),
            {error, E2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Plans next memory checking.
%% @end
%%--------------------------------------------------------------------
-spec next_mem_check() -> TimerRef :: reference().
next_mem_check() ->
    {ok, IntervalMin} = application:get_env(?CLUSTER_WORKER_APP_NAME, check_mem_interval_minutes),
    Interval = timer:minutes(IntervalMin),
    % random to reduce probability that two nodes clear memory simultanosly
    erlang:send_after(crypto:rand_uniform(round(0.8 * Interval), round(1.2 * Interval)), self(), {timer, check_mem}).


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

