%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module is a gen_server that coordinates the 
%% life cycle of node. It starts/stops appropriate services (according
%% to node type) and communicates with ccm (if node works as worker).
%%
%% Node can be ccm or worker. However, worker_hosts can be also
%% started at ccm nodes.
%% @end
%% ===================================================================

-module(node_manager).
-behaviour(gen_server).
-include("registered_names.hrl").
-include("records.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/1, stop/0]).
-export([check_vsn/0]).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% start_link/1
%% ====================================================================
%% @doc Starts the server
-spec start_link(Type) -> Result when
  Type :: worker | ccm,
  Result ::  {ok,Pid}
  | ignore
  | {error,Error},
  Pid :: pid(),
  Error :: {already_started,Pid} | term().
%% ====================================================================

start_link(Type) ->
  gen_server:start_link({local, ?Node_Manager_Name}, ?MODULE, [Type], []).

%% stop/0
%% ====================================================================
%% @doc Stops the server
-spec stop() -> ok.
%% ====================================================================

stop() ->
  gen_server:cast(?Node_Manager_Name, stop).

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
-spec init(Args :: term()) -> Result when
  Result :: {ok, State}
  | {ok, State, Timeout}
  | {ok, State, hibernate}
  | {stop, Reason :: term()}
  | ignore,
  State :: term(),
  Timeout :: non_neg_integer() | infinity.
%% ====================================================================
init([Type]) when Type =:= worker ; Type =:= ccm ->
  process_flag(trap_exit, true),
  erlang:send_after(10, self(), {timer, do_quick_heart_beat}),
  erlang:send_after(100, self(), {timer, monitor_mem_net}),

  {ok, Period} = application:get_env(veil_cluster_node, node_monitoring_period),
  LoadMemorySize = round(15 * 60 / Period + 1),
  {ok, #node_state{node_type = Type, ccm_con_status = not_connected, memory_and_network_info = {[], [], 0, LoadMemorySize}}};

init([_Type]) ->
  {stop, wrong_type}.

%% handle_call/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
  Result :: {reply, Reply, NewState}
  | {reply, Reply, NewState, Timeout}
  | {reply, Reply, NewState, hibernate}
  | {noreply, NewState}
  | {noreply, NewState, Timeout}
  | {noreply, NewState, hibernate}
  | {stop, Reason, Reply, NewState}
  | {stop, Reason, NewState},
  Reply :: term(),
  NewState :: term(),
  Timeout :: non_neg_integer() | infinity,
  Reason :: term().
%% ====================================================================
handle_call(getNodeType, _From, State) ->
  Reply = State#node_state.node_type,
  {reply, Reply, State};

handle_call(getNode, _From, State) ->
  Reply = node(),
  {reply, Reply, State};

handle_call(get_ccm_connection_status, _From, State) ->
  {reply, State#node_state.ccm_con_status, State};

handle_call({get_node_stats, Window}, _From, State) ->
  Reply = get_node_stats(Window, State#node_state.memory_and_network_info),
  {reply, Reply, State};

handle_call(_Request, _From, State) ->
  {reply, wrong_request, State}.

%% handle_cast/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
-spec handle_cast(Request :: term(), State :: term()) -> Result when
  Result :: {noreply, NewState}
  | {noreply, NewState, Timeout}
  | {noreply, NewState, hibernate}
  | {stop, Reason :: term(), NewState},
  NewState :: term(),
  Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_cast(do_heart_beat, State) ->
  {noreply, heart_beat(State#node_state.ccm_con_status, State)};

handle_cast(do_quick_heart_beat, State) ->
  {noreply, heart_beat(State#node_state.ccm_con_status, State, true)};

handle_cast({heart_beat_ok, StateNum}, State) ->
  {noreply, heart_beat_response(StateNum, State)};

handle_cast(reset_ccm_connection, State) ->
  {noreply, heart_beat(not_connected, State)};

handle_cast({dispatcher_updated, DispState}, State) ->
  NewState = State#node_state{ dispatcher_state = DispState},
  {noreply, NewState};

handle_cast(stop, State) ->
  {stop, normal, State};

handle_cast(monitor_mem_net, State) ->
  Info = get_memory_and_net_info(),
  NewInfo = save_progress(Info, State#node_state.memory_and_network_info),
  {ok, Period} = application:get_env(veil_cluster_node, node_monitoring_period),
  erlang:send_after(1000 * Period, self(), {timer, monitor_mem_net}),
  {noreply, State#node_state{memory_and_network_info = NewInfo}};

handle_cast(_Msg, State) ->
  {noreply, State}.

%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
  Result :: {noreply, NewState}
  | {noreply, NewState, Timeout}
  | {noreply, NewState, hibernate}
  | {stop, Reason :: term(), NewState},
  NewState :: term(),
  Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_info({timer, Msg}, State) ->
  gen_server:cast(?Node_Manager_Name, Msg),
  {noreply, State};

handle_info(_Info, State) ->
  {noreply, State}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
  Reason :: normal
  | shutdown
  | {shutdown, term()}
  | term().
%% ====================================================================
terminate(_Reason, _State) ->
  ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
  Result :: {ok, NewState :: term()} | {error, Reason :: term()},
  OldVsn :: Vsn | {down, Vsn},
  Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% heart_beat/2
%% ====================================================================
%% @doc Connects with ccm and tells that the node is alive.
%% First it establishes network connection, next sends message to ccm.
-spec heart_beat(Conn_status :: atom(), State::term()) -> NewStatus when
  NewStatus ::  term().
%% ====================================================================
heart_beat(Conn_status, State) ->
  heart_beat(Conn_status, State, false).

%% heart_beat/3
%% ====================================================================
%% @doc Connects with ccm and tells that the node is alive.
%% First it establishes network connection, next sends message to ccm.
-spec heart_beat(Conn_status :: atom(), State::term(), ShortPeriod :: boolean()) -> NewStatus when
  NewStatus ::  term().
%% ====================================================================
heart_beat(Conn_status, State, ShortPeriod) ->
  New_conn_status = case Conn_status of
                                    not_connected ->
                                      {ok, CCM_Nodes} = application:get_env(veil_cluster_node, ccm_nodes),
                                      Ans = init_net_connection(CCM_Nodes),
                                      case Ans of
                                        ok -> connected;
                                        error -> not_connected
                                      end;
                                    Other -> Other
                                  end,

  {ok, Interval} = application:get_env(veil_cluster_node, heart_beat),
  case New_conn_status of
    connected ->
      gen_server:cast({global, ?CCM}, {node_is_up, node()}),

      case ShortPeriod of
        true -> erlang:send_after(500, self(), {timer, do_heart_beat});
        false -> erlang:send_after(Interval * 1000, self(), {timer, do_heart_beat})
      end;
    _ -> erlang:send_after(500, self(), {timer, do_heart_beat})
  end,

  lager:info([{mod, ?MODULE}], "Heart beat on node: ~s: sent; connection: ~s", [node(), New_conn_status]),
  State#node_state{ccm_con_status = New_conn_status}.

%% heart_beat_response/2
%% ====================================================================
%% @doc Saves information about ccm connection when ccm answers to its request
-spec heart_beat_response(New_state_num :: integer(), State::term()) -> NewStatus when
  NewStatus ::  term().
%% ====================================================================
heart_beat_response(New_state_num, State) ->
  lager:info([{mod, ?MODULE}], "Heart beat on node: ~s: answered, new state_num: ~b", [node(), New_state_num]),

  case (New_state_num == State#node_state.state_num) and (New_state_num == State#node_state.dispatcher_state) of
    true -> State;
    false ->
      update_dispatcher(New_state_num, State#node_state.node_type),
      State#node_state{state_num = New_state_num}
  end.

%% update_dispatcher/2
%% ====================================================================
%% @doc Tells dispatcher that cluster state has changed.
-spec update_dispatcher(New_state_num :: integer(), Type :: atom()) -> Result when
  Result ::  atom().
%% ====================================================================
update_dispatcher(New_state_num, Type) ->
  case Type =:= ccm of
    true -> ok;
    false -> gen_server:cast(?Dispatcher_Name, {update_state, New_state_num})
  end.

%% init_net_connection/1
%% ====================================================================
%% @doc Initializes network connection with cluster that contains nodes
%% given in argument.
-spec init_net_connection(Nodes :: list()) -> Result when
  Result ::  atom().
%% ====================================================================
init_net_connection([]) ->
  error;

init_net_connection([Node | Nodes]) ->
  try
    Ans = net_adm:ping(Node),
    case Ans of
      pong -> ok;
      pang -> init_net_connection(Nodes)
    end
  catch
    _:_ -> error
  end.

%% check_vsn/0
%% ====================================================================
%% @doc Checks application version
-spec check_vsn() -> Result when
  Result :: term().
%% ====================================================================
check_vsn() ->
  check_vsn(application:which_applications()).

%% check_vsn/1
%% ====================================================================
%% @doc Checks application version
-spec check_vsn(ApplicationData :: list()) -> Result when
  Result :: term().
%% ====================================================================
check_vsn([]) ->
  non;

check_vsn([{Application, _Description, Vsn} | Apps]) ->
  case Application of
    ?APP_Name -> Vsn;
    _Other -> check_vsn(Apps)
  end.

%% get_node_stats/2
%% ====================================================================
%% @doc Get statistics about node load
-spec get_node_stats(Window :: atom(), Stats :: term()) -> Result when
  Result :: term().
%% ====================================================================

get_node_stats(Window, {New, Old, NewListSize, _Max}) ->
  {ok, Period} = application:get_env(veil_cluster_node, node_monitoring_period),
  {ProcTmp, MemAndNetSize}  = case Window of
                                short -> {cpu_sup:avg1(), round(60 / Period + 1)};
                                medium -> {cpu_sup:avg5(), round(5 * 60 / Period + 1)};
                                long -> {cpu_sup:avg15(), round(15 * 60 / Period + 1)};
                                _W -> wrong_window
                              end,
  Proc = ProcTmp / 256,

  MemAndNet = case NewListSize >= MemAndNetSize of
                true -> lists:sublist(New, MemAndNetSize);
                false -> lists:flatten([New, lists:sublist(Old, MemAndNetSize-NewListSize)])
              end,

  CalculateMemAndNet = fun({Mem, {In, Out}}, {MemTmpSum, {InTmpSum, OutTmpSum}, {LastIn, LastOut}}) ->
    case LastIn of
      non -> {MemTmpSum + Mem, {InTmpSum, OutTmpSum}, {In, Out}};
      _ -> {MemTmpSum + Mem, {InTmpSum + erlang:max(LastIn - In, 0), OutTmpSum + erlang:max(LastOut - Out, 0)}, {In, Out}}
    end
  end,
  {MemSum, {InSum, OutSum}, _} = lists:foldl(CalculateMemAndNet, {0, {0, 0}, {non, non}}, MemAndNet),
  MemAndNetListSize = length(MemAndNet),
  MemAvg = case MemAndNetListSize of
             0 -> 0;
             _ -> MemSum / MemAndNetListSize
           end,
  {Proc, MemAvg, {InSum, OutSum}}.

%% get_memory_and_net_info/0
%% ====================================================================
%% @doc Checks memory and network usage
-spec get_memory_and_net_info() -> Result when
  Result :: term().
%% ====================================================================

get_memory_and_net_info() ->
  {Total, Allocated, _Worst} = memsup:get_memory_data(),
  Mem = case Total of
          0 -> 0;
          _ -> Allocated / Total
        end,
  Ports = erlang:ports(),
  GetNetInfo = fun(Port, {InTmp, OutTmp}) ->
    In = case erlang:port_info(Port, input) of
           {input, V} -> V;
           _Other -> 0
         end,
    Out = case erlang:port_info(Port, output) of
            {output, V2} -> V2;
            _Other2 -> 0
          end,
    {InTmp + In, OutTmp + Out}
  end,
  Net = lists:foldl(GetNetInfo, {0, 0}, Ports),
  {Mem, Net}.

%% save_progress/2
%% ====================================================================
%% @doc Saves information about node load
-spec save_progress(Report :: atom(), Stats :: term()) -> Result when
  Result :: term().
%% ====================================================================

save_progress(Report, {New, Old, NewListSize, Max}) ->
  case NewListSize + 1 of
    Max ->
      {[], [Report | New], 0, Max};
    S ->
      {[Report | New], Old, S, Max}
  end.
