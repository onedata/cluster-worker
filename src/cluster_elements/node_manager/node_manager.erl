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
-export([start_link/1]).

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
	timer:apply_after(10, gen_server, cast, [?Node_Manager_Name, do_heart_beat]),
    {ok, #node_state{node_type = Type, ccm_con_status = not_connected}};

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

handle_cast(reset_ccm_connection, State) ->
	{noreply, heart_beat(not_connected, State)};

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

	New_conn_status2 = case New_conn_status of
		connected -> heart_beat();
		Other2 -> Other2
	end,

	{ok, Interval} = application:get_env(veil_cluster_node, heart_beat),
  {New_conn_status3, New_state_num} = case New_conn_status2 of
    {ok, Num} ->
      timer:apply_after(Interval * 1000, gen_server, cast, [?Node_Manager_Name, do_heart_beat]),
      {ok, Num};
		_Other3 ->
      timer:apply_after(Interval * 1000, gen_server, cast, [?Node_Manager_Name, reset_ccm_connection]),
      {New_conn_status2, 0}
	end,

	lager:info([{mod, ?MODULE}], "Haert beat on node: ~s: connection: ~s: heartbeat: ~s, new state_num: ~b", [node(), New_conn_status, New_conn_status3, New_state_num]),

  case New_conn_status3 of
    ok -> State#node_state{ccm_con_status = New_conn_status, state_num = New_state_num};
    _Other -> State#node_state{ccm_con_status = New_conn_status}
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

%% heart_beat/0
%% ====================================================================
%% @doc Tells ccm that node is alive.
-spec heart_beat() -> Result when
	Result ::  atom(). 
%% ====================================================================
heart_beat() ->
	case send_to_ccm({node_is_up, node()}) of
    {ok, Num} when is_number(Num) -> {ok, Num};
		_Other -> heart_beat_error
	end.

%% send_to_ccm/1
%% ====================================================================
%% @doc Sends message to ccm.
-spec send_to_ccm(Message :: term()) -> Result when
	Result ::  atom(). 
%% ====================================================================
send_to_ccm(Message) ->
	try
    {ok, gen_server:call({global, ?CCM}, Message)}
	catch
		_:_ -> connection_error
	end.