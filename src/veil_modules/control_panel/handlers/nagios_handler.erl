%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module handles Nagios monitoring requests.
%% @end
%% ===================================================================
-module(nagios_handler).
-include("registered_names.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-export([init/3, handle/2, terminate/3]).


%% init/3
%% ====================================================================
%% @doc Cowboy handler callback, no state is required
-spec init(any(), term(), any()) -> {ok, term(), []}.
%% ====================================================================
init(_Type, Req, _Opts) ->
	{ok, Req, []}.


%% handle/2
%% ====================================================================
%% @doc Handles a request producing an XML response
%% @end
-spec handle(term(), term()) -> {ok, term(), term()}.
%% ====================================================================
handle(Req, State) ->
	%get data from ccm and env
	{ok, Timeout} = application:get_env(veil_cluster_node, nagios_healtcheck_timeout),
	Nodes = gen_server:call({global, ?CCM}, get_nodes, Timeout),
	{Workers, StateNum} = gen_server:call({global, ?CCM}, get_workers, Timeout),
	{_, CStateNum} = gen_server:call({global, ?CCM}, get_callbacks, Timeout),

	%check workers
	WorkersStatus = pmap(fun(Worker) -> worker_status(Worker,Timeout) end,Workers),

	%check nodes
	WorkersOk = not contains_errors(WorkersStatus),
	NodesStatus = pmap(fun(Node) -> node_status(Node,StateNum,CStateNum,WorkersOk,Timeout) end, Nodes),

	%check if errors occured
	{HealthStatus, HttpStatusCode} = case contains_errors(NodesStatus) or contains_errors(WorkersStatus) of
		true -> {"error",500};
		false -> {"ok",200}
	end,

	%prepare current date
	{{YY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(now()),
	DateString = io_lib:format("~4..0w/~2..0w/~2..0w ~2..0w:~2..0w:~2..0w", [YY, MM, DD, Hour, Min, Sec]),

	%parse reply
	Healthdata = {healthdata,[{date,DateString},{status,HealthStatus}],NodesStatus++WorkersStatus},
	Content=lists:flatten([Healthdata]),
	Export=xmerl:export_simple(Content,xmerl_xml),
	Reply = io_lib:format("~s", [lists:flatten(Export)]),

	{ok, Req2} = cowboy_req:reply(HttpStatusCode, [{<<"content-type">>, <<"application/xml">>}], Reply, Req),
  {ok, Req2, State}.


%% terminate/3
%% ====================================================================
%% @doc Cowboy handler callback, no cleanup needed
-spec terminate(term(), term(), term()) -> ok.
%% ====================================================================
terminate(_Reason, _Req, _State) ->
	ok.

%% ====================================================================
%% Internal Functions
%% ====================================================================

%% node_status/5
%% ====================================================================
%% @doc Checks if callbacks num and state num on dispatcher and node manager are same as in ccm,
%% returns xmerl simple_xml output describing node health status. If callbacks don't match and some worker is
%% down - asume "error", if callbacks don't match and all workers are ok - assume "initializing"
%% @end
-spec node_status(Node :: atom(), CcmStateNum :: integer(), CcmCStateNum :: integer(), WorkersOk :: boolean(), Timeout :: integer()) -> Result when
	Result :: {veil_cluster_node, Attrs :: list(Atribute), []},
	Atribute :: {Name :: atom(),Value :: string()}.
%% ====================================================================
node_status(Node,CcmStateNum,CcmCStateNum,WorkersOk,Timeout) ->
	lager:debug("Healthcheck on node:~p",[Node]),
	%get state nuber and callback number from node manager and dispatcher
	{_, DispCStateNum} = gen_server:call({?Dispatcher_Name, Node}, get_callbacks, Timeout),
	DispStateNum = gen_server:call({?Dispatcher_Name, Node}, get_state_num, Timeout),
	{ManagerCStateNum,ManagerStateNum} = gen_server:call({?Node_Manager_Name, Node}, get_callback_and_state_num, Timeout),

	%compare them with numbers from ccm and prepare result
	case (CcmStateNum == DispStateNum) and (CcmCStateNum == DispCStateNum) and (CcmStateNum==ManagerStateNum) and (CcmCStateNum==ManagerCStateNum)  of
	true ->
		{veil_cluster_node,[{name,atom_to_list(Node)},{status,"ok"}],[]};
	false ->
		lager:error("Healthcheck on node ~p failed, callbacks/state number of ccm doesn't match values from node_manager and dispatcher",[Node]),
		lager:error("ccm_state_num: ~p, ccm_callback_num: ~p,disp_state_num: ~p, disp_callback_num: ~p,manager_state_num: ~p, manager_callback_num: ~p",
			[CcmStateNum,CcmCStateNum,DispStateNum,DispCStateNum,ManagerStateNum,ManagerCStateNum]),
		case WorkersOk of
			true -> {veil_cluster_node,[{name,atom_to_list(Node)},{status,"initializing"}],[]};
			false -> {veil_cluster_node,[{name,atom_to_list(Node)},{status,"error"}],[]}
		end
	end.

%% worker_status/2
%% ====================================================================
%% @doc Calls healthcheck method on selected worker and returns xmerl
%% simple_xml output describing worker health status
%% @end
-spec worker_status(Worker,Timeout :: integer()) -> Result when
	Worker :: {WorkerNode :: atom(), WorkerName :: atom()},
	Result :: {worker, Attrs :: list(Atribute), []},
	Atribute :: {Name :: atom(),Value :: string()}.
%% ====================================================================
worker_status(Worker,Timeout) ->
	lager:debug("Healthcheck on worker: ~p",[Worker]),
	{WorkerNode,WorkerName} = Worker,

	%do healthcheck
	gen_server:call({?Dispatcher_Name, WorkerNode}, {WorkerName, 1,self(), healthcheck}, Timeout),
	Ans = receive
			Any -> Any
		after Timeout ->
			{error, worker_response_timeout}
	end,

	%check healthcheck answer and prepare result
	NameString = atom_to_list(WorkerName),
	NodeString = atom_to_list(WorkerNode),
	case Ans of
		ok ->
			{worker,[{name,NameString},{node,NodeString},{status,"ok"}],[]};
		Error ->
			lager:error("Healthcheck on worker ~p failed with error: ~p",[Worker,Error]),
			ErrorString = io_lib:format("~p", [Error]),
			{worker,[{name,NameString},{node,NodeString},{status,ErrorString}],[]}
	end.

%% contains_errors/1
%% ====================================================================
%% @doc Checks if given list of health statuses (in xmerl simple_xml format)
%% contains error status
%% @end
-spec contains_errors(StatusList :: list(Status)) -> Result when
	Status :: {Tag :: atom(), Attrs :: list(Atribute), Content :: list()},
	Atribute :: {Name :: atom(),Value :: string()},
	Result :: true | false.
%% ====================================================================
contains_errors(StatusList) ->
	Errors = [Status || {_Tag,Attrs,_Content} <- StatusList, {status,Status} <-Attrs, Status /= "ok", Status /= "initializing"],
	Errors /= [].


%% ====================================================================
%% Paralel Map Function
%% ====================================================================

%% pmap/2
%% ====================================================================
%% @doc Works as lists:map/2, but in paralel, more explanation can be found
%% in: http://montsamu.blogspot.com/2007/02/erlang-parallel-map-and-parallel.html
%% @end
-spec pmap(F, L :: list()) -> Result when
	F :: fun((term()) -> term()),
	Result :: list().
%% ====================================================================
pmap(F, L) ->
	S = self(),
	Pids = lists:map(fun(I) -> spawn(fun() -> pmap_f(S, F, I) end) end, L),
	pmap_gather(Pids).

%% pmap_gather/1
%% ====================================================================
%% @doc Gather result from spawned process during pmap
-spec pmap_gather(List :: list()) -> Result when
	Result :: list().
%% ====================================================================
pmap_gather([H|T]) ->
	receive
		{H, Ret} -> [Ret|pmap_gather(T)]
	end;
pmap_gather([]) ->
	[].

%% pmap_f/3
%% ====================================================================
%% @doc Computes function value and sends result to parent process
-spec pmap_f(Parent :: pid(), F, I::term()) -> Result when
	F :: fun((term()) -> term()),
	Result :: term().
%% ====================================================================
pmap_f(Parent, F, I) ->
	Parent ! {self(), (catch F(I))}.