%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of cluster_manager.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(cluster_manager_tests).
-include("registered_names.hrl").
-include("records.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if all environment variables needed by ccm are defined.
env_test() ->
	ok = application:start(?APP_Name),
	{ok, _InitTime} = application:get_env(?APP_Name, initialization_time),
	{ok, _Period} = application:get_env(?APP_Name, cluster_clontrol_period),
	ok = application:stop(?APP_Name).

%% This test checks if ccm is resistant to incorrect requests.
wrong_request_test() ->
	net_kernel:start([node1, shortnames]),

	application:set_env(?APP_Name, node_type, ccm), 
	application:set_env(?APP_Name, ccm_nodes, [node()]), 

	ok = application:start(?APP_Name),

	gen_server:cast({global, ?CCM}, abc),
	Reply = gen_server:call({global, ?CCM}, abc),
	?assert(Reply =:= wrong_request),
	
	ok = application:stop(?APP_Name),
	net_kernel:stop().

%% This test checks if ccm properly registers nodes in cluster.
%% Furthermore, it checks if it properly monitors state of these nodes.
nodes_counting_and_monitoring_test() ->
	net_kernel:start([node1, shortnames]),

	application:set_env(?APP_Name, node_type, ccm), 
	application:set_env(?APP_Name, ccm_nodes, [node()]), 

	ok = application:start(?APP_Name),

	Nodes = [n1, n2, n3],
	gen_server:cast({global, ?CCM}, {set_monitoring, off}),
	timer:sleep(10),
	lists:foreach(fun(Node) -> gen_server:call({global, ?CCM}, {node_is_up, Node}) end, Nodes),

  %% the test will be used when distributed test environment will be ready
%% 	Nodes2 = gen_server:call({global, ?CCM}, get_nodes),
%% 	?assert(length(Nodes) + 1 == length(Nodes2)),
%% 	lists:foreach(fun(Node) -> ?assert(lists:member(Node, Nodes2)) end, Nodes),
%% 	?assert(lists:member(node(), Nodes2)),
%%
%% 	gen_server:call({global, ?CCM}, {node_is_up, n2}),
%% 	gen_server:call({global, ?CCM}, {node_is_up, n1}),
%% 	Nodes3 = gen_server:call({global, ?CCM}, get_nodes),
%% 	?assert(length(Nodes) + 1 == length(Nodes3)),
	
	gen_server:cast({global, ?CCM}, {set_monitoring, on}),
	timer:sleep(10),
	Nodes4 = gen_server:call({global, ?CCM}, get_nodes),
	?assert(length(Nodes4) == 1),

	ok = application:stop(?APP_Name),
	net_kernel:stop().

%% This test checks if ccm is able to start and stop workers.
worker_start_stop_test() ->
	net_kernel:start([node1, shortnames]),

	application:set_env(?APP_Name, node_type, ccm), 
	application:set_env(?APP_Name, ccm_nodes, [node()]), 

	ok = application:start(?APP_Name),

	State = gen_server:call({global, ?CCM}, get_state),
	?assert(length(State#cm_state.workers) == 0),

	Module = sample_plug_in,
	{ok, NewState} = cluster_manager:start_worker(node(), Module, [], State),
	?assert(length(NewState#cm_state.workers) == 1),

	{ok, NewState2} = cluster_manager:stop_worker(node(), Module, NewState),
	?assert(length(NewState2#cm_state.workers) == 0),

	ok = application:stop(?APP_Name),
	net_kernel:stop().

-endif.