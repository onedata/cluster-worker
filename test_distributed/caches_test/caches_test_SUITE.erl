%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This test creates many Erlang virtual machines and uses them
%% to test how ccm manages workers and monitors nodes.
%% @end
%% ===================================================================

-module(caches_test_SUITE).

-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").

-define(ProtocolVersion, 1).

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([sub_proc_test/1, node_cache_test/1]).

%% export nodes' codes
-export([ccm_code1/0, ccm_code2/0, worker_code/0]).

all() -> [sub_proc_test, node_cache_test].

%% ====================================================================
%% Code of nodes used during the test
%% ====================================================================

ccm_code1() ->
  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  ok.

ccm_code2() ->
  gen_server:cast({global, ?CCM}, init_cluster),
  ok.

worker_code() ->
  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  ok.

%% ====================================================================
%% Test function
%% ====================================================================

%% This node-wide caches
node_cache_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  nodes_manager:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    nodes_manager:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  nodes_manager:wait_for_cluster_init(),

  CreateCaches = fun(Node, Ans) ->
    Pid = spawn(Node, fun() ->
      ?assertEqual(ok, worker_host:create_simple_cache(test_cache)),
      receive
        stop_cache -> ok
      end
    end),
    nodes_manager:wait_for_cluster_cast({?Node_Manager_Name, Node}),
    [Pid | Ans]
  end,
  CachesPids = lists:foldl(CreateCaches, [], WorkerNodes),

  AddDataToCaches = fun(Node) ->
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key, test_value}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key2, test_value2}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key3, test_value3}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {get_atom_from_node(Node, test_key), get_atom_from_node(Node, test_value)}])),
    nodes_manager:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(AddDataToCaches, WorkerNodes),

  ?assertEqual(ok, rpc:call(CCM, worker_host, clear_cache, [{test_cache, test_key}])),
  CheckCaches1 = fun(Node) ->
    ?assertEqual(3, rpc:call(Node, ets, info, [test_cache, size])),
    ?assertEqual([{test_key2, test_value2}], rpc:call(Node, ets, lookup, [test_cache, test_key2])),
    ?assertEqual([{test_key3, test_value3}], rpc:call(Node, ets, lookup, [test_cache, test_key3])),
    K = get_atom_from_node(Node, test_key),
    V = get_atom_from_node(Node, test_value),
    ?assertEqual([{K, V}], rpc:call(Node, ets, lookup, [test_cache, K]))
  end,
  lists:foreach(CheckCaches1, WorkerNodes),

  ?assertEqual(ok, rpc:call(CCM, worker_host, clear_cache, [{test_cache, [test_key2, test_key3]}])),
  CheckCaches2 = fun(Node) ->
    ?assertEqual(1, rpc:call(Node, ets, info, [test_cache, size])),
    K = get_atom_from_node(Node, test_key),
    V = get_atom_from_node(Node, test_value),
    ?assertEqual([{K, V}], rpc:call(Node, ets, lookup, [test_cache, K]))
  end,
  lists:foreach(CheckCaches2, WorkerNodes),

  [WN1 | WorkerNodes2] = WorkerNodes,
  ?assertEqual(ok, rpc:call(CCM, worker_host, clear_cache, [{test_cache, get_atom_from_node(WN1, test_key)}])),
  lists:foreach(CheckCaches2, WorkerNodes2),

  ?assertEqual(ok, rpc:call(CCM, worker_host, clear_cache, [test_cache])),
  CheckCaches3 = fun(Node) ->
    ?assertEqual(0, rpc:call(Node, ets, info, [test_cache, size]))
  end,
  lists:foreach(CheckCaches3, WorkerNodes),

  lists:foreach(fun(Pid) -> Pid ! stop_cache end, CachesPids).



%% Test cache na sub_procesach, automatycznego czyszczenia cachy oraz czyszczenia po awarii

%% This test checks sub procs management (if requests are forwarded to apropriate sub procs)
sub_proc_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  nodes_manager:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    nodes_manager:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  nodes_manager:wait_for_cluster_init(),

  %% TODO !!! Check why late answer from gen_server sometimes appear !!!
  %% Late answer: If no reply is received within the specified time, the function call fails.
  %% If the caller catches the failure and continues running, and the server is just late with the reply,
  %% it may arrive at any time later into the caller's message queue. The caller must in this case be
  %% prepared for this and discard any such garbage messages that are two element tuples with a reference as the first element.
  {Workers, _} = gen_server:call({global, ?CCM}, get_workers, 1000),
  StartAdditionalWorker = fun(Node) ->
    case lists:member({Node, fslogic}, Workers) of
      true -> ok;
      false ->
        StartAns = gen_server:call({global, ?CCM}, {start_worker, Node, fslogic, []}, 3000),
        ?assertEqual(ok, StartAns)
    end
  end,
  lists:foreach(StartAdditionalWorker, NodesUp),
  nodes_manager:wait_for_cluster_init(length(NodesUp) - 1),

  ProcFun = fun(_ProtocolVersion, {sub_proc_test, _, AnsPid}) ->
    Pid = self(),
    Node = node(),
    AnsPid ! {Pid, Node}
  end,

  MapFun = fun({sub_proc_test, MapNum, _}) ->
    MapNum rem 10
  end,

  RequestMap = fun
    ({sub_proc_test, _, _}) ->
      sub_proc_test_proccess;
    (_) -> non
  end,

  DispMapFun = fun
    ({sub_proc_test, MapNum2, _}) ->
    trunc(MapNum2 / 10);
    (_) -> non
  end,

  RegisterSubProc = fun(Node) ->
    RegAns = gen_server:call({fslogic, Node}, {register_sub_proc, sub_proc_test_proccess, 2, 3, ProcFun, MapFun, RequestMap, DispMapFun}, 1000),
    ?assertEqual(ok, RegAns),
    nodes_manager:wait_for_cluster_cast({fslogic, Node})
  end,
  lists:foreach(RegisterSubProc, NodesUp),

  Self = self(),
  TestFun = fun() ->
    spawn(fun() ->
      gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 11, Self}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 12, Self}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 13, Self}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 21, Self}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 31, Self}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 41, Self}}, 500)
    end)
  end,

  TestRequestsNum = 100,
  for(1, TestRequestsNum, TestFun),

  Ans = count_answers(6 * TestRequestsNum),
%%   ct:print("Ans: ~p~n", [Ans]),
  ?assertEqual(10, length(Ans)),
  Keys = proplists:get_keys(Ans),
  ?assertEqual(6* TestRequestsNum, lists:foldl(fun(K, Sum) ->
    Sum + proplists:get_value(K, Ans, 0)
  end, 0, Keys)),

  NodesSums = lists:foldl(fun({P, Node}, Sum) ->
    V = proplists:get_value({P, Node}, Ans, 0),
    V2 = proplists:get_value(Node, Sum, 0),
    [{Node, V + V2} | proplists:delete(Node, Sum)]
  end, [], Keys),

  NodesSumSumarry = lists:foldl(fun({_, V}, Sum) ->
    V2 = proplists:get_value(V, Sum, 0),
    [{V, V2 + 1} | proplists:delete(V, Sum)]
  end, [], NodesSums),

  ?assertEqual(2, length(NodesSumSumarry)),
  ?assert(lists:member({TestRequestsNum, 3}, NodesSumSumarry)),
  ?assert(lists:member({3*TestRequestsNum, 1}, NodesSumSumarry)).



%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_, Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),

  NodesUp = nodes_manager:start_test_on_nodes(4),
  [CCM | _] = NodesUp,
  DBNode = nodes_manager:get_db_node(),

  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [
    [{node_type, ccm_test}, {dispatcher_port, 5055}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}],
    [{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1309}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}],
    [{node_type, worker}, {dispatcher_port, 7777}, {ccm_nodes, [CCM]}, {dns_port, 1310}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}],
    [{node_type, worker}, {dispatcher_port, 8888}, {ccm_nodes, [CCM]}, {dns_port, 1311}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}]]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{nodes, NodesUp}, {assertions, Assertions}, {dbnode, DBNode}], Config).

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),
  nodes_manager:stop_deps_for_tester_node(),

  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns).

for(N, N, F) -> [F()];
for(I, N, F) -> [F()|for(I+1, N, F)].

count_answers(ExpectedNum) ->
  count_answers(ExpectedNum, []).

count_answers(0, TmpAns) ->
  TmpAns;

count_answers(ExpectedNum, TmpAns) ->
  receive
    {Msg1, Msg2} when is_atom(Msg2) ->
      NewCounter = proplists:get_value({Msg1, Msg2}, TmpAns, 0) + 1,
      NewAns = [{{Msg1, Msg2}, NewCounter} | proplists:delete({Msg1, Msg2}, TmpAns)],
      count_answers(ExpectedNum - 1, NewAns)
  after 5000 ->
    TmpAns
  end.

get_atom_from_node(Node, Ending) ->
  list_to_atom(atom_to_list(Node) ++ atom_to_list(Ending)).