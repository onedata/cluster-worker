%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc Tests for gateway oneprovider_module.
%% ===================================================================

-module(gateway_test_SUITE).
-include("test_utils.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").
-include_lib("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/gateway/gateway.hrl").
-include("gwproto_pb.hrl").

%% the actual application uses GRPCA-generated cert
-define(CERT, "./certs/onedataServerFuse.pem").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([send_request_test/1, on_node_send_request/1]).

all() -> [send_request_test].


%% ====================================================================
%% Test functions
%% ====================================================================


send_request_test(Config) ->
  [Node | _] = Nodes = ?config(nodes, Config),

  lists:foreach(fun(N) -> gen_server:cast({?Node_Manager_Name, N}, do_heart_beat) end, Nodes),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  Self = self(),
  spawn_link(fun() ->
    {ok, ListenSocket} = ssl:listen(9999, [{certfile, ?CERT}, {reuseaddr, true},
                                           {mode, binary}, {active, false}, {packet, 4}]),
    {ok, Socket} = ssl:transport_accept(ListenSocket),
    ok = ssl:ssl_accept(Socket),
    {ok, Data} = ssl:recv(Socket, 0, timer:seconds(10)),
    Self ! {data, Data}
  end),

  FetchRequest = #fetchrequest{file_id = "1234", offset = 2345, size = 3456},

  timer:sleep(timer:seconds(2)),
  ?assertEqual(ok, rpc:call(Node, ?MODULE, on_node_send_request, [FetchRequest])),
  Data = receive {data, D} -> D after timer:seconds(10) -> ?assert(false) end,

  ?assertEqual(FetchRequest, gwproto_pb:decode_fetchrequest(Data)).


on_node_send_request(FetchRequest) ->
  gen_server:call(gateway, {test_call, 1, #fetch{remote = {{127,0,0,1}, 9999},
    notify = self(), request = FetchRequest}}).


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================


init_per_testcase(_, Config) ->
  ?INIT_CODE_PATH, ?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(1),
  [Node | _] = NodesUp,

  Port = 6666,
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp,
    [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [Node]},
      {dns_port, 1317}, {db_nodes, [?DB_NODE]}, {heart_beat, 1},
      {global_registry_provider_cert_path, ?CERT}]]),

  ?ENABLE_PROVIDER(lists:append([{port, Port}, {nodes, NodesUp}], Config)).


end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes).
