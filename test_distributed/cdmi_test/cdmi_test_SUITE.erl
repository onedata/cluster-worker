%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This test is an example that shows how distributed test
%% should look like.
%% @end
%% ===================================================================

-module(cdmi_test_SUITE).
-include("test_utils.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

-define(SH, "DirectIO").
-define(Test_dir_name, "dir").
-define(Test_file_name, "file.txt").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
%% -export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([list_dir_test/1]).

all() -> [list_dir_test].

%% ====================================================================
%% Test functions
%% ====================================================================

list_dir_test(_Config) ->
    {Code1, Headers1, Response1} = do_request(?Test_dir_name, get, [], []),
    ?assertEqual("200", Code1),
    ?assertEqual(proplists:get_value("content-type", Headers1), "application/cdmi-container"),
    ?assertEqual("{\"objectType\":\"application/cdmi-container\",\"metadata\":[],\"children\":[\""++?Test_file_name++"\"]}",Response1).

%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_,Config) ->
    ?INIT_CODE_PATH,
    DN = ?config(dn,Config),
    [CCM] = ?config(nodes,Config),
    Cert = ?config(cert,Config),
    StorageUUID = setup_user_in_db(DN,CCM),

    put(ccm,CCM),
    put(dn,DN),
    put(cert,Cert),
    put(storage_uuid, StorageUUID),

    ibrowse:start(),

    Config ++ [{storage_uuid, StorageUUID}].

end_per_testcase(_,_Config) ->
    ibrowse:stop().

init_per_suite(Config) ->
    ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
    test_node_starter:start_deps_for_tester_node(),

    [CCM] = Nodes = test_node_starter:start_test_nodes(1),

    test_node_starter:start_app_on_nodes(?APP_Name, ?VEIL_DEPS, Nodes,
        [[{node_type, ccm_test},
            {dispatcher_port, 5055},
            {ccm_nodes, [CCM]},
            {dns_port, 1308},
            {db_nodes, [?DB_NODE]},
            {heart_beat, 1},
            {nif_prefix, './'},
            {ca_dir, './cacerts/'}
        ]]),

    gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    test_utils:wait_for_cluster_cast(),
    gen_server:cast({global, ?CCM}, init_cluster),
    test_utils:wait_for_cluster_init(),

    ibrowse:start(),
    Cert = ?COMMON_FILE("peer.pem"),
    DN = get_dn_from_cert(Cert,CCM),

    lists:append([{nodes, Nodes},{dn,DN},{cert,Cert}], Config).

end_per_suite(Config) ->
    Nodes = ?config(nodes, Config),
    test_node_starter:stop_app_on_nodes(?APP_Name, ?VEIL_DEPS, Nodes),
    test_node_starter:stop_test_nodes(Nodes).

%% ====================================================================
%% Internal functions
%% ====================================================================

get_dn_from_cert(Cert,CCM) ->
    {Ans2, PemBin} = file:read_file(Cert),
    ?assertEqual(ok, Ans2),

    {Ans3, RDNSequence} = rpc:call(CCM, user_logic, extract_dn_from_cert, [PemBin]),
    ?assertEqual(rdnSequence, Ans3),

    {Ans4, DN} = rpc:call(CCM, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
    ?assertEqual(ok, Ans4),
    DN.

% Populates the database with one user and some files
setup_user_in_db(DN, CCM) ->
    DnList = [DN],
    Login = ?TEST_USER,
    Name = "user user",
    Teams = [?TEST_GROUP],
    Email = "user@email.net",

    rpc:call(CCM, user_logic, remove_user, [{dn, DN}]),

    {Ans1, StorageUUID} = rpc:call(CCM, fslogic_storage, insert_storage, [?SH, ?ARG_TEST_ROOT]),
    ?assertEqual(ok, Ans1),
    {Ans5, _} = rpc:call(CCM, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
    ?assertEqual(ok, Ans5),

    fslogic_context:set_user_dn(DN),
    Ans6 = rpc:call(CCM, erlang, apply, [
        fun() ->
            fslogic_context:set_user_dn(DN),
            logical_files_manager:mkdir(filename:join("/",?Test_dir_name))
        end, [] ]),
    ?assertEqual(ok, Ans6),


    Ans7 = rpc:call(CCM, erlang, apply, [
        fun() ->
            fslogic_context:set_user_dn(DN),
            logical_files_manager:create(filename:join(["/",?Test_dir_name,?Test_file_name]))
        end, [] ]),
    ?assertEqual(ok, Ans7),

    StorageUUID.

% Performs a single request using ibrowse
do_request(RestSubpath, Method, Headers, Body) ->
    Cert = get(cert),
    CCM = get(ccm),


    {ok, Port} = rpc:call(CCM, application, get_env, [veil_cluster_node, rest_port]),
    Hostname = case (Port =:= 80) or (Port =:= 443) of
                   true -> "https://localhost";
                   false -> "https://localhost:" ++ integer_to_list(Port)
               end,
    {ok, Code, RespHeaders, Response} =
        ibrowse:send_req(
            Hostname ++ "/cdmi/" ++ RestSubpath,
            Headers,
            Method,
            Body,
            [{ssl_options, [{certfile, Cert}, {reuse_sessions, false}]}]
        ),
    {Code, RespHeaders, Response}.