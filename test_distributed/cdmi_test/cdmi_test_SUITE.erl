%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This file contains cdmi protocol tests
%% @end
%% ===================================================================

-module(cdmi_test_SUITE).
-include("test_utils.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").
-include("veil_modules/fslogic/fslogic.hrl").

-define(SH, "DirectIO").
-define(Test_dir_name, "dir").
-define(Test_file_name, "file.txt").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
%% -export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([list_dir_test/1,get_file_test/1 , create_dir_test/1, create_file_test/1, delete_dir_test/1, delete_file_test/1]).

all() -> [list_dir_test, get_file_test, create_dir_test, create_file_test, delete_dir_test, delete_file_test].

%% ====================================================================
%% Test functions
%% ====================================================================

% Tests cdmi container GET request (also refered as LIST)
list_dir_test(_Config) ->
    %%------ list basic dir --------

    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code1, Headers1, Response1} = do_request(?Test_dir_name++"/", get, RequestHeaders1, []),
    ?assertEqual("200", Code1),
    ?assertEqual(proplists:get_value("content-type", Headers1), "application/cdmi-container"),
    {struct,CdmiPesponse1} = mochijson2:decode(Response1),
    ?assertEqual(<<"application/cdmi-container">>, proplists:get_value(<<"objectType">>,CdmiPesponse1)),
    ?assertEqual(<<"dir/">>, proplists:get_value(<<"objectName">>,CdmiPesponse1)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>,CdmiPesponse1)),
    ?assertEqual([<<"file.txt">>], proplists:get_value(<<"children">>,CdmiPesponse1)),
    %%------------------------------

    %%------ list root dir ---------
    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code2, _Headers2, Response2} = do_request([], get, RequestHeaders2, []),
    ?assertEqual("200", Code2),
    {struct,CdmiPesponse2} = mochijson2:decode(Response2),
    ?assertEqual(<<"/">>, proplists:get_value(<<"objectName">>,CdmiPesponse2)),
    ?assertEqual([<<"dir">>,<<"groups">>], proplists:get_value(<<"children">>,CdmiPesponse2)),
    %%------------------------------

    %%--- list nonexisting dir -----
    RequestHeaders3 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code3, _Headers3, _Response3} = do_request("nonexisting_dir/", get, RequestHeaders3, []),
    ?assertEqual("404",Code3),
    %%------------------------------

    %%-- selective params list -----
    RequestHeaders4 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code4, _Headers4, Response4} = do_request(?Test_dir_name ++ "/?children;objectName", get, RequestHeaders4, []),
    ?assertEqual("200", Code4),
    {struct,CdmiPesponse4} = mochijson2:decode(Response4),
    ?assertEqual(<<"dir/">>, proplists:get_value(<<"objectName">>,CdmiPesponse4)),
    ?assertEqual([<<"file.txt">>], proplists:get_value(<<"children">>,CdmiPesponse4)),
    ?assertEqual(2,length(CdmiPesponse4)).
    %%------------------------------

% Tests cdmi object GET request. Request can be done without cdmi header (in that case
% file conent is returned as response body), or with cdmi header (the response
% contains json string of type: application/cdmi-object, and we can specify what
% parameters we need by listing then as ';' separated list after '?' in URL ),
%  )
get_file_test(_Config) ->
    FileName = "/toRead.txt",
    FileContent = <<"Some content...">>,
    Before = now_in_secs(),

    create_file(FileName),
    ?assert(object_exists(FileName)),
    write_to_file(FileName,FileContent),
    ?assertEqual(FileContent,get_file_content(FileName)),
    After = now_in_secs(),

    %%-------- basic read ----------
    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code1, _Headers1, Response1} = do_request(FileName, get, RequestHeaders1, []),
    ?assertEqual("200",Code1),
    {struct,CdmiPesponse1} = mochijson2:decode(Response1),

    ?assertEqual(<<"application/cdmi-object">>, proplists:get_value(<<"objectType">>,CdmiPesponse1)),
    ?assertEqual(<<"toRead.txt">>, proplists:get_value(<<"objectName">>,CdmiPesponse1)),
    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>,CdmiPesponse1)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>,CdmiPesponse1)),
    ?assertEqual(<<"base64">>, proplists:get_value(<<"valuetransferencoding">>,CdmiPesponse1)),
    ?assertEqual(<<"0-14">>, proplists:get_value(<<"valuerange">>,CdmiPesponse1)),
    {struct, Metadata1} = proplists:get_value(<<"metadata">>,CdmiPesponse1),
    ?assertEqual(<<"15">>, proplists:get_value(<<"cdmi_size">>, Metadata1)),
    CTime = binary_to_integer(proplists:get_value(<<"cdmi_ctime">>, Metadata1)),
    ATime = binary_to_integer(proplists:get_value(<<"cdmi_atime">>, Metadata1)),
    MTime = binary_to_integer(proplists:get_value(<<"cdmi_mtime">>, Metadata1)),
    ?assert(Before =< CTime),
    ?assert(CTime =< After),
    ?assert(CTime =< ATime),
    ?assert(CTime =< MTime),
    ?assertEqual(<<"veilfstestuser">>, proplists:get_value(<<"cdmi_owner">>, Metadata1)),
    ?assertEqual(FileContent, base64:decode(proplists:get_value(<<"value">>,CdmiPesponse1))),
    %%------------------------------

    %%-- selective params read -----
    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code2, _Headers2, Response2} = do_request(FileName++"?parentURI;completionStatus", get, RequestHeaders2, []),
    ?assertEqual("200",Code2),
    {struct,CdmiPesponse2} = mochijson2:decode(Response2),

    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>,CdmiPesponse2)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>,CdmiPesponse2)),
    ?assertEqual(2, length(CdmiPesponse2)),
    %%------------------------------

    %%--- selective value read -----
    RequestHeaders3 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code3, _Headers3, Response3} = do_request(FileName++"?value:1-3;valuerange", get, RequestHeaders3, []),
    ?assertEqual("200",Code3),
    {struct,CdmiPesponse3} = mochijson2:decode(Response3),

    ?assertEqual(<<"1-3">>, proplists:get_value(<<"valuerange">>,CdmiPesponse3)),
    ?assertEqual(<<"ome">>, base64:decode(proplists:get_value(<<"value">>,CdmiPesponse3))), % 1-3 from FileContent = <<"Some content...">>
    %%------------------------------

    %%------- noncdmi read --------
    {Code4, _Headers4, Response4} = do_request(FileName, get, [], []),
    ?assertEqual("200",Code4),

    ?assertEqual(binary_to_list(FileContent), Response4).
    %%------------------------------

% Tests dir creation (cdmi container PUT), remember that every container URI ends
% with '/'
create_dir_test(_Config) ->
    DirName = "/toCreate/",
    MissingParentName="/unknown/",
    DirWithoutParentName = filename:join(MissingParentName,"dir")++"/",

    %%---- missing content type ----
    ?assert(not object_exists(DirName)),

    {Code1, _Headers1, _Response1} = do_request(DirName, put, [], []),
    ?assertEqual("415",Code1),

    ?assert(not object_exists(DirName)),
    %%------------------------------

    %%------ basic create ----------
    ?assert(not object_exists(DirName)),

    RequestHeaders2 = [{"content-type", "application/cdmi-container"},{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code2, _Headers2, Response2} = do_request(DirName, put, RequestHeaders2, []),
    ?assertEqual("201",Code2),
    {struct,CdmiPesponse2} = mochijson2:decode(Response2),
    ?assertEqual(<<"application/cdmi-container">>, proplists:get_value(<<"objectType">>,CdmiPesponse2)),
    ?assertEqual(<<"toCreate/">>, proplists:get_value(<<"objectName">>,CdmiPesponse2)),
    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>,CdmiPesponse2)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>,CdmiPesponse2)),
    ?assertEqual([], proplists:get_value(<<"children">>,CdmiPesponse2)),

    ?assert(object_exists(DirName)),
    %%------------------------------

    %%----- creation conflict ------
    ?assert(object_exists(DirName)),

    RequestHeaders3 = [{"content-type", "application/cdmi-container"},{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code3, _Headers3, _Response3} = do_request(DirName, put, RequestHeaders3, []),
    ?assertEqual("409",Code3),

    ?assert(object_exists(DirName)),
    %%------------------------------

    %%----- missing parent ---------
    ?assert(not object_exists(MissingParentName)),

    RequestHeaders4 = [{"content-type", "application/cdmi-container"},{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code4, _Headers4, _Response4} = do_request(DirWithoutParentName, put, RequestHeaders4, []),
    ?assertEqual("404",Code4).
    %%------------------------------

% Tests file creation (cdmi object PUT), It can be done with cdmi header (when file data is provided as cdmi-object
% json string), or without (when we treat request body as new file content)
create_file_test(_Config) ->
    ToCreate = "file.txt",
    ToCreate2 = filename:join(["groups",?TEST_GROUP,"file1.txt"]),
    ToCreate4 = "file2",
    ToCreate5 = "file3",
    FileContent = <<"File content!">>,

    %%-------- basic create --------
    ?assert(not object_exists(ToCreate)),
    Before1 = now_in_secs(),

    RequestHeaders1 = [{"content-type", "application/cdmi-object"},{"X-CDMI-Specification-Version", "1.0.2"}],
    RequestBody1 = [{<<"value">>, FileContent}],
    RawRequestBody1 = rest_utils:encode_to_json(RequestBody1),
    {Code1, _Headers1, Response1} = do_request(ToCreate, put, RequestHeaders1, RawRequestBody1),
    After1 = now_in_secs(),

    ?assertEqual("201",Code1),
    {struct,CdmiPesponse1} = mochijson2:decode(Response1),
    ?assertEqual(<<"application/cdmi-object">>, proplists:get_value(<<"objectType">>,CdmiPesponse1)),
    ?assertEqual(<<"file.txt">>, proplists:get_value(<<"objectName">>,CdmiPesponse1)),
    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>,CdmiPesponse1)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>,CdmiPesponse1)),
    {struct, Metadata1} = proplists:get_value(<<"metadata">>,CdmiPesponse1),
    ?assertEqual(<<"13">>, proplists:get_value(<<"cdmi_size">>, Metadata1)),
    CTime1 = binary_to_integer(proplists:get_value(<<"cdmi_ctime">>, Metadata1)),
    ATime1 = binary_to_integer(proplists:get_value(<<"cdmi_atime">>, Metadata1)),
    MTime1 = binary_to_integer(proplists:get_value(<<"cdmi_mtime">>, Metadata1)),
    ?assert(Before1 =< CTime1),
    ?assert(CTime1 =< After1),
    ?assert(CTime1 =< ATime1),
    ?assert(CTime1 =< MTime1),
    ?assertEqual(<<"veilfstestuser">>, proplists:get_value(<<"cdmi_owner">>, Metadata1)),

    ?assert(object_exists(ToCreate)),
    ?assertEqual(FileContent,get_file_content(ToCreate)),
    %%------------------------------

    %%------ base64 create ---------
    ?assert(not object_exists(ToCreate2)),
    Before2 = now_in_secs(),

    RequestHeaders2 = [{"content-type", "application/cdmi-object"},{"X-CDMI-Specification-Version", "1.0.2"}],
    RequestBody2 = [{<<"valuetransferencoding">>,<<"base64">>},{<<"value">>, base64:encode(FileContent)}],
    RawRequestBody2 = rest_utils:encode_to_json(RequestBody2),
    {Code2, _Headers2, Response2} = do_request(ToCreate2, put, RequestHeaders2, RawRequestBody2),
    After2 = now_in_secs(),

    ?assertEqual("201",Code2),
    {struct,CdmiPesponse2} = mochijson2:decode(Response2),
    ?assertEqual(<<"application/cdmi-object">>, proplists:get_value(<<"objectType">>,CdmiPesponse2)),
    ?assertEqual(<<"file1.txt">>, proplists:get_value(<<"objectName">>,CdmiPesponse2)),
    ?assertEqual(<<"/groups/veilfstestgroup">>, proplists:get_value(<<"parentURI">>,CdmiPesponse2)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>,CdmiPesponse2)),
    {struct, Metadata2} = proplists:get_value(<<"metadata">>,CdmiPesponse2),
    ?assertEqual(<<"13">>, proplists:get_value(<<"cdmi_size">>, Metadata2)),
    CTime2 = binary_to_integer(proplists:get_value(<<"cdmi_ctime">>, Metadata2)),
    ATime2 = binary_to_integer(proplists:get_value(<<"cdmi_atime">>, Metadata2)),
    MTime2 = binary_to_integer(proplists:get_value(<<"cdmi_mtime">>, Metadata2)),
    ?assert(Before2 =< CTime2),
    ?assert(CTime2 =< After2),
    ?assert(CTime2 =< ATime2),
    ?assert(CTime2 =< MTime2),
    ?assertEqual(<<"veilfstestuser">>, proplists:get_value(<<"cdmi_owner">>, Metadata2)),

    ?assert(object_exists(ToCreate2)),
    ?assertEqual(FileContent,get_file_content(ToCreate2)),
    %%------------------------------

    %%----- create conflict --------
    ?assert(object_exists(ToCreate)),

    RequestHeaders3 = [{"content-type", "application/cdmi-object"},{"X-CDMI-Specification-Version", "1.0.2"}],
    RequestBody3 = [{<<"value">>, FileContent}],
    RawRequestBody3 = rest_utils:encode_to_json(RequestBody3),
    {Code3, _Headers3, _Response3} = do_request(ToCreate, put, RequestHeaders3, RawRequestBody3),
    ?assertEqual("409",Code3),

    ?assert(object_exists(ToCreate)),
    %%------------------------------

    %%------- create empty ---------
    ?assert(not object_exists(ToCreate4)),

    RequestHeaders4 = [{"content-type", "application/cdmi-object"},{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code4, _Headers4, _Response4} = do_request(ToCreate4, put, RequestHeaders4, []),
    ?assertEqual("201",Code4),

    ?assert(object_exists(ToCreate4)),
    ?assertEqual(<<>>,get_file_content(ToCreate4)),
    %%------------------------------

    %%------ create noncdmi --------
    ?assert(not object_exists(ToCreate5)),

    RequestHeaders5 = [{"content-type", "application/binary"}],
    {Code5, _Headers5, _Response5} = do_request(ToCreate5, put, RequestHeaders5, FileContent),
    ?assertEqual("201",Code5),

    ?assert(object_exists(ToCreate5)),
    ?assertEqual(FileContent,get_file_content(ToCreate5)).
    %%------------------------------

% Tests cdmi container DELETE requests
delete_dir_test(_Config) ->
    DirName = "/toDelete/",
    ChildDirName = "/toDelete/child/",
    GroupsDirName = "/groups/",

    %%----- basic delete -----------
    create_dir(DirName),
    ?assert(object_exists(DirName)),

    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code1, _Headers1, _Response1} = do_request(DirName, delete, RequestHeaders1, []),
    ?assertEqual("204",Code1),

    ?assert(not object_exists(DirName)),
    %%------------------------------

    %%------ recursive delete ------
    create_dir(DirName),
    ?assert(object_exists(DirName)),
    create_dir(ChildDirName),
    ?assert(object_exists(ChildDirName)),

    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code2, _Headers2, _Response2} = do_request(DirName, delete, RequestHeaders2, []),
    ?assertEqual("204",Code2),

    ?assert(not object_exists(DirName)),
    ?assert(not object_exists(ChildDirName)),
    %%------------------------------

    %%----- delete group dir -------
    ?assert(object_exists(GroupsDirName)),

    RequestHeaders3 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code3, _Headers3, _Response3} = do_request(GroupsDirName, delete, RequestHeaders3, []),
    ?assertEqual("403",Code3),

    ?assert(object_exists(GroupsDirName)).
    %%------------------------------

% Tests cdmi object DELETE requests
delete_file_test(_Config) ->
    FileName = "/toDelete",
    GroupFileName = "/groups/veilfstestgroup/groupFile",

    %%----- basic delete -----------
    create_file(FileName),
    ?assert(object_exists(FileName)),

    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code1, _Headers1, _Response1} = do_request(FileName, delete, RequestHeaders1, []),
    ?assertEqual("204",Code1),

    ?assert(not object_exists(FileName)),
    %%------------------------------

    %%----- delete group file ------
    create_file(GroupFileName),
    ?assert(object_exists(GroupFileName)),

    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code2, _Headers2, _Response2} = do_request(GroupFileName, delete, RequestHeaders2, []),
    ?assertEqual("204",Code2),

    ?assert(not object_exists(GroupFileName)).
    %%------------------------------



%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_,Config) ->
    ?INIT_CODE_PATH,
    DN = ?config(dn,Config),
    [CCM] = ?config(nodes,Config),
    Cert = ?config(cert,Config),
    StorageUUID = ?config(storage_uuid,Config),

    put(ccm,CCM),
    put(dn,DN),
    put(cert,Cert),
    put(storage_uuid, StorageUUID),

    ibrowse:start(),

    Config.

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
    StorageUUID = setup_user_in_db(DN,CCM),

    lists:append([{nodes, Nodes},{dn,DN},{cert,Cert},{storage_uuid, StorageUUID}], Config).

end_per_suite(Config) ->
    Nodes = ?config(nodes, Config),
    test_node_starter:stop_app_on_nodes(?APP_Name, ?VEIL_DEPS, Nodes),
    test_node_starter:stop_test_nodes(Nodes).

%% ====================================================================
%% Internal functions
%% ====================================================================

object_exists(Path) ->
    DN=get(dn),
    Ans = rpc_call_node(fun() ->
        fslogic_context:set_user_dn(DN),
        logical_files_manager:getfileattr(Path)
    end),
    case Ans of
        {ok,_} -> true;
        _ -> false
    end.

create_dir(Path) ->
    DN=get(dn),
    Ans = rpc_call_node(fun() ->
        fslogic_context:set_user_dn(DN),
        logical_files_manager:mkdir(Path)
    end),
    ?assertEqual(ok, Ans).

create_file(Path) ->
    DN=get(dn),
    Ans = rpc_call_node(fun() ->
        fslogic_context:set_user_dn(DN),
        logical_files_manager:create(Path)
    end),
    ?assertEqual(ok, Ans).

get_file_content(Path) ->
    DN=get(dn),

    rpc_call_node(fun() ->
        GetFile = fun F(Filename, Size, BytesSent, BufferSize, Ans) ->
            {ok, BytesRead} = logical_files_manager:read(Filename, BytesSent, BufferSize),
            NewSent = BytesSent + size(BytesRead),
            if
                NewSent =:= Size -> <<Ans/binary,BytesRead/binary>>;
                true -> F(Filename, Size, NewSent, BufferSize,<<Ans/binary,BytesRead/binary>>)
            end
        end,

        fslogic_context:set_user_dn(DN),
        {ok,Attr} = logical_files_manager:getfileattr(Path),
        GetFile(Path,Attr#fileattributes.size,0,10,<<>>)
    end).

write_to_file(Path,Data) ->
    DN=get(dn),

    rpc_call_node(fun() ->
        fslogic_context:set_user_dn(DN),
        logical_files_manager:write(Path,Data)
    end).

rpc_call_node(F) ->
    rpc:call(get(ccm), erlang, apply, [F, [] ]).

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

% Returns current time in seconds
now_in_secs() ->
    {MegaSecs, Secs, _MicroSecs} = erlang:now(),
    MegaSecs * 1000000 + Secs.
