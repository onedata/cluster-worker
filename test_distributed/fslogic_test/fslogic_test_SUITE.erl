%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of fslogic.
%% It contains tests that base on ct.
%% @end
%% ===================================================================

-module(fslogic_test_SUITE).

-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/dao/dao_vfs.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/dao/dao_share.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([files_manager_standard_files_test/1, files_manager_tmp_files_test/1, storage_management_test/1, permissions_management_test/1, user_creation_test/1,
  fuse_requests_test/1, users_separation_test/1, file_sharing_test/1, dir_mv_test/1, user_file_counting_test/1, dirs_creating_test/1, groups_test/1]).
-export([create_standard_share/2, create_share/3, get_share/2]).

all() -> [groups_test, files_manager_tmp_files_test, files_manager_standard_files_test, storage_management_test, permissions_management_test, user_creation_test,
  fuse_requests_test, users_separation_test, file_sharing_test, dir_mv_test, user_file_counting_test, dirs_creating_test
].

-define(SH, "DirectIO").
-define(TEST_ROOT, ["/tmp/veilfs"]). %% Root of test filesystem
-define(TEST_ROOT2, ["/tmp/veilfs2"]).
-define(ProtocolVersion, 1).

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if groups are working as intended.
%% I.e all users see files moved/created in theirs group directory and users see only their groups
groups_test(Config) ->
    nodes_manager:check_start_assertions(Config),
    NodesUp = ?config(nodes, Config),
    [Node | _] = NodesUp,

    Cert1 = ?COMMON_FILE("peer.pem"),
    Cert2 = ?COMMON_FILE("peer2.pem"),

    Host = "localhost",
    Port = ?config(port, Config),

    %% Cluster init
    gen_server:cast({?Node_Manager_Name, Node}, do_heart_beat),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    timer:sleep(100),
    gen_server:cast({global, ?CCM}, init_cluster),
    timer:sleep(1500),

    %% files_manager call with given user's DN
    FM = fun(M, A, DN) ->
            Me = self(),
            Pid = spawn(Node, fun() -> put(user_id, DN), Me ! {self(), apply(logical_files_manager, M, A)} end),
            receive
                {Pid, Resp} -> Resp
            end
         end,

    %% Gets uid by name
    UID = fun(Name) ->
              list_to_integer(os:cmd("id -u " ++ Name) -- "\n")
          end,

    %% Gets gid by name
    GID = fun(Name) ->
        list_to_integer(os:cmd("getent group " ++ Name ++" | cut -d: -f3") -- "\n")
    end,

    %% Init storage
    {InsertStorageAns, StorageUUID} = rpc:call(Node, fslogic_storage, insert_storage, ["DirectIO", ?TEST_ROOT]),
    ?assertEqual(ok, InsertStorageAns),

    %% Init users
    AddUser = fun(Login, Teams, Cert) ->
        {ReadFileAns, PemBin} = file:read_file(Cert),
        ?assertEqual(ok, ReadFileAns),
        {ExtractAns, RDNSequence} = rpc:call(Node, user_logic, extract_dn_from_cert, [PemBin]),
        ?assertEqual(rdnSequence, ExtractAns),
        {ConvertAns, DN} = rpc:call(Node, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
        ?assertEqual(ok, ConvertAns),
        DnList = [DN],

        Name = "user1 user1",
        Email = "user1@email.net",
        {CreateUserAns, #veil_document{uuid = _UserID1}} = rpc:call(Node, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
        ?assertEqual(ok, CreateUserAns),
        DnList
    end,

    DN1 = AddUser("veilfstestuser", "veilfstestgroup(Grp)", Cert1),
    DN2 = AddUser("veilfstestuser2", "veilfstestgroup(Grp),veilfstestgroup2(Grp2)", Cert2),
    %% END init users

    %% Init connections
    {ConAns1, Socket1} = wss:connect(Host, Port, [{certfile, Cert1}, {cacertfile, Cert1}, auto_handshake]),
    ?assertEqual(ok, ConAns1),
    {ConAns2, Socket2} = wss:connect(Host, Port, [{certfile, Cert2}, {cacertfile, Cert2}, auto_handshake]),
    ?assertEqual(ok, ConAns2),
    %% END init connections

    %% Check if groups dirs are created and have valid owners
    {S0, A0} = rpc:call(Node, dao_lib, apply, [dao_vfs, get_file, ["/groups/veilfstestgroup"], ?ProtocolVersion]),
    ?assertEqual(ok, S0),
    ?assertEqual(["veilfstestgroup"], A0#veil_document.record#file.gids),

    {S1, A1} = rpc:call(Node, dao_lib, apply, [dao_vfs, get_file, ["/groups/veilfstestgroup2"], ?ProtocolVersion]),
    ?assertEqual(ok, S1),
    ?assertEqual(["veilfstestgroup2"], A1#veil_document.record#file.gids),


    %% Test not allowed operations
    {"ok", A2} = mkdir(Socket1, "/groups/test"),
    ?assertEqual(eperm, A2),

    {"ok", A3} = delete_file(Socket1, "/groups"),
    ?assertEqual(eperm, A3),

    {"ok", A10} = delete_file(Socket1, "/groups/veilfstestgroup"),
    ?assertEqual(eperm, A10),

    {"ok", A11} = delete_file(Socket1, "/groups/veilfstestgroup2"),
    ?assertEqual(eperm, A11),

    {"ok", A4} = rename_file(Socket1, "/groups/veilfstestgroup2", "/test"),
    ?assertEqual(eperm, A4),

    {"ok", A5} = rename_file(Socket1, "/groups", "/test"),
    ?assertEqual(eperm, A5),

    {"ok", ok} = mkdir(Socket1, "/test"), %% Test dir
    {"ok", _, _, _, "ok"} = create_file(Socket1, "/file"), %% Test file
    {"ok", A6} = rename_file(Socket1, "/test", "/groups/test"),
    ?assertEqual(eperm, A6),

    {"ok", A14} = rename_file(Socket1, "/file", "/groups/test"),
    ?assertEqual(eperm, A14),

    {"ok", A7} = change_file_perms(Socket1, "/groups", 8#555),
    ?assertEqual(eperm, A7),

    {"ok", A8} = change_file_perms(Socket1, "/groups/veilfstestgroup", 8#555),
    ?assertEqual(eperm, A8),

    {"ok", A9} = change_file_perms(Socket1, "/groups/veilfstestgroup2", 8#555),
    ?assertEqual(eperm, A9),

    {"ok", A12} = chown(Socket1, "/groups/veilfstestgroup2", 500, "veilfstestuser"),
    ?assertEqual(eperm, A12),

    {"ok", A13} = chown(Socket1, "/groups", 500, "veilfstestuser"),
    ?assertEqual(eperm, A13),

    {"ok",  _, _, _, A15} = create_file(Socket1, "/groups/file"),
    ?assertEqual(eperm, list_to_atom(A15)),

    {"ok", A16} = create_link(Socket1, "/groups/file", "link"),
    ?assertEqual(eperm, A16),


    %% Test groups visibility
    {"ok", C17, A17} = ls(Socket1, "/groups", 10, 0),
    ?assertEqual(ok, list_to_atom(A17)),
    ?assertEqual(["veilfstestgroup"], C17),

    {"ok", C18, A18} = ls(Socket2, "/groups", 10, 0),
    ?assertEqual(ok, list_to_atom(A18)),
    ?assert(lists:member("veilfstestgroup", C18)),
    ?assert(lists:member("veilfstestgroup2", C18)),

    %% Try to use group dir that is not visible for the user
    {"ok", A19} = mkdir(Socket1, "/groups/veilfstestgroup2/testdir"),
    ?assertNotEqual(ok, A19),


    %% Files visibility test
    {"ok", A20} = mkdir(Socket1, "/groups/veilfstestgroup/dir"),
    ?assertEqual(ok, A20),

    {"ok", _, _, _, A21} = create_file(Socket2, "/groups/veilfstestgroup/file"),
    ?assertEqual(ok, list_to_atom(A21)),

    {"ok", A22} = mkdir(Socket1, "/groups/veilfstestgroup/dir"), %% Dir should already exist
    ?assertEqual(eexist, A22),

    {"ok", A23} = mkdir(Socket2, "/groups/veilfstestgroup/file"), %% File should already exist
    ?assertEqual(eexist, A23),

    A24 = FM(create, ["/groups/veilfstestgroup/file2"], DN1),
    ?assertEqual(ok, A24),

    ?assert(rpc:call(Node, files_tester, file_exists, ["/groups/veilfstestgroup/file2"])),
    {ok, L1} = rpc:call(Node, files_tester, get_file_location, ["/groups/veilfstestgroup/file2"]),
    [_, _, BaseDir1, SecDir1 | _] = string:tokens(L1, "/"),
    ?assertEqual("groups", BaseDir1),
    ?assertEqual("veilfstestgroup", SecDir1),

    %% Check if owners are set correctly
    {"ok", #fileattr{gname = GName0}} = get_file_attr(Socket1, "/groups/veilfstestgroup/dir"),
    ?assertEqual("veilfstestgroup", GName0),

    {"ok", #fileattr{gname = GName0}} = get_file_attr(Socket1, "/groups/veilfstestgroup/file"),
    ?assertEqual("veilfstestgroup", GName0),

    %% Onwer on storage
    {ok, User1, Grp1} = rpc:call(Node, files_tester, get_owner, [L1]),
    ?assertEqual(UID("veilfstestuser"), User1),
    ?assertEqual(GID("veilfstestgroup"), Grp1),


    %% Check if file move changes group owner & storage file location
    A25 = FM(create, ["/f1"], DN1),
    ?assertEqual(ok, A25),

    A26 = FM(create, ["/groups/veilfstestgroup2/f2"], DN2),
    ?assertEqual(ok, A26),

    {ok, L2} = rpc:call(Node, files_tester, get_file_location, ["/groups/veilfstestgroup2/f2"]),
    {ok, User2, Grp2} = rpc:call(Node, files_tester, get_owner, [L2]),
    ?assertEqual(UID("veilfstestuser2"), User2),
    ?assertEqual(GID("veilfstestgroup2"), Grp2),
    [_, _, BaseDir2, SecDir2 | _] = string:tokens(L2, "/"),
    ?assertEqual("groups", BaseDir2),
    ?assertEqual("veilfstestgroup2", SecDir2),

    {ok, L3} = rpc:call(Node, files_tester, get_file_location, ["/veilfstestuser/f1"]),
    [_, _, BaseDir3, SecDir3 | _] = string:tokens(L3, "/"),
    ?assertEqual("users", BaseDir3),
    ?assertEqual("veilfstestuser", SecDir3),

    %% Now move those files
    A27 = FM(mv, ["/f1", "/groups/veilfstestgroup/f1"], DN1),
    ?assertEqual(ok, A27),

    A28 = FM(mv, ["/groups/veilfstestgroup2/f2", "/groups/veilfstestgroup/f2"], DN2),
    ?assertEqual(ok, A28),

    %% Check its location
    {ok, L4} = rpc:call(Node, files_tester, get_file_location, ["/groups/veilfstestgroup/f1"]),
    {ok, L5} = rpc:call(Node, files_tester, get_file_location, ["/groups/veilfstestgroup/f2"]),
    [_, _, BaseDir4, SecDir4 | _] = string:tokens(L4, "/"),
    [_, _, BaseDir5, SecDir5 | _] = string:tokens(L5, "/"),
    ?assertEqual("groups", BaseDir4),
    ?assertEqual("veilfstestgroup", SecDir4),
    ?assertEqual("groups", BaseDir5),
    ?assertEqual("veilfstestgroup", SecDir5),

    ?assert(rpc:call(Node, files_tester, file_exists, ["/groups/veilfstestgroup/f1"])),
    ?assert(rpc:call(Node, files_tester, file_exists, ["/groups/veilfstestgroup/f2"])),

    %% ... and owners
    {ok, User3, Grp3} = rpc:call(Node, files_tester, get_owner, [L4]),
    {ok, User4, Grp4} = rpc:call(Node, files_tester, get_owner, [L5]),
    ?assertEqual(UID("veilfstestuser"), User3),
    ?assertEqual(GID("veilfstestgroup"), Grp3),

    ?assertEqual(UID("veilfstestuser2"), User4),
    ?assertEqual(GID("veilfstestgroup"), Grp4),


    %% Cleanup
    rpc:call(Node, dao_lib, apply, [dao_vfs, remove_file, ["/veilfstestuser/file"], ?ProtocolVersion]),
    rpc:call(Node, dao_lib, apply, [dao_vfs, remove_file, ["/groups/veilfstestgroup/f1"], ?ProtocolVersion]),
    rpc:call(Node, dao_lib, apply, [dao_vfs, remove_file, ["/groups/veilfstestgroup/f2"], ?ProtocolVersion]),
    rpc:call(Node, dao_lib, apply, [dao_vfs, remove_file, ["/groups/veilfstestgroup2/f1"], ?ProtocolVersion]),
    rpc:call(Node, dao_lib, apply, [dao_vfs, remove_file, ["/groups/veilfstestgroup2/f2"], ?ProtocolVersion]),
    rpc:call(Node, dao_lib, apply, [dao_vfs, remove_file, ["/groups/veilfstestgroup/file"], ?ProtocolVersion]),
    rpc:call(Node, dao_lib, apply, [dao_vfs, remove_file, ["/groups/veilfstestgroup/file2"], ?ProtocolVersion]),
    rpc:call(Node, dao_lib, apply, [dao_vfs, remove_file, ["/groups/veilfstestgroup2/file"], ?ProtocolVersion]),
    rpc:call(Node, dao_lib, apply, [dao_vfs, remove_file, ["/groups/veilfstestgroup2/file2"], ?ProtocolVersion]),
    rpc:call(Node, dao_lib, apply, [dao_vfs, remove_file, ["/groups/veilfstestgroup2/testDir"], ?ProtocolVersion]),
    rpc:call(Node, dao_lib, apply, [dao_vfs, remove_file, ["/groups/veilfstestgroup"], ?ProtocolVersion]),
    rpc:call(Node, dao_lib, apply, [dao_vfs, remove_file, ["/groups/veilfstestgroup2"], ?ProtocolVersion]),

    rpc:call(Node, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
    rpc:call(Node, user_logic, remove_user, [{login, "veilfstestuser"}]),
    rpc:call(Node, user_logic, remove_user, [{login, "veilfstestuser2"}]),

    files_tester:delete_dir(?TEST_ROOT ++ "/users"),
    files_tester:delete_dir(?TEST_ROOT ++ "/groups").

%% Checks creating of directories at storage for users' files.
%% The test creates path for a new file that contains 2 directories
%% (fslogic uses it when the user has a lot of files).
dirs_creating_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  SHInfo = #storage_helper_info{name = ?SH, init_args = ?TEST_ROOT},

  AnsCreate = rpc:call(Node1, fslogic, create_dirs, [50, 5, SHInfo, "/"]),
  ?assertEqual(2, length(string:tokens(AnsCreate, "/"))),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ AnsCreate)),

  files_tester:delete(?TEST_ROOT ++ AnsCreate).

%% Checks user counting view.
%% The test creates some files for two users, and then checks if the view counts them properly.
user_file_counting_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  FileBeg = "user_dirs_at_storage_test_file",
  User1FilesEnding = ["1","2","3","4"],
  User2FilesEnding = ["x","y","z"],

  Cert = ?COMMON_FILE("peer.pem"),
  Cert2 = ?COMMON_FILE("peer2.pem"),   %% Cert of second test user (the test uses 2 users to check if files of one user are not counted as files of other user)
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  {ReadFileAns, PemBin} = file:read_file(Cert),
  ?assertEqual(ok, ReadFileAns),
  {ExtractAns, RDNSequence} = rpc:call(FSLogicNode, user_logic, extract_dn_from_cert, [PemBin]),
  ?assertEqual(rdnSequence, ExtractAns),
  {ConvertAns, DN} = rpc:call(FSLogicNode, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
  ?assertEqual(ok, ConvertAns),
  DnList = [DN],

  Login = "user1",
  Name = "user1 user1",
  Teams = "user1 team",
  Email = "user1@email.net",
  {CreateUserAns, #veil_document{uuid = UserID1}} = rpc:call(FSLogicNode, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
  ?assertEqual(ok, CreateUserAns),

  {ReadFileAns2, PemBin2} = file:read_file(Cert2),
  ?assertEqual(ok, ReadFileAns2),
  {ExtractAns2, RDNSequence2} = rpc:call(FSLogicNode, user_logic, extract_dn_from_cert, [PemBin2]),
  ?assertEqual(rdnSequence, ExtractAns2),
  {ConvertAns2, DN2} = rpc:call(FSLogicNode, user_logic, rdn_sequence_to_dn_string, [RDNSequence2]),
  ?assertEqual(ok, ConvertAns2),
  DnList2 = [DN2],

  Login2 = "user2",
  Name2 = "user2 user2",
  Teams2 = "user2 team",
  Email2 = "user2@email.net",
  {CreateUserAns2, #veil_document{uuid = UserID2}} = rpc:call(FSLogicNode, user_logic, create_user, [Login2, Name2, Teams2, Email2, DnList2]),
  ?assertEqual(ok, CreateUserAns2),

  rpc:call(FSLogicNode, fslogic, get_files_number, [user, "not_existing_id", 1]),
  timer:sleep(1000),
  {CountStatus0, Count0} = rpc:call(FSLogicNode, fslogic, get_files_number, [user, "not_existing_id", 1]),
  ?assertEqual(ok, CountStatus0),
  ?assertEqual(0, Count0),

  rpc:call(FSLogicNode, fslogic, get_files_number, [user, UserID1, 1]),
  timer:sleep(1000),
  {CountStatus00, Count00} = rpc:call(FSLogicNode, fslogic, get_files_number, [user, UserID1, 1]),
  ?assertEqual(ok, CountStatus00),
  ?assertEqual(0, Count00),

  %% Connect to cluster
  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}, auto_handshake]),
  ?assertEqual(ok, ConAns),

  %% Connect to cluster, user2
  {ConAns2, Socket2} = wss:connect(Host, Port, [{certfile, Cert2}, {cacertfile, Cert2}, auto_handshake]),
  ?assertEqual(ok, ConAns2),

  lists:foreach(fun(FileEnding) ->
    FileName = FileBeg ++ FileEnding,
    {Status, _, _, _, AnswerOpt} = create_file(Socket, FileName),
    ?assertEqual("ok", Status),
    ?assertEqual(?VOK, AnswerOpt)
  end, User1FilesEnding),

  lists:foreach(fun(FileEnding) ->
    FileName = FileBeg ++ FileEnding,
    {Status, _, _, _, AnswerOpt} = create_file(Socket2, FileName),
    ?assertEqual("ok", Status),
    ?assertEqual(?VOK, AnswerOpt)
  end, User2FilesEnding),

  rpc:call(FSLogicNode, fslogic, get_files_number, [user, UserID1, 1]),
  timer:sleep(1000),
  {CountStatus, Count} = rpc:call(FSLogicNode, fslogic, get_files_number, [user, UserID1, 1]),
  ?assertEqual(ok, CountStatus),
  ?assertEqual(length(User1FilesEnding), Count),

  rpc:call(FSLogicNode, fslogic, get_files_number, [user, UserID2, 1]),
  timer:sleep(1000),
  {CountStatus2, Count2} = rpc:call(FSLogicNode, fslogic, get_files_number, [user, UserID2, 1]),
  ?assertEqual(ok, CountStatus2),
  ?assertEqual(length(User2FilesEnding), Count2),

  lists:foreach(fun(FileEnding) ->
    FileName = FileBeg ++ FileEnding,
    {Status, Answer} = delete_file(Socket, FileName),
    ?assertEqual("ok", Status),
    ?assertEqual(list_to_atom(?VOK), Answer)
  end, User1FilesEnding),

  lists:foreach(fun(FileEnding) ->
    FileName = FileBeg ++ FileEnding,
    {Status, Answer} = delete_file(Socket2, FileName),
    ?assertEqual("ok", Status),
    ?assertEqual(list_to_atom(?VOK), Answer)
  end, User2FilesEnding),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns),
  RemoveUserAns2 = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN2}]),
  ?assertEqual(ok, RemoveUserAns2),

  files_tester:delete_dir(?TEST_ROOT ++ "/users/" ++ Login),
  files_tester:delete_dir(?TEST_ROOT ++ "/users/" ++ Login2),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups/" ++ Teams),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups/" ++ Teams2),

  files_tester:delete_dir(?TEST_ROOT ++ "/users"),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups").


%% Checks permissions management functions
%% The tests checks some files and then changes their permissions. Erlang functions are used to test if permissions were change properly.
permissions_management_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  SHInfo = #storage_helper_info{name = ?SH, init_args = ?TEST_ROOT},
  File = "permissions_management_test_file",

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  AnsCreate = rpc:call(Node1, storage_files_manager, create, [SHInfo, File]),
  ?assertEqual(ok, AnsCreate),
  ?assert(files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  {PermStatus, Perms} = files_tester:get_permissions(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, PermStatus),
  ?assertEqual(8#640, Perms rem 8#01000),

  {OwnStatus, User, Group} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus),
  ?assert(is_integer(User)),
  ?assert(is_integer(Group)),

  NewPerms = 8#521,
  AnsChmod = rpc:call(Node1, storage_files_manager, chmod, [SHInfo, File, NewPerms]),
  ?assertEqual(ok, AnsChmod),

  {PermStatus2, Perms2} = files_tester:get_permissions(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, PermStatus2),
  ?assertEqual(NewPerms, Perms2 rem 8#01000),

  TestUser = "veilfstestuser",
  TestGroup = "veilfstestgroup",
  AnsChown = rpc:call(Node1, storage_files_manager, chown, [SHInfo, File, TestUser, ""]),
  ?assertEqual(ok, AnsChown),

  {OwnStatus2, User2, Group2} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus2),
  ?assert(is_integer(User2)),
  ?assertEqual(false, User =:= User2),
  ?assertEqual(Group, Group2),

  AnsChown2 = rpc:call(Node1, storage_files_manager, chown, [SHInfo, File, "", TestGroup]),
  ?assertEqual(ok, AnsChown2),

  {OwnStatus3, User3, Group3} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus3),
  ?assertEqual(User2, User3),
  ?assertEqual(false, Group =:= Group3),

  AnsChown3 = rpc:call(Node1, storage_files_manager, chown, [SHInfo, File, "root", "root"]),
  ?assertEqual(ok, AnsChown3),

  {OwnStatus4, User4, Group4} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus4),
  ?assertEqual(User, User4),
  ?assertEqual(Group, Group4),

  files_tester:delete(?TEST_ROOT ++ "/" ++ File).

%% Checks user creation (root and dirs at storage creation).
%% The test checks if directories for user and group files are created when the user is added to the system.
user_creation_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  Cert = ?COMMON_FILE("peer.pem"),
  Cert2 = ?COMMON_FILE("peer2.pem"),
  [FSLogicNode | _] = NodesUp,
  SHInfo = #storage_helper_info{name = ?SH, init_args = ?TEST_ROOT},

  Login = "veilfstestuser",
  Name = "user1 user1",
  Team1 = "veilfstestgroup",
  Team2 = "plgteam2",
  Teams = Team1 ++ "(G1)," ++ Team2,
  Email = "user1@email.net",

  Login2 = "plgtestuser2",
  Name2 = "user2 user2",
  Teams2 = Teams,
  Email2 = "user2@email.net",

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/users")),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/groups")),

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/users")),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/groups")),

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/users/" ++ Login)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/users/" ++ Login2)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/groups/" ++ Team1)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/groups/" ++ Team2)),

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/users/" ++ Login)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/users/" ++ Login2)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/groups/" ++ Team1)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/groups/" ++ Team2)),

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  {InsertStorageAns2, StorageUUID2} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?TEST_ROOT2]),
  ?assertEqual(ok, InsertStorageAns2),

  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/users")),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/groups")),

  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/users")),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/groups")),

  {PermStatusUsersDir, PermsUsersDir} = files_tester:get_permissions(?TEST_ROOT ++ "/users"),
  ?assertEqual(ok, PermStatusUsersDir),
  ?assertEqual(8#773, PermsUsersDir rem 8#01000),

  {PermStatusGroupsDir, PermsUserGroupsDir} = files_tester:get_permissions(?TEST_ROOT ++ "/groups"),
  ?assertEqual(ok, PermStatusGroupsDir),
  ?assertEqual(8#773, PermsUserGroupsDir rem 8#01000),

  {PermStatusUsersDir2, PermsUsersDir2} = files_tester:get_permissions(?TEST_ROOT2 ++ "/users"),
  ?assertEqual(ok, PermStatusUsersDir2),
  ?assertEqual(8#773, PermsUsersDir2 rem 8#01000),

  {PermStatusGroupsDir2, PermsUserGroupsDir2} = files_tester:get_permissions(?TEST_ROOT2 ++ "/groups"),
  ?assertEqual(ok, PermStatusGroupsDir2),
  ?assertEqual(8#773, PermsUserGroupsDir2 rem 8#01000),

  {ReadFileAns, PemBin} = file:read_file(Cert),
  ?assertEqual(ok, ReadFileAns),
  {ExtractAns, RDNSequence} = rpc:call(FSLogicNode, user_logic, extract_dn_from_cert, [PemBin]),
  ?assertEqual(rdnSequence, ExtractAns),
  {ConvertAns, DN} = rpc:call(FSLogicNode, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
  ?assertEqual(ok, ConvertAns),
  DnList = [DN],

  {CreateUserAns, _} = rpc:call(FSLogicNode, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
  ?assertEqual(ok, CreateUserAns),

  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/users/" ++ Login)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/users/" ++ Login2)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/groups/" ++ Team1)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/groups/" ++ Team2)),

  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/users/" ++ Login)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/users/" ++ Login2)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/groups/" ++ Team1)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/groups/" ++ Team2)),

  {PermStatus, Perms} = files_tester:get_permissions(?TEST_ROOT ++ "/users/" ++ Login),
  ?assertEqual(ok, PermStatus),
  ?assertEqual(8#300, Perms rem 8#01000),
  {PermStatus2, Perms2} = files_tester:get_permissions(?TEST_ROOT ++ "/groups/" ++ Team1),
  ?assertEqual(ok, PermStatus2),
  ?assertEqual(8#730, Perms2 rem 8#01000),
  {PermStatus3, Perms3} = files_tester:get_permissions(?TEST_ROOT ++ "/groups/" ++ Team2),
  ?assertEqual(ok, PermStatus3),
  ?assertEqual(8#730, Perms3 rem 8#01000),

  {PermStatus4, Perms4} = files_tester:get_permissions(?TEST_ROOT2 ++ "/users/" ++ Login),
  ?assertEqual(ok, PermStatus4),
  ?assertEqual(8#300, Perms4 rem 8#01000),
  {PermStatus5, Perms5} = files_tester:get_permissions(?TEST_ROOT2 ++ "/groups/" ++ Team1),
  ?assertEqual(ok, PermStatus5),
  ?assertEqual(8#730, Perms5 rem 8#01000),
  {PermStatus6, Perms6} = files_tester:get_permissions(?TEST_ROOT2 ++ "/groups/" ++ Team2),
  ?assertEqual(ok, PermStatus6),
  ?assertEqual(8#730, Perms6 rem 8#01000),

  File = "user_creation_test_file",
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  AnsCreate = rpc:call(FSLogicNode, storage_files_manager, create, [SHInfo, File]),
  ?assertEqual(ok, AnsCreate),
  ?assert(files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  {OwnStatus0, User0, Group0} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus0),
  ?assert(is_integer(User0)),
  ?assert(is_integer(Group0)),

  AnsChown = rpc:call(FSLogicNode, storage_files_manager, chown, [SHInfo, File, Login, Login]),
  ?assertEqual(ok, AnsChown),

  {OwnStatus, User, Group} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus),
  ?assert(is_integer(User)),
  ?assert(is_integer(Group)),

  AnsChown2 = rpc:call(FSLogicNode, storage_files_manager, chown, [SHInfo, File, "", Team1]),
  ?assertEqual(ok, AnsChown2),

  {OwnStatus2, User2, Group2} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus2),
  ?assertEqual(User, User2),
  ?assertEqual(false, Group =:= Group2),

  {OwnStatus3, User3, Group3} = files_tester:get_owner(?TEST_ROOT ++ "/users/" ++ Login),
  ?assertEqual(ok, OwnStatus3),
  ?assertEqual(User, User3),
  ?assertEqual(Group, Group3),

  {OwnStatus4, User4, Group4} = files_tester:get_owner(?TEST_ROOT ++ "/groups/" ++ Team1),
  ?assertEqual(ok, OwnStatus4),
  ?assertEqual(User0, User4),
  ?assertEqual(Group2, Group4),

  {OwnStatus5, User5, Group5} = files_tester:get_owner(?TEST_ROOT2 ++ "/users/" ++ Login),
  ?assertEqual(ok, OwnStatus5),
  ?assertEqual(User, User5),
  ?assertEqual(Group, Group5),

  {OwnStatus6, User6, Group6} = files_tester:get_owner(?TEST_ROOT2 ++ "/groups/" ++ Team1),
  ?assertEqual(ok, OwnStatus6),
  ?assertEqual(User0, User6),
  ?assertEqual(Group2, Group6),

  {ReadFileAns2, PemBin2} = file:read_file(Cert2),
  ?assertEqual(ok, ReadFileAns2),
  {ExtractAns2, RDNSequence2} = rpc:call(FSLogicNode, user_logic, extract_dn_from_cert, [PemBin2]),
  ?assertEqual(rdnSequence, ExtractAns2),
  {ConvertAns2, DN2} = rpc:call(FSLogicNode, user_logic, rdn_sequence_to_dn_string, [RDNSequence2]),
  ?assertEqual(ok, ConvertAns2),
  DnList2 = [DN2],

  {CreateUserAns2, _} = rpc:call(FSLogicNode, user_logic, create_user, [Login2, Name2, Teams2, Email2, DnList2]),
  ?assertEqual(ok, CreateUserAns2),

  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/users/" ++ Login)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/users/" ++ Login2)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/groups/" ++ Team1)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/groups/" ++ Team2)),

  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/users/" ++ Login)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/users/" ++ Login2)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/groups/" ++ Team1)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/groups/" ++ Team2)),

  {PermStatus7, Perms7} = files_tester:get_permissions(?TEST_ROOT ++ "/users/" ++ Login2),
  ?assertEqual(ok, PermStatus7),
  ?assertEqual(8#300, Perms7 rem 8#01000),

  {PermStatus8, Perms8} = files_tester:get_permissions(?TEST_ROOT2 ++ "/users/" ++ Login2),
  ?assertEqual(ok, PermStatus8),
  ?assertEqual(8#300, Perms8 rem 8#01000),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveStorageAns2 = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID2}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns2),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns),

  RemoveUserAns2 = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN2}]),
  ?assertEqual(ok, RemoveUserAns2),

  files_tester:delete_dir(?TEST_ROOT ++ "/users/" ++ Login),
  files_tester:delete_dir(?TEST_ROOT ++ "/users/" ++ Login2),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups/" ++ Team1),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups/" ++ Team2),

  files_tester:delete_dir(?TEST_ROOT2 ++ "/users/" ++ Login),
  files_tester:delete_dir(?TEST_ROOT2 ++ "/users/" ++ Login2),
  files_tester:delete_dir(?TEST_ROOT2 ++ "/groups/" ++ Team1),
  files_tester:delete_dir(?TEST_ROOT2 ++ "/groups/" ++ Team2),

  files_tester:delete(?TEST_ROOT ++ "/" ++ File),

  files_tester:delete_dir(?TEST_ROOT ++ "/users"),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups"),

  files_tester:delete_dir(?TEST_ROOT2 ++ "/users"),
  files_tester:delete_dir(?TEST_ROOT2 ++ "/groups").

%% Checks storage management functions
%% The tests checks if functions used to manage user's files at storage (e.g. mv, mkdir) works well.
storage_management_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  SHInfo = #storage_helper_info{name = ?SH, init_args = ?TEST_ROOT},
  File = "storage_management_test_file",
  Dir = "storage_management_test_dir",
  NewDirName = "storage_management_test_dir_new_name",

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  AnsCreate = rpc:call(Node1, storage_files_manager, create, [SHInfo, File]),
  ?assertEqual(ok, AnsCreate),
  ?assert(files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  {CreateStatus2, AnsCreate2} = rpc:call(Node1, storage_files_manager, mkdir, [SHInfo, File]),
  ?assertEqual(error, CreateStatus2),
  ?assertEqual(dir_or_file_exists, AnsCreate2),

  AnsCreate3 = rpc:call(Node1, storage_files_manager, mkdir, [SHInfo, Dir]),
  ?assertEqual(ok, AnsCreate3),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ Dir)),

  AnsMV = rpc:call(Node1, storage_files_manager, mv, [SHInfo, Dir, NewDirName]),
  ?assertEqual(ok, AnsMV),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ NewDirName)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ Dir)),

  {MVStatus2, AnsMV2} = rpc:call(Node1, storage_files_manager, mv, [SHInfo, Dir, NewDirName]),
  ?assertEqual(wrong_rename_return_code, MVStatus2),
  ?assert(is_integer(AnsMV2)),

  AnsDel = rpc:call(Node1, storage_files_manager, delete_dir, [SHInfo, NewDirName]),
  ?assertEqual(ok, AnsDel),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ NewDirName)),

  {DelStatus2, AnsDel2} = rpc:call(Node1, storage_files_manager, delete_dir, [SHInfo, NewDirName]),
  ?assertEqual(wrong_getatt_return_code, DelStatus2),
  ?assert(is_integer(AnsDel2)),

  AnsDel3 = rpc:call(Node1, storage_files_manager, delete, [SHInfo, File]),
  ?assertEqual(ok, AnsDel3),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)).

%% Checks directory moving.
%% The test checks if fslogic blocks dir moving to its child.
dir_mv_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  Cert = ?COMMON_FILE("peer.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  DirName = "dir_mv_test_dir",
  DirName2 = "dir_mv_test_dir2",

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  {ReadFileAns, PemBin} = file:read_file(Cert),
  ?assertEqual(ok, ReadFileAns),
  {ExtractAns, RDNSequence} = rpc:call(FSLogicNode, user_logic, extract_dn_from_cert, [PemBin]),
  ?assertEqual(rdnSequence, ExtractAns),
  {ConvertAns, DN} = rpc:call(FSLogicNode, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
  ?assertEqual(ok, ConvertAns),
  DnList = [DN],

  Login = "user1",
  Name = "user1 user1",
  Teams = "user1 team",
  Email = "user1@email.net",
  {CreateUserAns, _} = rpc:call(FSLogicNode, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
  ?assertEqual(ok, CreateUserAns),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}, auto_handshake]),
  ?assertEqual(ok, ConAns),

  {StatusMkdir, AnswerMkdir} = mkdir(Socket, DirName),
  ?assertEqual("ok", StatusMkdir),
  ?assertEqual(list_to_atom(?VOK), AnswerMkdir),

  {StatusMkdir2, AnswerMkdir2} = mkdir(Socket, DirName2),
  ?assertEqual("ok", StatusMkdir2),
  ?assertEqual(list_to_atom(?VOK), AnswerMkdir2),

  {Status_MV, AnswerMV} = rename_file(Socket, DirName2, DirName ++ "/" ++ DirName2),
  ?assertEqual("ok", Status_MV),
  ?assertEqual(list_to_atom(?VOK), AnswerMV),

  {Status_MV2, AnswerMV2} = rename_file(Socket, DirName, DirName ++ "/" ++ DirName2 ++ "/new_dir_name"),
  ?assertEqual("ok", Status_MV2),
  ?assertEqual(list_to_atom(?VEREMOTEIO), AnswerMV2),

  {Status_MV3, AnswerMV3} = rename_file(Socket, DirName, DirName ++ "/new_dir_name"),
  ?assertEqual("ok", Status_MV3),
  ?assertEqual(list_to_atom(?VEREMOTEIO), AnswerMV3),

  {StatusDelete, AnswerDelete} = delete_file(Socket, DirName ++ "/" ++ DirName2),
  ?assertEqual("ok", StatusDelete),
  ?assertEqual(list_to_atom(?VOK), AnswerDelete),

  {StatusDelete2, AnswerDelete2} = delete_file(Socket, DirName),
  ?assertEqual("ok", StatusDelete2),
  ?assertEqual(list_to_atom(?VOK), AnswerDelete2),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns),

  files_tester:delete_dir(?TEST_ROOT ++ "/users/" ++ Login),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups/" ++ Teams),

  files_tester:delete_dir(?TEST_ROOT ++ "/users"),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups").

%% Checks file sharing functions
file_sharing_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  Cert = ?COMMON_FILE("peer.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  TestFile = "file_sharing_test_file",
  TestFile2 = "file_sharing_test_file2",
  DirName = "file_sharing_test_dir",

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  {ReadFileAns, PemBin} = file:read_file(Cert),
  ?assertEqual(ok, ReadFileAns),
  {ExtractAns, RDNSequence} = rpc:call(FSLogicNode, user_logic, extract_dn_from_cert, [PemBin]),
  ?assertEqual(rdnSequence, ExtractAns),
  {ConvertAns, DN} = rpc:call(FSLogicNode, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
  ?assertEqual(ok, ConvertAns),
  DnList = [DN],

  Login = "user1",
  Name = "user1 user1",
  Teams = "user1 team",
  Email = "user1@email.net",
  {CreateUserAns, User_Doc} = rpc:call(FSLogicNode, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
  ?assertEqual(ok, CreateUserAns),
  put(user_id, DN),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}, auto_handshake]),
  ?assertEqual(ok, ConAns),

  {StatusCreate1, AnsCreate1} = rpc:call(FSLogicNode, fslogic_test_SUITE, create_standard_share, [TestFile, DN]),
  ?assertEqual(error, StatusCreate1),
  ?assertEqual(file_not_found, AnsCreate1),

  {StatusCreateFile, _Helper, _Id, _Validity, AnswerCreateFile} = create_file(Socket, TestFile),
  ?assertEqual("ok", StatusCreateFile),
  ?assertEqual(?VOK, AnswerCreateFile),

  {StatusCreate2, AnsCreate2} = rpc:call(FSLogicNode, fslogic_test_SUITE, create_standard_share, [TestFile, DN]),
  ?assertEqual(ok, StatusCreate2),

  {StatusCreate3, AnsCreate3} = rpc:call(FSLogicNode, fslogic_test_SUITE, create_standard_share, [TestFile, DN]),
  ?assertEqual(exists, StatusCreate3),
  ?assertEqual(AnsCreate2, AnsCreate3#veil_document.uuid),

  {StatusGet, AnsGet} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{uuid, AnsCreate2}, DN]),
  ?assertEqual(ok, StatusGet),
  ?assertEqual(AnsCreate2, AnsGet#veil_document.uuid),

  {StatusGet2, AnsGet2} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{file, TestFile}, DN]),
  ?assertEqual(ok, StatusGet2),
  ?assertEqual(AnsCreate2, AnsGet2#veil_document.uuid),

  {StatusGet3, AnsGet3} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{user, User_Doc#veil_document.uuid}, DN]),
  ?assertEqual(ok, StatusGet3),
  ?assertEqual(AnsCreate2, AnsGet3#veil_document.uuid),
  ShareDoc = AnsGet3#veil_document.record,

  {StatusCreateFile2, _Helper2, _Id2, _Validity2, AnswerCreateFile2} = create_file(Socket, TestFile2),
  ?assertEqual("ok", StatusCreateFile2),
  ?assertEqual(?VOK, AnswerCreateFile2),

  {StatusCreate4, AnsCreate4} = rpc:call(FSLogicNode, fslogic_test_SUITE, create_share, [TestFile, some_share, DN]),
  ?assertEqual(ok, StatusCreate4),

  {StatusCreate5, AnsCreate5} = rpc:call(FSLogicNode, fslogic_test_SUITE, create_standard_share, [TestFile2, DN]),
  ?assertEqual(ok, StatusCreate5),

  {StatusGet4, AnsGet4} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{uuid, AnsCreate4}, DN]),
  ?assertEqual(ok, StatusGet4),
  ?assertEqual(AnsCreate4, AnsGet4#veil_document.uuid),

  {StatusGet5, AnsGet5} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{uuid, AnsCreate5}, DN]),
  ?assertEqual(ok, StatusGet5),
  ?assertEqual(AnsCreate5, AnsGet5#veil_document.uuid),
  ShareDoc2 = AnsGet5#veil_document.record,

  {StatusGet6, AnsGet6} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{file, TestFile}, DN]),
  ?assertEqual(ok, StatusGet6),
  ?assertEqual(2, length(AnsGet6)),
  ?assert(lists:member(AnsGet, AnsGet6)),
  ?assert(lists:member(AnsGet4, AnsGet6)),

  {StatusGet7, AnsGet7} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{user, User_Doc#veil_document.uuid}, DN]),
  ?assertEqual(ok, StatusGet7),
  ?assertEqual(3, length(AnsGet7)),
  ?assert(lists:member(AnsGet, AnsGet7)),
  ?assert(lists:member(AnsGet4, AnsGet7)),
  ?assert(lists:member(AnsGet5, AnsGet7)),

  {StatusGet8, AnsGet8} = rpc:call(FSLogicNode, logical_files_manager, get_file_by_uuid, [ShareDoc#share_desc.file]),
  ?assertEqual(ok, StatusGet8),
  ?assertEqual(ShareDoc#share_desc.file, AnsGet8#veil_document.uuid),
  FileRecord = AnsGet8#veil_document.record,
  ?assertEqual(TestFile, FileRecord#file.name),

  {StatusGet9, AnsGet9} = rpc:call(FSLogicNode, logical_files_manager, get_file_full_name_by_uuid, [ShareDoc#share_desc.file]),
  ?assertEqual(ok, StatusGet9),
  ?assertEqual(Login ++ "/" ++ TestFile, AnsGet9),



  {StatusMkdir, AnswerMkdir} = mkdir(Socket, DirName),
  ?assertEqual("ok", StatusMkdir),
  ?assertEqual(list_to_atom(?VOK), AnswerMkdir),

  {Status_MV, AnswerMV} = rename_file(Socket, TestFile2, DirName ++ "/" ++ TestFile2),
  ?assertEqual("ok", Status_MV),
  ?assertEqual(list_to_atom(?VOK), AnswerMV),

  {StatusGet10, AnsGet10} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{file, DirName ++ "/" ++ TestFile2}, DN]),
  ?assertEqual(ok, StatusGet10),
  ?assertEqual(AnsCreate5, AnsGet10#veil_document.uuid),

  {StatusGet11, AnsGet11} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{file, TestFile2}, DN]),
  ?assertEqual(error, StatusGet11),
  ?assertEqual(file_not_found, AnsGet11),

  {StatusGet12, AnsGet12} = rpc:call(FSLogicNode, logical_files_manager, get_file_full_name_by_uuid, [ShareDoc2#share_desc.file]),
  ?assertEqual(ok, StatusGet12),
  ?assertEqual(Login ++ "/" ++ DirName ++ "/" ++ TestFile2, AnsGet12),




  AnsRemove = rpc:call(FSLogicNode, logical_files_manager, remove_share, [{uuid, AnsCreate2}]),
  ?assertEqual(ok, AnsRemove),


  {StatusDelete, AnswerDelete} = delete_file(Socket, TestFile),
  ?assertEqual("ok", StatusDelete),
  ?assertEqual(list_to_atom(?VOK), AnswerDelete),

  {StatusDelete2, AnswerDelete2} = delete_file(Socket, DirName ++ "/" ++ TestFile2),
  ?assertEqual("ok", StatusDelete2),
  ?assertEqual(list_to_atom(?VOK), AnswerDelete2),

  {StatusDelete, AnswerDelete} = delete_file(Socket, DirName),
  ?assertEqual("ok", StatusDelete),
  ?assertEqual(list_to_atom(?VOK), AnswerDelete),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns),

  files_tester:delete_dir(?TEST_ROOT ++ "/users/" ++ Login),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups/" ++ Teams),

  files_tester:delete_dir(?TEST_ROOT ++ "/users"),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups").

%% Checks fslogic integration with dao and db
fuse_requests_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  Cert = ?COMMON_FILE("peer.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  TestFile = "fslogic_test_file",
  TestFile2 = "fslogic_test_file2",
  DirName = "fslogic_test_dir",
  FilesInDirNames = ["file_in_dir1", "file_in_dir2", "file_in_dir3", "file_in_dir4",  "file_in_dir5"],
  FilesInDir = lists:map(fun(N) ->
    DirName ++ "/" ++ N
  end, FilesInDirNames),
  NewNameOfFIle = "new_name_of_file",

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  {ReadFileAns, PemBin} = file:read_file(Cert),
  ?assertEqual(ok, ReadFileAns),
  {ExtractAns, RDNSequence} = rpc:call(FSLogicNode, user_logic, extract_dn_from_cert, [PemBin]),
  ?assertEqual(rdnSequence, ExtractAns),
  {ConvertAns, DN} = rpc:call(FSLogicNode, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
  ?assertEqual(ok, ConvertAns),
  DnList = [DN],

  Login = "user1",
  Name = "user1 user1",
  Teams = "user1 team",
  Email = "user1@email.net",
  {CreateUserAns, _} = rpc:call(FSLogicNode, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
  ?assertEqual(ok, CreateUserAns),

  %% Connect to cluster
  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}, auto_handshake]),
  ?assertEqual(ok, ConAns),


  {Status, Helper, Id, _Validity, AnswerOpt0} = create_file(Socket, TestFile),
  ?assertEqual("ok", Status),
  ?assertEqual(?VOK, AnswerOpt0),
  {Status1, _Helper1, _Id1, _Validity1, AnswerOpt1} = create_file(Socket, TestFile),
  ?assertEqual("ok", Status1),
  ?assertEqual(?VEEXIST, AnswerOpt1),



  {Status2, Helper2, Id2, _Validity2, AnswerOpt2} = get_file_location(Socket, TestFile),
  ?assertEqual("ok", Status2),
  ?assertEqual(?VOK, AnswerOpt2),
  ?assertEqual(Helper, Helper2),
  ?assertEqual(Id, Id2),

  {Status3, _Validity3, AnswerOpt3} = renew_file_location(Socket, TestFile),
  ?assertEqual("ok", Status3),
  ?assertEqual(?VOK, AnswerOpt3),

  {Status4, Answer4} = file_not_used(Socket, TestFile),
  ?assertEqual("ok", Status4),
  ?assertEqual(list_to_atom(?VOK), Answer4),
  {Status4_1, Answer4_1} = file_not_used(Socket, TestFile),
  ?assertEqual("ok", Status4_1),
  ?assertEqual(list_to_atom(?VOK), Answer4_1),



  %% Test automatic descriptors cleaning
  {Status4_2, Helper4_2, Id4_2, _Validity4_2, AnswerOpt4_2} = get_file_location(Socket, TestFile),
  ?assertEqual("ok", Status4_2),
  ?assertEqual(?VOK, AnswerOpt4_2),
  ?assertEqual(Helper, Helper4_2),
  ?assertEqual(Id, Id4_2),

  clear_old_descriptors(FSLogicNode),

  {Status4_4, _Validity4_4, AnswerOpt4_4} = renew_file_location(Socket, TestFile),
  ?assertEqual("ok", Status4_4),
  ?assertEqual(?VENOENT, AnswerOpt4_4),



  {Status5, Answer5} = mkdir(Socket, DirName),
  ?assertEqual("ok", Status5),
  ?assertEqual(list_to_atom(?VOK), Answer5),
  {Status5_1, Answer5_1} = mkdir(Socket, DirName),
  ?assertEqual("ok", Status5_1),
  ?assertEqual(list_to_atom(?VEEXIST), Answer5_1),

  CreateFile = fun(File) ->
    {Status6, _Helper6, _Id6, _Validity6, AnswerOpt6} = create_file(Socket, File),
    ?assertEqual("ok", Status6),
    ?assertEqual(?VOK, AnswerOpt6)
  end,
  lists:foreach(CreateFile, FilesInDir),

  {Status7, Files7, AnswerOpt7} = ls(Socket, DirName, 10, 0),
  ?assertEqual("ok", Status7),
  ?assertEqual(?VOK, AnswerOpt7),
  ?assertEqual(length(FilesInDir), length(Files7)),
  lists:foreach(fun(Name7) ->
    ?assert(lists:member(Name7, Files7))
  end, FilesInDirNames),


  {Status7_1, Files7_1, AnswerOpt7_1} = ls(Socket, DirName, 3, non),
  ?assertEqual("ok", Status7_1),
  ?assertEqual(?VOK, AnswerOpt7_1),
  ?assertEqual(3, length(Files7_1)),

  {Status7_2, Files7_2, AnswerOpt7_2} = ls(Socket, DirName, 5, 3),
  ?assertEqual("ok", Status7_2),
  ?assertEqual(?VOK, AnswerOpt7_2),
  ?assertEqual(2, length(Files7_2)),
  lists:foreach(fun(Name7_2) ->
    ?assert(lists:member(Name7_2, Files7_2) or lists:member(Name7_2, Files7_1))
  end, FilesInDirNames),



  [FirstFileInDir | FilesInDirTail] = FilesInDir,
  [_ | FilesInDirNamesTail] = FilesInDirNames,
  {Status8, Answer8} = delete_file(Socket, FirstFileInDir),
  ?assertEqual("ok", Status8),
  ?assertEqual(list_to_atom(?VOK), Answer8),

  {Status8_1, Answer8_1} = delete_file(Socket, FirstFileInDir),
  ?assertEqual("ok", Status8_1),
  ?assertEqual(list_to_atom(?VEREMOTEIO), Answer8_1),

  {Status9, Files9, AnswerOpt9} = ls(Socket, DirName, 10, non),
  ?assertEqual("ok", Status9),
  ?assertEqual(?VOK, AnswerOpt9),
  ?assertEqual(length(FilesInDirTail), length(Files9)),
  lists:foreach(fun(Name9) ->
    ?assert(lists:member(Name9, Files9))
  end, FilesInDirNamesTail),

  [SecondFileInDir | FilesInDirTail2] = FilesInDirTail,
  [_ | FilesInDirNamesTail2] = FilesInDirNamesTail,

  {Status19, Attr3} = get_file_attr(Socket, SecondFileInDir),
  ?assertEqual("ok", Status19),


  %% updatetimes message test
  {Status20, Answer20} = update_times(Socket, SecondFileInDir, 1234, 5678),
  ?assertEqual("ok", Status20),
  ?assertEqual(list_to_atom(?VOK), Answer20),

  %% times update is async so we need to wait for it
  timer:sleep(500),
  {Status21, Attr4} = get_file_attr(Socket, SecondFileInDir),
  ?assertEqual("ok", Status21),

  ?assertEqual(1234, Attr4#fileattr.atime),
  ?assertEqual(5678, Attr4#fileattr.mtime),
  %% updatetimes message test end


  timer:sleep(1100),
  {Status10, Answer10} = rename_file(Socket, SecondFileInDir, NewNameOfFIle),
  ?assertEqual("ok", Status10),
  ?assertEqual(list_to_atom(?VOK), Answer10),

  %% ctime update is async so we need to wait for it
  timer:sleep(500),

  {Status17, Attr1} = get_file_attr(Socket, NewNameOfFIle),
  ?assertEqual("ok", Status17),

  %% Check if ctime was updated after rename
  ?assert(Attr1#fileattr.ctime > Attr3#fileattr.ctime),


  timer:sleep(1100),
  {Status10_2, Answer10_2} = change_file_perms(Socket, NewNameOfFIle, 8#400),
  ?assertEqual("ok", Status10_2),
  ?assertEqual(list_to_atom(?VOK), Answer10_2),

  %% ctime update is async so we need to wait for it
  timer:sleep(500),

  {Status18, Attr2} = get_file_attr(Socket, NewNameOfFIle),
  ?assertEqual("ok", Status18),

  %% Check if ctime was updated after chmod
  ?assert(Attr2#fileattr.ctime > Attr1#fileattr.ctime),

  %% Check if perms are set
  ?assertEqual(8#400, Attr2#fileattr.mode),

  {Status11, Files11, AnswerOpt11} = ls(Socket, DirName, 10, non),
  ?assertEqual("ok", Status11),
  ?assertEqual(?VOK, AnswerOpt11),
  ?assertEqual(length(FilesInDirNamesTail2), length(Files11)),
  lists:foreach(fun(Name11) ->
    ?assert(lists:member(Name11, Files11))
  end, FilesInDirNamesTail2),


  %% create file and move to dir
  {Status_MV, _, _, _, AnswerMV} = create_file(Socket, TestFile2),
  ?assertEqual("ok", Status_MV),
  ?assertEqual(?VOK, AnswerMV),

  {Status_MV2, AnswerMV2} = rename_file(Socket, TestFile2, DirName ++ "/" ++ TestFile2),
  ?assertEqual("ok", Status_MV2),
  ?assertEqual(list_to_atom(?VOK), AnswerMV2),

  {Status_MV3, _, _, _, AnswerMV3} = create_file(Socket, TestFile2),
  ?assertEqual("ok", Status_MV3),
  ?assertEqual(?VOK, AnswerMV3),

  {Status_MV4, AnswerMV4} = delete_file(Socket, TestFile2),
  ?assertEqual("ok", Status_MV4),
  ?assertEqual(list_to_atom(?VOK), AnswerMV4),

  {Status_MV5, AnswerMV5} = delete_file(Socket, DirName ++ "/" ++TestFile2),
  ?assertEqual("ok", Status_MV5),
  ?assertEqual(list_to_atom(?VOK), AnswerMV5),



  {Status12, Answer12} = delete_file(Socket, DirName),
  ?assertEqual("ok", Status12),
  ?assertEqual(list_to_atom(?VENOTEMPTY), Answer12),

  Delete = fun(File) ->
    {Status13, Answer13} = delete_file(Socket, File),
    ?assertEqual("ok", Status13),
    ?assertEqual(list_to_atom(?VOK), Answer13)
  end,
  lists:foreach(Delete, FilesInDirTail2),

  {Status14, Answer14} = delete_file(Socket, DirName),
  ?assertEqual("ok", Status14),
  ?assertEqual(list_to_atom(?VOK), Answer14),
  {Status14_1, Answer14_1} = delete_file(Socket, DirName),
  ?assertEqual("ok", Status14_1),
  ?assertEqual(list_to_atom(?VEREMOTEIO), Answer14_1),

  {Status15, Answer15} = delete_file(Socket, TestFile),
  ?assertEqual("ok", Status15),
  ?assertEqual(list_to_atom(?VOK), Answer15),

  {Status16, Answer16} = delete_file(Socket, NewNameOfFIle),
  ?assertEqual("ok", Status16),
  ?assertEqual(list_to_atom(?VOK), Answer16),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns),

  files_tester:delete_dir(?TEST_ROOT ++ "/users/" ++ Login),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups/" ++ Teams),

  files_tester:delete_dir(?TEST_ROOT ++ "/users"),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups").

%% Checks fslogic integration with dao and db
%% This test also checks chown & chgrp behaviour
users_separation_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  Cert = ?COMMON_FILE("peer.pem"),
  Cert2 = ?COMMON_FILE("peer2.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  TestFile = "users_separation_test_file",

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  {ReadFileAns, PemBin} = file:read_file(Cert),
  ?assertEqual(ok, ReadFileAns),
  {ExtractAns, RDNSequence} = rpc:call(FSLogicNode, user_logic, extract_dn_from_cert, [PemBin]),
  ?assertEqual(rdnSequence, ExtractAns),
  {ConvertAns, DN} = rpc:call(FSLogicNode, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
  ?assertEqual(ok, ConvertAns),
  DnList = [DN],

  Login = "user1",
  Name = "user1 user1",
  Teams = "user1 team",
  Email = "user1@email.net",
  {CreateUserAns, #veil_document{uuid = UserID1}} = rpc:call(FSLogicNode, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
  ?assertEqual(ok, CreateUserAns),

  {ReadFileAns2, PemBin2} = file:read_file(Cert2),
  ?assertEqual(ok, ReadFileAns2),
  {ExtractAns2, RDNSequence2} = rpc:call(FSLogicNode, user_logic, extract_dn_from_cert, [PemBin2]),
  ?assertEqual(rdnSequence, ExtractAns2),
  {ConvertAns2, DN2} = rpc:call(FSLogicNode, user_logic, rdn_sequence_to_dn_string, [RDNSequence2]),
  ?assertEqual(ok, ConvertAns2),
  DnList2 = [DN2],

  Login2 = "user2",
  Name2 = "user2 user2",
  Teams2 = "user2 team",
  Email2 = "user2@email.net",
  {CreateUserAns2, #veil_document{uuid = UserID2}} = rpc:call(FSLogicNode, user_logic, create_user, [Login2, Name2, Teams2, Email2, DnList2]),
  ?assertEqual(ok, CreateUserAns2),

  %% Open connections
  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}, auto_handshake]),
  ?assertEqual(ok, ConAns),

  {ConAns1, Socket2} = wss:connect(Host, Port, [{certfile, Cert2}, {cacertfile, Cert2}, auto_handshake]),
  ?assertEqual(ok, ConAns1),

  %% Current time
  Time = fslogic_utils:time(),
  timer:sleep(1100),

  %% Users have different (and next to each other) IDs
  UID1 = list_to_integer(UserID1),
  UID2 = list_to_integer(UserID2),  
  ?assertEqual(UID2, UID1 + 1),

  {Status, Helper, Id, _Validity, AnswerOpt} = create_file(Socket, TestFile),
  ?assertEqual("ok", Status),
  ?assertEqual(?VOK, AnswerOpt),

  {Status2, Helper2, Id2, _Validity2, AnswerOpt2} = get_file_location(Socket, TestFile),
  ?assertEqual("ok", Status2),
  ?assertEqual(?VOK, AnswerOpt2),
  ?assertEqual(Helper, Helper2),
  ?assertEqual(Id, Id2),

  {Status3, _Helper3, _Id3, _Validity3, AnswerOpt3} = get_file_location(Socket2, TestFile),
  ?assertEqual("ok", Status3),
  ?assertEqual(?VENOENT, AnswerOpt3),

  {Status4, Helper4, Id4, _Validity4, AnswerOpt4} = create_file(Socket2, TestFile),
  ?assertEqual("ok", Status4),
  ?assertEqual(?VOK, AnswerOpt4),

  {Status5, Helper5, Id5, _Validity5, AnswerOpt5} = get_file_location(Socket2, TestFile),
  ?assertEqual("ok", Status5),
  ?assertEqual(?VOK, AnswerOpt5),
  ?assertEqual(Helper4, Helper5),
  ?assertEqual(Id4, Id5),

  %% Check if owners are set correctly

  {Status21, Attr1} = get_file_attr(Socket, TestFile),
  ?assertEqual("ok", Status21),
  {Status22, Attr2} = get_file_attr(Socket2, TestFile),
  ?assertEqual("ok", Status22),

  %% Check logins
  ?assertEqual(Login, Attr1#fileattr.uname),
  ?assertEqual(Login2, Attr2#fileattr.uname),

  %% Check UIDs
  ?assertEqual(UID1, Attr1#fileattr.uid),
  ?assertEqual(UID2, Attr2#fileattr.uid),

  timer:sleep(1100), 
  
  %% chown test
  {Status23, Answer23} = chown(Socket, TestFile, 77777, "unknown"),
  ?assertEqual("ok", Status23),
  ?assertEqual(list_to_atom(?VEINVAL), Answer23),

  {Status24, Answer24} = chown(Socket, TestFile, 0, Login2),
  ?assertEqual("ok", Status24),
  ?assertEqual(list_to_atom(?VOK), Answer24),

  {Status25, Answer25} = chown(Socket2, TestFile, UID1, "unknown"),
  ?assertEqual("ok", Status25),
  ?assertEqual(list_to_atom(?VOK), Answer25),

  %% Check if owners are set properly
  {Status26, Attr3} = get_file_attr(Socket, TestFile),
  ?assertEqual("ok", Status26),
  {Status27, Attr4} = get_file_attr(Socket2, TestFile),
  ?assertEqual("ok", Status27),

  %% Check logins
  ?assertEqual(Login2, Attr3#fileattr.uname),
  ?assertEqual(Login, Attr4#fileattr.uname),

  %% Check UIDs
  ?assertEqual(UID2, Attr3#fileattr.uid),
  ?assertEqual(UID1, Attr4#fileattr.uid),

  %% Check if change time was updated and if times was setup correctly on file creation
  ?assert(Attr1#fileattr.atime > Time),
  ?assert(Attr1#fileattr.mtime > Time),
  ?assert(Attr1#fileattr.ctime > Time),
  ?assert(Attr2#fileattr.atime > Time),
  ?assert(Attr2#fileattr.mtime > Time),
  ?assert(Attr2#fileattr.ctime > Time),

  ?assert(Attr3#fileattr.ctime > Attr1#fileattr.ctime),
  ?assert(Attr4#fileattr.ctime > Attr2#fileattr.ctime),

  %% Check attrs in logical_files_manager
  {FMStatys, FM_Attrs} = rpc:call(FSLogicNode, logical_files_manager, getfileattr, [Login2 ++ "/" ++ TestFile]),
  ?assertEqual(ok, FMStatys),
  ?assertEqual(Attr4#fileattr.mode, FM_Attrs#fileattributes.mode),
  ?assertEqual(Attr4#fileattr.uid, FM_Attrs#fileattributes.uid),
  ?assertEqual(Attr4#fileattr.gid, FM_Attrs#fileattributes.gid),
  ?assertEqual(Attr4#fileattr.type, FM_Attrs#fileattributes.type),
  ?assertEqual(Attr4#fileattr.size, FM_Attrs#fileattributes.size),
  ?assertEqual(Attr4#fileattr.uname, FM_Attrs#fileattributes.uname),
  ?assertEqual(Attr4#fileattr.gname, FM_Attrs#fileattributes.gname),
  ?assertEqual(Attr4#fileattr.ctime, FM_Attrs#fileattributes.ctime),
  ?assertEqual(Attr4#fileattr.mtime, FM_Attrs#fileattributes.mtime),
  ?assertEqual(Attr4#fileattr.atime, FM_Attrs#fileattributes.atime),

  {Status6, Answer6} = delete_file(Socket, TestFile),
  ?assertEqual("ok", Status6),
  ?assertEqual(list_to_atom(?VOK), Answer6),

  {Status7, _Helper7, _Id7, _Validity7, AnswerOpt7} = get_file_location(Socket, TestFile),
  ?assertEqual("ok", Status7),
  ?assertEqual(?VENOENT, AnswerOpt7),

  {Status8, Helper8, Id8, _Validity8, AnswerOpt8} = get_file_location(Socket2, TestFile),
  ?assertEqual("ok", Status8),
  ?assertEqual(?VOK, AnswerOpt8),
  ?assertEqual(Helper4, Helper8),
  ?assertEqual(Id4, Id8),

  {Status9, Answer9} = delete_file(Socket2, TestFile),
  ?assertEqual("ok", Status9),
  ?assertEqual(list_to_atom(?VOK), Answer9),

  {Status10, _Helper10, _Id10, _Validity10, AnswerOpt10} = get_file_location(Socket2, TestFile),
  ?assertEqual("ok", Status10),
  ?assertEqual(?VENOENT, AnswerOpt10),

  %% Link tests

  % Create link
  {Status17, Answer17} = create_link(Socket, "link_name", "/target/path"),
  ?assertEqual("ok", Status17),
  ?assertEqual(list_to_atom(?VOK), Answer17),

  % Create same link second time
  {Status18, Answer18} = create_link(Socket, "link_name", "/target/path1"),
  ?assertEqual("ok", Status18),
  ?assertEqual(list_to_atom(?VEEXIST), Answer18),

  % Check if created link has valid data
  {Status19, Answer19, LinkPath} = get_link(Socket, "link_name"),
  ?assertEqual("ok", Status19),
  ?assertEqual("ok", Answer19),
  ?assertEqual("/target/path", LinkPath),

  {Status19_2, Answer19_2} = delete_file(Socket, "link_name"),
  ?assertEqual("ok", Status19_2),
  ?assertEqual(list_to_atom(?VOK), Answer19_2),

  % Try to fetch invalid link data
  {Status20, Answer20, _} = get_link(Socket, "link_name1"),
  ?assertEqual("ok", Status20),
  ?assertEqual(?VENOENT, Answer20),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns),
  RemoveUserAns2 = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN2}]),
  ?assertEqual(ok, RemoveUserAns2),

  files_tester:delete_dir(?TEST_ROOT ++ "/users/" ++ Login),
  files_tester:delete_dir(?TEST_ROOT ++ "/users/" ++ Login2),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups/" ++ Teams),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups/" ++ Teams2),

  files_tester:delete_dir(?TEST_ROOT ++ "/users"),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups").

%% Checks files manager (manipulation on tmp files copies)
files_manager_tmp_files_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  SHInfo = #storage_helper_info{name = ?SH, init_args = ?TEST_ROOT},
  File = "files_manager_test_file1",
  NotExistingFile = "files_manager_test_not_existing_file",

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ NotExistingFile)),

  AnsCreate = rpc:call(Node1, storage_files_manager, create, [SHInfo, File]),
  ?assertEqual(ok, AnsCreate),
  ?assert(files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  AnsCreate2 = rpc:call(Node1, storage_files_manager, create, [SHInfo, File]),
  ?assertEqual({error,file_exists}, AnsCreate2),

  AnsWrite1 = rpc:call(Node1, storage_files_manager, write, [SHInfo, File, list_to_binary("abcdefgh")]),
  ?assertEqual(8, AnsWrite1),
  ?assertEqual({ok, "abcdefgh"}, files_tester:read_file_storage(?TEST_ROOT ++ "/" ++ File, 100)),

  {StatusRead1, AnsRead1} = rpc:call(Node1, storage_files_manager, read, [SHInfo, File, 2, 2]),
  ?assertEqual(ok, StatusRead1),
  ?assertEqual("cd", binary_to_list(AnsRead1)),

  {StatusRead2, AnsRead2} = rpc:call(Node1, storage_files_manager, read, [SHInfo, File, 7, 2]),
  ?assertEqual(ok, StatusRead2),
  ?assertEqual("h", binary_to_list(AnsRead2)),

  AnsWrite2 = rpc:call(Node1, storage_files_manager, write, [SHInfo, File, 3, list_to_binary("123")]),
  ?assertEqual(3, AnsWrite2),
  ?assertEqual({ok, "abc123gh"}, files_tester:read_file_storage(?TEST_ROOT ++ "/" ++ File, 100)),

  {StatusRead3, AnsRead3} = rpc:call(Node1, storage_files_manager, read, [SHInfo, File, 2, 5]),
  ?assertEqual(ok, StatusRead3),
  ?assertEqual("c123g", binary_to_list(AnsRead3)),

  AnsWrite3 = rpc:call(Node1, storage_files_manager, write, [SHInfo, File, list_to_binary("XYZ")]),
  ?assertEqual(3, AnsWrite3),
  ?assertEqual({ok, "abc123ghXYZ"}, files_tester:read_file_storage(?TEST_ROOT ++ "/" ++ File, 100)),

  {StatusRead4, AnsRead4} = rpc:call(Node1, storage_files_manager, read, [SHInfo, File, 2, 5]),
  ?assertEqual(ok, StatusRead4),
  ?assertEqual("c123g", binary_to_list(AnsRead4)),

  {StatusRead5, AnsRead5} = rpc:call(Node1, storage_files_manager, read, [SHInfo, File, 0, 100]),
  ?assertEqual(ok, StatusRead5),
  ?assertEqual("abc123ghXYZ", binary_to_list(AnsRead5)),

  AnsTruncate = rpc:call(Node1, storage_files_manager, truncate, [SHInfo, File, 5]),
  ?assertEqual(ok, AnsTruncate),
  ?assertEqual({ok, "abc12"}, files_tester:read_file_storage(?TEST_ROOT ++ "/" ++ File, 100)),

  {StatusRead5_1, AnsRead5_1} = rpc:call(Node1, storage_files_manager, read, [SHInfo, File, 0, 100]),
  ?assertEqual(ok, StatusRead5_1),
  ?assertEqual("abc12", binary_to_list(AnsRead5_1)),

  {StatusRead6, AnsRead6} = rpc:call(Node1, storage_files_manager, read, [SHInfo, NotExistingFile, 0, 100]),
  ?assertEqual(wrong_getatt_return_code, StatusRead6),
  ?assert(is_integer(AnsRead6)),

  AnsDel = rpc:call(Node1, storage_files_manager, delete, [SHInfo, File]),
  ?assertEqual(ok, AnsDel),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  {StatusDel2, AnsDel2}  = rpc:call(Node1, storage_files_manager, delete, [SHInfo, File]),
  ?assertEqual(wrong_getatt_return_code, StatusDel2),
  ?assert(is_integer(AnsDel2)).

%% Checks files manager (manipulation on users' files)
files_manager_standard_files_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  {InsertStorageAns, StorageUUID} = rpc:call(Node1, fslogic_storage, insert_storage, ["DirectIO", ?TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  TestFile = "files_manager_standard_test_file",
  DirName = "fslogic_test_dir2",
  FileInDir = "files_manager_test_file2",
  FileInDir2 = "files_manager_test_file3",
  FileInDir2NewName = "files_manager_test_file3_new_name",
  File = DirName ++ "/" ++ FileInDir,
  File2 = DirName ++ "/" ++ FileInDir2,
  File2NewName = DirName ++ "/" ++ FileInDir2NewName,

  NotExistingFile = "files_manager_test_not_existing_file",

  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [TestFile])),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [File])),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [File2])),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [File2NewName])),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [NotExistingFile])),

  MkDirAns = rpc:call(Node1, logical_files_manager, mkdir, [DirName]),
  ?assertEqual(ok, MkDirAns),

  MkDirAns2 = rpc:call(Node1, logical_files_manager, mkdir, [DirName]),
  ?assertEqual({logical_file_system_error, ?VEEXIST}, MkDirAns2),

  ?assertEqual(false, rpc:call(Node1, logical_files_manager, exists, [File])),

  AnsCreate = rpc:call(Node1, logical_files_manager, create, [File]),
  ?assertEqual(ok, AnsCreate),
  ?assert(rpc:call(Node1, files_tester, file_exists, [File])),

  ?assert(rpc:call(Node1, logical_files_manager, exists, [File])),

  AnsCreate2 = rpc:call(Node1, logical_files_manager, create, [File]),
  ?assertEqual({logical_file_system_error, ?VEEXIST}, AnsCreate2),

  AnsWrite1 = rpc:call(Node1, logical_files_manager, write, [File, list_to_binary("abcdefgh")]),
  ?assertEqual(8, AnsWrite1),
  ?assertEqual({ok, "abcdefgh"}, rpc:call(Node1, files_tester, read_file, [File, 100])),

  {StatusRead1, AnsRead1} = rpc:call(Node1, logical_files_manager, read, [File, 2, 2]),
  ?assertEqual(ok, StatusRead1),
  ?assertEqual("cd", binary_to_list(AnsRead1)),

  {StatusRead2, AnsRead2} = rpc:call(Node1, logical_files_manager, read, [File, 7, 2]),
  ?assertEqual(ok, StatusRead2),
  ?assertEqual("h", binary_to_list(AnsRead2)),

  AnsWrite2 = rpc:call(Node1, logical_files_manager, write, [File, 3, list_to_binary("123")]),
  ?assertEqual(3, AnsWrite2),
  ?assertEqual({ok, "abc123gh"}, rpc:call(Node1, files_tester, read_file, [File, 100])),

  {StatusRead3, AnsRead3} = rpc:call(Node1, logical_files_manager, read, [File, 2, 5]),
  ?assertEqual(ok, StatusRead3),
  ?assertEqual("c123g", binary_to_list(AnsRead3)),

  AnsWrite3 = rpc:call(Node1, logical_files_manager, write, [File, list_to_binary("XYZ")]),
  ?assertEqual(3, AnsWrite3),
  ?assertEqual({ok, "abc123ghXYZ"}, rpc:call(Node1, files_tester, read_file, [File, 100])),

  {StatusRead4, AnsRead4} = rpc:call(Node1, logical_files_manager, read, [File, 2, 5]),
  ?assertEqual(ok, StatusRead4),
  ?assertEqual("c123g", binary_to_list(AnsRead4)),

  {StatusRead5, AnsRead5} = rpc:call(Node1, logical_files_manager, read, [File, 0, 100]),
  ?assertEqual(ok, StatusRead5),
  ?assertEqual("abc123ghXYZ", binary_to_list(AnsRead5)),

  AnsTruncate = rpc:call(Node1, logical_files_manager, truncate, [File, 5]),
  ?assertEqual(ok, AnsTruncate),
  ?assertEqual({ok, "abc12"}, rpc:call(Node1, files_tester, read_file, [File, 100])),

  {StatusRead5_1, AnsRead5_1} = rpc:call(Node1, logical_files_manager, read, [File, 0, 100]),
  ?assertEqual(ok, StatusRead5_1),
  ?assertEqual("abc12", binary_to_list(AnsRead5_1)),

  {StatusRead6, AnsRead6} = rpc:call(Node1, logical_files_manager, read, [NotExistingFile, 0, 100]),
  ?assertEqual(logical_file_system_error, StatusRead6),
  ?assertEqual(?VENOENT, AnsRead6),

  AnsCreate3 = rpc:call(Node1, logical_files_manager, create, [File2]),
  ?assertEqual(ok, AnsCreate3),
  ?assert(rpc:call(Node1, files_tester, file_exists, [File2])),

  {StatusLs, AnsLs} = rpc:call(Node1, logical_files_manager, ls, [DirName, 100, 0]),
  ?assertEqual(ok, StatusLs),
  ?assertEqual(2, length(AnsLs)),
  ?assert(lists:member(FileInDir, AnsLs)),
  ?assert(lists:member(FileInDir2, AnsLs)),

  {File2LocationAns, File2Location} = rpc:call(Node1, files_tester, get_file_location, [File2]),
  ?assertEqual(ok, File2LocationAns),
  AnsMv = rpc:call(Node1, logical_files_manager, mv, [File2, File2NewName]),
  ?assertEqual(ok, AnsMv),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [File2])),
  ?assert(rpc:call(Node1, files_tester, file_exists, [File2NewName])),
  {File2LocationAns2, File2Location2} = rpc:call(Node1, files_tester, get_file_location, [File2NewName]),
  ?assertEqual(ok, File2LocationAns2),
  ?assertEqual(File2Location, File2Location2),

  AnsChPerm = rpc:call(Node1, logical_files_manager, change_file_perm, [File, 8#777]),
  ?assertEqual(ok, AnsChPerm),

  {StatusLs2, AnsLs2} = rpc:call(Node1, logical_files_manager, ls, [DirName, 100, 0]),
  ?assertEqual(ok, StatusLs2),
  ?assertEqual(2, length(AnsLs2)),
  ?assert(lists:member(FileInDir, AnsLs2)),
  ?assert(lists:member(FileInDir2NewName, AnsLs2)),




  %% create file and move to dir
  AnsCreate4 = rpc:call(Node1, logical_files_manager, create, [TestFile]),
  ?assertEqual(ok, AnsCreate4),
  ?assert(rpc:call(Node1, files_tester, file_exists, [TestFile])),

  {TestFileLocationAns, TestFileLocation} = rpc:call(Node1, files_tester, get_file_location, [TestFile]),
  ?assertEqual(ok, TestFileLocationAns),
  AnsMv2 = rpc:call(Node1, logical_files_manager, mv, [TestFile, DirName ++ "/" ++ TestFile]),
  ?assertEqual(ok, AnsMv2),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [TestFile])),
  ?assert(rpc:call(Node1, files_tester, file_exists, [DirName ++ "/" ++ TestFile])),
  {TestFileLocationAns2, TestFileLocation2} = rpc:call(Node1, files_tester, get_file_location, [DirName ++ "/" ++ TestFile]),
  ?assertEqual(ok, TestFileLocationAns2),
  ?assertEqual(TestFileLocation, TestFileLocation2),

  AnsCreate5 = rpc:call(Node1, logical_files_manager, create, [TestFile]),
  ?assertEqual(ok, AnsCreate5),
  ?assert(rpc:call(Node1, files_tester, file_exists, [TestFile])),
  {TestFileLocationAns3, TestFileLocation3} = rpc:call(Node1, files_tester, get_file_location, [TestFile]),
  ?assertEqual(ok, TestFileLocationAns3),
  ?assertEqual(false, TestFileLocation =:= TestFileLocation3),

  AnsMvDel = rpc:call(Node1, logical_files_manager, delete, [TestFile]),
  ?assertEqual(ok, AnsMvDel),
  ?assertEqual(false, files_tester:file_exists_storage(TestFileLocation3)),
  ?assert(files_tester:file_exists_storage(TestFileLocation)),

  AnsMvDel2 = rpc:call(Node1, logical_files_manager, delete, [DirName ++ "/" ++ TestFile]),
  ?assertEqual(ok, AnsMvDel2),
  ?assertEqual(false, files_tester:file_exists_storage(TestFileLocation)),



  {FileLocationAns, FileLocation} = rpc:call(Node1, files_tester, get_file_location, [File]),
  ?assertEqual(ok, FileLocationAns),
  AnsDel = rpc:call(Node1, logical_files_manager, delete, [File]),
  ?assertEqual(ok, AnsDel),
  ?assertEqual(false, files_tester:file_exists_storage(FileLocation)),

  AnsDel2 = rpc:call(Node1, logical_files_manager, delete, [File2NewName]),
  ?assertEqual(ok, AnsDel2),
  ?assertEqual(false, files_tester:file_exists_storage(File2Location2)),

  AnsDel3 = rpc:call(Node1, logical_files_manager, delete, [File2NewName]),
  ?assertEqual({logical_file_system_error, ?VENOENT}, AnsDel3),

  AnsDirDelete = rpc:call(Node1, logical_files_manager, rmdir, [DirName]),
  ?assertEqual(ok, AnsDirDelete),

  AnsDirDelete2 = rpc:call(Node1, logical_files_manager, rmdir, [DirName]),
  ?assertEqual({logical_file_system_error, ?VEREMOTEIO}, AnsDirDelete2),

  RemoveStorageAns = rpc:call(Node1, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  files_tester:delete_dir(?TEST_ROOT ++ "/users"),
  files_tester:delete_dir(?TEST_ROOT ++ "/groups").

%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_, Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),

  NodesUp = nodes_manager:start_test_on_nodes(1),
  [FSLogicNode | _] = NodesUp,

  DB_Node = nodes_manager:get_db_node(),
  Port = 6666,
  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [FSLogicNode]}, {dns_port, 1317}, {db_nodes, [DB_Node]}]]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{port, Port}, {nodes, NodesUp}, {assertions, Assertions}], Config).

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  [FSLogicNode | _] = Nodes,
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),
  nodes_manager:stop_deps_for_tester_node(),

  %% Remove users
  rpc:call(FSLogicNode, user_logic, remove_user, [{login, "user1"}]),
  rpc:call(FSLogicNode, user_logic, remove_user, [{login, "user2"}]),

  %% Clear test dir
  os:cmd("rm -rf " ++ ?TEST_ROOT ++ "/*"),

  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns).

%% ====================================================================
%% Helper functions
%% ====================================================================

%% Each of following functions simulate one request from FUSE.
create_file(Socket, FileName) ->
  FslogicMessage = #getnewfilelocation{file_logic_name = FileName, mode = 8#644},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getnewfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getnewfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocation",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Location = fuse_messages_pb:decode_filelocation(Bytes),
  Location2 = records_translator:translate(Location, "fuse_messages"),
  {Status, Location2#filelocation.storage_helper_name, Location2#filelocation.file_id, Location2#filelocation.validity, Location2#filelocation.answer}.

get_file_location(Socket, FileName) ->
  FslogicMessage = #getfilelocation{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocation",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Location = fuse_messages_pb:decode_filelocation(Bytes),
  Location2 = records_translator:translate(Location, "fuse_messages"),
  {Status, Location2#filelocation.storage_helper_name, Location2#filelocation.file_id, Location2#filelocation.validity, Location2#filelocation.answer}.

renew_file_location(Socket, FileName) ->
  FslogicMessage = #renewfilelocation{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_renewfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "renewfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocationvalidity",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Validity = fuse_messages_pb:decode_filelocationvalidity(Bytes),
  Validity2 = records_translator:translate(Validity, "fuse_messages"),
  {Status, Validity2#filelocationvalidity.validity, Validity2#filelocationvalidity.answer}.

file_not_used(Socket, FileName) ->
  FslogicMessage = #filenotused{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_filenotused(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "filenotused", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

mkdir(Socket, DirName) ->
  FslogicMessage = #createdir{dir_logic_name = DirName, mode = 8#644},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_createdir(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "createdir", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

ls(Socket, Dir, Num, Offset) ->
  FslogicMessage = case Offset of
                     non -> #getfilechildren{dir_logic_name = Dir, children_num = Num};
                     _Other -> #getfilechildren{dir_logic_name = Dir, children_num = Num, offset = Offset}
                   end,
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilechildren(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getfilechildren", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filechildren",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Files = fuse_messages_pb:decode_filechildren(Bytes),
  Files2 = records_translator:translate(Files, "fuse_messages"),
  {Status, Files2#filechildren.child_logic_name, Files2#filechildren.answer}.

delete_file(Socket, FileName) ->
  FslogicMessage = #deletefile{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_deletefile(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "deletefile", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

rename_file(Socket, FileName, NewName) ->
  FslogicMessage = #renamefile{from_file_logic_name = FileName, to_file_logic_name = NewName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_renamefile(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "renamefile", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

change_file_perms(Socket, FileName, Perms) ->
  FslogicMessage = #changefileperms{file_logic_name = FileName, perms = Perms},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_changefileperms(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "changefileperms", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

create_link(Socket, From, To) ->
  FslogicMessage = #createlink{from_file_logic_name = From, to_file_logic_name = To},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_createlink(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "createlink", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

get_link(Socket, FileName) ->
  FslogicMessage = #getlink{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getlink(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getlink", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "linkinfo",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Resp = fuse_messages_pb:decode_linkinfo(Bytes),
  Resp1 = records_translator:translate(Resp, "fuse_messages"),
  {Status, Resp#linkinfo.answer, Resp1#linkinfo.file_logic_name}.

get_file_attr(Socket, FileName) ->
  FslogicMessage = #getfileattr{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfileattr(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getfileattr", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "fileattr",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Resp = fuse_messages_pb:decode_fileattr(Bytes),
  Resp1 = records_translator:translate(Resp, "fuse_messages"),
  {Status, Resp1}.

update_times(Socket, FileName, ATime, MTime) ->
    FslogicMessage = #updatetimes{file_logic_name = FileName, atime = ATime, mtime = MTime},
    FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_updatetimes(FslogicMessage)),

    FuseMessage = #fusemessage{message_type = "updatetimes", input = FslogicMessageMessageBytes},
    FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

    Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
    MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

    wss:send(Socket, MessageBytes),
    {SendAns, Ans} = wss:recv(Socket, 5000),
    ?assertEqual(ok, SendAns),

    #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
    Answer = communication_protocol_pb:decode_atom(Bytes),
    Answer2 = records_translator:translate(Answer, "communication_protocol"),
    {Status, Answer2}.

chown(Socket, FileName, UID, UName) ->
    FslogicMessage = #changefileowner{file_logic_name = FileName, uid = UID, uname = UName},
    FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_changefileowner(FslogicMessage)),

    FuseMessage = #fusemessage{message_type = "changefileowner", input = FslogicMessageMessageBytes},
    FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

    Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
    MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

    wss:send(Socket, MessageBytes),
    {SendAns, Ans} = wss:recv(Socket, 5000),
    ?assertEqual(ok, SendAns),

    #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
    Answer = communication_protocol_pb:decode_atom(Bytes),
    Answer2 = records_translator:translate(Answer, "communication_protocol"),
    {Status, Answer2}.

chgrp(Socket, FileName, GID, GName) ->
    FslogicMessage = #changefilegroup{file_logic_name = FileName, gid = GID, gname = GName},
    FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_changefilegroup(FslogicMessage)),

    FuseMessage = #fusemessage{message_type = "changefilegroup", input = FslogicMessageMessageBytes},
    FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

    Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
    MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

    wss:send(Socket, MessageBytes),
    {SendAns, Ans} = wss:recv(Socket, 5000),
    ?assertEqual(ok, SendAns),

    #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
    Answer = communication_protocol_pb:decode_atom(Bytes),
    Answer2 = records_translator:translate(Answer, "communication_protocol"),
    {Status, Answer2}.


clear_old_descriptors(Node) ->
  {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
  Time = 1000000*Megaseconds + Seconds + 60*15 + 1,
  gen_server:call({?Dispatcher_Name, Node}, {fslogic, 1, {delete_old_descriptors_test, Time}}),
  timer:sleep(500).

create_standard_share(TestFile, DN) ->
  put(user_id, DN),
  logical_files_manager:create_standard_share(TestFile).

create_share(TestFile, Share_With, DN) ->
  put(user_id, DN),
  logical_files_manager:create_share(TestFile, Share_With).

get_share(Key, DN) ->
  put(user_id, DN),
  logical_files_manager:get_share(Key).
