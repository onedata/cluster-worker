%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of sequencer manager API.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files_test_SUITE).
-author("Rafal Slota").

-include("modules/datastore/datastore.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    fslogic_new_file_test/1,
    lfm_create_test/1,
    lfm_write_test/1
]).

-performance({test_cases, []}).
all() -> [
    fslogic_new_file_test,
    lfm_create_test,
    lfm_write_test
].

-define(TIMEOUT, timer:seconds(5)).

-define(req(W, SessId, FuseRequest), rpc:call(W, worker_proxy, call, [fslogic_worker, {fuse_request, SessId, FuseRequest}])).
-define(lfm_req(W, Method, Args), rpc:call(W, file_manager, Method, Args)).

%%%====================================================================
%%% Test function
%%%====================================================================


fslogic_new_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},

    ct:print("OMG ~p", [rpc:call(Worker, ets, info, [lfm_handles])]),
    [S] = ?config(servers, Config),
    ct:print("OMG ~p ~p", [S, S]),

    RootUUID1 = get_uuid_privileged(Worker, SessId1, <<"/">>),
    RootUUID2 = get_uuid_privileged(Worker, SessId2, <<"/">>),

    ct:print("New loc: ~p", [?req(Worker, SessId1, #get_new_file_location{parent_uuid = RootUUID1, name = <<"test">>})]),

    ok.

lfm_create_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},

    RootUUID1 = get_uuid_privileged(W, SessId1, <<"/">>),
    RootUUID2 = get_uuid_privileged(W, SessId2, <<"/">>),

    ?assertMatch({ok, _}, create(W, SessId1, <<"/test1">>, 8#755)),
    ?assertMatch({ok, _}, create(W, SessId1, <<"/test2">>, 8#755)),
    ?assertMatch({error, ?EEXIST}, create(W, SessId1, <<"/test1">>, 8#755)),

    ?assertMatch({ok, _}, create(W, SessId2, <<"/test1">>, 8#755)),
    ?assertMatch({ok, _}, create(W, SessId2, <<"/test2">>, 8#755)),
    ?assertMatch({error, ?EEXIST}, create(W, SessId2, <<"/test1">>, 8#755)),

    ok.


lfm_write_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},

    RootUUID1 = get_uuid_privileged(W, SessId1, <<"/">>),
    RootUUID2 = get_uuid_privileged(W, SessId2, <<"/">>),

    ?assertMatch({ok, _}, create(W, SessId1, <<"/test3">>, 8#755)),
    ?assertMatch({ok, _}, create(W, SessId1, <<"/test4">>, 8#755)),

    ?assertMatch({ok, _}, create(W, SessId2, <<"/test3">>, 8#755)),
    ?assertMatch({ok, _}, create(W, SessId2, <<"/test4">>, 8#755)),

    O11 = open(W, SessId1, {path, <<"/test3">>}, rdwr),
    O12 = open(W, SessId1, {path, <<"/test4">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    ?assertMatch({ok, _}, O12),

    {ok, Handle11} = O11,
    {ok, Handle12} = O12,

    WriteAndTest =
        fun(Worker, Handle, Offset, Bytes) ->
            ct:print("Testing lfm write with offset ~p and binary ~p", [Offset, Bytes]),
            Size = size(Bytes),
            ?assertMatch({ok, Size}, write(Worker, Handle, Offset, Bytes)),
            for(Offset, Offset + Size - 1,
                fun(I) ->
                    for(1, Offset + Size - I,
                        fun(J) ->
                            SubBytes = binary:part(Bytes, I - Offset, J),
                            ct:print("===== Offset ~p Size ~p Expected ~p", [I, J, SubBytes]),
                            ?assertMatch({ok, SubBytes}, read(Worker, Handle, I, J))
                        end)
                end)



        end,

    WriteAndTest(W, Handle11, 0, <<"abc">>),
    WriteAndTest(W, Handle12, 0, <<"abc">>),

    WriteAndTest(W, Handle11, 3, <<"def">>),
    WriteAndTest(W, Handle12, 3, <<"def">>),

    WriteAndTest(W, Handle11, 2, <<"qwerty">>),
    WriteAndTest(W, Handle12, 2, <<"qwerty">>),

    WriteAndTest(W, Handle11, 8, <<"zxcvbnm">>),
    WriteAndTest(W, Handle12, 8, <<"zxcvbnm">>),

    WriteAndTest(W, Handle11, 6, <<"qwerty">>),
    WriteAndTest(W, Handle12, 6, <<"qwerty">>),

    WriteAndTest(W, Handle11, 10, crypto:rand_bytes(20)),
    WriteAndTest(W, Handle12, 10, crypto:rand_bytes(20)),

    ok.

%% Get uuid of given by path file. Possible as root to bypass permissions checks.
get_uuid_privileged(Worker, SessId, Path) ->
    SessId1 = case Path of
                  <<"/">> ->
                      SessId;
                  <<"/spaces">> ->
                      SessId;
                  _ ->
                      ?ROOT_SESS_ID
              end,
    get_uuid(Worker, SessId1, Path).


get_uuid(Worker, SessId, Path) ->
    RootFileAttr = ?req(Worker, SessId, #get_file_attr{entry = {path, Path}}),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootFileAttr),
    #fuse_response{fuse_response = #file_attr{uuid = UUID}} = RootFileAttr,
    UUID.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Config1 = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
    [Worker | _] = ?config(op_worker_nodes, Config1),
    {ok, _} = rpc:call(Worker, storage, create, [#document{value = fslogic_storage:new_storage(<<"Test">>,
        [fslogic_storage:new_helper_init(<<"DirectIO">>, [<<?TEMP_DIR>>])])}]),
    Config1.

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),

    file_meta_mock_setup(Workers),
    Space1 = {<<"space_id1">>, <<"space_name1">>},
    Space2 = {<<"space_id2">>, <<"space_name2">>},
    Space3 = {<<"space_id3">>, <<"space_name3">>},
    Space4 = {<<"space_id4">>, <<"space_name4">>},
    gr_spaces_mock_setup(Workers, [Space1, Space2, Space3, Space4]),

    User1 = {1, [<<"space_id1">>, <<"space_id2">>, <<"space_id3">>, <<"space_id4">>]},
    User2 = {2, [<<"space_id2">>, <<"space_id3">>, <<"space_id4">>]},
    User3 = {3, [<<"space_id3">>, <<"space_id4">>]},
    User4 = {4, [<<"space_id4">>]},

    Host = self(),
    Servers = lists:map(
        fun(W) ->
            spawn_link(W,
                fun() ->
                    lfm_handles = ets:new(lfm_handles, [public, set, named_table]),
                    Host ! {self(), done},
                    receive
                        exit -> ok
                    end
                end)
        end, ?config(op_worker_nodes, Config)),

    lists:foreach(
        fun(Server) ->
            receive
                {Server, done} -> ok
            after timer:seconds(5) ->
                error("Cannot setup lfm_handles ETS")
            end
        end, Servers),


    Config1 = session_setup(Worker, [User1, User2, User3, User4], Config),
    [{servers, Servers} | Config1].

end_per_testcase(_, Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),

    lists:foreach(
        fun(Pid) ->
            Pid ! exit
        end, ?config(servers, Config)),

    session_teardown(Worker, Config),
    mocks_teardown(Workers, [file_meta, gr_spaces]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new test session.
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node(), [{UserNum :: non_neg_integer(), [SpaceIds :: binary()]}], Config :: term()) -> NewConfig :: term().
session_setup(_Worker, [], Config) ->
    Config;
session_setup(Worker, [{UserNum, SpaceIds} | R], Config) ->
    Self = self(),

    Name = fun(Text, Num) -> name(Text, Num) end,

    SessId = Name("session_id", UserNum),
    UserId = Name("user_id", UserNum),
    Iden = #identity{user_id = UserId},
    UserName = Name("username", UserNum),

    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, Iden, Self])),
    {ok, #document{value = Session}} = rpc:call(Worker, session, get, [SessId]),
    {ok, _} = rpc:call(Worker, onedata_user, create, [
        #document{key = UserId, value = #onedata_user{
            name = UserName, space_ids = SpaceIds
        }}
    ]),
    ?assertEqual({ok, onedata_user_setup}, test_utils:receive_msg(
        onedata_user_setup, ?TIMEOUT)),
    [
        {{spaces, UserNum}, SpaceIds}, {{user_id, UserNum}, UserId}, {{session_id, UserNum}, SessId},
        {{fslogic_ctx, UserNum}, #fslogic_ctx{session = Session}}
        | session_setup(Worker, R, Config)
    ].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes existing test session.
%% @end
%%--------------------------------------------------------------------
-spec session_teardown(Worker :: node(), Config :: term()) -> NewConfig :: term().
session_teardown(Worker, Config) ->
    lists:foldl(fun
                    ({{session_id, _}, SessId}, Acc) ->
                        ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])),
                        Acc;
                    ({{spaces, _}, SpaceIds}, Acc) ->
                        lists:foreach(fun(SpaceId) ->
                            ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [SpaceId]))
                                      end, SpaceIds),
                        Acc;
                    ({{user_id, _}, UserId}, Acc) ->
                        ?assertEqual(ok, rpc:call(Worker, onedata_user, delete, [UserId])),
                        ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [UserId])),
                        ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [fslogic_path:spaces_uuid(UserId)])),
                        Acc;
                    ({{fslogic_ctx, _}, _}, Acc) ->
                        Acc;
                    (Elem, Acc) ->
                        [Elem | Acc]
                end, [], Config).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks gr_spaces module, so that it returns default space details for default
%% space ID.
%% @end
%%--------------------------------------------------------------------
-spec gr_spaces_mock_setup(Workers :: node() | [node()],
    [{binary(), binary()}]) -> ok.
gr_spaces_mock_setup(Workers, Spaces) ->
    test_utils:mock_new(Workers, gr_spaces),
    test_utils:mock_expect(Workers, gr_spaces, get_details,
        fun(provider, SpaceId) ->
            {_, SpaceName} = lists:keyfind(SpaceId, 1, Spaces),
            {ok, #space_details{name = SpaceName}}
        end
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks file_meta module, so that creation of onedata user sends notification.
%% @end
%%--------------------------------------------------------------------
-spec file_meta_mock_setup(Workers :: node() | [node()]) -> ok.
file_meta_mock_setup(Workers) ->
    Self = self(),
    test_utils:mock_new(Workers, file_meta),
    test_utils:mock_expect(Workers, file_meta, 'after',
        fun(onedata_user, create, _, _, {ok, UUID}) ->
            file_meta:setup_onedata_user(UUID),
            Self ! onedata_user_setup
        end
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Validates and unloads mocks.
%% @end
%%--------------------------------------------------------------------
-spec mocks_teardown(Workers :: node() | [node()],
    Modules :: module() | [module()]) -> ok.
mocks_teardown(Workers, Modules) ->
    test_utils:mock_validate(Workers, Modules),
    test_utils:mock_unload(Workers, Modules).

name(Text, Num) ->
    list_to_binary(Text ++ "_" ++ integer_to_list(Num)).


create(Worker, SessId, FilePath, Mode) ->
    exec(Worker,
        fun(Host) ->
            Result =
                file_manager:create(SessId, FilePath, Mode),
            Host ! {self(), Result}
        end).


open(Worker, SessId, FileKey, OpenMode) ->
    exec(Worker,
        fun(Host) ->
            Result =
                case file_manager:open(SessId, FileKey, OpenMode) of
                    {ok, Handle} ->
                        TestHandle = crypto:rand_bytes(10),
                        ets:insert(lfm_handles, {TestHandle, Handle}),
                        {ok, TestHandle};
                    Other -> Other
                end,
            Host ! {self(), Result}
        end).

read(Worker, TestHandle, Offset, Size) ->
    exec(Worker,
        fun(Host) ->
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            Result =
                case file_manager:read(Handle, Offset, Size) of
                    {ok, NewHandle, Res}  ->
                        ets:insert(lfm_handles, {TestHandle, NewHandle}),
                        {ok, Res};
                    Other -> Other
                end,
            Host ! {self(), Result}
        end).

write(Worker, TestHandle, Offset, Bytes) ->
    exec(Worker,
        fun(Host) ->
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            Result =
                case file_manager:write(Handle, Offset, Bytes) of
                    {ok, NewHandle, Res}  ->
                        ets:insert(lfm_handles, {TestHandle, NewHandle}),
                        {ok, Res};
                    Other -> Other
                end,
            Host ! {self(), Result}
        end).


exec(Worker, Fun) ->
    Host = self(),
    Pid = spawn_link(Worker, fun() -> Fun(Host) end),
    receive
        {Pid, Result} -> Result
    after timer:seconds(5) ->
        {error, test_timeout}
    end.

for(From, To, Fun) ->
    for(From, To, 1, Fun).
for(From, To, Step, Fun) ->
    [Fun(I) || I <- lists:seq(From, To, Step)].
