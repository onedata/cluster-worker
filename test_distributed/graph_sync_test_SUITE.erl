%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module tests the Graph Sync server behaviour and Graph Sync channel by
%%% testing interaction between Graph Sync client and server.
%%% @end
%%%-------------------------------------------------------------------
-module(graph_sync_test_SUITE).
-author("Lukasz Opiola").

-include("api_errors.hrl").
-include("global_definitions.hrl").
-include("graph_sync/graph_sync.hrl").
-include("graph_sync_mocks.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([all/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).

-export([
    handshake_test/1,
    rpc_req_test/1,
    graph_req_test/1,
    subscribe_test/1,
    unsubscribe_test/1,
    nosub_test/1,
    session_persistence_test/1,
    subscribers_persistence_test/1,
    subscriptions_persistence_test/1,
    gs_server_session_clearing_test_api_level/1,
    gs_server_session_clearing_test_connection_level/1
]).

%%%===================================================================
%%% API functions
%%%===================================================================

all() -> ?ALL([
    handshake_test,
    rpc_req_test,
    graph_req_test,
    subscribe_test,
    unsubscribe_test,
    nosub_test,
    session_persistence_test,
    subscribers_persistence_test,
    subscriptions_persistence_test,
    gs_server_session_clearing_test_api_level,
    gs_server_session_clearing_test_connection_level
]).


handshake_test(Config) ->
    % Try to connect with no cookie - should be treated as anonymous
    ?assertMatch(
        {ok, _, #gs_resp_handshake{identity = nobody}},
        gs_client:start_link(get_gs_ws_url(Config),
            undefined,
            ?SUPPORTED_PROTO_VERSIONS,
            fun(_) -> ok end,
            [{cacerts, get_cacerts(Config)}]
        )
    ),

    % Try to connect with user 1 session cookie
    ?assertMatch(
        {ok, _, #gs_resp_handshake{identity = {user, ?USER_1}}},
        gs_client:start_link(get_gs_ws_url(Config),
            {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_1_COOKIE},
            ?SUPPORTED_PROTO_VERSIONS,
            fun(_) -> ok end,
            [{cacerts, get_cacerts(Config)}]
        )
    ),

    % Try to connect with user 2 session cookie
    ?assertMatch(
        {ok, _, #gs_resp_handshake{identity = {user, ?USER_2}}},
        gs_client:start_link(get_gs_ws_url(Config),
            {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_2_COOKIE},
            ?SUPPORTED_PROTO_VERSIONS,
            fun(_) -> ok end,
            [{cacerts, get_cacerts(Config)}]
        )
    ),

    % Try to connect with bad cookie
    ?assertMatch(
        ?ERROR_UNAUTHORIZED,
        gs_client:start_link(get_gs_ws_url(Config),
            {?GRAPH_SYNC_SESSION_COOKIE_NAME, <<"bkkwksdf">>},
            ?SUPPORTED_PROTO_VERSIONS,
            fun(_) -> ok end,
            [{cacerts, get_cacerts(Config)}]
        )
    ),

    % Try to connect with client certificates
    Opts = [
        {keyfile, ?TEST_FILE(Config, "client_key.pem")},
        {certfile, ?TEST_FILE(Config, "client_cert.pem")},
        {cacerts, get_cacerts(Config)}
    ],
    ?assertMatch(
        {ok, _, #gs_resp_handshake{identity = {provider, ?PROVIDER_1}}},
        gs_client:start_link(get_gs_ws_url(Config),
            undefined,
            ?SUPPORTED_PROTO_VERSIONS,
            fun(_) -> ok end,
            Opts
        )
    ),

    % Try to connect with bad protocol version
    SuppVersions = gs_protocol:supported_versions(),
    ?assertMatch(
        ?ERROR_BAD_VERSION(SuppVersions),
        gs_client:start_link(get_gs_ws_url(Config),
            {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_2_COOKIE},
            [lists:max(SuppVersions) + 1],
            fun(_) -> ok end,
            [{cacerts, get_cacerts(Config)}]
        )
    ),
    ok.


rpc_req_test(Config) ->
    {ok, Client1, #gs_resp_handshake{identity = {user, ?USER_1}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_1_COOKIE},
        ?SUPPORTED_PROTO_VERSIONS,
        fun(_) -> ok end,
        [{cacerts, get_cacerts(Config)}]
    ),
    {ok, Client2, #gs_resp_handshake{identity = {user, ?USER_2}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_2_COOKIE},
        ?SUPPORTED_PROTO_VERSIONS,
        fun(_) -> ok end,
        [{cacerts, get_cacerts(Config)}]
    ),
    ?assertMatch(
        {ok, #gs_resp_rpc{result = #{<<"a">> := <<"b">>}}},
        gs_client:rpc_request(Client1, <<"user1Fun">>, #{<<"a">> => <<"b">>})
    ),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        gs_client:rpc_request(Client1, <<"user2Fun">>, #{<<"a">> => <<"b">>})
    ),
    ?assertMatch(
        {ok, #gs_resp_rpc{result = #{<<"a">> := <<"b">>}}},
        gs_client:rpc_request(Client2, <<"user2Fun">>, #{<<"a">> => <<"b">>})
    ),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        gs_client:rpc_request(Client2, <<"user1Fun">>, #{<<"a">> => <<"b">>})
    ),
    ?assertMatch(
        ?ERROR_RPC_UNDEFINED,
        gs_client:rpc_request(Client1, <<"nonExistentFun">>, #{<<"a">> => <<"b">>})
    ),
    ok.


graph_req_test(Config) ->
    User1Data = (?USER_DATA_WITHOUT_GRI(?USER_1))#{
        <<"gri">> => gs_protocol:gri_to_string(#gri{type = od_user, id = ?USER_1, aspect = instance})
    },

    {ok, Client1, #gs_resp_handshake{identity = {user, ?USER_1}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_1_COOKIE},
        ?SUPPORTED_PROTO_VERSIONS,
        fun(_) -> ok end,
        [{cacerts, get_cacerts(Config)}]
    ),
    {ok, Client2, #gs_resp_handshake{identity = {user, ?USER_2}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_2_COOKIE},
        ?SUPPORTED_PROTO_VERSIONS,
        fun(_) -> ok end,
        [{cacerts, get_cacerts(Config)}]
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{result = User1Data}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, get)
    ),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        gs_client:graph_request(Client2, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, get)
    ),

    % User 2 should be able to get user 1 data through space ?SPACE_1
    ?assertMatch(
        {ok, #gs_resp_graph{result = User1Data}},
        gs_client:graph_request(Client2, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, get, #{}, false, ?THROUGH_SPACE(?SPACE_1))
    ),

    % User should be able to get it's own data using "self" as id
    ?assertMatch(
        {ok, #gs_resp_graph{result = User1Data}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?SELF, aspect = instance
        }, get)
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, update, #{
            <<"name">> => <<"newName">>
        })
    ),

    ?assertMatch(
        ?ERROR_BAD_VALUE_BINARY(<<"name">>),
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, update, #{
            <<"name">> => 1234
        })
    ),

    ?assertMatch(
        ?ERROR_MISSING_REQUIRED_VALUE(<<"name">>),
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, update, #{})
    ),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        gs_client:graph_request(Client2, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, update)
    ),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        gs_client:graph_request(Client2, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, delete)
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, delete)
    ),

    NewSpaceGRI = gs_protocol:gri_to_string(
        #gri{type = od_space, id = ?SPACE_1, aspect = instance}
    ),
    ?assertMatch(
        {ok, #gs_resp_graph{result = #{
            <<"gri">> := NewSpaceGRI,
            <<"name">> := ?SPACE_1_NAME
        }}},
        gs_client:graph_request(Client1, #gri{
            type = od_space, id = undefined, aspect = instance
        }, create, #{<<"name">> => ?SPACE_1_NAME}, false, ?AS_USER(?USER_1))
    ),

    % Make sure "self" works in auth hints
    NewGroupGRI = gs_protocol:gri_to_string(
        #gri{type = od_group, id = ?GROUP_1, aspect = instance}
    ),
    ?assertMatch(
        {ok, #gs_resp_graph{result = #{
            <<"gri">> := NewGroupGRI,
            <<"name">> := ?GROUP_1_NAME
        }}},
        gs_client:graph_request(Client1, #gri{
            type = od_group, id = undefined, aspect = instance
        }, create, #{<<"name">> => ?GROUP_1_NAME}, false, ?AS_USER(?SELF))
    ),

    ok.


subscribe_test(Config) ->
    GathererPid = spawn(fun() ->
        gatherer_loop(#{})
    end),

    User1Data = (?USER_DATA_WITHOUT_GRI(?USER_1))#{
        <<"gri">> => gs_protocol:gri_to_string(#gri{type = od_user, id = ?USER_1, aspect = instance})
    },
    User2Data = (?USER_DATA_WITHOUT_GRI(?USER_2))#{
        <<"gri">> => gs_protocol:gri_to_string(#gri{type = od_user, id = ?USER_2, aspect = instance})
    },

    {ok, Client1, #gs_resp_handshake{identity = {user, ?USER_1}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_1_COOKIE},
        ?SUPPORTED_PROTO_VERSIONS,
        fun(Push) -> GathererPid ! {gather_message, client1, Push} end,
        [{cacerts, get_cacerts(Config)}]
    ),

    {ok, Client2, #gs_resp_handshake{identity = {user, ?USER_2}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_2_COOKIE},
        ?SUPPORTED_PROTO_VERSIONS,
        fun(Push) -> GathererPid ! {gather_message, client2, Push} end,
        [{cacerts, get_cacerts(Config)}]
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{result = User1Data}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, get, #{}, true)
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{result = User2Data}},
        gs_client:graph_request(Client2, #gri{
            type = od_user, id = ?USER_2, aspect = instance
        }, get, #{}, true)
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, update, #{<<"name">> => <<"newName1">>})
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{}},
        gs_client:graph_request(Client2, #gri{
            type = od_user, id = ?USER_2, aspect = instance
        }, update, #{<<"name">> => <<"newName2">>})
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, delete)
    ),

    NewUser1Data = User1Data#{
        <<"name">> => <<"newName1">>
    },
    NewUser2Data = User2Data#{
        <<"name">> => <<"newName2">>
    },

    ?assertEqual(
        true,
        verify_message_present(GathererPid, client1, fun(Msg) ->
            case Msg of
                #gs_push_graph{gri = #gri{
                    type = od_user, id = ?USER_1, aspect = instance
                }, change_type = updated, data = NewUser1Data} ->
                    true;
                _ ->
                    false
            end
        end),
        50
    ),

    ?assertEqual(
        true,
        verify_message_present(GathererPid, client1, fun(Msg) ->
            case Msg of
                #gs_push_graph{gri = #gri{
                    type = od_user, id = ?USER_1, aspect = instance
                }, change_type = deleted, data = undefined} ->
                    true;
                _ ->
                    false
            end
        end),
        50
    ),

    ?assertEqual(
        true,
        verify_message_present(GathererPid, client2, fun(Msg) ->
            case Msg of
                #gs_push_graph{gri = #gri{
                    type = od_user, id = ?USER_2, aspect = instance
                }, change_type = updated, data = NewUser2Data} ->
                    true;
                _ ->
                    false
            end
        end),
        50
    ),
    ok.


unsubscribe_test(Config) ->
    GathererPid = spawn(fun() ->
        gatherer_loop(#{})
    end),

    User1Data = (?USER_DATA_WITHOUT_GRI(?USER_1))#{
        <<"gri">> => gs_protocol:gri_to_string(#gri{type = od_user, id = ?USER_1, aspect = instance})
    },

    {ok, Client1, #gs_resp_handshake{identity = {user, ?USER_1}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_1_COOKIE},
        ?SUPPORTED_PROTO_VERSIONS,
        fun(Push) -> GathererPid ! {gather_message, client1, Push} end,
        [{cacerts, get_cacerts(Config)}]
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{result = User1Data}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, get, #{}, true)
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, update, #{<<"name">> => <<"newName1">>})
    ),

    NewUser1Data = User1Data#{
        <<"name">> => <<"newName1">>
    },

    ?assertEqual(
        true,
        verify_message_present(GathererPid, client1, fun(Msg) ->
            case Msg of
                #gs_push_graph{gri = #gri{
                    type = od_user, id = ?USER_1, aspect = instance
                }, change_type = updated, data = NewUser1Data} ->
                    true;
                _ ->
                    false
            end
        end),
        50
    ),

    ?assertMatch(
        {ok, #gs_resp_unsub{}},
        gs_client:unsub_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        })
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, update, #{<<"name">> => <<"newName2">>})
    ),

    NewestUser1Data = User1Data#{
        <<"name">> => <<"newName2">>
    },

    ?assert(verify_message_absent(GathererPid, client1, fun(Msg) ->
        case Msg of
            #gs_push_graph{gri = #gri{
                type = od_user, id = ?USER_1, aspect = instance
            }, change_type = updated, data = NewestUser1Data} ->
                true;
            _ ->
                false
        end
    end, 20)),
    ok.


nosub_test(Config) ->
    GathererPid = spawn(fun() ->
        gatherer_loop(#{})
    end),

    User2Data = (?USER_DATA_WITHOUT_GRI(?USER_2))#{
        <<"gri">> => gs_protocol:gri_to_string(#gri{type = od_user, id = ?USER_2, aspect = instance})
    },

    {ok, Client1, #gs_resp_handshake{identity = {user, ?USER_1}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_1_COOKIE},
        ?SUPPORTED_PROTO_VERSIONS,
        fun(Push) -> GathererPid ! {gather_message, client1, Push} end,
        [{cacerts, get_cacerts(Config)}]
    ),

    {ok, Client2, #gs_resp_handshake{identity = {user, ?USER_2}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_2_COOKIE},
        ?SUPPORTED_PROTO_VERSIONS,
        fun(Push) -> GathererPid ! {gather_message, client2, Push} end,
        [{cacerts, get_cacerts(Config)}]
    ),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_2, aspect = instance
        }, get, #{}, true)
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{result = User2Data}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_2, aspect = instance
        }, get, #{}, true, ?THROUGH_SPACE(?SPACE_1))
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{}},
        gs_client:graph_request(Client2, #gri{
            type = od_user, id = ?USER_2, aspect = instance
        }, update, #{<<"name">> => <<"newName1">>})
    ),

    NewUser2Data = User2Data#{
        <<"name">> => <<"newName1">>
    },

    ?assertEqual(
        true,
        verify_message_present(GathererPid, client1, fun(Msg) ->
            case Msg of
                #gs_push_graph{gri = #gri{
                    type = od_user, id = ?USER_2, aspect = instance
                }, change_type = updated, data = NewUser2Data} ->
                    true;
                _ ->
                    false
            end
        end),
        50
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{}},
        gs_client:graph_request(Client2, #gri{
            type = od_user, id = ?USER_2, aspect = instance
        }, update, #{<<"name">> => ?USER_NAME_THAT_CAUSES_NO_ACCESS_THROUGH_SPACE})
    ),

    NewestUser2Data = User2Data#{
        <<"name">> => ?USER_NAME_THAT_CAUSES_NO_ACCESS_THROUGH_SPACE
    },

    ?assertEqual(
        true,
        verify_message_present(GathererPid, client1, fun(Msg) ->
            case Msg of
                #gs_push_nosub{gri = #gri{
                    type = od_user, id = ?USER_2, aspect = instance
                }, reason = forbidden} ->
                    true;
                O ->
                    ct:print("O ~p", [O]),
                    false
            end
        end),
        50
    ),

    ?assert(verify_message_absent(GathererPid, client1, fun(Msg) ->
        case Msg of
            #gs_push_graph{gri = #gri{
                type = od_user, id = ?USER_2, aspect = instance
            }, change_type = updated, data = NewestUser2Data} ->
                true;
            _ ->
                false
        end
    end, 20)),
    ok.


session_persistence_test(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    {ok, SessionId} = ?assertMatch(
        {ok, _},
        rpc:call(Node, gs_persistence, create_session, [#gs_session{
            client = dummyAuth,
            conn_ref = dummyConnRef,
            protocol_version = 7,
            translator = dummyTranslator
        }])
    ),

    ?assertMatch(
        {ok, #gs_session{
            id = SessionId,
            client = dummyAuth,
            conn_ref = dummyConnRef,
            protocol_version = 7,
            translator = dummyTranslator
        }},
        rpc:call(Node, gs_persistence, get_session, [SessionId])
    ),

    ?assertMatch(ok, rpc:call(Node, gs_persistence, delete_session, [SessionId])),

    ?assertNotMatch({ok, _}, rpc:call(Node, gs_persistence, get_session, [SessionId])),
    ok.


subscribers_persistence_test(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    DummyGsSession = #gs_session{
        client = dummyAuth,
        conn_ref = dummyConnRef,
        protocol_version = 7,
        translator = dummyTranslator
    },
    {ok, Session1} = rpc:call(Node, gs_persistence, create_session, [DummyGsSession]),
    {ok, Session2} = rpc:call(Node, gs_persistence, create_session, [DummyGsSession]),
    {ok, Session3} = rpc:call(Node, gs_persistence, create_session, [DummyGsSession]),
    Auth1 = dummyAuth1,
    Auth2 = dummyAuth2,
    Auth3 = dummyAuth3,
    AuthHint1 = dummyAuthHint1,
    AuthHint2 = dummyAuthHint2,
    AuthHint3 = dummyAuthHint3,
    Sub1 = {Session1, {Auth1, AuthHint1}},
    Sub2 = {Session2, {Auth2, AuthHint2}},
    Sub3 = {Session3, {Auth3, AuthHint3}},


    GRI = #gri{type = od_user, id = <<"dummyId">>, aspect = instance, scope = private},

    {ok, Subscribers1} = rpc:call(Node, gs_persistence, get_subscribers, [GRI]),
    ?assertMatch([], Subscribers1),

    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscriber, [GRI, Session1, Auth1, AuthHint1])),
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscriber, [GRI, Session2, Auth2, AuthHint2])),
    {ok, Subscribers2} = rpc:call(Node, gs_persistence, get_subscribers, [GRI]),
    Expected2 = lists:sort([Sub1, Sub2]),
    ?assertMatch(Expected2, lists:sort(Subscribers2)),

    % Subscribing should be idempotent
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscriber, [GRI, Session2, Auth2, AuthHint2])),
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscriber, [GRI, Session2, Auth2, AuthHint2])),
    {ok, Subscribers3} = rpc:call(Node, gs_persistence, get_subscribers, [GRI]),
    Expected3 = lists:sort([Sub1, Sub2]),
    ?assertMatch(Expected3, lists:sort(Subscribers3)),

    % Add third subscriber
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscriber, [GRI, Session3, Auth3, AuthHint3])),
    {ok, Subscribers4} = rpc:call(Node, gs_persistence, get_subscribers, [GRI]),
    Expected4 = lists:sort([Sub1, Sub2, Sub3]),
    ?assertMatch(Expected4, lists:sort(Subscribers4)),

    % Remove second subscriber
    ?assertMatch(ok, rpc:call(Node, gs_persistence, remove_subscriber, [GRI, Session2])),
    {ok, Subscribers5} = rpc:call(Node, gs_persistence, get_subscribers, [GRI]),
    Expected5 = lists:sort([Sub1, Sub3]),
    ?assertMatch(Expected5, lists:sort(Subscribers5)),

    % Remove all subscribers
    ?assertMatch(ok, rpc:call(Node, gs_persistence, remove_all_subscribers, [GRI])),
    {ok, Subscribers6} = rpc:call(Node, gs_persistence, get_subscribers, [GRI]),
    ?assertMatch([], Subscribers6),
    ok.


subscriptions_persistence_test(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    {ok, SessionId} = rpc:call(Node, gs_persistence, create_session, [#gs_session{
        client = dummyAuth,
        conn_ref = dummyConnRef,
        protocol_version = 7,
        translator = dummyTranslator
    }]),

    % Scopes differentiate resources, so below three GRIs are not the same
    GRI1 = #gri{type = od_user, id = <<"dummyId">>, aspect = instance, scope = private},
    GRI2 = #gri{type = od_user, id = <<"dummyId">>, aspect = instance, scope = protected},
    GRI3 = #gri{type = od_user, id = <<"dummyId">>, aspect = instance, scope = shared},
    Sub1 = gs_persistence:gri_to_hash(GRI1),
    Sub2 = gs_persistence:gri_to_hash(GRI2),
    Sub3 = gs_persistence:gri_to_hash(GRI3),

    {ok, Subscriptions1} = rpc:call(Node, gs_persistence, get_subscriptions, [SessionId]),
    ?assertMatch([], Subscriptions1),

    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscription, [SessionId, GRI1])),
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscription, [SessionId, GRI3])),
    {ok, Subscriptions2} = rpc:call(Node, gs_persistence, get_subscriptions, [SessionId]),
    Expected2 = lists:sort([Sub1, Sub3]),
    ?assertMatch(Expected2, lists:sort(Subscriptions2)),

    % Subscribing should be idempotent
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscription, [SessionId, GRI1])),
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscription, [SessionId, GRI1])),
    {ok, Subscriptions3} = rpc:call(Node, gs_persistence, get_subscriptions, [SessionId]),
    Expected3 = lists:sort([Sub1, Sub3]),
    ?assertMatch(Expected3, lists:sort(Subscriptions3)),

    % Add second GRI
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscription, [SessionId, GRI2])),
    {ok, Subscriptions4} = rpc:call(Node, gs_persistence, get_subscriptions, [SessionId]),
    Expected4 = lists:sort([Sub1, Sub2, Sub3]),
    ?assertMatch(Expected4, lists:sort(Subscriptions4)),

    % Remove first GRI
    ?assertMatch(ok, rpc:call(Node, gs_persistence, remove_subscription, [SessionId, GRI1])),
    {ok, Subscriptions5} = rpc:call(Node, gs_persistence, get_subscriptions, [SessionId]),
    Expected5 = lists:sort([Sub2, Sub3]),
    ?assertMatch(Expected5, lists:sort(Subscriptions5)),

    % Remove all GRIs
    ?assertMatch(ok, rpc:call(Node, gs_persistence, remove_all_subscriptions, [SessionId])),
    {ok, Subscriptions6} = rpc:call(Node, gs_persistence, get_subscriptions, [SessionId]),
    ?assertMatch([], Subscriptions6),
    ok.


gs_server_session_clearing_test_api_level(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    Auth = ?USER_AUTH(?USER_1),
    ConnRef = self(),
    Translator = ?GS_EXAMPLE_TRANSLATOR,
    HandshakeReq = #gs_req{request = #gs_req_handshake{
        supported_versions = gs_protocol:supported_versions()
    }},
    {ok, #gs_resp{response = #gs_resp_handshake{
        version = _Version,
        session_id = SessionId,
        identity = {user, ?USER_1}
    }}} = ?assertMatch(
        {ok, _},
        rpc:call(Node, gs_server, handshake, [Auth, ConnRef, Translator, HandshakeReq])
    ),

    GRI1 = #gri{type = od_user, id = ?USER_1, aspect = instance},
    % Make sure there are no leftovers from previous tests
    ?assertMatch(ok, rpc:call(Node, gs_persistence, remove_all_subscribers, [GRI1])),
    ?assertMatch(
        {ok, _},
        rpc:call(Node, gs_server, handle_request, [SessionId, #gs_req{request = #gs_req_graph{
            gri = GRI1,
            operation = get,
            subscribe = true
        }}])
    ),

    GRI2 = #gri{type = od_user, id = ?USER_2, aspect = instance},
    % Make sure there are no leftovers from previous tests
    ?assertMatch(ok, rpc:call(Node, gs_persistence, remove_all_subscribers, [GRI2])),
    ?assertMatch(
        {ok, _},
        rpc:call(Node, gs_server, handle_request, [SessionId, #gs_req{request = #gs_req_graph{
            gri = GRI2,
            operation = get,
            auth_hint = ?THROUGH_SPACE(?SPACE_1),
            subscribe = true
        }}])
    ),

    % Make sure that client disconnect removes all subscriptions
    ?assertMatch(ok, rpc:call(Node, gs_server, cleanup_client_session, [SessionId])),

    ?assertMatch({ok, []}, rpc:call(Node, gs_persistence, get_subscriptions, [SessionId])),
    ?assertMatch({ok, []}, rpc:call(Node, gs_persistence, get_subscribers, [GRI1])),
    ?assertMatch({ok, []}, rpc:call(Node, gs_persistence, get_subscribers, [GRI2])),
    ok.


gs_server_session_clearing_test_connection_level(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    {ok, Client1, #gs_resp_handshake{session_id = SessionId}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_1_COOKIE},
        ?SUPPORTED_PROTO_VERSIONS,
        fun(_) -> ok end,
        [{cacerts, get_cacerts(Config)}]
    ),

    GRI1 = #gri{type = od_user, id = ?USER_1, aspect = instance},
    % Make sure there are no leftovers from previous tests
    ?assertMatch(ok, rpc:call(Node, gs_persistence, remove_all_subscribers, [GRI1])),
    ?assertMatch(
        {ok, _},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, get, #{}, true)
    ),

    GRI2 = #gri{type = od_user, id = ?USER_2, aspect = instance},
    % Make sure there are no leftovers from previous tests
    ?assertMatch(ok, rpc:call(Node, gs_persistence, remove_all_subscribers, [GRI2])),
    ?assertMatch(
        {ok, _},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_2, aspect = instance
        }, get, #{}, true, ?THROUGH_SPACE(?SPACE_1))
    ),

    process_flag(trap_exit, true),
    exit(Client1, kill),
    % Grant a little time for cleanup
    timer:sleep(1000),
    process_flag(trap_exit, false),

    ?assertMatch({ok, []}, rpc:call(Node, gs_persistence, get_subscriptions, [SessionId])),
    ?assertMatch({ok, []}, rpc:call(Node, gs_persistence, get_subscribers, [GRI1])),
    ?assertMatch({ok, []}, rpc:call(Node, gs_persistence, get_subscribers, [GRI2])),
    ok.

%%%===================================================================
%%% Helper functions related to asynchronous subscriptions messages
%%%===================================================================

gatherer_loop(MessagesMap) ->
    NewMap = receive
        {gather_message, ClientRef, Message} ->
            ClientMessages = maps:get(ClientRef, MessagesMap, []),
            maps:put(ClientRef, [Message | ClientMessages], MessagesMap);
        {get_messages, ClientRef, Pid} ->
            ClientMessages = maps:get(ClientRef, MessagesMap, []),
            Pid ! ClientMessages,
            MessagesMap
    end,
    gatherer_loop(NewMap).


verify_message_present(GathererPid, ClientRef, MessageMatcherFun) ->
    GathererPid ! {get_messages, ClientRef, self()},
    AllMessages = receive
        M when is_list(M) -> M
    end,
    lists:any(fun(Message) ->
        MessageMatcherFun(Message)
    end, AllMessages).


verify_message_absent(_, _, _, 0) ->
    true;
verify_message_absent(GathererPid, ClientRef, MessageMatcherFun, Retries) ->
    case verify_message_present(GathererPid, ClientRef, MessageMatcherFun) of
        true ->
            false;
        false ->
            timer:sleep(1000),
            verify_message_absent(GathererPid, ClientRef, MessageMatcherFun, Retries - 1)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================


get_gs_ws_url(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    NodeIP = test_utils:get_docker_ip(Node),
    str_utils:format_bin("wss://~s:~B/", [NodeIP, ?GS_PORT]).



get_cacerts(Config) ->
    {ok, WebCaCertPem} = file:read_file(?TEST_FILE(Config, "web_cacert.pem")),
    {ok, ServerCaCertPem} = file:read_file(?TEST_FILE(Config, "server_cacert.pem")),
    lists:map(fun cert_decoder:pem_to_der/1, [WebCaCertPem, ServerCaCertPem]).


start_gs_listener(Config, Node) ->
    ok = rpc:call(Node, application, ensure_started, [ssl]),
    {ok, _} = rpc:call(Node, ranch, start_listener, [
        ?GS_LISTENER_ID, ?GS_HTTPS_ACCEPTORS,
        ranch_ssl, [
            {port, ?GS_PORT},
            {keyfile, ?TEST_FILE(Config, "web_key.pem")},
            {certfile, ?TEST_FILE(Config, "web_cert.pem")},
            {cacerts, get_cacerts(Config)},
            {verify, verify_peer},
            {ciphers, ssl:cipher_suites() -- ssl_utils:weak_ciphers()}
        ], cowboy_protocol,
        [
            {env, [{dispatch, cowboy_router:compile([
                {'_', [
                    {"/[...]", gs_ws_handler, [?GS_EXAMPLE_TRANSLATOR]}
                ]}
            ])}]}
        ]
    ]).

stop_gs_listener(Node) ->
    rpc:call(Node, ranch, stop_listener, [?GS_LISTENER_ID]).


%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    ssl:start(),
    [{?LOAD_MODULES, [graph_sync_mocks]} | Config].


init_per_testcase(_, Config) ->
    Nodes = ?config(cluster_worker_nodes, Config),
    [start_gs_listener(Config, N) || N <- Nodes],
    graph_sync_mocks:mock_callbacks(Config),
    Config.


end_per_testcase(_, Config) ->
    Nodes = ?config(cluster_worker_nodes, Config),
    [stop_gs_listener(N) || N <- Nodes],
    graph_sync_mocks:unmock_callbacks(Config).


end_per_suite(_Config) ->
    ssl:stop(),
    ok.
