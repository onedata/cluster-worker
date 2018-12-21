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

-include("global_definitions.hrl").
-include("graph_sync/graph_sync.hrl").
-include("graph_sync_mocks.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("performance_test_utils.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

-define(SSL_OPTS(__Config), [{secure, only_verify_peercert}, {cacerts, get_cacerts(__Config)}]).

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
    auto_scope_test/1,
    session_persistence_test/1,
    subscribers_persistence_test/1,
    subscriptions_persistence_test/1,
    gs_server_session_clearing_test_api_level/1,
    gs_server_session_clearing_test_connection_level/1
]).

-define(TEST_CASES, [
    handshake_test,
    rpc_req_test,
    graph_req_test,
    subscribe_test,
    unsubscribe_test,
    nosub_test,
    auto_scope_test,
    session_persistence_test,
    subscribers_persistence_test,
    subscriptions_persistence_test,
    gs_server_session_clearing_test_api_level,
    gs_server_session_clearing_test_connection_level
]).

%%%===================================================================
%%% API functions
%%%===================================================================

all() ->
    ?ALL(?TEST_CASES).


handshake_test(Config) ->
    [handshake_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

handshake_test_base(Config, ProtoVersion) ->
    % Try to connect with no cookie - should be treated as anonymous
    {ok, Client1, _} = ?assertMatch(
        {ok, _, #gs_resp_handshake{identity = nobody}},
        gs_client:start_link(get_gs_ws_url(Config),
            undefined,
            [ProtoVersion],
            fun(_) -> ok end,
            ?SSL_OPTS(Config)
        )
    ),

    % Try to connect with user 1 session cookie
    {ok, Client2, _} = ?assertMatch(
        {ok, _, #gs_resp_handshake{identity = {user, ?USER_1}}},
        gs_client:start_link(get_gs_ws_url(Config),
            {cookie, {?SESSION_COOKIE_NAME, ?USER_1_COOKIE}},
            [ProtoVersion],
            fun(_) -> ok end,
            ?SSL_OPTS(Config)
        )
    ),

    % Try to connect with user 2 session cookie
    {ok, Client3, _} = ?assertMatch(
        {ok, _, #gs_resp_handshake{identity = {user, ?USER_2}}},
        gs_client:start_link(get_gs_ws_url(Config),
            {cookie, {?SESSION_COOKIE_NAME, ?USER_2_COOKIE}},
            [ProtoVersion],
            fun(_) -> ok end,
            ?SSL_OPTS(Config)
        )
    ),

    % Try to connect with bad cookie
    ?assertMatch(
        ?ERROR_UNAUTHORIZED,
        gs_client:start_link(get_gs_ws_url(Config),
            {cookie, {?SESSION_COOKIE_NAME, <<"bkkwksdf">>}},
            [ProtoVersion],
            fun(_) -> ok end,
            ?SSL_OPTS(Config)
        )
    ),

    % Try to connect with provider macaroon
    {ok, Client4, _} = ?assertMatch(
        {ok, _, #gs_resp_handshake{identity = {provider, ?PROVIDER_1}}},
        gs_client:start_link(get_gs_ws_url(Config),
            {macaroon, ?PROVIDER_1_MACAROON},
            [ProtoVersion],
            fun(_) -> ok end,
            ?SSL_OPTS(Config)
        )
    ),

    % Try to connect with bad macaroon
    ?assertMatch(
        ?ERROR_UNAUTHORIZED,
        gs_client:start_link(get_gs_ws_url(Config),
            {macaroon, <<"badMacaroon">>},
            [ProtoVersion],
            fun(_) -> ok end,
            ?SSL_OPTS(Config)
        )
    ),

    % Try to connect with bad protocol version
    SuppVersions = gs_protocol:supported_versions(),
    ?assertMatch(
        ?ERROR_BAD_VERSION(SuppVersions),
        gs_client:start_link(get_gs_ws_url(Config),
            {cookie, {?SESSION_COOKIE_NAME, ?USER_2_COOKIE}},
            [lists:max(SuppVersions) + 1],
            fun(_) -> ok end,
            ?SSL_OPTS(Config)
        )
    ),

    disconnect_client([Client1, Client2, Client3, Client4]),

    ok.


rpc_req_test(Config) ->
    [rpc_req_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

rpc_req_test_base(Config, ProtoVersion) ->
    {ok, Client1, #gs_resp_handshake{identity = {user, ?USER_1}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {cookie, {?SESSION_COOKIE_NAME, ?USER_1_COOKIE}},
        [ProtoVersion],
        fun(_) -> ok end,
        ?SSL_OPTS(Config)
    ),
    {ok, Client2, #gs_resp_handshake{identity = {user, ?USER_2}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {cookie, {?SESSION_COOKIE_NAME, ?USER_2_COOKIE}},
        [ProtoVersion],
        fun(_) -> ok end,
        ?SSL_OPTS(Config)
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

    disconnect_client([Client1, Client2]),

    ok.


graph_req_test(Config) ->
    [graph_req_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

graph_req_test_base(Config, ProtoVersion) ->
    User1Data = (?USER_DATA_WITHOUT_GRI(?USER_1))#{
        <<"gri">> => gs_protocol:gri_to_string(#gri{type = od_user, id = ?USER_1, aspect = instance})
    },

    {ok, Client1, #gs_resp_handshake{identity = {user, ?USER_1}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {cookie, {?SESSION_COOKIE_NAME, ?USER_1_COOKIE}},
        [ProtoVersion],
        fun(_) -> ok end,
        ?SSL_OPTS(Config)
    ),
    {ok, Client2, #gs_resp_handshake{identity = {user, ?USER_2}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {cookie, {?SESSION_COOKIE_NAME, ?USER_2_COOKIE}},
        [ProtoVersion],
        fun(_) -> ok end,
        ?SSL_OPTS(Config)
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = User1Data}},
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
        {ok, #gs_resp_graph{data_format = resource, data = User1Data}},
        gs_client:graph_request(Client2, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, get, #{}, false, ?THROUGH_SPACE(?SPACE_1))
    ),

    % User should be able to get it's own data using "self" as id
    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = User1Data}},
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
        {ok, #gs_resp_graph{data_format = resource, data = #{
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
        {ok, #gs_resp_graph{data_format = resource, data = #{
            <<"gri">> := NewGroupGRI,
            <<"name">> := ?GROUP_1_NAME
        }}},
        gs_client:graph_request(Client1, #gri{
            type = od_group, id = undefined, aspect = instance
        }, create, #{<<"name">> => ?GROUP_1_NAME}, false, ?AS_USER(?SELF))
    ),

    % Test creating a value rather than resource
    Value = 1293462394,
    ?assertMatch(
        {ok, #gs_resp_graph{data_format = value, data = Value}},
        gs_client:graph_request(Client1, #gri{
            type = od_group, id = ?GROUP_1, aspect = int_value
        }, create, #{<<"value">> => integer_to_binary(Value)})
    ),

    disconnect_client([Client1, Client2]),

    ok.


subscribe_test(Config) ->
    [subscribe_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

subscribe_test_base(Config, ProtoVersion) ->
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
        {cookie, {?SESSION_COOKIE_NAME, ?USER_1_COOKIE}},
        [ProtoVersion],
        fun(Push) -> GathererPid ! {gather_message, client1, Push} end,
        ?SSL_OPTS(Config)
    ),

    {ok, Client2, #gs_resp_handshake{identity = {user, ?USER_2}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {cookie, {?SESSION_COOKIE_NAME, ?USER_2_COOKIE}},
        [ProtoVersion],
        fun(Push) -> GathererPid ! {gather_message, client2, Push} end,
        ?SSL_OPTS(Config)
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = User1Data}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, get, #{}, true)
    ),

    User1NameSubstring = binary:part(maps:get(<<"name">>, User1Data), 0, 4),

    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = #{<<"nameSubstring">> := User1NameSubstring}}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = {name_substring, <<"4">>}
        }, get, #{}, true)
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = User2Data}},
        gs_client:graph_request(Client2, #gri{
            type = od_user, id = ?USER_2, aspect = instance
        }, get, #{}, true)
    ),

    User2NameSubstring = binary:part(maps:get(<<"name">>, User2Data), 0, 6),

    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = #{<<"nameSubstring">> := User2NameSubstring}}},
        gs_client:graph_request(Client2, #gri{
            type = od_user, id = ?USER_2, aspect = {name_substring, <<"6">>}
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

    NewUser1NameSubstring = binary:part(maps:get(<<"name">>, NewUser1Data), 0, 4),
    NewUser2NameSubstring = binary:part(maps:get(<<"name">>, NewUser2Data), 0, 6),

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
                    type = od_user, id = ?USER_1, aspect = {name_substring, <<"4">>}
                }, change_type = updated, data = #{<<"nameSubstring">> := NewUser1NameSubstring}} ->
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

    ?assertEqual(
        true,
        verify_message_present(GathererPid, client2, fun(Msg) ->
            case Msg of
                #gs_push_graph{gri = #gri{
                    type = od_user, id = ?USER_2, aspect = {name_substring, <<"6">>}
                }, change_type = updated, data = #{<<"nameSubstring">> := NewUser2NameSubstring}} ->
                    true;
                _ ->
                    false
            end
        end),
        50
    ),

    disconnect_client([Client1, Client2]),

    ok.


unsubscribe_test(Config) ->
    [unsubscribe_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

unsubscribe_test_base(Config, ProtoVersion) ->
    GathererPid = spawn(fun() ->
        gatherer_loop(#{})
    end),

    User1Data = (?USER_DATA_WITHOUT_GRI(?USER_1))#{
        <<"gri">> => gs_protocol:gri_to_string(#gri{type = od_user, id = ?USER_1, aspect = instance})
    },

    {ok, Client1, #gs_resp_handshake{identity = {user, ?USER_1}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {cookie, {?SESSION_COOKIE_NAME, ?USER_1_COOKIE}},
        [ProtoVersion],
        fun(Push) -> GathererPid ! {gather_message, client1, Push} end,
        ?SSL_OPTS(Config)
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = User1Data}},
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

    disconnect_client([Client1]),

    ok.


nosub_test(Config) ->
    [nosub_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

nosub_test_base(Config, ProtoVersion) ->
    GathererPid = spawn(fun() ->
        gatherer_loop(#{})
    end),

    User2Data = (?USER_DATA_WITHOUT_GRI(?USER_2))#{
        <<"gri">> => gs_protocol:gri_to_string(#gri{type = od_user, id = ?USER_2, aspect = instance})
    },

    {ok, Client1, #gs_resp_handshake{identity = {user, ?USER_1}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {cookie, {?SESSION_COOKIE_NAME, ?USER_1_COOKIE}},
        [ProtoVersion],
        fun(Push) -> GathererPid ! {gather_message, client1, Push} end,
        ?SSL_OPTS(Config)
    ),

    {ok, Client2, #gs_resp_handshake{identity = {user, ?USER_2}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {cookie, {?SESSION_COOKIE_NAME, ?USER_2_COOKIE}},
        [ProtoVersion],
        fun(Push) -> GathererPid ! {gather_message, client2, Push} end,
        ?SSL_OPTS(Config)
    ),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_2, aspect = instance
        }, get, #{}, true)
    ),

    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = User2Data}},
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
                _ ->
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

    disconnect_client([Client1, Client2]),

    ok.


auto_scope_test(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),

    GathererPid = spawn(fun() ->
        gatherer_loop(#{})
    end),

    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_1, none),
    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_2, public),

    {ok, Client1, #gs_resp_handshake{identity = {user, ?USER_1}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {cookie, {?SESSION_COOKIE_NAME, ?USER_1_COOKIE}},
        ?SUPPORTED_PROTO_VERSIONS,
        fun(Push) -> GathererPid ! {gather_message, client1, Push} end,
        ?SSL_OPTS(Config)
    ),
    {ok, Client2, #gs_resp_handshake{identity = {user, ?USER_2}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {cookie, {?SESSION_COOKIE_NAME, ?USER_2_COOKIE}},
        ?SUPPORTED_PROTO_VERSIONS,
        fun(Push) -> GathererPid ! {gather_message, client2, Push} end,
        ?SSL_OPTS(Config)
    ),

    HsGRI = #gri{type = od_handle_service, id = ?HANDLE_SERVICE, aspect = instance},
    HsGRIAuto = HsGRI#gri{scope = auto},
    HsGRIAutoStr = gs_protocol:gri_to_string(HsGRIAuto),

    ?assertEqual(
        ?ERROR_FORBIDDEN,
        gs_client:graph_request(Client1, HsGRI#gri{scope = auto}, get, #{}, true)
    ),

    ?assertEqual(
        ?ERROR_FORBIDDEN,
        gs_client:graph_request(Client2, HsGRI#gri{scope = shared}, get, #{}, true)
    ),

    ?assertEqual(
        {ok, #gs_resp_graph{data_format = resource, data = #{
            <<"gri">> => HsGRIAutoStr, <<"public">> => <<"pub1">>}
        }},
        gs_client:graph_request(Client2, HsGRI#gri{scope = auto}, get, #{}, true)
    ),

    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_1, shared),

    ?assertEqual(
        {ok, #gs_resp_graph{data_format = resource, data = #{
            <<"gri">> => HsGRIAutoStr, <<"public">> => <<"pub1">>, <<"shared">> => <<"sha1">>}
        }},
        gs_client:graph_request(Client1, HsGRI#gri{scope = auto}, get, #{}, true)
    ),

    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_2, private),

    HServiceData2 = ?HANDLE_SERVICE_DATA(<<"pub2">>, <<"sha2">>, <<"pro2">>, <<"pri2">>),
    rpc:call(Node, gs_server, updated, [od_handle_service, ?HANDLE_SERVICE, HServiceData2]),

    ?assertEqual(
        true,
        verify_message_present(GathererPid, client1, fun(Msg) ->
            Expected = ?LIMIT_HANDLE_SERVICE_DATA(shared, HServiceData2)#{<<"gri">> => HsGRIAutoStr},
            case Msg of
                #gs_push_graph{gri = HsGRIAuto, change_type = updated, data = Expected} ->
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
            Expected = ?LIMIT_HANDLE_SERVICE_DATA(private, HServiceData2)#{<<"gri">> => HsGRIAutoStr},
            case Msg of
                #gs_push_graph{gri = HsGRIAuto, change_type = updated, data = Expected} ->
                    true;
                _ ->
                    false
            end
        end),
        50
    ),

    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_1, protected),
    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_2, none),

    HServiceData3 = ?HANDLE_SERVICE_DATA(<<"pub3">>, <<"sha3">>, <<"pro3">>, <<"pri3">>),
    rpc:call(Node, gs_server, updated, [od_handle_service, ?HANDLE_SERVICE, HServiceData3]),

    ?assertEqual(
        true,
        verify_message_present(GathererPid, client1, fun(Msg) ->
            Expected = ?LIMIT_HANDLE_SERVICE_DATA(protected, HServiceData3)#{<<"gri">> => HsGRIAutoStr},
            case Msg of
                #gs_push_graph{gri = HsGRIAuto, change_type = updated, data = Expected} ->
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
                #gs_push_nosub{gri = HsGRIAuto, reason = forbidden} ->
                    true;
                _ ->
                    false
            end
        end),
        50
    ),

    % Check if create with auto scope works as expected
    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_2, protected),
    ?assertEqual(
        {ok, #gs_resp_graph{data_format = resource, data = #{
            <<"gri">> => HsGRIAutoStr, <<"public">> => <<"pub1">>,
            <<"shared">> => <<"sha1">>, <<"protected">> => <<"pro1">>}
        }},
        gs_client:graph_request(Client2, HsGRI#gri{scope = auto}, create, #{}, true)
    ),

    HServiceData4 = ?HANDLE_SERVICE_DATA(<<"pub4">>, <<"sha4">>, <<"pro4">>, <<"pri4">>),
    rpc:call(Node, gs_server, updated, [od_handle_service, ?HANDLE_SERVICE, HServiceData4]),

    ?assertEqual(
        true,
        verify_message_present(GathererPid, client2, fun(Msg) ->
            Expected = ?LIMIT_HANDLE_SERVICE_DATA(protected, HServiceData4)#{<<"gri">> => HsGRIAutoStr},
            case Msg of
                #gs_push_graph{gri = HsGRIAuto, change_type = updated, data = Expected} ->
                    true;
                _ ->
                    false
            end
        end),
        50
    ),

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

    UserId = <<"dummyId">>,
    GRI = #gri{type = od_user, id = UserId, aspect = instance, scope = private},

    {ok, Subscribers1} = rpc:call(Node, gs_persistence, get_subscribers, [od_user, UserId]),
    ?assertEqual(#{}, Subscribers1),

    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscriber, [GRI, Session1, Auth1, AuthHint1])),
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscriber, [GRI, Session2, Auth2, AuthHint2])),
    {ok, Subscribers2} = rpc:call(Node, gs_persistence, get_subscribers, [od_user, UserId]),
    Expected2 = ordsets:from_list([Sub1, Sub2]),
    ?assertMatch(#{{instance, private} := Expected2}, Subscribers2),

    % Subscribing should be idempotent
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscriber, [GRI, Session2, Auth2, AuthHint2])),
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscriber, [GRI, Session2, Auth2, AuthHint2])),
    {ok, Subscribers3} = rpc:call(Node, gs_persistence, get_subscribers, [od_user, UserId]),
    Expected3 = ordsets:from_list([Sub1, Sub2]),
    ?assertMatch(#{{instance, private} := Expected3}, Subscribers3),

    % Add third subscriber
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscriber, [GRI, Session3, Auth3, AuthHint3])),
    {ok, Subscribers4} = rpc:call(Node, gs_persistence, get_subscribers, [od_user, UserId]),
    Expected4 = ordsets:from_list([Sub1, Sub2, Sub3]),
    ?assertMatch(#{{instance, private} := Expected4}, Subscribers4),

    % Remove second subscriber
    ?assertMatch(ok, rpc:call(Node, gs_persistence, remove_subscriber, [GRI, Session2])),
    {ok, Subscribers5} = rpc:call(Node, gs_persistence, get_subscribers, [od_user, UserId]),
    Expected5 = ordsets:from_list([Sub1, Sub3]),
    ?assertMatch(#{{instance, private} := Expected5}, Subscribers5),

    % Remove all subscribers
    ?assertMatch(ok, rpc:call(Node, gs_persistence, remove_all_subscribers, [GRI])),
    {ok, Subscribers6} = rpc:call(Node, gs_persistence, get_subscribers, [od_user, UserId]),
    ?assertEqual(#{}, Subscribers6),
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

    {ok, Subscriptions1} = rpc:call(Node, gs_persistence, get_subscriptions, [SessionId]),
    ?assertMatch([], Subscriptions1),

    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscription, [SessionId, GRI1])),
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscription, [SessionId, GRI3])),
    {ok, Subscriptions2} = rpc:call(Node, gs_persistence, get_subscriptions, [SessionId]),
    Expected2 = lists:sort([GRI1, GRI3]),
    ?assertMatch(Expected2, lists:sort(Subscriptions2)),

    % Subscribing should be idempotent
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscription, [SessionId, GRI1])),
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscription, [SessionId, GRI1])),
    {ok, Subscriptions3} = rpc:call(Node, gs_persistence, get_subscriptions, [SessionId]),
    Expected3 = lists:sort([GRI1, GRI3]),
    ?assertMatch(Expected3, lists:sort(Subscriptions3)),

    % Add second GRI
    ?assertMatch(ok, rpc:call(Node, gs_persistence, add_subscription, [SessionId, GRI2])),
    {ok, Subscriptions4} = rpc:call(Node, gs_persistence, get_subscriptions, [SessionId]),
    Expected4 = lists:sort([GRI1, GRI2, GRI3]),
    ?assertMatch(Expected4, lists:sort(Subscriptions4)),

    % Remove first GRI
    ?assertMatch(ok, rpc:call(Node, gs_persistence, remove_subscription, [SessionId, GRI1])),
    {ok, Subscriptions5} = rpc:call(Node, gs_persistence, get_subscriptions, [SessionId]),
    Expected5 = lists:sort([GRI2, GRI3]),
    ?assertMatch(Expected5, lists:sort(Subscriptions5)),

    % Remove all GRIs
    ?assertMatch(ok, rpc:call(Node, gs_persistence, remove_all_subscriptions, [SessionId])),
    {ok, Subscriptions6} = rpc:call(Node, gs_persistence, get_subscriptions, [SessionId]),
    ?assertMatch([], Subscriptions6),
    ok.


gs_server_session_clearing_test_api_level(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    Auth = ?USER_AUTH(?USER_1),
    ConnectionInfo = ?CONNECTION_INFO(Auth),
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
        rpc:call(Node, gs_server, handshake, [Auth, ConnectionInfo, ConnRef, Translator, HandshakeReq])
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
    ?assertEqual({ok, #{}}, rpc:call(Node, gs_persistence, get_subscribers, [od_user, ?USER_1])),
    ?assertEqual({ok, #{}}, rpc:call(Node, gs_persistence, get_subscribers, [od_user, ?USER_2])),
    ok.


gs_server_session_clearing_test_connection_level(Config) ->
    [gs_server_session_clearing_test_connection_level_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

gs_server_session_clearing_test_connection_level_base(Config, ProtoVersion) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    {ok, Client1, #gs_resp_handshake{session_id = SessionId}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {cookie, {?SESSION_COOKIE_NAME, ?USER_1_COOKIE}},
        [ProtoVersion],
        fun(_) -> ok end,
        ?SSL_OPTS(Config)
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

    disconnect_client(Client1),

    ?assertMatch({ok, []}, rpc:call(Node, gs_persistence, get_subscriptions, [SessionId])),
    ?assertEqual({ok, #{}}, rpc:call(Node, gs_persistence, get_subscribers, [od_user, ?USER_1])),
    ?assertEqual({ok, #{}}, rpc:call(Node, gs_persistence, get_subscribers, [od_user, ?USER_2])),

    disconnect_client([Client1]),

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


disconnect_client([]) ->
    % Allow some time for cleanup
    timer:sleep(5000),
    process_flag(trap_exit, false);
disconnect_client([Client | Rest]) ->
    process_flag(trap_exit, true),
    exit(Client, kill),
    disconnect_client(Rest);
disconnect_client(Client) ->
    disconnect_client([Client]).


get_cacerts(Config) ->
    cert_utils:load_ders(?TEST_FILE(Config, "web_cacert.pem")).


start_gs_listener(Config, Node) ->
    ok = rpc:call(Node, application, ensure_started, [cowboy]),
    {ok, _} = rpc:call(Node, cowboy, start_tls, [
        ?GS_LISTENER_ID,
        [
            {port, ?GS_PORT},
            {num_acceptors, ?GS_HTTPS_ACCEPTORS},
            {keyfile, ?TEST_FILE(Config, "web_key.pem")},
            {certfile, ?TEST_FILE(Config, "web_cert.pem")},
            {cacerts, get_cacerts(Config)},
            {verify, verify_peer},
            {ciphers, ssl_utils:safe_ciphers()}
        ],
        #{
            env => #{dispatch => cowboy_router:compile([
                {'_', [
                    {"/[...]", gs_ws_handler, [?GS_EXAMPLE_TRANSLATOR]}
                ]}
            ])}
        }
    ]).

stop_gs_listener(Node) ->
    rpc:call(Node, cowboy, stop_listener, [?GS_LISTENER_ID]).


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
