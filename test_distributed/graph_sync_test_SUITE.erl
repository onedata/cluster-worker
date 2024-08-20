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
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("gui/include/gui.hrl").

%% API
-export([all/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).

-export([
    handshake_test/1,
    rpc_req_test/1,
    async_req_test/1,
    graph_req_test/1,
    subscribe_test/1,
    unsubscribe_test/1,
    nosub_test/1,
    auth_override_test/1,
    nobody_auth_override_test/1,
    auto_scope_test/1,
    bad_entity_type_test/1,
    session_persistence_test/1,
    subscriptions_persistence_test/1,
    gs_server_session_clearing_test_api_level/1,
    gs_server_session_clearing_test_connection_level/1,
    service_availability_rpc_test/1,
    service_availability_graph_test/1,
    service_availability_handshake_test/1
]).

-define(TEST_CASES, [
    handshake_test,
    rpc_req_test,
    async_req_test,
    graph_req_test,
    subscribe_test,
    unsubscribe_test,
    nosub_test,
    auth_override_test,
    nobody_auth_override_test,
    auto_scope_test,
    bad_entity_type_test,
    session_persistence_test,
    subscriptions_persistence_test,
    gs_server_session_clearing_test_api_level,
    gs_server_session_clearing_test_connection_level,
    service_availability_rpc_test,
    service_availability_graph_test,
    service_availability_handshake_test
]).

-define(KEY_FILE, ?TEST_RELEASE_ETC_DIR("certs/web_key.pem")).
-define(CERT_FILE, ?TEST_RELEASE_ETC_DIR("certs/web_cert.pem")).
-define(CHAIN_FILE, ?TEST_RELEASE_ETC_DIR("certs/web_chain.pem")).
-define(TRUSTED_CACERTS_FILE, ?TEST_RELEASE_ETC_DIR("cacerts/OneDataTestWebServerCa.pem")).

-define(SSL_OPTS(Config), [{secure, only_verify_peercert}, {cacerts, get_trusted_cacerts(Config)}]).
-define(DUMMY_IP, {13, 190, 241, 56}).

-define(wait_until_true(Term), ?assertEqual(true, Term, 50)).

%%%===================================================================
%%% API functions
%%%===================================================================

all() ->
    ?ALL(?TEST_CASES).


handshake_test(Config) ->
    [handshake_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

handshake_test_base(Config, ProtoVersion) ->
    % Try to connect with no cookie - should be treated as anonymous
    Client1 = spawn_client(Config, ProtoVersion, undefined, ?SUB(nobody)),

    % Try to connect with user 1 token
    Client2 = spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1)),

    % Try to connect with user 2 token
    Client3 = spawn_client(Config, ProtoVersion, {token, ?USER_2_TOKEN}, ?SUB(user, ?USER_2)),

    % Try to connect with user 1 token requiring session cookies...
    Client4 = spawn_client(
        Config, ProtoVersion, {with_http_cookies, {token, ?USER_1_TOKEN_REQUIRING_COOKIES}, ?DUMMY_COOKIES},
        ?SUB(user, ?USER_1)
    ),
    % ... it should not succeed if there are no cookies provided
    spawn_client(
        Config, ProtoVersion, {with_http_cookies, {token, ?USER_1_TOKEN_REQUIRING_COOKIES}, []},
        ?ERROR_UNAUTHORIZED
    ),
    spawn_client(
        Config, ProtoVersion, {token, ?USER_1_TOKEN_REQUIRING_COOKIES},
        ?ERROR_UNAUTHORIZED
    ),

    % Try to connect with bad token
    spawn_client(Config, ProtoVersion, {token, <<"bkkwksdf">>}, ?ERROR_UNAUTHORIZED),

    % Try to connect with provider token
    Client5 = spawn_client(Config, ProtoVersion, {token, ?PROVIDER_1_TOKEN}, ?SUB(?ONEPROVIDER, ?PROVIDER_1)),

    % Try to connect with bad protocol version
    SuppVersions = gs_protocol:supported_versions(),
    spawn_client(Config, [lists:max(SuppVersions) + 1], undefined, ?ERROR_BAD_VERSION(SuppVersions)),

    disconnect_client([Client1, Client2, Client3, Client4, Client5]),

    ok.


rpc_req_test(Config) ->
    [rpc_req_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

rpc_req_test_base(Config, ProtoVersion) ->
    Client1 = spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1)),
    Client2 = spawn_client(Config, ProtoVersion, {token, ?USER_2_TOKEN}, ?SUB(user, ?USER_2)),

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


async_req_test(Config) ->
    [async_req_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

async_req_test_base(Config, ProtoVersion) ->
    Client1 = spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1)),

    Id = gs_client:async_request(Client1, #gs_req{
        subtype = rpc,
        request = #gs_req_rpc{
            function = <<"veryLongOperation">>,
            args = #{<<"someDummy">> => <<"arguments127">>}
        }
    }),

    AsyncResponse = receive
        {response, Id, Resp} ->
            Resp
    after
        timer:seconds(60) ->
            {error, timeout}
    end,

    ?assertEqual(
        {ok, #gs_resp_rpc{result = #{<<"someDummy">> => <<"arguments127">>}}},
        AsyncResponse
    ),

    disconnect_client([Client1]),

    ok.


graph_req_test(Config) ->
    [graph_req_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

graph_req_test_base(Config, ProtoVersion) ->
    User1Data = (?USER_DATA_WITHOUT_GRI(?USER_1))#{
        <<"gri">> => gri:serialize(#gri{type = od_user, id = ?USER_1, aspect = instance}),
        <<"revision">> => 1
    },

    Client1 = spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1)),
    Client2 = spawn_client(Config, ProtoVersion, {token, ?USER_2_TOKEN}, ?SUB(user, ?USER_2)),

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

    NewSpaceGRI = gri:serialize(
        #gri{type = od_space, id = ?SPACE_1, aspect = instance}
    ),
    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = #{
            <<"gri">> := NewSpaceGRI,
            <<"name">> := ?SPACE_1_NAME,
            <<"revision">> := 1
        }}},
        gs_client:graph_request(Client1, #gri{
            type = od_space, id = undefined, aspect = instance
        }, create, #{<<"name">> => ?SPACE_1_NAME}, false, ?AS_USER(?USER_1))
    ),

    % Make sure "self" works in auth hints
    NewGroupGRI = gri:serialize(
        #gri{type = od_group, id = ?GROUP_1, aspect = instance}
    ),
    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = #{
            <<"gri">> := NewGroupGRI,
            <<"name">> := ?GROUP_1_NAME,
            <<"revision">> := 1
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
        <<"gri">> => gri:serialize(#gri{type = od_user, id = ?USER_1, aspect = instance}),
        <<"revision">> => 1
    },
    User2Data = (?USER_DATA_WITHOUT_GRI(?USER_2))#{
        <<"gri">> => gri:serialize(#gri{type = od_user, id = ?USER_2, aspect = instance}),
        <<"revision">> => 1
    },

    Client1 = spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1), fun(Push) ->
        GathererPid ! {gather_message, client1, Push}
    end),

    Client2 = spawn_client(Config, ProtoVersion, {token, ?USER_2_TOKEN}, ?SUB(user, ?USER_2), fun(Push) ->
        GathererPid ! {gather_message, client2, Push}
    end),

    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = User1Data}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, get, #{}, true)
    ),

    User1NameSubstring = binary:part(maps:get(<<"name">>, User1Data), 0, 4),

    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = #{
            <<"nameSubstring">> := User1NameSubstring,
            <<"revision">> := 1
        }}},
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
        {ok, #gs_resp_graph{data_format = resource, data = #{
            <<"nameSubstring">> := User2NameSubstring,
            <<"revision">> := 1
        }}},
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
        <<"name">> => <<"newName1">>,
        <<"revision">> => 2
    },
    NewUser2Data = User2Data#{
        <<"name">> => <<"newName2">>,
        <<"revision">> => 2
    },

    NewUser1NameSubstring = binary:part(maps:get(<<"name">>, NewUser1Data), 0, 4),
    NewUser2NameSubstring = binary:part(maps:get(<<"name">>, NewUser2Data), 0, 6),

    ?wait_until_true(verify_message_present(GathererPid, client1, fun(Msg) ->
        case Msg of
            #gs_push_graph{gri = #gri{
                type = od_user, id = ?USER_1, aspect = instance
            }, change_type = updated, data = NewUser1Data} ->
                true;
            _ ->
                false
        end
    end)),

    ?wait_until_true(verify_message_present(GathererPid, client1, fun(Msg) ->
        case Msg of
            #gs_push_graph{gri = #gri{
                type = od_user, id = ?USER_1, aspect = {name_substring, <<"4">>}
            }, change_type = updated, data = #{
                <<"nameSubstring">> := NewUser1NameSubstring,
                <<"revision">> := 2
            }} ->
                true;
            _ ->
                false
        end
    end)),

    ?wait_until_true(verify_message_present(GathererPid, client1, fun(Msg) ->
        case Msg of
            #gs_push_graph{gri = #gri{
                type = od_user, id = ?USER_1, aspect = instance
            }, change_type = deleted, data = undefined} ->
                true;
            _ ->
                false
        end
    end)),

    ?wait_until_true(verify_message_present(GathererPid, client2, fun(Msg) ->
        case Msg of
            #gs_push_graph{gri = #gri{
                type = od_user, id = ?USER_2, aspect = instance
            }, change_type = updated, data = NewUser2Data} ->
                true;
            _ ->
                false
        end
    end)),

    ?wait_until_true(verify_message_present(GathererPid, client2, fun(Msg) ->
        case Msg of
            #gs_push_graph{gri = #gri{
                type = od_user, id = ?USER_2, aspect = {name_substring, <<"6">>}
            }, change_type = updated, data = #{
                <<"nameSubstring">> := NewUser2NameSubstring,
                <<"revision">> := 2
            }} ->
                true;
            _ ->
                false
        end
    end)),

    disconnect_client([Client1, Client2]),

    ok.


unsubscribe_test(Config) ->
    [unsubscribe_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

unsubscribe_test_base(Config, ProtoVersion) ->
    GathererPid = spawn(fun() ->
        gatherer_loop(#{})
    end),

    User1Data = (?USER_DATA_WITHOUT_GRI(?USER_1))#{
        <<"gri">> => gri:serialize(#gri{type = od_user, id = ?USER_1, aspect = instance}),
        <<"revision">> => 1
    },

    Client1 = spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1), fun(Push) ->
        GathererPid ! {gather_message, client1, Push}
    end),

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
        <<"name">> => <<"newName1">>,
        <<"revision">> => 2
    },

    ?wait_until_true(verify_message_present(GathererPid, client1, fun(Msg) ->
        case Msg of
            #gs_push_graph{gri = #gri{
                type = od_user, id = ?USER_1, aspect = instance
            }, change_type = updated, data = NewUser1Data} ->
                true;
            _ ->
                false
        end
    end)),

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
        <<"name">> => <<"newName2">>,
        <<"revision">> => 2
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
        <<"gri">> => gri:serialize(#gri{type = od_user, id = ?USER_2, aspect = instance}),
        <<"revision">> => 1
    },

    Client1 = spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1), fun(Push) ->
        GathererPid ! {gather_message, client1, Push}
    end),

    Client2 = spawn_client(Config, ProtoVersion, {token, ?USER_2_TOKEN}, ?SUB(user, ?USER_2), fun(Push) ->
        GathererPid ! {gather_message, client2, Push}
    end),

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
        <<"name">> => <<"newName1">>,
        <<"revision">> => 2
    },

    ?wait_until_true(verify_message_present(GathererPid, client1, fun(Msg) ->
        case Msg of
            #gs_push_graph{gri = #gri{
                type = od_user, id = ?USER_2, aspect = instance
            }, change_type = updated, data = NewUser2Data} ->
                true;
            _ ->
                false
        end
    end)),

    ?assertMatch(
        {ok, #gs_resp_graph{}},
        gs_client:graph_request(Client2, #gri{
            type = od_user, id = ?USER_2, aspect = instance
        }, update, #{<<"name">> => ?USER_NAME_THAT_CAUSES_NO_ACCESS_THROUGH_SPACE})
    ),

    NewestUser2Data = User2Data#{
        <<"name">> => ?USER_NAME_THAT_CAUSES_NO_ACCESS_THROUGH_SPACE,
        <<"revision">> => 2
    },

    ?wait_until_true(verify_message_present(GathererPid, client1, fun(Msg) ->
        case Msg of
            #gs_push_nosub{gri = #gri{
                type = od_user, id = ?USER_2, aspect = instance
            }, reason = forbidden} ->
                true;
            _ ->
                false
        end
    end)),

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


auth_override_test(Config) ->
    [auth_override_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

auth_override_test_base(Config, ProtoVersion) ->
    GathererPid = spawn(fun() ->
        gatherer_loop(#{})
    end),

    UserClient = spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1), fun(Push) ->
        GathererPid ! {gather_message, client1, Push}
    end),

    ProviderClient = spawn_client(Config, ProtoVersion, {token, ?PROVIDER_1_TOKEN}, ?SUB(?ONEPROVIDER, ?PROVIDER_1), fun(Push) ->
        GathererPid ! {gather_message, provider_client, Push}
    end),

    User2Data = (?USER_DATA_WITHOUT_GRI(?USER_2))#{
        <<"gri">> => gri:serialize(#gri{type = od_user, id = ?USER_2, aspect = instance}),
        <<"revision">> => 1
    },

    GetUserReq = #gs_req{
        subtype = graph,
        auth_override = #auth_override{
            client_auth = {token, ?USER_2_TOKEN},
            peer_ip = ?WHITELISTED_IP,
            interface = ?WHITELISTED_INTERFACE,
            consumer_token = ?WHITELISTED_CONSUMER_TOKEN
        },
        request = #gs_req_graph{
            gri = #gri{type = od_user, id = ?SELF, aspect = instance},
            operation = get,
            subscribe = true
        }
    },

    % Provider should be able to get user's 2 data by possessing his token and
    % using it as auth override during the request.
    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = User2Data}},
        gs_client:sync_request(ProviderClient, GetUserReq)
    ),

    % Auth override is allowed only for providers
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        gs_client:sync_request(UserClient, GetUserReq)
    ),

    % Subscribing should work too - provider should be receiving future changes of
    % user's 2 record.
    NewUser2Name = <<"newName2">>,
    NewUser2Data = User2Data#{
        <<"name">> => NewUser2Name,
        <<"revision">> => 2
    },

    ?assertMatch(
        {ok, #gs_resp_graph{}},
        gs_client:sync_request(ProviderClient, GetUserReq#gs_req{
            request = #gs_req_graph{
                gri = #gri{type = od_user, id = ?SELF, aspect = instance},
                operation = update,
                data = #{<<"name">> => NewUser2Name}
            }
        })
    ),

    ?wait_until_true(verify_message_present(GathererPid, provider_client, fun(Msg) ->
        case Msg of
            #gs_push_graph{
                gri = #gri{type = od_user, id = ?USER_2, aspect = instance},
                change_type = updated,
                data = NewUser2Data
            } ->
                true;
            _ ->
                false
        end
    end)),

    % Check if auth override data that is not whitelisted causes an error.
    % For test purposes, there are defined blacklisted ip, interface and consumer token.
    ReqWithOverrideData = fun(PeerIp, Interface, ConsumerToken) ->
        GetUserReq#gs_req{
            auth_override = #auth_override{
                client_auth = {token, ?USER_2_TOKEN},
                peer_ip = PeerIp,
                interface = Interface,
                consumer_token = ConsumerToken
            }
        } end,

    % Additional auth override options are supported since version 4
    case ProtoVersion > 3 of
        false ->
            ok;
        true ->
            ?assertMatch(?ERROR_UNAUTHORIZED, gs_client:sync_request(ProviderClient, ReqWithOverrideData(
                ?BLACKLISTED_IP, ?WHITELISTED_INTERFACE, ?WHITELISTED_CONSUMER_TOKEN
            ))),
            ?assertMatch(?ERROR_UNAUTHORIZED, gs_client:sync_request(ProviderClient, ReqWithOverrideData(
                ?WHITELISTED_IP, ?BLACKLISTED_INTERFACE, ?WHITELISTED_CONSUMER_TOKEN
            ))),
            ?assertMatch(?ERROR_UNAUTHORIZED, gs_client:sync_request(ProviderClient, ReqWithOverrideData(
                ?WHITELISTED_IP, ?WHITELISTED_INTERFACE, ?BLACKLISTED_CONSUMER_TOKEN
            )))
    end.


nobody_auth_override_test(Config) ->
    [nobody_auth_override_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

nobody_auth_override_test_base(Config, ProtoVersion) ->
    Client1 = spawn_client(Config, ProtoVersion, {token, ?PROVIDER_1_TOKEN}, ?SUB(?ONEPROVIDER, ?PROVIDER_1)),

    % Request with auth based on connection owner
    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = ?SHARE_DATA_MATCHER(<<"private">>)}},
        gs_client:sync_request(Client1, #gs_req{
            subtype = graph,
            request = #gs_req_graph{
                gri = #gri{type = od_share, id = ?SHARE, aspect = instance, scope = auto},
                operation = get
            }
        })
    ),

    % Request with nobody auth override
    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = ?SHARE_DATA_MATCHER(<<"public">>)}},
        gs_client:sync_request(Client1, #gs_req{
            subtype = graph,
            auth_override = #auth_override{
                client_auth = nobody,
                peer_ip = ?WHITELISTED_IP,
                interface = ?WHITELISTED_INTERFACE,
                consumer_token = ?WHITELISTED_CONSUMER_TOKEN
            },
            request = #gs_req_graph{
                gri = #gri{type = od_share, id = ?SHARE, aspect = instance, scope = auto},
                operation = get
            }
        })
    ),

    disconnect_client([Client1]).


auto_scope_test(Config) ->
    [auto_scope_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

auto_scope_test_base(Config, ProtoVersion) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),

    GathererPid = spawn(fun() ->
        gatherer_loop(#{})
    end),

    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_1, none),
    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_2, public),

    Client1 = spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1), fun(Push) ->
        GathererPid ! {gather_message, client1, Push}
    end),

    Client2 = spawn_client(Config, ProtoVersion, {token, ?USER_2_TOKEN}, ?SUB(user, ?USER_2), fun(Push) ->
        GathererPid ! {gather_message, client2, Push}
    end),

    HsGRI = #gri{type = od_handle_service, id = ?HANDLE_SERVICE, aspect = instance},
    HsGRIAuto = HsGRI#gri{scope = auto},
    HsGRIAutoStr = gri:serialize(HsGRIAuto),

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
            <<"gri">> => HsGRIAutoStr, <<"public">> => <<"pub1">>,
            <<"revision">> => 1
        }}},
        gs_client:graph_request(Client2, HsGRI#gri{scope = auto}, get, #{}, true)
    ),

    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_1, shared),

    ?assertEqual(
        {ok, #gs_resp_graph{data_format = resource, data = #{
            <<"gri">> => HsGRIAutoStr, <<"public">> => <<"pub1">>, <<"shared">> => <<"sha1">>,
            <<"revision">> => 1
        }}},
        gs_client:graph_request(Client1, HsGRI#gri{scope = auto}, get, #{}, true)
    ),

    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_2, private),

    Revision2 = 5,
    HServiceData2 = ?HANDLE_SERVICE_DATA(<<"pub2">>, <<"sha2">>, <<"pro2">>, <<"pri2">>),
    rpc:call(Node, gs_server, updated, [od_handle_service, ?HANDLE_SERVICE, {HServiceData2, Revision2}]),

    ?wait_until_true(verify_message_present(GathererPid, client1, fun(Msg) ->
        Expected = ?LIMIT_HANDLE_SERVICE_DATA(shared, HServiceData2)#{
            <<"gri">> => HsGRIAutoStr,
            <<"revision">> => Revision2
        },
        case Msg of
            #gs_push_graph{gri = HsGRIAuto, change_type = updated, data = Expected} ->
                true;
            _ ->
                false
        end
    end)),

    ?wait_until_true(verify_message_present(GathererPid, client2, fun(Msg) ->
        Expected = ?LIMIT_HANDLE_SERVICE_DATA(private, HServiceData2)#{
            <<"gri">> => HsGRIAutoStr,
            <<"revision">> => Revision2
        },
        case Msg of
            #gs_push_graph{gri = HsGRIAuto, change_type = updated, data = Expected} ->
                true;
            _ ->
                false
        end
    end)),

    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_1, protected),
    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_2, none),

    Revision3 = 12,
    HServiceData3 = ?HANDLE_SERVICE_DATA(<<"pub3">>, <<"sha3">>, <<"pro3">>, <<"pri3">>),
    rpc:call(Node, gs_server, updated, [od_handle_service, ?HANDLE_SERVICE, {HServiceData3, Revision3}]),

    ?wait_until_true(verify_message_present(GathererPid, client1, fun(Msg) ->
        Expected = ?LIMIT_HANDLE_SERVICE_DATA(protected, HServiceData3)#{
            <<"gri">> => HsGRIAutoStr,
            <<"revision">> => Revision3
        },
        case Msg of
            #gs_push_graph{gri = HsGRIAuto, change_type = updated, data = Expected} ->
                true;
            _ ->
                false
        end
    end)),

    ?wait_until_true(verify_message_present(GathererPid, client2, fun(Msg) ->
        case Msg of
            #gs_push_nosub{gri = HsGRIAuto, reason = forbidden} ->
                true;
            _ ->
                false
        end
    end)),

    % Check if create with auto scope works as expected
    graph_sync_mocks:mock_max_scope_towards_handle_service(Config, ?USER_2, protected),
    ?assertEqual(
        {ok, #gs_resp_graph{data_format = resource, data = #{
            <<"gri">> => HsGRIAutoStr, <<"public">> => <<"pub1">>,
            <<"shared">> => <<"sha1">>, <<"protected">> => <<"pro1">>,
            <<"revision">> => 1
        }}},
        gs_client:graph_request(Client2, HsGRI#gri{scope = auto}, create, #{}, true)
    ),

    Revision4 = 14,
    HServiceData4 = ?HANDLE_SERVICE_DATA(<<"pub4">>, <<"sha4">>, <<"pro4">>, <<"pri4">>),
    rpc:call(Node, gs_server, updated, [od_handle_service, ?HANDLE_SERVICE, {HServiceData4, Revision4}]),

    ?wait_until_true(verify_message_present(GathererPid, client2, fun(Msg) ->
        Expected = ?LIMIT_HANDLE_SERVICE_DATA(protected, HServiceData4)#{
            <<"gri">> => HsGRIAutoStr,
            <<"revision">> => Revision4
        },
        case Msg of
            #gs_push_graph{gri = HsGRIAuto, change_type = updated, data = Expected} ->
                true;
            _ ->
                false
        end
    end)),

    disconnect_client([Client1, Client2]).


bad_entity_type_test(Config) ->
    [bad_entity_type_test_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

bad_entity_type_test_base(Config, ProtoVersion) ->
    Client1 = spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1)),

    ?assertMatch(
        ?ERROR_BAD_GRI,
        % op_file entity type is not supported (as per gs_logic_plugin)
        gs_client:graph_request(Client1, #gri{
            type = op_file, id = <<"123">>, aspect = instance
        }, get, #{}, false)
    ),

    disconnect_client([Client1]).


session_persistence_test(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    Session = ?assertMatch(
        #gs_session{
            auth = dummyAuth,
            conn_ref = dummyConnRef,
            translator = dummyTranslator
        },
        rpc:call(Node, gs_persistence, create_session, [dummyAuth, dummyConnRef, 7, dummyTranslator])
    ),

    SessionId = Session#gs_session.id,

    ?assertMatch({ok, Session}, rpc:call(Node, gs_persistence, get_session, [SessionId])),

    ?assertMatch(ok, rpc:call(Node, gs_persistence, delete_session, [SessionId])),

    ?assertMatch({error, not_found}, rpc:call(Node, gs_persistence, get_session, [SessionId])).


subscriptions_persistence_test(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    #gs_session{id = Sess1} = rpc:call(Node, gs_persistence, create_session, [auth, self(), 7, dummyTranslator]),
    #gs_session{id = Sess2} = rpc:call(Node, gs_persistence, create_session, [auth, self(), 7, dummyTranslator]),
    #gs_session{id = Sess3} = rpc:call(Node, gs_persistence, create_session, [auth, self(), 7, dummyTranslator]),
    Auth1 = dummyAuth1,
    Auth2 = dummyAuth2,
    Auth3 = dummyAuth3,
    AuthHint1 = dummyAuthHint1,
    AuthHint2 = dummyAuthHint2,
    AuthHint3 = dummyAuthHint3,
    Sub1 = {Sess1, {Auth1, AuthHint1}},
    Sub2 = {Sess2, {Auth2, AuthHint2}},
    Sub3 = {Sess3, {Auth3, AuthHint3}},

    UserId = <<"dummyId">>,
    GRIPriv = #gri{type = od_user, id = UserId, aspect = instance, scope = private},
    GRIProt = #gri{type = od_user, id = UserId, aspect = instance, scope = protected},
    GRIShrd = #gri{type = od_user, id = UserId, aspect = instance, scope = shared},

    ?assertEqual(#{}, rpc:call(Node, gs_persistence, get_entity_subscribers, [od_user, UserId])),
    ?assertEqual([], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess1])),
    ?assertEqual([], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess2])),
    ?assertEqual([], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess3])),

    ?assertEqual(ok, rpc:call(Node, gs_persistence, subscribe, [Sess1, GRIPriv, Auth1, AuthHint1])),
    ?assertEqual(ok, rpc:call(Node, gs_persistence, subscribe, [Sess2, GRIShrd, Auth2, AuthHint2])),
    ?assertEqual(#{
        {instance, private} => [Sub1],
        {instance, shared} => [Sub2]
    }, rpc:call(Node, gs_persistence, get_entity_subscribers, [od_user, UserId])),
    ?assertEqual([GRIPriv], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess1])),
    ?assertEqual([GRIShrd], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess2])),
    ?assertEqual([], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess3])),

    % subscribing should be idempotent
    ?assertEqual(ok, rpc:call(Node, gs_persistence, subscribe, [Sess1, GRIPriv, Auth1, AuthHint1])),
    ?assertEqual(ok, rpc:call(Node, gs_persistence, subscribe, [Sess2, GRIShrd, Auth2, AuthHint2])),
    ?assertEqual(#{
        {instance, private} => [Sub1],
        {instance, shared} => [Sub2]
    }, rpc:call(Node, gs_persistence, get_entity_subscribers, [od_user, UserId])),
    ?assertEqual([GRIPriv], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess1])),
    ?assertEqual([GRIShrd], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess2])),
    ?assertEqual([], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess3])),

    % subscribe for different scopes
    ?assertEqual(ok, rpc:call(Node, gs_persistence, subscribe, [Sess1, GRIProt, Auth1, AuthHint1])),
    ?assertEqual(ok, rpc:call(Node, gs_persistence, subscribe, [Sess2, GRIProt, Auth2, AuthHint2])),
    ?assertEqual(#{
        {instance, private} => [Sub1],
        {instance, protected} => ordsets:from_list([Sub1, Sub2]),
        {instance, shared} => [Sub2]
    }, rpc:call(Node, gs_persistence, get_entity_subscribers, [od_user, UserId])),
    ?assertEqual(ordsets:from_list([GRIPriv, GRIProt]), rpc:call(Node, gs_subscriber, get_subscriptions, [Sess1])),
    ?assertEqual(ordsets:from_list([GRIProt, GRIShrd]), rpc:call(Node, gs_subscriber, get_subscriptions, [Sess2])),
    ?assertEqual([], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess3])),

    ?assertEqual(ok, rpc:call(Node, gs_persistence, subscribe, [Sess3, GRIShrd, Auth3, AuthHint3])),
    ?assertEqual(#{
        {instance, private} => [Sub1],
        {instance, protected} => ordsets:from_list([Sub1, Sub2]),
        {instance, shared} => ordsets:from_list([Sub2, Sub3])
    }, rpc:call(Node, gs_persistence, get_entity_subscribers, [od_user, UserId])),
    ?assertEqual(ordsets:from_list([GRIPriv, GRIProt]), rpc:call(Node, gs_subscriber, get_subscriptions, [Sess1])),
    ?assertEqual(ordsets:from_list([GRIProt, GRIShrd]), rpc:call(Node, gs_subscriber, get_subscriptions, [Sess2])),
    ?assertEqual([GRIShrd], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess3])),

    ?assertEqual(ok, rpc:call(Node, gs_persistence, unsubscribe, [Sess2, GRIProt])),
    ?assertEqual(#{
        {instance, private} => [Sub1],
        {instance, protected} => [Sub1],
        {instance, shared} => ordsets:from_list([Sub2, Sub3])
    }, rpc:call(Node, gs_persistence, get_entity_subscribers, [od_user, UserId])),
    ?assertEqual(ordsets:from_list([GRIPriv, GRIProt]), rpc:call(Node, gs_subscriber, get_subscriptions, [Sess1])),
    ?assertEqual([GRIShrd], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess2])),
    ?assertEqual([GRIShrd], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess3])),

    ?assertEqual(ok, rpc:call(Node, gs_persistence, delete_session, [Sess1])),
    ?assertEqual({error, not_found}, rpc:call(Node, gs_persistence, get_session, [Sess1])),
    ?assertEqual(#{
        {instance, shared} => ordsets:from_list([Sub2, Sub3])
    }, rpc:call(Node, gs_persistence, get_entity_subscribers, [od_user, UserId])),
    ?assertEqual([], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess1])),
    ?assertEqual([GRIShrd], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess2])),
    ?assertEqual([GRIShrd], rpc:call(Node, gs_subscriber, get_subscriptions, [Sess3])),

    ?assertEqual(ok, rpc:call(Node, gs_persistence, delete_session, [Sess2])),
    ?assertEqual(ok, rpc:call(Node, gs_persistence, delete_session, [Sess3])).


gs_server_session_clearing_test_api_level(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    Auth = {token, ?USER_1_TOKEN},
    ConnRef = self(),
    Translator = ?GS_EXAMPLE_TRANSLATOR,
    HandshakeReq = #gs_req{request = #gs_req_handshake{
        auth = Auth,
        supported_versions = gs_protocol:supported_versions()
    }},
    {ok, SessionData, #gs_resp_handshake{
        identity = ?SUB(user, ?USER_1)
    }} = ?assertMatch(
        {ok, _, _},
        rpc:call(Node, gs_server, handshake, [ConnRef, Translator, HandshakeReq, ?DUMMY_IP, _Cookies = []])
    ),

    GRI1 = #gri{type = od_user, id = ?USER_1, aspect = instance},
    % Make sure there are no leftovers from previous tests
    ?assertMatch(ok, rpc:call(Node, gs_persistence, remove_all_subscribers, [GRI1])),
    ?assertMatch(
        {ok, _},
        rpc:call(Node, gs_server, handle_request, [SessionData, #gs_req{request = #gs_req_graph{
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
        rpc:call(Node, gs_server, handle_request, [SessionData, #gs_req{request = #gs_req_graph{
            gri = GRI2,
            operation = get,
            auth_hint = ?THROUGH_SPACE(?SPACE_1),
            subscribe = true
        }}])
    ),

    % Make sure that client disconnect removes all subscriptions
    ?assertEqual(ok, rpc:call(Node, gs_server, cleanup_session, [SessionData])),
    ?assertEqual([], rpc:call(Node, gs_subscriber, get_subscriptions, [SessionData#gs_session.id])),
    ?assertEqual(#{}, rpc:call(Node, gs_subscription, get_entity_subscribers, [od_user, ?USER_1])),
    ?assertEqual(#{}, rpc:call(Node, gs_subscription, get_entity_subscribers, [od_user, ?USER_2])).


gs_server_session_clearing_test_connection_level(Config) ->
    [gs_server_session_clearing_test_connection_level_base(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

gs_server_session_clearing_test_connection_level_base(Config, ProtoVersion) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),

    {ok, Client1, #gs_resp_handshake{session_id = SessionId}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {token, ?USER_1_TOKEN},
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
    ?assertMatch([], rpc:call(Node, gs_subscriber, get_subscriptions, [SessionId])),
    ?assertEqual(#{}, rpc:call(Node, gs_subscription, get_entity_subscribers, [od_user, ?USER_1])),
    ?assertEqual(#{}, rpc:call(Node, gs_subscription, get_entity_subscribers, [od_user, ?USER_2])).


service_availability_rpc_test(Config) ->
    [service_availability_rpc_test(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

service_availability_rpc_test(Config, ProtoVersion) ->
    Nodes = ?config(cluster_worker_nodes, Config),
    Self = self(),
    RpcArgs = #{<<"a">> => <<"b">>},

    Client1 = spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1), fun(Push) ->
        Self ! Push
    end),

    graph_sync_mocks:simulate_service_availability(true, Nodes),
    ?assertMatch(
        {ok, #gs_resp_rpc{result = #{<<"a">> := <<"b">>}}},
        gs_client:rpc_request(Client1, <<"user1Fun">>, RpcArgs)
    ),

    graph_sync_mocks:simulate_service_availability(false, Nodes),
    ?assertMatch(
        ?ERROR_SERVICE_UNAVAILABLE,
        gs_client:rpc_request(Client1, <<"user1Fun">>, RpcArgs)
    ),

    graph_sync_mocks:simulate_service_availability(true, Nodes),
    ?assertMatch(
        {ok, #gs_resp_rpc{result = #{<<"a">> := <<"b">>}}},
        gs_client:rpc_request(Client1, <<"user1Fun">>, RpcArgs)
    ).


service_availability_graph_test(Config) ->
    [service_availability_graph_test(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

service_availability_graph_test(Config, ProtoVersion) ->
    Nodes = ?config(cluster_worker_nodes, Config),
    User1Data = (?USER_DATA_WITHOUT_GRI(?USER_1))#{
        <<"gri">> => gri:serialize(#gri{type = od_user, id = ?USER_1, aspect = instance}),
        <<"revision">> => 1
    },
    Client1 = spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1)),

    graph_sync_mocks:simulate_service_availability(true, Nodes),
    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = User1Data}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, get)
    ),

    graph_sync_mocks:simulate_service_availability(false, Nodes),
    ?assertMatch(
        ?ERROR_SERVICE_UNAVAILABLE,
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, get)
    ),

    graph_sync_mocks:simulate_service_availability(true, Nodes),
    ?assertMatch(
        {ok, #gs_resp_graph{data_format = resource, data = User1Data}},
        gs_client:graph_request(Client1, #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, get)
    ).


service_availability_handshake_test(Config) ->
    [service_availability_handshake_test(Config, ProtoVersion) || ProtoVersion <- ?SUPPORTED_PROTO_VERSIONS].

service_availability_handshake_test(Config, ProtoVersion) ->
    Nodes = ?config(cluster_worker_nodes, Config),

    graph_sync_mocks:simulate_service_availability(true, Nodes),
    spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1)),

    graph_sync_mocks:simulate_service_availability(false, Nodes),

    spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?ERROR_SERVICE_UNAVAILABLE),

    graph_sync_mocks:simulate_service_availability(true, Nodes),
    spawn_client(Config, ProtoVersion, {token, ?USER_1_TOKEN}, ?SUB(user, ?USER_1)).


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

% ExpResult :: {error, term()} | aai:subject().
spawn_client(Config, ProtoVersion, Auth, ExpResult) ->
    spawn_client(Config, ProtoVersion, Auth, ExpResult, fun(_) -> ok end).

spawn_client(Config, ProtoVersion, Auth, ExpResult, PushCallback) when is_integer(ProtoVersion) ->
    spawn_client(Config, [ProtoVersion], Auth, ExpResult, PushCallback);
spawn_client(Config, ProtoVersions, Auth, ExpResult, PushCallback) ->
    Result = gs_client:start_link(
        get_gs_ws_url(Config),
        Auth,
        ProtoVersions,
        PushCallback,
        ?SSL_OPTS(Config)
    ),
    case ExpResult of
        {error, _} ->
            ?assertMatch(ExpResult, Result),
            connection_error;
        ExpIdentity ->
            ?assertMatch({ok, _, #gs_resp_handshake{identity = ExpIdentity}}, Result),
            {ok, Client, _} = Result,
            Client
    end.


get_gs_ws_url(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    NodeIP = test_utils:get_docker_ip(Node),
    str_utils:format_bin("wss://~ts:~B/", [NodeIP, ?GS_PORT]).


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


start_gs_listener(Node) ->
    ok = rpc:call(Node, application, ensure_started, [cowboy]),
    ?assertMatch(ok, rpc:call(Node, gui, start, [#gui_config{
        port = ?GS_PORT,
        key_file = ?KEY_FILE,
        cert_file = ?CERT_FILE,
        chain_file = ?CHAIN_FILE,
        number_of_acceptors = ?GS_HTTPS_ACCEPTORS,
        custom_cowboy_routes = [
            {"/[...]", gs_ws_handler, [?GS_EXAMPLE_TRANSLATOR]}
        ]
    }])).


stop_gs_listener(Node) ->
    rpc:call(Node, gui, stop, []).


get_trusted_cacerts(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Node, cert_utils, load_ders, [?TRUSTED_CACERTS_FILE]).

%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    ssl:start(),
    [{?LOAD_MODULES, [graph_sync_mocks]} | Config].


init_per_testcase(_, Config) ->
    Nodes = ?config(cluster_worker_nodes, Config),
    [start_gs_listener(N) || N <- Nodes],
    graph_sync_mocks:mock_callbacks(Config),
    Config.


end_per_testcase(_, Config) ->
    Nodes = ?config(cluster_worker_nodes, Config),
    [stop_gs_listener(N) || N <- Nodes],
    graph_sync_mocks:unmock_callbacks(Config).


end_per_suite(_Config) ->
    ssl:stop(),
    ok.
