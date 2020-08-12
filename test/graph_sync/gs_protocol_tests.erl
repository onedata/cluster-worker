%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains unit tests for gs_protocol module that mostly check if
%%% encoding / decoding messages works correctly.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_protocol_tests).
-author("Lukasz Opiola").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include("graph_sync/graph_sync.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/aai/aai.hrl").

%%%===================================================================
%%% Tests
%%%===================================================================

greatest_common_version_test() ->
    ?assertEqual(
        {true, 7},
        gs_protocol:greatest_common_version([1, 2, 5, 6, 7], [7])
    ),
    ?assertEqual(
        {true, 7},
        gs_protocol:greatest_common_version([7], [1, 2, 5, 6, 7])
    ),

    ?assertEqual(
        {true, 7},
        gs_protocol:greatest_common_version([1, 2, 5, 6, 7, 8, 9], [7])
    ),
    ?assertEqual(
        {true, 7},
        gs_protocol:greatest_common_version([7], [1, 2, 5, 6, 7, 10, 14])
    ),

    ?assertEqual(
        {true, 3},
        gs_protocol:greatest_common_version([1, 3, 5, 7], [2, 3, 4, 6, 8])
    ),
    ?assertEqual(
        {true, 3},
        gs_protocol:greatest_common_version([2, 3, 4, 6, 8], [1, 3, 5, 7])
    ),

    ?assertEqual(
        false,
        gs_protocol:greatest_common_version([], [2, 3, 4, 6, 8])
    ),
    ?assertEqual(
        false,
        gs_protocol:greatest_common_version([2, 3, 4, 6, 8], [])
    ),

    ?assertEqual(
        false,
        gs_protocol:greatest_common_version([1, 3, 5, 7], [2, 4, 6, 8])
    ),
    ?assertEqual(
        false,
        gs_protocol:greatest_common_version([2, 4, 6, 8], [1, 3, 5, 7])
    ),

    ok.


encode_decode_message_test() ->
    % Check if requests are encoded and decoded correctly
    RequestsToCheck = [
        #gs_req{
            id = <<"mess1">>,
            subtype = handshake,
            auth_override = undefined,
            request = #gs_req_handshake{
                supported_versions = [123123, 34534, 123, 5],
                session_id = undefined
            }
        },
        #gs_req{
            id = <<"mess2">>,
            subtype = handshake,
            auth_override = undefined,
            request = #gs_req_handshake{
                supported_versions = [1],
                session_id = <<"23423424qdsfdsgew456235tegdfg">>
            }
        },
        #gs_req{
            id = <<"mess3">>,
            subtype = handshake,
            auth_override = undefined,
            request = #gs_req_handshake{
                supported_versions = [],
                session_id = <<"23423424qdsfdsgew456235tegdfg">>
            }
        },
        #gs_req{
            id = <<"mess4">>,
            subtype = handshake,
            auth_override = undefined,
            request = #gs_req_handshake{
                supported_versions = [],
                auth = undefined,
                session_id = <<"23423424qdsfdsgew456235tegdfg">>
            }
        },
        #gs_req{
            id = <<"mess5">>,
            subtype = handshake,
            auth_override = undefined,
            request = #gs_req_handshake{
                supported_versions = [123123, 34534, 123, 5],
                auth = {token, <<"token-token">>},
                session_id = <<"zxvcert245234234234234">>
            }
        },
        #gs_req{
            id = <<"mess6">>,
            subtype = handshake,
            auth_override = undefined,
            request = #gs_req_handshake{
                supported_versions = [1],
                auth = {token, <<"another-token">>},
                session_id = <<"ccxvsdfsdfsdfsdf">>
            }
        },
        #gs_req{
            id = <<"mess7">>,
            subtype = rpc,
            auth_override = #auth_override{client_auth = {token, <<"third-token">>}},
            request = #gs_req_rpc{
                function = <<"f6">>,
                args = #{<<"args6">> => 6}
            }
        },
        #gs_req{
            id = <<"mess8">>,
            subtype = graph,
            auth_override = undefined,
            request = #gs_req_graph{
                gri = #gri{type = od_user, id = undefined, aspect = whatever},
                operation = get,
                data = undefined,
                subscribe = false,
                auth_hint = undefined
            }
        },
        #gs_req{
            id = <<"mess9">>,
            subtype = graph,
            auth_override = undefined,
            request = #gs_req_graph{
                gri = #gri{type = od_user, id = <<"id9">>, aspect = whatever, scope = shared},
                operation = create,
                data = #{<<"data9">> => 9},
                subscribe = true,
                auth_hint = ?AS_USER(<<"otherid9">>)
            }
        },
        #gs_req{
            id = <<"mess10">>,
            subtype = graph,
            auth_override = undefined,
            request = #gs_req_graph{
                gri = #gri{type = od_space, id = <<"id10">>, aspect = whatever},
                operation = update,
                data = #{<<"data10">> => 10},
                subscribe = false,
                auth_hint = ?AS_GROUP(<<"otherid10">>)
            }
        },
        #gs_req{
            id = <<"mess11">>,
            subtype = graph,
            auth_override = undefined,
            request = #gs_req_graph{
                gri = #gri{type = od_space, id = <<"id11">>, aspect = whatever},
                operation = delete,
                data = undefined,
                subscribe = false,
                auth_hint = undefined
            }
        },
        #gs_req{
            id = <<"mess12">>,
            subtype = graph,
            auth_override = undefined,
            request = #gs_req_graph{
                gri = #gri{type = od_user, id = <<"id12">>, aspect = whatever},
                operation = get,
                data = undefined,
                subscribe = true,
                auth_hint = ?THROUGH_USER(<<"otherid12">>)
            }
        },
        #gs_req{
            id = <<"mess13">>,
            subtype = graph,
            auth_override = undefined,
            request = #gs_req_graph{
                gri = #gri{type = od_user, id = <<"id13">>, aspect = whatever},
                operation = get,
                data = undefined,
                subscribe = true,
                auth_hint = ?THROUGH_GROUP(<<"otherid13">>)
            }
        },
        #gs_req{
            id = <<"mess14">>,
            subtype = graph,
            auth_override = undefined,
            request = #gs_req_graph{
                gri = #gri{type = od_user, id = <<"id14">>, aspect = whatever},
                operation = get,
                data = undefined,
                subscribe = true,
                auth_hint = ?THROUGH_SPACE(<<"otherid14">>)
            }
        },
        #gs_req{
            id = <<"mess15">>,
            subtype = graph,
            auth_override = undefined,
            request = #gs_req_graph{
                gri = #gri{type = od_user, id = <<"id15">>, aspect = whatever},
                operation = get,
                data = undefined,
                subscribe = true,
                auth_hint = ?THROUGH_PROVIDER(<<"otherid15">>)
            }
        },
        #gs_req{
            id = <<"mess17">>,
            subtype = graph,
            auth_override = undefined,
            request = #gs_req_graph{
                gri = #gri{type = od_user, id = <<"id17">>, aspect = whatever},
                operation = get,
                data = undefined,
                subscribe = true,
                auth_hint = ?THROUGH_HANDLE_SERVICE(<<"otherid17">>)
            }
        },
        #gs_req{
            id = <<"mess18">>,
            subtype = graph,
            auth_override = undefined,
            request = #gs_req_graph{
                gri = #gri{type = od_user, id = <<"id18">>, aspect = whatever},
                operation = get,
                data = undefined,
                subscribe = true,
                auth_hint = ?THROUGH_HANDLE(<<"otherid18">>)
            }
        },
        #gs_req{
            id = <<"mess19">>,
            subtype = unsub,
            auth_override = undefined,
            request = #gs_req_unsub{
                gri = #gri{type = od_user, id = <<"id19">>, aspect = whatever}
            }
        },
        #gs_resp{
            id = <<"mess20">>,
            subtype = handshake,
            success = false,
            error = ?ERROR_BAD_VERSION([1, 2, 3]),
            response = undefined
        },
        #gs_resp{
            id = <<"mess21">>,
            subtype = handshake,
            success = true,
            error = undefined,
            response = #gs_resp_handshake{
                version = 21,
                session_id = <<"adsfadsfadsf">>,
                identity = ?SUB(nobody),
                attributes = #{<<"attr21">> => 21}
            }
        },
        #gs_resp{
            id = <<"mess22">>,
            subtype = handshake,
            success = true,
            error = undefined,
            response = #gs_resp_handshake{
                version = 22,
                session_id = <<"adsfadsfadsf">>,
                identity = ?SUB(user, <<"u22">>),
                attributes = #{<<"attr22">> => 22}
            }
        },
        #gs_resp{
            id = <<"mess23">>,
            subtype = handshake,
            success = true,
            error = undefined,
            response = #gs_resp_handshake{
                version = 23,
                session_id = <<"adsfadsfadsf">>,
                identity = ?SUB(?ONEPROVIDER, <<"p23">>),
                attributes = #{<<"attr23">> => 23}
            }
        },
        #gs_resp{
            id = <<"mess23.3">>,
            subtype = rpc,
            success = false,
            error = ?ERROR_RPC_UNDEFINED,
            response = undefined
        },
        #gs_resp{
            id = <<"mess23.6">>,
            subtype = rpc,
            success = true,
            error = undefined,
            response = #gs_resp_rpc{
                result = undefined
            }
        },
        #gs_resp{
            id = <<"mess24">>,
            subtype = rpc,
            success = true,
            error = undefined,
            response = #gs_resp_rpc{
                result = #{<<"result24">> => 24}
            }
        },
        #gs_resp{
            id = <<"mess25">>,
            subtype = graph,
            success = false,
            error = ?ERROR_FORBIDDEN,
            response = undefined
        },
        #gs_resp{
            id = <<"mess26.1">>,
            subtype = graph,
            success = true,
            error = undefined,
            response = #gs_resp_graph{
                data = undefined
            }
        },
        #gs_resp{
            id = <<"mess26.2">>,
            subtype = graph,
            success = true,
            error = undefined,
            response = #gs_resp_graph{}
        },
        #gs_resp{
            id = <<"mess27.1">>,
            subtype = graph,
            success = true,
            error = undefined,
            response = #gs_resp_graph{
                data_format = value,
                data = #{<<"data27">> => 27}
            }
        },
        #gs_resp{
            id = <<"mess27.2">>,
            subtype = graph,
            success = true,
            error = undefined,
            response = #gs_resp_graph{
                data_format = value,
                data = 12345
            }
        },
        #gs_resp{
            id = <<"mess27.3">>,
            subtype = graph,
            success = true,
            error = undefined,
            response = #gs_resp_graph{
                data_format = value,
                data = <<"12345">>
            }
        },
        #gs_resp{
            id = <<"mess27.4">>,
            subtype = graph,
            success = true,
            error = undefined,
            response = #gs_resp_graph{
                data_format = resource,
                data = #{<<"data27">> => 27, <<"gri">> => <<"user.id.instance:protected">>}
            }
        },
        #gs_resp{
            id = <<"mess28">>,
            subtype = unsub,
            success = false,
            error = ?ERROR_NOT_SUBSCRIBABLE,
            response = undefined
        },
        #gs_resp{
            id = <<"mess28">>,
            subtype = unsub,
            success = true,
            error = undefined,
            response = #gs_resp_unsub{
            }
        },
        #gs_push{
            subtype = error,
            message = #gs_push_error{
                error = ?ERROR_BAD_MESSAGE(#{<<"mesaz">> => <<"mesaz">>})
            }
        },
        #gs_push{
            subtype = graph,
            message = #gs_push_graph{
                gri = #gri{type = od_user, id = <<"id30">>, aspect = whatever},
                change_type = updated,
                data = #{<<"data30">> => 30}
            }
        },
        #gs_push{
            subtype = graph,
            message = #gs_push_graph{
                gri = #gri{type = od_user, id = <<"id31">>, aspect = whatever},
                change_type = deleted,
                data = undefined
            }
        },
        #gs_push{
            subtype = nosub,
            message = #gs_push_nosub{
                gri = #gri{type = od_user, id = <<"id32">>, aspect = whatever},
                reason = forbidden
            }
        },
        #gs_push{
            subtype = nosub,
            message = #gs_push_nosub{
                gri = #gri{type = od_user, id = <<"id33">>, aspect = whatever},
                auth_hint = ?THROUGH_USER(<<"otherid33">>),
                reason = forbidden
            }
        }
    ],

    lists:foreach(fun(ProtoVersion) ->
        lists:foreach(fun(Request) ->
            check_encode_decode_for_proto_version(ProtoVersion, Request),
            ?assertMatch(?ERROR_BAD_MESSAGE(_), gs_protocol:decode(ProtoVersion, <<"sdfsdviuyasd9fas">>))
        end, RequestsToCheck)
    end, gs_protocol:supported_versions()).


% In protocol version 4, auth override has been extended with additional fields
% so it cannot be checked in the encode_decode_message_test.
encode_decode_auth_override_v4_test() ->
    RequestsToCheck = [
        #gs_req{
            id = <<"mess1">>,
            subtype = rpc,
            auth_override = #auth_override{
                client_auth = {token, <<"third-token">>},
                peer_ip = {9, 31, 216, 211},
                interface = graphsync,
                consumer_token = <<"983947234">>,
                data_access_caveats_policy = disallow_data_access_caveats
            },
            request = #gs_req_rpc{
                function = <<"f6">>,
                args = #{<<"args6">> => 6}
            }
        },
        #gs_req{
            id = <<"mess1">>,
            subtype = rpc,
            auth_override = #auth_override{
                client_auth = nobody,
                peer_ip = undefined,
                interface = oneclient,
                consumer_token = undefined,
                data_access_caveats_policy = allow_data_access_caveats
            },
            request = #gs_req_rpc{
                function = <<"f6">>,
                args = #{<<"args6">> => 6}
            }
        },
        #gs_req{
            id = <<"mess2">>,
            subtype = graph,
            auth_override = #auth_override{
                client_auth = {token, <<"fourth-token">>},
                peer_ip = undefined,
                interface = rest,
                consumer_token = undefined,
                data_access_caveats_policy = allow_data_access_caveats
            },
            request = #gs_req_graph{
                gri = #gri{type = od_user, id = <<"id">>, aspect = instance},
                operation = get,
                data = undefined,
                subscribe = true
            }
        }
    ],
    VersionsWithNewAuthOverride = [V || V <- gs_protocol:supported_versions(), V > 3],

    lists:foreach(fun(ProtoVersion) ->
        lists:foreach(fun(Request) ->
            check_encode_decode_for_proto_version(ProtoVersion, Request)
        end, RequestsToCheck)
    end, VersionsWithNewAuthOverride).


check_encode_decode_for_proto_version(ProtoVersion, Request) ->
    {ok, Encoded} = gs_protocol:encode(ProtoVersion, Request),
    true = is_map(Encoded),
    EncodedJSON = json_utils:encode(Encoded),
    DecodedJSON = json_utils:decode(EncodedJSON),
    {ok, Decoded} = gs_protocol:decode(ProtoVersion, DecodedJSON),
    ?assertEqual(Decoded, Request).


-endif.
