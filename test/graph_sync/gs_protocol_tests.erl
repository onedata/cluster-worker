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

-include("api_errors.hrl").
-include("graph_sync/graph_sync.hrl").
-include_lib("eunit/include/eunit.hrl").

gri_conversion_test() ->
    ?assertEqual(
        #gri{
            type = od_user, id = <<"12345">>,
            aspect = instance, scope = protected
        },
        gs_protocol:string_to_gri(<<"user.12345.instance:protected">>)
    ),
    ?assertEqual(
        <<"user.12345.instance:protected">>,
        gs_protocol:gri_to_string(#gri{
            type = od_user, id = <<"12345">>,
            aspect = instance, scope = protected
        })
    ),

    ?assertEqual(
        #gri{
            type = od_group, id = <<"ghdkeos2wsrt4">>,
            aspect = user_invite_token, scope = private
        },
        gs_protocol:string_to_gri(<<"group.ghdkeos2wsrt4.user_invite_token">>)
    ),
    ?assertEqual(
        <<"group.ghdkeos2wsrt4.user_invite_token:private">>,
        gs_protocol:gri_to_string(#gri{
            type = od_group, id = <<"ghdkeos2wsrt4">>,
            aspect = user_invite_token, scope = private
        })
    ),

    ?assertEqual(
        #gri{
            type = od_space, id = <<"spaceId">>,
            aspect = {user, <<"12345">>}, scope = private
        },
        gs_protocol:string_to_gri(<<"space.spaceId.user,12345">>)
    ),
    ?assertEqual(
        <<"space.spaceId.user,12345:private">>,
        gs_protocol:gri_to_string(#gri{
            type = od_space, id = <<"spaceId">>,
            aspect = {user, <<"12345">>}, scope = private
        })
    ),

    ?assertEqual(
        #gri{
            type = od_space, id = <<"spaceId">>,
            aspect = {user, <<"12345">>}, scope = public
        },
        gs_protocol:string_to_gri(<<"space.spaceId.user,12345:public">>)
    ),
    ?assertEqual(
        <<"space.spaceId.user,12345:public">>,
        gs_protocol:gri_to_string(#gri{
            type = od_space, id = <<"spaceId">>,
            aspect = {user, <<"12345">>}, scope = public
        })
    ),

    ?assertEqual(
        #gri{
            type = od_share, id = <<"sh732js">>,
            aspect = space, scope = protected
        },
        gs_protocol:string_to_gri(<<"share.sh732js.space:protected">>)
    ),
    ?assertEqual(
        <<"share.sh732js.space:protected">>,
        gs_protocol:gri_to_string(#gri{
            type = od_share, id = <<"sh732js">>,
            aspect = space, scope = protected
        })
    ),

    ?assertEqual(
        #gri{
            type = od_provider, id = <<"pr2ndv7y3bs">>,
            aspect = {space, <<"sp9fhh83">>}, scope = private
        },
        gs_protocol:string_to_gri(<<"provider.pr2ndv7y3bs.space,sp9fhh83:private">>)
    ),
    ?assertEqual(
        <<"provider.pr2ndv7y3bs.space,sp9fhh83:private">>,
        gs_protocol:gri_to_string(#gri{
            type = od_provider, id = <<"pr2ndv7y3bs">>,
            aspect = {space, <<"sp9fhh83">>}, scope = private
        })
    ),

    ?assertEqual(
        #gri{
            type = od_handle, id = <<"hndl76dhsha">>,
            aspect = handle, scope = public
        },
        gs_protocol:string_to_gri(<<"handle.hndl76dhsha.handle:public">>)
    ),
    ?assertEqual(
        <<"handle.hndl76dhsha.handle:public">>,
        gs_protocol:gri_to_string(#gri{
            type = od_handle, id = <<"hndl76dhsha">>,
            aspect = handle, scope = public
        })
    ),

    ?assertEqual(
        #gri{
            type = od_handle_service, id = <<"hs726shshao">>,
            aspect = instance, scope = private
        },
        gs_protocol:string_to_gri(<<"handleService.hs726shshao.instance">>)
    ),
    ?assertEqual(
        <<"handleService.hs726shshao.instance:private">>,
        gs_protocol:gri_to_string(#gri{
            type = od_handle_service, id = <<"hs726shshao">>,
            aspect = instance, scope = private
        })
    ),

    ok.


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
                session_id = <<"23423424qdsfdsgew456235tegdfg">>
            }
        },
        #gs_req{
            id = <<"mess5">>,
            subtype = rpc,
            auth_override = {token, <<"123123">>},
            request = #gs_req_rpc{
                function = <<"f5">>,
                args = #{<<"args5">> => 5}
            }
        },
        #gs_req{
            id = <<"mess6">>,
            subtype = rpc,
            auth_override = {basic, <<"123sdfadsfq345r123">>},
            request = #gs_req_rpc{
                function = <<"f6">>,
                args = #{<<"args6">> => 6}
            }
        },
        #gs_req{
            id = <<"mess7">>,
            subtype = rpc,
            auth_override = {macaroon, <<"123sdfadsfq345r123">>, [<<"dfsghdsy456ergadfg">>]},
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
                identity = nobody,
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
                identity = {user, <<"u22">>},
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
                identity = {user, <<"p23">>},
                attributes = #{<<"attr23">> => 23}
            }
        },
        #gs_resp{
            id = <<"mess23">>,
            subtype = rpc,
            success = false,
            error = ?ERROR_RPC_UNDEFINED,
            response = undefined
        },
        #gs_resp{
            id = <<"mess23">>,
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
            id = <<"mess26">>,
            subtype = graph,
            success = true,
            error = undefined,
            response = #gs_resp_graph{
                result = undefined
            }
        },
        #gs_resp{
            id = <<"mess27">>,
            subtype = graph,
            success = true,
            error = undefined,
            response = #gs_resp_graph{
                result = #{<<"data27">> => 27}
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
        }
    ],

    lists:foreach(
        fun(Request) ->
            {ok, Encoded} = gs_protocol:encode(1, Request),
            true = is_map(Encoded),
            EncodedJSON = json_utils:encode_map(Encoded),
            DecodedJSON = json_utils:decode_map(EncodedJSON),
            {ok, Decoded} = gs_protocol:decode(1, DecodedJSON),
            ?assertEqual(Decoded, Request)
        end, RequestsToCheck),

    ?assertMatch(?ERROR_BAD_MESSAGE(_), gs_protocol:decode(1, <<"sdfsdviuyasd9fas">>)).


encode_decode_error_test() ->
    % Tuple means that after encoding a decoding the left hand side error, right
    % one should be obtained.
    ErrorsToCheck = [
        ?ERROR_BAD_MESSAGE(<<"edaml-wsesjapfs">>),
        ?ERROR_BAD_VERSION([123, 345, 234, 1, 34]),
        ?ERROR_EXPECTED_HANDSHAKE_MESSAGE,
        ?ERROR_HANDSHAKE_ALREADY_DONE,
        ?ERROR_UNCLASSIFIED_ERROR(<<"desc">>),
        ?ERROR_BAD_TYPE,
        ?ERROR_NOT_SUBSCRIBABLE,
        ?ERROR_RPC_UNDEFINED,
        ?ERROR_INTERNAL_SERVER_ERROR,
        ?ERROR_NOT_IMPLEMENTED,
        ?ERROR_NOT_SUPPORTED,
        ?ERROR_NOT_FOUND,
        ?ERROR_UNAUTHORIZED,
        ?ERROR_FORBIDDEN,
        ?ERROR_BAD_MACAROON,
        ?ERROR_BAD_BASIC_CREDENTIALS,
        ?ERROR_MALFORMED_DATA,
        ?ERROR_MISSING_REQUIRED_VALUE(<<"spaceId">>),
        ?ERROR_MISSING_AT_LEAST_ONE_VALUE([<<"name">>, <<"type">>]),
        ?ERROR_BAD_DATA(<<"spaceId">>),
        ?ERROR_BAD_VALUE_EMPTY(<<"spaceId">>),
        ?ERROR_BAD_VALUE_BOOLEAN(<<"subdomainDelegation">>),
        ?ERROR_BAD_VALUE_BINARY(<<"spaceId">>),
        ?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<"urls">>),
        {?ERROR_BAD_VALUE_ATOM(<<"spaceId">>), ?ERROR_BAD_VALUE_BINARY(<<"spaceId">>)},
        {?ERROR_BAD_VALUE_LIST_OF_ATOMS(<<"privileges">>), ?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<"privileges">>)},
        ?ERROR_BAD_VALUE_DOMAIN(<<"domain">>),
        ?ERROR_BAD_VALUE_SUBDOMAIN,
        ?ERROR_BAD_VALUE_LIST_OF_IPV4_ADDRESSES(<<"ip_list">>),
        ?ERROR_BAD_VALUE_INTEGER(<<"size">>),
        ?ERROR_BAD_VALUE_FLOAT(<<"latitude">>),
        ?ERROR_BAD_VALUE_JSON(<<"<xml></xml>">>),
        ?ERROR_BAD_VALUE_TOKEN(<<"supportToken">>),
        ?ERROR_BAD_VALUE_TOO_LOW(<<"size">>, 500),
        ?ERROR_BAD_VALUE_TOO_HIGH(<<"size">>, 1000),
        ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"size">>, 500, 1000),
        ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"type">>, [<<"a">>, <<"b">>]),
        ?ERROR_BAD_VALUE_LIST_NOT_ALLOWED(<<"type">>, [<<"a">>, <<"b">>]),
        ?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"spaceId">>),
        ?ERROR_BAD_VALUE_IDENTIFIER_OCCUPIED(<<"spaceId">>),
        ?ERROR_BAD_VALUE_BAD_TOKEN_TYPE(<<"supportToken">>),
        ?ERROR_BAD_VALUE_IDENTIFIER(<<"id">>),
        ?ERROR_BAD_VALUE_ALIAS(<<"alias">>),
        ?ERROR_BAD_VALUE_ALIAS_WRONG_PREFIX(<<"alias">>),
        ?ERROR_RELATION_DOES_NOT_EXIST(od_user, <<"user1">>, od_space, <<"space1">>),
        ?ERROR_RELATION_ALREADY_EXISTS(od_user, <<"user1">>, od_space, <<"space1">>),
        ?ERROR_CANNOT_DELETE_ENTITY(od_user, <<"user1">>)
    ],

    lists:foreach(
        fun(ErrorToCheck) ->
            {Error, Expected} = case ErrorToCheck of
                Err = {error, _} -> {Err, Err};
                {A = {error, _}, B = {error, _}} -> {A, B}
            end,
            % Error push message will be used to wrap the errors.
            WrappedError = #gs_push{subtype = error, message = #gs_push_error{
                error = Error
            }},
            {ok, Encoded} = gs_protocol:encode(1, WrappedError),
            true = is_map(Encoded),
            EncodedJSON = json_utils:encode_map(Encoded),
            DecodedJSON = json_utils:decode_map(EncodedJSON),
            {ok, Decoded} = gs_protocol:decode(1, DecodedJSON),
            ?assertEqual(Decoded#gs_push.message#gs_push_error.error, Expected)
        end, ErrorsToCheck).

-endif.