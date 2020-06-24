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
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/aai/aai.hrl").

%%%===================================================================
%%% Tests generator
%%%===================================================================

gs_protocol_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            fun gri_conversion/0,
            fun greatest_common_version/0,
            fun encode_decode_message/0,
            fun encode_decode_error/0
        ]
    }.


%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

setup() ->
    meck:new(gs_protocol_plugin, [non_strict]),
    meck:expect(gs_protocol_plugin, encode_entity_type, fun(Atom) -> atom_to_binary(Atom, utf8) end),
    meck:expect(gs_protocol_plugin, decode_entity_type, fun(Bin) -> binary_to_atom(Bin, utf8) end).

teardown(_) ->
    ?assert(meck:validate(gs_protocol_plugin)),
    ok = meck:unload(gs_protocol_plugin).


%%%===================================================================
%%% Tests
%%%===================================================================

gri_conversion() ->
    ?assertEqual(
        #gri{
            type = user, id = <<"12345">>,
            aspect = instance, scope = protected
        },
        gs_protocol:string_to_gri(<<"user.12345.instance:protected">>)
    ),
    ?assertEqual(
        <<"user.12345.instance:protected">>,
        gs_protocol:gri_to_string(#gri{
            type = user, id = <<"12345">>,
            aspect = instance, scope = protected
        })
    ),

    ?assertEqual(
        #gri{
            type = group, id = <<"ghdkeos2wsrt4">>,
            aspect = user_invite_token, scope = private
        },
        gs_protocol:string_to_gri(<<"group.ghdkeos2wsrt4.user_invite_token">>)
    ),
    ?assertEqual(
        <<"group.ghdkeos2wsrt4.user_invite_token:private">>,
        gs_protocol:gri_to_string(#gri{
            type = group, id = <<"ghdkeos2wsrt4">>,
            aspect = user_invite_token, scope = private
        })
    ),

    ?assertEqual(
        #gri{
            type = space, id = <<"spaceId">>,
            aspect = {user, <<"12345">>}, scope = private
        },
        gs_protocol:string_to_gri(<<"space.spaceId.user,12345">>)
    ),
    ?assertEqual(
        <<"space.spaceId.user,12345:private">>,
        gs_protocol:gri_to_string(#gri{
            type = space, id = <<"spaceId">>,
            aspect = {user, <<"12345">>}, scope = private
        })
    ),

    ?assertEqual(
        #gri{
            type = space, id = <<"spaceId">>,
            aspect = {user, <<"12345">>}, scope = public
        },
        gs_protocol:string_to_gri(<<"space.spaceId.user,12345:public">>)
    ),
    ?assertEqual(
        <<"space.spaceId.user,12345:public">>,
        gs_protocol:gri_to_string(#gri{
            type = space, id = <<"spaceId">>,
            aspect = {user, <<"12345">>}, scope = public
        })
    ),

    ?assertEqual(
        #gri{
            type = share, id = <<"sh732js">>,
            aspect = space, scope = protected
        },
        gs_protocol:string_to_gri(<<"share.sh732js.space:protected">>)
    ),
    ?assertEqual(
        <<"share.sh732js.space:protected">>,
        gs_protocol:gri_to_string(#gri{
            type = share, id = <<"sh732js">>,
            aspect = space, scope = protected
        })
    ),

    ?assertEqual(
        #gri{
            type = provider, id = <<"pr2ndv7y3bs">>,
            aspect = {space, <<"sp9fhh83">>}, scope = private
        },
        gs_protocol:string_to_gri(<<"provider.pr2ndv7y3bs.space,sp9fhh83:private">>)
    ),
    ?assertEqual(
        <<"provider.pr2ndv7y3bs.space,sp9fhh83:private">>,
        gs_protocol:gri_to_string(#gri{
            type = provider, id = <<"pr2ndv7y3bs">>,
            aspect = {space, <<"sp9fhh83">>}, scope = private
        })
    ),

    ?assertEqual(
        #gri{
            type = handle, id = <<"hndl76dhsha">>,
            aspect = handle, scope = public
        },
        gs_protocol:string_to_gri(<<"handle.hndl76dhsha.handle:public">>)
    ),
    ?assertEqual(
        <<"handle.hndl76dhsha.handle:public">>,
        gs_protocol:gri_to_string(#gri{
            type = handle, id = <<"hndl76dhsha">>,
            aspect = handle, scope = public
        })
    ),

    ?assertEqual(
        #gri{
            type = handle_service, id = <<"hs726shshao">>,
            aspect = instance, scope = private
        },
        gs_protocol:string_to_gri(<<"handle_service.hs726shshao.instance">>)
    ),
    ?assertEqual(
        <<"handle_service.hs726shshao.instance:private">>,
        gs_protocol:gri_to_string(#gri{
            type = handle_service, id = <<"hs726shshao">>,
            aspect = instance, scope = private
        })
    ),

    ok.


greatest_common_version() ->
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


encode_decode_message() ->
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
                auth = {macaroon, <<"macaroon">>, []},
                session_id = <<"zxvcert245234234234234">>
            }
        },
        #gs_req{
            id = <<"mess6">>,
            subtype = handshake,
            auth_override = undefined,
            request = #gs_req_handshake{
                supported_versions = [1],
                auth = {macaroon, <<"macaroon">>, [<<"d1">>, <<"d2">>]},
                session_id = <<"ccxvsdfsdfsdfsdf">>
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
                gri = #gri{type = user, id = undefined, aspect = whatever},
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
                gri = #gri{type = user, id = <<"id9">>, aspect = whatever, scope = shared},
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
                gri = #gri{type = space, id = <<"id10">>, aspect = whatever},
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
                gri = #gri{type = space, id = <<"id11">>, aspect = whatever},
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
                gri = #gri{type = user, id = <<"id12">>, aspect = whatever},
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
                gri = #gri{type = user, id = <<"id13">>, aspect = whatever},
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
                gri = #gri{type = user, id = <<"id14">>, aspect = whatever},
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
                gri = #gri{type = user, id = <<"id15">>, aspect = whatever},
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
                gri = #gri{type = user, id = <<"id17">>, aspect = whatever},
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
                gri = #gri{type = user, id = <<"id18">>, aspect = whatever},
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
                gri = #gri{type = user, id = <<"id19">>, aspect = whatever}
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
                gri = #gri{type = user, id = <<"id30">>, aspect = whatever},
                change_type = updated,
                data = #{<<"data30">> => 30}
            }
        },
        #gs_push{
            subtype = graph,
            message = #gs_push_graph{
                gri = #gri{type = user, id = <<"id31">>, aspect = whatever},
                change_type = deleted,
                data = undefined
            }
        },
        #gs_push{
            subtype = nosub,
            message = #gs_push_nosub{
                gri = #gri{type = user, id = <<"id32">>, aspect = whatever},
                reason = forbidden
            }
        },
        #gs_push{
            subtype = nosub,
            message = #gs_push_nosub{
                gri = #gri{type = user, id = <<"id33">>, aspect = whatever},
                auth_hint = ?THROUGH_USER(<<"otherid33">>),
                reason = forbidden
            }
        }
    ],

    lists:foreach(fun(ProtoVersion) ->
        lists:foreach(fun(Request) ->
            {ok, Encoded} = gs_protocol:encode(ProtoVersion, Request),
            true = is_map(Encoded),
            EncodedJSON = json_utils:encode(Encoded),
            DecodedJSON = json_utils:decode(EncodedJSON),
            {ok, Decoded} = gs_protocol:decode(ProtoVersion, DecodedJSON),
            ?assertEqual(Decoded, Request)
        end, RequestsToCheck)
    end, gs_protocol:supported_versions()),

    ?assertMatch(?ERROR_BAD_MESSAGE(_), gs_protocol:decode(1, <<"sdfsdviuyasd9fas">>)),
    ?assertMatch(?ERROR_BAD_MESSAGE(_), gs_protocol:decode(2, <<"sdfsdviuyasd9fas">>)).


encode_decode_error() ->
    % Tuple means that after encoding and decoding the left hand side error,
    % right one should be obtained.
    ErrorsToCheck = [
        ?ERROR_BAD_MESSAGE(<<"edaml-wsesjapfs">>),
        ?ERROR_BAD_VERSION([123, 345, 234, 1, 34]),
        ?ERROR_EXPECTED_HANDSHAKE_MESSAGE,
        ?ERROR_HANDSHAKE_ALREADY_DONE,
        ?ERROR_UNKNOWN_ERROR(#{<<"id">> => <<"unknownErrorId">>, <<"details">> => #{<<"desc">> => <<"serious">>}}),
        ?ERROR_BAD_TYPE,
        ?ERROR_NOT_SUBSCRIBABLE,
        ?ERROR_RPC_UNDEFINED,
        ?ERROR_INTERNAL_SERVER_ERROR,
        ?ERROR_NOT_IMPLEMENTED,
        ?ERROR_NOT_SUPPORTED,
        ?ERROR_NOT_FOUND,
        ?ERROR_UNAUTHORIZED(?ERROR_UNKNOWN_ERROR(#{<<"id">> => <<"tokenInvalid">>})),
        ?ERROR_UNAUTHORIZED,
        ?ERROR_FORBIDDEN,
        ?ERROR_BAD_MACAROON,
        ?ERROR_MACAROON_INVALID,
        ?ERROR_MACAROON_EXPIRED,
        ?ERROR_MACAROON_TTL_TO_LONG(10202),
        ?ERROR_BAD_AUDIENCE_TOKEN,
        ?ERROR_TOKEN_AUDIENCE_FORBIDDEN,
        ?ERROR_TOKEN_SESSION_INVALID,
        ?ERROR_BAD_BASIC_CREDENTIALS,
        ?ERROR_MALFORMED_DATA,
        ?ERROR_MISSING_REQUIRED_VALUE(<<"spaceId">>),
        ?ERROR_BAD_IDP_ACCESS_TOKEN(<<"keycloak">>),
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
        ?ERROR_BAD_VALUE_FULL_NAME,
        ?ERROR_BAD_VALUE_USERNAME,
        ?ERROR_BAD_VALUE_PASSWORD,
        ?ERROR_BAD_VALUE_NAME,
        ?ERROR_SUBDOMAIN_DELEGATION_NOT_SUPPORTED,
        ?ERROR_SUBDOMAIN_DELEGATION_DISABLED,
        ?ERROR_BASIC_AUTH_NOT_SUPPORTED,
        ?ERROR_BASIC_AUTH_DISABLED,
        ?ERROR_PROTECTED_GROUP,
        ?ERROR_RELATION_DOES_NOT_EXIST(user, <<"user1">>, space, <<"space1">>),
        ?ERROR_RELATION_ALREADY_EXISTS(user, <<"user1">>, space, <<"space1">>),
        ?ERROR_CANNOT_DELETE_ENTITY(user, <<"user1">>),
        ?ERROR_CANNOT_ADD_RELATION_TO_SELF,
        ?ERROR_TEMPORARY_FAILURE,
        ?ERROR_BAD_GUI_PACKAGE,
        ?ERROR_GUI_PACKAGE_TOO_LARGE,
        ?ERROR_GUI_PACKAGE_UNVERIFIED(<<"efff1f3e912dac2dce88ccf02352e32b906d30b78fb47b89b0d8b2744cd81b29">>),
        ?ERROR_ALREADY_EXISTS,
        ?ERROR_POSIX(eacess),
        ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"viewName">>),
        ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"providerId">>),
        ?ERROR_VIEW_NOT_EXISTS_ON(<<"providerId">>),
        ?ERROR_TRANSFER_ALREADY_ENDED,
        ?ERROR_TRANSFER_NOT_ENDED
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
            EncodedJSON = json_utils:encode(Encoded),
            DecodedJSON = json_utils:decode(EncodedJSON),
            {ok, Decoded} = gs_protocol:decode(1, DecodedJSON),
            ?assertEqual(Decoded#gs_push.message#gs_push_error.error, Expected)
        end, ErrorsToCheck).

-endif.
