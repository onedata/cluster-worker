%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header file contains record definitions and macros used in appmock client to
%%% interface with remote control endpoint.
%%% @end
%%%-------------------------------------------------------------------

% Term that is sent back when an operation has completed successfully.
-define(OK_RESULT, [{<<"result">>, <<"ok">>}]).

-define(NAGIOS_ENPOINT, "/nagios").

% Endpoint used to verify if all mocked endpoint were requested in proper order.
-define(VERIFY_REST_HISTORY_PATH, "/verify_rest_history").
% Transform a proplist of pairs {Port, Path} into a term that is sent as JSON to verify_rest_history endpoint (client side).
-define(VERIFY_REST_HISTORY_PACK_REQUEST(_VerificationList),
    lists:map(
        fun({_Port, _Path}) ->
            {<<"endpoint">>, [{<<"port">>, _Port}, {<<"path">>, _Path}]}
        end, _VerificationList)
).
% Transform a struct obtained by decoding JSON into a proplist of pairs {Port, Path} (server side).
-define(VERIFY_REST_HISTORY_UNPACK_REQUEST(_Struct),
    lists:map(
        fun({<<"endpoint">>, [{<<"port">>, _Port}, {<<"path">>, _Path}]}) ->
            {_Port, _Path}
        end, _Struct)
).
% Produces an error message if verification fails (server side).
-define(VERIFY_REST_HISTORY_PACK_ERROR(_History),
    [{<<"result">>, <<"error">>}, {<<"history">>, ?VERIFY_REST_HISTORY_PACK_REQUEST(_History)}]).
% Retrieves the error details from verify_rest_history error (actual request history) (client side).
-define(VERIFY_REST_HISTORY_UNPACK_ERROR(_RespBody),
    begin
        [{<<"result">>, <<"error">>}, {<<"history">>, _Struct}] = _RespBody,
        ?VERIFY_REST_HISTORY_UNPACK_REQUEST(_Struct)
    end
).


% Endpoint used to verify if a mocked endpoint has been requested certain amount of times.
-define(VERIFY_REST_ENDPOINT_PATH, "/verify_rest_endpoint").
% Creates a term that is sent as JSON to verify_rest_endpoint endpoint (client side).
-define(VERIFY_REST_ENDPOINT_PACK_REQUEST(_Port, _Path, _Number),
    [
        {<<"port">>, _Port},
        {<<"path">>, _Path},
        {<<"number">>, _Number}
    ]
).
% Retrieves params sent to verify_rest_endpoint endpoint (server side).
-define(VERIFY_REST_ENDPOINT_UNPACK_REQUEST(_Struct),
    {
        proplists:get_value(<<"port">>, _Struct),
        proplists:get_value(<<"path">>, _Struct),
        proplists:get_value(<<"number">>, _Struct)
    }
).
% Produces an error message if verification fails (server side).
-define(VERIFY_REST_ENDPOINT_PACK_ERROR(_Number),
    [{<<"result">>, <<"error">>}, {<<"number">>, _Number}]).
% Produces an error message if the endpoint requested to be verified does not exis (server side).
-define(VERIFY_REST_ENDPOINT_PACK_ERROR_WRONG_ENDPOINT,
    [{<<"result">>, <<"error">>}, {<<"reason">>, <<"wrong_endpoint">>}]).
% Retrieves the error details from verify_rest_endpoint error (client side).
-define(VERIFY_REST_ENDPOINT_UNPACK_ERROR(_RespBody),
    case _RespBody of
        [{<<"result">>, <<"error">>}, {<<"number">>, _Number}] -> {error, _Number};
        [{<<"result">>, <<"error">>}, {<<"reason">>, <<"wrong_endpoint">>}] -> {error, wrong_endpoint}
    end
).


% Endpoint used to verify if a mocked TCP server has received a given packet.
% The port binding is used to identify a tcp server.
-define(VERIFY_TCP_SERVER_RECEIVED_PATH(_Port), "/verify_tcp_server_received/" ++ integer_to_list(_Port)).
-define(VERIFY_TCP_SERVER_RECEIVED_COWBOY_ROUTE, "/verify_tcp_server_received/:port").
% Creates message that is sent to verify_tcp_server_received endpoint (client side).
% For now, its just bare bytes, but the macro stays so it can be easily changed -
% for example to base64 encoded.
-define(VERIFY_TCP_SERVER_RECEIVED_PACK_REQUEST(_BinaryData),
    _BinaryData
).
% Retrieves params sent to verify_tcp_server_received endpoint (server side).
-define(VERIFY_TCP_SERVER_RECEIVED_UNPACK_REQUEST(_BinaryData),
    _BinaryData
).
% Produces an error message if verification fails (server side).
-define(VERIFY_TCP_SERVER_RECEIVED_PACK_ERROR,
    [{<<"result">>, <<"error">>}]).
