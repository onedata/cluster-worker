%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains procedures to encode and decode Graph Sync messages
%%% and definitions of types used both on client and server side.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_protocol).
-author("Lukasz Opiola").

-include_lib("ctool/include/api_errors.hrl").
-include("graph_sync/graph_sync.hrl").
-include_lib("ctool/include/logging.hrl").


-type req_wrapper() :: #gs_req{}.
-type handshake_req() :: #gs_req_handshake{}.
-type rpc_req() :: #gs_req_rpc{}.
-type graph_req() :: #gs_req_graph{}.
-type unsub_req() :: #gs_req_unsub{}.
-type req() :: handshake_req() | rpc_req() | graph_req() | unsub_req().

-type resp_wrapper() :: #gs_resp{}.
-type handshake_resp() :: #gs_resp_handshake{}.
-type rpc_resp() :: #gs_resp_rpc{}.
-type graph_resp() :: #gs_resp_graph{}.
-type unsub_resp() :: #gs_resp_unsub{}.
-type resp() :: handshake_resp() | rpc_resp() | graph_resp() | unsub_resp().

-type push_wrapper() :: #gs_push{}.
-type graph_push() :: #gs_push_graph{}.
-type nosub_push() :: #gs_push_nosub{}.
-type error_push() :: #gs_push_error{}.
-type push() :: graph_push() | nosub_push() | error_push().

-export_type([
    req_wrapper/0,
    handshake_req/0,
    rpc_req/0,
    graph_req/0,
    unsub_req/0,
    req/0,

    resp_wrapper/0,
    handshake_resp/0,
    rpc_resp/0,
    graph_resp/0,
    unsub_resp/0,
    resp/0,

    push_wrapper/0,
    graph_push/0,
    nosub_push/0,
    error_push/0,
    push/0
]).

% Unique message id used to match requests to responses.
-type message_id() :: binary().
% An opaque term, understood by gs_logic_plugin, identifying requesting client
% and his authorization.
-type client() :: term().
-type protocol_version() :: non_neg_integer().
% Graph Sync session id, used as reference to store subscriptions data
-type session_id() :: gs_session:id().
% Identity of the client that connects to the Graph Sync server
-type identity() :: nobody | {user, UId :: binary()} | {provider, PId :: binary()}.
% Optional, arbitrary attributes that can be sent by the sever with successful
% handshake response.
-type handshake_attributes() :: undefined | maps:map().

% Denotes type of message so the payload can be properly interpreted
-type message_type() :: request | response | push.
% Denotes subtype of message so the payload can be properly interpreted
-type message_subtype() :: handshake | rpc | graph | unsub | nosub | error.

% Used to override the client authorization established on connection level, per
% request.
-type auth_override() :: undefined | {token, binary()} | {basic, binary()} |
{macaroon, Macaroon :: binary(), DischMacaroons :: [binary()]}.

% Possible entity types
-type entity_type() :: atom().
-type entity_id() :: undefined | binary().
% Aspect of given entity, one of resource identifiers
-type aspect() :: atom() | {atom(), term()}.
% Scope of given aspect, allows to differentiate access to subsets of aspect data
% 'auto' scope means the maximum scope (if any) the client is authorized to access.
-type scope() :: private | protected | shared | public | auto.
% Graph Resource Identifier - a record identifying a certain resource in the graph.
-type gri() :: #gri{}.
% Requested operation
-type operation() :: create | get | update | delete.
% Data included in the request or response and its format:
%   resource - data carries a resource with included gri
%   value - data carries a value that is not related to any resource
-type data_format() :: undefined | resource | value.
-type data() :: undefined | json_map() | binary() | integer().
% Authorization hint that indicates the context needed to access shared
% resources or disambiguate issuer of an operation.
-type auth_hint() :: undefined | {
    throughUser | throughGroup | throughSpace | throughProvider |
    throughHandleService | throughHandle | asUser | asGroup,
    EntityId :: binary()
}.
% A prefetched entity that can be passed to gs_logic_plugin to speed up request
% handling. Undefined if no entity was prefetched.
-type entity() :: undefined | term().

-type change_type() :: updated | deleted.
-type nosub_reason() :: forbidden.

% Function identifier in RPC
-type rpc_function() :: binary().
% Arguments map in RPC
-type rpc_args() :: maps:map().

-type rpc_result() :: {ok, data()} | {error, term()}.

-type error() :: {error, term()}.

-type graph_create_result() :: ok | {ok, value, term()} |
{ok, resource, {gri(), term()} | {gri(), auth_hint(), term()}} | error().
-type graph_get_result() :: {ok, term()} | {ok, gri(), term()} | error().
-type graph_delete_result() :: ok | error().
-type graph_update_result() :: ok | error().

-type graph_request_result() :: graph_create_result() | graph_get_result() |
graph_update_result() | graph_delete_result().

-type json_map() :: maps:map().

-export_type([
    message_id/0,
    client/0,
    protocol_version/0,
    session_id/0,
    identity/0,
    handshake_attributes/0,
    message_type/0,
    message_subtype/0,
    auth_override/0,
    entity_type/0,
    entity_id/0,
    aspect/0,
    scope/0,
    gri/0,
    operation/0,
    data_format/0,
    data/0,
    auth_hint/0,
    entity/0,
    change_type/0,
    nosub_reason/0,
    rpc_function/0,
    rpc_args/0,
    rpc_result/0,
    error/0,
    graph_create_result/0,
    graph_get_result/0,
    graph_delete_result/0,
    graph_update_result/0,
    graph_request_result/0,
    json_map/0
]).

%% API
-export([supported_versions/0]).
-export([greatest_common_version/2]).
-export([encode/2]).
-export([decode/2]).
-export([
    generate_success_response/2,
    generate_error_response/2,
    generate_error_push_message/1
]).
-export([string_to_gri/1, gri_to_string/1]).
-export([undefined_to_null/1, null_to_undefined/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of supported protocol versions by this version of module.
%% @end
%%--------------------------------------------------------------------
-spec supported_versions() -> [protocol_version()].
supported_versions() -> [1, 2].


%%--------------------------------------------------------------------
%% @doc
%% Finds the greatest common protocol versions given two lists of supported
%% protocol versions.
%% @end
%%--------------------------------------------------------------------
-spec greatest_common_version([protocol_version()], [protocol_version()]) ->
    {true, protocol_version()} | false.
greatest_common_version(ClientVersions, ServerVersions) ->
    greatest_common_version_sorted(
        lists:reverse(lists:usort(ClientVersions)),
        lists:reverse(lists:usort(ServerVersions))
    ).

-spec greatest_common_version_sorted([protocol_version()], [protocol_version()]) ->
    {true, protocol_version()} | false.
greatest_common_version_sorted([Cl | _ClTail], [Sv | _SvTail]) when Cl =:= Sv ->
    {true, Cl};
greatest_common_version_sorted([Cl | ClTail], [Sv | SvTail]) when Cl > Sv ->
    greatest_common_version_sorted(ClTail, [Sv | SvTail]);
greatest_common_version_sorted([Cl | ClTail], [Sv | SvTail]) when Cl < Sv ->
    greatest_common_version_sorted([Cl | ClTail], SvTail);
greatest_common_version_sorted(_, _) ->
    false.


%%--------------------------------------------------------------------
%% @doc
%% Encodes a graph sync message returning a map ready to be converted to JSON.
%% @end
%%--------------------------------------------------------------------
-spec encode(protocol_version(), req_wrapper() | resp_wrapper() | push_wrapper()) ->
    {ok, json_map()} | error().
encode(ProtocolVersion, Record) ->
    try
        JSONMap = case Record of
            #gs_req{} ->
                encode_request(ProtocolVersion, Record);
            #gs_resp{} ->
                encode_response(ProtocolVersion, Record);
            #gs_push{} ->
                encode_push(ProtocolVersion, Record)
        end,
        {ok, JSONMap}
    catch Type:Reason ->
        ?error_stacktrace("Cannot encode gs message - ~p:~p~nMessage: ~p", [
            Type, Reason, Record
        ]),
        ?ERROR_BAD_MESSAGE(Record)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Decodes a graph sync message expressed by a map obtained from JSON parsing,
%% returning message record.
%% @end
%%--------------------------------------------------------------------
-spec decode(protocol_version(), json_map()) ->
    {ok, req_wrapper() | resp_wrapper() | push_wrapper()} | error().
decode(ProtocolVersion, JSONMap) ->
    try
        Record = case maps:get(<<"type">>, JSONMap) of
            <<"request">> ->
                decode_request(ProtocolVersion, JSONMap);
            <<"response">> ->
                decode_response(ProtocolVersion, JSONMap);
            <<"push">> ->
                decode_push(ProtocolVersion, JSONMap)
        end,
        {ok, Record}
    catch _:_ ->
        % No log is needed here, as this code is expected to crash any time
        % a malformed JSON comes. The error pushed back to the client will
        % contain the message that could not be decoded.
        ?ERROR_BAD_MESSAGE(JSONMap)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates a #gs_resp{} record with message id matching given request and a
%% success response.
%% @end
%%--------------------------------------------------------------------
-spec generate_success_response(req_wrapper(), data()) -> resp_wrapper().
generate_success_response(#gs_req{id = Id, subtype = Subtype}, Response) ->
    #gs_resp{
        id = Id,
        subtype = Subtype,
        success = true,
        response = Response
    }.


%%--------------------------------------------------------------------
%% @doc
%% Creates a #gs_resp{} record with message id matching given request and an
%% error response.
%% @end
%%--------------------------------------------------------------------
-spec generate_error_response(req_wrapper(), Error :: error()) ->
    resp_wrapper().
generate_error_response(#gs_req{id = Id, subtype = Subtype}, Error) ->
    #gs_resp{
        id = Id,
        subtype = Subtype,
        success = false,
        error = Error
    }.


%%--------------------------------------------------------------------
%% @doc
%% Creates a #gs_push{} record expressing an error encountered on server side
%% that could not be coupled with any request.
%% @end
%%--------------------------------------------------------------------
-spec generate_error_push_message(Error :: error()) -> push_wrapper().
generate_error_push_message(Error) ->
    #gs_push{
        subtype = error, message = #gs_push_error{
            error = Error
        }}.


%%--------------------------------------------------------------------
%% @doc
%% If given term is undefined returns null, otherwise returns unchanged term.
%% @end
%%--------------------------------------------------------------------
-spec undefined_to_null(undefined | term()) -> null | term().
undefined_to_null(undefined) ->
    null;
undefined_to_null(Other) ->
    Other.


%%--------------------------------------------------------------------
%% @doc
%% If given term is null returns undefined, otherwise returns unchanged term.
%% @end
%%--------------------------------------------------------------------
-spec null_to_undefined(null | term()) -> undefined | term().
null_to_undefined(null) ->
    undefined;
null_to_undefined(Other) ->
    Other.


%%--------------------------------------------------------------------
%% @doc
%% Converts GRI expressed as string into record.
%% @end
%%--------------------------------------------------------------------
-spec string_to_gri(binary()) -> gri().
string_to_gri(String) ->
    [TypeStr, IdBinary, AspectScope] = binary:split(String, <<".">>, [global]),
    Type = ?GS_PROTOCOL_PLUGIN:decode_entity_type(TypeStr),
    Id = string_to_id(IdBinary),
    {Aspect, Scope} = case binary:split(AspectScope, <<":">>, [global]) of
        [A, S] -> {string_to_aspect(A), string_to_scope(S)};
        [A] -> {string_to_aspect(A), private}
    end,
    #gri{type = Type, id = Id, aspect = Aspect, scope = Scope}.


%%--------------------------------------------------------------------
%% @doc
%% Converts GRI expressed as record into string.
%% @end
%%--------------------------------------------------------------------
-spec gri_to_string(gri()) -> binary().
gri_to_string(#gri{type = Type, id = Id, aspect = Aspect, scope = Scope}) ->
    <<
        (?GS_PROTOCOL_PLUGIN:encode_entity_type(Type))/binary, ".",
        (id_to_string(Id))/binary, ".",
        (aspect_to_string(Aspect))/binary, ":",
        (scope_to_string(Scope))/binary
    >>.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec encode_request(protocol_version(), req_wrapper()) -> json_map().
encode_request(ProtocolVersion, #gs_req{} = GSReq) ->
    #gs_req{
        id = Id, subtype = Subtype, auth_override = AuthOverride, request = Request
    } = GSReq,
    Payload = case Request of
        #gs_req_handshake{} ->
            encode_request_handshake(ProtocolVersion, Request);
        #gs_req_rpc{} ->
            encode_request_rpc(ProtocolVersion, Request);
        #gs_req_graph{} ->
            encode_request_graph(ProtocolVersion, Request);
        #gs_req_unsub{} ->
            encode_request_unsub(ProtocolVersion, Request)
    end,
    #{
        <<"id">> => Id,
        <<"type">> => <<"request">>,
        <<"subtype">> => subtype_to_string(Subtype),
        <<"authOverride">> => auth_override_to_json(AuthOverride),
        <<"payload">> => Payload
    }.


-spec encode_request_handshake(protocol_version(), handshake_req()) -> json_map().
encode_request_handshake(_, #gs_req_handshake{} = Req) ->
    % Handshake messages do not change regardless of the protocol version
    #gs_req_handshake{
        supported_versions = SupportedVersions, session_id = SessionId
    } = Req,
    #{
        <<"supportedVersions">> => SupportedVersions,
        <<"sessionId">> => undefined_to_null(SessionId)
    }.


-spec encode_request_rpc(protocol_version(), rpc_req()) -> json_map().
encode_request_rpc(_, #gs_req_rpc{} = Req) ->
    #gs_req_rpc{
        function = Function, args = Args
    } = Req,
    #{
        <<"function">> => Function,
        <<"args">> => Args
    }.


-spec encode_request_graph(protocol_version(), graph_req()) -> json_map().
encode_request_graph(_, #gs_req_graph{} = Req) ->
    #gs_req_graph{
        gri = GRI, operation = Operation, data = Data,
        subscribe = Subscribe, auth_hint = AuthHint
    } = Req,
    #{
        <<"gri">> => gri_to_string(GRI),
        <<"operation">> => operation_to_string(Operation),
        <<"data">> => undefined_to_null(Data),
        <<"subscribe">> => Subscribe,
        <<"authHint">> => auth_hint_to_json(AuthHint)
    }.


-spec encode_request_unsub(protocol_version(), unsub_req()) -> json_map().
encode_request_unsub(_, #gs_req_unsub{} = Req) ->
    #gs_req_unsub{
        gri = GRI
    } = Req,
    #{
        <<"gri">> => gri_to_string(GRI)
    }.


-spec encode_response(protocol_version(), resp_wrapper()) -> json_map().
encode_response(ProtocolVersion, #gs_resp{} = GSReq) ->
    #gs_resp{
        id = Id, subtype = Subtype, success = Success, error = Error, response = Response
    } = GSReq,
    Data = case Success of
        false ->
            #{};
        true ->
            case Response of
                #gs_resp_handshake{} ->
                    encode_response_handshake(ProtocolVersion, Response);
                #gs_resp_rpc{} ->
                    encode_response_rpc(ProtocolVersion, Response);
                #gs_resp_graph{} ->
                    encode_response_graph(ProtocolVersion, Response);
                #gs_resp_unsub{} ->
                    encode_response_unsub(ProtocolVersion, Response)
            end
    end,
    #{
        <<"id">> => Id,
        <<"type">> => <<"response">>,
        <<"subtype">> => subtype_to_string(Subtype),
        <<"payload">> => #{
            <<"success">> => Success,
            <<"error">> => gs_protocol_errors:error_to_json(ProtocolVersion, Error),
            <<"data">> => undefined_to_null(Data)
        }
    }.


-spec encode_response_handshake(protocol_version(), handshake_resp()) -> json_map().
encode_response_handshake(_, #gs_resp_handshake{} = Resp) ->
    % Handshake messages do not change regardless of the protocol version
    #gs_resp_handshake{
        version = Version, session_id = SessionId,
        identity = Identity, attributes = Attributes
    } = Resp,
    #{
        <<"version">> => Version,
        <<"sessionId">> => SessionId,
        <<"identity">> => identity_to_json(Identity),
        <<"attributes">> => undefined_to_null(Attributes)
    }.


-spec encode_response_rpc(protocol_version(), rpc_resp()) -> json_map().
encode_response_rpc(_, #gs_resp_rpc{} = Resp) ->
    #gs_resp_rpc{
        result = Result
    } = Resp,
    undefined_to_null(Result).


-spec encode_response_graph(protocol_version(), graph_resp()) -> json_map().
encode_response_graph(_, #gs_resp_graph{data_format = undefined}) ->
    undefined;
encode_response_graph(1, #gs_resp_graph{data_format = Format, data = Result}) ->
    case Format of
        value -> #{<<"data">> => undefined_to_null(Result)};
        resource -> Result
    end;
encode_response_graph(2, #gs_resp_graph{data_format = Format, data = Result}) ->
    FormatStr = data_format_to_str(Format),
    #{
        <<"format">> => FormatStr,
        FormatStr => undefined_to_null(Result)
    }.


-spec encode_response_unsub(protocol_version(), unsub_resp()) -> json_map().
encode_response_unsub(_, #gs_resp_unsub{}) ->
    % Currently the response does not carry any information
    #{}.


-spec encode_push(protocol_version(), push_wrapper()) -> json_map().
encode_push(ProtocolVersion, #gs_push{} = GSReq) ->
    #gs_push{
        subtype = Subtype, message = Message
    } = GSReq,
    Payload = case Message of
        #gs_push_graph{} ->
            encode_push_graph(ProtocolVersion, Message);
        #gs_push_nosub{} ->
            encode_push_nosub(ProtocolVersion, Message);
        #gs_push_error{} ->
            encode_push_error(ProtocolVersion, Message)
    end,
    #{
        <<"type">> => <<"push">>,
        <<"subtype">> => subtype_to_string(Subtype),
        <<"payload">> => Payload
    }.


-spec encode_push_graph(protocol_version(), graph_push()) -> json_map().
encode_push_graph(_, #gs_push_graph{} = Message) ->
    #gs_push_graph{
        gri = GRI, change_type = UpdateType, data = Data
    } = Message,
    #{
        <<"gri">> => gri_to_string(GRI),
        <<"updateType">> => update_type_to_string(UpdateType),
        <<"data">> => undefined_to_null(Data)
    }.


-spec encode_push_nosub(protocol_version(), nosub_push()) -> json_map().
encode_push_nosub(_, #gs_push_nosub{} = Message) ->
    #gs_push_nosub{
        gri = GRI, auth_hint = AuthHint, reason = Reason
    } = Message,
    #{
        <<"gri">> => gri_to_string(GRI),
        <<"authHint">> => auth_hint_to_json(AuthHint),
        <<"reason">> => nosub_reason_to_json(Reason)
    }.


-spec encode_push_error(protocol_version(), error_push()) -> json_map().
encode_push_error(?BASIC_PROTOCOL, Message) ->
    % Error push message may be sent before negotiating handshake, so it must
    % support the basic protocol (currently the same as 1. protocol version).
    encode_push_error(2, Message);
encode_push_error(ProtocolVersion, #gs_push_error{} = Message) ->
    #gs_push_error{
        error = Error
    } = Message,
    #{
        <<"error">> => gs_protocol_errors:error_to_json(ProtocolVersion, Error)
    }.


-spec decode_request(protocol_version(), json_map()) -> req_wrapper().
decode_request(ProtocolVersion, ReqJSON) ->
    PayloadJSON = maps:get(<<"payload">>, ReqJSON),
    Subtype = string_to_subtype(maps:get(<<"subtype">>, ReqJSON)),
    AuthOverride = maps:get(<<"authOverride">>, ReqJSON, null),
    Request = case Subtype of
        handshake ->
            decode_request_handshake(ProtocolVersion, PayloadJSON);
        rpc ->
            decode_request_rpc(ProtocolVersion, PayloadJSON);
        graph ->
            decode_request_graph(ProtocolVersion, PayloadJSON);
        unsub ->
            decode_request_unsub(ProtocolVersion, PayloadJSON)

    end,
    #gs_req{
        id = maps:get(<<"id">>, ReqJSON),
        subtype = Subtype,
        auth_override = json_to_aut_override(AuthOverride),
        request = Request
    }.


-spec decode_request_handshake(protocol_version(), json_map()) -> handshake_req().
decode_request_handshake(_, PayloadJSON) ->
    % Handshake messages do not change regardless of the protocol version
    SessionId = maps:get(<<"sessionId">>, PayloadJSON, null),
    SupportedVersions = maps:get(<<"supportedVersions">>, PayloadJSON),
    #gs_req_handshake{
        supported_versions = SupportedVersions,
        session_id = null_to_undefined(SessionId)
    }.


-spec decode_request_rpc(protocol_version(), json_map()) -> rpc_req().
decode_request_rpc(_, PayloadJSON) ->
    #gs_req_rpc{
        function = maps:get(<<"function">>, PayloadJSON),
        args = maps:get(<<"args">>, PayloadJSON)
    }.


-spec decode_request_graph(protocol_version(), json_map()) -> graph_req().
decode_request_graph(_, PayloadJSON) ->
    #gs_req_graph{
        gri = string_to_gri(maps:get(<<"gri">>, PayloadJSON)),
        operation = string_to_operation(maps:get(<<"operation">>, PayloadJSON)),
        data = null_to_undefined(maps:get(<<"data">>, PayloadJSON, #{})),
        subscribe = maps:get(<<"subscribe">>, PayloadJSON, false),
        auth_hint = json_to_auth_hint(maps:get(<<"authHint">>, PayloadJSON, null))
    }.


-spec decode_request_unsub(protocol_version(), json_map()) -> unsub_req().
decode_request_unsub(_, PayloadJSON) ->
    #gs_req_unsub{
        gri = string_to_gri(maps:get(<<"gri">>, PayloadJSON))
    }.


-spec decode_response(protocol_version(), json_map()) -> resp_wrapper().
decode_response(ProtocolVersion, ReqJSON) ->
    PayloadJSON = maps:get(<<"payload">>, ReqJSON),
    Success = maps:get(<<"success">>, PayloadJSON),
    Error = gs_protocol_errors:json_to_error(
        ProtocolVersion, maps:get(<<"error">>, PayloadJSON, null)
    ),
    DataJSON = maps:get(<<"data">>, PayloadJSON, #{}),
    Subtype = string_to_subtype(maps:get(<<"subtype">>, ReqJSON)),
    Response = case Success of
        false ->
            undefined;
        true ->
            case Subtype of
                handshake ->
                    decode_response_handshake(ProtocolVersion, DataJSON);
                rpc ->
                    decode_response_rpc(ProtocolVersion, DataJSON);
                graph ->
                    decode_response_graph(ProtocolVersion, DataJSON);
                unsub ->
                    decode_response_unsub(ProtocolVersion, DataJSON)
            end
    end,
    #gs_resp{
        id = maps:get(<<"id">>, ReqJSON),
        subtype = Subtype,
        success = Success,
        error = Error,
        response = Response
    }.


-spec decode_response_handshake(protocol_version(), json_map()) -> handshake_resp().
decode_response_handshake(_, DataJSON) ->
    % Handshake messages do not change regardless of the protocol version
    Version = maps:get(<<"version">>, DataJSON),
    SessionId = maps:get(<<"sessionId">>, DataJSON),
    Identity = maps:get(<<"identity">>, DataJSON),
    Attributes = maps:get(<<"attributes">>, DataJSON, #{}),
    #gs_resp_handshake{
        version = Version,
        session_id = null_to_undefined(SessionId),
        identity = json_to_identity(Identity),
        attributes = null_to_undefined(Attributes)
    }.


-spec decode_response_rpc(protocol_version(), json_map()) -> rpc_resp().
decode_response_rpc(_, DataJSON) ->
    #gs_resp_rpc{
        result = null_to_undefined(DataJSON)
    }.


-spec decode_response_graph(protocol_version(), json_map()) -> graph_resp().
decode_response_graph(1, DataJSON) ->
    case DataJSON of
        null ->
            #gs_resp_graph{};
        Map when map_size(Map) == 0 ->
            #gs_resp_graph{};
        #{<<"data">> := Data} ->
            #gs_resp_graph{
                data_format = value,
                data = Data
            };
        #{<<"gri">> := _} ->
            #gs_resp_graph{
                data_format = resource,
                data = DataJSON
            }
    end;
decode_response_graph(2, null) ->
    #gs_resp_graph{};
decode_response_graph(2, Map) when map_size(Map) == 0 ->
    #gs_resp_graph{};
decode_response_graph(2, DataJSON) ->
    FormatStr = maps:get(<<"format">>, DataJSON),
    #gs_resp_graph{
        data_format = str_to_data_format(FormatStr),
        data = maps:get(FormatStr, DataJSON)
    }.


-spec decode_response_unsub(protocol_version(), json_map()) -> unsub_resp().
decode_response_unsub(_, _DataJSON) ->
    % Currently the response does not carry any information
    #gs_resp_unsub{}.



-spec decode_push(protocol_version(), json_map()) -> push_wrapper().
decode_push(ProtocolVersion, ReqJSON) ->
    PayloadJSON = maps:get(<<"payload">>, ReqJSON),
    Subtype = string_to_subtype(maps:get(<<"subtype">>, ReqJSON)),
    Message = case Subtype of
        graph ->
            decode_push_graph(ProtocolVersion, PayloadJSON);
        nosub ->
            decode_push_nosub(ProtocolVersion, PayloadJSON);
        error ->
            decode_push_error(ProtocolVersion, PayloadJSON)
    end,
    #gs_push{
        subtype = Subtype, message = Message
    }.


-spec decode_push_graph(protocol_version(), json_map()) -> graph_push().
decode_push_graph(_, PayloadJSON) ->
    GRI = string_to_gri(maps:get(<<"gri">>, PayloadJSON)),
    UpdateType = string_to_update_type(maps:get(<<"updateType">>, PayloadJSON)),
    Data = null_to_undefined(maps:get(<<"data">>, PayloadJSON, #{})),
    #gs_push_graph{
        gri = GRI,
        change_type = UpdateType,
        data = null_to_undefined(Data)
    }.


-spec decode_push_nosub(protocol_version(), json_map()) -> nosub_push().
decode_push_nosub(_, PayloadJSON) ->
    GRI = maps:get(<<"gri">>, PayloadJSON),
    Reason = maps:get(<<"reason">>, PayloadJSON),
    AuthHint = maps:get(<<"authHint">>, PayloadJSON, null),
    #gs_push_nosub{
        gri = string_to_gri(GRI),
        auth_hint = json_to_auth_hint(AuthHint),
        reason = json_to_nosub_reason(Reason)
    }.


-spec decode_push_error(protocol_version(), json_map()) -> error_push().
decode_push_error(?BASIC_PROTOCOL, PayloadJSON) ->
    % Error push message may be sent before negotiating handshake, so it must
    % support the basic protocol (currently the same as 1. protocol version).
    decode_push_error(2, PayloadJSON);
decode_push_error(ProtocolVersion, PayloadJSON) ->
    Error = maps:get(<<"error">>, PayloadJSON),
    #gs_push_error{
        error = gs_protocol_errors:json_to_error(ProtocolVersion, Error)
    }.


-spec subtype_to_string(message_subtype()) -> binary().
subtype_to_string(handshake) -> <<"handshake">>;
subtype_to_string(rpc) -> <<"rpc">>;
subtype_to_string(graph) -> <<"graph">>;
subtype_to_string(unsub) -> <<"unsub">>;
subtype_to_string(nosub) -> <<"nosub">>;
subtype_to_string(error) -> <<"error">>.


-spec string_to_subtype(binary()) -> message_subtype().
string_to_subtype(<<"handshake">>) -> handshake;
string_to_subtype(<<"rpc">>) -> rpc;
string_to_subtype(<<"graph">>) -> graph;
string_to_subtype(<<"unsub">>) -> unsub;
string_to_subtype(<<"nosub">>) -> nosub;
string_to_subtype(<<"error">>) -> error.


-spec json_to_aut_override(null | json_map()) -> undefined | auth_override().
json_to_aut_override(null) -> undefined;
json_to_aut_override(#{<<"token">> := Token}) -> {token, Token};
json_to_aut_override(#{<<"basic">> := UserPasswdB64}) ->
    {basic, UserPasswdB64};
json_to_aut_override(#{
    <<"macaroon">> := Macaroon,
    <<"discharge-macaroons">> := DischargeMacaroons}
) ->
    {macaroon, Macaroon, DischargeMacaroons}.


-spec auth_override_to_json(undefined | auth_override()) -> null | json_map().
auth_override_to_json(undefined) -> null;
auth_override_to_json({token, Token}) -> #{<<"token">> => Token};
auth_override_to_json({basic, UserPasswdB64}) ->
    #{<<"basic">> => UserPasswdB64};
auth_override_to_json({macaroon, Macaroon, DischargeMacaroons}) -> #{
    <<"macaroon">> => Macaroon,
    <<"discharge-macaroons">> => DischargeMacaroons
}.


-spec operation_to_string(operation()) -> binary().
operation_to_string(create) -> <<"create">>;
operation_to_string(get) -> <<"get">>;
operation_to_string(update) -> <<"update">>;
operation_to_string(delete) -> <<"delete">>.


-spec string_to_operation(binary()) -> operation().
string_to_operation(<<"create">>) -> create;
string_to_operation(<<"get">>) -> get;
string_to_operation(<<"update">>) -> update;
string_to_operation(<<"delete">>) -> delete.


-spec id_to_string(undefined | binary()) -> binary().
id_to_string(undefined) -> <<"null">>;
id_to_string(?SELF) -> <<"self">>;
id_to_string(Bin) -> Bin.


-spec string_to_id(binary()) -> undefined | binary().
string_to_id(<<"null">>) -> undefined;
string_to_id(<<"self">>) -> ?SELF;
string_to_id(Bin) -> Bin.


-spec aspect_to_string(aspect()) -> binary().
aspect_to_string(Aspect) ->
    case Aspect of
        {A, B} -> <<(atom_to_binary(A, utf8))/binary, ",", B/binary>>;
        A -> atom_to_binary(A, utf8)
    end.


-spec string_to_aspect(binary()) -> aspect().
string_to_aspect(String) ->
    case binary:split(String, <<",">>, [global]) of
        [A] -> binary_to_existing_atom(A, utf8);
        [A, B] -> {binary_to_existing_atom(A, utf8), B}
    end.


-spec scope_to_string(scope()) -> binary().
scope_to_string(private) -> <<"private">>;
scope_to_string(protected) -> <<"protected">>;
scope_to_string(shared) -> <<"shared">>;
scope_to_string(public) -> <<"public">>;
scope_to_string(auto) -> <<"auto">>.


-spec string_to_scope(binary()) -> scope().
string_to_scope(<<"private">>) -> private;
string_to_scope(<<"protected">>) -> protected;
string_to_scope(<<"shared">>) -> shared;
string_to_scope(<<"public">>) -> public;
string_to_scope(<<"auto">>) -> auto.


-spec auth_hint_to_json(undefined | auth_hint()) -> null | json_map().
auth_hint_to_json(undefined) -> null;
auth_hint_to_json(?THROUGH_USER(UserId)) -> <<"throughUser:", UserId/binary>>;
auth_hint_to_json(?THROUGH_GROUP(GroupId)) ->
    <<"throughGroup:", GroupId/binary>>;
auth_hint_to_json(?THROUGH_SPACE(SpaceId)) ->
    <<"throughSpace:", SpaceId/binary>>;
auth_hint_to_json(?THROUGH_PROVIDER(ProvId)) ->
    <<"throughProvider:", ProvId/binary>>;
auth_hint_to_json(?THROUGH_HANDLE_SERVICE(HSId)) ->
    <<"throughHandleService:", HSId/binary>>;
auth_hint_to_json(?THROUGH_HANDLE(HandleId)) ->
    <<"throughHandle:", HandleId/binary>>;
auth_hint_to_json(?THROUGH_CLUSTER(ClusterId)) ->
    <<"throughCluster:", ClusterId/binary>>;
auth_hint_to_json(?AS_USER(UserId)) -> <<"asUser:", UserId/binary>>;
auth_hint_to_json(?AS_GROUP(GroupId)) -> <<"asGroup:", GroupId/binary>>.


-spec json_to_auth_hint(null | json_map()) -> undefined | auth_hint().
json_to_auth_hint(null) -> undefined;
json_to_auth_hint(<<"throughUser:", UserId/binary>>) -> ?THROUGH_USER(UserId);
json_to_auth_hint(<<"throughGroup:", GroupId/binary>>) ->
    ?THROUGH_GROUP(GroupId);
json_to_auth_hint(<<"throughSpace:", SpaceId/binary>>) ->
    ?THROUGH_SPACE(SpaceId);
json_to_auth_hint(<<"throughProvider:", ProvId/binary>>) ->
    ?THROUGH_PROVIDER(ProvId);
json_to_auth_hint(<<"throughHandleService:", HSId/binary>>) ->
    ?THROUGH_HANDLE_SERVICE(HSId);
json_to_auth_hint(<<"throughHandle:", HandleId/binary>>) ->
    ?THROUGH_HANDLE(HandleId);
json_to_auth_hint(<<"throughCluster:", ClusterId/binary>>) ->
    ?THROUGH_CLUSTER(ClusterId);
json_to_auth_hint(<<"asUser:", UserId/binary>>) -> ?AS_USER(UserId);
json_to_auth_hint(<<"asGroup:", GroupId/binary>>) -> ?AS_GROUP(GroupId).


-spec identity_to_json(identity()) -> binary() | json_map().
identity_to_json(nobody) -> <<"nobody">>;
identity_to_json({user, UserId}) -> #{<<"user">> => UserId};
identity_to_json({provider, ProviderId}) -> #{<<"provider">> => ProviderId}.


-spec json_to_identity(binary() | json_map()) -> identity().
json_to_identity(<<"nobody">>) -> nobody;
json_to_identity(#{<<"user">> := UserId}) -> {user, UserId};
json_to_identity(#{<<"provider">> := ProviderId}) -> {provider, ProviderId}.


-spec nosub_reason_to_json(nosub_reason()) -> binary().
nosub_reason_to_json(forbidden) -> <<"forbidden">>.


-spec json_to_nosub_reason(binary()) -> nosub_reason().
json_to_nosub_reason(<<"forbidden">>) -> forbidden.


-spec update_type_to_string(change_type()) -> binary().
update_type_to_string(updated) -> <<"updated">>;
update_type_to_string(deleted) -> <<"deleted">>.


-spec string_to_update_type(binary()) -> change_type().
string_to_update_type(<<"updated">>) -> updated;
string_to_update_type(<<"deleted">>) -> deleted.


-spec data_format_to_str(atom()) -> binary().
data_format_to_str(resource) -> <<"resource">>;
data_format_to_str(value) -> <<"value">>.


-spec str_to_data_format(binary()) -> atom().
str_to_data_format(<<"resource">>) -> resource;
str_to_data_format(<<"value">>) -> value.
