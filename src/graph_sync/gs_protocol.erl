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

-include("graph_sync/graph_sync.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/aai/aai.hrl").
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
-type protocol_version() :: non_neg_integer().
% Graph Sync session id, used as reference to store subscriptions data
-type session_id() :: gs_session:key().
% Optional, arbitrary attributes that can be sent by the sever with successful
% handshake response.
-type handshake_attributes() :: undefined | json_map().

% Denotes type of message so the payload can be properly interpreted
-type message_type() :: request | response | push.
% Denotes subtype of message so the payload can be properly interpreted
-type message_subtype() :: handshake | rpc | graph | unsub | nosub | error.

% Clients authorization, used during handshake or auth override.
% Special 'nobody' auth can be used to indicate that the client is requesting
% public access (typically works the same as not providing any auth, but might
% be subject to server's implementation).
-type client_auth() :: undefined | nobody | {token, tokens:serialized()}.

% Used to override the client authorization established on connection level, per
% request. Can be used by providers to authorize a certain request with a user's
% token, while still using the Graph Sync channel that was opened with provider's auth.
% PeerIp is used to indicate the IP address of the client on behalf of whom the
% channel owner is authorizing.
% Interface can be passed to indicate the interface of the Oneprovider to which
% the user has connected.
% Auth override can only be specified if the owner of the GS channel is
% an op-worker or op-panel service.
-type auth_override() :: undefined | #auth_override{}.

% Possible entity types
-type entity_type() :: atom().
-type entity_id() :: undefined | binary().
% Aspect of given entity, one of resource identifiers
-type aspect() :: atom() | {atom(), term()}.
% Scope of given aspect, allows to differentiate access to subsets of aspect data
% 'auto' scope means the maximum scope (if any) the client is authorized to access.
-type scope() :: private | protected | shared | public | auto.
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
    throughHandleService | throughHandle | throughHarvester | throughCluster |
    asUser | asGroup | asSpace | asHarvester,
    EntityId :: binary()
}.
% Generic term representing an entity in the system. If prefetched, it can be
% passed to gs_logic_plugin to speed up request handling.
-type entity() :: undefined | term().
% Revision of the entity - rises strictly monotonically with every modification
-type revision() :: pos_integer().
-type versioned_entity() :: {entity(), revision()}.

-type change_type() :: updated | deleted.
-type nosub_reason() :: forbidden.

% Function identifier in RPC
-type rpc_function() :: binary().
% Arguments map in RPC
-type rpc_args() :: json_map().

-type rpc_result() :: {ok, data()} | {error, term()}.

-type graph_create_result() :: ok | {ok, value, term()} |
{ok, resource, {gri:gri(), {term(), revision()}} | {gri:gri(), auth_hint(), {term(), revision()}}} |
errors:error().
-type graph_get_result() :: {ok, {term(), revision()}} |
{ok, gri:gri(), {term(), revision()}} | {ok, value, term()} | errors:error().
-type graph_delete_result() :: ok | {ok, value, term()} | errors:error().
-type graph_update_result() :: ok | errors:error().

-type graph_request_result() :: graph_create_result() | graph_get_result() |
graph_update_result() | graph_delete_result().

-type json_map() :: #{binary() => json_utils:json_term()}.

-export_type([
    message_id/0,
    protocol_version/0,
    session_id/0,
    handshake_attributes/0,
    message_type/0,
    message_subtype/0,
    auth_override/0,
    entity_type/0,
    entity_id/0,
    aspect/0,
    scope/0,
    operation/0,
    data_format/0,
    data/0,
    auth_hint/0,
    entity/0,
    revision/0,
    versioned_entity/0,
    change_type/0,
    nosub_reason/0,
    rpc_function/0,
    rpc_args/0,
    rpc_result/0,
    graph_create_result/0,
    graph_get_result/0,
    graph_delete_result/0,
    graph_update_result/0,
    graph_request_result/0,
    json_map/0,
    client_auth/0
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

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of supported protocol versions by this version of module.
%% @end
%%--------------------------------------------------------------------
-spec supported_versions() -> [protocol_version()].
supported_versions() -> ?SUPPORTED_PROTO_VERSIONS.


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
    {ok, json_map()} | errors:error().
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
    {ok, req_wrapper() | resp_wrapper() | push_wrapper()} | errors:error().
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
-spec generate_success_response(req_wrapper(), resp()) -> resp_wrapper().
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
-spec generate_error_response(req_wrapper(), errors:error()) ->
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
-spec generate_error_push_message(errors:error()) -> push_wrapper().
generate_error_push_message(Error) ->
    #gs_push{
        subtype = error, message = #gs_push_error{
            error = Error
        }}.


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
        <<"authOverride">> => auth_override_to_json(ProtocolVersion, AuthOverride),
        <<"payload">> => Payload
    }.


-spec encode_request_handshake(protocol_version(), handshake_req()) -> json_map().
encode_request_handshake(_, #gs_req_handshake{} = Req) ->
    % Handshake messages do not change regardless of the protocol version
    #gs_req_handshake{
        supported_versions = SupportedVersions, auth = Auth, session_id = SessionId
    } = Req,
    #{
        <<"supportedVersions">> => SupportedVersions,
        <<"auth">> => client_auth_to_json(Auth),
        <<"sessionId">> => utils:undefined_to_null(SessionId)
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
        <<"gri">> => gri:serialize(GRI),
        <<"operation">> => operation_to_string(Operation),
        <<"data">> => utils:undefined_to_null(Data),
        <<"subscribe">> => Subscribe,
        <<"authHint">> => auth_hint_to_json(AuthHint)
    }.


-spec encode_request_unsub(protocol_version(), unsub_req()) -> json_map().
encode_request_unsub(_, #gs_req_unsub{} = Req) ->
    #gs_req_unsub{
        gri = GRI
    } = Req,
    #{
        <<"gri">> => gri:serialize(GRI)
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
            <<"error">> => errors:to_json(Error),
            <<"data">> => utils:undefined_to_null(Data)
        }
    }.


-spec encode_response_handshake(protocol_version(), handshake_resp()) -> json_map().
encode_response_handshake(ProtocolVersion, #gs_resp_handshake{} = Resp) ->
    % Handshake messages do not change regardless of the protocol version
    #gs_resp_handshake{
        version = Version, session_id = SessionId,
        identity = Identity, attributes = Attributes
    } = Resp,
    #{
        <<"version">> => Version,
        <<"sessionId">> => SessionId,
        <<"identity">> => case ProtocolVersion >= 4 of
            true -> aai:serialize_subject(Identity);
            false -> deprecated_subject_to_json(Identity)
        end,
        <<"attributes">> => utils:undefined_to_null(Attributes)
    }.


-spec encode_response_rpc(protocol_version(), rpc_resp()) -> json_map().
encode_response_rpc(_, #gs_resp_rpc{} = Resp) ->
    #gs_resp_rpc{
        result = Result
    } = Resp,
    utils:undefined_to_null(Result).


-spec encode_response_graph(protocol_version(), graph_resp()) -> json_map().
encode_response_graph(_, #gs_resp_graph{data_format = undefined}) ->
    undefined;
encode_response_graph(_, #gs_resp_graph{data_format = Format, data = Result}) ->
    FormatStr = data_format_to_str(Format),
    #{
        <<"format">> => FormatStr,
        FormatStr => utils:undefined_to_null(Result)
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
            encode_push_error(Message)
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
        <<"gri">> => gri:serialize(GRI),
        <<"updateType">> => update_type_to_str(UpdateType),
        <<"data">> => utils:undefined_to_null(Data)
    }.


-spec encode_push_nosub(protocol_version(), nosub_push()) -> json_map().
encode_push_nosub(_, #gs_push_nosub{} = Message) ->
    #gs_push_nosub{
        gri = GRI, auth_hint = AuthHint, reason = Reason
    } = Message,
    #{
        <<"gri">> => gri:serialize(GRI),
        <<"authHint">> => auth_hint_to_json(AuthHint),
        <<"reason">> => nosub_reason_to_str(Reason)
    }.


-spec encode_push_error(error_push()) -> json_map().
encode_push_error(#gs_push_error{error = Error}) ->
    #{
        <<"error">> => errors:to_json(Error)
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
        auth_override = json_to_auth_override(ProtocolVersion, AuthOverride),
        request = Request
    }.


-spec decode_request_handshake(protocol_version(), json_map()) -> handshake_req().
decode_request_handshake(_, PayloadJSON) ->
    % Handshake messages do not change regardless of the protocol version
    SessionId = maps:get(<<"sessionId">>, PayloadJSON, null),
    Auth = maps:get(<<"auth">>, PayloadJSON, null),
    SupportedVersions = maps:get(<<"supportedVersions">>, PayloadJSON),
    #gs_req_handshake{
        supported_versions = SupportedVersions,
        auth = json_to_client_auth(Auth),
        session_id = utils:null_to_undefined(SessionId)
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
        gri = gri:deserialize(maps:get(<<"gri">>, PayloadJSON)),
        operation = string_to_operation(maps:get(<<"operation">>, PayloadJSON)),
        data = utils:null_to_undefined(maps:get(<<"data">>, PayloadJSON, #{})),
        subscribe = maps:get(<<"subscribe">>, PayloadJSON, false),
        auth_hint = json_to_auth_hint(maps:get(<<"authHint">>, PayloadJSON, null))
    }.


-spec decode_request_unsub(protocol_version(), json_map()) -> unsub_req().
decode_request_unsub(_, PayloadJSON) ->
    #gs_req_unsub{
        gri = gri:deserialize(maps:get(<<"gri">>, PayloadJSON))
    }.


-spec decode_response(protocol_version(), json_map()) -> resp_wrapper().
decode_response(ProtocolVersion, ReqJSON) ->
    PayloadJSON = maps:get(<<"payload">>, ReqJSON),
    Success = maps:get(<<"success">>, PayloadJSON),
    Error = errors:from_json(maps:get(<<"error">>, PayloadJSON, null)),
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
decode_response_handshake(ProtocolVersion, DataJSON) ->
    % Handshake messages do not change regardless of the protocol version
    Version = maps:get(<<"version">>, DataJSON),
    SessionId = maps:get(<<"sessionId">>, DataJSON),
    Identity = maps:get(<<"identity">>, DataJSON),
    Attributes = maps:get(<<"attributes">>, DataJSON, #{}),
    #gs_resp_handshake{
        version = Version,
        session_id = utils:null_to_undefined(SessionId),
        identity = case ProtocolVersion >= 4 of
            true -> aai:deserialize_subject(Identity);
            false -> deprecated_json_to_subject(Identity)
        end,
        attributes = utils:null_to_undefined(Attributes)
    }.


-spec decode_response_rpc(protocol_version(), json_map()) -> rpc_resp().
decode_response_rpc(_, DataJSON) ->
    #gs_resp_rpc{
        result = utils:null_to_undefined(DataJSON)
    }.


-spec decode_response_graph(protocol_version(), json_map()) -> graph_resp().
decode_response_graph(_, null) ->
    #gs_resp_graph{};
decode_response_graph(_, Map) when map_size(Map) == 0 ->
    #gs_resp_graph{};
decode_response_graph(_, DataJSON) ->
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
            decode_push_error(PayloadJSON)
    end,
    #gs_push{
        subtype = Subtype, message = Message
    }.


-spec decode_push_graph(protocol_version(), json_map()) -> graph_push().
decode_push_graph(_, PayloadJSON) ->
    GRI = gri:deserialize(maps:get(<<"gri">>, PayloadJSON)),
    UpdateType = str_to_update_type(maps:get(<<"updateType">>, PayloadJSON)),
    Data = utils:null_to_undefined(maps:get(<<"data">>, PayloadJSON, #{})),
    #gs_push_graph{
        gri = GRI,
        change_type = UpdateType,
        data = utils:null_to_undefined(Data)
    }.


-spec decode_push_nosub(protocol_version(), json_map()) -> nosub_push().
decode_push_nosub(_, PayloadJSON) ->
    GRI = maps:get(<<"gri">>, PayloadJSON),
    Reason = maps:get(<<"reason">>, PayloadJSON),
    AuthHint = maps:get(<<"authHint">>, PayloadJSON, null),
    #gs_push_nosub{
        gri = gri:deserialize(GRI),
        auth_hint = json_to_auth_hint(AuthHint),
        reason = str_to_nosub_reason(Reason)
    }.


-spec decode_push_error(json_map()) -> error_push().
decode_push_error(#{<<"error">> := Error}) ->
    #gs_push_error{
        error = errors:from_json(Error)
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


-spec json_to_client_auth(null | json_map() | binary()) -> client_auth().
json_to_client_auth(null) ->
    undefined;
json_to_client_auth(<<"nobody">>) ->
    nobody;
json_to_client_auth(#{<<"token">> := Token}) ->
    {token, Token};
%% @todo VFS-5554 Deprecated, kept for backward compatibility
json_to_client_auth(#{<<"macaroon">> := Token}) ->
    {token, Token}.


-spec client_auth_to_json(client_auth()) -> null | json_map() | binary().
client_auth_to_json(undefined) ->
    null;
client_auth_to_json(nobody) ->
    <<"nobody">>;
client_auth_to_json({token, Token}) -> #{
    <<"token">> => Token,
%% @todo VFS-5554 Deprecated, kept for backward compatibility
    <<"macaroon">> => Token
}.


-spec json_to_auth_override(protocol_version(), null | json_map() | binary()) -> auth_override().
json_to_auth_override(_, null) ->
    undefined;
json_to_auth_override(3, Data) ->
    #auth_override{
        client_auth = json_to_client_auth(Data),
        peer_ip = undefined,
        interface = undefined,
        consumer_token = undefined,
        data_access_caveats_policy = disallow_data_access_caveats
    };
json_to_auth_override(_, #{<<"clientAuth">> := ClientAuth} = Data) ->
    #auth_override{
        client_auth = json_to_client_auth(ClientAuth),
        peer_ip = case maps:get(<<"peerIp">>, Data, null) of
            null -> undefined;
            IpBin -> element(2, {ok, _} = ip_utils:to_ip4_address(IpBin))
        end,
        interface = case maps:get(<<"interface">>, Data, null) of
            null -> undefined;
            Interface -> cv_interface:deserialize_interface(Interface)
        end,
        consumer_token = utils:null_to_undefined(maps:get(<<"consumerToken">>, Data, null)),
        data_access_caveats_policy = case maps:get(<<"dataAccessCaveatsPolicy">>, Data, null) of
            null -> disallow_data_access_caveats;
            Bin -> str_to_data_access_caveats_policy(Bin)
        end
    }.


-spec auth_override_to_json(protocol_version(), auth_override()) -> null | json_map() | binary().
auth_override_to_json(_, undefined) ->
    null;
auth_override_to_json(3, #auth_override{client_auth = ClientAuth}) ->
    client_auth_to_json(ClientAuth);
auth_override_to_json(_, #auth_override{} = AuthOverride) ->
    #{
        <<"clientAuth">> => client_auth_to_json(AuthOverride#auth_override.client_auth),
        <<"peerIp">> => case AuthOverride#auth_override.peer_ip of
            undefined -> null;
            PeerIp -> element(2, {ok, _} = ip_utils:to_binary(PeerIp))
        end,
        <<"interface">> => case AuthOverride#auth_override.interface of
            undefined -> null;
            Interface -> cv_interface:serialize_interface(Interface)
        end,
        <<"consumerToken">> => utils:undefined_to_null(AuthOverride#auth_override.consumer_token),
        <<"dataAccessCaveatsPolicy">> => data_access_caveats_policy_to_str(
            AuthOverride#auth_override.data_access_caveats_policy
        )
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


-spec auth_hint_to_json(undefined | auth_hint()) -> null | json_map().
auth_hint_to_json(undefined) -> null;
auth_hint_to_json(?THROUGH_USER(UserId)) -> <<"throughUser:", UserId/binary>>;
auth_hint_to_json(?THROUGH_GROUP(GroupId)) -> <<"throughGroup:", GroupId/binary>>;
auth_hint_to_json(?THROUGH_SPACE(SpaceId)) -> <<"throughSpace:", SpaceId/binary>>;
auth_hint_to_json(?THROUGH_PROVIDER(ProvId)) -> <<"throughProvider:", ProvId/binary>>;
auth_hint_to_json(?THROUGH_HANDLE_SERVICE(HSId)) -> <<"throughHandleService:", HSId/binary>>;
auth_hint_to_json(?THROUGH_HANDLE(HandleId)) -> <<"throughHandle:", HandleId/binary>>;
auth_hint_to_json(?THROUGH_HARVESTER(HarvesterId)) -> <<"throughHarvester:", HarvesterId/binary>>;
auth_hint_to_json(?THROUGH_CLUSTER(ClusterId)) -> <<"throughCluster:", ClusterId/binary>>;
auth_hint_to_json(?AS_USER(UserId)) -> <<"asUser:", UserId/binary>>;
auth_hint_to_json(?AS_GROUP(GroupId)) -> <<"asGroup:", GroupId/binary>>;
auth_hint_to_json(?AS_SPACE(SpaceId)) -> <<"asSpace:", SpaceId/binary>>;
auth_hint_to_json(?AS_HARVESTER(HarvesterId)) -> <<"asHarvester:", HarvesterId/binary>>.


-spec json_to_auth_hint(null | json_map()) -> undefined | auth_hint().
json_to_auth_hint(null) -> undefined;
json_to_auth_hint(<<"throughUser:", UserId/binary>>) -> ?THROUGH_USER(UserId);
json_to_auth_hint(<<"throughGroup:", GroupId/binary>>) -> ?THROUGH_GROUP(GroupId);
json_to_auth_hint(<<"throughSpace:", SpaceId/binary>>) -> ?THROUGH_SPACE(SpaceId);
json_to_auth_hint(<<"throughProvider:", ProvId/binary>>) -> ?THROUGH_PROVIDER(ProvId);
json_to_auth_hint(<<"throughHandleService:", HSId/binary>>) -> ?THROUGH_HANDLE_SERVICE(HSId);
json_to_auth_hint(<<"throughHandle:", HandleId/binary>>) -> ?THROUGH_HANDLE(HandleId);
json_to_auth_hint(<<"throughHarvester:", HarvesterId/binary>>) -> ?THROUGH_HARVESTER(HarvesterId);
json_to_auth_hint(<<"throughCluster:", ClusterId/binary>>) -> ?THROUGH_CLUSTER(ClusterId);
json_to_auth_hint(<<"asUser:", UserId/binary>>) -> ?AS_USER(UserId);
json_to_auth_hint(<<"asGroup:", GroupId/binary>>) -> ?AS_GROUP(GroupId);
json_to_auth_hint(<<"asSpace:", SpaceId/binary>>) -> ?AS_SPACE(SpaceId);
json_to_auth_hint(<<"asHarvester:", HarvesterId/binary>>) -> ?AS_HARVESTER(HarvesterId).


-spec nosub_reason_to_str(nosub_reason()) -> binary().
nosub_reason_to_str(forbidden) -> <<"forbidden">>.


-spec str_to_nosub_reason(binary()) -> nosub_reason().
str_to_nosub_reason(<<"forbidden">>) -> forbidden.


-spec update_type_to_str(change_type()) -> binary().
update_type_to_str(updated) -> <<"updated">>;
update_type_to_str(deleted) -> <<"deleted">>.


-spec str_to_update_type(binary()) -> change_type().
str_to_update_type(<<"updated">>) -> updated;
str_to_update_type(<<"deleted">>) -> deleted.


-spec data_format_to_str(atom()) -> binary().
data_format_to_str(resource) -> <<"resource">>;
data_format_to_str(value) -> <<"value">>.


-spec str_to_data_format(binary()) -> atom().
str_to_data_format(<<"resource">>) -> resource;
str_to_data_format(<<"value">>) -> value.


-spec data_access_caveats_policy_to_str(data_access_caveats:policy()) -> binary().
data_access_caveats_policy_to_str(disallow_data_access_caveats) -> <<"disallowDataAccessCaveats">>;
data_access_caveats_policy_to_str(allow_data_access_caveats) -> <<"allowDataAccessCaveats">>.


-spec str_to_data_access_caveats_policy(binary()) -> data_access_caveats:policy().
str_to_data_access_caveats_policy(<<"disallowDataAccessCaveats">>) -> disallow_data_access_caveats;
str_to_data_access_caveats_policy(<<"allowDataAccessCaveats">>) -> allow_data_access_caveats.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deprecated - used for backward compatibility with 19.02.* (proto version < 4).
%% @end
%%--------------------------------------------------------------------
-spec deprecated_subject_to_json(aai:subject()) -> json_utils:json_term().
deprecated_subject_to_json(?SUB(nobody)) -> <<"nobody">>;
% root subject must not have a representation outside of the application
deprecated_subject_to_json(?SUB(root)) -> <<"nobody">>;
deprecated_subject_to_json(?SUB(user, UserId)) -> #{<<"user">> => UserId};
deprecated_subject_to_json(?SUB(?ONEPROVIDER, PrId)) -> #{<<"provider">> => PrId}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deprecated - used for backward compatibility with 19.02.* (proto version < 4).
%% @end
%%--------------------------------------------------------------------
-spec deprecated_json_to_subject(json_utils:json_term()) -> aai:subject().
deprecated_json_to_subject(<<"nobody">>) -> ?SUB(nobody);
deprecated_json_to_subject(#{<<"user">> := UserId}) -> ?SUB(user, UserId);
deprecated_json_to_subject(#{<<"provider">> := PrId}) -> ?SUB(?ONEPROVIDER, PrId).