%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements Graph Sync server that is able to process requests
%%% and push updates to clients.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_server).
-author("Lukasz Opiola").

-include("graph_sync/graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").


% Identifier of connection process, one per session.
-type conn_ref() :: pid().
% Plugin module used to translate request results.
-type translator() :: module().

-export_type([conn_ref/0, translator/0]).

%% Simple cache used to remember calculated payloads per GRI and limit the
%% number of requests to GS_LOGIC_PLUGIN when there is a lot of subscribers
%% for the same records.
-type payload_cache() :: #{gri:gri() => {gs_protocol:data(), gs_protocol:revision()}}.


%% API
-export([handshake/5]).
-export([report_heartbeat/1]).
-export([cleanup_session/1, terminate_connection/1]).
-export([updated/3, deleted/2]).
-export([handle_request/2]).
% Functions returning plugin module names
-export([gs_logic_plugin_module/0]).

-define(GS_LOGIC_PLUGIN, (gs_logic_plugin_module())).

%%%===================================================================
%%% Plugin module names. Defined as functions instead of using it literally in code to prevent dialyzer warnings about
%%  unknown module, since the module exists only in apps having cluster_worker as a dependency.
%%%===================================================================

-spec gs_logic_plugin_module() -> module().
gs_logic_plugin_module() ->
    gs_logic_plugin.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Validates a handshake request and if it's correct, creates a new session.
%% Returns success or error handshake response depending on the outcome.
%% @end
%%--------------------------------------------------------------------
-spec handshake(conn_ref(), translator(), gs_protocol:req_wrapper(), ip_utils:ip(), gs_protocol:cookies()) ->
    {ok, gs_session:data(), gs_protocol:handshake_resp()} | errors:error().
handshake(ConnRef, Translator, Req, PeerIp, Cookies) ->
    ?catch_exceptions(handshake_internal(ConnRef, Translator, Req, PeerIp, Cookies)).

%% @private
-spec handshake_internal(conn_ref(), translator(), gs_protocol:req_wrapper(), ip_utils:ip(), gs_protocol:cookies()) ->
    {ok, gs_session:data(), gs_protocol:handshake_resp()} | errors:error().
handshake_internal(ConnRef, Translator, #gs_req{request = #gs_req_handshake{} = HReq}, PeerIp, Cookies) ->
    ?GS_LOGIC_PLUGIN:assert_service_available(),
    #gs_req_handshake{supported_versions = AuthVersions, auth = ClientAuth} = HReq,
    ServerVersions = gs_protocol:supported_versions(),
    case gs_protocol:greatest_common_version(AuthVersions, ServerVersions) of
        false ->
            ?ERROR_BAD_VERSION(ServerVersions);
        {true, Version} ->
            case ?GS_LOGIC_PLUGIN:verify_handshake_auth(ClientAuth, PeerIp, Cookies) of
                {error, _} = Error ->
                    Error;
                {ok, Auth} ->
                    SessionData = gs_persistence:create_session(Auth, ConnRef, Version, Translator),
                    ?GS_LOGIC_PLUGIN:client_connected(Auth, ConnRef),
                    Attributes = Translator:handshake_attributes(Auth),
                    {ok, SessionData, #gs_resp_handshake{
                        version = Version,
                        session_id = SessionData#gs_session.id,
                        identity = Auth#auth.subject,
                        attributes = Attributes
                    }}
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Called by the connection process on every heartbeat received from the client.
%% @end
%%--------------------------------------------------------------------
-spec report_heartbeat(gs_session:data()) -> ok.
report_heartbeat(#gs_session{auth = Auth, conn_ref = ConnRef}) ->
    ?GS_LOGIC_PLUGIN:client_heartbeat(Auth, ConnRef),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Cleans up all data related to given session, including its subscriptions.
%% @end
%%--------------------------------------------------------------------
-spec cleanup_session(gs_session:data()) -> ok.
cleanup_session(#gs_session{id = SessionId, auth = Auth, conn_ref = ConnRef}) ->
    ?GS_LOGIC_PLUGIN:client_disconnected(Auth, ConnRef),
    gs_persistence:delete_session(SessionId).


%%--------------------------------------------------------------------
%% @doc
%% Terminates a client connection by given connection ref.
%% @end
%%--------------------------------------------------------------------
-spec terminate_connection(conn_ref()) -> ok.
terminate_connection(ConnRef) ->
    gs_ws_handler:kill(ConnRef).


%%--------------------------------------------------------------------
%% @doc
%% Can be called to report that given entity has changed. Based on subscriptions
%% made by clients and subscribable resources for that entity, Graph Sync server
%% will broadcast 'updated' messages to interested clients.
%% @end
%%--------------------------------------------------------------------
-spec updated(gs_protocol:entity_type(), gs_protocol:entity_id(), gs_protocol:versioned_entity()) -> ok.
updated(EntityType, EntityId, VersionedEntity) ->
    EntitySubscribers = gs_persistence:get_entity_subscribers(EntityType, EntityId),
    maps:fold(fun
        ({_Aspect, _Scope}, [], PayloadCache) ->
            PayloadCache;
        ({Aspect, Scope}, Subscribers, PayloadCache) ->
            GRI = #gri{type = EntityType, id = EntityId, aspect = Aspect, scope = Scope},
            lists:foldl(fun(Subscriber, PayloadCacheAcc) ->
                push_updated(GRI, VersionedEntity, Subscriber, PayloadCacheAcc)
            end, PayloadCache, Subscribers)
    end, #{}, EntitySubscribers),
    ok.


%% @private
-spec push_updated(gri:gri(), gs_protocol:versioned_entity(), gs_persistence:subscriber(), payload_cache()) ->
    payload_cache().
push_updated(ReqGRI, VersionedEntity, {SessionId, {Auth, AuthHint}}, PayloadCache) ->
    case gs_persistence:get_session(SessionId) of
        {error, not_found} ->
            % possible when session cleanup is in progress
            PayloadCache;
        {ok, #gs_session{protocol_version = ProtoVer, conn_ref = ConnRef, translator = Translator}} ->
            case ?GS_LOGIC_PLUGIN:is_authorized(Auth, AuthHint, ReqGRI, get, VersionedEntity) of
                {true, ResGRI} ->
                    {Data, NewPayloadCache} = updated_payload(
                        ReqGRI, ResGRI, Translator, ProtoVer, Auth, VersionedEntity, PayloadCache
                    ),
                    gs_ws_handler:push(ConnRef, #gs_push{
                        subtype = graph, message = #gs_push_graph{
                            gri = ReqGRI, change_type = updated, data = Data
                        }}),
                    NewPayloadCache;
                false ->
                    gs_persistence:unsubscribe(SessionId, ReqGRI),
                    gs_ws_handler:push(ConnRef, #gs_push{
                        subtype = nosub, message = #gs_push_nosub{
                            gri = ReqGRI, auth_hint = AuthHint, reason = forbidden
                        }
                    }),
                    PayloadCache
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Can be called to report that given entity has been deleted. Based on subscriptions
%% made by clients and subscribable resources for that entity, Graph Sync server
%% will broadcast 'deleted' messages to interested clients.
%% @end
%%--------------------------------------------------------------------
-spec deleted(gs_protocol:entity_type(), gs_protocol:entity_id()) -> ok.
deleted(EntityType, EntityId) ->
    EntitySubscribers = gs_persistence:get_entity_subscribers(EntityType, EntityId),
    maps:map(fun
        ({_Aspect, _Scope}, []) ->
            ok;
        ({Aspect, Scope}, Subscribers) ->
            GRI = #gri{type = EntityType, id = EntityId, aspect = Aspect, scope = Scope},
            lists:foreach(fun({SessionId, _}) ->
                case gs_persistence:get_session(SessionId) of
                    {error, not_found} ->
                        % possible when session cleanup is in progress
                        ok;
                    {ok, #gs_session{conn_ref = ConnRef}} ->
                        gs_ws_handler:push(ConnRef, #gs_push{
                            subtype = graph, message = #gs_push_graph{
                                gri = GRI, change_type = deleted
                            }})
                end
            end, Subscribers),
            gs_persistence:remove_all_subscribers(GRI)
    end, EntitySubscribers),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Handles a request expressed by #gs_req{} record. Calls back to
%% gs_logic_plugin for application specific logic handling.
%% @end
%%--------------------------------------------------------------------
-spec handle_request(gs_session:data(), gs_protocol:req_wrapper() | gs_protocol:req()) ->
    {ok, gs_protocol:resp()} | errors:error().
handle_request(SessionData, Req) ->
    ?catch_exceptions(begin
        ?GS_LOGIC_PLUGIN:assert_service_available(),
        handle_request_internal(SessionData, Req)
    end).


%% @private
-spec handle_request_internal(gs_session:data(), gs_protocol:req_wrapper() | gs_protocol:req()) ->
    {ok, gs_protocol:resp()} | errors:error().
% No authorization override - unpack the gs_req record as it's context is
% no longer important.
handle_request_internal(SessionData, #gs_req{auth_override = undefined, request = Req}) ->
    handle_request_internal(SessionData, Req);

% This request has the authorization field specified, override the default
% authorization - but only allow this for op-worker and op-panel (?PROVIDER auth).
handle_request_internal(SessionData = #gs_session{auth = ?PROVIDER = Auth}, #gs_req{auth_override = AuthOverride} = Req) ->
    case ?GS_LOGIC_PLUGIN:verify_auth_override(Auth, AuthOverride) of
        {ok, OverridenAuth} ->
            handle_request_internal(
                SessionData#gs_session{auth = OverridenAuth},
                Req#gs_req{auth_override = undefined}
            );
        {error, _} = Error ->
            Error
    end;
handle_request_internal(#gs_session{auth = _Auth}, #gs_req{auth_override = _AuthOverride}) ->
    % Non-provider auth, disallow auth overrides
    ?ERROR_FORBIDDEN;


handle_request_internal(_Session, #gs_req_handshake{}) ->
    % Handshake is done in handshake/4 function
    ?ERROR_HANDSHAKE_ALREADY_DONE;

handle_request_internal(SessionData, #gs_req_rpc{} = Req) ->
    #gs_session{auth = Auth, protocol_version = ProtoVer} = SessionData,
    #gs_req_rpc{function = Function, args = Args} = Req,
    case ?GS_LOGIC_PLUGIN:handle_rpc(ProtoVer, Auth, Function, Args) of
        {ok, Result} ->
            {ok, #gs_resp_rpc{result = Result}};
        {error, _} = Error ->
            Error
    end;

handle_request_internal(SessionData, #gs_req_graph{gri = #gri{id = ?SELF} = GRI} = Req) ->
    #gs_session{auth = Auth} = SessionData,
    case {GRI#gri.type, Auth#auth.subject} of
        {od_user, ?SUB(user, UserId)} ->
            handle_request_internal(SessionData, Req#gs_req_graph{gri = GRI#gri{id = UserId}});
        {od_provider, ?SUB(?ONEPROVIDER, ProviderId)} ->
            handle_request_internal(SessionData, Req#gs_req_graph{gri = GRI#gri{id = ProviderId}});
        _ ->
            ?ERROR_NOT_FOUND
    end;

handle_request_internal(SessionData, #gs_req_graph{auth_hint = AuthHint = {_, ?SELF}} = Req) ->
    #gs_session{auth = Auth} = SessionData,
    case {AuthHint, Auth#auth.subject} of
        {?THROUGH_USER(?SELF), ?SUB(user, UserId)} ->
            handle_request_internal(SessionData, Req#gs_req_graph{auth_hint = ?THROUGH_USER(UserId)});
        {?AS_USER(?SELF), ?SUB(user, UserId)} ->
            handle_request_internal(SessionData, Req#gs_req_graph{auth_hint = ?AS_USER(UserId)});
        {?THROUGH_PROVIDER(?SELF), ?SUB(?ONEPROVIDER, ProviderId)} ->
            handle_request_internal(SessionData, Req#gs_req_graph{auth_hint = ?THROUGH_PROVIDER(ProviderId)});
        _ ->
            ?ERROR_FORBIDDEN
    end;

handle_request_internal(SessionData, #gs_req_graph{} = Req) ->
    #gs_session{
        id = SessionId, auth = Auth, protocol_version = ProtoVer,
        translator = Translator
    } = SessionData,
    #gs_req_graph{
        gri = RequestedGRI,
        operation = Operation,
        data = Data,
        auth_hint = AuthHint,
        subscribe = Subscribe
    } = Req,
    ?GS_LOGIC_PLUGIN:is_type_supported(RequestedGRI) orelse throw(?ERROR_BAD_GRI),
    case Subscribe of
        true ->
            case is_subscribable(Operation, RequestedGRI) of
                true -> ok;
                false -> throw(?ERROR_NOT_SUBSCRIBABLE)
            end;
        false ->
            ok
    end,
    Result = ?GS_LOGIC_PLUGIN:handle_graph_request(
        Auth, AuthHint, RequestedGRI, Operation, Data, {undefined, 1}
    ),
    case Result of
        {error, _} = Error -> throw(Error);
        _ -> ok
    end,
    {NewGRI, NewAuthHint, Response} = case {Operation, Result} of
        {create, ok} ->
            {not_subscribable, AuthHint, #gs_resp_graph{}};
        {create, {ok, value, Value}} ->
            {not_subscribable, AuthHint, #gs_resp_graph{
                data_format = value,
                data = translate_value(Translator, ProtoVer, RequestedGRI, Auth, Value)
            }};
        {create, {ok, resource, {ResultGRI, ResData}}} ->
            {coalesce_gri(RequestedGRI, ResultGRI), AuthHint, #gs_resp_graph{
                data_format = resource,
                data = translate_resource(Translator, ProtoVer, RequestedGRI, ResultGRI, Auth, ResData)
            }};
        {create, {ok, resource, {ResultGRI, NAuthHint, ResData}}} ->
            {coalesce_gri(RequestedGRI, ResultGRI), NAuthHint, #gs_resp_graph{
                data_format = resource,
                data = translate_resource(Translator, ProtoVer, RequestedGRI, ResultGRI, Auth, ResData)
            }};

        {get, {ok, ResData}} ->
            {RequestedGRI, AuthHint, #gs_resp_graph{
                data_format = resource,
                data = translate_resource(Translator, ProtoVer, RequestedGRI, Auth, ResData)
            }};
        {get, {ok, #gri{} = ResultGRI, ResData}} ->
            {RequestedGRI, AuthHint, #gs_resp_graph{
                data_format = resource,
                data = translate_resource(Translator, ProtoVer, RequestedGRI, ResultGRI, Auth, ResData)
            }};
        {get, {ok, value, Value}} ->
            {not_subscribable, AuthHint, #gs_resp_graph{
                data_format = value,
                data = translate_value(Translator, ProtoVer, RequestedGRI, Auth, Value)
            }};

        {update, ok} ->
            {RequestedGRI, AuthHint, #gs_resp_graph{}};

        {delete, ok} ->
            {not_subscribable, AuthHint, #gs_resp_graph{}};
        {delete, {ok, value, Value}} ->
            {not_subscribable, AuthHint, #gs_resp_graph{
                data_format = value,
                data = translate_value(Translator, ProtoVer, RequestedGRI, Auth, Value)
            }}
    end,
    case {Subscribe, NewAuthHint, NewGRI} of
        {true, _, not_subscribable} ->
            throw(?ERROR_NOT_SUBSCRIBABLE);
        {true, _, _} ->
            gs_persistence:subscribe(SessionId, NewGRI, Auth, NewAuthHint);
        {false, _, _} ->
            ok
    end,
    {ok, Response};

handle_request_internal(#gs_session{id = SessionId}, #gs_req_unsub{gri = GRI}) ->
    gs_persistence:unsubscribe(SessionId, GRI),
    {ok, #gs_resp_unsub{}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec translate_value(translator(), gs_protocol:protocol_version(), gri:gri(),
    aai:auth(), term()) -> gs_protocol:data() | errors:error().
translate_value(Translator, ProtoVer, GRI, Auth, Data) ->
    % Used only when data is returned rather than GRI and resource
    case Translator:translate_value(ProtoVer, GRI, Data) of
        Fun when is_function(Fun, 1) -> Fun(Auth);
        Result -> Result
    end.


%% @private
-spec translate_resource(translator(), gs_protocol:protocol_version(), gri:gri(),
    aai:auth(), {term(), gs_protocol:revision()}) -> gs_protocol:data() | errors:error().
translate_resource(Translator, ProtoVer, GRI, Auth, DatAndRev) ->
    translate_resource(Translator, ProtoVer, GRI, GRI, Auth, DatAndRev).

%% @private
-spec translate_resource(translator(), gs_protocol:protocol_version(),
    RequestedGRI :: gri:gri(), ResultGRI :: gri:gri(),
    aai:auth(), {term(), gs_protocol:revision()}) -> gs_protocol:data() | errors:error().
translate_resource(Translator, ProtoVer, RequestedGRI, ResultGRI, Auth, {Data, Revision}) ->
    Resp = case Translator:translate_resource(ProtoVer, ResultGRI, Data) of
        Fun when is_function(Fun, 1) -> Fun(Auth);
        Result -> Result
    end,
    %% GRI must be sent back with every GET response
    insert_gri_and_rev(RequestedGRI, ResultGRI, Resp, Revision).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is used to generate payload for the "updated" push message.
%% It uses a simple PayloadCache, which remembers past results between function
%% calls - it is useful to minimize the number of calls to GS_LOGIC_PLUGIN when
%% generating the same payload for many subscribers.
%% @end
%%--------------------------------------------------------------------
-spec updated_payload(ReqGRI :: gri:gri(), ResGRI :: gri:gri(),
    translator(), gs_protocol:protocol_version(), aai:auth(),
    gs_protocol:versioned_entity(), payload_cache()) -> {gs_protocol:data(), payload_cache()}.
updated_payload(ReqGRI, ResGRI, Translator, ProtoVer, Auth, VersionedEntity, PayloadCache) ->
    case maps:find({Translator, ResGRI}, PayloadCache) of
        {ok, {Fun, Revision}} when is_function(Fun, 1) ->
            % Function-type translator, must be evaluated per client
            {insert_gri_and_rev(ReqGRI, ResGRI, Fun(Auth), Revision), PayloadCache};
        {ok, {Result, Revision}} ->
            % Literal translator, the results can be reused for all clients
            {insert_gri_and_rev(ReqGRI, ResGRI, Result, Revision), PayloadCache};
        _ ->
            {ok, {GetResult, Revision}} = ?GS_LOGIC_PLUGIN:handle_graph_request(
                ?ROOT, undefined, ResGRI, get, #{}, VersionedEntity
            ),
            TranslateResult = Translator:translate_resource(ProtoVer, ResGRI, GetResult),
            Result = case TranslateResult of
                Fun when is_function(Fun, 1) -> Fun(Auth);
                DataJSONMap -> DataJSONMap
            end,
            % Cache the translator result for faster consecutive calls
            {
                insert_gri_and_rev(ReqGRI, ResGRI, Result, Revision),
                PayloadCache#{{Translator, ResGRI} => {TranslateResult, Revision}}
            }
    end.


%% @private
-spec insert_gri_and_rev(RequestedGRI :: gri:gri(), ResultGRI :: gri:gri(),
    gs_protocol:data(), gs_protocol:revision()) -> gs_protocol:data().
insert_gri_and_rev(RequestedGRI, ResultGRI, Data, Revision) ->
    Data#{
        <<"gri">> => gri:serialize(coalesce_gri(RequestedGRI, ResultGRI)),
        <<"revision">> => Revision
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if auto scope was requested - in this case overrides the scope in
%% result GRI to auto.
%% @end
%%--------------------------------------------------------------------
-spec coalesce_gri(RequestedGRI :: gri:gri(), ResultGRI :: gri:gri()) ->
    gri:gri().
coalesce_gri(#gri{scope = auto}, ResultGRI) ->
    ResultGRI#gri{scope = auto};
coalesce_gri(_RequestedGRI, ResultGRI) ->
    ResultGRI.


%% @private
-spec is_subscribable(gs_protocol:operation(), gri:gri()) -> boolean().
is_subscribable(Operation, GRI = #gri{scope = auto}) ->
    is_subscribable(Operation, GRI#gri{scope = private}) orelse
        is_subscribable(Operation, GRI#gri{scope = protected}) orelse
        is_subscribable(Operation, GRI#gri{scope = shared}) orelse
        is_subscribable(Operation, GRI#gri{scope = public});
is_subscribable(create, GRI = #gri{id = undefined}) ->
    % Newly created resources can be subscribed for depending on logic_plugin
    ?GS_LOGIC_PLUGIN:is_subscribable(GRI);
is_subscribable(_, #gri{id = undefined}) ->
    % But resources that do not correspond to any entity cannot
    false;
is_subscribable(_, GRI) ->
    % Resources corresponding to an entity can be subscribed for depending on logic_plugin
    ?GS_LOGIC_PLUGIN:is_subscribable(GRI).
