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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").


% An opaque term, understood by gs_logic_plugin, attached to a connection and
% carrying arbitrary data for internal use in gs_logic_plugin.
-type connection_info() :: undefined | term().
% Identifier of connection process, one per session.
-type conn_ref() :: pid().
% Plugin module used to translate request results.
-type translator() :: module().

-export_type([conn_ref/0, translator/0, connection_info/0]).

%% Simple cache used to remember calculated payloads per GRI and limit the
%% number of requests to GS_LOGIC_PLUGIN when there is a lot of subscribers
%% for the same records.
-type payload_cache() :: #{gs_protocol:gri() => gs_protocol:data()}.


%% API
-export([authorize/1, handshake/5]).
-export([cleanup_client_session/1, terminate_connection/1]).
-export([updated/3, deleted/2]).
-export([handle_request/2]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Authorizes a client based on cowboy request, used during handshake.
%% @end
%%--------------------------------------------------------------------
-spec authorize(cowboy_req:req()) ->
    {ok, gs_protocol:client(), connection_info(), cowboy_req:req()} |
    gs_protocol:error().
authorize(Req) ->
    ?GS_LOGIC_PLUGIN:authorize(Req).


%%--------------------------------------------------------------------
%% @doc
%% Validates a handshake request and if it's correct, creates a new session.
%% Returns success or error handshake response depending on the outcome.
%% @end
%%--------------------------------------------------------------------
-spec handshake(gs_protocol:client(), connection_info(), conn_ref(), translator(), gs_protocol:req_wrapper()) ->
    {ok, gs_protocol:resp_wrapper()} | {error, gs_protocol:resp_wrapper()}.
handshake(Client, ConnectionInfo, ConnRef, Translator, #gs_req{request = #gs_req_handshake{} = HReq} = Req) ->
    #gs_req_handshake{supported_versions = ClientVersions} = HReq,
    ServerVersions = gs_protocol:supported_versions(),
    case gs_protocol:greatest_common_version(ClientVersions, ServerVersions) of
        false ->
            {error, gs_protocol:generate_error_response(
                Req, ?ERROR_BAD_VERSION(ServerVersions))
            };
        {true, Version} ->
            {ok, SessionId} = gs_persistence:create_session(#gs_session{
                client = Client,
                connection_info = ConnectionInfo,
                conn_ref = ConnRef,
                protocol_version = Version,
                translator = Translator
            }),
            ?GS_LOGIC_PLUGIN:client_connected(Client, ConnectionInfo, ConnRef),
            Identity = ?GS_LOGIC_PLUGIN:client_to_identity(Client),
            Attributes = Translator:handshake_attributes(Client),
            {ok, gs_protocol:generate_success_response(Req, #gs_resp_handshake{
                version = Version,
                session_id = SessionId,
                identity = Identity,
                attributes = Attributes
            })}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Cleans up all data related to given session, including its subscriptions.
%% @end
%%--------------------------------------------------------------------
-spec cleanup_client_session(gs_protocol:session_id()) -> ok.
cleanup_client_session(SessionId) ->
    {ok, #gs_session{
        client = Client, conn_ref = ConnRef, connection_info = ConnectionInfo
    }} = gs_persistence:get_session(SessionId),
    ?GS_LOGIC_PLUGIN:client_disconnected(Client, ConnectionInfo, ConnRef),
    gs_persistence:remove_all_subscriptions(SessionId),
    gs_persistence:delete_session(SessionId),
    ok.


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
-spec updated(gs_protocol:entity_type(), gs_protocol:entity_id(), gs_protocol:entity()) -> ok.
updated(EntityType, EntityId, Entity) ->
    {ok, AllSubscribers} = gs_persistence:get_subscribers(EntityType, EntityId),
    maps:fold(fun
        ({_Aspect, _Scope}, [], PayloadCache) ->
            PayloadCache;
        ({Aspect, Scope}, Subscribers, PayloadCache) ->
            GRI = #gri{type = EntityType, id = EntityId, aspect = Aspect, scope = Scope},
            lists:foldl(fun(Subscriber, PayloadCacheAcc) ->
                push_updated(GRI, Entity, Subscriber, PayloadCacheAcc)
            end, PayloadCache, Subscribers)
    end, #{}, AllSubscribers),
    ok.


-spec push_updated(gs_protocol:gri(), gs_protocol:entity(),
    gs_persistence:subscriber(), payload_cache()) -> payload_cache().
push_updated(ReqGRI, Entity, {SessionId, {Client, AuthHint}}, PayloadCache) ->
    case gs_persistence:get_session(SessionId) of
        {error, not_found} ->
            % Possible when session cleanup is in progress
            ok;
        {ok, #gs_session{protocol_version = ProtoVer, conn_ref = ConnRef, translator = Translator}} ->
            case ?GS_LOGIC_PLUGIN:is_authorized(Client, AuthHint, ReqGRI, get, Entity) of
                {true, ResGRI} ->
                    {Data, NewPayloadCache} = updated_payload(
                        ReqGRI, ResGRI, Translator, ProtoVer, Client, Entity, PayloadCache
                    ),
                    gs_ws_handler:push(ConnRef, #gs_push{
                        subtype = graph, message = #gs_push_graph{
                            gri = ReqGRI, change_type = updated, data = Data
                        }}),
                    NewPayloadCache;
                false ->
                    unsubscribe(SessionId, ReqGRI),
                    gs_ws_handler:push(ConnRef, #gs_push{
                        subtype = nosub, message = #gs_push_nosub{
                            gri = ReqGRI, reason = forbidden
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
    {ok, AllSubscribers} = gs_persistence:get_subscribers(EntityType, EntityId),
    maps:map(fun
        ({_Aspect, _Scope}, []) ->
            ok;
        ({Aspect, Scope}, Subscribers) ->
            GRI = #gri{type = EntityType, id = EntityId, aspect = Aspect, scope = Scope},
            lists:foreach(
                fun({SessionId, _}) ->
                    case gs_persistence:get_session(SessionId) of
                        {error, not_found} ->
                            % Possible when session cleanup is in progress
                            ok;
                        {ok, #gs_session{conn_ref = ConnRef}} ->
                            gs_ws_handler:push(ConnRef, #gs_push{
                                subtype = graph, message = #gs_push_graph{
                                    gri = GRI, change_type = deleted
                                }})
                    end
                end, Subscribers),
            gs_persistence:remove_all_subscribers(GRI)
    end, AllSubscribers),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Handles a request expressed by #gs_req{} record. Calls back to
%% gs_logic_plugin for application specific logic handling.
%% @end
%%--------------------------------------------------------------------
-spec handle_request(gs_protocol:session_id() | #gs_session{},
    gs_protocol:req_wrapper() | gs_protocol:req()) ->
    {ok, gs_protocol:resp()} | gs_protocol:error().
handle_request(SessionId, Req) when is_binary(SessionId) ->
    {ok, Session} = gs_persistence:get_session(SessionId),
    handle_request(Session, Req);

% No authorization override - unpack the gs_req record as it's context is
% no longer important.
handle_request(Session, #gs_req{auth_override = undefined, request = Req}) ->
    handle_request(Session, Req);

% This request has the authorization field specified, override the default
% authorization.
handle_request(Session, #gs_req{auth_override = AuthOverride} = Req) ->
    case ?GS_LOGIC_PLUGIN:verify_auth_override(AuthOverride) of
        {ok, Client} ->
            handle_request(
                Session#gs_session{client = Client},
                Req#gs_req{auth_override = undefined}
            );
        {error, _} = Error ->
            Error
    end;

handle_request(_Session, #gs_req_handshake{}) ->
    % Handshake is done in handshake/4 function
    ?ERROR_HANDSHAKE_ALREADY_DONE;

handle_request(Session, #gs_req_rpc{} = Req) ->
    #gs_session{client = Client, protocol_version = ProtoVer} = Session,
    #gs_req_rpc{function = Function, args = Args} = Req,
    case ?GS_LOGIC_PLUGIN:handle_rpc(ProtoVer, Client, Function, Args) of
        {ok, Result} ->
            {ok, #gs_resp_rpc{result = Result}};
        {error, _} = Error ->
            Error
    end;

handle_request(Session, #gs_req_graph{gri = #gri{id = ?SELF} = GRI} = Req) ->
    #gs_session{client = Client} = Session,
    Identity = ?GS_LOGIC_PLUGIN:client_to_identity(Client),
    case {GRI#gri.type, Identity} of
        {od_user, {user, UserId}} ->
            handle_request(Session, Req#gs_req_graph{gri = GRI#gri{id = UserId}});
        {od_provider, {provider, ProviderId}} ->
            handle_request(Session, Req#gs_req_graph{gri = GRI#gri{id = ProviderId}});
        _ ->
            ?ERROR_NOT_FOUND
    end;

handle_request(Session, #gs_req_graph{auth_hint = AuthHint = {_, ?SELF}} = Req) ->
    #gs_session{client = Client} = Session,
    Identity = ?GS_LOGIC_PLUGIN:client_to_identity(Client),
    case {AuthHint, Identity} of
        {?THROUGH_USER(?SELF), {user, UserId}} ->
            handle_request(Session, Req#gs_req_graph{auth_hint = ?THROUGH_USER(UserId)});
        {?AS_USER(?SELF), {user, UserId}} ->
            handle_request(Session, Req#gs_req_graph{auth_hint = ?AS_USER(UserId)});
        {?THROUGH_PROVIDER(?SELF), {provider, ProviderId}} ->
            handle_request(Session, Req#gs_req_graph{auth_hint = ?THROUGH_PROVIDER(ProviderId)});
        _ ->
            ?ERROR_FORBIDDEN
    end;

handle_request(Session, #gs_req_graph{} = Req) ->
    #gs_session{
        id = SessionId, client = Client, protocol_version = ProtoVer,
        translator = Translator
    } = Session,
    #gs_req_graph{
        gri = RequestedGRI,
        operation = Operation,
        data = Data,
        auth_hint = AuthHint,
        subscribe = Subscribe
    } = Req,
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
        Client, AuthHint, RequestedGRI, Operation, Data, undefined
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
                data = translate_value(Translator, ProtoVer, RequestedGRI, Client, Value)
            }};
        {create, {ok, resource, {ResultGRI, ResData}}} ->
            {ResultGRI, AuthHint, #gs_resp_graph{
                data_format = resource,
                data = translate_resource(Translator, ProtoVer, RequestedGRI, ResultGRI, Client, ResData)
            }};
        {create, {ok, resource, {ResultGRI, NAuthHint, ResData}}} ->
            {ResultGRI, NAuthHint, #gs_resp_graph{
                data_format = resource,
                data = translate_resource(Translator, ProtoVer, RequestedGRI, ResultGRI, Client, ResData)
            }};

        {get, {ok, ResData}} ->
            {RequestedGRI, AuthHint, #gs_resp_graph{
                data_format = resource,
                data = translate_resource(Translator, ProtoVer, RequestedGRI, Client, ResData)
            }};
        {get, {ok, ResultGRI, ResData}} ->
            {ResultGRI, AuthHint, #gs_resp_graph{
                data_format = resource,
                data = translate_resource(Translator, ProtoVer, RequestedGRI, ResultGRI, Client, ResData)
            }};

        {update, ok} ->
            {RequestedGRI, AuthHint, #gs_resp_graph{}};

        {delete, ok} ->
            {not_subscribable, AuthHint, #gs_resp_graph{}}
    end,
    case {Subscribe, NewAuthHint, NewGRI} of
        {true, _, not_subscribable} ->
            throw(?ERROR_NOT_SUBSCRIBABLE);
        {true, _, _} ->
            subscribe(SessionId, NewGRI, Client, NewAuthHint);
        {false, _, _} ->
            ok
    end,
    {ok, Response};

handle_request(#gs_session{id = SessionId}, #gs_req_unsub{gri = GRI}) ->
    unsubscribe(SessionId, GRI),
    {ok, #gs_resp_unsub{}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec translate_value(translator(), gs_protocol:protocol_version(), gs_protocol:gri(),
    gs_protocol:client(), term()) -> gs_protocol:data() | gs_protocol:error().
translate_value(Translator, ProtoVer, GRI, Client, Data) ->
    % Used only when data is returned rather than GRI and resource
    case Translator:translate_value(ProtoVer, GRI, Data) of
        Fun when is_function(Fun, 1) -> Fun(Client);
        Result -> Result
    end.


%% @private
-spec translate_resource(translator(), gs_protocol:protocol_version(), gs_protocol:gri(),
    gs_protocol:client(), term()) -> gs_protocol:data() | gs_protocol:error().
translate_resource(Translator, ProtoVer, GRI, Client, Data) ->
    translate_resource(Translator, ProtoVer, GRI, GRI, Client, Data).

%% @private
-spec translate_resource(translator(), gs_protocol:protocol_version(),
    RequestedGRI :: gs_protocol:gri(), ResultGRI :: gs_protocol:gri(),
    gs_protocol:client(), term()) -> gs_protocol:data() | gs_protocol:error().
translate_resource(Translator, ProtoVer, RequestedGRI, ResultGRI, Client, Data) ->
    Resp = case Translator:translate_resource(ProtoVer, ResultGRI, Data) of
        Fun when is_function(Fun, 1) -> Fun(Client);
        Result -> Result
    end,
    %% GRI must be sent back with every GET response
    insert_gri(RequestedGRI, ResultGRI, Resp).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is used to generate payload for the "updated" push message.
%% It uses a simple PayloadCache, which remembers past results between function
%% calls - it is useful to minimize the number of calls to GS_LOGIC_PLUGIN when
%% generating the same payload for many subscribers.
%% @end
%%--------------------------------------------------------------------
-spec updated_payload(ReqGRI :: gs_protocol:gri(), ResGRI :: gs_protocol:gri(),
    translator(), gs_protocol:protocol_version(), gs_protocol:client(),
    gs_protocol:entity(), payload_cache()) -> {gs_protocol:data(), payload_cache()}.
updated_payload(ReqGRI, ResGRI, Translator, ProtoVer, Client, Entity, PayloadCache) ->
    case maps:find(ResGRI, PayloadCache) of
        {ok, Fun} when is_function(Fun, 1) ->
            % Function-type translator, must be evaluated per client
            {insert_gri(ReqGRI, ResGRI, Fun(Client)), PayloadCache};
        {ok, Data} ->
            % Literal translator, the results can be reused for all clients
            {insert_gri(ReqGRI, ResGRI, Data), PayloadCache};
        _ ->
            {ok, Data} = ?GS_LOGIC_PLUGIN:handle_graph_request(
                ?GS_LOGIC_PLUGIN:root_client(), undefined, ResGRI, get, #{}, Entity
            ),
            TranslateResult = Translator:translate_resource(ProtoVer, ResGRI, Data),
            Result = case TranslateResult of
                Fun when is_function(Fun, 1) -> Fun(Client);
                DataJSONMap -> DataJSONMap
            end,
            % Cache the translator result for faster consecutive calls
            {insert_gri(ReqGRI, ResGRI, Result), PayloadCache#{ResGRI => TranslateResult}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Inserts the "gri" field in the result. Checks if auto scope was
%% requested - in this case overrides the scope in result to auto.
%% @end
%%--------------------------------------------------------------------
-spec insert_gri(RequestedGRI :: gs_protocol:gri(), ResultGRI :: gs_protocol:gri(),
    gs_protocol:data()) -> gs_protocol:data().
insert_gri(#gri{scope = auto}, ResultGRI, Data) ->
    Data#{<<"gri">> => gs_protocol:gri_to_string(ResultGRI#gri{scope = auto})};
insert_gri(_RequestedGRI, ResultGRI, Data) ->
    Data#{<<"gri">> => gs_protocol:gri_to_string(ResultGRI)}.


%% @private
-spec subscribe(gs_protocol:session_id(), gs_protocol:gri(), gs_protocol:client(),
    gs_protocol:auth_hint()) -> ok.
subscribe(SessionId, GRI, Client, AuthHint) ->
    gs_persistence:add_subscriber(GRI, SessionId, Client, AuthHint),
    gs_persistence:add_subscription(SessionId, GRI),
    ok.


%% @private
-spec unsubscribe(gs_protocol:session_id(), gs_protocol:gri()) -> ok.
unsubscribe(SessionId, GRI) ->
    gs_persistence:remove_subscriber(GRI, SessionId),
    gs_persistence:remove_subscription(SessionId, GRI),
    ok.


%% @private
-spec is_subscribable(gs_protocol:operation(), gs_protocol:gri()) -> boolean().
is_subscribable(create, GRI = #gri{id = undefined}) ->
    % Newly created resources can be subscribed for depending on logic_plugin
    ?GS_LOGIC_PLUGIN:is_subscribable(GRI);
is_subscribable(_, #gri{id = undefined}) ->
    % But resources that do not correspond to any entity cannot
    false;
is_subscribable(_, GRI) ->
    % Resources corresponding to an entity can be subscribed for depending on logic_plugin
    ?GS_LOGIC_PLUGIN:is_subscribable(GRI).
