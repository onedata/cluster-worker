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

-include_lib("ctool/include/api_errors.hrl").
-include("graph_sync/graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").


% An opaque term, understood by gs_logic_plugin, attached to a connection and
% carrying arbitrary data for internal use in gs_logic_plugin.
-type connection_info() :: undefined | term().
% Identifier of connection process, one per session.
-type conn_ref() :: pid().
% Plugin module used to translate request results.
-type translator() :: module().

-export_type([conn_ref/0, translator/0, connection_info/0]).


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
    maps:map(fun
        ({_Aspect, _Scope}, []) ->
            ok;
        ({Aspect, Scope}, Subscribers) ->
            GRI = #gri{type = EntityType, id = EntityId, aspect = Aspect, scope = Scope},
            {ok, Data} = ?GS_LOGIC_PLUGIN:handle_graph_request(
                ?GS_LOGIC_PLUGIN:root_client(), undefined, GRI, get, #{}, Entity
            ),
            lists:foreach(
                fun(Subscriber) ->
                    updated(GRI, Entity, Subscriber, Data)
                end, Subscribers)
    end, AllSubscribers),
    ok.


-spec updated(gs_protocol:gri(), gs_protocol:entity(), gs_persistence:subscriber(), term()) -> ok.
updated(GRI, Entity, {SessionId, {Client, AuthHint}}, Data) ->
    {ok, #gs_session{
        protocol_version = ProtoVersion,
        conn_ref = ConnRef,
        translator = Translator
    }} = gs_persistence:get_session(SessionId),
    case ?GS_LOGIC_PLUGIN:is_authorized(Client, AuthHint, GRI, get, Entity) of
        true ->
            DataJSONMap = translate_get(
                Translator, Client, ProtoVersion, GRI, Data
            ),
            gs_ws_handler:push(ConnRef, #gs_push{
                subtype = graph, message = #gs_push_graph{
                    gri = GRI, change_type = updated, data = DataJSONMap
                }});
        false ->
            unsubscribe(SessionId, GRI),
            gs_ws_handler:push(ConnRef, #gs_push{
                subtype = nosub, message = #gs_push_nosub{
                    gri = GRI, reason = forbidden
                }})
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
                    {ok, #gs_session{
                        conn_ref = ConnRef
                    }} = gs_persistence:get_session(SessionId),
                    gs_ws_handler:push(ConnRef, #gs_push{
                        subtype = graph, message = #gs_push_graph{
                            gri = GRI, change_type = deleted
                        }})
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
        gri = GRI,
        operation = Operation,
        data = Data,
        auth_hint = AuthHint,
        subscribe = Subscribe
    } = Req,
    case Subscribe of
        true ->
            case is_subscribable(Operation, GRI) of
                true -> ok;
                false -> throw(?ERROR_NOT_SUBSCRIBABLE)
            end;
        false ->
            ok
    end,
    Result = ?GS_LOGIC_PLUGIN:handle_graph_request(
        Client, AuthHint, GRI, Operation, Data, undefined
    ),
    case Result of
        {error, _} = Error ->
            throw(Error);
        _ ->
            ok
    end,
    {NewGRI, NewAuthHint, ResultJSONMap} = case {Operation, Result} of
        {create, ok} ->
            {not_subscribable, AuthHint, null};
        {create, {ok, {data, ResData}}} ->
            {not_subscribable, AuthHint, #{<<"data">> => translate_create(
                Translator, Client, ProtoVer, GRI, ResData
            )}};
        {create, {ok, {fetched, UpdatedGRI, ResData}}} ->
            {UpdatedGRI, AuthHint, translate_get(
                Translator, Client, ProtoVer, UpdatedGRI, ResData
            )};
        {create, {ok, {not_fetched, UpdatedGRI}}} ->
            % This must succeed, otherwise crash is better to trace the problem
            {ok, FetchedData} = ?GS_LOGIC_PLUGIN:handle_graph_request(
                Client, undefined, UpdatedGRI, get, #{}, undefined
            ),
            {UpdatedGRI, AuthHint, translate_get(
                Translator, Client, ProtoVer, UpdatedGRI, FetchedData
            )};
        {create, {ok, {not_fetched, UpdatedGRI, NAuthHint}}} ->
            % This must succeed, otherwise crash is better to trace the problem
            {ok, FetchedData} = ?GS_LOGIC_PLUGIN:handle_graph_request(
                Client, NAuthHint, UpdatedGRI, get, #{}, undefined
            ),
            {UpdatedGRI, NAuthHint, translate_get(
                Translator, Client, ProtoVer, UpdatedGRI, FetchedData
            )};

        {get, {ok, ResData}} ->
            {GRI, AuthHint, translate_get(
                Translator, Client, ProtoVer, GRI, ResData
            )};

        {update, ok} ->
            {GRI, AuthHint, null};

        {delete, ok} ->
            {not_subscribable, AuthHint, null}
    end,
    case {Subscribe, NewAuthHint, NewGRI} of
        {true, _, not_subscribable} ->
            throw(?ERROR_NOT_SUBSCRIBABLE);
        {true, _, _} ->
            subscribe(SessionId, NewGRI, Client, NewAuthHint);
        {false, _, _} ->
            ok
    end,
    {ok, #gs_resp_graph{result = ResultJSONMap}};

handle_request(#gs_session{id = SessionId}, #gs_req_unsub{gri = GRI}) ->
    unsubscribe(SessionId, GRI),
    {ok, #gs_resp_unsub{}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec translate_create(translator(), gs_protocol:client(), gs_protocol:protocol_version(),
    gs_protocol:gri(), term()) -> gs_protocol:data() | gs_protocol:error().
translate_create(Translator, Client, ProtoVer, GRI, Data) ->
    % Used only when data is returned rather than GRI and resource
    case Translator:translate_create(ProtoVer, GRI, Data) of
        Fun when is_function(Fun, 1) -> Fun(Client);
        Result -> Result
    end.


-spec translate_get(translator(), gs_protocol:client(), gs_protocol:protocol_version(),
    gs_protocol:gri(), term()) -> gs_protocol:data() | gs_protocol:error().
translate_get(Translator, Client, ProtoVer, GRI, Data) ->
    CallbackResult = case Translator:translate_get(ProtoVer, GRI, Data) of
        Fun when is_function(Fun, 1) -> Fun(Client);
        Result -> Result
    end,
    {NewGRI, Resp} = case CallbackResult of
        {ModifiedGRI, Response} -> {ModifiedGRI, Response};
        Response -> {GRI, Response}
    end,
    % GRI must be sent back with every request
    Resp#{<<"gri">> => gs_protocol:gri_to_string(NewGRI)}.


-spec subscribe(gs_protocol:session_id(), gs_protocol:gri(), gs_protocol:client(),
    gs_protocol:auth_hint()) -> ok.
subscribe(SessionId, GRI, Client, AuthHint) ->
    gs_persistence:add_subscriber(GRI, SessionId, Client, AuthHint),
    gs_persistence:add_subscription(SessionId, GRI),
    ok.


-spec unsubscribe(gs_protocol:session_id(), gs_protocol:gri()) -> ok.
unsubscribe(SessionId, GRI) ->
    gs_persistence:remove_subscriber(GRI, SessionId),
    gs_persistence:remove_subscription(SessionId, GRI),
    ok.


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
