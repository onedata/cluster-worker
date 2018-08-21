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


% Identifier of connection process, one per session.
-type conn_ref() :: pid().
% Plugin module used to translate request results.
-type translator() :: module().

-export_type([conn_ref/0, translator/0]).


%% API
-export([authorize/1, handshake/4]).
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
-spec authorize(cowboy_req:req()) -> {ok, gs_protocol:client()} | gs_protocol:error().
authorize(Req) ->
    case authorize_by_session_cookie(Req) of
        {true, CookieClient} ->
            {ok, CookieClient};
        {error, _} ->
            ?ERROR_UNAUTHORIZED;
        false ->
            case authorize_by_macaroon(Req) of
                {true, MacaroonClient} ->
                    {ok, MacaroonClient};
                {error, _} ->
                    ?ERROR_UNAUTHORIZED;
                false ->
                    {ok, ?GS_LOGIC_PLUGIN:guest_client()}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Validates a handshake request and if it's correct, creates a new session.
%% Returns success or error handshake response depending on the outcome.
%% @end
%%--------------------------------------------------------------------
-spec handshake(gs_protocol:client(), conn_ref(), translator(), gs_protocol:req_wrapper()) ->
    {ok, gs_protocol:resp_wrapper()} | {error, gs_protocol:resp_wrapper()}.
handshake(Client, ConnRef, Translator, #gs_req{request = #gs_req_handshake{} = HReq} = Req) ->
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
                conn_ref = ConnRef,
                protocol_version = Version,
                translator = Translator
            }),
            ?GS_LOGIC_PLUGIN:client_connected(Client, ConnRef),
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
        client = Client, conn_ref = ConnRef
    }} = gs_persistence:get_session(SessionId),
    ?GS_LOGIC_PLUGIN:client_disconnected(Client, ConnRef),
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
    lists:foreach(
        fun(GRI) ->
            updated(GRI, Entity)
        end, generate_subscribable_resources(EntityType, EntityId)).

-spec updated(gs_protocol:gri(), gs_protocol:entity()) -> ok.
updated(GRI, Entity) ->
    {ok, Subs} = gs_persistence:get_subscribers(GRI),
    case Subs of
        [] ->
            ok;
        _ ->
            {ok, Data} = ?GS_LOGIC_PLUGIN:handle_graph_request(
                ?GS_LOGIC_PLUGIN:root_client(), undefined, GRI, get, #{}, Entity
            ),
            lists:foreach(
                fun(Subscriber) ->
                    updated(GRI, Entity, Subscriber, Data)
                end, Subs)
    end.

-spec updated(gs_protocol:gri(), gs_protocol:entity(), gs_persistence:subscriber(), term()) -> ok.
updated(GRI, Entity, {SessionId, {Client, AuthHint}} = _Subscriber, Data) ->
    case gs_persistence:get_session(SessionId) of
        {error, not_found} ->
            % Possible when session cleanup is in progress
            ok;
        {ok, GsSession} ->
            #gs_session{
                protocol_version = ProtoVersion,
                conn_ref = ConnRef,
                translator = Translator
            } = GsSession,
            case ?GS_LOGIC_PLUGIN:is_authorized(Client, AuthHint, GRI, get, Entity) of
                true ->
                    DataJSONMap = translate_resource(
                        Translator, ProtoVersion, GRI, Data
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
    lists:foreach(
        fun(GRI) ->
            deleted(GRI)
        end, generate_subscribable_resources(EntityType, EntityId)).


-spec deleted(gs_protocol:gri()) -> ok.
deleted(GRI) ->
    {ok, Subs} = gs_persistence:get_subscribers(GRI),
    lists:foreach(fun({SessionId, _}) ->
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
    end, Subs),
    gs_persistence:remove_all_subscribers(GRI).


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
    AuthResult = case AuthOverride of
        {token, Token} ->
            ?GS_LOGIC_PLUGIN:authorize_by_token(Token);
        {macaroon, Mac, DischMacs} ->
            ?GS_LOGIC_PLUGIN:authorize_by_macaroons(Mac, DischMacs);
        {basic, UserPasswdB64} ->
            ?GS_LOGIC_PLUGIN:authorize_by_basic_auth(UserPasswdB64)
    end,
    case AuthResult of
        {true, Client} ->
            handle_request(
                Session#gs_session{client = Client},
                Req#gs_req{auth_override = undefined}
            );
        false ->
            ?ERROR_UNAUTHORIZED;
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
            case is_subscribable(GRI) of
                true ->
                    ok;
                false ->
                    throw(?ERROR_NOT_SUBSCRIBABLE)
            end;
        false ->
            ok
    end,
    Result = ?GS_LOGIC_PLUGIN:handle_graph_request(
        Client, AuthHint, GRI, Operation, Data, undefined
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
                data = translate_value(Translator, ProtoVer, GRI, Value)
            }};
        {create, {ok, resource, {UpdatedGRI, ResData}}} ->
            {UpdatedGRI, AuthHint, #gs_resp_graph{
                data_format = resource,
                data = translate_resource(Translator, ProtoVer, UpdatedGRI, ResData)
            }};
        {create, {ok, resource, {UpdatedGRI, NAuthHint, ResData}}} ->
            {UpdatedGRI, NAuthHint, #gs_resp_graph{
                data_format = resource,
                data = translate_resource(Translator, ProtoVer, UpdatedGRI, ResData)
            }};

        {get, {ok, ResData}} ->
            {GRI, AuthHint, #gs_resp_graph{
                data_format = resource,
                data = translate_resource(Translator, ProtoVer, GRI, ResData)
            }};

        {update, ok} ->
            {GRI, AuthHint, #gs_resp_graph{}};

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

-spec translate_value(translator(), gs_protocol:protocol_version(),
    gs_protocol:gri(), term()) -> gs_protocol:data() | gs_protocol:error().
translate_value(Translator, ProtoVer, GRI, Data) ->
    % Used only when data is returned rather than GRI and resource
    Translator:translate_value(ProtoVer, GRI, Data).


-spec translate_resource(translator(), gs_protocol:protocol_version(),
    gs_protocol:gri(), term()) -> gs_protocol:data() | gs_protocol:error().
translate_resource(Translator, ProtoVer, GRI, Data) ->
    {NewGRI, Resp} = case Translator:translate_resource(ProtoVer, GRI, Data) of
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


-spec generate_subscribable_resources(gs_protocol:entity_type(),
    gs_protocol:entity_id()) -> [gs_protocol:gri()].
generate_subscribable_resources(EntityType, EntityId) ->
    lists:map(
        fun({A, S}) ->
            #gri{type = EntityType, id = EntityId, aspect = A, scope = S}
        end, ?GS_LOGIC_PLUGIN:subscribable_resources(EntityType)).


-spec is_subscribable(gs_protocol:gri()) -> boolean().
is_subscribable(#gri{type = EntityType, aspect = Aspect, scope = Scope}) ->
    lists:member(
        {Aspect, Scope},
        ?GS_LOGIC_PLUGIN:subscribable_resources(EntityType)
    ).


%%--------------------------------------------------------------------
%% @doc
%% Tries to authorize client by HTTP cookie.
%% {true, Client} - client was authorized
%% false - this method cannot verify authorization, other methods should be tried
%% {error, term()} - authorization invalid
%% @end
%%--------------------------------------------------------------------
-spec authorize_by_session_cookie(cowboy_req:req()) ->
    {true, gs_protocol:client()} | false | gs_protocol:error().
authorize_by_session_cookie(Req) ->
    case get_cookie(?GRAPH_SYNC_SESSION_COOKIE_NAME, Req) of
        undefined ->
            false;
        SessionCookie ->
            ?GS_LOGIC_PLUGIN:authorize_by_session_cookie(SessionCookie)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Tries to authorize client by provider certificate.
%% {true, Client} - client was authorized
%% false - this method cannot verify authorization, other methods should be tried
%% {error, term()} - authorization invalid
%% @end
%%--------------------------------------------------------------------
-spec authorize_by_macaroon(cowboy_req:req()) ->
    {true, gs_protocol:client()} | false | gs_protocol:error().
authorize_by_macaroon(Req) ->
    case get_macaroon(Req) of
        undefined ->
            false;
        Macaroon ->
            ?GS_LOGIC_PLUGIN:authorize_by_macaroons(Macaroon, [])
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns cookie value for given cookie name.
%% Undefined if no such cookie was sent.
%% NOTE! This should be used instead of cowboy_req:cookie as it contains a bug.
%% @end
%%--------------------------------------------------------------------
-spec get_cookie(Name :: binary(), cowboy_req:req()) -> binary() | undefined.
get_cookie(Name, Req) ->
    proplists:get_value(Name, cowboy_req:parse_cookies(Req)).


%%--------------------------------------------------------------------
%% @doc
%% Returns macaroon from "macaroon" or "X-Auth-token" header, if present.
%% @end
%%--------------------------------------------------------------------
-spec get_macaroon(cowboy_req:req()) -> binary() | undefined.
get_macaroon(Req) ->
    MacaroonHeader = cowboy_req:header(<<"macaroon">>, Req),
    XAuthTokenHeader = cowboy_req:header(<<"x-auth-token">>, Req),
    % X-Auth-Token is an alias for macaroon header, check if any of them is given.
    case MacaroonHeader of
        <<_/binary>> -> MacaroonHeader;
        _ -> XAuthTokenHeader
    end.
