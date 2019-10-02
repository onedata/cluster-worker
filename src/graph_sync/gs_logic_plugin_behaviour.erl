%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This behaviour defines callbacks that must be implemented in gs_logic_plugin
%%% that handles application specific logic in Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_logic_plugin_behaviour).
-author("Lukasz Opiola").


%%--------------------------------------------------------------------
%% @doc
%% Resolves the authorization of the requesting client based on handshake auth.
%% If error is returned, the handshake is denied.
%% @end
%%--------------------------------------------------------------------
-callback verify_handshake_auth(gs_protocol:client_auth(), ip_utils:ip()) ->
    {ok, aai:auth()} | errors:error().


%%--------------------------------------------------------------------
%% @doc
%% Callback called when a new client connects to the Graph Sync server.
%% @end
%%--------------------------------------------------------------------
-callback client_connected(aai:auth(), gs_server:conn_ref()) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when a client disconnects from the Graph Sync server.
%% @end
%%--------------------------------------------------------------------
-callback client_disconnected(aai:auth(), gs_server:conn_ref()) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when auth override is sent in request to verify it.
%% {@link gs_protocol:auth_override()}.
%% @end
%%--------------------------------------------------------------------
-callback verify_auth_override(aai:auth(), gs_protocol:auth_override()) ->
    {ok, aai:auth()} | errors:error().


%%--------------------------------------------------------------------
%% @doc
%% Determines if given authorization allows to perform certain operation.
%% GRI is returned to indicate how auto scope was resolved. If a specific
%% scope was requested, it must return the same gri.
%% @end
%%--------------------------------------------------------------------
-callback is_authorized(aai:auth(), gs_protocol:auth_hint(),
    gri:gri(), gs_protocol:operation(), gs_protocol:versioned_entity()) ->
    {true, gri:gri()} | false.


%%--------------------------------------------------------------------
%% @doc
%% Handles an RPC request and returns the result.
%% @end
%%--------------------------------------------------------------------
-callback handle_rpc(gs_protocol:protocol_version(), aai:auth(),
    gs_protocol:rpc_function(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().


%%--------------------------------------------------------------------
%% @doc
%% Handles a graph request and returns the result.
%% @end
%%--------------------------------------------------------------------
-callback handle_graph_request(aai:auth(), gs_protocol:auth_hint(),
    gri:gri(), gs_protocol:operation(), gs_protocol:data(),
    gs_protocol:versioned_entity()) -> gs_protocol:graph_request_result().


%%--------------------------------------------------------------------
%% @doc
%% Returns whether given GRI is subscribable, i.e. clients can subscribe for
%% changes concerning that GRI.
%% @end
%%--------------------------------------------------------------------
-callback is_subscribable(gri:gri()) -> boolean().


%%--------------------------------------------------------------------
%% @doc
%% Returns whether entity type in given GRI is supported by the server.
%% @end
%%--------------------------------------------------------------------
-callback is_type_supported(gri:gri()) -> boolean().
