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
%% Authorizes the requesting client. If error is returned, the Graph Sync
%% connection will be denied.
%% @end
%%--------------------------------------------------------------------
-callback authorize(cowboy_req:req()) ->
    {ok, gs_protocol:client(), gs_server:connection_info(), cowboy_req:req()} |
    gs_protocol:error().


%%--------------------------------------------------------------------
%% @doc
%% Converts client, which is an opaque term for gs_server, into identity of
%% the client.
%% @end
%%--------------------------------------------------------------------
-callback client_to_identity(gs_protocol:client()) -> gs_protocol:identity().


%%--------------------------------------------------------------------
%% @doc
%% Returns the ROOT client as understood by gs_logic_plugin, i.e. a client that
%% is authorized to do everything. ROOT client can be used only in internal
%% code (i.e. cannot be accessed via any API).
%% @end
%%--------------------------------------------------------------------
-callback root_client() -> gs_protocol:client().


%%--------------------------------------------------------------------
%% @doc
%% Callback called when a new client connects to the Graph Sync server.
%% @end
%%--------------------------------------------------------------------
-callback client_connected(gs_protocol:client(), gs_server:connection_info(), gs_server:connection_ref()) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when a client disconnects from the Graph Sync server.
%% @end
%%--------------------------------------------------------------------
-callback client_disconnected(gs_protocol:client(), gs_server:connection_info(), gs_server:connection_ref()) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when auth override is sent in request to verify it.
%% @end
%%--------------------------------------------------------------------
-callback verify_auth_override(gs_protocol:client(), gs_protocol:auth_override()) ->
    {ok, gs_protocol:client()} | gs_protocol:error().


%%--------------------------------------------------------------------
%% @doc
%% Determines if given client is authorized to perform certain operation.
%% GRI is returned to indicate how auto scope was resolved. If a specific
%% scope was requested, it must return the same gri.
%% @end
%%--------------------------------------------------------------------
-callback is_authorized(gs_protocol:client(), gs_protocol:auth_hint(),
    gs_protocol:gri(), gs_protocol:operation(), gs_protocol:data()) ->
    {true, gs_protocol:gri()} | false.


%%--------------------------------------------------------------------
%% @doc
%% Handles an RPC request and returns the result.
%% @end
%%--------------------------------------------------------------------
-callback handle_rpc(gs_protocol:protocol_version(), gs_protocol:client(),
    gs_protocol:rpc_function(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().


%%--------------------------------------------------------------------
%% @doc
%% Handles a graph request and returns the result.
%% @end
%%--------------------------------------------------------------------
-callback handle_graph_request(gs_protocol:client(), gs_protocol:auth_hint(),
    gs_protocol:gri(), gs_protocol:operation(), gs_protocol:data(),
    gs_protocol:entity()) -> gs_protocol:graph_request_result().


%%--------------------------------------------------------------------
%% @doc
%% Returns if given GRI is subscribable, i.e. clients can subscribe for changes
%% concerning that GRI.
%% @end
%%--------------------------------------------------------------------
-callback is_subscribable(gs_protocol:gri()) -> boolean().
