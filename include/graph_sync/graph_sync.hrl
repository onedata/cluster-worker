%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions and records connected with Graph Sync.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(GRAPH_SYNC_HRL).
-define(GRAPH_SYNC_HRL, 1).

% Module that must be implemented in projects that use Graph Sync, contains
% callbacks according to gs_logic_plugin_behaviour.
-define(GS_LOGIC_PLUGIN, gs_logic_plugin).

% Protocol version used for structures that may not change over time.
-define(BASIC_PROTOCOL, 0).

% Cookie name for cookie based session.
-define(GRAPH_SYNC_SESSION_COOKIE_NAME, <<"session_id">>).


% Graph Resource Identifier - a record identifying a certain resource in the
% graph.
-record(gri, {
    type :: undefined | gs_protocol:entity_type(),
    id :: undefined | gs_protocol:entity_id(),
    aspect :: undefined | gs_protocol:aspect(),
    scope = private :: gs_protocol:scope()
}).

-record(gs_req_handshake, {
    supported_versions = [] :: [gs_protocol:protocol_version()],
    session_id :: undefined | gs_protocol:session_id()
}).

-record(gs_resp_handshake, {
    version = ?BASIC_PROTOCOL :: gs_protocol:protocol_version(),
    session_id :: undefined | gs_protocol:session_id(),
    identity = nobody :: nobody | gs_protocol:identity(),
    attributes = undefined :: gs_protocol:handshake_attributes()
}).

-record(gs_req_rpc, {
    function :: gs_protocol:rpc_function(),
    args = #{} :: gs_protocol:rpc_args()
}).

-record(gs_resp_rpc, {
    result :: undefined | gs_protocol:data()
}).

-record(gs_req_graph, {
    gri :: gs_protocol:gri(),
    operation :: gs_protocol:operation(),
    data :: gs_protocol:data(),
    subscribe = false :: boolean(),
    auth_hint :: gs_protocol:auth_hint()
}).

-record(gs_resp_graph, {
    result :: undefined | gs_protocol:data()
}).

-record(gs_req_unsub, {
    gri :: gs_protocol:gri()
}).

-record(gs_resp_unsub, {
    % Currently, the success response does not carry any data
}).

-record(gs_push_graph, {
    gri :: gs_protocol:gri(),
    change_type = updated :: gs_protocol:change_type(),
    data :: gs_protocol:data()
}).

-record(gs_push_nosub, {
    gri :: gs_protocol:gri(),
    reason = forbidden :: gs_protocol:nosub_reason()
}).

-record(gs_push_error, {
    error :: gs_protocol:error()
}).

-record(gs_req, {
    id = undefined :: undefined | gs_protocol:message_id(),
    subtype :: gs_protocol:message_subtype(),
    auth_override :: gs_protocol:auth_override(),
    request :: gs_protocol:req()
}).

-record(gs_resp, {
    id :: gs_protocol:entity_id(),
    subtype :: gs_protocol:message_subtype(),
    success :: boolean(),
    error :: undefined | gs_protocol:error(),
    response :: undefined | gs_protocol:resp()
}).

-record(gs_push, {
    subtype :: gs_protocol:message_subtype(),
    message :: gs_protocol:push()
}).

% Special id expressing "myself" (the client that is authenticated)
-define(SELF, <<"self">>).

% Possible auth hints
% Auth hint denoting rights to view certain aspects
-define(THROUGH_USER(__UserId), {throughUser, __UserId}).
-define(THROUGH_GROUP(__GroupId), {throughGroup, __GroupId}).
-define(THROUGH_SPACE(__SpaceId), {throughSpace, __SpaceId}).
-define(THROUGH_PROVIDER(__ProviderId), {throughProvider, __ProviderId}).
-define(THROUGH_HANDLE_SERVICE(__HServiceId), {throughHandleService, __HServiceId}).
-define(THROUGH_HANDLE(__HandleId), {throughHandle, __HandleId}).
% Auth hint denoting context in which an aspect is created
-define(AS_USER(__UserId), {asUser, __UserId}).
-define(AS_GROUP(__GroupId), {asGroup, __GroupId}).

-endif.