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

-ifndef(GRAPH_SYNC_CW_HRL).
-define(GRAPH_SYNC_CW_HRL, 1).

-include_lib("ctool/include/graph_sync/gri.hrl").

% Protocol version used for structures that may not change over time.
-define(BASIC_PROTOCOL, 0).

% Protocol versions currently supported by this software
-define(SUPPORTED_PROTO_VERSIONS, [3, 4]).

-record(gs_req_handshake, {
    supported_versions = [] :: [gs_protocol:protocol_version()],
    auth = undefined :: undefined | gs_protocol:client_auth(),
    session_id :: undefined | gs_protocol:session_id()
}).

-record(gs_resp_handshake, {
    version = ?BASIC_PROTOCOL :: gs_protocol:protocol_version(),
    session_id :: undefined | gs_protocol:session_id(),
    identity = nobody :: aai:subject(),
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
    gri :: gri:gri(),
    operation :: gs_protocol:operation(),
    data :: gs_protocol:data(),
    subscribe = false :: boolean(),
    auth_hint :: gs_protocol:auth_hint()
}).

-record(gs_resp_graph, {
    data_format :: gs_protocol:data_format(),
    data :: gs_protocol:data()
}).

-record(gs_req_unsub, {
    gri :: gri:gri()
}).

-record(gs_resp_unsub, {
    % Currently, the success response does not carry any data
}).

-record(gs_push_graph, {
    gri :: gri:gri(),
    change_type = updated :: gs_protocol:change_type(),
    data :: gs_protocol:data()
}).

-record(gs_push_nosub, {
    gri :: gri:gri(),
    auth_hint :: gs_protocol:auth_hint(),
    reason = forbidden :: gs_protocol:nosub_reason()
}).

-record(gs_push_error, {
    error :: errors:error()
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
    error :: undefined | errors:error(),
    response :: undefined | gs_protocol:resp()
}).

-record(gs_push, {
    subtype :: gs_protocol:message_subtype(),
    message :: gs_protocol:push()
}).

-record(auth_override, {
    client_auth :: gs_protocol:client_auth(),
    peer_ip :: undefined | ip_utils:ip(),
    interface :: undefined | cv_interface:interface(),
    consumer_token :: undefined | tokens:serialized(),
    data_access_caveats_policy = disallow_data_access_caveats :: data_access_caveats:policy()
}).

% Possible auth hints
% Auth hint denoting rights to view certain aspects
-define(THROUGH_USER(UserId), {throughUser, UserId}).
-define(THROUGH_GROUP(GroupId), {throughGroup, GroupId}).
-define(THROUGH_SPACE(SpaceId), {throughSpace, SpaceId}).
-define(THROUGH_PROVIDER(ProviderId), {throughProvider, ProviderId}).
-define(THROUGH_HANDLE_SERVICE(HServiceId), {throughHandleService, HServiceId}).
-define(THROUGH_HANDLE(HandleId), {throughHandle, HandleId}).
-define(THROUGH_HARVESTER(HarvesterId), {throughHarvester, HarvesterId}).
-define(THROUGH_CLUSTER(ClusterId), {throughCluster, ClusterId}).
-define(THROUGH_ATM_INVENTORY(AtmInventoryId), {throughInventory, AtmInventoryId}).
% Auth hint denoting context in which an aspect is created
-define(AS_USER(UserId), {asUser, UserId}).
-define(AS_GROUP(GroupId), {asGroup, GroupId}).
-define(AS_SPACE(SpaceId), {asSpace, SpaceId}).
-define(AS_HARVESTER(HarvesterId), {asHarvester, HarvesterId}).

-endif.