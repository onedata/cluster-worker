%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used to configure ha.
%%% NOTE: Functions in this module work node-wide as failures concern whole nodes.
%%% NOTE: Config functions should be executed on all nodes during cluster reconfiguration.
%%% TODO - VFS-6166 - Verify HA Cast
%%% TODO - VFS-6167 - Datastore HA supports nodes adding and deleting
%%% For more information see ha.hrl.
%%% @end
%%%-------------------------------------------------------------------
-module(ha_management).
-author("Michał Wrzeszcz").

-include("modules/datastore/ha.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_propagation_method/0, get_backup_nodes/0, get_slave_mode/0]).
-export([master_down/0, master_up/0, change_config/2]).

-type propagation_method() :: ?HA_CALL_PROPAGATION | ?HA_CAST_PROPAGATION.
% Mode determines whether slave process only backups data or process also handles requests when master is down
-type slave_mode() :: ?STANDBY_SLAVE_MODE | ?TAKEOVER_SLAVE_MODE.

-export_type([propagation_method/0, slave_mode/0]).

% Envs used to configure ha
-define(HA_PROPAGATION_METHOD, ha_propagation_method).
-define(SLAVE_MODE, slave_mode).

%%%===================================================================
%%% API getters
%%%===================================================================


-spec get_propagation_method() -> propagation_method().
get_propagation_method() ->
    application:get_env(?CLUSTER_WORKER_APP_NAME, ?HA_PROPAGATION_METHOD, ?HA_CAST_PROPAGATION).


-spec get_backup_nodes() -> [node()].
get_backup_nodes() ->
    case consistent_hashing:get_key_connected_nodes() of
        1 ->
            [];
        BackupNodesNum ->
            Nodes = get_backup_nodes(node(), consistent_hashing:get_all_nodes()),
            lists:sublist(Nodes, min(BackupNodesNum - 1, length(Nodes)))
    end.


-spec get_slave_mode() -> slave_mode().
get_slave_mode() ->
    application:get_env(?CLUSTER_WORKER_APP_NAME, ?SLAVE_MODE, ?STANDBY_SLAVE_MODE).

%%%===================================================================
%%% API to configure processes - sets information in environment variables
%%% and sends it to all tp processes on this node (to inform them about
%%% the change - processes usually read environment variables only during initialization).
%%%===================================================================

-spec master_down() -> ok.
master_down() ->
    ?info("Set and broadcast master_down"),
    application:set_env(?CLUSTER_WORKER_APP_NAME, ?SLAVE_MODE, ?TAKEOVER_SLAVE_MODE),
    tp_router:send_to_each(?MASTER_DOWN).


-spec master_up() -> ok.
master_up() ->
    ?info("Set and broadcast master_up"),
    application:set_env(?CLUSTER_WORKER_APP_NAME, ?SLAVE_MODE, ?STANDBY_SLAVE_MODE),
    tp_router:send_to_each(?MASTER_UP).


-spec change_config(non_neg_integer(), propagation_method()) -> ok.
change_config(NodesNumber, PropagationMethod) ->
    ?info("Set and broadcast new ha config: nodes number: ~p, propagation method: ~p", [NodesNumber, PropagationMethod]),
    consistent_hashing:set_key_connected_nodes(NodesNumber),
    application:set_env(?CLUSTER_WORKER_APP_NAME, ?HA_PROPAGATION_METHOD, PropagationMethod),
    tp_router:send_to_each(?MANAGEMENT_MSG(?CONFIG_CHANGED)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_backup_nodes(MyNode :: node(), AllNodes :: [node()]) -> BackupNodes :: [node()].
get_backup_nodes(MyNode, [MyNode | Nodes]) ->
    Nodes;
get_backup_nodes(MyNode, [Node | Nodes]) ->
    get_backup_nodes(MyNode, Nodes ++ [Node]).