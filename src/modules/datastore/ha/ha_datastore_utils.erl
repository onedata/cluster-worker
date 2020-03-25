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
%%% For more information see ha_datastore.hrl.
%%% @end
%%%-------------------------------------------------------------------
-module(ha_datastore_utils).
-author("Michał Wrzeszcz").

-include("modules/datastore/ha_datastore.hrl").
-include("modules/datastore/datastore_protocol.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% Message sending API
-export([send_async_internall_message/2, send_sync_internall_message/2,
    send_async_slave_message/2, send_sync_slave_message/2,
    send_async_master_message/2, send_sync_master_message/4,
    broadcast_management_message/1]).
%% API
-export([get_propagation_method/0, get_backup_nodes/0, get_slave_mode/0]).
-export([master_down/0, master_up/0, change_config/2]).

% Propagation methods - see ha_datastore.hrl
-type propagation_method() :: ?HA_CALL_PROPAGATION | ?HA_CAST_PROPAGATION.
% Slave working mode -  see ha_datastore.hrl
-type slave_mode() :: ?STANDBY_SLAVE_MODE | ?FAILOVER_SLAVE_MODE.

-export_type([propagation_method/0, slave_mode/0]).

% HA messages' types (see ha_datastore.hrl)
-type ha_message_type() :: master | slave | internal | management.
-type ha_message() :: ha_slave:backup_message() | ha_master:unlink_request() |
    ha_master:failover_request_data_processed_message() | ha_slave:get_slave_status() |
    ha_slave:master_node_status_message() | ha_master:config_changed_message().

-export_type([ha_message_type/0, ha_message/0]).

%%%===================================================================
%%% Message sending API
%%%===================================================================

-spec send_async_internall_message(pid(), ha_master:failover_request_data_processed_message() | ha_slave:master_node_status_message() |
    ha_master:config_changed_message()) -> ok.
send_async_internall_message(Pid, Msg) ->
    gen_server:cast(Pid, ?INTERNAL_MSG(Msg)).

-spec send_sync_internall_message(pid(), ha_master:failover_request_data_processed_message() | ha_slave:master_node_status_message() |
ha_master:config_changed_message()) -> ok.
send_sync_internall_message(Pid, Msg) ->
    gen_server:call(Pid, ?INTERNAL_MSG(Msg), infinity).

-spec send_async_slave_message(pid(), ha_master:failover_request_data_processed_message()) -> ok.
send_async_slave_message(Pid, Msg) ->
    gen_server:cast(Pid, ?SLAVE_MSG(Msg)).

-spec send_sync_slave_message(pid(), ha_master:unlink_request()) -> term().
send_sync_slave_message(Ref, Msg) ->
    gen_server:call(Ref, ?SLAVE_MSG(Msg), infinity).

-spec send_async_master_message(pid(), ha_slave:backup_message()) -> ok.
send_async_master_message(Ref, Msg) ->
    gen_server:cast(Ref, ?MASTER_MSG(Msg)).

-spec send_sync_master_message(node(), datastore:key(), ha_slave:backup_message() | ha_slave:get_slave_status() |
    #datastre_internal_requests_batch{}, StartIfNotAlive :: boolean()) -> term().
send_sync_master_message(Node, ProcessKey, Msg, true) ->
    rpc:call(Node, datastore_writer, custom_call, [ProcessKey, ?MASTER_MSG(Msg)]);
send_sync_master_message(Node, ProcessKey, Msg, _StartIfNotAlive) ->
    rpc:call(Node, datastore_writer, call_if_alive, [ProcessKey, ?MASTER_MSG(Msg)]).

-spec broadcast_management_message(ha_slave:master_node_status_message() | ha_master:config_changed_message()) -> ok.
broadcast_management_message(Msg) ->
    tp_router:send_to_each(?MANAGEMENT_MSG(Msg)).

%%%===================================================================
%%% Getters / setters
%%%===================================================================

-spec get_propagation_method() -> propagation_method().
get_propagation_method() ->
    application:get_env(?CLUSTER_WORKER_APP_NAME, ha_propagation_method, ?HA_CAST_PROPAGATION).

-spec set_propagation_method(propagation_method()) -> ok.
set_propagation_method(PropagationMethod) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, ha_propagation_method, PropagationMethod).


-spec get_slave_mode() -> slave_mode().
get_slave_mode() ->
    application:get_env(?CLUSTER_WORKER_APP_NAME, slave_mode, ?STANDBY_SLAVE_MODE).

-spec set_slave_mode(slave_mode()) -> ok.
set_slave_mode(SlaveMode) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, slave_mode, SlaveMode).


-spec get_backup_nodes() -> [node()].
get_backup_nodes() ->
    case application:get_env(?CLUSTER_WORKER_APP_NAME, ha_backup_nodes) of
        {ok, Env} ->
            Env;
        undefined ->
            critical_section:run(?MODULE, fun() ->
                Ans = case consistent_hashing:get_label_associated_nodes_count() of
                    1 ->
                        [];
                    BackupNodesNum ->
                        Nodes = get_backup_nodes(node(), consistent_hashing:get_all_nodes()),
                        lists:sublist(Nodes, min(BackupNodesNum - 1, length(Nodes)))
                end,
                application:set_env(?CLUSTER_WORKER_APP_NAME, ha_backup_nodes, Ans),
                Ans
            end)
    end.

-spec clean_backup_nodes_cache() -> ok.
clean_backup_nodes_cache() ->
    critical_section:run(?MODULE, fun() ->
        application_controller:unset_env(?CLUSTER_WORKER_APP_NAME, ha_backup_nodes)
    end),
    ok.

%%%===================================================================
%%% API to configure processes - sets information in environment variables
%%% and sends it to all tp processes on this node (to inform them about
%%% the change - processes usually read environment variables only during initialization).
%%%===================================================================

-spec master_down() -> ok.
master_down() ->
    ?info("Set and broadcast master_down"),
    set_slave_mode(?FAILOVER_SLAVE_MODE),
    broadcast_management_message(?MASTER_DOWN).


-spec master_up() -> ok.
master_up() ->
    ?info("Set and broadcast master_up"),
    set_slave_mode(?STANDBY_SLAVE_MODE),
    broadcast_management_message(?MASTER_UP).


-spec change_config(non_neg_integer(), propagation_method()) -> ok.
change_config(NodesNumber, PropagationMethod) ->
    ?info("Set and broadcast new ha config: nodes number: ~p, propagation method: ~p", [NodesNumber, PropagationMethod]),
    consistent_hashing:set_label_associated_nodes_count(NodesNumber),
    clean_backup_nodes_cache(),
    set_propagation_method(PropagationMethod),
    broadcast_management_message(?CONFIG_CHANGED).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_backup_nodes(MyNode :: node(), AllNodes :: [node()]) -> BackupNodes :: [node()].
get_backup_nodes(MyNode, [MyNode | Nodes]) ->
    Nodes;
get_backup_nodes(MyNode, [Node | Nodes]) ->
    get_backup_nodes(MyNode, Nodes ++ [Node]).