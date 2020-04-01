%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used to configure HA.
%%% NOTE: Functions in this module work node-wide as failures concern whole nodes.
%%% NOTE: Config functions should be executed on all nodes during cluster reconfiguration.
%%% TODO - VFS-6166 - Verify HA Cast
%%% TODO - VFS-6167 - Datastore HA supports nodes adding and deleting
%%% For more information see ha_datastore.hrl.
%%% @end
%%%-------------------------------------------------------------------
-module(ha_datastore).
-author("Michał Wrzeszcz").

-include("modules/datastore/ha_datastore.hrl").
-include("modules/datastore/datastore_protocol.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/hashing/consistent_hashing.hrl").
-include_lib("ctool/include/logging.hrl").

%% Message sending API
-export([send_async_internal_message/2, send_sync_internal_message/2,
    send_async_slave_message/2, send_sync_slave_message/2,
    send_async_master_message/2, send_sync_master_message/4,
    broadcast_async_management_message/1]).
%% API
-export([get_propagation_method/0, get_backup_nodes/0, get_slave_mode/0]).
-export([set_failover_mode_and_broadcast_master_down_message/0, set_standby_mode_and_broadcast_master_up_message/0,
    change_config/2]).
-export([reconfigure_cluster/0, finish_reconfiguration/0, check_migration/1, reconfiguration_nodes_to_check/0]).

% Propagation methods - see ha_datastore.hrl
-type propagation_method() :: ?HA_CALL_PROPAGATION | ?HA_CAST_PROPAGATION.
% Slave working mode -  see ha_datastore.hrl
-type slave_mode() :: ?STANDBY_SLAVE_MODE | ?FAILOVER_SLAVE_MODE | ?CLUSTER_RECONFIGURATION_SLAVE_MODE.

-export_type([propagation_method/0, slave_mode/0]).

% HA messages' types (see ha_datastore.hrl)
-type ha_message_type() :: master | slave | internal | management.
-type ha_message() :: ha_datastore_slave:backup_message() | ha_datastore_master:unlink_request() |
    ha_datastore_master:failover_request_data_processed_message() | ha_datastore_slave:get_slave_failover_status() |
    ha_datastore_slave:master_node_status_message() | ha_datastore_master:config_changed_message() |
    ha_datastore_slave:reconfiguration_message().

-type cluster_reconfiguration_request() :: #cluster_reconfiguration{}.

-export_type([ha_message_type/0, ha_message/0, cluster_reconfiguration_request/0]).

% Internal module types
-type nodes_assigned_per_key() :: pos_integer().

-define(MEMORY_COPY_BATCH_SIZE, 200).

%%%===================================================================
%%% Message sending API
%%%===================================================================

-spec send_async_internal_message(pid(), ha_datastore_master:failover_request_data_processed_message() |
    ha_datastore_slave:master_node_status_message() | ha_datastore_master:config_changed_message()) -> ok.
send_async_internal_message(Pid, Msg) ->
    gen_server:cast(Pid, ?INTERNAL_MSG(Msg)).

-spec send_sync_internal_message(pid(), ha_datastore_master:failover_request_data_processed_message() |
    ha_datastore_slave:master_node_status_message() | ha_datastore_master:config_changed_message()) -> ok.
send_sync_internal_message(Pid, Msg) ->
    gen_server:call(Pid, ?INTERNAL_MSG(Msg), infinity).

-spec send_async_slave_message(pid(), ha_datastore_master:failover_request_data_processed_message()) -> ok.
send_async_slave_message(Pid, Msg) ->
    gen_server:cast(Pid, ?SLAVE_MSG(Msg)).

-spec send_sync_slave_message(pid(), ha_datastore_master:unlink_request()) -> term().
send_sync_slave_message(Pid, Msg) ->
    gen_server:call(Pid, ?SLAVE_MSG(Msg), infinity).

-spec send_async_master_message(pid(), ha_datastore_slave:backup_message()) -> ok.
send_async_master_message(Pid, Msg) ->
    gen_server:cast(Pid, ?MASTER_MSG(Msg)).

-spec send_sync_master_message(node(), datastore:key(), ha_datastore_slave:backup_message() |
    ha_datastore_slave:get_slave_failover_status() | #datastore_internal_requests_batch{}, StartIfNotAlive :: boolean()) ->
    term().
send_sync_master_message(Node, ProcessKey, Msg, true) ->
    rpc:call(Node, datastore_writer, generic_call, [ProcessKey, ?MASTER_MSG(Msg)]);
send_sync_master_message(Node, ProcessKey, Msg, _StartIfNotAlive) ->
    rpc:call(Node, datastore_writer, call_if_alive, [ProcessKey, ?MASTER_MSG(Msg)]).

-spec broadcast_async_management_message(ha_datastore_slave:master_node_status_message() |
    ha_datastore_master:config_changed_message()) -> ok.
broadcast_async_management_message(Msg) ->
    tp_router:send_to_each(?MANAGEMENT_MSG(Msg)).

-spec broadcast_sync_management_message(ha_datastore_slave:reconfiguration_message()) -> ok | {error, term()}.
broadcast_sync_management_message(Msg) ->
    tp_router:send_to_each_and_wait_for_ans(?MANAGEMENT_MSG(Msg)).

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
                Ans = case consistent_hashing:get_nodes_assigned_per_label() of
                    1 ->
                        [];
                    BackupNodesNum ->
                        Nodes = arrange_nodes(node(), consistent_hashing:get_all_nodes()),
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

-spec set_failover_mode_and_broadcast_master_down_message() -> ok.
set_failover_mode_and_broadcast_master_down_message() ->
    ?notice("Master node down: setting failover mode and broadcasting information to tp processes"),
    set_slave_mode(?FAILOVER_SLAVE_MODE),
    broadcast_async_management_message(?MASTER_DOWN).


-spec set_standby_mode_and_broadcast_master_up_message() -> ok.
set_standby_mode_and_broadcast_master_up_message() ->
    ?notice("Master node up: seting standby mode and broadcasting information to tp processes"),
    set_slave_mode(?STANDBY_SLAVE_MODE),
    broadcast_async_management_message(?MASTER_UP).


-spec change_config(nodes_assigned_per_key(), propagation_method()) -> ok.
change_config(NodesNumber, PropagationMethod) ->
    ?notice("New HA configuration: nodes number: ~p, propagation method: ~p - setting environment variables"
        " and broadcasting information to tp processes~n", [NodesNumber, PropagationMethod]),
    consistent_hashing:set_nodes_assigned_per_label(NodesNumber),
    clean_backup_nodes_cache(),
    set_propagation_method(PropagationMethod),
    broadcast_async_management_message(?CONFIG_CHANGED).

%%%===================================================================
%%% API to reconfigure cluster
%%%===================================================================

-spec reconfigure_cluster() -> ok | no_return().
reconfigure_cluster() ->
    set_slave_mode(?CLUSTER_RECONFIGURATION_SLAVE_MODE),
    ok = broadcast_sync_management_message(?CLUSTER_RECONFIGURATION),

    Mutator = self(),
    ok = datastore_model:foreach_memory_key(fun
        (_, end_of_memory, _Doc, Acc) ->
            {ok, copy_memory(Acc)};
        (Model, Key, Doc, Acc) ->
            RoutingKey = datastore_router:get_routing_key(Doc),
            {Acc2, CopyNow} = case check_migration(RoutingKey) of
                {migrate_to_new_master, Node} ->
                    Ctx = datastore_model_default:get_ctx(Model, RoutingKey),
                    Ctx2 = Ctx#{mutator_pid => Mutator},
                    NodeAcc = maps:get(Node, Acc, []),
                    NewNodeAcc = [{Ctx2, Key, Doc} | NodeAcc],
                    NewAcc = maps:put(Node, NewNodeAcc, Acc),
                    {NewAcc, length(NewNodeAcc) >= ?MEMORY_COPY_BATCH_SIZE};
                _ ->
                    {Acc, false}
            end,
            case CopyNow of
                true ->
                    case copy_memory(Acc2) of
                        ok -> {ok, #{}};
                        Other -> {stop, Other}
                    end;
                _ ->
                    {ok, Acc2}
            end
    end, #{}).

-spec finish_reconfiguration() -> ok.
finish_reconfiguration() ->
    set_slave_mode(?STANDBY_SLAVE_MODE).

-spec check_migration(datastore:key()) -> local_key | {migrate_to_new_master, node()}.
check_migration(Key) ->
    LocalNode = node(),
    Seed = datastore_key:get_chash_seed(Key),
    #node_routing_info{assigned_nodes = [NewNode | _]} = consistent_hashing:get_reconfigured_routing_info(Seed),
    case NewNode of
        LocalNode -> local_key;
        _ -> {migrate_to_new_master, NewNode}
    end.

-spec reconfiguration_nodes_to_check() -> [node()].
reconfiguration_nodes_to_check() ->
    Node = node(),
    CurrentNodes = consistent_hashing:get_all_nodes(),
    ReconfigurationNodes = consistent_hashing:get_reconfiguration_nodes(),
    lists:usort(get_neighbors(Node, CurrentNodes) ++ get_neighbors(Node, ReconfigurationNodes)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns nodes' list without local node, starting from the node that is next after local node at the original list
%% (function wraps list if needed).
%% @end
%%--------------------------------------------------------------------
-spec arrange_nodes(MyNode :: node(), AllNodes :: [node()]) -> ArrangedNodes :: [node()].
arrange_nodes(MyNode, [MyNode | Nodes]) ->
    Nodes;
arrange_nodes(MyNode, [Node | Nodes]) ->
    arrange_nodes(MyNode, Nodes ++ [Node]).

-spec get_neighbors(node(), [node()]) -> [node()].
get_neighbors(Node, AllNodes) ->
    case lists:member(Node, AllNodes) of
        true ->
            [N1 | _] = arrange_nodes(Node, AllNodes),
            [N2 | _] = arrange_nodes(Node, lists:reverse(AllNodes)),
            [N1, N2];
        _ ->
            []
    end.

-spec copy_memory(#{node() => [datastore_cache:cache_save_request()]}) -> ok | {error, term()}.
copy_memory(ItemsMap) ->
    maps:fold(fun
        (Node, Items, ok) -> copy_memory(Node, Items);
        (_Node, _Items, Acc) -> Acc
    end, ok, ItemsMap).


-spec copy_memory(node(), [datastore_cache:cache_save_request()]) -> ok | {error, term()}.
copy_memory(Node, Items) ->
    Ans = rpc:call(Node, datastore_cache, save, [Items]),
    case Ans of
        {badrpc, Reason} ->
            {error, Reason};
        _ ->
            % TODO - moze trzeba wyczyscic klucze zestarego node'a?
            FoldlAns = lists:foldl(fun
                (_, {error, _} = Error) -> Error;
                (ItemAns, _) -> ItemAns
            end, {ok, ok, ok}, Ans),
            case FoldlAns of
                {ok, _, _} -> ok;
                Other -> Other
            end
    end.