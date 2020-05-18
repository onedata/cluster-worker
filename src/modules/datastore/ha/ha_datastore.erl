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
%%% NOTE: Config functions should be executed on all nodes during cluster reorganization.
%%% TODO - VFS-6166 - Verify HA Cast
%%% TODO - VFS-6167 - Datastore HA supports nodes adding and deleting
%%% For more information see ha_datastore.hrl.
%%% @end
%%%-------------------------------------------------------------------
-module(ha_datastore).
-author("Michał Wrzeszcz").

-include("modules/datastore/ha_datastore.hrl").
-include("modules/datastore/datastore_protocol.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/hashing/consistent_hashing.hrl").
-include_lib("ctool/include/logging.hrl").

%% Message sending API
-export([send_async_internal_message/2, send_sync_internal_message/2,
    send_async_slave_message/2, send_sync_slave_message/2,
    send_async_master_message/2, send_sync_master_message/4,
    broadcast_async_management_message/1]).
%% API
-export([get_propagation_method/0, get_backup_nodes/0, get_backup_nodes/1, is_master/1, is_slave/1, get_slave_mode/0]).
-export([set_failover_mode_and_broadcast_master_down_message/0, set_standby_mode_and_broadcast_master_up_message/0,
    change_config/2]).
-export([reorganize_cluster/0, finish_reorganization/0, qualify_by_key/2, possible_neighbors_during_reconfiguration/0]).
-export([init_memory_backup/0]).

% Propagation methods - see ha_datastore.hrl
-type propagation_method() :: ?HA_CALL_PROPAGATION | ?HA_CAST_PROPAGATION.
% Slave working mode -  see ha_datastore.hrl
-type slave_mode() :: ?STANDBY_SLAVE_MODE | ?FAILOVER_SLAVE_MODE | ?CLUSTER_REORGANIZATION_SLAVE_MODE.

-export_type([propagation_method/0, slave_mode/0]).

% HA messages' types (see ha_datastore.hrl)
-type ha_message_type() :: master | slave | internal | management.
-type ha_message() :: ha_datastore_slave:backup_message() | ha_datastore_master:unlink_request() |
    ha_datastore_master:failover_request_data_processed_message() | ha_datastore_slave:get_slave_failover_status() |
    ha_datastore_slave:master_node_status_message() | ha_datastore_master:config_changed_message() |
    ha_datastore_slave:reorganization_message().

-type cluster_reorganization_request() :: #cluster_reorganization_started{}.

-export_type([ha_message_type/0, ha_message/0, cluster_reorganization_request/0]).

% Internal module types
-type nodes_assigned_per_key() :: pos_integer().
-type memory_copy_acc() :: #{node() | undefined => [datastore_cache:cache_save_request()]}.

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
    gen_server:call(Pid, ?INTERNAL_MSG(Msg), 5000). % TODO VFS-6169 - use infinity timeout

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
    tp_router:broadcast(?MANAGEMENT_MSG(Msg)).

-spec broadcast_sync_management_message(ha_datastore_slave:reorganization_message()) -> ok | {error, term()}.
broadcast_sync_management_message(Msg) ->
    tp_router:broadcast_and_await_answer(?MANAGEMENT_MSG(Msg)).

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
                Ans = get_backup_nodes(node()),
                application:set_env(?CLUSTER_WORKER_APP_NAME, ha_backup_nodes, Ans),
                Ans
            end)
    end.

-spec get_backup_nodes(node()) -> [node()].
get_backup_nodes(Node) ->
    case consistent_hashing:get_nodes_assigned_per_label() of
        1 ->
            [];
        BackupNodesNum ->
            AllNodes = consistent_hashing:get_all_nodes(),
            case lists:member(Node, AllNodes) of
                true ->
                    Nodes = arrange_nodes(Node, AllNodes),
                    lists:sublist(Nodes, min(BackupNodesNum - 1, length(Nodes)));
                _ ->
                    []
            end
    end.

-spec is_master(node()) -> boolean().
is_master(Node) ->
    case consistent_hashing:get_nodes_assigned_per_label() of
        1 -> % HA is disabled
            false;
        _ ->
            [SlaveNode | _] = arrange_nodes(Node, consistent_hashing:get_all_nodes()),
            SlaveNode =:= node()
    end.

-spec is_slave(node()) -> boolean().
is_slave(Node) ->
    case consistent_hashing:get_nodes_assigned_per_label() of
        1 -> % HA is disabled
            false;
        _ ->
            [SlaveNode | _] = arrange_nodes(node(), consistent_hashing:get_all_nodes()),
            SlaveNode =:= Node
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
    copy_keys(?CURRENT_RING, remote),
    set_slave_mode(?STANDBY_SLAVE_MODE),
    broadcast_async_management_message(?MASTER_UP).


-spec change_config(nodes_assigned_per_key(), propagation_method()) -> ok.
change_config(NodesNumber, PropagationMethod) ->
    ?notice("New HA configuration: nodes number: ~p, propagation method: ~p - setting environment variables"
        " and broadcasting information to tp processes~n", [NodesNumber, PropagationMethod]),
    consistent_hashing:set_nodes_assigned_per_label(NodesNumber),
    clean_backup_nodes_cache(),
    set_propagation_method(PropagationMethod),
    broadcast_async_management_message(?CONFIG_CHANGED),
    case NodesNumber > 1 of
        true -> ok = init_memory_backup();
        _ -> ok
    end.

%%%===================================================================
%%% API to reorganize cluster
%%%===================================================================

-spec reorganize_cluster() -> ok | no_return().
reorganize_cluster() ->
    set_slave_mode(?CLUSTER_REORGANIZATION_SLAVE_MODE),
    ok = broadcast_sync_management_message(?CLUSTER_REORGANIZATION_STARTED),
    copy_keys(?FUTURE_RING, remote).

-spec finish_reorganization() -> ok.
finish_reorganization() ->
    % TODO VFS-6169 - inactivate all memory cells migrated to new node
    set_slave_mode(?STANDBY_SLAVE_MODE).

%%--------------------------------------------------------------------
%% @doc
%% Check whether key should be handled locally or on remote node.
%% Can be used to change current or future node responsible for the key depending on ring generation.
%% @end
%%--------------------------------------------------------------------
-spec qualify_by_key(datastore:key(), consistent_hashing:ring_generation()) -> local_key | {remote_key, node()}.
qualify_by_key(Key, Generation) ->
    LocalNode = node(),
    Seed = datastore_key:get_chash_seed(Key),
    #node_routing_info{assigned_nodes = [NewNode | _]} = consistent_hashing:get_routing_info(Generation, Seed),
    case NewNode of
        LocalNode -> local_key;
        _ -> {remote_key, NewNode}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Slave could work on one of nodes returned by this function (depending on reorganization process).
%% @end
%%--------------------------------------------------------------------
-spec possible_neighbors_during_reconfiguration() -> [node()].
possible_neighbors_during_reconfiguration() ->
    Node = node(),
    CurrentNeighbors = get_neighbors(Node, get_ring_nodes_or_empty(?CURRENT_RING)),
    PreviousNeighbors = get_neighbors(Node, get_ring_nodes_or_empty(?PREVIOUS_RING)),
    lists:usort(CurrentNeighbors ++ PreviousNeighbors).

%%%===================================================================
%%% API - Memory management
%%%===================================================================

-spec init_memory_backup() -> ok | no_return().
init_memory_backup() ->
    copy_keys(?CURRENT_RING, local).

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

-spec get_ring_nodes_or_empty(consistent_hashing:ring_generation()) -> [node()].
get_ring_nodes_or_empty(RingGeneration) ->
    try
        consistent_hashing:get_all_nodes(RingGeneration)
    catch
        _:chash_ring_not_initialized  -> []
    end.

-spec copy_keys(consistent_hashing:ring_generation(), remote | local) -> ok | no_return().
copy_keys(Ring, DocsToCopy) ->
    ok = datastore_model:fold_memory_keys(fun
        (end_of_memory, Acc) ->
            {ok, copy_memory(Acc)};
        ({_Model, _Key, #document{deleted = true}}, Acc) ->
            {ok, Acc};
        ({Model, Key, Doc}, Acc) ->
            BasicCtx = datastore_model_default:get_basic_ctx(Model),
            RoutingKey = datastore_router:get_routing_key(BasicCtx, Doc),
            Ctx = datastore_model_default:set_defaults(RoutingKey, BasicCtx),
            {Acc2, CopyNow} = case {qualify_by_key(RoutingKey, Ring), DocsToCopy} of
                {{remote_key, Node}, remote} -> prepare_key_copy(Key, Doc, Ctx, Node, Acc);
                {local_key, local} -> prepare_key_copy(Key, Doc, Ctx, undefined, Acc);
                _ -> {Acc, false}
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

-spec prepare_key_copy(datastore:key(), datastore:doc(), datastore:ctx(), node() | undefined,
    memory_copy_acc()) -> {memory_copy_acc(), Flush :: boolean()}.
prepare_key_copy(Key, Doc, Ctx, Node, Acc) ->
    Ctx2 = Ctx#{mutator_pid => self()},
    ShouldCopy = case Ctx of
        #{routing := local} ->
            not maps:get(ha_disabled, Ctx, true);
        _ ->
            not maps:get(ha_disabled, Ctx, false)
    end,

    case ShouldCopy of
        false ->
            {Acc, false};
        true ->
            NodeAcc = maps:get(Node, Acc, []),
            NewNodeAcc = [{Ctx2, Key, Doc} | NodeAcc],
            NewAcc = maps:put(Node, NewNodeAcc, Acc),
            {NewAcc, length(NewNodeAcc) >= ?MEMORY_COPY_BATCH_SIZE}
    end.

-spec copy_memory(memory_copy_acc()) -> ok | {error, term()}.
copy_memory(ItemsMap) ->
    maps:fold(fun
        (undefined, Items, ok) -> create_backup(Items);
        (Node, Items, ok) -> copy_memory(Node, Items);
        (_Node, _Items, Acc) -> Acc
    end, ok, ItemsMap).


-spec copy_memory(node(), [datastore_cache:cache_save_request()]) -> ok | {error, term()}.
copy_memory(Node, Items) ->
    case rpc:call(Node, datastore_cache, save, [Items]) of
        {badrpc, Reason} ->
            {error, Reason};
        SaveResults ->
            % TODO VFS-6271 - delete keys from old node
            lists:foldl(fun
                (_, {error, _} = Error) -> Error;
                ({error, _} = Error, _) -> Error;
                ({ok, _, _}, _) -> ok
            end, ok, SaveResults)
    end.

-spec create_backup([datastore_cache:cache_save_request()]) -> ok | {error, term()}.
create_backup(Items) ->
    lists:foldl(fun
        ({Ctx, Key, _Doc} , ok) ->
            case datastore:create_backup(Ctx, Key) of
                {ok, _} -> ok;
                {error, not_found} -> ok;
                Error -> Error
            end;
        (_, Acc) ->
            Acc
    end, ok, Items).