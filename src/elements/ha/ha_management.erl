%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module for high level HA management. It reacts on other nodes failures/recovery.
%%% It uses datastore HA (see ha_datastore.hrl) and internal_services_manager
%%% (see internal_services_manager.erl).
%%% @end
%%%-------------------------------------------------------------------
-module(ha_management).
-author("Michał Wrzeszcz").

%% API
-export([node_down/1, node_up/1, node_ready/1]).

-type node_relationship() :: master | non_master.

%%%===================================================================
%%% API - Working in failover mode
%%%===================================================================

-spec node_down(node()) -> node_relationship() | no_return().
node_down(Node) ->
    % TODO VFS-6388 - maybe send message to all tp processes that slave is down to unlink slave proc
    ok = consistent_hashing:report_node_failure(Node),
    case ha_datastore:is_master(Node) of
        true ->
            ok = ha_datastore:set_failover_mode_and_broadcast_master_down_message(),
            ok = internal_services_manager:takeover(Node),
            master;
        false ->
            non_master
    end.

-spec node_up(node()) -> node_relationship() | no_return().
node_up(Node) ->
    ok = consistent_hashing:report_node_recovery(Node),

    Relationship = case ha_datastore:is_master(Node) of
        true ->
            ok = ha_datastore:replicate_propagation_method_settings_to_node(Node),
            ok = ha_datastore:set_standby_mode_and_broadcast_master_up_message(),
            master;
        false ->
            non_master
    end,

    case ha_datastore:is_slave(Node) of
        true ->
            ok = ha_datastore:init_memory_backup();
        false ->
            ok
    end,
    Relationship.

-spec node_ready(node()) -> node_relationship() | no_return().
node_ready(Node) ->
    case ha_datastore:is_master(Node) of
        true ->
            ok = internal_services_manager:migrate_to_recovered_master(Node),
            master;
        false ->
            non_master
    end.