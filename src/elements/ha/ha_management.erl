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

%%%===================================================================
%%% API - Working in failover mode
%%%===================================================================

-spec node_down(node()) -> ok | no_return().
node_down(Node) ->
    ok = consistent_hashing:report_node_failure(Node),
    IsMaster = ha_datastore:is_master(Node),
    case IsMaster of
        true ->
            ok = ha_datastore:set_failover_mode_and_broadcast_master_down_message(),
            ok = internal_services_manager:takeover(Node);
        false ->
            ok
    end,
    ok = plugins:apply(node_manager_plugin, node_down, [Node, IsMaster]).

-spec node_up(node()) -> ok | no_return().
node_up(Node) ->
    % TODO - sprawdzic czy jest masterem dla wstajacego slave'a i wyslac tam kopie (uwage na race z aktualnymi zapisami)
    ok = consistent_hashing:report_node_recovery(Node),

    IsMaster = ha_datastore:is_master(Node),
    case IsMaster of
        true ->
            ok = ha_datastore:set_standby_mode_and_broadcast_master_up_message();
        false ->
            ok
    end,

    IsSlave = ha_datastore:is_slave(Node),
    case IsSlave of
        true ->
            ok = ha_datastore:init_memory_backup();
        false ->
            ok
    end,

    ok = plugins:apply(node_manager_plugin, node_recovery, [Node, IsMaster]).

-spec node_ready(node()) -> ok | no_return().
node_ready(Node) ->
    % TODO - moze powinny byc 2 callbacki - node_up i node_ready?
%%    IsMaster = ha_datastore:is_master(Node),
%%    ok = plugins:apply(node_manager_plugin, node_recovery, [Node, IsMaster]),
    IsMaster = ha_datastore:is_master(Node),
    case IsMaster of
        true ->
            ok = internal_services_manager:migrate_to_recovered_master(Node);
        false ->
            ok
    end.