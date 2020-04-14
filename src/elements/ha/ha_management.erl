%%%-------------------------------------------------------------------
%%% @author michal
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Apr 2020 17:48
%%%-------------------------------------------------------------------
-module(ha_management).
-author("michal").

%% API
-export([node_down/1, node_up/1]).

node_down(Node) ->
    case ha_datastore:is_master_node(Node) of
        true ->
            ok = internal_services_manager:takeover(Node),
            ok = ha_datastore:set_failover_mode_and_broadcast_master_down_message();
        false ->
            ok
    end.

node_up(Node) ->
    ok.