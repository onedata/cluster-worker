%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains utility functions used across `sliding_proplist` modules.
%%% @end
%%%-------------------------------------------------------------------
-module(sliding_proplist_utils).
-author("Michal Stanisz").

-include("modules/datastore/sliding_proplist.hrl").

%% API
-export([adjust_min_key_in_newer_nodes/3, adjust_max_key_in_older_nodes/2]).
-export([get_max_key_in_current_and_older_nodes/1, get_min_key_in_current_and_newer_nodes/1]).
-export([get_starting_node_id/2]).

%%=====================================================================
%% API
%%=====================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function recursively updates `min_key_in_newer_nodes` in given node and in all 
%% nodes to the right (pointed by prev) that are outdated. When CheckToTheEnd 
%% is set to `true` function will always adjust this value until reaching last node.
%% If elements were added to sliding proplist instance as recommended (i.e with increasing 
%% keys, @see `sliding_proplist` module doc) at most one node will be updated.
%% @end
%%--------------------------------------------------------------------
-spec adjust_min_key_in_newer_nodes(sliding_proplist:node_id() | undefined | sliding_proplist:list_node(), 
    sliding_proplist:key(), CheckToTheEnd :: boolean()) -> ok.
adjust_min_key_in_newer_nodes(undefined, _MinSoFar, _CheckToTheEnd) ->
    ok;
adjust_min_key_in_newer_nodes(#node{} = Node, MinSoFar, CheckToTheEnd) ->
    #node{node_id = NodeId, min_key_in_newer_nodes = PreviousMinInNewer, min_key_in_node = MinInNode} = Node,
    case PreviousMinInNewer of
        MinSoFar -> ok;
        _ -> sliding_proplist_persistence:save_record(NodeId, Node#node{min_key_in_newer_nodes = MinSoFar})
    end,
    Min = min(MinSoFar, MinInNode),
    case CheckToTheEnd orelse (PreviousMinInNewer =/= MinSoFar orelse MinSoFar > Min) of
        true -> adjust_min_key_in_newer_nodes(Node#node.prev, Min, CheckToTheEnd);
        _ -> ok
    end;
adjust_min_key_in_newer_nodes(NodeId, CurrentMin, CheckToTheEnd) ->
    {ok, Node} = sliding_proplist_persistence:get_record(NodeId),
    adjust_min_key_in_newer_nodes(Node, CurrentMin, CheckToTheEnd).


%%--------------------------------------------------------------------
%% @doc
%% This function recursively updates `max_key_in_older_nodes` in given node and in all 
%% nodes to the left (pointed by next) that are outdated. 
%% If elements were added to sliding proplist instance as recommended (i.e with 
%% increasing keys, @see `sliding_proplist` module doc) at most one node will be updated.
%% @end
%%--------------------------------------------------------------------
-spec adjust_max_key_in_older_nodes(undefined | sliding_proplist:node_id() | sliding_proplist:list_node(), 
    sliding_proplist:key() | undefined) -> ok.
adjust_max_key_in_older_nodes(undefined, _) ->
    ok;
adjust_max_key_in_older_nodes(#node{} = Node, MaxSoFar) ->
    #node{
        node_id = NodeId, next = Next, max_key_in_older_nodes = PreviousMaxInOlder, max_key_in_node = MaxInNode
    } = Node,
    case MaxSoFar of
        PreviousMaxInOlder -> ok;
        _ ->
            sliding_proplist_persistence:save_record(NodeId, Node#node{max_key_in_older_nodes = MaxSoFar}),
            case MaxSoFar of
                undefined -> adjust_max_key_in_older_nodes(Next, MaxInNode);
                _ -> adjust_max_key_in_older_nodes(Next, max(MaxSoFar, MaxInNode))
            end
    end;
adjust_max_key_in_older_nodes(NodeId, CurrentMax) ->
    {ok, Node} = sliding_proplist_persistence:get_record(NodeId),
    adjust_max_key_in_older_nodes(Node, CurrentMax).


%%--------------------------------------------------------------------
%% @doc
%% Based on `max_in_older_nodes` value and maximal key in given node, 
%% this function returns highest key in all older nodes (pointed by prev) 
%% and given node.
%% @end
%%--------------------------------------------------------------------
-spec get_max_key_in_current_and_older_nodes(undefined | sliding_proplist:list_node()) -> 
    sliding_proplist:key() | undefined.
get_max_key_in_current_and_older_nodes(undefined) -> undefined;
get_max_key_in_current_and_older_nodes(#node{max_key_in_older_nodes = MaxInOlder, max_key_in_node = MaxInNode}) ->
    case {MaxInNode, MaxInOlder} of
        {undefined, _} -> MaxInOlder;
        {_, undefined} -> MaxInNode;
        _ -> max(MaxInNode, MaxInOlder)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Based on `min_in_newer_nodes` value and minimal key in given node, 
%% this function returns smallest key in all newer nodes (pointed by next) 
%% and given node.
%% @end
%%--------------------------------------------------------------------
-spec get_min_key_in_current_and_newer_nodes(undefined | sliding_proplist:list_node()) ->
    sliding_proplist:key() | undefined.
get_min_key_in_current_and_newer_nodes(undefined) -> undefined;
get_min_key_in_current_and_newer_nodes(#node{min_key_in_newer_nodes = MinInNewer, min_key_in_node = MinInNode}) ->
    case {MinInNode, MinInNewer} of
        {undefined, _} -> MinInNewer;
        {_, undefined} -> MinInNode;
        _ -> min(MinInNode, MinInNewer)
    end.


-spec get_starting_node_id(sliding_proplist_get:direction(), sliding_proplist:sentinel()) -> 
    sliding_proplist:node_id() | undefined.
get_starting_node_id(back_from_newest, #sentinel{first = First}) -> First;
get_starting_node_id(forward_from_oldest, #sentinel{last = Last}) -> Last.
