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

-type adjust_strategy() :: check_to_the_end | basic.

%% API
-export([adjust_min_key_in_newer_nodes/3, adjust_max_key_in_older_nodes/2]).
-export([get_max_key_in_current_and_older_nodes/1, get_min_key_in_current_and_newer_nodes/1]).
-export([get_starting_node_id/2]).

%%=====================================================================
%% API
%%=====================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function recursively updates `min_key_in_newer_nodes` in given 
%% node and in all outdated nodes older than given (pointed by prev). 
%% Functions stops when `min_key_in_newer_nodes` in currently processed 
%% node is equal to minimal key seen so far (MinSoFar) unless 
%% `check_to_the_end` strategy is selected. Then function will always 
%% adjust `min_key_in_newer_nodes` until reaching oldest node.
%% @end
%%--------------------------------------------------------------------
-spec adjust_min_key_in_newer_nodes(sliding_proplist:node_id() | undefined | sliding_proplist:list_node(), 
    sliding_proplist:key(), adjust_strategy()) -> ok.
adjust_min_key_in_newer_nodes(undefined, _MinSoFar, _Strategy) ->
    ok;
adjust_min_key_in_newer_nodes(NodeId, CurrentMin, Strategy) when is_binary(NodeId) ->
    {ok, Node} = sliding_proplist_persistence:get_record(NodeId),
    adjust_min_key_in_newer_nodes(Node, CurrentMin, Strategy);
adjust_min_key_in_newer_nodes(#node{} = Node, MinSoFar, Strategy) ->
    #node{
        node_id = NodeId, prev = Prev,
        min_key_in_newer_nodes = PreviousMinKeyInNewer, 
        min_key_in_node = MinInNode
    } = Node,
    case PreviousMinKeyInNewer of
        MinSoFar -> ok;
        _ -> sliding_proplist_persistence:save_record(NodeId, Node#node{min_key_in_newer_nodes = MinSoFar})
    end,
    case Strategy == check_to_the_end orelse (PreviousMinKeyInNewer =/= MinSoFar) of
        true -> adjust_min_key_in_newer_nodes(Prev, min(MinSoFar, MinInNode), Strategy);
        _ -> ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% This function recursively updates `max_key_in_older_nodes` in given 
%% node and in all outdated nodes newer than given (pointed by next). 
%% Functions stops when `max_key_in_older_nodes` in currently processed 
%% node is equal to highest key seen so far (MaxSoFar).
%% @end
%%--------------------------------------------------------------------
-spec adjust_max_key_in_older_nodes(undefined | sliding_proplist:node_id() | sliding_proplist:list_node(), 
    sliding_proplist:key() | undefined) -> ok.
adjust_max_key_in_older_nodes(undefined, _) ->
    ok;
adjust_max_key_in_older_nodes(NodeId, CurrentMax) when is_binary(NodeId) ->
    {ok, Node} = sliding_proplist_persistence:get_record(NodeId),
    adjust_max_key_in_older_nodes(Node, CurrentMax);
adjust_max_key_in_older_nodes(#node{} = Node, MaxSoFar) ->
    #node{
        node_id = NodeId, next = Next, 
        max_key_in_older_nodes = PreviousMaxKeyInOlder, 
        max_key_in_node = MaxKeyInNode
    } = Node,
    case MaxSoFar of
        PreviousMaxKeyInOlder -> ok;
        _ ->
            sliding_proplist_persistence:save_record(NodeId, Node#node{max_key_in_older_nodes = MaxSoFar}),
            case MaxSoFar of
                undefined -> adjust_max_key_in_older_nodes(Next, MaxKeyInNode);
                _ -> adjust_max_key_in_older_nodes(Next, max(MaxSoFar, MaxKeyInNode))
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Based on `max_key_in_older_nodes` value and maximal key in given node, 
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
%% Based on `min_key_in_newer_nodes` value and minimal key in given node, 
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
