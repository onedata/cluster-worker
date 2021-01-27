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
-include_lib("ctool/include/errors.hrl").

%% API
-export([adjust_min_on_left/3, adjust_max_on_right/2]).
-export([get_max_key_in_prev_nodes/1]).
-export([get_starting_node_id/2]).

%%=====================================================================
%% API
%%=====================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function recursively updates `min_on_left` in given node and in all 
%% nodes to the right (pointed by prev) that are outdated. When CheckToTheEnd 
%% is set to `true` function will always adjust this value until reaching last node.
%% If elements are added to structure as recommended (i.e with increasing keys, 
%% see `sliding_proplist` module doc) at most one node will be updated.
%% @end
%%--------------------------------------------------------------------
-spec adjust_min_on_left(sliding_proplist:id() | undefined, sliding_proplist:key(), CheckToTheEnd :: boolean()) -> ok.
adjust_min_on_left(undefined, _CurrentMin, _CheckToTheEnd) ->
    ok;
adjust_min_on_left(#node{} = Node, CurrentMin, CheckToTheEnd) ->
    #node{node_id = NodeId, min_on_left = PreviousMinOnLeft} = Node,
    case PreviousMinOnLeft of
        CurrentMin -> ok;
        _ -> sliding_proplist_persistence:save_node(NodeId, Node#node{min_on_left = CurrentMin})
    end,
    Min = case maps:size(Node#node.elements) of
        0 -> CurrentMin;
        _ -> min(CurrentMin, lists:min(maps:keys(Node#node.elements)))
    end,
    case CheckToTheEnd orelse (PreviousMinOnLeft =/= CurrentMin orelse CurrentMin > Min) of
        true -> adjust_min_on_left(Node#node.prev, Min, CheckToTheEnd);
        _ -> ok
    end;
adjust_min_on_left(NodeId, CurrentMin, CheckToTheEnd) ->
    {ok, Node} = sliding_proplist_persistence:get_node(NodeId),
    adjust_min_on_left(Node, CurrentMin, CheckToTheEnd).


%%--------------------------------------------------------------------
%% @doc
%% This function recursively updates `max_on_right` in given node and in all 
%% nodes to the left (pointed by next) that are outdated. 
%% If elements are added to structure as recommended (i.e with increasing keys, 
%% see `sliding_proplist` module doc) at most one node will be updated.
%% @end
%%--------------------------------------------------------------------
-spec adjust_max_on_right(undefined | sliding_proplist:id() | sliding_proplist:list_node(), sliding_proplist:key() | undefined) -> ok.
adjust_max_on_right(undefined, _) ->
    ok;
adjust_max_on_right(#node{} = Node, CurrentMax) ->
    #node{node_id = NodeId, next = Next, max_on_right = PreviousMaxOnRight} = Node,
    case CurrentMax of
        PreviousMaxOnRight -> ok;
        _ ->
            sliding_proplist_persistence:save_node(NodeId, Node#node{max_on_right = CurrentMax}),
            MaxInNode = lists:max(maps:keys(Node#node.elements)),
            case CurrentMax of
                undefined -> adjust_max_on_right(Next, MaxInNode);
                _ -> adjust_max_on_right(Next, max(CurrentMax, MaxInNode))
            end
    end;
adjust_max_on_right(NodeId, CurrentMax) ->
    {ok, Node} = sliding_proplist_persistence:get_node(NodeId),
    adjust_max_on_right(Node, CurrentMax).


%%--------------------------------------------------------------------
%% @doc
%% Based on `max_on_right` value and maximum key in given node, this 
%% function returns highest key in all nodes to the right (pointed by prev) 
%% and given node.
%% @end
%%--------------------------------------------------------------------
-spec get_max_key_in_prev_nodes(undefined | sliding_proplist:list_node()) -> sliding_proplist:key() | undefined.
get_max_key_in_prev_nodes(undefined) -> undefined;
get_max_key_in_prev_nodes(#node{elements = Elements, max_on_right = MaxOnRight}) ->
    case maps:size(Elements) of
        0 -> MaxOnRight;
        _ ->
            MaxInNode = lists:max(maps:keys(Elements)),
            case MaxOnRight of
                undefined -> MaxInNode;
                _ -> max(MaxInNode, MaxOnRight)
            end
    end.


-spec get_starting_node_id(sliding_proplist_get:direction(), sliding_proplist:sentinel()) -> 
    sliding_proplist:id() | undefined.
get_starting_node_id(back_from_newest, #sentinel{first = First}) -> First;
get_starting_node_id(forward_from_oldest, #sentinel{last = Last}) -> Last.
