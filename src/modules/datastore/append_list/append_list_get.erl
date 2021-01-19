%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that are responsible for retrieving
%%% elements from append_list. For more details about this structure 
%%% consult `append_list` module doc.
%%% @end
%%%-------------------------------------------------------------------
-module(append_list_get).
-author("Michal Stanisz").

-include("modules/datastore/datastore_append_list.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([list/3, get/2, get_highest/1, get_max_key/1]).

%%=====================================================================
%% API
%%=====================================================================

-spec list(#internal_listing_data{}, Size :: non_neg_integer(), [append_list:elem()]) ->
    {[append_list:elem()], #listing_info{}}.
list(#internal_listing_data{last_node_id = undefined}, _Size, Acc) ->
    {Acc, #listing_info{
        finished = true
    }};
list(#internal_listing_data{last_node_id = NodeId} = ListingData, _Size = 0, Acc) ->
    {L, _} = lists:last(Acc),
    {Acc, #listing_info{
        finished = false,
        internal_listing_data = ListingData#internal_listing_data{
            last_node_id = NodeId,
            last_key = L
        }
    }};
list(#internal_listing_data{
    last_node_id = NodeId,
    last_key = LastKey,
    last_node_num = LastNodeNum
} = ListingData, Size, Acc) ->
    case find_node(ListingData) of
        #node{elements = OrigElements, node_num = NodeNum} = Node ->
            Elements = case LastKey == undefined orelse LastNodeNum =/= NodeNum of
                true -> OrigElements;
                false ->
                    % when node since last listing was not deleted select only 
                    % elements with key less than those already listed
                    maps:filter(fun(Key, _) -> Key < LastKey end, OrigElements)
            end,
            NumberOfElemsToTake = min(Size, maps:size(Elements)),
            {E, Rest} = lists:split(NumberOfElemsToTake,
                lists:reverse(lists:sort(maps:to_list(Elements)))),
            NextNodeId = case Rest of
                [] -> Node#node.prev;
                _ -> NodeId
            end,
            list(ListingData#internal_listing_data{
                last_node_id = NextNodeId,
                last_node_num = NodeNum
            }, Size - NumberOfElemsToTake, Acc ++ E)
    end.


-spec get(id() | undefined, append_list:key()) -> ?ERROR_NOT_FOUND | {ok, append_list:value()}.
get(undefined, _Key) ->
    ?ERROR_NOT_FOUND;
get(NodeId, Key) ->
    case append_list_persistence:get_node(NodeId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        #node{elements = Elements, max_on_right = MaxOnRight, prev = Prev} ->
            case maps:find(Key, Elements) of
                {ok, Value} -> {ok, Value};
                error -> case Key > MaxOnRight of
                    true -> ?ERROR_NOT_FOUND;
                    false -> get(Prev, Key)
                end
            end
    end.


-spec get_highest(undefined | id()) -> append_list:elem() | ?ERROR_NOT_FOUND.
get_highest(undefined) -> ?ERROR_NOT_FOUND;
get_highest(NodeId) ->
    case append_list_persistence:get_node(NodeId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        #node{elements = Elements, max_on_right = MaxOnRight, prev = Prev} ->
            case {Prev == undefined, maps:size(Elements) > 0 andalso lists:max(maps:keys(Elements))} of
                {true, false} -> ?ERROR_NOT_FOUND;
                {true, MaxInNode} -> {MaxInNode, maps:get(MaxInNode, Elements)};
                {false, MaxInNode} when MaxInNode > MaxOnRight ->
                    {MaxInNode, maps:get(MaxInNode, Elements)};
                {false, _} -> get_highest(Prev)
            end
    end.


-spec get_max_key(undefined | id()) -> append_list:key() | ?ERROR_NOT_FOUND.
get_max_key(undefined) -> ?ERROR_NOT_FOUND;
get_max_key(NodeId) ->
    case append_list_persistence:get_node(NodeId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        Node -> case append_list_utils:get_max_key_in_prev_nodes(Node) of
            undefined -> ?ERROR_NOT_FOUND;
            Res -> Res
        end
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function returns Node based on information included in ListingData.
%% If node with given id exists it is returned. If not (it was deleted in 
%% meantime) first node with number less than `last_node_num` is returned.
%% Because last node is never deleted, such node always exists.
%% @end
%%--------------------------------------------------------------------
-spec find_node(#internal_listing_data{}) -> #node{}.
find_node(#internal_listing_data{last_node_id = NodeId, last_node_num = LastNodeNum, structure_id = StructId} = ListingData) ->
    case append_list_persistence:get_node(NodeId) of
        ?ERROR_NOT_FOUND -> 
            #sentinel{first = First} = append_list_persistence:get_node(StructId),
            find_node(ListingData#internal_listing_data{last_node_id = First});
        #node{node_num = NodeNum} = Node when not (is_integer(LastNodeNum) andalso NodeNum > LastNodeNum) ->
            Node;
        #node{prev = Prev} ->
            find_node(ListingData#internal_listing_data{last_node_id = Prev})
    end.
