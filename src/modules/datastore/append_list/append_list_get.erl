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
-export([list/4, get/3, get_highest/1, get_max_key/1]).

%%=====================================================================
%% API
%%=====================================================================

-spec list(#internal_listing_data{}, Size :: non_neg_integer(), append_list:fold_fun(), [append_list:elem()]) ->
    {[append_list:elem()], #listing_info{}}.
list(#internal_listing_data{last_node_id = undefined}, _Size, _FoldFun, Acc) ->
    {Acc, #listing_info{
        finished = true
    }};
list(#internal_listing_data{last_node_id = NodeId} = ListingData, _Size = 0, _FoldFun, Acc) ->
    {Acc, #listing_info{
        finished = false,
        internal_listing_data = ListingData#internal_listing_data{
            last_node_id = NodeId
        }
    }};
list(#internal_listing_data{
    last_node_id = NodeId,
    last_key = LastKey,
    last_node_num = LastNodeNum,
    start_from = StartFrom
} = ListingData, Size, FoldFun, Acc) ->
    case find_node(ListingData) of
        #node{elements = OrigElements, node_num = NodeNum} = Node ->
            Elements = case LastKey == undefined orelse LastNodeNum =/= NodeNum of
                true -> OrigElements;
                false ->
                    % when node since last listing was not deleted select only 
                    % elements with key less than those already listed
                    case StartFrom of
                        first -> maps:filter(fun(Key, _) -> Key < LastKey end, OrigElements);
                        last -> maps:filter(fun(Key, _) -> Key > LastKey end, OrigElements)
                    end
            end,
            NumberOfElemsToTake = min(Size, maps:size(Elements)),
            NodeElements = case StartFrom of
                first -> lists:reverse(lists:sort(maps:to_list(Elements)));
                last -> lists:sort(maps:to_list(Elements))
            end,
            {ListedElems, NotListedElems} = lists:split(NumberOfElemsToTake, NodeElements),
            NextStartNodeId = case NotListedElems of
                [] -> select_neighbour(StartFrom, Node);
                _ -> NodeId
            end,
            {Status, FinalElems} = apply_fold_fun(FoldFun, ListedElems),
            NewSize = case Status of
                stop -> 0;
                continue -> Size - NumberOfElemsToTake
            end,
            {LastListedKey, _} = lists:last(ListedElems),
            list(ListingData#internal_listing_data{
                last_node_id = NextStartNodeId,
                last_node_num = NodeNum,
                last_key = LastListedKey
            }, NewSize, FoldFun, Acc ++ lists:reverse(FinalElems));
        {error, _} = Error -> Error
    end.


-spec get(id() | undefined, append_list:key(), first | last) -> [append_list:elem()].
get(undefined, _Keys, _StartFrom) ->
    [];
get(NodeId, Keys, StartFrom) ->
    case append_list_persistence:get_node(NodeId) of
        ?ERROR_NOT_FOUND -> [];
        #node{} = Node ->
            {Selected, RemainingKeys} = select_elems_from_node(Node, Keys, StartFrom),
            case RemainingKeys of
                [] -> Selected;
                _ -> Selected ++ get(select_neighbour(StartFrom, Node), RemainingKeys, StartFrom)
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
%% meantime) first node with number less (greater when listing started from 
%% last node) than `last_node_num` is returned.
%% @end
%%--------------------------------------------------------------------
-spec find_node(#internal_listing_data{}) -> #node{}.
find_node(ListingData) ->
    #internal_listing_data{
        last_node_id = NodeId, 
        last_node_num = LastNodeNum, 
        structure_id = StructId, 
        start_from = StartFrom
    } = ListingData,
    case append_list_persistence:get_node(NodeId) of
        ?ERROR_NOT_FOUND ->
            StartingNodeId = append_list_utils:get_starting_node_id(
                StartFrom, append_list_persistence:get_node(StructId)),
            case StartingNodeId of
                undefined -> ?ERROR_NOT_FOUND;
                _ -> find_node(ListingData#internal_listing_data{last_node_id = StartingNodeId})
            end;
        #node{node_num = NodeNum} = Node  ->
            NodeFound = case StartFrom of
                first -> not (is_integer(LastNodeNum) andalso NodeNum > LastNodeNum);
                last -> not (is_integer(LastNodeNum) andalso NodeNum < LastNodeNum)
            end,
            case NodeFound of
                true -> Node;
                false ->
                    find_node(ListingData#internal_listing_data{
                        last_node_id = select_neighbour(StartFrom, Node)
                    })
            end
    end.


-spec apply_fold_fun(append_list:fold_fun(), [append_list:elem()]) -> {continue | stop, [term()]}.
apply_fold_fun(FoldFun, OriginalElems) ->
    lists:foldl(fun
        (_Elem, {stop, Elems}) -> {stop, Elems};
        (E, {continue, Elems}) ->
            case FoldFun(E) of
                stop -> {stop, Elems};
                {ok, Res} -> {continue, [Res | Elems]}
            end
    end, {continue, []}, OriginalElems).


-spec select_elems_from_node(#node{}, [append_list:key()], first | last) ->
    {[append_list:elem()], [append_list:key()]}.
select_elems_from_node(#node{elements = Elements} = Node, Keys, StartFrom) ->
    Selected = maps:with(Keys, Elements),
    RemainingKeys = Keys -- maps:keys(Selected),
    {maps:to_list(Selected), filter_keys(StartFrom, Node, RemainingKeys)}.


-spec filter_keys(first | last, #node{}, [append_list:key()]) -> [append_list:key()].
filter_keys(first, #node{max_on_right = Max}, Keys) ->
    [Key || Key <- Keys, Key =< Max];
filter_keys(last, #node{min_on_left = Min}, Keys) ->
    [Key || Key <- Keys, Key >= Min].


-spec select_neighbour(first | last, #node{}) -> append_list:id().
select_neighbour(first, #node{prev = Prev}) -> Prev;
select_neighbour(last, #node{next = Next}) -> Next.
