%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that are responsible for adding new 
%%% elements to append_list. For more details about this structure 
%%% consult `append_list` module doc.
%%% @end
%%%-------------------------------------------------------------------
-module(append_list_add).
-author("Michal Stanisz").

-include("modules/datastore/append_list.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([insert_elements/3]).

%%=====================================================================
%% API
%%=====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds elements given in Batch to the beginning of a structure. 
%% Elements in structure should be sorted ascending by Key and Keys should 
%% be unique. Returns list of keys that were overwritten (in reversed order).
%% @end
%%--------------------------------------------------------------------
-spec insert_elements(#sentinel{}, #node{}, [append_list:element()]) -> {ok, [append_list:element()]}.
insert_elements(Sentinel, FirstNode, Batch) ->
    #sentinel{max_elements_per_node = MaxElementsPerNode} = Sentinel,

    {UpdatedFirstNode, UniqueElements, OverwrittenElements} = 
        overwrite_existing_elements(FirstNode, Batch),
    
    {FinalFirstNode, ElementsTail} = 
        add_unique_elements(UpdatedFirstNode, UniqueElements, MaxElementsPerNode),
    ok = add_to_beginning(Sentinel, ElementsTail, FinalFirstNode),
    {ok, lists:map(fun({Key, _Value}) -> Key end, OverwrittenElements)}.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec add_unique_elements(#node{}, [append_list:element()], pos_integer()) -> 
    {#node{}, [append_list:element()]}.
add_unique_elements(FirstNode, [] = _Elements, _MaxElementsPerNode) ->
    {FirstNode, []};
add_unique_elements(#node{elements = ElementsInFirstNode} = FirstNode, [{MinInBatch, _} | _] = Elements, MaxElementsPerNode) ->
    ToFill = MaxElementsPerNode - maps:size(ElementsInFirstNode),
    {ElementsToAdd, ElementsTail} = split_list(Elements, ToFill),
    Node = FirstNode#node{
        elements = maps:merge(
            ElementsInFirstNode,
            maps:from_list(ElementsToAdd)
        )
    },
    case maps:size(ElementsInFirstNode) > 0 andalso MinInBatch > lists:min(maps:keys(ElementsInFirstNode)) of
        true -> ok;
        false ->
            % update `min_on_left` value in all nodes that have minimal key greater that minimal 
            % key in batch (may happen when adding elements with lower keys than existing ones)
            case maps:size(ElementsInFirstNode) > 0 andalso lists:min(maps:keys(ElementsInFirstNode)) > MinInBatch of
                true -> ok = append_list_utils:adjust_min_on_left(Node#node.prev, MinInBatch, false);
                false -> ok
            end
    end, 
    {Node, ElementsTail}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds elements given in Batch to the beginning of a structure. 
%% Creates new nodes if necessary.
%% @end
%%--------------------------------------------------------------------
-spec add_to_beginning(#sentinel{}, [append_list:element()], #node{}) -> ok.
add_to_beginning(#sentinel{structure_id = StructId} = Sentinel, [], #node{node_id = NodeId} = Node) ->
    append_list_persistence:save_node(NodeId, Node),
    append_list_persistence:save_node(StructId, Sentinel#sentinel{first = NodeId}),
    ok;
add_to_beginning(Sentinel, [{Min, _} | _] = Batch, PrevNode) ->
    #sentinel{structure_id = StructId, max_elements_per_node = MaxElementsPerNode} = Sentinel,
    #node{node_id = PrevNodeId} = PrevNode,
    Size = min(length(Batch), MaxElementsPerNode),
    {ElementsToAdd, Tail} = lists:split(Size, Batch),
    #node{node_id = NewFirstNodeId} = NewFirstNode = 
        prepare_new_first_node(StructId, ElementsToAdd, PrevNode),
    append_list_persistence:save_node(PrevNodeId, PrevNode#node{
        next = NewFirstNodeId,
        min_on_left = Min
    }),
    add_to_beginning(Sentinel, Tail, NewFirstNode).


%% @private
-spec prepare_new_first_node(append_list:id(), [append_list:element()], PrevNode :: #node{}) -> 
    NewNode :: #node{}.
prepare_new_first_node(StructureId, ElementsList, #node{
    node_id = PrevNodeId, 
    node_number = PrevNodeNum
} = PrevNode) ->
    NodeNum = PrevNodeNum + 1,
    Max = append_list_utils:get_max_key_in_prev_nodes(PrevNode),
    #node{
        node_id = datastore_key:adjacent_from_digest([NodeNum], StructureId),
        structure_id = StructureId,
        prev = PrevNodeId,
        max_on_right = Max,
        node_number = NodeNum,
        elements = maps:from_list(ElementsList)
    }.


%% @private
-spec overwrite_existing_elements(#node{}, Batch :: [append_list:element()]) -> 
    {#node{}, UniqueElements :: [append_list:element()], OverwrittenElements :: [append_list:element()]}.
overwrite_existing_elements(FirstNode, [{MinInBatch, _} | _] = Batch) ->
    #node{max_on_right = MaxOnRight, prev = Prev} = FirstNode,
    {NewNode, RemainingElements, Overwritten} = overwrite_existing_elements_in_node(FirstNode, Batch),
    case MaxOnRight == undefined orelse MinInBatch > MaxOnRight of
        true -> {NewNode, RemainingElements, Overwritten};
        false ->
            {FinalRemainingElements, OverwrittenInPrev} = 
                overwrite_existing_elements_in_prev_nodes(Prev, RemainingElements),
            {NewNode, FinalRemainingElements, Overwritten ++ OverwrittenInPrev}
    end.


%% @private
-spec overwrite_existing_elements_in_prev_nodes(#node{} | append_list:id() | undefined, Batch :: [append_list:element()]) -> 
    {UniqueElements :: [append_list:element()], OverwrittenElements :: [append_list:element()]}.
overwrite_existing_elements_in_prev_nodes(undefined, Batch) ->
    {Batch, []};
overwrite_existing_elements_in_prev_nodes(_, []) ->
    {[], []};
overwrite_existing_elements_in_prev_nodes(#node{} = Node, [{MinInBatch, _} | _] = Batch) ->
    #node{max_on_right = MaxOnRight, prev = Prev} = Node,
    {NewNode, RemainingElements, Overwritten} = overwrite_existing_elements_in_node(Node, Batch),
    case Overwritten of
        [] -> ok;
        _ -> append_list_persistence:save_node(Node#node.node_id, NewNode)
    end,
    case MaxOnRight == undefined orelse MinInBatch > MaxOnRight of
        true -> {RemainingElements, Overwritten};
        false -> 
            {FinalRemainingElements, OverwrittenInPrev} = 
                overwrite_existing_elements_in_prev_nodes(Prev, RemainingElements),
            {FinalRemainingElements, Overwritten ++ OverwrittenInPrev}
    end;
overwrite_existing_elements_in_prev_nodes(NodeId, Batch) ->
    overwrite_existing_elements_in_prev_nodes(append_list_persistence:get_node(NodeId), Batch).


%% @private
-spec overwrite_existing_elements_in_node(#node{}, Batch :: [append_list:element()]) -> 
    {#node{}, UniqueElements :: [append_list:element()], CommonElements :: [append_list:element()]}.
overwrite_existing_elements_in_node(Node, Batch) ->
    #node{elements = Elements} = Node,
    {Common, ReversedRemainingElements} = split_common_and_unique_elements(Batch, Elements),
    NewElements = maps:merge(Elements, maps:from_list(Common)),
    {Node#node{elements = NewElements}, lists:reverse(ReversedRemainingElements), Common}.


%% @private
-spec split_common_and_unique_elements([append_list:element()], #{append_list:key() => append_list:value()}) -> 
    {ExistingElements :: [append_list:element()], RemainingElements :: [append_list:element()]}.
split_common_and_unique_elements(Batch, ElementsInNode) ->
    BatchKeys = [Key || {Key, _} <- Batch],
    CommonKeys = lists:sort(maps:keys(maps:with(BatchKeys, ElementsInNode))),
    {CommonElements, UniqueElements, []} = lists:foldl(fun
        ({Key, _} = Elem, {CommonElementsAcc, UniqueElementsAcc, [Key | Tail]}) ->
            {[Elem | CommonElementsAcc], UniqueElementsAcc, Tail};
        (UniqueElem, {CommonAcc, RemainingElementsAcc, CommonKeysLeft}) ->
            {CommonAcc, [UniqueElem | RemainingElementsAcc], CommonKeysLeft}
    end, {[], [], CommonKeys}, Batch),
    {CommonElements, UniqueElements}.


%% @private
-spec split_list([any()], non_neg_integer()) -> {[any()], [any()]}.
split_list(List, MaxElementsInFirstPart) ->
    case length(List) < MaxElementsInFirstPart of
        true -> {List, []};
        false -> lists:split(MaxElementsInFirstPart, List)
    end.
