%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that are responsible for adding new 
%%% elements to sliding_proplist. For more details about sliding proplist
%%% consult `sliding_proplist` module doc.
%%% @end
%%%-------------------------------------------------------------------
-module(sliding_proplist_add).
-author("Michal Stanisz").

-include("modules/datastore/sliding_proplist.hrl").

%% API
-export([insert_elements/3]).

%%=====================================================================
%% API
%%=====================================================================

-spec insert_elements(sliding_proplist:sentinel(), sliding_proplist:list_node(), 
    [sliding_proplist:element()]) -> [sliding_proplist:key()].
insert_elements(Sentinel, FirstNode, Elements) ->
    #sentinel{max_elements_per_node = MaxElementsPerNode} = Sentinel,

    {UpdatedFirstNode, UniqueElements, OverwrittenElements} = 
        overwrite_existing_elements(FirstNode, Elements, first_node),
    
    {FinalFirstNode, RemainingElements} = 
        fill_node_with_unique_elements(UpdatedFirstNode, UniqueElements, MaxElementsPerNode),
    case RemainingElements of
        [] -> sliding_proplist_persistence:save_record(FinalFirstNode#node.node_id, FinalFirstNode);
        _ -> 
            % This saves FirstNode after `next` pointer is updated
            ok = insert_to_new_node(Sentinel, RemainingElements, FinalFirstNode)
    end,
    {OverwrittenKeys, _} = lists:unzip(OverwrittenElements),
    OverwrittenKeys.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec overwrite_existing_elements(
    sliding_proplist:list_node() | sliding_proplist:node_id() | undefined,
    Elements :: [sliding_proplist:element()],
    NodePosition :: first_node | non_first_node
) ->
    {
        sliding_proplist:list_node(),
        UniqueElements :: [sliding_proplist:element()],
        OverwrittenElements :: [sliding_proplist:element()]
    }.
overwrite_existing_elements(undefined, Elements, _) ->
    {Elements, []};
overwrite_existing_elements(_, [], _) ->
    {[], []};
overwrite_existing_elements(#node{} = Node, [{MinInBatch, _} | _] = Elements, NodePosition) ->
    #node{max_key_in_older_nodes = MaxInOlder, prev = Prev} = Node,
    {UpdatedNode, RemainingElements, Overwritten} =
        overwrite_existing_elements_in_node(Node, Elements),
    case NodePosition == first_node orelse Overwritten == [] of
        true -> ok; % do not save first node as it will be done later
        _ -> sliding_proplist_persistence:save_record(UpdatedNode#node.node_id, UpdatedNode)
    end,
    case MaxInOlder == undefined orelse MinInBatch > MaxInOlder of
        true -> {UpdatedNode, RemainingElements, Overwritten};
        false ->
            {_UpdatedPrevNode, FinalRemainingElements, OverwrittenInPrev} =
                overwrite_existing_elements(Prev, RemainingElements, non_first_node),
            {UpdatedNode, FinalRemainingElements, Overwritten ++ OverwrittenInPrev}
    end;
overwrite_existing_elements(NodeId, Elements, IsFirstNode) ->
    {ok, Node} = sliding_proplist_persistence:get_record(NodeId),
    overwrite_existing_elements(Node, Elements, IsFirstNode).


%% @private
-spec overwrite_existing_elements_in_node(
    sliding_proplist:list_node(),
    Elements :: [sliding_proplist:element()]
) ->
    {
        sliding_proplist:list_node(),
        UniqueElements :: [sliding_proplist:element()],
        CommonElements :: [sliding_proplist:element()]
    }.
overwrite_existing_elements_in_node(Node, Elements) ->
    #node{elements = ElementsInNode} = Node,
    {Common, RemainingElements} = split_into_common_and_unique_elements(Elements, ElementsInNode),
    NewElements = maps:merge(ElementsInNode, maps:from_list(Common)),
    {Node#node{elements = NewElements}, RemainingElements, Common}.


%% @private
-spec fill_node_with_unique_elements(sliding_proplist:list_node(), [sliding_proplist:element()], 
    pos_integer()) -> {sliding_proplist:list_node(), [sliding_proplist:element()]}.
fill_node_with_unique_elements(Node, [] = _Elements, _MaxElementsPerNode) ->
    {Node, []};
fill_node_with_unique_elements(#node{} = Node, AllElementsToAdd,  MaxElementsPerNode) ->
    #node{
        elements = ElementsInNode, min_key_in_node = MinInNode, max_key_in_node = MaxInNodeBefore
    } = Node,
    [{MinInBatch, _} | _] = AllElementsToAdd,
    FillSize = MaxElementsPerNode - maps:size(ElementsInNode),
    {ElementsToFillNode, ElementsTail} = split_list(AllElementsToAdd, FillSize),
    NewMaxInNode = case FillSize == 0 of
        true -> MaxInNodeBefore;
        false ->
            {MaxInAddedElements, _} = lists:last(ElementsToFillNode),
            case MaxInNodeBefore of
                undefined -> MaxInAddedElements;
                _ -> max(MaxInNodeBefore, MaxInAddedElements)
            end
    end,
    UpdatedNode = Node#node{
        elements = maps:merge(
            ElementsInNode,
            maps:from_list(ElementsToFillNode)
        ),
        min_key_in_node = min(MinInNode, MinInBatch), % undefined is always greater than any number
        max_key_in_node = NewMaxInNode
    },
    case maps:size(ElementsInNode) > 0 andalso MinInBatch > MinInNode of
        true -> ok;
        false ->
            % update `min_in_newer` value in all nodes that have minimal key greater that minimal 
            % key in batch (may happen when adding elements with lower keys than existing ones)
            case maps:size(ElementsInNode) > 0 of
                true -> 
                    ok = sliding_proplist_utils:adjust_min_key_in_newer_nodes(
                        UpdatedNode#node.prev, MinInBatch, basic);
                false -> ok
            end
    end, 
    {UpdatedNode, ElementsTail}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds Elements to the beginning of a sliding proplist instance. 
%% Creates new nodes if necessary.
%% @end
%%--------------------------------------------------------------------
-spec insert_to_new_node(sliding_proplist:sentinel(), [sliding_proplist:element()], 
    sliding_proplist:list_node()) -> ok.
insert_to_new_node(#sentinel{structure_id = StructId} = Sentinel, [], #node{node_id = NodeId} = Node) ->
    sliding_proplist_persistence:save_record(NodeId, Node),
    sliding_proplist_persistence:save_record(StructId, Sentinel#sentinel{first = NodeId}),
    ok;
insert_to_new_node(Sentinel, [{Min, _} | _] = Elements, PrevNode) ->
    #sentinel{structure_id = StructId, max_elements_per_node = MaxElementsPerNode} = Sentinel,
    #node{node_id = PrevNodeId} = PrevNode,
    Size = min(length(Elements), MaxElementsPerNode),
    {ElementsToAdd, Tail} = lists:split(Size, Elements),
    #node{node_id = NewFirstNodeId} = NewFirstNode = 
        prepare_new_first_node(StructId, ElementsToAdd, PrevNode),
    sliding_proplist_persistence:save_record(PrevNodeId, PrevNode#node{
        next = NewFirstNodeId,
        min_key_in_newer_nodes = Min
    }),
    insert_to_new_node(Sentinel, Tail, NewFirstNode).


%% @private
-spec prepare_new_first_node(sliding_proplist:id(), [sliding_proplist:element()], 
    PrevNode :: sliding_proplist:list_node()) -> NewFirstNode :: sliding_proplist:list_node().
prepare_new_first_node(StructureId, ElementsList, #node{
    node_id = PrevNodeId, 
    node_number = PrevNodeNum
} = PrevNode) ->
    NodeNum = PrevNodeNum + 1,
    MaxInOlder = sliding_proplist_utils:get_max_key_in_current_and_older_nodes(PrevNode),
    [{Min, _} | _] = ElementsList,
    {Max, _} = lists:last(ElementsList),
    #node{
        node_id = datastore_key:adjacent_from_digest([NodeNum], StructureId),
        structure_id = StructureId,
        prev = PrevNodeId,
        max_key_in_older_nodes = MaxInOlder,
        node_number = NodeNum,
        elements = maps:from_list(ElementsList),
        min_key_in_node = Min,
        max_key_in_node = Max
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Splits given Elements list into two lists, first of which containing 
%% elements that exist in given ElementsInNodeMap (i.e. there is elements 
%% with the same Key in the map) and second list with remaining elements.
%% @end
%%--------------------------------------------------------------------
-spec split_into_common_and_unique_elements(
    [sliding_proplist:element()], 
    sliding_proplist:elements_map()
) -> 
    {
        CommonElements :: [sliding_proplist:element()], 
        UniqueElements :: [sliding_proplist:element()]
    }.
split_into_common_and_unique_elements(Elements, ElementsInNodeMap) ->
    Keys = [Key || {Key, _} <- Elements],
    CommonKeys = lists:sort(maps:keys(maps:with(Keys, ElementsInNodeMap))),
    {CommonElements, UniqueElements, []} = lists:foldl(fun
        ({Key, _} = Elem, {CommonElementsAcc, UniqueElementsAcc, [Key | Tail]}) ->
            {[Elem | CommonElementsAcc], UniqueElementsAcc, Tail};
        (UniqueElem, {CommonAcc, RemainingElementsAcc, CommonKeysLeft}) ->
            {CommonAcc, [UniqueElem | RemainingElementsAcc], CommonKeysLeft}
    end, {[], [], CommonKeys}, Elements),
    {CommonElements, lists:reverse(UniqueElements)}.


%% @private
-spec split_list([any()], non_neg_integer()) -> {[any()], [any()]}.
split_list(List, MaxElementsInFirstPart) ->
    case length(List) < MaxElementsInFirstPart of
        true -> {List, []};
        false -> lists:split(MaxElementsInFirstPart, List)
    end.
