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

-spec insert_elements(sliding_proplist:sentinel(), sliding_proplist:list_node(), [sliding_proplist:element()]) -> 
    {ok, [sliding_proplist:key()]}.
insert_elements(Sentinel, FirstNode, Batch) ->
    #sentinel{max_elements_per_node = MaxElementsPerNode} = Sentinel,

    {UpdatedFirstNode, UniqueElements, OverwrittenElements} = 
        overwrite_existing_elements(FirstNode, Batch),
    
    {FinalFirstNode, ElementsTail} = 
        add_unique_elements(UpdatedFirstNode, UniqueElements, MaxElementsPerNode),
    ok = add_to_beginning(Sentinel, ElementsTail, FinalFirstNode),
    {OverwrittenKeys, _} = lists:unzip(OverwrittenElements),
    {ok, OverwrittenKeys}.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec add_unique_elements(sliding_proplist:list_node(), [sliding_proplist:element()], pos_integer()) -> 
    {sliding_proplist:list_node(), [sliding_proplist:element()]}.
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
            % update `min_in_newer` value in all nodes that have minimal key greater that minimal 
            % key in batch (may happen when adding elements with lower keys than existing ones)
            case maps:size(ElementsInFirstNode) > 0 andalso lists:min(maps:keys(ElementsInFirstNode)) > MinInBatch of
                true -> ok = sliding_proplist_utils:adjust_min_in_newer(Node#node.prev, MinInBatch, false);
                false -> ok
            end
    end, 
    {Node, ElementsTail}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds elements given in Batch to the beginning of a sliding proplist instance. 
%% Creates new nodes if necessary.
%% @end
%%--------------------------------------------------------------------
-spec add_to_beginning(sliding_proplist:sentinel(), [sliding_proplist:element()], sliding_proplist:list_node()) -> ok.
add_to_beginning(#sentinel{structure_id = StructId} = Sentinel, [], #node{node_id = NodeId} = Node) ->
    sliding_proplist_persistence:save_node(NodeId, Node),
    sliding_proplist_persistence:save_node(StructId, Sentinel#sentinel{first = NodeId}),
    ok;
add_to_beginning(Sentinel, [{Min, _} | _] = Batch, PrevNode) ->
    #sentinel{structure_id = StructId, max_elements_per_node = MaxElementsPerNode} = Sentinel,
    #node{node_id = PrevNodeId} = PrevNode,
    Size = min(length(Batch), MaxElementsPerNode),
    {ElementsToAdd, Tail} = lists:split(Size, Batch),
    #node{node_id = NewFirstNodeId} = NewFirstNode = 
        prepare_new_first_node(StructId, ElementsToAdd, PrevNode),
    sliding_proplist_persistence:save_node(PrevNodeId, PrevNode#node{
        next = NewFirstNodeId,
        min_in_newer = Min
    }),
    add_to_beginning(Sentinel, Tail, NewFirstNode).


%% @private
-spec prepare_new_first_node(sliding_proplist:id(), [sliding_proplist:element()], PrevNode :: sliding_proplist:list_node()) -> 
    NewNode :: sliding_proplist:list_node().
prepare_new_first_node(StructureId, ElementsList, #node{
    node_id = PrevNodeId, 
    node_number = PrevNodeNum
} = PrevNode) ->
    NodeNum = PrevNodeNum + 1,
    Max = sliding_proplist_utils:get_max_key_in_prev_nodes(PrevNode),
    #node{
        node_id = datastore_key:adjacent_from_digest([NodeNum], StructureId),
        structure_id = StructureId,
        prev = PrevNodeId,
        max_in_older = Max,
        node_number = NodeNum,
        elements = maps:from_list(ElementsList)
    }.


%% @private
-spec overwrite_existing_elements(sliding_proplist:list_node(), Batch :: [sliding_proplist:element()]) -> 
    {sliding_proplist:list_node(), UniqueElements :: [sliding_proplist:element()], OverwrittenElements :: [sliding_proplist:element()]}.
overwrite_existing_elements(FirstNode, [{MinInBatch, _} | _] = Batch) ->
    #node{max_in_older = MaxInOlder, prev = Prev} = FirstNode,
    {NewNode, RemainingElements, Overwritten} = overwrite_existing_elements_in_node(FirstNode, Batch),
    case MaxInOlder == undefined orelse MinInBatch > MaxInOlder of
        true -> {NewNode, RemainingElements, Overwritten};
        false ->
            {FinalRemainingElements, OverwrittenInPrev} = 
                overwrite_existing_elements_in_prev_nodes(Prev, RemainingElements),
            {NewNode, FinalRemainingElements, Overwritten ++ OverwrittenInPrev}
    end.


%% @private
-spec overwrite_existing_elements_in_prev_nodes(sliding_proplist:list_node() | sliding_proplist:id() | undefined, Batch :: [sliding_proplist:element()]) -> 
    {UniqueElements :: [sliding_proplist:element()], OverwrittenElements :: [sliding_proplist:element()]}.
overwrite_existing_elements_in_prev_nodes(undefined, Batch) ->
    {Batch, []};
overwrite_existing_elements_in_prev_nodes(_, []) ->
    {[], []};
overwrite_existing_elements_in_prev_nodes(#node{} = Node, [{MinInBatch, _} | _] = Batch) ->
    #node{max_in_older = MaxInOlder, prev = Prev} = Node,
    {NewNode, RemainingElements, Overwritten} = overwrite_existing_elements_in_node(Node, Batch),
    case Overwritten of
        [] -> ok;
        _ -> sliding_proplist_persistence:save_node(Node#node.node_id, NewNode)
    end,
    case MaxInOlder == undefined orelse MinInBatch > MaxInOlder of
        true -> {RemainingElements, Overwritten};
        false -> 
            {FinalRemainingElements, OverwrittenInPrev} = 
                overwrite_existing_elements_in_prev_nodes(Prev, RemainingElements),
            {FinalRemainingElements, Overwritten ++ OverwrittenInPrev}
    end;
overwrite_existing_elements_in_prev_nodes(NodeId, Batch) ->
    {ok, Node} = sliding_proplist_persistence:get_node(NodeId),
    overwrite_existing_elements_in_prev_nodes(Node, Batch).


%% @private
-spec overwrite_existing_elements_in_node(sliding_proplist:list_node(), Batch :: [sliding_proplist:element()]) -> 
    {sliding_proplist:list_node(), UniqueElements :: [sliding_proplist:element()], CommonElements :: [sliding_proplist:element()]}.
overwrite_existing_elements_in_node(Node, Batch) ->
    #node{elements = Elements} = Node,
    {Common, ReversedRemainingElements} = split_common_and_unique_elements(Batch, Elements),
    NewElements = maps:merge(Elements, maps:from_list(Common)),
    {Node#node{elements = NewElements}, lists:reverse(ReversedRemainingElements), Common}.


%% @private
-spec split_common_and_unique_elements([sliding_proplist:element()], #{sliding_proplist:key() => sliding_proplist:value()}) -> 
    {ExistingElements :: [sliding_proplist:element()], RemainingElements :: [sliding_proplist:element()]}.
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
