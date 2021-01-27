%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that are responsible for deletion of 
%%% elements in append_list. For more details about this structure 
%%% consult `append_list` module doc.
%%% @end
%%%-------------------------------------------------------------------
-module(append_list_delete).
-author("Michal Stanisz").

-include("modules/datastore/append_list.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([delete_elements/3]).


%%=====================================================================
%% API
%%=====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes given elements from structure. 
%% Elements that were not found are ignored.
%% @end
%%--------------------------------------------------------------------
-spec delete_elements(append_list:sentinel(), append_list:list_node(), [append_list:key()]) -> ok.
delete_elements(Sentinel, LastNode, Elements) ->
    delete_elements_in_nodes(Sentinel, LastNode, Elements, undefined).


%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec delete_elements_in_nodes(
    append_list:sentinel(),
    CurrentNode :: undefined | append_list:list_node() | append_list:id(),
    [append_list:key()], 
    PrevNode :: undefined | append_list:list_node()
) -> ok.
delete_elements_in_nodes(_Sentinel, undefined, _, _PrevNode) ->
    ok;
delete_elements_in_nodes(_Sentinel, _CurrentNode, [], _PrevNode) ->
    ok;
delete_elements_in_nodes(Sentinel, #node{} = CurrentNode, ElementsToDelete, PrevNode) ->
    #sentinel{max_elements_per_node = MaxElementsPerNode} = Sentinel,
    #node{elements = ElementsBeforeDeletion, next = NextNodeId} = CurrentNode,
    NewElements = maps:without(ElementsToDelete, ElementsBeforeDeletion),
    ElementsInPreviousNode = case PrevNode of
        undefined  -> #{};
        _ -> PrevNode#node.elements
    end,
    NewElementsToDelete = ElementsToDelete -- maps:keys(ElementsBeforeDeletion),
    ShouldMergeNodes = PrevNode =/= undefined andalso 
        maps:size(ElementsInPreviousNode) + maps:size(NewElements) =< 0.5 * MaxElementsPerNode,
    {UpdatedCurrentNode, NextNodeOrId} = case ShouldMergeNodes of
        false -> 
            case maps:size(NewElements) == 0 of
                true -> delete_node(Sentinel, CurrentNode, NextNodeId, PrevNode);
                false -> {update_current_node(CurrentNode, NewElements, PrevNode), NextNodeId}
            end;
        true -> 
            MergedNode = merge_nodes(
                Sentinel, CurrentNode, PrevNode, maps:merge(ElementsInPreviousNode, NewElements)),
            NextNode = update_next_node_pointer(NextNodeId, MergedNode#node.node_id),
            {MergedNode, NextNode}
    end,
    UpdatedCurrentNode =/= undefined andalso 
        append_list_persistence:save_node(UpdatedCurrentNode#node.node_id, UpdatedCurrentNode),
    ShouldStop = NewElementsToDelete == [] orelse 
        (UpdatedCurrentNode =/= undefined andalso 
            UpdatedCurrentNode#node.min_on_left > lists:max(NewElementsToDelete)),
    case ShouldStop of
        true -> 
            MaxOnRightBefore = append_list_utils:get_max_key_in_prev_nodes(CurrentNode),
            case UpdatedCurrentNode of
                undefined ->
                    % this will always save updated next node
                    ok = append_list_utils:adjust_max_on_right(NextNodeOrId, undefined);
                _ -> 
                    handle_deletion_finished(UpdatedCurrentNode, PrevNode, MaxOnRightBefore),
                    % save next node if it was updated
                    case NextNodeOrId of
                        #node{} -> append_list_persistence:save_node(NextNodeId, NextNodeOrId);
                        _ -> ok
                end
            end;
        false ->
            delete_elements_in_nodes(Sentinel, NextNodeOrId, NewElementsToDelete, UpdatedCurrentNode)
    end;
delete_elements_in_nodes(Sentinel, CurrentNodeId, ElementsToDelete, PrevNode) when is_binary(CurrentNodeId)->
    {ok, CurrentNode} = append_list_persistence:get_node(CurrentNodeId),
    delete_elements_in_nodes(Sentinel, CurrentNode, ElementsToDelete, PrevNode).


%% @private
-spec delete_node(append_list:sentinel(), CurrentNode :: append_list:list_node(), append_list:id() | undefined, PrevNode :: append_list:list_node() | undefined) -> 
    {UpdatedCurrentNode :: undefined | append_list:list_node(), UpdatedNextNode :: undefined | append_list:list_node()}.
delete_node(Sentinel, #node{node_id = NodeId}, undefined, undefined) ->
    % deleting last remaining node in the structure
    append_list_persistence:save_node(Sentinel#sentinel.structure_id, Sentinel#sentinel{last = undefined, first = undefined}),
    append_list_persistence:delete_node(NodeId),
    {undefined, undefined};
delete_node(Sentinel, #node{node_id = NodeId}, undefined, #node{node_id = PrevNodeId} = PrevNode) ->
    % deleting first node
    append_list_persistence:save_node(Sentinel#sentinel.structure_id, Sentinel#sentinel{first = PrevNodeId}),
    UpdatedPrevNode = PrevNode#node{min_on_left = undefined, next = undefined},
    append_list_persistence:delete_node(NodeId),
    {UpdatedPrevNode, undefined};
delete_node(Sentinel, #node{node_id = NodeId}, NextNodeId, undefined) ->
    % deleting last node
    append_list_persistence:save_node(Sentinel#sentinel.structure_id, Sentinel#sentinel{last = NextNodeId}),
    {ok, #node{} = NextNode} = append_list_persistence:get_node(NextNodeId),
    UpdatedNextNode = NextNode#node{prev = undefined},
    append_list_persistence:delete_node(NodeId),
    {undefined, UpdatedNextNode};
delete_node(_Sentinel, #node{node_id = NodeId} = CurrentNode, NextNodeId, PrevNode) ->
    {ok, #node{} = NextNode} = append_list_persistence:get_node(NextNodeId),
    UpdatedNextNode = NextNode#node{
        prev = PrevNode#node.node_id
    },
    UpdatedPrevNode = PrevNode#node{
        next = NextNodeId,
        min_on_left = CurrentNode#node.min_on_left
    },
    append_list_persistence:delete_node(NodeId),
    {UpdatedPrevNode, UpdatedNextNode}.


%% @private
-spec update_next_node_pointer(append_list:id(), append_list:id()) -> append_list:list_node() | undefined.
update_next_node_pointer(undefined, _CurrentNodeId) ->
    undefined;
update_next_node_pointer(NextNodeId, CurrentNodeId) ->
    {ok, NextNode} = append_list_persistence:get_node(NextNodeId),
    NextNode#node{
        prev = CurrentNodeId
    }.


%% @private
-spec handle_deletion_finished(append_list:list_node(), append_list:list_node() | undefined, append_list:key()) -> ok.
handle_deletion_finished(#node{node_id = NodeId, prev = Prev} = Node, #node{node_id = NodeId}, MaxOnRightBefore) ->
    PrevNode = case Prev of
        undefined -> 
            undefined;
        _ -> 
            {ok, N} = append_list_persistence:get_node(Prev),
            N
    end,
    handle_deletion_finished(Node, PrevNode, MaxOnRightBefore);
handle_deletion_finished(CurrentNode, PrevNode, MaxOnRightBefore) ->
    #node{min_on_left = MinOnLeft, elements = Elements, next = Next} = CurrentNode,
    Min = lists:min([MinOnLeft | maps:keys(Elements)]),
    PrevNode =/= undefined andalso append_list_utils:adjust_min_on_left(PrevNode#node.node_id, Min, true),
    % if MaxOnRight did not change, there is no need to update this value in the next nodes
    case append_list_utils:get_max_key_in_prev_nodes(CurrentNode) of
        MaxOnRightBefore -> ok;
        CurrentMax -> append_list_utils:adjust_max_on_right(Next, CurrentMax)
    end.


%% @private
-spec update_current_node(append_list:list_node(), #{append_list:key() => append_list:value()}, append_list:list_node() | undefined) -> 
    append_list:list_node().
update_current_node(CurrentNode, NewElements, PrevNode) ->
    PrevNodeId = case PrevNode of
        undefined -> undefined;
        #node{} -> PrevNode#node.node_id
    end,
    CurrentNode#node{
        elements = NewElements,
        prev = PrevNodeId,
        max_on_right = append_list_utils:get_max_key_in_prev_nodes(PrevNode)
    }.


%% @private
-spec merge_nodes(append_list:sentinel(), CurrentNode :: append_list:list_node(), PrevNode :: append_list:list_node(), 
    #{append_list:key() => append_list:value()}) -> append_list:list_node().
merge_nodes(Sentinel, CurrentNode, PrevNode, Elements) ->
    MergedNode = PrevNode#node{
        elements = Elements,
        next = CurrentNode#node.next,
        min_on_left = CurrentNode#node.min_on_left
    },
    % if this is the first node, modify sentinel
    CurrentNode#node.next == undefined andalso 
        append_list_persistence:save_node(
            CurrentNode#node.structure_id, 
            Sentinel#sentinel{first = PrevNode#node.node_id}
        ),
    append_list_persistence:delete_node(CurrentNode#node.node_id),
    MergedNode.
