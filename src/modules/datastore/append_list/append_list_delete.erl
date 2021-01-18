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

-include("modules/datastore/datastore_append_list.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([delete_elems/3]).


%%=====================================================================
%% API
%%=====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes given elements from structure. 
%% Elements that were not found are ignored.
%% @end
%%--------------------------------------------------------------------
-spec delete_elems(#sentinel{}, #node{}, [append_list:key()]) -> ok.
delete_elems(Sentinel, LastNode, Elems) ->
    delete_elems_in_nodes(Sentinel, LastNode, Elems, undefined).


%%=====================================================================
%% Internal functions
%%=====================================================================

-spec delete_elems_in_nodes(
    #sentinel{},
    CurrentNode :: undefined | #node{} | append_list:id(),
    [append_list:key()], 
    PrevNode :: undefined | #node{}
) -> ok.
delete_elems_in_nodes(_Sentinel, undefined, _, _PrevNode) ->
    ok;
delete_elems_in_nodes(_Sentinel, #node{} = CurrentNode, [], _PrevNode) ->
    append_list_persistence:save_node(CurrentNode#node.node_id, CurrentNode),
    ok;
delete_elems_in_nodes(_Sentinel, _CurrentNode, [], _PrevNode) ->
    ok;
delete_elems_in_nodes(Sentinel, #node{} = CurrentNode, ElemsToDelete, PrevNode) ->
    #sentinel{max_elems_per_node = MaxElemsPerNode} = Sentinel,
    #node{elements = ElementsBeforeDeletion, next = NextNodeId} = CurrentNode,
    NewElements = maps:without(ElemsToDelete, ElementsBeforeDeletion),
    ElementsInPreviousNode = case PrevNode of
        undefined  -> #{};
        _ -> PrevNode#node.elements
    end,
    NewElemsToDelete = ElemsToDelete -- maps:keys(ElementsBeforeDeletion),
    ShouldMergeNodes = PrevNode =/= undefined andalso 
        maps:size(ElementsInPreviousNode) + maps:size(NewElements) =< MaxElemsPerNode,
    {#node{node_id = CurrentNodeId} = UpdatedCurrentNode, NextNodeOrId} = case ShouldMergeNodes of
        false -> 
            {update_current_node(CurrentNode, NewElements, PrevNode), NextNodeId};
        true -> 
            MergedNode = merge_nodes(
                Sentinel, CurrentNode, PrevNode, maps:merge(ElementsInPreviousNode, NewElements)),
            NextNode = update_next_node_pointer(NextNodeId, MergedNode#node.node_id),
            {MergedNode, NextNode}
    end,
    append_list_persistence:save_node(CurrentNodeId, UpdatedCurrentNode),
    ShouldStop = NewElemsToDelete == [] orelse 
        UpdatedCurrentNode#node.min_on_left > lists:max(NewElemsToDelete),
    case ShouldStop of
        true -> 
            MaxOnRightBefore = append_list_utils:get_max_key_in_prev_nodes(CurrentNode),
            handle_deletion_finished(UpdatedCurrentNode, PrevNode, MaxOnRightBefore),
            % call one more time to save next node if it was updated
            delete_elems_in_nodes(Sentinel, NextNodeOrId, [], UpdatedCurrentNode);
        false ->
            delete_elems_in_nodes(Sentinel, NextNodeOrId, NewElemsToDelete, UpdatedCurrentNode)
    end;
delete_elems_in_nodes(Sentinel, CurrentNodeId, ElemsToDelete, PrevNode) when is_binary(CurrentNodeId)->
    CurrentNode = append_list_persistence:get_node(CurrentNodeId),
    delete_elems_in_nodes(Sentinel, CurrentNode, ElemsToDelete, PrevNode).


-spec update_next_node_pointer(append_list:id(), append_list:id()) -> #node{} | undefined.
update_next_node_pointer(NextNodeId, CurrentNodeId) ->
    case NextNodeId of
        undefined -> undefined;
        _ ->
            NextNode = append_list_persistence:get_node(NextNodeId),
            NextNode#node{
                prev = CurrentNodeId
            }
    end.


-spec handle_deletion_finished(#node{}, #node{}, append_list:key()) -> ok.
handle_deletion_finished(#node{node_id = NodeId, prev = Prev} = Node, #node{node_id = NodeId}, MaxOnRightBefore) ->
    PrevNode = case Prev of
        undefined -> undefined;
        _ -> append_list_persistence:get_node(Prev)
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


-spec update_current_node(#node{}, #{append_list:key() => append_list:value()}, #node{} | undefined) -> 
    #node{}.
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


-spec merge_nodes(#sentinel{}, CurrentNode :: #node{}, PrevNode :: #node{}, 
    #{append_list:key() => append_list:value()}) -> #node{}.
merge_nodes(Sentinel, CurrentNode, PrevNode, Elements) ->
    MergedNode = PrevNode#node{
        elements = Elements,
        next = CurrentNode#node.next,
        min_on_left = CurrentNode#node.min_on_left
    },
    % if this is the first node, modify sentinel
    CurrentNode#node.next == undefined andalso 
        append_list_persistence:save_node(
            CurrentNode#node.sentinel_id, 
            Sentinel#sentinel{first = PrevNode#node.node_id}
        ),
    append_list_persistence:delete_node(CurrentNode#node.node_id),
    MergedNode.
