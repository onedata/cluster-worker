%%%-------------------------------------------------------------------:
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that are responsible for deletion of 
%%% elements in sliding_proplist. For more details about sliding proplist 
%%% consult `sliding_proplist` module doc.
%%% @end
%%%-------------------------------------------------------------------
-module(sliding_proplist_delete).
-author("Michal Stanisz").

-include("modules/datastore/sliding_proplist.hrl").

%% API
-export([delete_elements/3]).


%%=====================================================================
%% API
%%=====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes given elements from a sliding proplist instance. 
%% Elements that were not found are ignored.
%% Returns list of keys that were not found.
%% @end
%%--------------------------------------------------------------------
-spec delete_elements(sliding_proplist:sentinel(), sliding_proplist:list_node(), 
    [sliding_proplist:key()]) -> NotFoundKeys :: [sliding_proplist:key()].
delete_elements(Sentinel, LastNode, Elements) ->
    delete_elements_in_nodes(Sentinel, LastNode, Elements, undefined).


%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec delete_elements_in_nodes(
    sliding_proplist:sentinel(),
    CurrentNode :: undefined | sliding_proplist:list_node() | sliding_proplist:node_id(),
    [sliding_proplist:key()], 
    PrevNode :: undefined | sliding_proplist:list_node()
) -> 
    NotFoundKeys :: [sliding_proplist:key()].
delete_elements_in_nodes(_Sentinel, undefined, KeysToDelete, _PrevNode) ->
    KeysToDelete;
delete_elements_in_nodes(_Sentinel, _CurrentNode, [], _PrevNode) ->
    [];
delete_elements_in_nodes(Sentinel, CurrentNodeId, ElementsToDelete, PrevNode) when is_binary(CurrentNodeId)->
    {ok, CurrentNode} = sliding_proplist_persistence:get_record(CurrentNodeId),
    delete_elements_in_nodes(Sentinel, CurrentNode, ElementsToDelete, PrevNode);
delete_elements_in_nodes(Sentinel, #node{} = CurrentNode, KeysToDelete, PrevNode) ->
    #node{elements = ElementsBeforeDeletion, next = NextNodeId} = CurrentNode,
    ElementsAfterDelete = maps:without(KeysToDelete, ElementsBeforeDeletion),
    NewKeysToDelete = KeysToDelete -- maps:keys(ElementsBeforeDeletion),
    {UpdatedCurrentNode, NextNodeOrId} = case maps:size(ElementsAfterDelete) == 0 of
        true ->
            delete_node(Sentinel, CurrentNode, NextNodeId, PrevNode);
        false ->
            case should_merge_nodes(Sentinel, ElementsAfterDelete, PrevNode) of
                true ->
                    MergedNode = merge_nodes(Sentinel, CurrentNode, PrevNode, ElementsAfterDelete),
                    NextNode = set_prev_pointer_in_node(NextNodeId, MergedNode#node.node_id),
                    {MergedNode, NextNode};
                false -> 
                    {adjust_node_after_elements_deletion(CurrentNode, ElementsAfterDelete, PrevNode), NextNodeId}
            end
    end,
     % UpdatedCurrentNode is undefined, if CurrentNode was the last one in the structure and it was deleted
    UpdatedCurrentNode =/= undefined andalso
        sliding_proplist_persistence:save_record(UpdatedCurrentNode#node.node_id, UpdatedCurrentNode),
    ShouldStop = NewKeysToDelete == [] orelse 
        (UpdatedCurrentNode =/= undefined andalso 
            UpdatedCurrentNode#node.min_key_in_newer_nodes > lists:max(NewKeysToDelete)),
    case ShouldStop of
        true -> 
            case UpdatedCurrentNode of
                undefined ->
                    % this will always save updated next node
                    ok = sliding_proplist_utils:adjust_max_key_in_older_nodes(NextNodeOrId, undefined);
                _ ->
                    handle_deletion_finished(UpdatedCurrentNode, PrevNode, 
                        sliding_proplist_utils:get_max_key_in_current_and_older_nodes(CurrentNode)),
                    % save next node if it was updated
                    case NextNodeOrId of
                        #node{} -> sliding_proplist_persistence:save_record(NextNodeId, NextNodeOrId);
                        _ -> ok
                end
            end,
            NewKeysToDelete;
        false ->
            delete_elements_in_nodes(Sentinel, NextNodeOrId, NewKeysToDelete, UpdatedCurrentNode)
    end.
    
%% @private
-spec delete_node(
    sliding_proplist:sentinel(), 
    CurrentNode :: sliding_proplist:list_node(), 
    NextNodeId :: sliding_proplist:node_id() | undefined, 
    PrevNode :: sliding_proplist:list_node() | undefined
) -> 
    {
        UpdatedCurrentNode :: undefined | sliding_proplist:list_node(), 
        UpdatedNextNode :: undefined | sliding_proplist:list_node()
    }.
delete_node(Sentinel, #node{node_id = NodeId}, undefined, undefined) ->
    % deleting last remaining node in the structure
    sliding_proplist_persistence:save_record(Sentinel#sentinel.structure_id, 
        Sentinel#sentinel{last = undefined, first = undefined}),
    sliding_proplist_persistence:delete_record(NodeId),
    {undefined, undefined};
delete_node(Sentinel, #node{node_id = NodeId}, undefined, #node{node_id = PrevNodeId} = PrevNode) ->
    % deleting first node
    sliding_proplist_persistence:save_record(Sentinel#sentinel.structure_id, 
        Sentinel#sentinel{first = PrevNodeId}),
    UpdatedPrevNode = PrevNode#node{min_key_in_newer_nodes = undefined, next = undefined},
    sliding_proplist_persistence:delete_record(NodeId),
    {UpdatedPrevNode, undefined};
delete_node(Sentinel, #node{node_id = NodeId}, NextNodeId, undefined) ->
    % deleting last node
    sliding_proplist_persistence:save_record(Sentinel#sentinel.structure_id, 
        Sentinel#sentinel{last = NextNodeId}),
    {ok, #node{} = NextNode} = sliding_proplist_persistence:get_record(NextNodeId),
    UpdatedNextNode = NextNode#node{prev = undefined},
    sliding_proplist_persistence:delete_record(NodeId),
    {undefined, UpdatedNextNode};
delete_node(_Sentinel, #node{node_id = NodeId} = CurrentNode, NextNodeId, PrevNode) ->
    {ok, #node{} = NextNode} = sliding_proplist_persistence:get_record(NextNodeId),
    UpdatedNextNode = NextNode#node{
        prev = PrevNode#node.node_id
    },
    UpdatedPrevNode = PrevNode#node{
        next = NextNodeId,
        min_key_in_newer_nodes = CurrentNode#node.min_key_in_newer_nodes
    },
    sliding_proplist_persistence:delete_record(NodeId),
    {UpdatedPrevNode, UpdatedNextNode}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates `min_in_newer_nodes` and `max_in_older_nodes` parameters in nodes, 
%% where it is no longer valid.
%% @end
%%--------------------------------------------------------------------
-spec handle_deletion_finished(sliding_proplist:list_node(), 
    sliding_proplist:list_node() | undefined, sliding_proplist:key()) -> ok.
handle_deletion_finished(
    #node{node_id = NodeId, prev = Prev} = Node, #node{node_id = NodeId}, MaxInOlderBefore
) ->
    PrevNode = case Prev of
        undefined -> 
            undefined;
        _ -> 
            {ok, N} = sliding_proplist_persistence:get_record(Prev),
            N
    end,
    handle_deletion_finished(Node, PrevNode, MaxInOlderBefore);
handle_deletion_finished(CurrentNode, PrevNode, MaxInCurrentAndOlderBefore) ->
    #node{min_key_in_newer_nodes = MinInNewer, min_key_in_node = MinInNode, next = Next} = CurrentNode,
    Min = min(MinInNewer, MinInNode),
    PrevNode =/= undefined andalso 
        sliding_proplist_utils:adjust_min_key_in_newer_nodes(PrevNode#node.node_id, Min, true),
    % if MaxInOlder did not change, there is no need to update this value in the next nodes
    case sliding_proplist_utils:get_max_key_in_current_and_older_nodes(CurrentNode) of
        MaxInCurrentAndOlderBefore -> ok;
        CurrentMax -> sliding_proplist_utils:adjust_max_key_in_older_nodes(Next, CurrentMax)
    end.


%% @private
-spec adjust_node_after_elements_deletion(sliding_proplist:list_node(), sliding_proplist:elements_map(), 
    sliding_proplist:list_node() | undefined) -> sliding_proplist:list_node().
adjust_node_after_elements_deletion(CurrentNode, NewElementsMap, PrevNode) ->
    #node{min_key_in_node = MinInNode, max_key_in_node = MaxInNode} = CurrentNode,
    PrevNodeId = case PrevNode of
        undefined -> undefined;
        #node{} -> PrevNode#node.node_id
    end,
    CurrentNode#node{
        elements = NewElementsMap,
        prev = PrevNodeId,
        max_key_in_older_nodes = sliding_proplist_utils:get_max_key_in_current_and_older_nodes(PrevNode),
        % if previous min/max in node was deleted find a new one
        min_key_in_node = find_extremum(MinInNode, NewElementsMap, min),
        max_key_in_node = find_extremum(MaxInNode, NewElementsMap, max)
    }.


%% @private
-spec should_merge_nodes(sliding_proplist:sentinel(), sliding_proplist:elements_map(), 
    undefined | sliding_proplist:list_node()) -> boolean().
should_merge_nodes(_Sentinel, _ElementsAfterDelete, undefined) ->
    false;
should_merge_nodes(Sentinel, ElementsAfterDelete, PrevNode) ->
    #sentinel{max_elements_per_node = MaxElementsPerNode} = Sentinel,
    #node{elements = ElementsInPrevNode} = PrevNode,
    maps:size(ElementsInPrevNode) + maps:size(ElementsAfterDelete) =< 0.5 * MaxElementsPerNode.


%% @private
-spec merge_nodes(
    sliding_proplist:sentinel(), 
    CurrentNode :: sliding_proplist:list_node(), 
    PrevNode :: sliding_proplist:list_node(), 
    sliding_proplist:elements_map()
) -> 
    sliding_proplist:list_node().
merge_nodes(Sentinel, CurrentNode, #node{elements = ElementsMapInPrevNode} = PrevNode, ElementsMap) ->
    MergedElementsMaps = maps:merge(ElementsMapInPrevNode, ElementsMap),
    NewMinInNode = case PrevNode#node.min_key_in_node < CurrentNode#node.min_key_in_node of
        true -> PrevNode#node.min_key_in_node;
        % if previous min in node was deleted find a new one
        false -> find_extremum(CurrentNode#node.min_key_in_node, ElementsMap, min)
    end,
    NewMaxInNode = case PrevNode#node.max_key_in_node > CurrentNode#node.max_key_in_node of
        true -> PrevNode#node.max_key_in_node;
        % if previous max in node was deleted find a new one
        false -> find_extremum(CurrentNode#node.max_key_in_node, ElementsMap, max)
    end,
    MergedNode = PrevNode#node{
        elements = MergedElementsMaps,
        next = CurrentNode#node.next,
        min_key_in_newer_nodes = CurrentNode#node.min_key_in_newer_nodes,
        min_key_in_node = NewMinInNode,
        max_key_in_node = NewMaxInNode
    },
    % if this is the first node, modify sentinel
    CurrentNode#node.next == undefined andalso 
        sliding_proplist_persistence:save_record(
            CurrentNode#node.structure_id, 
            Sentinel#sentinel{first = PrevNode#node.node_id}
        ),
    sliding_proplist_persistence:delete_record(CurrentNode#node.node_id),
    MergedNode.


%% @private
-spec set_prev_pointer_in_node(sliding_proplist:node_id() | undefined, sliding_proplist:node_id()) ->
    sliding_proplist:list_node() | undefined.
set_prev_pointer_in_node(undefined, _ValueToSet) ->
    undefined;
set_prev_pointer_in_node(NodeId, ValueToSet) ->
    {ok, Node} = sliding_proplist_persistence:get_record(NodeId),
    Node#node{
        prev = ValueToSet
    }.


%% @private
-spec find_extremum(sliding_proplist:key(), sliding_proplist:elements_map(), min | max) -> 
    sliding_proplist:key().
find_extremum(Key, ElementsMap, Operator) ->
    case maps:find(Key, ElementsMap) of
        {ok, _} -> Key;
        error -> lists:Operator(maps:keys(ElementsMap))
    end.