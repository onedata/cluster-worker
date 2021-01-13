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

-include("modules/datastore/datastore_append_list.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([add/3]).

%%=====================================================================
%% API
%%=====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds elements given in Batch to the beginning of a structure. 
%% Elements in structure should be sorted by Key and Keys should be unique.
%% Returns list of keys that were overwritten.
%% @end
%%--------------------------------------------------------------------
-spec add(#sentinel{}, #node{}, [append_list:elem()]) -> {ok, [append_list:elem()]}.
add(Sentinel, FirstNode, Batch) ->
    #sentinel{max_elems_per_node = MaxElemsPerNode} = Sentinel,

    {UpdatedFirstNode, UniqueElements, OverwrittenElems} = 
        overwrite_existing_elements(FirstNode, Batch),
    
    {FinalFirstNode, ElementsTail} = 
        add_to_first_node(UpdatedFirstNode, UniqueElements, MaxElemsPerNode),
    ok = create_new_nodes(Sentinel, ElementsTail, FinalFirstNode),
    {ok, lists:map(fun({Key, _Value}) -> Key end, OverwrittenElems)}.

%%=====================================================================
%% Internal functions
%%=====================================================================

-spec add_to_first_node(#node{}, [append_list:elem()], pos_integer()) -> 
    {#node{}, [append_list:elem()], [append_list:key()]}.
add_to_first_node(FirstNode, [] = _UniqueElements, _MaxElemsPerNode) ->
    % may happen when all elements from original batch already existed in structure
    {FirstNode, []};
add_to_first_node(#node{elements = ElementsInFirstNode} = FirstNode, UniqueElements, MaxElemsPerNode) ->
    ToFill = MaxElemsPerNode - maps:size(ElementsInFirstNode),
    {ElemsToAdd, ElementsTail} = split_list(UniqueElements, ToFill),
    Node = FirstNode#node{
        elements = maps:merge(
            ElementsInFirstNode,
            maps:from_list(ElemsToAdd)
        )
    },
    case ElementsTail of
        [] -> ok;
        [{MinInBatch, _} | _] ->
            % update `min_on_left` value in all nodes that have minimal key greater that minimal 
            % key in batch (may happen when adding elements with lower keys than existing ones)
            case maps:size(ElementsInFirstNode) > 0 andalso lists:min(maps:keys(ElementsInFirstNode)) > MinInBatch of
                true -> append_list_utils:adjust_min_on_left(Node#node.prev, MinInBatch, false);
                false -> ok
            end
    end, 
    {Node, ElementsTail}.


-spec create_new_nodes(#sentinel{}, [append_list:elem()], #node{}) -> ok.
create_new_nodes(#sentinel{structure_id = StructId} = Sentinel, [], #node{node_id = NodeId} = Node) ->
    append_list_persistence:save_node(NodeId, Node),
    append_list_persistence:save_node(StructId, Sentinel#sentinel{first = NodeId}),
    ok;
create_new_nodes(Sentinel, [{Min, _} | _] = Batch, PrevNode) ->
    #sentinel{structure_id = StructId, max_elems_per_node = MaxElemsPerNode} = Sentinel,
    #node{node_id = PrevNodeId} = PrevNode,
    Size = min(length(Batch), MaxElemsPerNode),
    {ElemsToAdd, Tail} = lists:split(Size, Batch),
    #node{node_id = NewFirstNodeId} = NewFirstNode = 
        prepare_new_first_node(StructId, ElemsToAdd, PrevNode),
    append_list_persistence:save_node(PrevNodeId, PrevNode#node{
        next = NewFirstNodeId,
        min_on_left = Min
    }),
    create_new_nodes(Sentinel, Tail, NewFirstNode).


-spec prepare_new_first_node(id(), [append_list:elem()], PrevNode :: #node{}) -> 
    NewNode :: #node{}.
prepare_new_first_node(SentinelId, ElemsList, #node{
    node_id = PrevNodeId, 
    node_num = PrevNodeNum
} = PrevNode) ->
    NodeNum = PrevNodeNum + 1,
    Max = append_list_utils:get_max_key_in_prev_nodes(PrevNode),
    #node{
        node_id = datastore_key:adjacent_from_digest([NodeNum], SentinelId),
        sentinel_id = SentinelId,
        prev = PrevNodeId,
        max_on_right = Max,
        node_num = NodeNum,
        elements = maps:from_list(ElemsList)
    }.


-spec overwrite_existing_elements(#node{}, Batch :: [append_list:elem()]) -> 
    {#node{}, UniqueElements :: [append_list:elem()], OverwrittenElems :: [append_list:elem()]}.
overwrite_existing_elements(FirstNode, [{MinInBatch, _} | _] = Batch) ->
    #node{max_on_right = MaxOnRight, prev = Prev} = FirstNode,
    {NewNode, RemainingElems, Overwritten} = overwrite_existing_elements_in_node(FirstNode, Batch),
    ReversedRemainingElems = lists:reverse(RemainingElems),
    case MaxOnRight == undefined orelse MinInBatch > MaxOnRight of
        true -> {NewNode, ReversedRemainingElems, Overwritten};
        false ->
            {FinalRemainingElems, OverwrittenInPrev} = 
                overwrite_existing_elements_in_prev_nodes(Prev, ReversedRemainingElems),
            {NewNode, FinalRemainingElems, Overwritten ++ OverwrittenInPrev}
    end.

-spec overwrite_existing_elements_in_prev_nodes(#node{} | id() | undefined, Batch :: [append_list:elem()]) -> 
    {UniqueElements :: [append_list:elem()], OverwrittenElems :: [append_list:elem()]}.
overwrite_existing_elements_in_prev_nodes(undefined, Batch) ->
    {Batch, []};
overwrite_existing_elements_in_prev_nodes(_, []) ->
    {[], []};
overwrite_existing_elements_in_prev_nodes(#node{} = Node, [{MinInBatch, _} | _] = Batch) ->
    #node{max_on_right = MaxOnRight, prev = Prev} = Node,
    {NewNode, RemainingElems, Overwritten} = overwrite_existing_elements_in_node(Node, Batch),
    case NewNode of
        Node -> ok;
        _ -> append_list_persistence:save_node(Node#node.node_id, NewNode)
    end,
    ReversedRemainingElems = lists:reverse(RemainingElems),
    case MaxOnRight == undefined orelse MinInBatch > MaxOnRight of
        true -> {ReversedRemainingElems, Overwritten};
        false -> 
            {FinalRemainingElems, OverwrittenInPrev} = 
                overwrite_existing_elements_in_prev_nodes(Prev, ReversedRemainingElems),
            {FinalRemainingElems, Overwritten ++ OverwrittenInPrev}
    end;
overwrite_existing_elements_in_prev_nodes(NodeId, Batch) ->
    overwrite_existing_elements_in_prev_nodes(append_list_persistence:get_node(NodeId), Batch).


-spec overwrite_existing_elements_in_node(#node{}, Batch :: [append_list:elem()]) -> 
    {#node{}, UniqueElements :: [append_list:elem()], CommonElements :: [append_list:elem()]}.
overwrite_existing_elements_in_node(Node, Batch) ->
    #node{elements = Elements} = Node,
    {Common, RemainingElems} = split_batch_on_existing_elements(Batch, Elements),
    NewElements = maps:merge(Elements, maps:from_list(Common)),
    {Node#node{elements = NewElements}, RemainingElems, Common}.


-spec split_batch_on_existing_elements(Batch :: [append_list:elem()], ElementsInNode :: [append_list:elem()]) -> 
    {ExistingElems :: [append_list:elem()], RemainingElems :: [append_list:elem()]}.
split_batch_on_existing_elements(Batch, ElementsInNode) ->
    BatchKeys = [X || {X, _} <- Batch],
    ExistingKeys = lists:sort(maps:keys(maps:with(BatchKeys, ElementsInNode))),
    {ExistingElems, RemainingElems, []} = lists:foldl(fun
        ({X, _} = Y, {ExistingElemsAcc, RemainingElemsAcc, [X | Tail]}) ->
            {[Y | ExistingElemsAcc], RemainingElemsAcc, Tail};
        (Y, {CommonAcc, RemainingElemsAcc, CK}) ->
            {CommonAcc, [Y | RemainingElemsAcc], CK}
    end, {[], [], ExistingKeys}, Batch),
    {ExistingElems, RemainingElems}.


-spec split_list([any()], non_neg_integer()) -> {[any()], [any()]}.
split_list(List, MaxElemsInFirstPart) ->
    case length(List) < MaxElemsInFirstPart of
        true -> {List, []};
        false -> lists:split(MaxElemsInFirstPart, List)
    end.
