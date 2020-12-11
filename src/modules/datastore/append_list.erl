%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements structure responsible for storing data in 
%%% key -> value format.
%%%
%%% This structure is implemented as a bidirectional linked list, with each 
%%% node storing up to MaxElemsPerNode elements (i.e key value pairs).
%%% MaxElemsPerNode value is provided during structure creation.
%%% 
%%%                    +--------+             +--------+             
%%%          prev      |        |    prev     |        |    prev      
%%%        +------->   |  node  |  +------->  |  node  |  +------->  
%%%  ...               |        |             |        |              ...  
%%%        <-------+   |        |  <-------+  |        |  <-------+  
%%%          next      +--------+    next     +--------+    next     
%%% 
%%% This structure stores elements in arbitrary order, i.e not necessarily sorted.
%%% Adding new elements is only allowed to the beginning of the list. 
%%% It is highly recommended that each new element have key greater 
%%% than those already existing (if not this structure might be inefficient).
%%% Adding elements with the existing keys will result in overwriting of 
%%% existing elements.
%%%
%%% Deletion of arbitrary elements is allowed, although it is recommended 
%%% to delete elements from the list's end. 
%%% During elements deletion if two adjacent nodes have less than 
%%% MaxElemsPerNode elements combined, one of those nodes (the newer one) 
%%% will be deleted, and all elements will be now stored in the other node.
%%%
%%% Each node stores also value `min_on_left`. It represents minimal value 
%%% in all nodes, that are newer (are pointed by `next`) than this node. 
%%% It is used during deletion  - it allows to determine whether it is 
%%% necessary to fetch next nodes and allows to finish deletion without 
%%% traversing all list nodes. This is why it is optimal to have 
%%% increasing keys.
%%%
%%% In each node there is also value `max_on_right`. It works similarly to 
%%% `min_on_left` but it represents maximum value in all nodes, that are 
%%% older (are pointed by `prev`) than this node. 
%%% It is used to optimize functions finding elements (`get/2`, `get_highest/1`) 
%%% and also when overwriting existing elements during addition.
%%% @end
%%%-------------------------------------------------------------------
-module(append_list).
-author("Michal Stanisz").

-include_lib("ctool/include/errors.hrl").

-compile({no_auto_import, [get/1]}).

% id that allows for finding entities (nodes, sentinel) in persistence.
-type id() :: binary().
% this type represents keys of elements which are stored as data in this structure.
-type key() :: integer().
% representation of one element in batch of elements to add.
-type elem() :: {key(), binary()}.

% This record holds pointers to the first and last node of list.
% Only one record of this type is persisted for each structure 
% so `id` of this record is equivalent to id of whole structure.
-record(sentinel, {
    structure_id :: id(),
    max_elems_per_node :: pos_integer(),
    first = undefined :: id() | undefined,
    last = undefined :: id() | undefined
}).

-record(node, {
    sentinel_id :: id(),
    node_id :: id(),
    prev = undefined :: id() | undefined,
    next = undefined :: id() | undefined,
    elements = #{} :: #{key() => binary()},
    min_on_left :: integer() | undefined,
    max_on_right :: integer() | undefined,
    node_num :: integer()
}).

% This record hold information that allows to start listing elements 
% at the point where previous listing finished. 
% For more details consult `find_node/1` doc.
-record(listing_info, {
    id :: id() | undefined,
    elems_to_list :: non_neg_integer() | undefined,
    finished = false ::  boolean() | undefined,
    last_node_id :: id() | undefined,
    last_key :: integer() | undefined,
    seen_node_num :: integer() | undefined
}).

%% API
-export([create/1, delete/1]).
-export([add_to_beginning/2, delete_elems/2, list_elems/2, get/2, get_highest/1]).

% fixme remove
-export([init/0]).

-spec create(pos_integer()) -> {ok, id()}.
create(MaxElemsPerNode) ->
    Id = datastore_key:new(),
    Sentinel = #sentinel{structure_id = Id, max_elems_per_node = MaxElemsPerNode},
    save(Id, Sentinel),
    {ok, Id}.


-spec delete(id()) -> ok | ?ERROR_NOT_FOUND.
delete(StructId) ->
    case get(StructId) of
        ?ERROR_NOT_FOUND -> ok;
        Sentinel ->
            delete_all_nodes(Sentinel),
            true = del(Sentinel#sentinel.structure_id),
            ok
    end.


- spec list_elems(id() | #listing_info{}, non_neg_integer()) -> {[elem()], #listing_info{}}.
list_elems(_, 0) ->
    {[], #listing_info{finished = true}};
list_elems(#listing_info{finished = true}, _Size) ->
    {[], #listing_info{finished = true}};
list_elems(#listing_info{} = ListingInfo, Size) ->
    list_elems_internal(ListingInfo#listing_info{elems_to_list = Size}, []);
list_elems(SentinelId, Size) ->
    case get(SentinelId) of
        ?ERROR_NOT_FOUND -> list_elems(SentinelId, 0);
        #sentinel{first = FirstNodeId} ->
            list_elems_internal(#listing_info{
                id = SentinelId, 
                last_node_id = FirstNodeId, 
                elems_to_list = Size
            }, [])
    end.


-spec add_to_beginning(id(), [elem()]) -> ok | ?ERROR_NOT_FOUND.
add_to_beginning(_SentinelId, []) ->
    ok;
add_to_beginning(SentinelId, UnsortedBatch) ->
    case fetch_or_create_first_doc(SentinelId) of
        {error, _} = Error -> Error;
        {Sentinel, FirstNode} ->
            #sentinel{max_elems_per_node = MaxElemsPerNode} = Sentinel,
            Batch = lists:ukeysort(1, UnsortedBatch),
            {#node{elements = ElementsInFirstNode} = NewFirstNode, UniqueElements} = 
                overwrite_existing_elements(FirstNode, Batch),
            ToFill = MaxElemsPerNode - maps:size(ElementsInFirstNode),
            {ElemsToAdd, ElementsTail} = split_list(UniqueElements, ToFill),
            Node = NewFirstNode#node{
                elements = maps:merge(
                    ElementsInFirstNode,
                    maps:from_list(ElemsToAdd)
                )
            },
            [Min | _] = Mins = precalculate_mins_on_left(Batch),
            {_, MinsTail} = split_list(Mins, ToFill),
            case maps:size(ElementsInFirstNode) > 0 andalso lists:min(maps:keys(ElementsInFirstNode)) > Min of
                true -> adjust_min_on_left(Node#node.prev, Min, false);
                false -> ok
            end,
            add_elems_internal(Sentinel, ElementsTail, MinsTail, Node)
    end.
    

-spec delete_elems(id(), [key()]) -> ok | ?ERROR_NOT_FOUND.
delete_elems(SentinelId, Elems) ->
    case fetch_last_doc(SentinelId) of
        {error, _} = Error -> Error;
        {Sentinel, LastNode} ->
            delete_elems_internal(Sentinel, LastNode, Elems, undefined)
    end.


-spec get_highest(id()) -> elem() | undefined.
get_highest(SentinelId) ->
    case get(SentinelId) of
        ?ERROR_NOT_FOUND -> undefined;
        Sentinel -> get_highest_internal(Sentinel#sentinel.first)
    end.


-spec get(id(), key()) -> ?ERROR_NOT_FOUND | {ok, binary()}.
get(SentinelId, Key) ->
    case get(SentinelId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        Sentinel -> get_internal(Sentinel#sentinel.first, Key)
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

-spec delete_all_nodes(#sentinel{}) -> ok.
delete_all_nodes(#sentinel{first = undefined}) ->
    ok;
delete_all_nodes(#sentinel{first = NodeId} = Sentinel) ->
    #node{prev = Prev} = get(NodeId),
    NewSentinel = Sentinel#sentinel{first = Prev},
    save(Sentinel#sentinel.structure_id, NewSentinel),
    del(NodeId),
    delete_all_nodes(NewSentinel).


-spec list_elems_internal(#listing_info{}, [elem()]) -> {[elem()], #listing_info{}}.
list_elems_internal(#listing_info{last_node_id = undefined}, Acc) ->
    {Acc, #listing_info{
        finished = true
    }};
list_elems_internal(#listing_info{elems_to_list = 0, last_node_id = NodeId} = ListingInfo, Acc) ->
    {L, _} = lists:last(Acc),
    {Acc, ListingInfo#listing_info{
        last_node_id = NodeId, 
        last_key = L
    }};
list_elems_internal(#listing_info{
    elems_to_list = Size, 
    last_node_id = NodeId, 
    last_key = LastKey,
    seen_node_num = SeenNodeNum
} = ListingInfo, Acc) ->
    case find_node(ListingInfo) of
        #node{elements = OrigElements, node_num = NodeNum} = Node ->
            Elements = case LastKey == undefined orelse SeenNodeNum =/= NodeNum of
                true -> OrigElements;
                false ->
                    maps:filter(fun(Key, _) -> Key < LastKey end, OrigElements)
            end,
            NumElemsToTake = min(Size, maps:size(Elements)),
            {E, Rest} = lists:split(NumElemsToTake, lists:reverse(lists:sort(maps:to_list(Elements)))),
            NextNodeId = case Rest of
                [] -> Node#node.prev;
                _ -> NodeId
            end,
            list_elems_internal(ListingInfo#listing_info{
                last_node_id = NextNodeId, 
                elems_to_list = Size - NumElemsToTake, 
                seen_node_num = NodeNum
            }, Acc ++ E)
    end.


%%--------------------------------------------------------------------
%% @doc
%% This function returns Node based on information included in ListingInfo.
%% If node with given id exists it is returned. If not (it was deleted in 
%% meantime) first node with number less than `seen_node_num` is returned.
%% Because last node is never deleted, such node always exists.
%% @end
%%--------------------------------------------------------------------
-spec find_node(#listing_info{}) -> #node{}.
find_node(#listing_info{last_node_id = NodeId, seen_node_num = SeenNodeNum, id = StructId} = LS) ->
    case get(NodeId) of
        ?ERROR_NOT_FOUND -> 
            #sentinel{first = First} = get(StructId),
            find_node(LS#listing_info{last_node_id = First});
        #node{node_num = NodeNum} = Node when not (is_integer(SeenNodeNum) andalso NodeNum > SeenNodeNum) ->
            Node;
        #node{prev = Prev} ->
            find_node(LS#listing_info{last_node_id = Prev})
    end.


-spec add_elems_internal(#sentinel{}, [elem()], [key()], #node{}) -> ok.
add_elems_internal(#sentinel{structure_id = StructId} = Sentinel, [], _, #node{node_id = NodeId} = Node) ->
    save(NodeId, Node),
    save(StructId, Sentinel#sentinel{first = NodeId}),
    ok;
add_elems_internal(Sentinel, Batch, [Min | _] = Mins, PrevNode) ->
    #sentinel{structure_id = StructId, max_elems_per_node = MaxElemsPerNode} = Sentinel,
    #node{node_id = PrevNodeId} = PrevNode,
    Size = min(length(Batch), MaxElemsPerNode),
    {ElemsToAdd, Tail} = lists:split(Size, Batch),
    {_, MinsTail} = lists:split(Size, Mins),
    #node{node_id = NewFirstNodeId} = NewFirstNode = 
        prepare_new_first_node(StructId, ElemsToAdd, PrevNode),
    save(PrevNodeId, PrevNode#node{
        next = NewFirstNodeId,
        min_on_left = Min
    }),
    add_elems_internal(Sentinel, Tail, MinsTail, NewFirstNode).


-spec prepare_new_first_node(id(), [elem()], PrevNode :: #node{}) -> 
    NewNode :: #node{}.
prepare_new_first_node(SentinelId, ElemsList, #node{
    node_id = PrevNodeId, 
    node_num = PrevNodeNum
} = PrevNode) ->
    NodeNum = PrevNodeNum + 1,
    Max = calculate_max_on_right(PrevNode),
    #node{
        node_id = datastore_key:adjacent_from_digest([NodeNum], SentinelId),
        sentinel_id = SentinelId,
        prev = PrevNodeId,
        max_on_right = Max,
        node_num = NodeNum,
        elements = maps:from_list(ElemsList)
    }.


-spec precalculate_mins_on_left([elem()]) -> [key()].
precalculate_mins_on_left(Batch) ->
    lists:foldl(fun
        ({X, _}, undefined) -> [X];
        ({X, _}, [M | _] = Acc) -> [min(X, M) | Acc]
    end, undefined, Batch).


-spec overwrite_existing_elements(#node{}, Batch :: [elem()]) -> 
    {#node{}, UniqueElements :: [elem()]}.
overwrite_existing_elements(#node{max_on_right = MaxOnRight, prev = Prev} = Node, [{MinInBatch, _} | _] = Batch) ->
    {NewNode, RemainingElems} = overwrite_existing_elements_in_node(Node, Batch),
    ReversedRemainingElems = lists:reverse(RemainingElems),
    case MaxOnRight == undefined orelse MinInBatch > MaxOnRight of
        true -> {NewNode, ReversedRemainingElems};
        false -> {NewNode, overwrite_existing_elements_rec(Prev, ReversedRemainingElems)}
    end.

-spec overwrite_existing_elements_rec(#node{} | id() | undefined, Batch :: [elem()]) -> 
    UniqueElements :: [elem()].
overwrite_existing_elements_rec(undefined, Batch) ->
    Batch;
overwrite_existing_elements_rec(_, []) ->
    [];
overwrite_existing_elements_rec(#node{max_on_right = MaxOnRight, prev = Prev} = Node, [{MinInBatch, _} | _] = Batch) ->
    {NewNode, RemainingElems} = overwrite_existing_elements_in_node(Node, Batch),
    case NewNode of
        Node -> ok;
        _ -> save(Node#node.node_id, NewNode)
    end,
    ReversedRemainingElems = lists:reverse(RemainingElems),
    case MaxOnRight == undefined orelse MinInBatch > MaxOnRight of
        true -> ReversedRemainingElems;
        false -> overwrite_existing_elements_rec(Prev, ReversedRemainingElems)
    end;
overwrite_existing_elements_rec(NodeId, Batch) ->
    overwrite_existing_elements_rec(get(NodeId), Batch).


-spec overwrite_existing_elements_in_node(#node{}, Batch :: [elem()]) -> 
    {#node{}, UniqueElements :: [elem()]}.
overwrite_existing_elements_in_node(Node, Batch) ->
    #node{elements = Elements} = Node,
    {Common, RemainingElems} = split_batch_on_existing_elements(Batch, Elements),
    NewElements = maps:merge(Elements, maps:from_list(Common)),
    {Node#node{elements = NewElements}, RemainingElems}.


-spec split_batch_on_existing_elements(Batch :: [elem()], ElementsInNode :: [elem()]) -> 
    {ExistingElems :: [elem()], RemainingElems :: [elem()]}.
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


-spec delete_elems_internal(
    #sentinel{},
    CurrentNode :: undefined | #node{}, 
    [key()], 
    PrevNode :: undefined | #node{}
) -> ok.
delete_elems_internal(_Sentinel, undefined, _, _PrevNode) ->
    ok;
delete_elems_internal(_Sentinel, CurrentNode, [], PrevNode) ->
    #node{node_id = NodeId, min_on_left = MinOnLeft, elements = Elements, next = Next} = CurrentNode,
    save(NodeId, CurrentNode),
    Min = lists:min([MinOnLeft | maps:keys(Elements)]),
    adjust_max_on_right(Next, CurrentNode),
    adjust_min_on_left(PrevNode#node.node_id, Min, true);
delete_elems_internal(Sentinel, CurrentNode, ElemsToDelete, PrevNode) ->
    #sentinel{max_elems_per_node = MaxElemsPerNode} = Sentinel,
    #node{
        node_id = CurrentNodeId,
        elements = ElementsInCurrentNode
    } = CurrentNode,
    NewElements = maps:without(ElemsToDelete, ElementsInCurrentNode),
    ElementsInPreviousNode = case PrevNode of
        undefined  -> #{};
        _ ->
            PrevNode#node.elements
    end,
    NewElemsToDelete = ElemsToDelete -- maps:keys(ElementsInCurrentNode),
    NextNodeId = CurrentNode#node.next,
    NextNode = case NextNodeId of
        undefined -> undefined;
        _ -> get(NextNodeId)
    end,
    ShouldCoalesceNodes = PrevNode =/= undefined andalso 
        maps:size(ElementsInPreviousNode) + maps:size(NewElements) =< MaxElemsPerNode,
    {NewCurrentNode, NewNextNode} = case ShouldCoalesceNodes of
        false -> 
            PrevNodeId = case PrevNode of
                undefined -> undefined;
                #node{} -> PrevNode#node.node_id
            end, 
            UpdatedCurrentNode = CurrentNode#node{
                elements = NewElements, 
                prev = PrevNodeId
            },
            case UpdatedCurrentNode of
                CurrentNode -> ok;
                _ -> save(CurrentNodeId, UpdatedCurrentNode)
            end,
            UpdatedNextNode = case NextNode of
                undefined -> undefined;
                _ -> NextNode#node{max_on_right = calculate_max_on_right(UpdatedCurrentNode)}
            end, 
            {UpdatedCurrentNode, UpdatedNextNode};
        true -> 
            UpdatedCurrentNode = coalesce_nodes(
                Sentinel, CurrentNode, PrevNode, maps:merge(ElementsInPreviousNode, NewElements)),
            UpdatedNextNode = case NextNode of
                undefined -> undefined;
                _ ->
                    NextNode#node{
                        prev = UpdatedCurrentNode#node.node_id, 
                        max_on_right = calculate_max_on_right(UpdatedCurrentNode)
                    }
            end,
            {UpdatedCurrentNode, UpdatedNextNode}
    end,
    ShouldStop = NewElemsToDelete == [] orelse 
        CurrentNode#node.min_on_left > lists:max(NewElemsToDelete),
    NewElemsToDelete1 = case ShouldStop of
        true -> [];
        false -> NewElemsToDelete
    end,
    delete_elems_internal(Sentinel, NewNextNode, NewElemsToDelete1, NewCurrentNode).


-spec coalesce_nodes(#sentinel{}, CurrentNode :: #node{}, PrevNode :: #node{}, #{key() => binary()}) -> 
    #node{}.
coalesce_nodes(Sentinel, CurrentNode, PrevNode, Elements) ->
    % Set min_on_left value to undefined if this is first node in structure. 
    % If not leave it unchanged as it will be adjusted after deletion is completed.
    MinOnLeft = case CurrentNode#node.next of
        undefined -> undefined;
        _ -> PrevNode#node.min_on_left
    end, 
    MergedNode = PrevNode#node{
        elements = Elements,
        next = CurrentNode#node.next,
        min_on_left = MinOnLeft
    },
    save(MergedNode#node.node_id, MergedNode),
    % if this is the first doc, modify guard
    CurrentNode#node.next == undefined andalso 
        save(CurrentNode#node.sentinel_id, Sentinel#sentinel{first = PrevNode#node.node_id}),
    del(CurrentNode#node.node_id),
    MergedNode.


-spec get_highest_internal(undefined | id()) -> elem() | undefined.
get_highest_internal(undefined) -> undefined;
get_highest_internal(NodeId) ->
    case get(NodeId) of
        ?ERROR_NOT_FOUND -> undefined;
        #node{elements = Elements, max_on_right = MaxOnRight, prev = Prev} ->
            case {Prev == undefined, maps:size(Elements) > 0 andalso lists:max(maps:keys(Elements))} of
                {true, false} -> undefined;
                {true, MaxInNode} -> {MaxInNode, maps:get(MaxInNode, Elements)};
                {false, MaxInNode} when MaxInNode > MaxOnRight ->
                    {MaxInNode, maps:get(MaxInNode, Elements)};
                {false, _} -> get_highest_internal(Prev)
            end
    end.


-spec get_internal(id() | undefined, key()) -> ?ERROR_NOT_FOUND | {ok, binary()}.
get_internal(undefined, _Key) ->
    ?ERROR_NOT_FOUND;
get_internal(NodeId, Key) ->
    case get(NodeId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        #node{elements = Elements, max_on_right = MaxOnRight, prev = Prev} ->
            case maps:find(Key, Elements) of
                {ok, Value} -> {ok, Value};
                error -> case Key > MaxOnRight of
                    true -> ?ERROR_NOT_FOUND;
                    false -> get_internal(Prev, Key)
                end
            end
    end.


-spec adjust_min_on_left(id() | undefined, key(), CheckToTheEnd :: boolean()) -> ok.
adjust_min_on_left(undefined, _CurrentMin, _CheckToTheEnd) ->
    ok;
adjust_min_on_left(NodeId, CurrentMin, CheckToTheEnd) ->
    Node = get(NodeId),
    UpdatedNode = Node#node{min_on_left = CurrentMin},
    case UpdatedNode of
        Node -> ok;
        _ -> save(NodeId, UpdatedNode)
    end,
    Min = case maps:size(Node#node.elements) of
        0 -> CurrentMin;
        _ -> min(CurrentMin, lists:min(maps:keys(Node#node.elements)))
    end,
    case not CheckToTheEnd andalso CurrentMin > Min of
        true -> ok;
        false -> adjust_min_on_left(Node#node.prev, Min, CheckToTheEnd)
    end.


-spec adjust_max_on_right(undefined | id(), #node{}) -> ok.
adjust_max_on_right(undefined, _) ->
    ok;
adjust_max_on_right(NodeId, PrevNode) ->
    #node{elements = Elements, node_id = NodeId, next = Next} = Node = get(NodeId),
    MaxOnRight = calculate_max_on_right(PrevNode),
    UpdatedNode = Node#node{max_on_right = MaxOnRight},
    case UpdatedNode of
        Node -> ok;
        _ -> save(NodeId, UpdatedNode)
    end,
    case MaxOnRight > lists:max(maps:keys(Elements)) of
        false -> ok;
        true -> adjust_max_on_right(Next, UpdatedNode)
    end.


-spec calculate_max_on_right(#node{}) -> key().
calculate_max_on_right(#node{elements = Elements, max_on_right = MaxOnRight}) ->
    case maps:size(Elements) of
        0 -> MaxOnRight;
        _ ->
            MaxInNode = lists:max(maps:keys(Elements)),
            case MaxOnRight of
                undefined -> MaxInNode;
                _ -> max(MaxInNode, MaxOnRight)
            end
    end.
    

-spec fetch_or_create_first_doc(id()) -> {#sentinel{}, #node{}} | ?ERROR_NOT_FOUND.
fetch_or_create_first_doc(SentinelId) ->
    case get(SentinelId) of
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND;
        #sentinel{first = undefined} = Sentinel ->
            NodeId = datastore_key:new(),
            Sentinel1 = Sentinel#sentinel{first = NodeId, last = NodeId},
            save(SentinelId, Sentinel1),
            Node = #node{node_id = NodeId, sentinel_id = SentinelId, node_num = 0},
            {Sentinel1, Node};
        #sentinel{first = FirstNodeId} = Sentinel ->
            {Sentinel, get(FirstNodeId)}
    end.


-spec fetch_last_doc(id()) -> #node{} | ?ERROR_NOT_FOUND.
fetch_last_doc(SentinelId) ->
    case get(SentinelId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        #sentinel{last = undefined} -> ?ERROR_NOT_FOUND;
        #sentinel{last = Last} = Sentinel -> {Sentinel, get(Last)}
    end.


-spec split_list([any()], non_neg_integer()) -> {[any()], [any()]}.
split_list(List, MaxElemsInFirstPart) ->
    case length(List) < MaxElemsInFirstPart of
        true -> {List, []};
        false -> lists:split(MaxElemsInFirstPart, List)
    end.
    

% fixme all below is temporary, to be removed

init() ->
    ?MODULE = ets:new(
        ?MODULE,
        [set, public, named_table, {read_concurrency, true}]
    ).

get(Id) ->
%%    io:format("get~n"),
    case ets:lookup(?MODULE, Id) of
        [{Id, Node}] -> Node;
        [] -> ?ERROR_NOT_FOUND
    end.

save(Id, Value) ->
%%    io:format("save: ~p~n~p~n~n", [Id, Value]),
    true = ets:insert(?MODULE, {Id, Value}).
    

del(Id) ->
%%    io:format("del~n"),
    ets:delete(?MODULE, Id).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

delete_struct_test() ->
    init(),
    ?assertEqual(ok, delete(<<"dummy_id">>)),
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    ?assertEqual(ok, delete(Id)),
    ?assertEqual([], ets:tab2list(?MODULE)).


add_test() ->
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10,30)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).
    

add1_test() ->
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(100, 1, -1))),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(100, 1, -1)),
    {Res, _} = list_elems(Id, 1000),
    ?assertMatch(Expected, lists:sort(Res)).


add2_test() ->
    ?assertEqual(?ERROR_NOT_FOUND, add_to_beginning(<<"dummy_id">>, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(100, 1, -1)))),
    ?assertEqual(ok, add_to_beginning(<<"dummy_id">>, [])),
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, [{1,1}]),
    ?assertMatch({[{1,1}], _}, list_elems(Id, 100)).


add_existing_test() ->
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10,30)),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).


add_overwrite_existing_test() ->
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    Expected = lists:foldl(fun(A, Acc) -> [{A, 2*A} | Acc] end, [], lists:seq(10, 32)),
    add_to_beginning(Id, Expected),
    ?assertMatch({Expected, _}, list_elems(Id, 100)),
    add_to_beginning(Id, [{20,40}]),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).


list_test() ->
    ?assertMatch({[], _}, list_elems(<<"dummy_id">>, 1000)),
    {ok, Id} = append_list:create(10),
    ?assertMatch({[], _}, list_elems(Id, 0)),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    lists:foldl(fun(X, LS) ->
        {Res, NewLS} = list_elems(LS, 1),
        ?assertEqual([{X, X}], Res),
        NewLS
    end, Id, lists:seq(30, 10, -1)),
    {_, ListingInfo} = list_elems(Id, 1000),
    ?assertMatch({[], _}, list_elems(ListingInfo, 100)).
    

delete_elems_test() ->
    ?assertEqual(?ERROR_NOT_FOUND, delete_elems(<<"dummy_id">>, [])),
    {ok, Id} = append_list:create(10),
    ?assertEqual(?ERROR_NOT_FOUND, delete_elems(Id, [])),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    delete_elems(Id, lists:seq(10, 20)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(21,30)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)),
    delete_elems(Id, lists:seq(25, 30)),
    Expected1 = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(21,24)),
    ?assertMatch({Expected1, _}, list_elems(Id, 100)).

delete_elems1_test() ->
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(1, 100))),
    delete_elems(Id, lists:seq(1,100, 2)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(2,100, 2)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).


delete_elems2_test() ->
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    delete_elems(Id, [30]),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10,29)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)),
    delete_elems(Id, lists:seq(20,30)),
    Expected1 = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10,19)),
    ?assertMatch({Expected1, _}, list_elems(Id, 100)).


delete_elems3_test() ->
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    delete_elems(Id, lists:seq(10, 29)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(30, 30)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).


delete_elems4_test() ->
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 31))),
    delete_elems(Id, lists:seq(10, 30)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(31, 31)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).


delete_elems5_test() ->
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 31))),
    lists:foreach(fun(Elem) ->
        delete_elems(Id, [Elem])
    end, lists:seq(30, 10, -1)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(31, 31)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).


delete_elems6_test() ->
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 31))),
    lists:foreach(fun(Elem) ->
        delete_elems(Id, [Elem])
    end, lists:seq(10, 30)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(31, 31)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).


delete_elems7_test() ->
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(31, 20, -1))),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(20, 10, -1))),
    lists:foreach(fun(Elem) ->
        delete_elems(Id, [Elem])
    end, lists:seq(30, 10, -1)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(31, 31)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).


delete_elems8_test() ->
    {ok, Id} = append_list:create(1),
    lists:foreach(fun(Elem) ->
        add_to_beginning(Id, [{Elem, Elem}])
    end, lists:seq(5, 1, -1)),
    delete_elems(Id, [5]),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(4, 1, -1)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).


delete_between_listings_test() ->
    {ok, Id} = append_list:create(10),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    {_, ListingInfo} = list_elems(Id, 2),
    delete_elems(Id, [28,27]),
    {Res, _} = list_elems(ListingInfo, 1),
    ?assertMatch([{26,26}], Res),
    delete_elems(Id, lists:seq(20,29)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 19)),
    ?assertMatch({Expected, _}, list_elems(ListingInfo, 100)).


get_highest_test() ->
    ?assertEqual(undefined, get_highest(<<"dummy_id">>)),
    {ok, Id} = append_list:create(10),
    ?assertEqual(undefined, get_highest(Id)),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(1,100))),
    ?assertEqual({100, 100}, get_highest(Id)),
    delete_elems(Id, lists:seq(2,99)),
    ?assertEqual({100, 100}, get_highest(Id)),
    delete_elems(Id, [100]),
    ?assertEqual({1,1}, get_highest(Id)),
    delete_elems(Id, [1]),
    ?assertEqual(undefined, get_highest(Id)),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(100,1, -1))),
    ?assertEqual({100, 100}, get_highest(Id)),
    delete_elems(Id, lists:seq(2,99)),
    ?assertEqual({100, 100}, get_highest(Id)).


get_test() ->
    ?assertEqual(?ERROR_NOT_FOUND, get(<<"dummy_id">>), 8),
    {ok, Id} = append_list:create(10),
    ?assertEqual(?ERROR_NOT_FOUND, get(Id, 8)),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(1,100))),
    ?assertEqual({ok, 8}, get(Id, 8)),
    delete_elems(Id, lists:seq(2,99)),
    ?assertEqual(?ERROR_NOT_FOUND, get(Id, 8)),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(100,1, -1))),
    ?assertEqual({ok, 8}, get(Id, 8)),
    delete_elems(Id, lists:seq(2,99)),
    ?assertEqual(?ERROR_NOT_FOUND, get(Id, 8)).

-endif.
