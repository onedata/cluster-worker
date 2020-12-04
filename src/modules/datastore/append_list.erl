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
%%% It is highly recommended that each new element have key greater 
%%% than those already existing (if not this structure might be inefficient).
%%%
%%% This structure is implemented as a bidirectional linked list, with each 
%%% node storing up to ?MAX_ELEMS_PER_NODE elements (i.e key value pairs). 
%%% 
%%%                    +--------+             +--------+             
%%%          prev      |        |    prev     |        |    prev      
%%%        +------->   |  node  |  +------->  |  node  |  +------->  
%%%  ...               |        |             |        |              ...  
%%%        <-------+   |        |  <-------+  |        |  <-------+  
%%%          next      +--------+    next     +--------+    next     
%%% 
%%% Adding new elements is only allowed to the beginning of the list. 
%%% Deletion of arbitrary elements is allowed, although it is recommended 
%%% to delete elements from the list's end. 
%%%
%%% During elements deletion if two adjacent nodes have less than 
%%% ?MAX_ELEMS_PER_NODE elements combined, one of those nodes (the newer one) 
%%% will be deleted, and all elements will be now stored in the other node.
%%%
%%% Each node stores also value `min_on_left`. It represents minimal value 
%%% in all nodes, that are newer (are pointed by `next`) than this node. 
%%% It is used during deletion  - it allows to determine whether it is 
%%% necessary to fetch next nodes and allows to finish deletion without 
%%% traversing all list nodes. That is why it is optimal to have 
%%% increasing keys.
%%%
%%% Adding elements with the same key may or may not result in duplication
%%% of this key in structure. It depends whether those elements would be 
%%% stored in the same node (only one element would be stored in that case) 
%%% or in many nodes (both elements would be stored).
%%% @end
%%%-------------------------------------------------------------------
-module(append_list).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include_lib("ctool/include/errors.hrl").

-compile({no_auto_import, [get/1]}).
-define(MAX_ELEMS_PER_NODE, 10). % fixme set real value

-type id() :: binary().
-type key() :: integer().
-type elem() :: {key(), binary()}.

-record(node, {
    node_id :: id(),
    structure_id :: id(),
    prev = undefined :: id(),
    next = undefined :: id(),
    elements = #{} :: #{integer() => binary()},
    min_on_left :: integer(),
    max_on_right :: integer(),
    node_num :: integer()
}).

-record(guard, {
    id :: id(),
    first = undefined :: id(),
    last = undefined :: id()
}).

-record(listing_info, {
    id :: id(),
    size :: non_neg_integer(),
    finished = false ::  boolean(),
    node_id :: id(),
    last_key :: integer(),
    seen_node_num :: integer()
}).

%% API
-export([create/0, delete/1]).
-export([add_to_beginning/2, delete_elems/2, list_elems/2, get_highest/1]).

% fixme remove
-export([init/0]).

-spec create() -> {ok, id()}.
create() ->
    Id = datastore_key:new(),
    Guard = #guard{id = Id},
    set(Id, Guard),
    {ok, Id}.


-spec delete(id()) -> ok | ?ERROR_NOT_FOUND.
delete(StructId) ->
    case get(StructId) of
        ?ERROR_NOT_FOUND -> ok;
        Guard ->
            delete_all_nodes(Guard),
            true = del(Guard#guard.id),
            ok
    end.


- spec list_elems(id() | #listing_info{}, non_neg_integer()) -> {[elem()], #listing_info{}}.
list_elems(_, 0) ->
    {[], #listing_info{finished = true}};
list_elems(#listing_info{finished = true}, _Size) ->
    {[], #listing_info{finished = true}};
list_elems(#listing_info{} = ListingInfo, Size) ->
    list_elems_internal(ListingInfo#listing_info{size = Size}, []);
list_elems(StructureId, Size) ->
    case get(StructureId) of
        ?ERROR_NOT_FOUND -> list_elems(StructureId, 0);
        #guard{first = FirstNodeId} ->
            list_elems_internal(#listing_info{
                id = StructureId, 
                node_id = FirstNodeId, 
                size = Size
            }, [])
    end.


-spec add_to_beginning(id(), [elem()]) -> ok | ?ERROR_NOT_FOUND.
add_to_beginning(_StructureId, []) ->
    ok;
add_to_beginning(StructureId, Batch) ->
    case fetch_or_create_first_doc(StructureId) of
        {error, _} = Error -> Error;
        {Guard, #node{elements = ElementsInFirstNode} = FirstNode} ->
            ReversedBatch = lists:reverse(Batch),
            ToFill = ?MAX_ELEMS_PER_NODE - maps:size(ElementsInFirstNode),
            {ElemsToAdd, ElementsTail} = case length(Batch) < ToFill of
                true -> {ReversedBatch, []};
                false -> lists:split(ToFill, ReversedBatch)
            end,
            Node = FirstNode#node{
                elements = maps:merge(
                    ElementsInFirstNode,
                    maps:from_list(ElemsToAdd)
                )
            },
            [Min | _] = Mins = lists:foldl(fun
                ({X, _}, undefined) -> [X]; 
                ({X, _}, [M | _] = Acc) -> [min(X, M) | Acc] 
            end, undefined, Batch),
            MinsTail = case length(Batch) < ToFill of
                true -> [];
                false -> 
                    {_, MT} = lists:split(ToFill, Mins), 
                    MT
            end,
            adjust_min_on_left(Node#node.prev, Min, false),
            add_elems_internal(Guard, ElementsTail, MinsTail, Node)
    end.


-spec delete_elems(id(), [key()]) -> ok | ?ERROR_NOT_FOUND.
delete_elems(StructureId, Elems) ->
    case fetch_last_doc(StructureId) of
        {error, _} = Error -> Error;
        LastNode ->
            delete_elems_internal(LastNode, Elems, undefined)
    end.


-spec get_highest(id()) -> elem() | undefined.
get_highest(StructureId) ->
    case get(StructureId) of
        ?ERROR_NOT_FOUND -> undefined;
        Guard -> get_highest_internal(Guard#guard.first)
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

-spec delete_all_nodes(#guard{}) -> ok.
delete_all_nodes(#guard{first = undefined}) ->
    ok;
delete_all_nodes(#guard{first = NodeId} = Guard) ->
    #node{prev = Prev} = get(NodeId),
    NewGuard = Guard#guard{first = Prev},
    set(Guard#guard.id, NewGuard),
    del(NodeId),
    delete_all_nodes(NewGuard).


-spec list_elems_internal(#listing_info{}, [elem()]) -> {[elem()], #listing_info{}}.
list_elems_internal(#listing_info{node_id = undefined}, Acc) ->
    {Acc, #listing_info{
        finished = true
    }};
list_elems_internal(#listing_info{size = 0, node_id = NodeId} = ListingInfo, Acc) ->
    {L, _} = lists:last(Acc),
    {Acc, ListingInfo#listing_info{
        node_id = NodeId, 
        last_key = L
    }};
list_elems_internal(#listing_info{
    size = Size, 
    node_id = NodeId, 
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
                node_id = NextNodeId, 
                size = Size - NumElemsToTake, 
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
find_node(#listing_info{node_id = NodeId, seen_node_num = SeenNodeNum, id = StructId} = LS) ->
    case get(NodeId) of
        ?ERROR_NOT_FOUND -> 
            #guard{first = First} = get(StructId),
            find_node(LS#listing_info{node_id = First});
        #node{node_num = NodeNum} = Node when not (is_integer(SeenNodeNum) andalso NodeNum > SeenNodeNum) ->
            Node;
        #node{prev = Prev} ->
            find_node(LS#listing_info{node_id = Prev})
    end.


-spec add_elems_internal(#guard{}, [elem()], [key()], #node{}) -> ok.
add_elems_internal(#guard{id = StructId} = Guard, [], _, #node{node_id = NodeId} = Node) ->
    set(NodeId, Node),
    set(StructId, Guard#guard{first = NodeId}),
    ok;
add_elems_internal(#guard{id = StructId} = Guard, Batch, [Min | _] = Mins, PrevNode) ->
    #node{node_id = PrevNodeId} = PrevNode,
    Size = min(length(Batch), ?MAX_ELEMS_PER_NODE),
    {ElemsToAdd, Tail} = lists:split(Size, Batch),
    {_, MinsTail} = lists:split(Size, Mins),
    #node{node_id = NewFirstNodeId} = NewFirstNode = 
        prepare_new_first_node(StructId, ElemsToAdd, PrevNode),
    set(PrevNodeId, PrevNode#node{
        next = NewFirstNodeId,
        min_on_left = Min
    }),
    add_elems_internal(Guard, Tail, MinsTail, NewFirstNode).


-spec prepare_new_first_node(id(), [elem()], PrevNode :: #node{}) -> 
    NewNode :: #node{}.
prepare_new_first_node(StructureId, ElemsList, #node{
    node_id = PrevNodeId, 
    node_num = PrevNodeNum
} = PrevNode) ->
    NodeNum = PrevNodeNum + 1,
    Max = calculate_max_on_right(PrevNode),
    #node{
        node_id = datastore_key:adjacent_from_digest([NodeNum], StructureId),
        structure_id = StructureId,
        prev = PrevNodeId,
        max_on_right = Max,
        node_num = NodeNum,
        elements = maps:from_list(ElemsList)
    }.


-spec delete_elems_internal(
    CurrentNode :: undefined | #node{}, 
    [key()], 
    PrevNode :: undefined | #node{}
) -> ok.
delete_elems_internal(undefined, _, _PrevNode) ->
    ok;
delete_elems_internal(CurrentNode, [], PrevNode) ->
    #node{min_on_left = MinOnLeft, elements = Elements} = CurrentNode,
    Min = lists:min([MinOnLeft | maps:keys(Elements)]),
    adjust_min_on_left(PrevNode#node.node_id, Min, true);
delete_elems_internal(CurrentNode, ElemsToDelete, PrevNode) ->
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
        maps:size(ElementsInPreviousNode) + maps:size(NewElements) =< ?MAX_ELEMS_PER_NODE,
    {NewCurrentNode, NewNextNode} = case ShouldCoalesceNodes of
        false -> 
            {PrevNodeId, MaxOnRight} = case PrevNode of
                undefined -> {undefined, undefined};
                _ -> {PrevNode#node.node_id, calculate_max_on_right(PrevNode)}
            end, 
            D = CurrentNode#node{
                elements = NewElements, 
                prev = PrevNodeId, 
                max_on_right = MaxOnRight
            },
            set(CurrentNodeId, D),
            {D, NextNode};
        true -> 
            D = coalesce_nodes(CurrentNode, PrevNode, maps:merge(ElementsInPreviousNode, NewElements)),
            NextNode1 = case NextNode of
                undefined -> undefined;
                _ ->
                    N = NextNode#node{prev = PrevNode#node.node_id},
                    set(NextNodeId, N),
                    N
            end,
            {D, NextNode1}
    end,
    ShouldStop = NewElemsToDelete == [] orelse 
        CurrentNode#node.min_on_left > lists:max(NewElemsToDelete),
    NewElemsToDelete1 = case ShouldStop of
        true -> [];
        false -> NewElemsToDelete
    end,
    delete_elems_internal(NewNextNode, NewElemsToDelete1, NewCurrentNode).


-spec coalesce_nodes(CurrentNode :: #node{}, PrevNode :: #node{}, [elem()]) -> 
    #node{}.
coalesce_nodes(CurrentNode, PrevNode, Elements) ->
    % Set min_on_left value to undefined if this is first node in structure. 
    % If not leave it unchanged as it will be adjusted after deletion is completed.
    MinOnLeft = case CurrentNode#node.next of
        undefined -> undefined;
        _ -> PrevNode#node.min_on_left
    end, 
    NewNode = PrevNode#node{
        elements = Elements,
        next = CurrentNode#node.next,
        min_on_left = MinOnLeft
    },
    set(NewNode#node.node_id, NewNode),
    case CurrentNode#node.next of
        undefined -> % this is the first doc, modify guard
            StructId = CurrentNode#node.structure_id,
            Guard = get(StructId),
            set(StructId, Guard#guard{first = PrevNode#node.node_id});
        _ -> ok
    end,
    del(CurrentNode#node.node_id),
    NewNode.


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


-spec calculate_max_on_right(#node{}) -> key().
calculate_max_on_right(#node{elements = Elements, max_on_right = MaxOnRight}) ->
    MaxInNode = lists:max(maps:keys(Elements)),
    case MaxOnRight of
        undefined -> MaxInNode;
        Max -> max(MaxInNode, Max)
    end.


-spec adjust_min_on_left(id() | undefined, key(), CheckToTheEnd :: boolean()) -> ok.
adjust_min_on_left(undefined, _CurrentMin, _CheckToTheEnd) ->
    ok;
adjust_min_on_left(NodeId, CurrentMin, CheckToTheEnd) ->
    Node = get(NodeId),
    UpdatedNode = Node#node{min_on_left = CurrentMin},
    set(NodeId, UpdatedNode),
    Min = case maps:size(Node#node.elements) of
        0 -> CurrentMin;
        _ -> min(CurrentMin, lists:min(maps:keys(Node#node.elements)))
    end,
    case not CheckToTheEnd andalso CurrentMin > Min of
        true -> ok;
        false -> adjust_min_on_left(Node#node.prev, Min, CheckToTheEnd)
    end.

-spec fetch_or_create_first_doc(id()) -> {#guard{}, #node{}} | ?ERROR_NOT_FOUND.
fetch_or_create_first_doc(StructureId) ->
    case get(StructureId) of
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND;
        #guard{first = undefined} = Guard ->
            NodeId = datastore_key:new(),
            Guard1 = Guard#guard{first = NodeId, last = NodeId},
            set(StructureId, Guard1),
            Node = #node{node_id = NodeId, structure_id = StructureId, node_num = 0},
            {Guard1, Node};
        #guard{first = FirstNodeId} = Guard ->
            {Guard, get(FirstNodeId)}
    end.


-spec fetch_last_doc(id()) -> #node{} | ?ERROR_NOT_FOUND.
fetch_last_doc(StructureId) ->
    case get(StructureId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        #guard{last = undefined} -> ?ERROR_NOT_FOUND;
        #guard{last = Last} -> get(Last)
    end.
    

% fixme all below is temporary, remove 

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

set(Id, Value) ->
%%    io:format("set~n"),
    true = ets:insert(?MODULE, {Id, Value}).
    

del(Id) ->
%%    io:format("del~n"),
    ets:delete(?MODULE, Id).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

delete_struct_test() ->
    init(),
    ?assertEqual(ok, delete(<<"dummy_id">>)),
    {ok, Id} = append_list:create(),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    ?assertEqual(ok, delete(Id)),
    ?assertEqual([], ets:tab2list(?MODULE)).


add_test() ->
    {ok, Id} = append_list:create(),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10,30)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).
    

add1_test() ->
    {ok, Id} = append_list:create(),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(100, 1, -1))),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(100, 1, -1)),
    {Res, _} = list_elems(Id, 1000),
    ?assertMatch(Expected, lists:sort(Res)).


add2_test() ->
    ?assertEqual(?ERROR_NOT_FOUND, add_to_beginning(<<"dummy_id">>, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(100, 1, -1)))),
    ?assertEqual(ok, add_to_beginning(<<"dummy_id">>, [])),
    {ok, Id} = append_list:create(),
    add_to_beginning(Id, [{1,1}]),
    ?assertMatch({[{1,1}], _}, list_elems(Id, 100)).


list_test() ->
    ?assertMatch({[], _}, list_elems(<<"dummy_id">>, 1000)),
    {ok, Id} = append_list:create(),
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
    {ok, Id} = append_list:create(),
    ?assertEqual(?ERROR_NOT_FOUND, delete_elems(Id, [])),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    delete_elems(Id, lists:seq(10, 20)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(21,30)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)),
    delete_elems(Id, lists:seq(25, 30)),
    Expected1 = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(21,24)),
    ?assertMatch({Expected1, _}, list_elems(Id, 100)).

delete_elems1_test() ->
    {ok, Id} = append_list:create(),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(1, 100))),
    delete_elems(Id, lists:seq(1,100, 2)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(2,100, 2)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).


delete_elems2_test() ->
    {ok, Id} = append_list:create(),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    delete_elems(Id, [30]),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10,29)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)),
    delete_elems(Id, lists:seq(20,30)),
    Expected1 = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10,19)),
    ?assertMatch({Expected1, _}, list_elems(Id, 100)).


delete_elems3_test() ->
    {ok, Id} = append_list:create(),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    delete_elems(Id, lists:seq(10, 29)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(30, 30)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).


delete_elems4_test() ->
    {ok, Id} = append_list:create(),
    add_to_beginning(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 31))),
    delete_elems(Id, lists:seq(10, 30)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(31, 31)),
    ?assertMatch({Expected, _}, list_elems(Id, 100)).


delete_between_listings_test() ->
    {ok, Id} = append_list:create(),
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
    {ok, Id} = append_list:create(),
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

-endif.
