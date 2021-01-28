%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that are responsible for retrieving
%%% elements from sliding_proplist. For more details about sliding proplist 
%%% consult `sliding_proplist` module doc.
%%% @end
%%%-------------------------------------------------------------------
-module(sliding_proplist_get).
-author("Michal Stanisz").

-include("modules/datastore/sliding_proplist.hrl").

%% API
-export([fold/4, fold/6, get_elements/3, get_highest/1, get_max_key/1]).


% This record is used as cache during listing. Holds information where fold should continue.
-record(listing_state, {
    structure_id :: sliding_proplist:id() | undefined,
    last_node_id :: sliding_proplist:node_id() | undefined,
    last_listed_key :: sliding_proplist:key() | undefined,
    % Node number of last node that was encountered during elements listing.
    % By remembering this value it is possible to find where fold should 
    % continue after last encountered node was deleted.
    last_node_number :: sliding_proplist:node_number() | undefined,
    direction =  back_from_newest :: direction(),
    finished = false :: boolean()
}).

-type fold_fun() :: fun((sliding_proplist:element(), term()) -> {ok, term()} | stop).
-type direction() :: back_from_newest | forward_from_oldest.
-type batch_size() :: non_neg_integer().
-type fold_result(Acc) :: {more, Acc, state()} | {done, Acc}.
-type fold_result() :: fold_result(term()).
-opaque state() :: #listing_state{}.

-export_type([fold_fun/0, direction/0, batch_size/0, fold_result/0, fold_result/1, state/0]).

%%=====================================================================
%% API
%%=====================================================================

-spec fold(state() | sliding_proplist:id(), batch_size(), fold_fun(), term()) -> 
    fold_result() | {error, term()}.
fold(#listing_state{finished = true}, _Size, _FoldFun, Acc) ->
    {done, Acc};
fold(#listing_state{last_node_id = undefined}, _Size, _FoldFun, Acc) ->
    {done, Acc};
fold(#listing_state{last_node_id = NodeId} = State, _Size = 0, _FoldFun, Acc) ->
    {more, Acc, State#listing_state{last_node_id = NodeId}};
fold(#listing_state{
    last_node_id = NodeId,
    direction = Direction
} = State, Size, FoldFun, Acc) ->
    case find_node(State) of
        #node{elements = AllElementsInNode, node_number = NodeNumber} = Node ->
            Elements = select_not_listed_elements(State, AllElementsInNode, NodeNumber),
            NumberOfElementsToTake = min(Size, length(Elements)),
            {ListedElements, NotListedElements} = lists:split(NumberOfElementsToTake, Elements),
            NextStartNodeId = case NotListedElements of
                [] -> select_neighbour(Direction, Node);
                _ -> NodeId
            end,
            {Status, Result} = apply_fold_fun(FoldFun, Acc, ListedElements),
            Finished = case Status of
                stop -> true;
                continue -> false
            end,
            {LastListedKey, _} = lists:last(ListedElements),
            fold(State#listing_state{
                last_node_id = NextStartNodeId,
                last_node_number = NodeNumber,
                last_listed_key = LastListedKey,
                finished = Finished
            }, Size - NumberOfElementsToTake, FoldFun, Result);
        {error, _} = Error -> Error
    end.


-spec fold(sliding_proplist:id(), sliding_proplist:node_id(), batch_size(), direction(), 
    fold_fun(), term()) -> fold_result() | {error, term()}.
fold(StructureId, StartingNodeId, Size, Direction, FoldFun, Acc0) when is_binary(StructureId) ->
    State = #listing_state{
        structure_id = StructureId,
        last_node_id = StartingNodeId,
        direction = Direction
    },
    fold(State, Size, FoldFun, Acc0).


-spec get_elements(sliding_proplist:node_id() | undefined, [sliding_proplist:key()], direction()) -> 
    [sliding_proplist:element()].
get_elements(undefined, _Keys, _Direction) ->
    [];
get_elements(NodeId, Keys, Direction) ->
    case sliding_proplist_persistence:get_node(NodeId) of
        {ok, Node} ->
            {Selected, RemainingKeys} = select_elements_from_node(Node, Keys, Direction),
            case RemainingKeys of
                [] -> Selected;
                _ -> Selected ++ get_elements(
                    select_neighbour(Direction, Node), RemainingKeys, Direction)
            end;
        {error, not_found} -> []
    end.


-spec get_highest(undefined | sliding_proplist:node_id()) -> 
    {ok, sliding_proplist:element()} | {error, term()}.
get_highest(undefined) -> {error, not_found};
get_highest(NodeId) ->
    case sliding_proplist_persistence:get_node(NodeId) of
        {ok, #node{elements = Elements, max_in_older = MaxInOlder, prev = Prev}} ->
            case {Prev == undefined, maps:size(Elements) > 0 andalso lists:max(maps:keys(Elements))} of
                {true, false} -> {error, not_found};
                {true, MaxKeyInNode} -> {ok, {MaxKeyInNode, maps:get(MaxKeyInNode, Elements)}};
                {false, MaxKeyInNode} when MaxKeyInNode > MaxInOlder ->
                    {ok, {MaxKeyInNode, maps:get(MaxKeyInNode, Elements)}};
                {false, _} -> get_highest(Prev)
            end;
        {error, _} = Error -> Error
    end.


-spec get_max_key(undefined | sliding_proplist:node_id()) -> 
    {ok, sliding_proplist:key()} | {error, term()}.
get_max_key(undefined) -> {error, not_found};
get_max_key(NodeId) ->
    case sliding_proplist_persistence:get_node(NodeId) of
        {ok, Node} -> case sliding_proplist_utils:get_max_key_in_prev_nodes(Node) of
            undefined -> {error, not_found};
            Res -> {ok, Res}
        end;
        {error, _} = Error -> Error
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec select_not_listed_elements(state(), sliding_proplist:elements_map(), 
    sliding_proplist:node_number()) -> [sliding_proplist:element()].
select_not_listed_elements(#listing_state{
    last_listed_key = LastKey,
    last_node_number = LastNodeNum,
    direction = Direction
}, OrigElements, NodeNumber) ->
    Elements = case LastKey == undefined orelse LastNodeNum =/= NodeNumber of
        true -> OrigElements;
        false ->
            % when node since last listing was not deleted select only 
            % elements with keys less than those already listed
            % (higher when listing forward from oldest) 
            case Direction of
                back_from_newest -> maps:filter(fun(Key, _) -> 
                    Key < LastKey end, OrigElements);
                forward_from_oldest -> maps:filter(fun(Key, _) -> 
                    Key > LastKey end, OrigElements)
            end
    end,
    case Direction of
        back_from_newest -> lists:reverse(lists:sort(maps:to_list(Elements)));
        forward_from_oldest -> lists:sort(maps:to_list(Elements))
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function returns Node based on information included in ListingState.
%% If node with given id exists it is returned. If not (it was deleted in 
%% meantime) first node with number less (greater when listing started from 
%% last node) than `last_node_num` is returned.
%% @end
%%--------------------------------------------------------------------
-spec find_node(#listing_state{}) -> sliding_proplist:list_node() | {error, term()}.
find_node(State) ->
    #listing_state{
        last_node_id = NodeId, 
        last_node_number = LastNodeNum, 
        structure_id = StructId, 
        direction = Direction
    } = State,
    case sliding_proplist_persistence:get_node(NodeId) of
        {ok, #node{node_number = NodeNum} = Node}  ->
            NodeFound = case Direction of
                back_from_newest -> 
                    not (is_integer(LastNodeNum) andalso NodeNum > LastNodeNum);
                forward_from_oldest -> 
                    not (is_integer(LastNodeNum) andalso NodeNum < LastNodeNum)
            end,
            case NodeFound of
                true -> Node;
                false ->
                    find_node(State#listing_state{
                        last_node_id = select_neighbour(Direction, Node)
                    })
            end;
        {error, not_found} ->
            {ok, Sentinel} = sliding_proplist_persistence:get_node(StructId),
            StartingNodeId = sliding_proplist_utils:get_starting_node_id(Direction, Sentinel),
            case StartingNodeId of
                undefined -> {error, not_found};
                _ -> find_node(State#listing_state{last_node_id = StartingNodeId})
            end
    end.


%% @private
-spec apply_fold_fun(fold_fun(), term(), [sliding_proplist:element()]) -> 
    {continue | stop, [term()]}.
apply_fold_fun(FoldFun, Acc0, OriginalElements) ->
    lists:foldl(fun
        (_Elem, {stop, Acc}) -> {stop, Acc};
        (E, {continue, Acc}) ->
            case FoldFun(E, Acc) of
                stop -> {stop, Acc};
                {ok, Res} -> {continue, Res}
            end
    end, {continue, Acc0}, OriginalElements).


%% @private
-spec select_elements_from_node(sliding_proplist:list_node(), [sliding_proplist:key()], 
    direction()) -> {[sliding_proplist:element()], [sliding_proplist:key()]}.
select_elements_from_node(#node{elements = Elements} = Node, Keys, Direction) ->
    Selected = maps:with(Keys, Elements),
    RemainingKeys = Keys -- maps:keys(Selected),
    {maps:to_list(Selected), filter_keys(Direction, Node, RemainingKeys)}.


%% @private
-spec filter_keys(direction(), sliding_proplist:list_node(), [sliding_proplist:key()]) -> 
    [sliding_proplist:key()].
filter_keys(back_from_newest, #node{max_in_older = Max}, Keys) ->
    [Key || Key <- Keys, Key =< Max];
filter_keys(forward_from_oldest, #node{min_in_newer = Min}, Keys) ->
    [Key || Key <- Keys, Key >= Min].


%% @private
-spec select_neighbour(direction(), sliding_proplist:list_node()) -> 
    sliding_proplist:node_id().
select_neighbour(back_from_newest, #node{prev = Prev}) -> Prev;
select_neighbour(forward_from_oldest, #node{next = Next}) -> Next.
