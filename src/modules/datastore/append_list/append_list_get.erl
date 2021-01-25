%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that are responsible for retrieving
%%% elements from append_list. For more details about this structure 
%%% consult `append_list` module doc.
%%% @end
%%%-------------------------------------------------------------------
-module(append_list_get).
-author("Michal Stanisz").

-include("modules/datastore/append_list.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([fold/4, get_elements/3, get_highest/1, get_max_key/1]).


% This record is used as cache during listing. Holds information where listing should continue.
% @see find_node/1 % fixme
-record(listing_state, {
    structure_id :: append_list:id() | undefined,
    last_node_id :: append_list:id() | undefined,
    last_listed_key :: integer() | undefined,
    % Node number of last node that was encountered during elements listing.
    last_node_number :: non_neg_integer() | undefined,
    direction =  back_from_newest :: direction(),
    finished = false :: boolean()
}).

-type fold_fun() :: fun((append_list:element()) -> {ok, term()} | stop).
-type direction() :: back_from_newest | forward_from_oldest.
-type batch_size() :: non_neg_integer().
-type fold_result() :: {more, [append_list:element()], state()} | {done, [append_list:element()]}.
-opaque state() :: #listing_state{}.

-export_type([fold_fun/0, direction/0, batch_size/0, fold_result/0, state/0]).

%%=====================================================================
%% API
%%=====================================================================

-spec fold(#listing_state{} | append_list:id(), batch_size(), direction(), 
    fold_fun()) -> fold_result() | {error, term()}.
fold(#listing_state{} = State, Size, _Direction, FoldFun) ->
    prepare_fold_result(continue_fold(State, Size, FoldFun, []));
fold(StructureId, Size, Direction, FoldFun) when is_binary(StructureId) ->
    case append_list_persistence:get_node(StructureId) of
        {error, _} = Error -> Error;
        #sentinel{} = Sentinel ->
            StartingNodeId = append_list_utils:get_starting_node_id(Direction, Sentinel),
            prepare_fold_result(continue_fold(#listing_state{
                structure_id = StructureId,
                last_node_id = StartingNodeId,
                direction = Direction
            }, Size, FoldFun, []))
    end.


-spec get_elements(append_list:id() | undefined, [append_list:key()], direction()) -> [append_list:element()].
get_elements(undefined, _Keys, _Direction) ->
    [];
get_elements(NodeId, Keys, Direction) ->
    case append_list_persistence:get_node(NodeId) of
        ?ERROR_NOT_FOUND -> [];
        #node{} = Node ->
            {Selected, RemainingKeys} = select_elements_from_node(Node, Keys, Direction),
            case RemainingKeys of
                [] -> Selected;
                _ -> Selected ++ get_elements(select_neighbour(Direction, Node), RemainingKeys, Direction)
            end
    end.


-spec get_highest(undefined | append_list:id()) -> append_list:element() | ?ERROR_NOT_FOUND.
get_highest(undefined) -> ?ERROR_NOT_FOUND;
get_highest(NodeId) ->
    case append_list_persistence:get_node(NodeId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        #node{elements = Elements, max_on_right = MaxOnRight, prev = Prev} ->
            case {Prev == undefined, maps:size(Elements) > 0 andalso lists:max(maps:keys(Elements))} of
                {true, false} -> ?ERROR_NOT_FOUND;
                {true, MaxInNode} -> {MaxInNode, maps:get(MaxInNode, Elements)};
                {false, MaxInNode} when MaxInNode > MaxOnRight ->
                    {MaxInNode, maps:get(MaxInNode, Elements)};
                {false, _} -> get_highest(Prev)
            end
    end.


-spec get_max_key(undefined | append_list:id()) -> append_list:key() | ?ERROR_NOT_FOUND.
get_max_key(undefined) -> ?ERROR_NOT_FOUND;
get_max_key(NodeId) ->
    case append_list_persistence:get_node(NodeId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        Node -> case append_list_utils:get_max_key_in_prev_nodes(Node) of
            undefined -> ?ERROR_NOT_FOUND;
            Res -> Res
        end
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec prepare_fold_result({[append_list:element()], state()}) -> fold_result().
prepare_fold_result({Result, #listing_state{finished = true}}) ->
    {done, Result};
prepare_fold_result({Result, #listing_state{finished = false} = State}) ->
    {more, Result, State}.


%% @private
-spec continue_fold(state(), batch_size(), fold_fun(), [append_list:element()]) -> 
    {[append_list:element()], state()}.
continue_fold(#listing_state{finished = true} = State, _Size, _FoldFun, Acc) ->
    {Acc, State};
continue_fold(#listing_state{last_node_id = undefined} = State, _Size, _FoldFun, Acc) ->
    {Acc, State#listing_state{finished = true}};
continue_fold(#listing_state{last_node_id = NodeId} = State, _Size = 0, _FoldFun, Acc) ->
    {Acc, State#listing_state{last_node_id = NodeId}};
continue_fold(#listing_state{
    last_node_id = NodeId,
    direction = Direction
} = State, Size, FoldFun, Acc) ->
    case find_node(State) of
        #node{elements = AllElementsInNode, node_number = NodeNumber} = Node ->
            Elements = retrieve_not_listed_elements(State, AllElementsInNode, NodeNumber),
            NumberOfElementsToTake = min(Size, length(Elements)),
            {ListedElements, NotListedElements} = lists:split(NumberOfElementsToTake, Elements),
            NextStartNodeId = case NotListedElements of
                [] -> select_neighbour(Direction, Node);
                _ -> NodeId
            end,
            {Status, Result} = apply_fold_fun(FoldFun, ListedElements),
            Finished = case Status of
                stop -> true;
                continue -> false
            end,
            {LastListedKey, _} = lists:last(ListedElements),
            continue_fold(State#listing_state{
                last_node_id = NextStartNodeId,
                last_node_number = NodeNumber,
                last_listed_key = LastListedKey,
                finished = Finished
            }, Size - NumberOfElementsToTake, FoldFun, Acc ++ lists:reverse(Result));
        {error, _} = Error -> Error
    end.


%% @private
-spec retrieve_not_listed_elements(state(), append_list:elements_map(), append_list:node_number()) -> 
    [append_list:element()].
retrieve_not_listed_elements(#listing_state{
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
                back_from_newest -> maps:filter(fun(Key, _) -> Key < LastKey end, OrigElements);
                forward_from_oldest -> maps:filter(fun(Key, _) -> Key > LastKey end, OrigElements)
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
-spec find_node(#listing_state{}) -> #node{} | ?ERROR_NOT_FOUND.
find_node(State) ->
    #listing_state{
        last_node_id = NodeId, 
        last_node_number = LastNodeNum, 
        structure_id = StructId, 
        direction = StartFrom
    } = State,
    case append_list_persistence:get_node(NodeId) of
        ?ERROR_NOT_FOUND ->
            StartingNodeId = append_list_utils:get_starting_node_id(
                StartFrom, append_list_persistence:get_node(StructId)),
            case StartingNodeId of
                undefined -> ?ERROR_NOT_FOUND;
                _ -> find_node(State#listing_state{last_node_id = StartingNodeId})
            end;
        #node{node_number = NodeNum} = Node  ->
            NodeFound = case StartFrom of
                back_from_newest -> not (is_integer(LastNodeNum) andalso NodeNum > LastNodeNum);
                forward_from_oldest -> not (is_integer(LastNodeNum) andalso NodeNum < LastNodeNum)
            end,
            case NodeFound of
                true -> Node;
                false ->
                    find_node(State#listing_state{
                        last_node_id = select_neighbour(StartFrom, Node)
                    })
            end
    end.


%% @private
-spec apply_fold_fun(fold_fun(), [append_list:element()]) -> {continue | stop, [term()]}.
apply_fold_fun(FoldFun, OriginalElements) ->
    lists:foldl(fun
        (_Elem, {stop, Elements}) -> {stop, Elements};
        (E, {continue, Elements}) ->
            case FoldFun(E) of
                stop -> {stop, Elements};
                {ok, Res} -> {continue, [Res | Elements]}
            end
    end, {continue, []}, OriginalElements).


%% @private
-spec select_elements_from_node(#node{}, [append_list:key()], direction()) ->
    {[append_list:element()], [append_list:key()]}.
select_elements_from_node(#node{elements = Elements} = Node, Keys, StartFrom) ->
    Selected = maps:with(Keys, Elements),
    RemainingKeys = Keys -- maps:keys(Selected),
    {maps:to_list(Selected), filter_keys(StartFrom, Node, RemainingKeys)}.


%% @private
-spec filter_keys(direction(), #node{}, [append_list:key()]) -> [append_list:key()].
filter_keys(back_from_newest, #node{max_on_right = Max}, Keys) ->
    [Key || Key <- Keys, Key =< Max];
filter_keys(forward_from_oldest, #node{min_on_left = Min}, Keys) ->
    [Key || Key <- Keys, Key >= Min].


%% @private
-spec select_neighbour(direction(), #node{}) -> append_list:id().
select_neighbour(back_from_newest, #node{prev = Prev}) -> Prev;
select_neighbour(forward_from_oldest, #node{next = Next}) -> Next.
