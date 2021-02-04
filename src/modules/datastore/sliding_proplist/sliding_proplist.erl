%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements a datastore internal structure that allows elements to be 
%%% stored as {key, value} - similar to a proplist. It has been optimized for inserting 
%%% elements with increasing keys and removing the oldest ones. Therefore, when used 
%%% optimally, it looks like a sliding window as elements are inserted on one side and 
%%% removed on the other.
%%% 
%%% Because there is no limit to the amount of stored elements, to allow for saving 
%%% this structure in datastore, it is implemented as a bidirectional linked list, 
%%% with each node storing up to MaxElementsPerNode elements (i.e key value pairs).
%%% MaxElementsPerNode value is provided during structure creation.
%%% 
%%%                    +--------+             +--------+             
%%%          prev      |        |    prev     |        |    prev      
%%%        +------->   |  node  |  +------->  |  node  |  +------->  
%%%  ...               |        |             |        |              ...  
%%%        <-------+   |        |  <-------+  |        |  <-------+  
%%%          next      +--------+    next     +--------+    next     
%%% 
%%% This structure stores elements in arbitrary order, i.e not necessarily sorted.
%%% Inserting new elements is only allowed to the beginning of the list. New elements 
%%% provided in batch must be sorted ascending by key and keys must be unique.
%%% This requirement is due to optimizations when creating new nodes.
%%% It is highly recommended that each new element have key greater 
%%% than those already existing (if not this structure might be inefficient).
%%% Adding elements with the existing keys will result in overwriting of 
%%% existing elements.
%%%
%%% Deletion of arbitrary elements is allowed, although it is recommended 
%%% to delete elements from the list's end. 
%%% During elements deletion if two adjacent nodes have less than half
%%% MaxElementsPerNode elements combined, one of those nodes (the newer one) 
%%% will be deleted, and all elements will be now stored in the other node.
%%% Nodes merging is omitted if deletion finished in the last node of the 
%%% resulting structure after deletions, as it would require additional fetch 
%%% of next node.
%%%
%%% Each node stores also value `min_in_newer_nodes`. It represents minimal key 
%%% in all nodes, that are newer (are pointed by `next`) than this node. 
%%% It is used during deletion  - it allows to determine whether it is 
%%% necessary to fetch next nodes and allows to finish deletion without 
%%% traversing all list nodes. This is why it is optimal to have 
%%% increasing keys.
%%%
%%% In each node there is also value `max_in_older_nodes`. It works similarly to 
%%% `min_in_newer_nodes` but it represents maximum key in all nodes, that are 
%%% older (are pointed by `prev`) than this node. 
%%% It is used to optimize functions finding elements (`get_elements/2`, `get_highest_element/1`) 
%%% and also when overwriting existing elements during addition.
%%% @end
%%%-------------------------------------------------------------------
-module(sliding_proplist).
-author("Michal Stanisz").

-include("modules/datastore/sliding_proplist.hrl").


%% API
-export([
    create/1,
    destroy/1
]).

-export([
    insert_uniquely_sorted_elements/2, 
    remove_elements/2,
    list_elements/2, list_elements/3,
    fold_elements/4, fold_elements/5, 
    get_elements/2, get_elements/3, 
    get_highest_element/1, get_max_key/1
]).

-compile({no_auto_import, [get/1]}).

% id of a sliding proplist instance.
-type id() :: binary().
% id of individual node in a sliding proplist
-type node_id() :: binary().
% this type represents keys of elements which are stored as data.
-type key() :: integer().
% this type represents values of elements which are stored as data.
-type value() :: binary().
% representation of one element in sliding proplist (key-value pair)
-type element() :: {key(), value()}.
% Each new node have number exactly 1 higher than previous first one. 
% Because of that number of next node is always higher that number of prev node.
-type node_number() :: non_neg_integer().
-type elements_map() :: #{sliding_proplist:key() => sliding_proplist:value()}.

-type sentinel() :: #sentinel{}.
-type list_node() :: #node{}.

-define(LIST_FOLD_FUN, fun(Elem, Acc) -> {ok, [Elem | Acc]} end).

-export_type([id/0, node_id/0, key/0, value/0, element/0, node_number/0, elements_map/0]).
-export_type([sentinel/0, list_node/0]).

%%=====================================================================
%% API
%%=====================================================================

-spec create(pos_integer()) -> {ok, id()}.
create(MaxElementsPerNode) ->
    Id = datastore_key:new(),
    Sentinel = #sentinel{structure_id = Id, max_elements_per_node = MaxElementsPerNode},
    sliding_proplist_persistence:save_record(Id, Sentinel),
    {ok, Id}.


-spec destroy(id()) -> ok | {error, term()}.
destroy(StructId) ->
    case sliding_proplist_persistence:get_record(StructId) of
        {ok, Sentinel} ->
            delete_all_nodes(Sentinel#sentinel.first),
            true = sliding_proplist_persistence:delete_record(Sentinel#sentinel.structure_id),
            ok;
        {error, not_found} -> ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Adds a Elements to the beginning of a sliding proplist instance.
%% Returns unordered list of keys that were overwritten.
%% @end
%%--------------------------------------------------------------------
-spec insert_uniquely_sorted_elements(id(), [element()] | element()) -> 
    {ok, OverwrittenKeys :: [key()]} | {error, term()}.
insert_uniquely_sorted_elements(StructureId, Elements) ->
    case sliding_proplist_persistence:get_record(StructureId) of
        {ok, #sentinel{first = FirstNodeId} = Sentinel} ->
            {ok, UpdatedSentinel, FirstNode} = fetch_or_create_first_node(Sentinel, FirstNodeId),
            sliding_proplist_add:insert_elements(
                UpdatedSentinel, FirstNode, utils:ensure_list(Elements));
        {error, _} = Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes given elements from the sliding proplist instance. 
%% Elements that were not found are ignored.
%% Returns list of keys that were not found.
%% Returns {error, not_found} when there is no such instance.
%% @end
%%--------------------------------------------------------------------
-spec remove_elements(id(), key() | [key()]) -> {ok, [key()]} | {error, term()}.
remove_elements(StructureId, Elements) ->
    case sliding_proplist_persistence:get_record(StructureId) of
        {ok, #sentinel{last = undefined}} -> ok;
        {ok, #sentinel{last = Last} = Sentinel} ->
            {ok, LastNode} = sliding_proplist_persistence:get_record(Last),
            sliding_proplist_delete:delete_elements(
                Sentinel, LastNode, utils:ensure_list(Elements));
        {error, _} = Error -> Error
    end.


-spec list_elements(id() | sliding_proplist_get:state(), sliding_proplist_get:batch_size()) ->
    sliding_proplist_get:fold_result([element()]) | {error, term()}.
list_elements(Id, Size) when is_binary(Id) ->
    list_elements(Id, Size, back_from_newest);
list_elements(State, Size) ->
    fold_elements(State, Size, ?LIST_FOLD_FUN, []).


%%--------------------------------------------------------------------
%% @doc
%% List elements in sliding proplist instance. When elements where added as recommended 
%% (i.e with increasing keys, consult module doc) elements are listed in the following order: 
%%  * starting from beginning (Direction = back_from_newest) -> 
%%      returns elements in the same order as they were added (ascending)
%%  * starting from end (Direction = forward_from_oldest) -> 
%%      returns elements reversed to adding order (descending)  
%% When elements are not added in recommended order there is no guarantee about listing order.
%% @end
%%--------------------------------------------------------------------
-spec list_elements(id(), sliding_proplist_get:batch_size(), sliding_proplist_get:direction()) ->
    sliding_proplist_get:fold_result([element()]) | {error, term()}.
list_elements(Id, Size, Direction) when is_binary(Id) and is_atom(Direction) ->
    fold_elements(Id, Size, Direction, ?LIST_FOLD_FUN, []).



-spec fold_elements(
    id() | sliding_proplist_get:state(), 
    sliding_proplist_get:batch_size(), 
    sliding_proplist_get:fold_fun(), 
    term()
) ->
    sliding_proplist_get:fold_result() | {error, term()}.
fold_elements(Id, Size, FoldFun, Acc0) when is_binary(Id) and is_function(FoldFun, 2) ->
    fold_elements(Id, Size, back_from_newest, FoldFun, Acc0);
fold_elements(State, Size, FoldFun, Acc0) when is_function(FoldFun, 2) ->
    sliding_proplist_get:fold(State, Size, FoldFun, Acc0).

-spec fold_elements(id(), sliding_proplist_get:batch_size(), sliding_proplist_get:direction(), 
    sliding_proplist_get:fold_fun(), term()) -> sliding_proplist_get:fold_result() | {error, term()}.
fold_elements(Id, Size, Direction, FoldFun, Acc0) when is_binary(Id) ->
    case sliding_proplist_persistence:get_record(Id) of
        {ok, #sentinel{} = Sentinel} ->
            StartingNodeId = sliding_proplist_utils:get_starting_node_id(Direction, Sentinel),
            sliding_proplist_get:fold(Id, StartingNodeId, Size, Direction, FoldFun, Acc0);
        {error, _} = Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves elements value from the sliding proplist instance. 
%% There is no guarantee about returned elements order.
%% @end
%%--------------------------------------------------------------------
-spec get_elements(id(), key() | [key()]) -> {ok, [element()]} | {error, term()}.
get_elements(StructureId, Keys) ->
    get_elements(StructureId, Keys, back_from_newest).


-spec get_elements(id(), key() | [key()], sliding_proplist_get:direction()) -> 
    {ok, [element()]} | {error, term()}.
get_elements(StructureId, Key, Direction) when not is_list(Key) ->
    get_elements(StructureId, [Key], Direction);
get_elements(StructureId, Keys, Direction) ->
    case sliding_proplist_persistence:get_record(StructureId) of
        {ok, Sentinel} ->
            StartingNodeId = sliding_proplist_utils:get_starting_node_id(Direction, Sentinel), 
            {ok, sliding_proplist_get:get_elements(StartingNodeId, Keys, Direction)};
        {error, _} = Error -> Error
    end.


-spec get_highest_element(id()) -> {ok, element()} | {error, term()}.
get_highest_element(StructureId) ->
    case sliding_proplist_persistence:get_record(StructureId) of
        {ok, Sentinel} -> sliding_proplist_get:get_highest_element(Sentinel#sentinel.first);
        {error, _} = Error -> Error
    end.


-spec get_max_key(id()) -> {ok, key()} | {error, term()}.
get_max_key(StructureId) ->
    case sliding_proplist_persistence:get_record(StructureId) of
        {ok, Sentinel} -> sliding_proplist_get:get_max_key(Sentinel#sentinel.first);
        {error, _} = Error -> Error
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec delete_all_nodes(node_id() | undefined) -> ok.
delete_all_nodes(undefined) ->
    ok;
delete_all_nodes(NodeId) ->
    {ok, #node{prev = Prev}} = sliding_proplist_persistence:get_record(NodeId),
    sliding_proplist_persistence:delete_record(NodeId),
    delete_all_nodes(Prev).


%% @private
-spec fetch_or_create_first_node(sentinel(), node_id() | undefined) -> 
    {ok, sentinel(), list_node()} | {error, term()}.
fetch_or_create_first_node(#sentinel{structure_id = StructureId} = Sentinel, undefined) ->
    NodeId = datastore_key:new(),
    UpdatedSentinel = Sentinel#sentinel{first = NodeId, last = NodeId},
    sliding_proplist_persistence:save_record(StructureId, UpdatedSentinel),
    FirstNode = #node{node_id = NodeId, structure_id = StructureId, node_number = 0},
    {ok, UpdatedSentinel, FirstNode};
fetch_or_create_first_node(Sentinel, FirstNodeId) ->
    {ok, FirstNode} = sliding_proplist_persistence:get_record(FirstNodeId),
    {ok, Sentinel, FirstNode}.
