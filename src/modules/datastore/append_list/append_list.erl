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
%%% node storing up to MaxElementsPerNode elements (i.e key value pairs).
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
%%% Adding new elements is only allowed to the beginning of the list. 
%%% New elements provided in batch must be sorted ascending by key and keys must be unique.
%%% It is highly recommended that each new element have key greater 
%%% than those already existing (if not this structure might be inefficient).
%%% Adding elements with the existing keys will result in overwriting of 
%%% existing elements.
%%%
%%% Deletion of arbitrary elements is allowed, although it is recommended 
%%% to delete elements from the list's end. 
%%% During elements deletion if two adjacent nodes have less than 
%%% MaxElementsPerNode elements combined, one of those nodes (the newer one) 
%%% will be deleted, and all elements will be now stored in the other node.
%%% Nodes merging is omitted if deletion finished in the last node of the 
%%% resulting structure after deletions, as it would require additional fetch 
%%% of next node.
%%%
%%% Each node stores also value `min_on_left`. It represents minimal key 
%%% in all nodes, that are newer (are pointed by `next`) than this node. 
%%% It is used during deletion  - it allows to determine whether it is 
%%% necessary to fetch next nodes and allows to finish deletion without 
%%% traversing all list nodes. This is why it is optimal to have 
%%% increasing keys.
%%%
%%% In each node there is also value `max_on_right`. It works similarly to 
%%% `min_on_left` but it represents maximum key in all nodes, that are 
%%% older (are pointed by `prev`) than this node. 
%%% It is used to optimize functions finding elements (`get/2`, `get_highest/1`) 
%%% and also when overwriting existing elements during addition.
%%% @end
%%%-------------------------------------------------------------------
-module(append_list).
-author("Michal Stanisz").

-include("modules/datastore/append_list.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    create/1, 
    destroy/1
]).

-export([
    insert_elements/2, 
    remove_elements/2, 
    fold_elements/2, fold_elements/3, fold_elements/4, 
    get_elements/2, get_elements/3, 
    get_highest/1, get_max_key/1
]).

-compile({no_auto_import, [get/1]}).

% id that allows for finding entities (nodes, sentinel) in persistence.
-type id() :: binary().
-type node_id() :: binary(). % fixme + opis powyżej
% this type represents keys of elements which are stored as data in this structure.
-type key() :: integer().
% this type represents values of elements which are stored as data in this structure.
-type value() :: binary().
% representation of one element in structure (key-value pair)
-type element() :: {key(), value()}.
% Each new node have number exactly 1 higher than previous one. 
% Because of that number of next node is always higher that number of prev node.
% fixme
-type node_number() :: non_neg_integer().
-type elements_map() :: #{append_list:key() => append_list:value()}.


-export_type([id/0, key/0, value/0, element/0, node_number/0, elements_map/0]).

%%=====================================================================
%% API
%%=====================================================================

-spec create(pos_integer()) -> {ok, id()}.
create(MaxElementsPerNode) ->
    Id = datastore_key:new(),
    Sentinel = #sentinel{structure_id = Id, max_elements_per_node = MaxElementsPerNode},
    append_list_persistence:save_node(Id, Sentinel),
    {ok, Id}.


-spec destroy(id()) -> ok | {error, term()}.
destroy(StructId) ->
    case append_list_persistence:get_node(StructId) of
        ?ERROR_NOT_FOUND -> ok;
        Sentinel ->
            delete_all_nodes(Sentinel#sentinel.first),
            true = append_list_persistence:delete_node(Sentinel#sentinel.structure_id),
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Adds a Batch of elements to the beginning of an append list instance.
%% Elements in Batch must be sorted ascending by key and keys must be unique.
%% Returns keys of elements that were overwritten (in reversed order).
%% @end
%%--------------------------------------------------------------------
-spec insert_elements(id(), [element()] | element()) -> ok | {error, term()}.
insert_elements(_StructureId, []) ->
    ok;
insert_elements(StructureId, Batch) ->
    case fetch_or_create_first_node(StructureId) of
        {ok, Sentinel, FirstNode} ->
            append_list_add:insert_elements(Sentinel, FirstNode, utils:ensure_list(Batch));
        {error, _} = Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes given elements from the structure. 
%% Elements that were not found are ignored.
%% Returns ?ERROR_NOT_FOUND when there is no such structure. 
%% @end
%%--------------------------------------------------------------------
-spec remove_elements(id(), key() | [key()]) -> ok | {error, term()}.
remove_elements(StructureId, Elements) ->
    case fetch_last_node(StructureId) of
        {ok, Sentinel, LastNode} ->
            append_list_delete:delete_elements(Sentinel, LastNode, utils:ensure_list(Elements));
        undefined -> ok;
        {error, _} = Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Lists elements in structure. When elements where added as recommended (i.e with increasing keys,  % fixme lists
%% consult module doc) elements are listed in the following order: 
%%  * starting from beginning (StartFrom = first) -> returns elements reversed to adding order (descending) % fixme Startfrom
%%  * starting from end (StartFrom = last) -> return elements in the same order as they were added (ascending) 
%% When elements are not added in recommended order there is no guarantee about listing order.
%% @end
%%--------------------------------------------------------------------
-spec fold_elements(id() | append_list_get:state(), append_list_get:batch_size()) ->
    append_list_get:fold_result() | {error, term()}.
fold_elements(IdOrListingState, Size) ->
    fold_elements(IdOrListingState, Size, back_from_newest).

-spec fold_elements(id() | append_list_get:state(), append_list_get:batch_size(), 
    append_list_get:direction() | append_list_get:fold_fun()) ->
    append_list_get:fold_result() | {error, term()}.
fold_elements(IdOrListingState, Size, Direction) when is_atom(Direction) ->
    append_list_get:fold(IdOrListingState, Size, Direction, fun(Elem) -> {ok, Elem} end);
fold_elements(IdOrListingState, Size, FoldFun) when is_function(FoldFun, 1)->
    append_list_get:fold(IdOrListingState, Size, back_from_newest, FoldFun).

-spec fold_elements(id(), append_list_get:batch_size(), append_list_get:direction(), append_list_get:fold_fun()) ->
    append_list_get:fold_result() | {error, term()}.
fold_elements(Id, Size, Direction, FoldFun) when is_binary(Id) ->
    append_list_get:fold(Id, Size, Direction, FoldFun).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves elements value from the structure. When elements where added as recommended (i.e with increasing keys, 
%% consult module doc) elements are returned in the following order: 
%%  * starting from beginning (StartFrom = first) -> returns elements reversed to adding order (descending)
%%  * starting from end (StartFrom = last) -> return elements in the same order as they were added (ascending) % fixme direction
%% If elements were not added in recommended order there is no guarantee about returned elements order.
%% @end
%%--------------------------------------------------------------------
-spec get_elements(id(), key() | [key()]) -> [element()] | {error, term()}.
get_elements(StructureId, Keys) ->
    get_elements(StructureId, Keys, back_from_newest).


-spec get_elements(id(), key() | [key()], append_list_get:direction()) -> 
    [element()] | {error, term()}.
get_elements(StructureId, Key, StartFrom) when not is_list(Key) ->
    get_elements(StructureId, [Key], StartFrom);
get_elements(StructureId, Keys, StartFrom) ->
    case append_list_persistence:get_node(StructureId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        Sentinel ->
            StartingNodeId = append_list_utils:get_starting_node_id(StartFrom, Sentinel), 
            append_list_get:get_elements(StartingNodeId, Keys, StartFrom)
    end.


-spec get_highest(id()) -> element() | undefined.
get_highest(StructureId) ->
    case append_list_persistence:get_node(StructureId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        Sentinel -> append_list_get:get_highest(Sentinel#sentinel.first)
    end.


-spec get_max_key(id()) -> key() | undefined.
get_max_key(StructureId) ->
    case append_list_persistence:get_node(StructureId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        Sentinel -> append_list_get:get_max_key(Sentinel#sentinel.first)
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec delete_all_nodes(id() | undefined) -> ok.
delete_all_nodes(undefined) ->
    ok;
delete_all_nodes(NodeId) ->
    #node{prev = Prev} = append_list_persistence:get_node(NodeId),
    append_list_persistence:delete_node(NodeId),
    delete_all_nodes(Prev).


%% @private
-spec fetch_or_create_first_node(id()) -> {ok, #sentinel{}, #node{}} | {error, term()}.
fetch_or_create_first_node(StructureId) ->
    case append_list_persistence:get_node(StructureId) of
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND;
        #sentinel{first = undefined} = Sentinel ->
            NodeId = datastore_key:new(),
            Sentinel1 = Sentinel#sentinel{first = NodeId, last = NodeId},
            append_list_persistence:save_node(StructureId, Sentinel1),
            Node = #node{node_id = NodeId, structure_id = StructureId, node_number = 0},
            {ok, Sentinel1, Node};
        #sentinel{first = FirstNodeId} = Sentinel ->
            {ok, Sentinel, append_list_persistence:get_node(FirstNodeId)}
    end.


%% @private
-spec fetch_last_node(id()) -> {ok, #sentinel{}, #node{}} | undefined | {error, term()} .
fetch_last_node(StructureId) ->
    case append_list_persistence:get_node(StructureId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        #sentinel{last = undefined} -> undefined;
        #sentinel{last = Last} = Sentinel -> {ok, Sentinel, append_list_persistence:get_node(Last)}
    end.
