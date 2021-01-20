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
%%% New elements provided in batch must be sorted ascending by key and keys must be unique.
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
%%% Nodes merging is omitted if deletion finished in the last node of the 
%%% resulting structure after deletions, as it would require additional fetch 
%%% of next node.
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

-include("modules/datastore/datastore_append_list.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([create_structure/1, delete_structure/1]).
-export([add/2, delete/2, list/2, get/2, get/3, get_highest/1, get_max_key/1]).

-compile({no_auto_import, [get/1]}).

-export_type([id/0, key/0, value/0, elem/0]).

%%=====================================================================
%% API
%%=====================================================================

-spec create_structure(pos_integer()) -> {ok, id()}.
create_structure(MaxElemsPerNode) ->
    Id = datastore_key:new(),
    Sentinel = #sentinel{structure_id = Id, max_elems_per_node = MaxElemsPerNode},
    append_list_persistence:save_node(Id, Sentinel),
    {ok, Id}.


-spec delete_structure(id()) -> ok | ?ERROR_NOT_FOUND.
delete_structure(StructId) ->
    case append_list_persistence:get_node(StructId) of
        ?ERROR_NOT_FOUND -> ok;
        Sentinel ->
            delete_all_nodes(Sentinel#sentinel.first),
            true = append_list_persistence:delete_node(Sentinel#sentinel.structure_id),
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Adds elements given in Batch to the beginning of a structure. 
%% Elements in Batch must be sorted ascending by Key and Keys must be unique.
%% @end
%%--------------------------------------------------------------------
-spec add(id(), [elem()]) -> ok | ?ERROR_NOT_FOUND.
add(_SentinelId, []) ->
    ok;
add(SentinelId, Elem) when not is_list(Elem) ->
    add(SentinelId, [Elem]);
add(SentinelId, Batch) ->
    case fetch_or_create_first_node(SentinelId) of
        {error, _} = Error -> Error;
        {Sentinel, FirstNode} ->
            append_list_add:add(Sentinel, FirstNode, Batch)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes given elements from structure. 
%% Elements that were not found are ignored.
%% Returns ?ERROR_NOT_FOUND when there is no such structure. 
%% @end
%%--------------------------------------------------------------------
-spec delete(id(), [key()]) -> ok | ?ERROR_NOT_FOUND.
delete(SentinelId, Elems) ->
    case fetch_last_node(SentinelId) of
        {error, _} = Error -> Error;
        undefined -> ok;
        {Sentinel, LastNode} ->
            append_list_delete:delete_elems(Sentinel, LastNode, Elems)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Lists elements from the beginning (last added). % fixme
%% @end
%%--------------------------------------------------------------------
- spec list(id() | #listing_info{}, non_neg_integer()) -> {[append_list:elem()], #listing_info{}}.
list(_, 0) ->
    {[], #listing_info{finished = true}};
list(#listing_info{finished = true}, _Size) ->
    {[], #listing_info{finished = true}};
list(#listing_info{internal_listing_data = ListingData}, Size) ->
    append_list_get:list(ListingData, Size, []);
list(SentinelId, Size) ->
    case append_list_persistence:get_node(SentinelId) of
        ?ERROR_NOT_FOUND -> list(SentinelId, 0);
        #sentinel{first = FirstNodeId} ->
            append_list_get:list(#internal_listing_data{
                structure_id = SentinelId,
                last_node_id = FirstNodeId
            }, Size, [])
    end.


% fixme doc elements returned in arbitrary order
-spec get(id(), key() | [key()]) -> ?ERROR_NOT_FOUND | [elem()].
get(SentinelId, Keys) ->
    get(SentinelId, Keys, first).


-spec get(id(), key() | [key()], first | last) -> ?ERROR_NOT_FOUND | [elem()].
get(SentinelId, Key, StartFrom) when not is_list(Key) ->
    get(SentinelId, [Key], StartFrom);
get(SentinelId, Keys, StartFrom) ->
    case append_list_persistence:get_node(SentinelId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        Sentinel -> 
            StartingNodeId = case StartFrom of
                first -> Sentinel#sentinel.first;
                last -> Sentinel#sentinel.last
            end, append_list_get:get(StartingNodeId, Keys, StartFrom)
    end.


-spec get_highest(id()) -> elem() | undefined.
get_highest(SentinelId) ->
    case append_list_persistence:get_node(SentinelId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        Sentinel -> append_list_get:get_highest(Sentinel#sentinel.first)
    end.


-spec get_max_key(id()) -> key() | undefined.
get_max_key(SentinelId) ->
    case append_list_persistence:get_node(SentinelId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        Sentinel -> append_list_get:get_max_key(Sentinel#sentinel.first)
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

-spec delete_all_nodes(#sentinel{}) -> ok.
delete_all_nodes(undefined) ->
    ok;
delete_all_nodes(NodeId) ->
    #node{prev = Prev} = append_list_persistence:get_node(NodeId),
    append_list_persistence:delete_node(NodeId),
    delete_all_nodes(Prev).


-spec fetch_or_create_first_node(id()) -> {#sentinel{}, #node{}} | ?ERROR_NOT_FOUND.
fetch_or_create_first_node(SentinelId) ->
    case append_list_persistence:get_node(SentinelId) of
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND;
        #sentinel{first = undefined} = Sentinel ->
            NodeId = datastore_key:new(),
            Sentinel1 = Sentinel#sentinel{first = NodeId, last = NodeId},
            append_list_persistence:save_node(SentinelId, Sentinel1),
            Node = #node{node_id = NodeId, sentinel_id = SentinelId, node_num = 0},
            {Sentinel1, Node};
        #sentinel{first = FirstNodeId} = Sentinel ->
            {Sentinel, append_list_persistence:get_node(FirstNodeId)}
    end.


-spec fetch_last_node(id()) -> {#sentinel{}, #node{}} | ?ERROR_NOT_FOUND.
fetch_last_node(SentinelId) ->
    case append_list_persistence:get_node(SentinelId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        #sentinel{last = undefined} = Sentinel-> undefined;
        #sentinel{last = Last} = Sentinel -> {Sentinel, append_list_persistence:get_node(Last)}
    end.
