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
%%% Because of that last node can not be deleted during elements deletion.
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
-export([create/1, delete/1]).
-export([add/2, delete_elems/2, get_many/2, get/2, get_highest/1]).

%%-ifdef(TEST).
-compile(export_all). % fixme
%%-endif.
-compile({no_auto_import, [get/1]}).

-export_type([id/0, key/0, value/0, elem/0]).

%%=====================================================================
%% API
%%=====================================================================

-spec create(pos_integer()) -> {ok, id()}.
create(MaxElemsPerNode) ->
    Id = datastore_key:new(),
    Sentinel = #sentinel{structure_id = Id, max_elems_per_node = MaxElemsPerNode},
    append_list_persistence:save_node(Id, Sentinel),
    {ok, Id}.


-spec delete(id()) -> ok | ?ERROR_NOT_FOUND.
delete(StructId) ->
    case append_list_persistence:get_node(StructId) of
        ?ERROR_NOT_FOUND -> ok;
        Sentinel ->
            delete_all_nodes(Sentinel),
            true = append_list_persistence:delete_node(Sentinel#sentinel.structure_id),
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Adds elements given in Batch to the beginning of a structure. 
%% Elements in structure should be sorted by Key and Keys should be unique.
%% @end
%%--------------------------------------------------------------------
-spec add(id(), [elem()]) -> ok | ?ERROR_NOT_FOUND.
add(_SentinelId, []) ->
    ok;
add(SentinelId, UnsortedBatch) ->
    case fetch_or_create_first_node(SentinelId) of
        {error, _} = Error -> Error;
        {Sentinel, FirstNode} ->
            Batch = lists:ukeysort(1, UnsortedBatch), % fixme maybe remove
            append_list_add:add(Sentinel, FirstNode, Batch)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes given elements from structure. 
%% Elements that were not found are ignored.
%% Returns ?ERROR_NOT_FOUND when there is no such structure. 
%% @end
%%--------------------------------------------------------------------
-spec delete_elems(id(), [key()]) -> ok | ?ERROR_NOT_FOUND.
delete_elems(SentinelId, Elems) ->
    case fetch_last_node(SentinelId) of
        {error, _} = Error -> Error;
        {Sentinel, LastNode} ->
            append_list_delete:delete_elems(Sentinel, LastNode, Elems)
    end.


- spec get_many(id() | #listing_info{}, non_neg_integer()) -> {[append_list:elem()], #listing_info{}}.
get_many(_, 0) ->
    {[], #listing_info{finished = true}};
get_many(#listing_info{finished = true}, _Size) ->
    {[], #listing_info{finished = true}};
get_many(#listing_info{internal_listing_data = ListingData}, Size) ->
    append_list_get:get_many(ListingData, Size, []);
get_many(SentinelId, Size) ->
    case append_list_persistence:get_node(SentinelId) of
        ?ERROR_NOT_FOUND -> get_many(SentinelId, 0);
        #sentinel{first = FirstNodeId} ->
            append_list_get:get_many(#internal_listing_data{
                id = SentinelId,
                last_node_id = FirstNodeId
            }, Size, [])
    end.


-spec get_highest(id()) -> elem() | undefined.
get_highest(SentinelId) ->
    case append_list_persistence:get_node(SentinelId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        Sentinel -> append_list_get:get_highest(Sentinel#sentinel.first)
    end.


-spec get(id(), key()) -> ?ERROR_NOT_FOUND | {ok, value()}.
get(SentinelId, Key) ->
    case append_list_persistence:get_node(SentinelId) of
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        Sentinel -> append_list_get:get(Sentinel#sentinel.first, Key)
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

-spec delete_all_nodes(#sentinel{}) -> ok.
delete_all_nodes(#sentinel{first = undefined}) ->
    ok;
delete_all_nodes(#sentinel{first = NodeId} = Sentinel) ->
    #node{prev = Prev} = append_list_persistence:get_node(NodeId),
    NewSentinel = Sentinel#sentinel{first = Prev},
    append_list_persistence:save_node(Sentinel#sentinel.structure_id, NewSentinel),
    append_list_persistence:delete_node(NodeId),
    delete_all_nodes(NewSentinel).


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
        #sentinel{last = undefined} -> ?ERROR_NOT_FOUND;
        #sentinel{last = Last} = Sentinel -> {Sentinel, append_list_persistence:get_node(Last)}
    end.
