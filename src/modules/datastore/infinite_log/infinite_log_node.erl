%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API related to the infinite_log_node datastore model.
%%% @TODO VFS-7411 This module will be reworked during integration with datastore
%%% @end
%%%-------------------------------------------------------------------
-module(infinite_log_node).
-author("Lukasz Opiola").

-include("modules/datastore/infinite_log.hrl").

%% Model API
-export([get/2, save/3, delete/2, set_ttl/3]).
%% Convenience functions
-export([append_entry/2]).
-export([get_node_entries_length/2]).
-export([latest_node_number/1]).
-export([entry_index_to_node_number/2]).

% id of individual node in an infinite log as stored in database
-type id() :: binary().
% nodes are numbered from 0 (oldest entries), and the newest node is always
% stored inside the sentinel
-type node_number() :: non_neg_integer().
-type record() :: #node{}.
-export_type([id/0, node_number/0, record/0]).


%%=====================================================================
%% Model API
%%=====================================================================

-spec get(infinite_log:log_id(), node_number()) -> {ok, term()} | {error, term()}.
get(LogId, NodeNumber) ->
    NodeId = build_node_id(LogId, NodeNumber),
    case node_cache:get({?MODULE, NodeId}, undefined) of
        undefined -> {error, not_found};
        Record -> {ok, Record}
    end.


-spec save(infinite_log:log_id(), node_number(), term()) -> ok | {error, term()}.
save(LogId, NodeNumber, Value) ->
    NodeId = build_node_id(LogId, NodeNumber),
    %% @TODO VFS-7411 trick dialyzer into thinking that errors can be returned
    %% they actually will after integration with datastore
    case NodeId of
        <<"123">> -> {error, etmpfail};
        _ -> node_cache:put({?MODULE, NodeId}, Value)
    end.


-spec delete(infinite_log:log_id(), node_number()) -> ok | {error, term()}.
delete(LogId, NodeNumber) ->
    NodeId = build_node_id(LogId, NodeNumber),
    %% @TODO VFS-7411 trick dialyzer into thinking that errors can be returned
    %% they actually will after integration with datastore
    case NodeId of
        <<"123">> -> {error, etmpfail};
        %% @TODO VFS-7411 should return ok if the document is not found
        _ -> node_cache:clear({?MODULE, NodeId})
    end.


-spec set_ttl(infinite_log:log_id(), node_number(), time:seconds()) -> ok | {error, term()}.
set_ttl(LogId, NodeNumber, Ttl) ->
    NodeId = build_node_id(LogId, NodeNumber),
    %% @TODO VFS-7411 trick dialyzer into thinking that errors can be returned
    %% they actually will after integration with datastore
    case NodeId of
        <<"123">> ->
            {error, etmpfail};
        _ ->
            {ok, Record} = get(LogId, NodeNumber),
            node_cache:put({?MODULE, NodeId}, Record, Ttl)
    end.

%%=====================================================================
%% Convenience functions
%%=====================================================================

-spec append_entry(record(), infinite_log:entry()) -> record().
append_entry(Node = #node{entries = Entries, oldest_timestamp = OldestTimestamp}, Entry = {Timestamp, _}) ->
    Node#node{
        entries = [Entry | Entries],
        newest_timestamp = Timestamp,
        oldest_timestamp = case Entries of
            [] -> Timestamp;
            _ -> OldestTimestamp
        end
    }.


%% Entries length can be easily calculated to avoid calling the length/1 function.
-spec get_node_entries_length(infinite_log_sentinel:record(), node_number()) ->
    node_number().
get_node_entries_length(Sentinel = #sentinel{max_entries_per_node = MaxEntriesPerNode}, NodeNumber) ->
    case latest_node_number(Sentinel) of
        NodeNumber ->
            Sentinel#sentinel.entry_count - (NodeNumber * MaxEntriesPerNode);
        _ ->
            MaxEntriesPerNode
    end.


-spec latest_node_number(infinite_log_sentinel:record()) ->
    node_number().
latest_node_number(Sentinel = #sentinel{entry_count = EntryCount}) ->
    entry_index_to_node_number(Sentinel, EntryCount - 1).


-spec entry_index_to_node_number(infinite_log_sentinel:record(), infinite_log:entry_index()) ->
    node_number().
entry_index_to_node_number(#sentinel{max_entries_per_node = MaxEntriesPerNode}, EntryIndex) ->
    EntryIndex div MaxEntriesPerNode.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec build_node_id(infinite_log:log_id(), node_number()) -> id().
build_node_id(LogId, NodeNumber) ->
    datastore_key:build_adjacent(integer_to_binary(NodeNumber), LogId).
