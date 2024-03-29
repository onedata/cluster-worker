%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API related to the infinite_log_node datastore model.
%%% @end
%%%-------------------------------------------------------------------
-module(infinite_log_node).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/infinite_log.hrl").

%% Model API
-export([get/4, save/5, delete/4, adjust_expiry_threshold/6, save_with_expiry_threshold_adjustment/7]).
%% Convenience functions
-export([append_entry/2]).
-export([get_node_entries_length/2]).
-export([newest_node_number/1]).
-export([oldest_node_number/1]).
-export([entry_index_to_node_number/2]).

% id of individual node in an infinite log as stored in database
-type id() :: binary().
% nodes are numbered from 0 (oldest entries), and the newest node is always
% stored inside the sentinel
-type node_number() :: non_neg_integer().
-type record() :: #infinite_log_node{}.
-export_type([id/0, node_number/0, record/0]).

%% Datastore API
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-define(CTX, #{
    model => ?MODULE
}).


%%=====================================================================
%% Model API
%%=====================================================================

-spec get(infinite_log:ctx(), infinite_log:log_id(), node_number(), infinite_log:batch()) ->
    {{ok, record()} | {error, term()}, infinite_log:batch()}.
get(Ctx, LogId, NodeNumber, Batch) ->
    NodeId = build_node_id(LogId, NodeNumber),
    case datastore_doc:fetch(Ctx, NodeId, Batch) of
        {{ok, #document{value = Record}}, UpdatedBatch} ->
            {{ok, Record}, UpdatedBatch};
        {{error, _}, _} = Error ->
            Error
    end.


-spec save(infinite_log:ctx(), infinite_log:log_id(), node_number(), record(), infinite_log:batch()) ->
    {ok | {error, term()}, infinite_log:batch()}.
save(Ctx, LogId, NodeNumber, Record, Batch) ->
    NodeId = build_node_id(LogId, NodeNumber),
    case datastore_doc:save(Ctx, NodeId, #document{key = NodeId, value = Record}, Batch) of
        {{ok, _}, UpdatedBatch} -> {ok, UpdatedBatch};
        {{error, _}, _} = Error -> Error
    end.


-spec delete(infinite_log:ctx(), infinite_log:log_id(), node_number(), infinite_log:batch()) ->
    {ok | {error, term()}, infinite_log:batch()}.
delete(Ctx, LogId, NodeNumber, Batch) ->
    NodeId = build_node_id(LogId, NodeNumber),
    datastore_doc:delete(Ctx, NodeId, Batch).


-spec adjust_expiry_threshold(
    infinite_log:ctx(),
    infinite_log:log_id(),
    node_number(),
    time:seconds(),
    infinite_log:timestamp_millis(),
    infinite_log:batch()
) ->
    {ok | {error, term()}, infinite_log:batch()}.
adjust_expiry_threshold(Ctx, LogId, NodeNumber, ThresholdSeconds, NowMillis, Batch) ->
    {{ok, Record}, UpdatedBatch} = get(Ctx, LogId, NodeNumber, Batch),
    save_with_expiry_threshold_adjustment(Ctx, LogId, NodeNumber, Record, ThresholdSeconds, NowMillis, UpdatedBatch).


-spec save_with_expiry_threshold_adjustment(
    infinite_log:ctx(),
    infinite_log:log_id(),
    node_number(),
    record(),
    time:seconds(),
    infinite_log:timestamp_millis(),
    infinite_log:batch()
) ->
    {ok | {error, term()}, infinite_log:batch()}.
save_with_expiry_threshold_adjustment(Ctx, LogId, NodeNumber, Record, ThresholdSeconds, NowMillis, Batch) ->
    AdjustedThresholdSeconds = max(0, ThresholdSeconds - (NowMillis - Record#infinite_log_node.newest_timestamp) div 1000),
    Ctx1 = datastore_doc:set_expiry_in_ctx(Ctx, AdjustedThresholdSeconds),
    save(Ctx1, LogId, NodeNumber, Record, Batch).

%%=====================================================================
%% Convenience functions
%%=====================================================================

-spec append_entry(record(), infinite_log:entry()) -> record().
append_entry(Node = #infinite_log_node{entries = Entries, oldest_timestamp = OldestTimestamp}, Entry = {Timestamp, _}) ->
    Node#infinite_log_node{
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
get_node_entries_length(Sentinel = #infinite_log_sentinel{max_entries_per_node = MaxEntriesPerNode}, NodeNumber) ->
    case newest_node_number(Sentinel) of
        NodeNumber ->
            Sentinel#infinite_log_sentinel.total_entry_count - (NodeNumber * MaxEntriesPerNode);
        _ ->
            MaxEntriesPerNode
    end.


-spec newest_node_number(infinite_log_sentinel:record()) -> node_number().
newest_node_number(Sentinel = #infinite_log_sentinel{total_entry_count = EntryCount}) ->
    entry_index_to_node_number(Sentinel, max(0, EntryCount - 1)).


-spec oldest_node_number(infinite_log_sentinel:record()) -> node_number().
oldest_node_number(Sentinel = #infinite_log_sentinel{oldest_entry_index = PrunedEntryCount}) ->
    entry_index_to_node_number(Sentinel, PrunedEntryCount).


-spec entry_index_to_node_number(infinite_log_sentinel:record(), infinite_log:entry_index()) ->
    node_number().
entry_index_to_node_number(#infinite_log_sentinel{max_entries_per_node = MaxEntriesPerNode}, EntryIndex) ->
    EntryIndex div MaxEntriesPerNode.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec build_node_id(infinite_log:log_id(), node_number()) -> id().
build_node_id(LogId, NodeNumber) ->
    datastore_key:build_adjacent(integer_to_binary(NodeNumber), LogId).


%%%===================================================================
%%% Datastore API
%%%===================================================================

-spec get_ctx() -> datastore_model:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    % NOTE: this model is a part of `infinite_log_sentinel`, which must be
    % upgraded if it changes.
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {entries, [{integer, binary}]},
        {oldest_timestamp, integer},
        {newest_timestamp, integer}
    ]}.
