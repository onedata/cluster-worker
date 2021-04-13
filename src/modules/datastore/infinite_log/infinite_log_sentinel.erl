%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API related to the infinite_log_sentinel datastore model.
%%% @TODO VFS-7411 This module will be reworked during integration with datastore
%%% @end
%%%-------------------------------------------------------------------
-module(infinite_log_sentinel).
-author("Lukasz Opiola").

-include("modules/datastore/infinite_log.hrl").
-include_lib("ctool/include/logging.hrl").

%% Model API
-export([acquire/3, save/2, delete/1, set_ttl/2]).
%% Convenience functions
-export([append/2]).
-export([get_node_by_number/2]).

-type record() :: #sentinel{}.
-export_type([record/0]).

%%=====================================================================
%% API
%%=====================================================================

-spec acquire(infinite_log:log_id(), skip_pruning | apply_pruning, infinite_log:access_mode()) ->
    {ok, term()} | {error, term()}.
acquire(LogId, skip_pruning, _) ->
    case node_cache:get({?MODULE, LogId}, undefined) of
        undefined -> {error, not_found};
        Record -> {ok, Record}
    end;
acquire(LogId, apply_pruning, AccessMode) ->
    case acquire(LogId, skip_pruning, readonly) of
        {error, _} = GetError ->
            GetError;
        {ok, Sentinel} ->
            case apply_age_pruning(Sentinel, AccessMode) of
                {error, _} = PruningError ->
                    PruningError;
                {ok, Sentinel} ->
                    {ok, Sentinel};
                {ok, UpdatedSentinel} ->
                    case save(LogId, UpdatedSentinel) of
                        ok ->
                            {ok, UpdatedSentinel};
                        {error, _} = SaveError ->
                            SaveError
                    end
            end
    end.


-spec save(infinite_log:log_id(), term()) -> ok | {error, term()}.
save(LogId, Record) ->
    %% @TODO VFS-7411 trick dialyzer into thinking that errors can be returned
    %% they actually will after integration with datastore
    case LogId of
        <<"123">> ->
            {error, etmpfail};
        _ ->
            %% @TODO VFS-7411 adjust to CB's way of handling expiration
            Ttl = case Record#sentinel.expiration_time of
                undefined -> infinity;
                Timestamp -> Timestamp - current_timestamp(Record) div 1000
            end,
            node_cache:put({?MODULE, LogId}, Record, Ttl)
    end.


-spec delete(infinite_log:log_id()) -> ok | {error, term()}.
delete(LogId) ->
    %% @TODO VFS-7411 trick dialyzer into thinking that errors can be returned
    %% they actually will after integration with datastore
    case LogId of
        <<"123">> -> {error, etmpfail};
        %% @TODO VFS-7411 should return ok if the document is not found
        _ -> node_cache:clear({?MODULE, LogId})
    end.


-spec set_ttl(infinite_log:log_id(), time:seconds()) -> ok | {error, term()}.
set_ttl(LogId, Ttl) ->
    %% @TODO VFS-7411 trick dialyzer into thinking that errors can be returned
    %% they actually will after integration with datastore
    case LogId of
        <<"123">> ->
            {error, etmpfail};
        _ ->
            %% @TODO VFS-7411 adjust to CB's way of handling expiration
            {ok, Record} = infinite_log_sentinel:acquire(LogId, skip_pruning, readonly),
            save(LogId, Record#sentinel{
                expiration_time = current_timestamp(Record) div 1000 + Ttl
            })
    end.

%%=====================================================================
%% Convenience functions
%%=====================================================================

-spec append(record(), infinite_log:content()) -> ok | {error, term()}.
append(Sentinel = #sentinel{log_id = LogId, total_entry_count = EntryCount, oldest_timestamp = OldestTimestamp}, Content) ->
    case transfer_entries_to_new_node_upon_full_buffer(Sentinel) of
        {error, _} = Error ->
            Error;
        {ok, UpdatedSentinel = #sentinel{buffer = Buffer}} ->
            Timestamp = current_timestamp(UpdatedSentinel),
            FinalSentinel = UpdatedSentinel#sentinel{
                total_entry_count = EntryCount + 1,
                buffer = infinite_log_node:append_entry(Buffer, {Timestamp, Content}),
                oldest_timestamp = case EntryCount of
                    0 -> Timestamp;
                    _ -> OldestTimestamp
                end,
                newest_timestamp = Timestamp
            },
            save(LogId, FinalSentinel)
    end.


-spec get_node_by_number(record(), infinite_log_node:node_number()) ->
    {ok, infinite_log_node:record()} | {error, term()}.
get_node_by_number(Sentinel = #sentinel{log_id = LogId}, NodeNumber) ->
    case infinite_log_node:newest_node_number(Sentinel) of
        NodeNumber ->
            {ok, Sentinel#sentinel.buffer};
        _ ->
            infinite_log_node:get(LogId, NodeNumber)
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec transfer_entries_to_new_node_upon_full_buffer(record()) -> {ok, record()} | {error, term()}.
transfer_entries_to_new_node_upon_full_buffer(Sentinel = #sentinel{max_entries_per_node = MaxEntriesPerNode}) ->
    case infinite_log_node:get_node_entries_length(Sentinel, infinite_log_node:newest_node_number(Sentinel)) of
        MaxEntriesPerNode ->
            save_buffer_as_new_node(Sentinel);
        _ ->
            {ok, Sentinel}
    end.


%% @private
-spec save_buffer_as_new_node(record()) -> {ok, record()} | {error, term()}.
save_buffer_as_new_node(Sentinel = #sentinel{buffer = Buffer}) ->
    NodeNumber = infinite_log_node:newest_node_number(Sentinel),
    UpdatedSentinel = case infinite_log_node:oldest_node_number(Sentinel) of
        NodeNumber ->
            Sentinel#sentinel{
                oldest_timestamp = Buffer#node.oldest_timestamp,
                oldest_node_timestamp = Buffer#node.newest_timestamp
            };
        _ ->
            Sentinel
    end,
    case save_node(Sentinel, NodeNumber, Buffer) of
        ok ->
            apply_size_and_age_pruning(UpdatedSentinel#sentinel{buffer = #node{}}, allow_updates);
        {error, _} = SaveError ->
            SaveError
    end.


%% @private
-spec save_node(record(), infinite_log_node:node_number(), infinite_log_node:record()) ->
    ok | {error, term()}.
save_node(#sentinel{expiration_time = undefined, age_pruning_threshold = undefined} = S, NodeNumber, Node) ->
    save_node(S, NodeNumber, Node, undefined);
save_node(#sentinel{expiration_time = undefined, age_pruning_threshold = AgeThreshold} = S, NodeNumber, Node) ->
    save_node(S, NodeNumber, Node, AgeThreshold);
save_node(#sentinel{expiration_time = ExpirationTime, age_pruning_threshold = undefined} = S, NodeNumber, Node) ->
    Ttl = ExpirationTime - current_timestamp(S),
    save_node(S, NodeNumber, Node, Ttl);
save_node(#sentinel{expiration_time = ExpirationTime, age_pruning_threshold = AgeThreshold} = S, NodeNumber, Node) ->
    Ttl = ExpirationTime - current_timestamp(S),
    save_node(S, NodeNumber, Node, min(Ttl, AgeThreshold)).


%% @private
-spec save_node(record(), infinite_log_node:node_number(), infinite_log_node:record(), undefined | time:seconds()) ->
    ok | {error, term()}.
save_node(#sentinel{log_id = LogId}, NodeNumber, Node, undefined) ->
    infinite_log_node:save(LogId, NodeNumber, Node);
save_node(#sentinel{log_id = LogId} = Sentinel, NodeNumber, Node, Ttl) ->
    case save_node(Sentinel, NodeNumber, Node, undefined) of
        {error, _} = Error ->
            Error;
        ok ->
            infinite_log_node:set_ttl(LogId, NodeNumber, Ttl)
    end.


%% @private
-spec apply_size_and_age_pruning(record(), infinite_log:access_mode()) ->
    {ok, record()} | {error, term()}.
apply_size_and_age_pruning(Sentinel, AccessMode) ->
    case apply_size_pruning(Sentinel, AccessMode) of
        {ok, UpdatedSentinel} ->
            apply_age_pruning(UpdatedSentinel, AccessMode);
        {error, _} = Error ->
            Error
    end.


%% @private
-spec apply_size_pruning(record(), infinite_log:access_mode()) -> {ok, record()} | {error, term()}.
apply_size_pruning(#sentinel{size_pruning_threshold = undefined} = Sentinel, _) ->
    {ok, Sentinel};
apply_size_pruning(Sentinel, AccessMode) ->
    prune_while(Sentinel, AccessMode, fun(Acc) ->
        CurrentEntryCount = Acc#sentinel.total_entry_count - Acc#sentinel.oldest_entry_index,
        CurrentEntryCount - Acc#sentinel.max_entries_per_node >= Acc#sentinel.size_pruning_threshold
    end).


%% @private
-spec apply_age_pruning(record(), infinite_log:access_mode()) -> {ok, record()} | {error, term()}.
apply_age_pruning(#sentinel{age_pruning_threshold = undefined} = Sentinel, _) ->
    {ok, Sentinel};
apply_age_pruning(Sentinel, AccessMode) ->
    Now = current_timestamp(Sentinel),
    prune_while(Sentinel, AccessMode, fun(Acc) ->
        % timestamps are in milliseconds, while the threshold is in seconds
        Now >= Acc#sentinel.oldest_node_timestamp + Acc#sentinel.age_pruning_threshold * 1000
    end).


%% @private
-spec prune_while(record(), infinite_log:access_mode(), fun((record()) -> boolean())) ->
    {ok, record()} | {error, term()}.
prune_while(Sentinel, AccessMode, Condition) ->
    OldestNodeNumber = infinite_log_node:oldest_node_number(Sentinel),
    NewestNodeNumber = infinite_log_node:newest_node_number(Sentinel),
    case OldestNodeNumber of
        NewestNodeNumber ->
            % no nodes left to be pruned (the buffer node is never pruned)
            {ok, Sentinel};
        _ ->
            case {Condition(Sentinel), AccessMode} of
                {false, _} ->
                    {ok, Sentinel};
                {true, readonly} ->
                    {error, update_required};
                {true, allow_updates} ->
                    case prune_oldest_node(Sentinel) of
                        {ok, UpdatedSentinel} ->
                            % the procedure is applied recursively until the
                            % oldest existing node is found
                            prune_while(UpdatedSentinel, AccessMode, Condition);
                        {error, _} = Error ->
                            Error
                    end
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% The procedure assumes that some nodes may have expired by themselves due to
%% a TTL, in such case it adjusts the sentinel to acknowledge that.
%% @end
%%--------------------------------------------------------------------
prune_oldest_node(Sentinel = #sentinel{oldest_entry_index = PrunedCount, max_entries_per_node = MaxEntriesPerNode}) ->
    OldestNodeNumber = infinite_log_node:oldest_node_number(Sentinel),
    case infinite_log_node:delete(Sentinel#sentinel.log_id, OldestNodeNumber) of
        {error, _} = Error ->
            Error;
        ok ->
            UpdatedSentinel = Sentinel#sentinel{oldest_entry_index = PrunedCount + MaxEntriesPerNode},
            NewOldestNodeNumber = OldestNodeNumber + 1,
            case infinite_log_node:newest_node_number(UpdatedSentinel) of
                NewOldestNodeNumber ->
                    % the sentinel may have no entries, in which case the buffer node's
                    % newest_timestamp is meaningless - take the global newest known timestamp
                    {ok, UpdatedSentinel#sentinel{
                        oldest_node_timestamp = UpdatedSentinel#sentinel.newest_timestamp
                    }};
                _ ->
                    case get_node_by_number(Sentinel, NewOldestNodeNumber) of
                        {ok, #node{entries = []}} ->
                            Sentinel#sentinel.newest_timestamp;
                        {ok, NewOldestNode} ->
                            {ok, UpdatedSentinel#sentinel{
                                oldest_node_timestamp = NewOldestNode#node.newest_timestamp
                            }};
                        {error, not_found} ->
                            % the next node may have already expired, in such case
                            % the oldest_node_timestamp will be adjusted during another
                            % pruning when the oldest existing node is found
                            {ok, UpdatedSentinel}
                    end
            end
    end.


%% @private
-spec current_timestamp(record()) -> infinite_log:timestamp().
current_timestamp(#sentinel{newest_timestamp = NewestTimestamp}) ->
    global_clock:monotonic_timestamp_millis(NewestTimestamp).
