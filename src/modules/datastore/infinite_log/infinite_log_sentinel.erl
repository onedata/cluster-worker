%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API related to the infinite_log_sentinel datastore model.
%%% @end
%%%-------------------------------------------------------------------
-module(infinite_log_sentinel).
-author("Lukasz Opiola").

-include("modules/datastore/infinite_log.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% Model API
-export([acquire/5, create/4, delete/3, adjust_expiry_threshold/4]).
%% Convenience functions
-export([append/4]).
-export([get_node_by_number/4]).
-export([current_timestamp/1]).

-type record() :: #infinite_log_sentinel{}.
-export_type([record/0]).

%% Datastore API
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2]).

-define(CTX, #{
    model => ?MODULE
}).

%%=====================================================================
%% API
%%=====================================================================

-spec acquire(
    infinite_log:ctx(),
    infinite_log:log_id(),
    skip_pruning | apply_pruning,
    infinite_log:access_mode(),
    infinite_log:batch()
) ->
    {{ok, term()} | {error, term()}, infinite_log:batch()}.
acquire(Ctx, LogId, skip_pruning, _, Batch) ->
    case datastore_doc:fetch(Ctx, LogId, Batch) of
        {{ok, #document{value = Value}}, UpdatedBatch} ->
            {{ok, Value}, UpdatedBatch};
        {{error, _}, _} = Error ->
            Error
    end;
acquire(Ctx, LogId, apply_pruning, AccessMode, Batch) ->
    case acquire(Ctx, LogId, skip_pruning, AccessMode, Batch) of
        {{error, _}, _} = GetError ->
            GetError;
        {{ok, Sentinel}, AcquireBatch} ->
            case apply_age_pruning(Ctx, Sentinel, AccessMode, AcquireBatch) of
                {{error, _}, _} = PruningError ->
                    PruningError;
                {{ok, unchanged, FinalSentinel}, UpdatedBatch} ->
                    {{ok, FinalSentinel}, UpdatedBatch};
                {{ok, updated, FinalSentinel}, UpdatedBatch} ->
                    case save(Ctx, LogId, FinalSentinel, UpdatedBatch) of
                        {ok, FinalBatch} ->
                            {{ok, FinalSentinel}, FinalBatch};
                        {{error, _}, _} = SaveError ->
                            SaveError
                    end
            end
    end.


-spec create(infinite_log:ctx(), infinite_log:log_id(), term(), infinite_log:batch()) ->
    {ok | {error, term()}, infinite_log:batch()}.
create(Ctx, LogId, Sentinel, Batch) ->
    persist(create, Ctx, LogId, Sentinel, Batch).


-spec delete(infinite_log:ctx(), infinite_log:log_id(), infinite_log:batch()) ->
    {ok | {error, term()}, infinite_log:batch()}.
delete(Ctx, LogId, Batch) ->
    datastore_doc:delete(Ctx, LogId, Batch).


-spec adjust_expiry_threshold(infinite_log:ctx(), infinite_log:log_id(), time:seconds(), infinite_log:batch()) ->
    {ok | {error, term()}, infinite_log:batch()}.
adjust_expiry_threshold(Ctx, LogId, Threshold, Batch) ->
    {{ok, Record}, UpdatedBatch} = infinite_log_sentinel:acquire(Ctx, LogId, skip_pruning, allow_updates, Batch),
    save(Ctx, LogId, Record#infinite_log_sentinel{
        expiry_threshold = Threshold
    }, UpdatedBatch).

%%=====================================================================
%% Convenience functions
%%=====================================================================

-spec append(infinite_log:ctx(), record(), infinite_log:content(), infinite_log:batch()) ->
    {ok | {error, term()}, infinite_log:batch()}.
append(Ctx, Sentinel = #infinite_log_sentinel{log_id = LogId, total_entry_count = EntryCount, oldest_timestamp = OldestTimestamp}, Content, Batch) ->
    case transfer_entries_to_new_node_upon_full_buffer(Ctx, Sentinel, Batch) of
        {{error, _}, _} = Error ->
            Error;
        {{ok, UpdatedSentinel = #infinite_log_sentinel{buffer = Buffer}}, UpdatedBatch} ->
            Timestamp = current_timestamp(UpdatedSentinel),
            FinalSentinel = UpdatedSentinel#infinite_log_sentinel{
                total_entry_count = EntryCount + 1,
                buffer = infinite_log_node:append_entry(Buffer, {Timestamp, Content}),
                oldest_timestamp = case EntryCount of
                    0 -> Timestamp;
                    _ -> OldestTimestamp
                end,
                newest_timestamp = Timestamp
            },
            save(Ctx, LogId, FinalSentinel, UpdatedBatch)
    end.


-spec get_node_by_number(infinite_log:ctx(), record(), infinite_log_node:node_number(), infinite_log:batch()) ->
    {{ok, infinite_log_node:record()} | {error, term()}, infinite_log:batch()}.
get_node_by_number(Ctx, Sentinel = #infinite_log_sentinel{log_id = LogId}, NodeNumber, Batch) ->
    case infinite_log_node:newest_node_number(Sentinel) of
        NodeNumber ->
            {{ok, Sentinel#infinite_log_sentinel.buffer}, Batch};
        _ ->
            infinite_log_node:get(Ctx, LogId, NodeNumber, Batch)
    end.


-spec current_timestamp(record()) -> infinite_log:timestamp_millis().
current_timestamp(#infinite_log_sentinel{newest_timestamp = NewestTimestamp}) ->
    global_clock:monotonic_timestamp_millis(NewestTimestamp).

%%=====================================================================
%% Internal functions
%%=====================================================================


%% @private
-spec save(infinite_log:ctx(), infinite_log:log_id(), record(), infinite_log:batch()) ->
    {ok | {error, term()}, infinite_log:batch()}.
save(Ctx, LogId, Sentinel, Batch) ->
    persist(save, Ctx, LogId, Sentinel, Batch).


%% @private
-spec persist(save | create, infinite_log:ctx(), infinite_log:log_id(), record(), infinite_log:batch()) ->
    {ok | {error, term()}, infinite_log:batch()}.
persist(Operation, Ctx, LogId, Sentinel, Batch) ->
    Ctx1 = case Sentinel#infinite_log_sentinel.expiry_threshold of
        undefined ->
            Ctx;
        ThresholdSeconds ->
            NowMillis = current_timestamp(Sentinel),
            NewestLogTimestampMillis = case Sentinel#infinite_log_sentinel.total_entry_count of
                0 -> NowMillis;
                _ -> Sentinel#infinite_log_sentinel.newest_timestamp
            end,
            % expiry countdown always starts from the newest log timestamp rather than current time
            AdjustedThresholdSeconds = max(0, ThresholdSeconds - (NowMillis - NewestLogTimestampMillis) div 1000),
            datastore_doc:set_expiry_in_ctx(Ctx, AdjustedThresholdSeconds)
    end,
    case datastore_doc:Operation(Ctx1, LogId, #document{key = LogId, value = Sentinel}, Batch) of
        {{ok, _}, UpdatedBatch} -> {ok, UpdatedBatch};
        {{error, _}, _} = Error -> Error
    end.


%% @private
-spec transfer_entries_to_new_node_upon_full_buffer(infinite_log:ctx(), record(), infinite_log:batch()) ->
    {{ok, record()} | {error, term()}, infinite_log:batch()}.
transfer_entries_to_new_node_upon_full_buffer(Ctx, Sentinel = #infinite_log_sentinel{max_entries_per_node = MaxEntriesPerNode}, Batch) ->
    case infinite_log_node:get_node_entries_length(Sentinel, infinite_log_node:newest_node_number(Sentinel)) of
        MaxEntriesPerNode ->
            save_buffer_as_new_node(Ctx, Sentinel, Batch);
        _ ->
            {{ok, Sentinel}, Batch}
    end.


%% @private
-spec save_buffer_as_new_node(infinite_log:ctx(), record(), infinite_log:batch()) ->
    {{ok, record()} | {error, term()}, infinite_log:batch()}.
save_buffer_as_new_node(Ctx, Sentinel = #infinite_log_sentinel{buffer = Buffer}, Batch) ->
    NodeNumber = infinite_log_node:newest_node_number(Sentinel),
    UpdatedSentinel = case infinite_log_node:oldest_node_number(Sentinel) of
        NodeNumber ->
            Sentinel#infinite_log_sentinel{
                oldest_timestamp = Buffer#infinite_log_node.oldest_timestamp,
                oldest_node_timestamp = Buffer#infinite_log_node.newest_timestamp
            };
        _ ->
            Sentinel
    end,
    case save_node(Ctx, Sentinel, NodeNumber, Buffer, Batch) of
        {ok, UpdatedBatch} ->
            prune_upon_node_archivisation(Ctx, UpdatedSentinel#infinite_log_sentinel{buffer = #infinite_log_node{}}, allow_updates, UpdatedBatch);
        {{error, _}, _} = SaveError ->
            SaveError
    end.


%% @private
-spec save_node(
    infinite_log:ctx(),
    record(),
    infinite_log_node:node_number(),
    infinite_log_node:record(),
    infinite_log:batch()
) ->
    {ok | {error, term()}, infinite_log:batch()}.
save_node(Ctx, #infinite_log_sentinel{log_id = LogId} = Sentinel, NodeNumber, Node, Batch) ->
    case effective_node_age_pruning_threshold(Sentinel) of
        undefined ->
            infinite_log_node:save(Ctx, LogId, NodeNumber, Node, Batch);
        ExpiryThresholdSeconds ->
            NowMillis = current_timestamp(Sentinel),
            infinite_log_node:save_with_expiry_threshold_adjustment(
                Ctx, LogId, NodeNumber, Node, ExpiryThresholdSeconds, NowMillis, Batch
            )
    end.


%% @private
-spec prune_upon_node_archivisation(infinite_log:ctx(), record(), infinite_log:access_mode(), infinite_log:batch()) ->
    {{ok, record()} | {error, term()}, infinite_log:batch()}.
prune_upon_node_archivisation(Ctx, Sentinel, AccessMode, Batch) ->
    case apply_size_pruning(Ctx, Sentinel, AccessMode, Batch) of
        {{ok, _, NewSentinel}, UpdatedBatch} ->
            case apply_age_pruning(Ctx, NewSentinel, AccessMode, UpdatedBatch) of
                {{ok, _, FinalSentinel}, FinalBatch} ->
                    {{ok, FinalSentinel}, FinalBatch};
                {{error, _}, _} = AgePruningError ->
                    AgePruningError
            end;
        {{error, _}, _} = SizePruningError ->
            SizePruningError
    end.


%% @private
-spec apply_size_pruning(infinite_log:ctx(), record(), infinite_log:access_mode(), infinite_log:batch()) ->
    {{ok, updated | unchanged, record()} | {error, term()}, infinite_log:batch()}.
apply_size_pruning(_Ctx, #infinite_log_sentinel{size_pruning_threshold = undefined} = Sentinel, _, Batch) ->
    {{ok, unchanged, Sentinel}, Batch};
apply_size_pruning(Ctx, Sentinel, AccessMode, Batch) ->
    prune_while(Ctx, Sentinel, AccessMode, unchanged, fun(Acc) ->
        CurrentEntryCount = Acc#infinite_log_sentinel.total_entry_count - Acc#infinite_log_sentinel.oldest_entry_index,
        CurrentEntryCount - Acc#infinite_log_sentinel.max_entries_per_node >= Acc#infinite_log_sentinel.size_pruning_threshold
    end, Batch).


%% @private
-spec apply_age_pruning(infinite_log:ctx(), record(), infinite_log:access_mode(), infinite_log:batch()) ->
    {{ok, updated | unchanged, record()} | {error, term()}, infinite_log:batch()}.
apply_age_pruning(Ctx, Sentinel, AccessMode, Batch) ->
    case effective_node_age_pruning_threshold(Sentinel) of
        undefined ->
            {{ok, unchanged, Sentinel}, Batch};
        PruningThresholdSeconds ->
            NowMillis = current_timestamp(Sentinel),
            prune_while(Ctx, Sentinel, AccessMode, unchanged, fun(Acc) ->
                NowMillis >= Acc#infinite_log_sentinel.oldest_node_timestamp + PruningThresholdSeconds * 1000
            end, Batch)
    end.


%% @private
-spec prune_while(infinite_log:ctx(), record(), infinite_log:access_mode(), updated | unchanged, fun((record()) -> boolean()), infinite_log:batch()) ->
    {{ok, updated | unchanged, record()} | {error, term()}, infinite_log:batch()}.
prune_while(Ctx, Sentinel, AccessMode, UpdateStatus, Condition, Batch) ->
    OldestNodeNumber = infinite_log_node:oldest_node_number(Sentinel),
    NewestNodeNumber = infinite_log_node:newest_node_number(Sentinel),
    case OldestNodeNumber of
        NewestNodeNumber ->
            % no nodes left to be pruned (the buffer node is never pruned)
            {{ok, UpdateStatus, Sentinel}, Batch};
        _ ->
            case {Condition(Sentinel), AccessMode} of
                {false, _} ->
                    {{ok, UpdateStatus, Sentinel}, Batch};
                {true, readonly} ->
                    {{error, update_required}, Batch};
                {true, allow_updates} ->
                    case prune_oldest_node(Ctx, Sentinel, Batch) of
                        {{ok, UpdatedSentinel}, UpdatedBatch} ->
                            % the procedure is applied recursively until the
                            % oldest existing node is found
                            prune_while(Ctx, UpdatedSentinel, AccessMode, updated, Condition, UpdatedBatch);
                        {{error, _}, _} = Error ->
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
-spec prune_oldest_node(infinite_log:ctx(), record(), infinite_log:batch()) ->
    {{ok, record()} | {error, term()}, infinite_log:batch()}.
prune_oldest_node(Ctx, Sentinel = #infinite_log_sentinel{oldest_entry_index = PrunedCount, max_entries_per_node = MaxEntriesPerNode}, Batch) ->
    OldestNodeNumber = infinite_log_node:oldest_node_number(Sentinel),
    case infinite_log_node:delete(Ctx, Sentinel#infinite_log_sentinel.log_id, OldestNodeNumber, Batch) of
        {{error, _}, _} = Error ->
            Error;
        {ok, UpdatedBatch} ->
            UpdatedSentinel = Sentinel#infinite_log_sentinel{oldest_entry_index = PrunedCount + MaxEntriesPerNode},
            NewOldestNodeNumber = OldestNodeNumber + 1,
            case infinite_log_node:newest_node_number(UpdatedSentinel) of
                NewOldestNodeNumber ->
                    % the sentinel may have no entries, in which case the buffer node's
                    % newest_timestamp is meaningless - take the global newest known timestamp
                    {{ok, UpdatedSentinel#infinite_log_sentinel{
                        oldest_node_timestamp = UpdatedSentinel#infinite_log_sentinel.newest_timestamp
                    }}, UpdatedBatch};
                _ ->
                    case get_node_by_number(Ctx, Sentinel, NewOldestNodeNumber, UpdatedBatch) of
                        {{ok, #infinite_log_node{entries = []}}, FinalBatch} ->
                            {{ok, Sentinel#infinite_log_sentinel.newest_timestamp}, FinalBatch};
                        {{ok, NewOldestNode}, FinalBatch} ->
                            {{ok, UpdatedSentinel#infinite_log_sentinel{
                                oldest_node_timestamp = NewOldestNode#infinite_log_node.newest_timestamp
                            }}, FinalBatch};
                        {{error, not_found}, FinalBatch} ->
                            % the next node may have already expired, in such case
                            % the oldest_node_timestamp will be adjusted during another
                            % pruning when the oldest existing node is found
                            {{ok, UpdatedSentinel}, FinalBatch}
                    end
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Determines the prevailing age pruning threshold for archival nodes.
%% @end
%%--------------------------------------------------------------------
-spec effective_node_age_pruning_threshold(record()) -> undefined | time:seconds().
effective_node_age_pruning_threshold(#infinite_log_sentinel{expiry_threshold = undefined, age_pruning_threshold = undefined}) ->
    undefined;
effective_node_age_pruning_threshold(#infinite_log_sentinel{expiry_threshold = undefined, age_pruning_threshold = AgeThreshold}) ->
    AgeThreshold;
effective_node_age_pruning_threshold(#infinite_log_sentinel{expiry_threshold = ExpiryThreshold, age_pruning_threshold = undefined}) ->
    ExpiryThreshold;
effective_node_age_pruning_threshold(#infinite_log_sentinel{expiry_threshold = ExpiryThreshold, age_pruning_threshold = AgeThreshold}) ->
    min(ExpiryThreshold, AgeThreshold).

%%%===================================================================
%%% Datastore API
%%%===================================================================

-spec get_ctx() -> datastore_model:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    % NOTE: this model contains `infinite_log_node` and must be upgraded whenever
    % it changes.
    2.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {log_id, string},
        {max_entries_per_node, integer},
        {total_entry_count, integer},
        {oldest_entry_index, integer},
        {oldest_timestamp, integer},
        {newest_timestamp, integer},
        {oldest_node_timestamp, integer},
        {buffer, infinite_log_node:get_record_struct(1)},
        {size_pruning_threshold, integer},
        {age_pruning_threshold, integer},
        {expiration_time, integer}
    ]};
get_record_struct(2) ->
    % changed expiration_time -> expiry_threshold
    {record, [
        {log_id, string},
        {max_entries_per_node, integer},
        {total_entry_count, integer},
        {oldest_entry_index, integer},
        {oldest_timestamp, integer},
        {newest_timestamp, integer},
        {oldest_node_timestamp, integer},
        {buffer, infinite_log_node:get_record_struct(1)},
        {size_pruning_threshold, integer},
        {age_pruning_threshold, integer},
        {expiry_threshold, integer}  % changed field
    ]}.


-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, Record) ->
    {
        ?MODULE,
        LogId,
        MaxEntriesPerNode,
        TotalEntryCount,
        OldestEntryIndex,
        OldestTimestamp,
        NewestTimestamp,
        OldestNodeTimestamp,
        Buffer,
        SizePruningThreshold,
        AgePruningThreshold,
        ExpirationTime
    } = Record,
    {2, {?MODULE,
        LogId,
        MaxEntriesPerNode,
        TotalEntryCount,
        OldestEntryIndex,
        OldestTimestamp,
        NewestTimestamp,
        OldestNodeTimestamp,
        Buffer,
        SizePruningThreshold,
        AgePruningThreshold,
        _ExpiryThreshold = case ExpirationTime of
            undefined -> undefined;
            _ -> min(1, ExpirationTime - global_clock:timestamp_seconds())
        end
    }}.
