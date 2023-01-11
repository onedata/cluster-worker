%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for the infinite log datastore structure that handles
%%% listing the log entries.
%%% @end
%%%-------------------------------------------------------------------
-module(infinite_log_browser).
-author("Lukasz Opiola").

-include("modules/datastore/infinite_log.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([list/4]).

-type direction() :: ?FORWARD | ?BACKWARD.
-type start_from() :: undefined | {index, entry_index()} | {timestamp, timestamp()}.
% offset from the starting point requested using start_from parameter, may be negative
% if the offset points to an entry before the beginning of the list, it is rounded up to the first index
% if the offset points to an entry after the end of the list, the returned result will always be empty
-type offset() :: integer().
-type limit() :: pos_integer().
%% @formatter:off
-type listing_opts() :: #{
    direction => direction(),
    start_from => start_from(),
    offset => offset(),
    limit => limit()
}.
%% @formatter:on

-type entry_series() :: [{entry_index(), entry()}].
% indicates if listing has ended
-type progress_marker() :: more | done.
-type listing_result() :: {progress_marker(), entry_series()}.

-export_type([direction/0, offset/0, limit/0]).
-export_type([listing_opts/0, progress_marker/0, listing_result/0]).

%%%-------------------------------------------------------------------
%%% internal definitions
%%%-------------------------------------------------------------------
-record(listing_state, {
    sentinel :: sentinel_record(),
    direction :: direction(),
    start_index :: entry_index(), % inclusive
    end_index :: entry_index(),   % inclusive
    acc :: entry_series()
}).
-type listing_state() :: #listing_state{}.

-record(search_state, {
    sentinel :: sentinel_record(),
    direction :: direction(),
    target_tstamp :: timestamp(),
    oldest_node_number = 0 :: node_number(),
    oldest_index = 0 :: entry_index(),
    oldest_tstamp = 0 :: timestamp(),
    newest_node_number = 0 :: node_number(),
    newest_index = 0 :: entry_index(),
    newest_tstamp = 0 :: timestamp()
}).
-type search_state() :: #search_state{}.

% index of an entry within a node, starting from 0
-type index_in_node() :: non_neg_integer().

-type timestamp() :: infinite_log:timestamp_millis().
-type entry_index() :: infinite_log:entry_index().
-type entry() :: infinite_log:entry().
-type sentinel_record() :: infinite_log_sentinel:record().
-type node_record() :: infinite_log_node:record().
-type node_number() :: infinite_log_node:node_number().

%%=====================================================================
%% API
%%=====================================================================

-spec list(infinite_log:ctx(), sentinel_record(), listing_opts(), infinite_log:batch()) -> 
    {listing_result(), infinite_log:batch()}.
list(Ctx, Sentinel = #infinite_log_sentinel{total_entry_count = TotalEntryCount, oldest_entry_index = OldestIndex}, Opts, Batch) ->
    Direction = maps:get(direction, Opts, ?FORWARD),
    StartFrom = maps:get(start_from, Opts, undefined),
    OffsetFromStartPoint = maps:get(offset, Opts, 0),
    Limit = maps:get(limit, Opts, ?MAX_LISTING_BATCH),

    {StartPoint, UpdatedBatch} = start_from_param_to_offset(Ctx, Sentinel, Direction, StartFrom, Batch),
    EffectiveOffset = snap_to_range(StartPoint + OffsetFromStartPoint, {0, infinity}),

    StartIndex = case Direction of
        ?FORWARD -> max(EffectiveOffset, OldestIndex);
        ?BACKWARD -> TotalEntryCount - 1 - EffectiveOffset
    end,

    EffectiveLimit = snap_to_range(Limit, {1, ?MAX_LISTING_BATCH}),
    UnboundedEndIndex = calc_end_index(Direction, StartIndex, EffectiveLimit),
    EndIndex = case Direction of
        ?FORWARD -> min(UnboundedEndIndex, TotalEntryCount - 1);
        ?BACKWARD -> max(UnboundedEndIndex, OldestIndex)
    end,

    {EntrySeries, FinalBatch} = list(Ctx, #listing_state{
        sentinel = Sentinel,
        direction = Direction,
        start_index = StartIndex,
        end_index = EndIndex,
        acc = []
    }, UpdatedBatch),
    {{gen_progress_marker(Sentinel, Direction, EndIndex), EntrySeries}, FinalBatch}.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec list(infinite_log:ctx(), listing_state(), infinite_log:batch()) -> 
    {entry_series(), infinite_log:batch()}.
list(_Ctx, #listing_state{direction = ?FORWARD, start_index = From, end_index = To, acc = Acc}, Batch) when From > To ->
    {Acc, Batch};
list(_Ctx, #listing_state{direction = ?BACKWARD, start_index = From, end_index = To, acc = Acc}, Batch) when From < To ->
    {Acc, Batch};
list(_Ctx, #listing_state{sentinel = Sentinel, start_index = From, acc = Acc}, Batch) when From >= Sentinel#infinite_log_sentinel.total_entry_count ->
    {Acc, Batch};
list(Ctx, #listing_state{sentinel = Sentinel, direction = Direction, start_index = StartIdx, end_index = EndIdx} = State, Batch) ->
    MaxEntriesPerNode = Sentinel#infinite_log_sentinel.max_entries_per_node,
    RemainingEntryCount = abs(EndIdx - StartIdx) + 1,

    CurrentNodeNum = infinite_log_node:entry_index_to_node_number(Sentinel, StartIdx),

    StartIdxInNode = StartIdx rem MaxEntriesPerNode,
    LimitInNode = min(RemainingEntryCount, case Direction of
        ?FORWARD -> MaxEntriesPerNode - StartIdxInNode;
        ?BACKWARD -> StartIdxInNode + 1
    end),

    {Entries, UpdatedBatch} = extract_entries(Ctx, Sentinel, Direction, CurrentNodeNum, StartIdxInNode, LimitInNode, Batch),
    SeriesEndIdx = calc_end_index(Direction, StartIdx, LimitInNode),
    EntrySeries = assign_entry_indices(Direction, Entries, StartIdx, SeriesEndIdx),

    list(Ctx, State#listing_state{
        start_index = calc_end_index(Direction, StartIdx, LimitInNode + 1),
        acc = State#listing_state.acc ++ EntrySeries
    }, UpdatedBatch).


%% @private
-spec extract_entries(infinite_log:ctx(), sentinel_record(), direction(), node_number(), index_in_node(), limit(), infinite_log:batch()) ->
    {[entry()], infinite_log:batch()}.
extract_entries(Ctx, Sentinel, Direction, NodeNumber, StartIndexInNode, Limit, Batch) ->
    {{ok, #infinite_log_node{entries = Entries}}, UpdatedBatch} = infinite_log_sentinel:get_node_by_number(Ctx, Sentinel, NodeNumber, Batch),
    EntriesLength = infinite_log_node:get_node_entries_length(Sentinel, NodeNumber),
    case Direction of
        ?FORWARD ->
            EndIndexInNode = calc_end_index(Direction, StartIndexInNode, Limit),
            ReversedStartIndex = EntriesLength - 1 - EndIndexInNode,
            {lists:reverse(lists:sublist(Entries, ReversedStartIndex + 1, Limit)), UpdatedBatch};
        ?BACKWARD ->
            {lists:sublist(Entries, EntriesLength - StartIndexInNode, Limit), UpdatedBatch}
    end.


%% @private
-spec assign_entry_indices(direction(), [entry()], entry_index(), entry_index()) ->
    entry_series().
assign_entry_indices(?FORWARD, Entries, StartIndex, EndIndex) ->
    lists:zip(lists:seq(StartIndex, EndIndex), Entries);
assign_entry_indices(?BACKWARD, Entries, StartIndex, EndIndex) ->
    lists:zip(lists:seq(StartIndex, EndIndex, -1), Entries).


%% @private
-spec gen_progress_marker(sentinel_record(), direction(), entry_index()) ->
    progress_marker().
gen_progress_marker(#infinite_log_sentinel{total_entry_count = EntryCount}, ?FORWARD, EndIndex) when EndIndex >= EntryCount - 1 ->
    done;
gen_progress_marker(_, ?FORWARD, _) ->
    more;
gen_progress_marker(#infinite_log_sentinel{oldest_entry_index = OldestIndex}, ?BACKWARD, EndIndex) when EndIndex =< OldestIndex ->
    done;
gen_progress_marker(_, ?BACKWARD, _) ->
    more.


%% @private
-spec calc_end_index(direction(), entry_index(), limit()) ->
    entry_index().
calc_end_index(?FORWARD, StartIndex, Limit) ->
    StartIndex + Limit - 1;
calc_end_index(?BACKWARD, StartIndex, Limit) ->
    StartIndex - Limit + 1.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Transform the start_from() parameter to the corresponding starting offset,
%% relevant for given direction (so that the zero offset is the oldest entry
%% for ?FORWARD direction and the newest entry for ?BACKWARD direction).
%% The returned starting offset can still be adjusted using the offset listing opt.
%% Every offset lower than the oldest index is rounded up to it.
%% Every offset exceeding the log length is rounded down to the next index after the last element.
%% @end
%%--------------------------------------------------------------------
-spec start_from_param_to_offset(infinite_log:ctx(), sentinel_record(), direction(), start_from(), infinite_log:batch()) ->
    {entry_index(), infinite_log:batch()}.
start_from_param_to_offset(_Ctx, #infinite_log_sentinel{oldest_entry_index = OldestIndex}, ?FORWARD, undefined, Batch) ->
    {OldestIndex, Batch};
start_from_param_to_offset(_Ctx, _Sentinel, ?BACKWARD, undefined, Batch) ->
    {0, Batch};
start_from_param_to_offset(_Ctx, #infinite_log_sentinel{total_entry_count = EntryCount} = S, ?FORWARD, {index, EntryIdx}, Batch) ->
    {snap_to_range(EntryIdx, {S#infinite_log_sentinel.oldest_entry_index, EntryCount}), Batch};
start_from_param_to_offset(_Ctx, #infinite_log_sentinel{total_entry_count = EntryCount} = S, ?BACKWARD, {index, EntryIdx}, Batch) ->
    {snap_to_range(EntryCount - 1 - EntryIdx, {0, EntryCount - S#infinite_log_sentinel.oldest_entry_index}), Batch};
start_from_param_to_offset(Ctx, Sentinel, Direction, {timestamp, Timestamp}, Batch) ->
    SS1 = #search_state{sentinel = Sentinel, direction = Direction, target_tstamp = Timestamp},
    SS2 = set_search_since(SS1, Sentinel#infinite_log_sentinel.oldest_entry_index, Sentinel#infinite_log_sentinel.oldest_timestamp),
    SS3 = set_search_until(SS2, Sentinel#infinite_log_sentinel.total_entry_count - 1, Sentinel#infinite_log_sentinel.newest_timestamp),
    {EntryIndex, UpdatedBatch} = locate_timestamp(Ctx, SS3, Batch),
    start_from_param_to_offset(Ctx, Sentinel, Direction, {index, EntryIndex}, UpdatedBatch).


%% @private
-spec set_search_since(search_state(), entry_index(), timestamp()) -> search_state().
set_search_since(SS = #search_state{sentinel = Sentinel}, OldestIndex, OldestTimestamp) ->
    SS#search_state{
        oldest_node_number = infinite_log_node:entry_index_to_node_number(Sentinel, OldestIndex),
        oldest_index = OldestIndex,
        oldest_tstamp = OldestTimestamp
    }.


%% @private
-spec set_search_until(search_state(), entry_index(), timestamp()) -> search_state().
set_search_until(SS = #search_state{sentinel = Sentinel}, NewestIndex, NewestTimestamp) ->
    SS#search_state{
        newest_node_number = infinite_log_node:entry_index_to_node_number(Sentinel, NewestIndex),
        newest_index = NewestIndex,
        newest_tstamp = NewestTimestamp
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Uses a heuristic approach to find an entry with requested timestamp in a
%% range of entries (or the closest one relevant for given direction).
%% Knowing the newest entry and its timestamp as well as oldest entry and timestamp
%% in the range, approximates the target index assuming that the timestamps are
%% growing linearly. This yields two separate ranges, and the algorithm is called
%% recursively for the range which includes the timestamp, until the search is
%% narrowed down to one or two adjacent nodes.
%% Depending on the direction, the procedure finds:
%%    ?FORWARD -> first entry with timestamp that is not lower than the target timestamp,
%%    ?BACKWARD -> last entry with timestamp that is not higher than the target timestamp.
%% @end
%%--------------------------------------------------------------------
-spec locate_timestamp(infinite_log:ctx(), search_state(), infinite_log:batch()) -> 
    {entry_index(), infinite_log:batch()}.
locate_timestamp(_Ctx, SS = #search_state{direction = ?FORWARD, target_tstamp = T, oldest_tstamp = Oldest}, Batch) when T =< Oldest ->
    {SS#search_state.oldest_index, Batch};
locate_timestamp(_Ctx, SS = #search_state{direction = ?FORWARD, target_tstamp = T, newest_tstamp = Newest}, Batch) when T > Newest ->
    {SS#search_state.newest_index + 1, Batch};
locate_timestamp(_Ctx, SS = #search_state{direction = ?BACKWARD, target_tstamp = T, newest_tstamp = Newest}, Batch) when T >= Newest ->
    {SS#search_state.newest_index, Batch};
locate_timestamp(_Ctx, SS = #search_state{direction = ?BACKWARD, target_tstamp = T, oldest_tstamp = Oldest}, Batch) when T < Oldest ->
    {SS#search_state.oldest_index - 1, Batch};
locate_timestamp(Ctx, SS = #search_state{oldest_node_number = OldestNodeNum, newest_node_number = NewestNodeNum}, Batch) ->
    case NewestNodeNum - OldestNodeNum of
        0 ->
            locate_timestamp_within_one_node(Ctx, SS, Batch);
        1 ->
            locate_timestamp_within_two_adjacent_nodes(Ctx, SS, Batch);
        _ ->
            locate_timestamp_within_more_than_two_nodes(Ctx, SS, Batch)
    end.


%% @private
-spec locate_timestamp_within_one_node(infinite_log:ctx(), search_state(), infinite_log:batch()) -> 
    {entry_index(), infinite_log:batch()}.
locate_timestamp_within_one_node(Ctx, #search_state{
    sentinel = Sentinel, direction = Direction, target_tstamp = TStamp, oldest_node_number = NodeNumber
}, Batch) ->
    case lookup_timestamp_in_node(Ctx, Sentinel, NodeNumber, Direction, TStamp, Batch) of
        {{older_than, _}, Batch2} ->
            {entry_index_older_than_first_in_node(Sentinel, NodeNumber, Direction), Batch2};
        {{included, IndexInNode}, Batch2} ->
            {index_in_node_to_entry_index(Sentinel, NodeNumber, IndexInNode), Batch2};
        {{newer_than, _}, Batch2} ->
            {entry_index_newer_than_newest_in_node(Sentinel, NodeNumber, Direction), Batch2}
    end.


%% @private
-spec locate_timestamp_within_two_adjacent_nodes(infinite_log:ctx(), search_state(), infinite_log:batch()) -> 
    {entry_index(), infinite_log:batch()}.
locate_timestamp_within_two_adjacent_nodes(Ctx, SS = #search_state{
    sentinel = Sentinel, direction = Direction, target_tstamp = TStamp,
    oldest_node_number = FirstNodeNum, newest_node_number = SecondNodeNum
}, Batch) ->
    case lookup_timestamp_in_node(Ctx, Sentinel, FirstNodeNum, Direction, TStamp, Batch) of
        {{older_than, _}, Batch2} ->
            {entry_index_older_than_first_in_node(Sentinel, FirstNodeNum, Direction), Batch2};
        {{included, IndexInNode}, Batch2} ->
            {index_in_node_to_entry_index(Sentinel, FirstNodeNum, IndexInNode), Batch2};
        {{newer_than, _}, Batch2} ->
            locate_timestamp_within_one_node(Ctx, SS#search_state{oldest_node_number = SecondNodeNum}, Batch2)
    end.


%% @private
-spec locate_timestamp_within_more_than_two_nodes(infinite_log:ctx(), search_state(), infinite_log:batch()) -> 
    {entry_index(), infinite_log:batch()}.
locate_timestamp_within_more_than_two_nodes(Ctx, SS = #search_state{
    sentinel = Sentinel, direction = Direction, target_tstamp = TStamp,
    oldest_node_number = OldestNodeNum, oldest_index = OldestIndex, oldest_tstamp = OldestTStamp,
    newest_node_number = NewestNodeNum, newest_index = NewestIndex, newest_tstamp = NewestTStamp
}, Batch) ->
    PivotIndex = OldestIndex + round(
        (NewestIndex - OldestIndex) * (TStamp - OldestTStamp) / (NewestTStamp - OldestTStamp)
    ),
    PivotNodeNum = snap_to_range(
        infinite_log_node:entry_index_to_node_number(Sentinel, PivotIndex),
        {OldestNodeNum + 1, NewestNodeNum - 1}
    ),
    case lookup_timestamp_in_node(Ctx, Sentinel, PivotNodeNum, Direction, TStamp, Batch) of
        {{older_than, PivotOldestTStamp}, UpdatedBatch} ->
            PivotOldestIndex = index_in_node_to_entry_index(Sentinel, PivotNodeNum, 0),
            locate_timestamp(Ctx, set_search_until(SS, PivotOldestIndex, PivotOldestTStamp), UpdatedBatch);
        {{included, IndexInNode}, UpdatedBatch} ->
            {index_in_node_to_entry_index(Sentinel, PivotNodeNum, IndexInNode), UpdatedBatch};
        {{newer_than, PivotNewestTStamp}, UpdatedBatch} ->
            PivotNewestIndex = index_in_node_to_entry_index(Sentinel, PivotNodeNum + 1, 0) - 1,
            locate_timestamp(Ctx, set_search_since(SS, PivotNewestIndex, PivotNewestTStamp), UpdatedBatch)
    end.


%% @private
-spec lookup_timestamp_in_node(infinite_log:ctx(), sentinel_record(), node_number(), direction(), timestamp(), infinite_log:batch()) ->
    {{older_than, timestamp()} | {included, index_in_node()} | {newer_than, timestamp()}, infinite_log:batch()}.
lookup_timestamp_in_node(Ctx, Sentinel, NodeNumber, Direction, TargetTimestamp, Batch) ->
    {{ok, Node}, UpdatedBatch} = infinite_log_sentinel:get_node_by_number(Ctx, Sentinel, NodeNumber, Batch),
    case compare_timestamp_against_node(Node, Direction, TargetTimestamp) of
        older ->
            {{older_than, Node#infinite_log_node.oldest_timestamp}, UpdatedBatch};
        included ->
            {{included, index_in_node_of_timestamp(Sentinel, NodeNumber, Node, Direction, TargetTimestamp)}, UpdatedBatch};
        newer ->
            {{newer_than, Node#infinite_log_node.newest_timestamp}, UpdatedBatch}
    end.


%% @private
-spec compare_timestamp_against_node(node_record(), direction(), timestamp()) ->
    older | included | newer.
compare_timestamp_against_node(#infinite_log_node{oldest_timestamp = Oldest}, ?FORWARD, T) when T =< Oldest -> older;
compare_timestamp_against_node(#infinite_log_node{newest_timestamp = Newest}, ?FORWARD, T) when T =< Newest -> included;
compare_timestamp_against_node(#infinite_log_node{newest_timestamp = Newest}, ?FORWARD, T) when T > Newest -> newer;
compare_timestamp_against_node(#infinite_log_node{oldest_timestamp = Oldest}, ?BACKWARD, T) when T < Oldest -> older;
compare_timestamp_against_node(#infinite_log_node{newest_timestamp = Newest}, ?BACKWARD, T) when T < Newest -> included;
compare_timestamp_against_node(#infinite_log_node{newest_timestamp = Newest}, ?BACKWARD, T) when T >= Newest -> newer.


%% @private
-spec index_in_node_of_timestamp(sentinel_record(), node_number(), node_record(), direction(), timestamp()) ->
    index_in_node().
index_in_node_of_timestamp(Sentinel, NodeNumber, Node, Direction, TargetTimestamp) ->
    EntriesLength = infinite_log_node:get_node_entries_length(Sentinel, NodeNumber),
    index_in_node_of_timestamp(Node, EntriesLength, Direction, TargetTimestamp).


%% @private
-spec index_in_node_of_timestamp(node_record(), non_neg_integer(), direction(), timestamp()) ->
    index_in_node().
index_in_node_of_timestamp(#infinite_log_node{entries = Entries}, EntriesLength, ?FORWARD, TargetTimestamp) ->
    lists_utils:foldl_while(fun({Timestamp, _}, Index) ->
        case TargetTimestamp > Timestamp of
            true -> {halt, Index};
            false -> {cont, Index - 1}
        end
    end, EntriesLength, Entries);
index_in_node_of_timestamp(#infinite_log_node{entries = Entries}, EntriesLength, ?BACKWARD, TargetTimestamp) ->
    lists_utils:foldl_while(fun({Timestamp, _}, Index) ->
        case TargetTimestamp >= Timestamp of
            true -> {halt, Index - 1};
            false -> {cont, Index - 1}
        end
    end, EntriesLength, Entries).


%% @private
-spec entry_index_older_than_first_in_node(sentinel_record(), node_number(), direction()) ->
    entry_index().
entry_index_older_than_first_in_node(Sentinel, NodeNumber, Direction) ->
    FirstEntryIndex = index_in_node_to_entry_index(Sentinel, NodeNumber, 0),
    case Direction of
        ?FORWARD -> FirstEntryIndex;
        ?BACKWARD -> FirstEntryIndex - 1
    end.


%% @private
-spec entry_index_newer_than_newest_in_node(sentinel_record(), node_number(), direction()) ->
    entry_index().
entry_index_newer_than_newest_in_node(Sentinel, NodeNumber, Direction) ->
    NewestIndex = index_in_node_to_entry_index(Sentinel, NodeNumber + 1, 0) - 1,
    case Direction of
        ?FORWARD -> NewestIndex;
        ?BACKWARD -> NewestIndex + 1
    end.


%% @private
-spec index_in_node_to_entry_index(sentinel_record(), node_number(), index_in_node()) ->
    entry_index().
index_in_node_to_entry_index(Sentinel, NewestNodeNum, Index) ->
    NewestNodeNum * Sentinel#infinite_log_sentinel.max_entries_per_node + Index.


%% @private
-spec snap_to_range(integer(), {integer(), integer() | infinity}) -> integer().
snap_to_range(Int, {From, infinity}) ->
    max(From, Int);
snap_to_range(Int, {From, To}) ->
    max(From, min(To, Int)).
