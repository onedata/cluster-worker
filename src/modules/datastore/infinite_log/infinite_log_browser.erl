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
-export([list/2]).

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

-type listing_batch() :: [{entry_index(), entry()}].
% indicates if listing has ended
-type progress_marker() :: more | done.
-type listing_result() :: {progress_marker(), listing_batch()}.
-export_type([listing_opts/0, listing_result/0]).

%%%-------------------------------------------------------------------
%%% internal definitions
%%%-------------------------------------------------------------------
-record(listing_state, {
    sentinel :: sentinel_record(),
    direction :: direction(),
    start_index :: entry_index(), % inclusive
    end_index :: entry_index(),   % inclusive
    acc :: listing_batch()
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

-type timestamp() :: infinite_log:timestamp().
-type entry_index() :: infinite_log:entry_index().
-type entry() :: infinite_log:entry().
-type sentinel_record() :: infinite_log_sentinel:record().
-type node_record() :: infinite_log_node:record().
-type node_number() :: infinite_log_node:node_number().

%%=====================================================================
%% API
%%=====================================================================

-spec list(sentinel_record(), listing_opts()) -> listing_result().
list(Sentinel = #sentinel{total_entry_count = TotalEntryCount, oldest_entry_index = OldestIndex}, Opts) ->
    Direction = maps:get(direction, Opts, forward_from_newest),
    StartFrom = maps:get(start_from, Opts, undefined),
    OffsetFromStartPoint = maps:get(offset, Opts, 0),
    Limit = maps:get(limit, Opts, ?MAX_LISTING_BATCH),

    StartPoint = start_from_param_to_offset(Sentinel, Direction, StartFrom),
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

    ListingBatch = list(#listing_state{
        sentinel = Sentinel,
        direction = Direction,
        start_index = StartIndex,
        end_index = EndIndex,
        acc = []
    }),
    {gen_progress_marker(Sentinel, Direction, EndIndex), ListingBatch}.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec list(listing_state()) -> listing_batch().
list(#listing_state{direction = ?FORWARD, start_index = From, end_index = To, acc = Acc}) when From > To ->
    Acc;
list(#listing_state{direction = ?BACKWARD, start_index = From, end_index = To, acc = Acc}) when From < To ->
    Acc;
list(#listing_state{sentinel = Sentinel, start_index = From, acc = Acc}) when From >= Sentinel#sentinel.total_entry_count ->
    Acc;
list(#listing_state{sentinel = Sentinel, direction = Direction, start_index = StartIdx, end_index = EndIdx} = State) ->
    MaxEntriesPerNode = Sentinel#sentinel.max_entries_per_node,
    RemainingEntryCount = abs(EndIdx - StartIdx) + 1,

    CurrentNodeNum = infinite_log_node:entry_index_to_node_number(Sentinel, StartIdx),

    StartIdxInNode = StartIdx rem MaxEntriesPerNode,
    LimitInNode = min(RemainingEntryCount, case Direction of
        ?FORWARD -> MaxEntriesPerNode - StartIdxInNode;
        ?BACKWARD -> StartIdxInNode + 1
    end),

    Entries = extract_entries(Sentinel, Direction, CurrentNodeNum, StartIdxInNode, LimitInNode),
    BatchEndIdx = calc_end_index(Direction, StartIdx, LimitInNode),
    Batch = assign_entry_indices(Direction, Entries, StartIdx, BatchEndIdx),

    list(State#listing_state{
        start_index = calc_end_index(Direction, StartIdx, LimitInNode + 1),
        acc = State#listing_state.acc ++ Batch
    }).


%% @private
-spec extract_entries(sentinel_record(), direction(), node_number(), index_in_node(), limit()) ->
    [entry()].
extract_entries(Sentinel, Direction, NodeNumber, StartIndexInNode, Limit) ->
    {ok, #node{entries = Entries}} = infinite_log_sentinel:get_node_by_number(Sentinel, NodeNumber),
    EntriesLength = infinite_log_node:get_node_entries_length(Sentinel, NodeNumber),
    case Direction of
        ?FORWARD ->
            EndIndexInNode = calc_end_index(Direction, StartIndexInNode, Limit),
            ReversedStartIndex = EntriesLength - 1 - EndIndexInNode,
            lists:reverse(lists:sublist(Entries, ReversedStartIndex + 1, Limit));
        ?BACKWARD ->
            lists:sublist(Entries, EntriesLength - StartIndexInNode, Limit)
    end.


%% @private
-spec assign_entry_indices(direction(), [entry()], entry_index(), entry_index()) ->
    listing_batch().
assign_entry_indices(?FORWARD, Entries, StartIndex, EndIndex) ->
    lists:zip(lists:seq(StartIndex, EndIndex), Entries);
assign_entry_indices(?BACKWARD, Entries, StartIndex, EndIndex) ->
    lists:zip(lists:seq(StartIndex, EndIndex, -1), Entries).


%% @private
-spec gen_progress_marker(sentinel_record(), direction(), entry_index()) ->
    progress_marker().
gen_progress_marker(#sentinel{total_entry_count = EntryCount}, ?FORWARD, EndIndex) when EndIndex >= EntryCount - 1 ->
    done;
gen_progress_marker(_, ?FORWARD, _) ->
    more;
gen_progress_marker(#sentinel{oldest_entry_index = OldestIndex}, ?BACKWARD, EndIndex) when EndIndex =< OldestIndex ->
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
-spec start_from_param_to_offset(sentinel_record(), direction(), start_from()) ->
    entry_index().
start_from_param_to_offset(#sentinel{oldest_entry_index = OldestIndex}, ?FORWARD, undefined) ->
    OldestIndex;
start_from_param_to_offset(_Sentinel, ?BACKWARD, undefined) ->
    0;
start_from_param_to_offset(#sentinel{total_entry_count = EntryCount} = S, ?FORWARD, {index, EntryIdx}) ->
    snap_to_range(EntryIdx, {S#sentinel.oldest_entry_index, EntryCount});
start_from_param_to_offset(#sentinel{total_entry_count = EntryCount} = S, ?BACKWARD, {index, EntryIdx}) ->
    snap_to_range(EntryCount - 1 - EntryIdx, {0, EntryCount - S#sentinel.oldest_entry_index});
start_from_param_to_offset(Sentinel, Direction, {timestamp, Timestamp}) ->
    SS1 = #search_state{sentinel = Sentinel, direction = Direction, target_tstamp = Timestamp},
    SS2 = set_search_since(SS1, Sentinel#sentinel.oldest_entry_index, Sentinel#sentinel.oldest_timestamp),
    SS3 = set_search_until(SS2, Sentinel#sentinel.total_entry_count - 1, Sentinel#sentinel.newest_timestamp),
    EntryIndex = locate_timestamp(SS3),
    start_from_param_to_offset(Sentinel, Direction, {index, EntryIndex}).


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
-spec locate_timestamp(search_state()) -> entry_index().
locate_timestamp(SS = #search_state{direction = ?FORWARD, target_tstamp = T, oldest_tstamp = Oldest}) when T =< Oldest ->
    SS#search_state.oldest_index;
locate_timestamp(SS = #search_state{direction = ?FORWARD, target_tstamp = T, newest_tstamp = Newest}) when T > Newest ->
    SS#search_state.newest_index + 1;
locate_timestamp(SS = #search_state{direction = ?BACKWARD, target_tstamp = T, newest_tstamp = Newest}) when T >= Newest ->
    SS#search_state.newest_index;
locate_timestamp(SS = #search_state{direction = ?BACKWARD, target_tstamp = T, oldest_tstamp = Oldest}) when T < Oldest ->
    SS#search_state.oldest_index - 1;
locate_timestamp(SS = #search_state{oldest_node_number = OldestNodeNum, newest_node_number = NewestNodeNum}) ->
    case NewestNodeNum - OldestNodeNum of
        0 ->
            locate_timestamp_within_one_node(SS);
        1 ->
            locate_timestamp_within_two_adjacent_nodes(SS);
        _ ->
            locate_timestamp_within_more_than_two_nodes(SS)
    end.


%% @private
-spec locate_timestamp_within_one_node(search_state()) -> entry_index().
locate_timestamp_within_one_node(#search_state{
    sentinel = Sentinel, direction = Direction, target_tstamp = TStamp, oldest_node_number = NodeNumber
}) ->
    case lookup_timestamp_in_node(Sentinel, NodeNumber, Direction, TStamp) of
        {older_than, _} ->
            entry_index_older_than_first_in_node(Sentinel, NodeNumber, Direction);
        {included, IndexInNode} ->
            index_in_node_to_entry_index(Sentinel, NodeNumber, IndexInNode);
        {newer_than, _} ->
            entry_index_newer_than_newest_in_node(Sentinel, NodeNumber, Direction)
    end.


%% @private
-spec locate_timestamp_within_two_adjacent_nodes(search_state()) -> entry_index().
locate_timestamp_within_two_adjacent_nodes(SS = #search_state{
    sentinel = Sentinel, direction = Direction, target_tstamp = TStamp,
    oldest_node_number = FirstNodeNum, newest_node_number = SecondNodeNum
}) ->
    case lookup_timestamp_in_node(Sentinel, FirstNodeNum, Direction, TStamp) of
        {older_than, _} ->
            entry_index_older_than_first_in_node(Sentinel, FirstNodeNum, Direction);
        {included, IndexInNode} ->
            index_in_node_to_entry_index(Sentinel, FirstNodeNum, IndexInNode);
        {newer_than, _} ->
            locate_timestamp_within_one_node(SS#search_state{oldest_node_number = SecondNodeNum})
    end.


%% @private
-spec locate_timestamp_within_more_than_two_nodes(search_state()) -> entry_index().
locate_timestamp_within_more_than_two_nodes(SS = #search_state{
    sentinel = Sentinel, direction = Direction, target_tstamp = TStamp,
    oldest_node_number = OldestNodeNum, oldest_index = OldestIndex, oldest_tstamp = OldestTStamp,
    newest_node_number = NewestNodeNum, newest_index = NewestIndex, newest_tstamp = NewestTStamp
}) ->
    PivotIndex = OldestIndex + round(
        (NewestIndex - OldestIndex) * (TStamp - OldestTStamp) / (NewestTStamp - OldestTStamp)
    ),
    PivotNodeNum = snap_to_range(
        infinite_log_node:entry_index_to_node_number(Sentinel, PivotIndex),
        {OldestNodeNum + 1, NewestNodeNum - 1}
    ),
    case lookup_timestamp_in_node(Sentinel, PivotNodeNum, Direction, TStamp) of
        {older_than, PivotOldestTStamp} ->
            PivotOldestIndex = index_in_node_to_entry_index(Sentinel, PivotNodeNum, 0),
            locate_timestamp(set_search_until(SS, PivotOldestIndex, PivotOldestTStamp));
        {included, IndexInNode} ->
            index_in_node_to_entry_index(Sentinel, PivotNodeNum, IndexInNode);
        {newer_than, PivotNewestTStamp} ->
            PivotNewestIndex = index_in_node_to_entry_index(Sentinel, PivotNodeNum + 1, 0) - 1,
            locate_timestamp(set_search_since(SS, PivotNewestIndex, PivotNewestTStamp))
    end.


%% @private
-spec lookup_timestamp_in_node(sentinel_record(), node_number(), direction(), timestamp()) ->
    {older_than, timestamp()} | {included, index_in_node()} | {newer_than, timestamp()}.
lookup_timestamp_in_node(Sentinel, NodeNumber, Direction, TargetTimestamp) ->
    {ok, Node} = infinite_log_sentinel:get_node_by_number(Sentinel, NodeNumber),
    case compare_timestamp_against_node(Node, Direction, TargetTimestamp) of
        older ->
            {older_than, Node#node.oldest_timestamp};
        included ->
            {included, index_in_node_of_timestamp(Sentinel, NodeNumber, Node, Direction, TargetTimestamp)};
        newer ->
            {newer_than, Node#node.newest_timestamp}
    end.


%% @private
-spec compare_timestamp_against_node(node_record(), direction(), timestamp()) ->
    older | included | newer.
compare_timestamp_against_node(#node{oldest_timestamp = Oldest}, ?FORWARD, T) when T =< Oldest -> older;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, ?FORWARD, T) when T =< Newest -> included;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, ?FORWARD, T) when T > Newest -> newer;
compare_timestamp_against_node(#node{oldest_timestamp = Oldest}, ?BACKWARD, T) when T < Oldest -> older;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, ?BACKWARD, T) when T < Newest -> included;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, ?BACKWARD, T) when T >= Newest -> newer.


%% @private
-spec index_in_node_of_timestamp(sentinel_record(), node_number(), node_record(), direction(), timestamp()) ->
    index_in_node().
index_in_node_of_timestamp(Sentinel, NodeNumber, Node, Direction, TargetTimestamp) ->
    EntriesLength = infinite_log_node:get_node_entries_length(Sentinel, NodeNumber),
    index_in_node_of_timestamp(Node, EntriesLength, Direction, TargetTimestamp).


%% @private
-spec index_in_node_of_timestamp(node_record(), non_neg_integer(), direction(), timestamp()) ->
    index_in_node().
index_in_node_of_timestamp(#node{entries = Entries}, EntriesLength, ?FORWARD, TargetTimestamp) ->
    lists_utils:foldl_while(fun({Timestamp, _}, Index) ->
        case TargetTimestamp > Timestamp of
            true -> {halt, Index};
            false -> {cont, Index - 1}
        end
    end, EntriesLength, Entries);
index_in_node_of_timestamp(#node{entries = Entries}, EntriesLength, ?BACKWARD, TargetTimestamp) ->
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
    NewestNodeNum * Sentinel#sentinel.max_entries_per_node + Index.


%% @private
-spec snap_to_range(integer(), {integer(), integer() | infinity}) -> integer().
snap_to_range(Int, {From, infinity}) ->
    max(From, Int);
snap_to_range(Int, {From, To}) ->
    max(From, min(To, Int)).
