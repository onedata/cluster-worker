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
-type start_from() :: undefined | {index, infinite_log:entry_index()} | {timestamp, infinite_log:timestamp()}.
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

-type listing_batch() :: [{infinite_log:entry_index(), infinite_log:entry()}].
% indicates if listing has ended
-type progress_marker() :: more | done.
-type listing_result() :: {progress_marker(), listing_batch()}.
-export_type([listing_opts/0, listing_result/0]).

-record(listing_state, {
    sentinel :: infinite_log_sentinel:record(),
    direction :: direction(),
    start_index :: infinite_log:entry_index(), % inclusive
    end_index :: infinite_log:entry_index(),   % inclusive
    acc :: listing_batch()
}).
-type listing_state() :: #listing_state{}.

% index of an entry within a node, starting from 0
-type index_in_node() :: non_neg_integer().

-define(MAX_LISTING_BATCH, 1000).

%%=====================================================================
%% API
%%=====================================================================

-spec list(infinite_log_sentinel:record(), listing_opts()) -> listing_result().
list(Sentinel = #sentinel{entry_count = EntryCount}, Opts) ->
    Direction = maps:get(direction, Opts, forward_from_newest),
    StartFrom = maps:get(start_from, Opts, undefined),
    OffsetFromStartPoint = maps:get(offset, Opts, 0),
    Limit = maps:get(limit, Opts, ?MAX_LISTING_BATCH),

    StartPoint = start_from_param_to_offset(Sentinel, Direction, StartFrom),
    EffectiveOffset = snap_to_range(StartPoint + OffsetFromStartPoint, {0, infinity}),

    StartIndex = case Direction of
        ?FORWARD -> EffectiveOffset;
        ?BACKWARD -> EntryCount - 1 - EffectiveOffset
    end,

    EffectiveLimit = snap_to_range(Limit, {1, ?MAX_LISTING_BATCH}),
    UnboundedEndIndex = calc_end_index(Direction, StartIndex, EffectiveLimit),
    EndIndex = case Direction of
        ?FORWARD -> min(UnboundedEndIndex, EntryCount - 1);
        ?BACKWARD -> max(UnboundedEndIndex, 0)
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
list(#listing_state{sentinel = Sentinel, start_index = From, acc = Acc}) when From >= Sentinel#sentinel.entry_count ->
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
-spec extract_entries(
    infinite_log_sentinel:record(),
    direction(),
    infinite_log_node:node_number(),
    index_in_node(),
    limit()
) ->
    [infinite_log:entry()].
extract_entries(Sentinel, Direction, NodeNumber, StartIndexInNode, Limit) ->
    #node{entries = Entries} = infinite_log_sentinel:get_node_by_number(Sentinel, NodeNumber),
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
-spec assign_entry_indices(
    direction(),
    [infinite_log:entry()],
    infinite_log:entry_index(),
    infinite_log:entry_index()
) -> listing_batch().
assign_entry_indices(?FORWARD, Entries, StartIndex, EndIndex) ->
    lists:zip(lists:seq(StartIndex, EndIndex), Entries);
assign_entry_indices(?BACKWARD, Entries, StartIndex, EndIndex) ->
    lists:zip(lists:seq(StartIndex, EndIndex, -1), Entries).


%% @private
-spec gen_progress_marker(infinite_log_sentinel:record(), direction(), infinite_log:entry_index()) ->
    progress_marker().
gen_progress_marker(#sentinel{entry_count = EntryCount}, ?FORWARD, EndIndex) when EndIndex >= EntryCount - 1 ->
    done;
gen_progress_marker(_, ?FORWARD, _) ->
    more;
gen_progress_marker(_, ?BACKWARD, EndIndex) when EndIndex =< 0 ->
    done;
gen_progress_marker(_, ?BACKWARD, _) ->
    more.


%% @private
-spec calc_end_index(direction(), infinite_log:entry_index(), limit()) -> infinite_log:entry_index().
calc_end_index(?FORWARD, StartIndex, Limit) ->
    StartIndex + Limit - 1;
calc_end_index(?BACKWARD, StartIndex, Limit) ->
    StartIndex - Limit + 1.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Transform the start_from() parameter to the corresponding starting offset,
%% relevant for given direction (so that the zero offset is the oldest entry
%% for ?FORWARD direction and the latest entry for ?BACKWARD direction).
%% The returned starting offset can still be adjusted using the offset listing opt.
%% Every offset lower than 0 is rounded up to 0.
%% Every offset exceeding the log length is rounded down to the next index after the last element.
%% @end
%%--------------------------------------------------------------------
-spec start_from_param_to_offset(infinite_log_sentinel:record(), direction(), start_from()) ->
    infinite_log:entry_index().
start_from_param_to_offset(_Sentinel, _, undefined) ->
    0;
start_from_param_to_offset(#sentinel{entry_count = EntryCount}, ?FORWARD, {index, EntryIndex}) ->
    snap_to_range(EntryIndex, {0, EntryCount});
start_from_param_to_offset(#sentinel{entry_count = EntryCount}, ?BACKWARD, {index, EntryIndex}) ->
    snap_to_range(EntryCount - 1 - EntryIndex, {0, EntryCount});
start_from_param_to_offset(Sentinel, Direction, {timestamp, Timestamp}) ->
    EntryIndex = locate_entry(
        Sentinel, Direction, Timestamp,
        0, Sentinel#sentinel.oldest_timestamp,
        Sentinel#sentinel.entry_count - 1, Sentinel#sentinel.newest_timestamp
    ),
    start_from_param_to_offset(Sentinel, Direction, {index, EntryIndex}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Uses a heuristic approach to find an entry with requested timestamp in a
%% range of entries (or the closest one relevant for given direction).
%% Knowing the latest entry and its timestamp as well as oldest entry and timestamp
%% in the range, approximates the target index assuming that the timestamps are
%% growing linearly. This yields two separate ranges, and the algorithm is called
%% recursively for the range which includes the timestamp, until the search is
%% narrowed down to one or two adjacent nodes.
%% Depending on the direction, the procedure finds:
%%    ?FORWARD -> first entry with timestamp that is not lower than the target timestamp,
%%    ?BACKWARD -> last entry with timestamp that is not higher than the target timestamp.
%% @end
%%--------------------------------------------------------------------
-spec locate_entry(
    infinite_log_sentinel:record(), direction(), infinite_log:timestamp(),
    infinite_log:entry_index(), infinite_log:timestamp(),
    infinite_log:entry_index(), infinite_log:timestamp()
) ->
    infinite_log:entry_index().
locate_entry(_, ?FORWARD, TStamp, OldestIndex, OldestTStamp, _, _) when TStamp =< OldestTStamp ->
    OldestIndex;
locate_entry(_, ?FORWARD, TStamp, _, _, NewestIndex, NewestTStamp) when TStamp > NewestTStamp ->
    NewestIndex + 1;
locate_entry(_, ?BACKWARD, TStamp, _, _, NewestIndex, NewestTStamp) when TStamp >= NewestTStamp ->
    NewestIndex;
locate_entry(_, ?BACKWARD, TStamp, OldestIndex, OldestTStamp, _, _) when TStamp < OldestTStamp ->
    OldestIndex - 1;
locate_entry(Sentinel, Direction, TStamp, OldestIndex, OldestTStamp, NewestIndex, NewestTStamp) ->
    OldestNodeNum = infinite_log_node:entry_index_to_node_number(Sentinel, OldestIndex),
    NewestNodeNum = infinite_log_node:entry_index_to_node_number(Sentinel, NewestIndex),
    case NewestNodeNum - OldestNodeNum of
        0 ->
            locate_entry_within_one_node(Sentinel, Direction, TStamp, OldestNodeNum);
        1 ->
            locate_entry_within_two_adjacent_nodes(Sentinel, Direction, TStamp, OldestNodeNum, NewestNodeNum);
        _ ->
            locate_entry_within_more_than_two_nodes(
                Sentinel, Direction, TStamp, OldestIndex, OldestTStamp, NewestIndex, NewestTStamp
            )
    end.


%% @private
-spec locate_entry_within_one_node(
    infinite_log_sentinel:record(),
    direction(),
    infinite_log:timestamp(),
    infinite_log_node:node_number()
) ->
    infinite_log:entry_index().
locate_entry_within_one_node(Sentinel, Direction, TStamp, NodeNumber) ->
    case locate_timestamp_in_node(Sentinel, NodeNumber, Direction, TStamp) of
        {older_than, _} ->
            entry_index_older_than_first_in_node(Sentinel, NodeNumber, Direction);
        {included, IndexInNode} ->
            index_in_node_to_entry_index(Sentinel, NodeNumber, IndexInNode);
        {newer_than, _} ->
            entry_index_newer_than_latest_in_node(Sentinel, NodeNumber, Direction)
    end.


%% @private
-spec locate_entry_within_two_adjacent_nodes(
    infinite_log_sentinel:record(),
    direction(),
    infinite_log:timestamp(),
    infinite_log_node:node_number(),
    infinite_log_node:node_number()
) ->
    infinite_log:entry_index().
locate_entry_within_two_adjacent_nodes(Sentinel, Direction, TStamp, FirstNodeNum, SecondNodeNum) ->
    case locate_timestamp_in_node(Sentinel, FirstNodeNum, Direction, TStamp) of
        {older_than, _} ->
            entry_index_older_than_first_in_node(Sentinel, FirstNodeNum, Direction);
        {included, IndexInNode} ->
            index_in_node_to_entry_index(Sentinel, FirstNodeNum, IndexInNode);
        {newer_than, _} ->
            locate_entry_within_one_node(Sentinel, Direction, TStamp, SecondNodeNum)
    end.


%% @private
-spec locate_entry_within_more_than_two_nodes(
    infinite_log_sentinel:record(),
    direction(),
    infinite_log:timestamp(),
    infinite_log:entry_index(), infinite_log:timestamp(),
    infinite_log:entry_index(), infinite_log:timestamp()
) ->
    infinite_log:entry_index().
locate_entry_within_more_than_two_nodes(
    Sentinel, Direction, TStamp, OldestIndex, OldestTStamp, NewestIndex, NewestTStamp
) ->
    OldestNodeNum = infinite_log_node:entry_index_to_node_number(Sentinel, OldestIndex),
    NewestNodeNum = infinite_log_node:entry_index_to_node_number(Sentinel, NewestIndex),
    PivotIndex = OldestIndex + round(
        (NewestIndex - OldestIndex) * (TStamp - OldestTStamp) / (NewestTStamp - OldestTStamp)
    ),
    PivotNodeNum = snap_to_range(
        infinite_log_node:entry_index_to_node_number(Sentinel, PivotIndex),
        {OldestNodeNum + 1, NewestNodeNum - 1}
    ),
    case locate_timestamp_in_node(Sentinel, PivotNodeNum, Direction, TStamp) of
        {older_than, PivotOldestTStamp} ->
            PivotOldestIndex = index_in_node_to_entry_index(Sentinel, PivotNodeNum, 0),
            locate_entry(
                Sentinel, Direction, TStamp, OldestIndex, OldestTStamp, PivotOldestIndex, PivotOldestTStamp
            );
        {included, IndexInNode} ->
            index_in_node_to_entry_index(Sentinel, PivotNodeNum, IndexInNode);
        {newer_than, PivotNewestTStamp} ->
            PivotNewestIndex = index_in_node_to_entry_index(Sentinel, PivotNodeNum + 1, 0) - 1,
            locate_entry(
                Sentinel, Direction, TStamp, PivotNewestIndex, PivotNewestTStamp, NewestIndex, NewestTStamp
            )
    end.


%% @private
-spec locate_timestamp_in_node(
    infinite_log_sentinel:record(),
    infinite_log_node:node_number(),
    direction(),
    infinite_log:timestamp()
) ->
    {older_than, infinite_log:timestamp()} | {included, index_in_node()} | {newer_than, infinite_log:timestamp()}.
locate_timestamp_in_node(Sentinel, NodeNumber, Direction, TargetTimestamp) ->
    Node = infinite_log_sentinel:get_node_by_number(Sentinel, NodeNumber),
    case compare_timestamp_against_node(Node, Direction, TargetTimestamp) of
        older ->
            {older_than, Node#node.oldest_timestamp};
        included ->
            {included, index_in_node_of_timestamp(Sentinel, NodeNumber, Node, Direction, TargetTimestamp)};
        newer ->
            {newer_than, Node#node.newest_timestamp}
    end.


%% @private
-spec compare_timestamp_against_node(infinite_log_node:record(), direction(), infinite_log:timestamp()) ->
    older | included | newer.
compare_timestamp_against_node(#node{oldest_timestamp = Oldest}, ?FORWARD, T) when T =< Oldest -> older;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, ?FORWARD, T) when T =< Newest -> included;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, ?FORWARD, T) when T > Newest -> newer;
compare_timestamp_against_node(#node{oldest_timestamp = Oldest}, ?BACKWARD, T) when T < Oldest -> older;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, ?BACKWARD, T) when T < Newest -> included;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, ?BACKWARD, T) when T >= Newest -> newer.


%% @private
-spec index_in_node_of_timestamp(
    infinite_log_sentinel:record(),
    infinite_log_node:node_number(),
    infinite_log_node:record(),
    direction(),
    infinite_log:timestamp()
) ->
    index_in_node().
index_in_node_of_timestamp(Sentinel, NodeNumber, Node, Direction, TargetTimestamp) ->
    EntriesLength = infinite_log_node:get_node_entries_length(Sentinel, NodeNumber),
    index_in_node_of_timestamp(Node, EntriesLength, Direction, TargetTimestamp).


%% @private
-spec index_in_node_of_timestamp(
    infinite_log_node:record(),
    non_neg_integer(),
    direction(),
    infinite_log:timestamp()
) ->
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
-spec entry_index_older_than_first_in_node(
    infinite_log_sentinel:record(),
    infinite_log_node:node_number(),
    direction()
) ->
    infinite_log:entry_index().
entry_index_older_than_first_in_node(Sentinel, NodeNumber, Direction) ->
    FirstEntryIndex = index_in_node_to_entry_index(Sentinel, NodeNumber, 0),
    case Direction of
        ?FORWARD -> FirstEntryIndex;
        ?BACKWARD -> FirstEntryIndex - 1
    end.


%% @private
-spec entry_index_newer_than_latest_in_node(
    infinite_log_sentinel:record(),
    infinite_log_node:node_number(),
    direction()
) ->
    infinite_log:entry_index().
entry_index_newer_than_latest_in_node(Sentinel, NodeNumber, Direction) ->
    NewestIndex = index_in_node_to_entry_index(Sentinel, NodeNumber + 1, 0) - 1,
    case Direction of
        ?FORWARD -> NewestIndex;
        ?BACKWARD -> NewestIndex + 1
    end.


%% @private
-spec index_in_node_to_entry_index(infinite_log_sentinel:record(), infinite_log_node:node_number(), index_in_node()) ->
    infinite_log:entry_index().
index_in_node_to_entry_index(Sentinel, NewestNodeNum, Index) ->
    NewestNodeNum * Sentinel#sentinel.max_entries_per_node + Index.


%% @private
-spec snap_to_range(integer(), {integer(), integer() | infinity}) -> integer().
snap_to_range(Int, {From, infinity}) ->
    max(From, Int);
snap_to_range(Int, {From, To}) ->
    max(From, min(To, Int)).
