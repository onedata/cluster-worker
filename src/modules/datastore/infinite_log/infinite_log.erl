%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements a datastore internal structure that stores an
%%% infinite log - an append-only list of logs (with arbitrary text content)
%%% with monotonic timestamps. Apart from timestamp, each entry is assigned a
%%% consecutive index, starting from zero (first, oldest log). The API offers
%%% listing the log entries, starting from requested offsets, entry indices or
%%% timestamps.
%%%
%%% Internally, the log is divided into nodes, stored in separate documents,
%%% according to the requested max_entries_per_node parameter. The nodes do not
%%% need any relations between each other, as they are addressed using offsets
%%% in the log and deterministically generated documents ids.
%%% Consider a log with Id "ab", its documents would look like the following:
%%%
%%%  Id: ab x 0   Id: ab x 1       Id: ab x n      Id: ab
%%%  +--------+   +--------+       +--------+   +----------+
%%%  |        |   |        |       |        |   | sentinel |
%%%  |  node  |   |  node  |  ...  |  node  |   |+--------+|
%%%  |   0    |   |   1    |       |   n    |   ||        ||
%%%  +--------+   +--------+       +--------+   ||  node  ||
%%%                                             ||  n + 1 ||
%%%                                             |+--------+|
%%%                                             +----------+
%%%
%%% The newest node is stored in the sentinel for performance reasons. When it
%%% gets full, it is saved as a new archival node with number (n + 1) and no
%%% longer modified - only read when listing is performed.
%%%
%%% Entries are stored in lists and new ones are always prepended, which means
%%% that the order of entries in a node is descending index-wise.
%%% @end
%%%-------------------------------------------------------------------
-module(infinite_log).
-author("Lukasz Opiola").

-include("modules/datastore/infinite_log.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/2, destroy/1]).
-export([append/2]).
-export([list/2]).

% unit of timestamps used across the module for stamping entries and searching
-type timestamp() :: time:millis().

% id of an infinite log instance as stored in database
-type log_id() :: binary().
-type sentinel() :: #sentinel{}.
% id of individual node in an infinite log as stored in database
-type node_id() :: binary().
% nodes are numbered from 0 (oldest entries), and the newest node is always
% stored inside the sentinel
-type node_number() :: non_neg_integer().
-type log_node() :: #node{}.
% content of a log entry, must be a text (suitable for JSON),
% if needed may encode some arbitrary structures as a JSON or base64
-type content() :: binary().
% single entry in the log, numbered from 0 (oldest entry)
-type entry_index() :: non_neg_integer().
-type entry() :: {timestamp(), content()}.
% index of an entry within a node, starting from 0
-type index_in_node() :: non_neg_integer().

-export_type([log_id/0, entry/0, log_node/0, timestamp/0]).

% listing options
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
-type progress_marker() :: more | done.  % indicates if listing has ended
-type listing_result() :: {progress_marker(), listing_batch()}.

-record(listing_state, {
    direction :: direction(),
    start_index :: entry_index(), % inclusive
    end_index :: entry_index(),   % inclusive
    acc :: listing_batch()
}).
-type listing_state() :: #listing_state{}.

-define(MAX_LISTING_BATCH, 1000).
% macros used to determine safe log content size
% couchbase sets the document size limit at 20MB, assume 19MB as safe
-define(SAFE_NODE_DB_SIZE, 19000000).
-define(APPROXIMATE_EMPTY_ENTRY_DB_SIZE, 500).

%%=====================================================================
%% API
%%=====================================================================

-spec create(log_id(), pos_integer()) -> ok | {error, term()}.
create(LogId, MaxEntriesPerNode) ->
    infinite_log_persistence:save_record(LogId, #sentinel{
        log_id = LogId,
        max_entries_per_node = MaxEntriesPerNode
    }).


-spec destroy(log_id()) -> ok | {error, term()}.
destroy(LogId) ->
    case infinite_log_persistence:get_record(LogId) of
        {ok, Sentinel} ->
            delete_log_nodes(Sentinel),
            infinite_log_persistence:delete_record(LogId);
        {error, not_found} ->
            ok
    end.


-spec append(log_id() | sentinel(), content()) -> ok | {error, term()}.
append(LogId, Content) when is_binary(LogId) ->
    case infinite_log_persistence:get_record(LogId) of
        {error, _} = GetError ->
            GetError;
        {ok, Sentinel} ->
            case sanitize_append_request(Sentinel, Content) of
                {error, _} = SanitizeError ->
                    SanitizeError;
                ok ->
                    append(Sentinel, Content)
            end
    end;
append(Sentinel = #sentinel{log_id = LogId, entry_count = EntryCount, oldest_timestamp = OldestTimestamp}, Content) ->
    case transfer_entries_to_new_node_upon_full_buffer(Sentinel) of
        {error, _} = Error ->
            Error;
        {ok, UpdatedSentinel = #sentinel{buffer = Buffer}} ->
            Timestamp = current_timestamp(UpdatedSentinel),
            FinalSentinel = UpdatedSentinel#sentinel{
                entry_count = EntryCount + 1,
                buffer = append_entry_to_node(Buffer, {Timestamp, Content}),
                oldest_timestamp = case EntryCount of
                    0 -> Timestamp;
                    _ -> OldestTimestamp
                end,
                newest_timestamp = Timestamp
            },
            infinite_log_persistence:save_record(LogId, FinalSentinel)
    end.


-spec list(log_id(), listing_opts()) -> {ok, listing_result()} | {error, term()}.
list(LogId, Opts) ->
    case infinite_log_persistence:get_record(LogId) of
        {error, _} = GetError ->
            GetError;
        {ok, Sentinel = #sentinel{entry_count = EntryCount}} ->
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

            ListingAcc = list_internal(Sentinel, #listing_state{
                direction = Direction,
                start_index = StartIndex,
                end_index = EndIndex,
                acc = []
            }),
            {ok, {gen_progress_marker(Sentinel, Direction, EndIndex), ListingAcc}}
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec list_internal(sentinel(), listing_state()) -> listing_batch().
list_internal(_, #listing_state{direction = ?FORWARD, start_index = From, end_index = To, acc = Acc}) when From > To ->
    Acc;
list_internal(_, #listing_state{direction = ?BACKWARD, start_index = From, end_index = To, acc = Acc}) when From < To ->
    Acc;
list_internal(#sentinel{entry_count = Count}, #listing_state{start_index = StartIdx, acc = Acc}) when StartIdx >= Count ->
    Acc;
list_internal(Sentinel, #listing_state{direction = Direction, start_index = StartIdx, end_index = EndIdx, acc = Acc} = State) ->
    MaxEntriesPerNode = Sentinel#sentinel.max_entries_per_node,
    RemainingEntryCount = abs(EndIdx - StartIdx) + 1,

    % @fixme variable names
    CurrentNodeNumber = entry_index_to_node_number(Sentinel, StartIdx),

    LocalStartIdx = StartIdx rem MaxEntriesPerNode,
    LocalLimit = min(RemainingEntryCount, case Direction of
        ?FORWARD -> MaxEntriesPerNode - LocalStartIdx;
        ?BACKWARD -> LocalStartIdx + 1
    end),

    UnnumberedEntries = extract_entries(Sentinel, Direction, CurrentNodeNumber, LocalStartIdx, LocalLimit),
    BatchEndIdx = calc_end_index(Direction, StartIdx, LocalLimit),
    Batch = assign_entry_indices(Direction, StartIdx, BatchEndIdx, UnnumberedEntries),

    list_internal(Sentinel, State#listing_state{
        start_index = calc_end_index(Direction, StartIdx, LocalLimit + 1),
        acc = Acc ++ Batch
    }).


%% @private
-spec extract_entries(sentinel(), direction(), node_number(), index_in_node(), limit()) -> [entry()].
extract_entries(Sentinel, Direction, NodeNumber, StartIndexInNode, Limit) ->
    #node{entries = Entries} = get_node_by_number(Sentinel, NodeNumber),
    EntriesLength = get_node_entries_length(Sentinel, NodeNumber),
    case Direction of
        ?FORWARD ->
            EndIndexInNode = calc_end_index(Direction, StartIndexInNode, Limit),
            ReversedStartIndex = EntriesLength - 1 - EndIndexInNode,
            lists:reverse(lists:sublist(Entries, ReversedStartIndex + 1, Limit));
        ?BACKWARD ->
            lists:sublist(Entries, EntriesLength - StartIndexInNode, Limit)
    end.


%% @private
-spec assign_entry_indices(direction(), entry_index(), entry_index(), [entry()]) -> listing_batch().
assign_entry_indices(?FORWARD, StartIndex, EndIndex, Entries) ->
    lists:zip(lists:seq(StartIndex, EndIndex), Entries);
assign_entry_indices(?BACKWARD, StartIndex, EndIndex, Entries) ->
    lists:zip(lists:seq(StartIndex, EndIndex, -1), Entries).


%% @private
-spec calc_end_index(direction(), entry_index(), limit()) -> entry_index().
calc_end_index(?FORWARD, StartIndex, Limit) ->
    StartIndex + Limit - 1;
calc_end_index(?BACKWARD, StartIndex, Limit) ->
    StartIndex - Limit + 1.


%% @private
-spec gen_progress_marker(sentinel(), direction(), entry_index()) -> progress_marker().
gen_progress_marker(#sentinel{entry_count = EntryCount}, ?FORWARD, EndIndex) when EndIndex >= EntryCount - 1 ->
    done;
gen_progress_marker(_, ?FORWARD, _) ->
    more;
gen_progress_marker(_, ?BACKWARD, EndIndex) when EndIndex =< 0 ->
    done;
gen_progress_marker(_, ?BACKWARD, _) ->
    more.


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
-spec start_from_param_to_offset(sentinel(), direction(), start_from()) -> entry_index().
start_from_param_to_offset(_Sentinel, _, undefined) ->
    0;
start_from_param_to_offset(#sentinel{entry_count = EntryCount}, ?FORWARD, {index, EntryIndex}) ->
    snap_to_range(EntryIndex, {0, EntryCount});
start_from_param_to_offset(#sentinel{entry_count = EntryCount}, ?BACKWARD, {index, EntryIndex}) ->
    snap_to_range(EntryCount - 1 - EntryIndex, {0, EntryCount});
start_from_param_to_offset(Sentinel, Direction, {timestamp, Timestamp}) ->
    EntryIndex = find_entry_by_timestamp(
        Sentinel, Direction, Timestamp,
        0, Sentinel#sentinel.oldest_timestamp,
        Sentinel#sentinel.entry_count - 1, Sentinel#sentinel.newest_timestamp
    ),
    start_from_param_to_offset(Sentinel, Direction, {index, EntryIndex}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Uses a heuristic approach to find requested timestamp in a range of entries.
%% Knowing the latest entry and its timestamp as well as oldest entry and timestamp
%% in the range, approximates the target index assuming that the timestamps are
%% growing linearly. This yields two separate ranges, and the algorithm is called
%% recursively for the range which includes the timestamp.
%% Depending on the direction, the procedure finds:
%%    ?FORWARD -> first entry with timestamp that is not lower than the target timestamp,
%%    ?BACKWARD -> last entry with timestamp that is not higher than the target timestamp.
%% @end
%%--------------------------------------------------------------------
-spec find_entry_by_timestamp(sentinel(), direction(), timestamp(), timestamp(), entry_index(), timestamp(), entry_index()) ->
    entry_index().
find_entry_by_timestamp(_, ?FORWARD, TStamp, OldestIndex, OldestTStamp, _, _) when TStamp =< OldestTStamp ->
    OldestIndex;
find_entry_by_timestamp(_, ?FORWARD, TStamp, _, _, NewestIndex, NewestTStamp) when TStamp > NewestTStamp ->
    NewestIndex + 1;
find_entry_by_timestamp(_, ?BACKWARD, TStamp, _, _, NewestIndex, NewestTStamp) when TStamp >= NewestTStamp ->
    NewestIndex;
find_entry_by_timestamp(_, ?BACKWARD, TStamp, OldestIndex, OldestTStamp, _, _) when TStamp < OldestTStamp ->
    OldestIndex - 1;
find_entry_by_timestamp(Sentinel, Direction, TStamp, OldestIndex, OldestTStamp, NewestIndex, NewestTStamp) ->
    OldestNodeNum = entry_index_to_node_number(Sentinel, OldestIndex),
    NewestNodeNum = entry_index_to_node_number(Sentinel, NewestIndex),
    case NewestNodeNum - OldestNodeNum of
        0 ->
            % the considered range is included in one node
            OnlyNode = get_node_by_number(Sentinel, NewestNodeNum),
            IndexInNode = index_in_node_of_timestamp(OnlyNode, Direction, TStamp),
            index_in_node_to_entry_index(Sentinel, NewestNodeNum, IndexInNode);
        1 ->
            % the considered range covers two adjacent nodes
            OldestNode = get_node_by_number(Sentinel, OldestNodeNum),
            case locate_timestamp_in_node(OldestNode, Direction, TStamp) of
                older ->
                    OldestIndex;
                {included, IndexInNode} ->
                    index_in_node_to_entry_index(Sentinel, OldestNodeNum, IndexInNode);
                newer ->
                    NewestNode = get_node_by_number(Sentinel, NewestNodeNum),
                    case locate_timestamp_in_node(NewestNode, Direction, TStamp) of
                        older ->
                            FirstEntryIndex = index_in_node_to_entry_index(Sentinel, NewestNodeNum, 0),
                            case Direction of
                                ?FORWARD -> FirstEntryIndex;
                                ?BACKWARD -> FirstEntryIndex - 1
                            end;
                        {included, IndexInNode} ->
                            index_in_node_to_entry_index(Sentinel, NewestNodeNum, IndexInNode);
                        newer ->
                            case Direction of
                                ?FORWARD -> NewestIndex;
                                ?BACKWARD -> NewestIndex + 1
                            end
                    end
            end;
        _ ->
            % the considered range covers more than two nodes - approximate target
            % entry location assuming linearly growing timestamps to divide the problem
            PivotIndex = OldestIndex + round(
                (NewestIndex - OldestIndex) * (TStamp - OldestTStamp) / (NewestTStamp - OldestTStamp)
            ),
            PivotNodeNum = snap_to_range(
                entry_index_to_node_number(Sentinel, PivotIndex),
                {OldestNodeNum + 1, NewestNodeNum - 1}
            ),
            PivotNode = get_node_by_number(Sentinel, PivotNodeNum),
            case locate_timestamp_in_node(PivotNode, Direction, TStamp) of
                older ->
                    PivotOldestIndex = index_in_node_to_entry_index(Sentinel, PivotNodeNum, 0),
                    PivotOldestTStamp = PivotNode#node.oldest_timestamp,
                    find_entry_by_timestamp(
                        Sentinel, Direction, TStamp, OldestIndex, OldestTStamp, PivotOldestIndex, PivotOldestTStamp
                    );
                {included, IndexInNode} ->
                    index_in_node_to_entry_index(Sentinel, PivotNodeNum, IndexInNode);
                newer ->
                    PivotNewestIndex = index_in_node_to_entry_index(Sentinel, PivotNodeNum + 1, 0) - 1,
                    PivotNewestTStamp = PivotNode#node.newest_timestamp,
                    find_entry_by_timestamp(
                        Sentinel, Direction, TStamp, PivotNewestIndex, PivotNewestTStamp, NewestIndex, NewestTStamp
                    )
            end
    end.


%% @private
-spec locate_timestamp_in_node(log_node(), direction(), timestamp()) ->
    older | {included, index_in_node()} | newer.
locate_timestamp_in_node(Node, Direction, TargetTimestamp) ->
    case compare_timestamp_against_node(Node, Direction, TargetTimestamp) of
        older -> older;
        included -> {included, index_in_node_of_timestamp(Node, Direction, TargetTimestamp)};
        newer -> newer
    end.


%% @private
-spec compare_timestamp_against_node(log_node(), direction(), timestamp()) -> older | included | newer.
compare_timestamp_against_node(#node{oldest_timestamp = Oldest}, ?FORWARD, T) when T =< Oldest -> older;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, ?FORWARD, T) when T =< Newest -> included;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, ?FORWARD, T) when T > Newest -> newer;
compare_timestamp_against_node(#node{oldest_timestamp = Oldest}, ?BACKWARD, T) when T < Oldest -> older;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, ?BACKWARD, T) when T < Newest -> included;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, ?BACKWARD, T) when T >= Newest -> newer.


%% @private
-spec index_in_node_of_timestamp(log_node(), direction(), timestamp()) -> index_in_node().
index_in_node_of_timestamp(#node{entries = Entries}, ?FORWARD, TargetTimestamp) ->
    lists_utils:foldl_while(fun({Timestamp, _}, Index) ->
        case TargetTimestamp > Timestamp of
            true -> {halt, Index};
            false -> {cont, Index - 1}
        end
    end, length(Entries), Entries); % @fixme can we use get_node_entries_length here
index_in_node_of_timestamp(#node{entries = Entries}, ?BACKWARD, TargetTimestamp) ->
    lists_utils:foldl_while(fun({Timestamp, _}, Index) ->
        case TargetTimestamp >= Timestamp of
            true -> {halt, Index - 1};
            false -> {cont, Index - 1}
        end
    end, length(Entries), Entries).


%% @private
-spec index_in_node_to_entry_index(sentinel(), node_number(), index_in_node()) -> entry_index().
index_in_node_to_entry_index(Sentinel, NewestNodeNum, Index) ->
    NewestNodeNum * Sentinel#sentinel.max_entries_per_node + Index.


%% @private
-spec append_entry_to_node(log_node(), entry()) -> log_node().
append_entry_to_node(Node = #node{entries = Entries, oldest_timestamp = OldestTimestamp}, Entry = {Timestamp, _}) ->
    Node#node{
        entries = [Entry | Entries],
        newest_timestamp = Timestamp,
        oldest_timestamp = case Entries of
            [] -> Timestamp;
            _ -> OldestTimestamp
        end
    }.


%% @private
-spec transfer_entries_to_new_node_upon_full_buffer(sentinel()) -> ok | {error, term()}.
transfer_entries_to_new_node_upon_full_buffer(Sentinel = #sentinel{max_entries_per_node = MaxEntriesPerNode}) ->
    case get_node_entries_length(Sentinel, latest_node_number(Sentinel)) of
        MaxEntriesPerNode ->
            case save_buffer_as_new_node(Sentinel) of
                ok ->
                    {ok, Sentinel#sentinel{buffer = #node{}}};
                {error, _} = Error ->
                    Error
            end;
        _ ->
            {ok, Sentinel}
    end.


%% @private
-spec save_buffer_as_new_node(sentinel()) -> ok | {error, term()}.
save_buffer_as_new_node(Sentinel = #sentinel{buffer = Buffer}) ->
    NodeId = build_node_id(Sentinel, latest_node_number(Sentinel)),
    infinite_log_persistence:save_record(NodeId, Buffer).


%% @private
-spec delete_log_nodes(sentinel()) -> ok | {error, term()}.
delete_log_nodes(Sentinel) ->
    BufferNodeNumber = latest_node_number(Sentinel),
    % the newest node (buffer) is included in the sentinel - no need to delete it
    NodeNumbersToDelete = lists:seq(0, BufferNodeNumber - 1),
    lists_utils:foldl_while(fun(NodeNumber, _) ->
        case infinite_log_persistence:delete_record(build_node_id(Sentinel, NodeNumber)) of
            ok ->
                {cont, ok};
            {error, _} = Error ->
                {halt, Error}
        end
    end, ok, NodeNumbersToDelete).


%% @private
-spec build_node_id(sentinel(), node_number()) -> node_id().
build_node_id(Sentinel, NodeNumber) ->
    datastore_key:build_adjacent(integer_to_binary(NodeNumber), Sentinel#sentinel.log_id).


%% @private
-spec get_node_by_number(sentinel(), node_number()) -> log_node().
get_node_by_number(Sentinel, NodeNumber) ->
    case latest_node_number(Sentinel) of
        NodeNumber ->
            Sentinel#sentinel.buffer;
        _ ->
            {ok, Node} = infinite_log_persistence:get_record(build_node_id(Sentinel, NodeNumber)),
            Node
    end.


%% @private
%% Entries length can be easily calculated to avoid calling the length/1 function.
-spec get_node_entries_length(sentinel(), node_number()) -> node_number().
get_node_entries_length(Sentinel = #sentinel{max_entries_per_node = MaxEntriesPerNode}, NodeNumber) ->
    case latest_node_number(Sentinel) of
        NodeNumber ->
            Sentinel#sentinel.entry_count - (NodeNumber * MaxEntriesPerNode);
        _ ->
            MaxEntriesPerNode
    end.


%% @private
-spec latest_node_number(sentinel()) -> node_number().
latest_node_number(Sentinel = #sentinel{entry_count = EntryCount}) ->
    entry_index_to_node_number(Sentinel, EntryCount - 1).


%% @private
-spec entry_index_to_node_number(sentinel(), entry_index()) -> node_number().
entry_index_to_node_number(#sentinel{max_entries_per_node = MaxEntriesPerNode}, EntryIndex) ->
    EntryIndex div MaxEntriesPerNode.


%% @private
-spec current_timestamp(sentinel()) -> timestamp().
current_timestamp(#sentinel{newest_timestamp = NewestTimestamp}) ->
    global_clock:monotonic_timestamp_millis(NewestTimestamp).


%% @private
-spec snap_to_range(integer(), {integer(), integer() | infinity}) -> integer().
snap_to_range(Int, {From, infinity}) ->
    max(From, Int);
snap_to_range(Int, {From, To}) ->
    max(From, min(To, Int)).


%% @private
-spec sanitize_append_request(sentinel(), content()) -> {ok, sentinel()} | {error, term()}.
sanitize_append_request(Sentinel, Content) ->
    case byte_size(Content) > safe_log_content_size(Sentinel) of
        true -> {error, log_content_too_large};
        false -> ok
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% The limit is calculated not to exceed max couchbase document size, assuming
%% all logs are of maximum length and adding some safe margins for json encoding.
%% This averaged maximum value is used as the limit for all entries in the log.
%% @end
%%--------------------------------------------------------------------
-spec safe_log_content_size(sentinel()) -> integer().
safe_log_content_size(#sentinel{max_entries_per_node = MaxEntriesPerNode}) ->
    ?SAFE_NODE_DB_SIZE div MaxEntriesPerNode - ?APPROXIMATE_EMPTY_ENTRY_DB_SIZE.
