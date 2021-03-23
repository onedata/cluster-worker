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
%%% consecutive id, starting from zero (first, oldest log). The API offers
%%% listing the log entries, starting from requested offsets, entry ids or
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
%%%  |        |   |        |       |        |   ||        ||
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
%%% that the order of entries in a node is descending id-wise.
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
-type id() :: binary().
-type sentinel() :: #sentinel{}.
% id of individual node in an infinite log as stored in database
-type log_node_id() :: binary().
% nodes are numbered from 0 (oldest entries), and the newest node is always
% stored inside the sentinel
-type log_node_number() :: non_neg_integer().
-type log_node() :: #node{}.
% content of a log entry, must be a text (suitable for JSON),
% if needed may encode some arbitrary structures as a JSON or base64
-type content() :: binary().
% single entry in the log, numbered from 0 (oldest entry)
-type entry_id() :: non_neg_integer().
-type entry() :: {timestamp(), content()}.
% index of an entry within a node, starting from 0
-type index_in_node() :: non_neg_integer().

-export_type([id/0, entry/0, log_node/0, timestamp/0]).

% listing options
-type direction() :: forward_from_oldest | backward_from_newest.
-type start_from() :: undefined | {id, entry_id()}.
% offset from the starting point requested using start_from parameter, may be negative
% if the offset points to an entry before the beginning of the list, it is rounded up to the first index
% if the offset points to an entry after the end of the list, the returned result will always be empty
-type offset() :: integer().
-type limit() :: pos_integer().
-type listing_opts() :: #{direction => direction(), start_from => start_from(), offset => offset(), limit => limit()}.
-type listing_batch() :: [{entry_id(), entry()}].
-type progress_marker() :: more | done.  % indicates if listing has ended
-type listing_result() :: {progress_marker(), listing_batch()}.

-record(listing_state, {
    direction :: direction(),
    start_id :: entry_id(), % inclusive
    end_id :: entry_id(),   % inclusive
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

-spec create(id(), pos_integer()) -> ok | {error, term()}.
create(LogId, MaxEntriesPerNode) ->
    infinite_log_persistence:save_record(LogId, #sentinel{
        structure_id = LogId,
        max_entries_per_node = MaxEntriesPerNode
    }).


-spec destroy(id()) -> ok | {error, term()}.
destroy(LogId) ->
    case infinite_log_persistence:get_record(LogId) of
        {ok, Sentinel} ->
            delete_log_nodes(Sentinel),
            infinite_log_persistence:delete_record(Sentinel#sentinel.structure_id);
        {error, not_found} ->
            ok
    end.


-spec append(id(), content()) -> ok | {error, term()}.
append(LogId, Content) ->
    case sanitize_append_request(LogId, Content) of
        {ok, Sentinel = #sentinel{buffer = Buffer, entry_count = EntryCount, oldest_timestamp = OldestEntry}} ->
            Timestamp = current_timestamp(Sentinel),
            UpdatedSentinel = Sentinel#sentinel{
                entry_count = EntryCount + 1,
                buffer = append_entry_to_node(Buffer, {Timestamp, Content}),
                oldest_timestamp = case EntryCount of
                    0 -> Timestamp;
                    _ -> OldestEntry
                end,
                newest_timestamp = Timestamp
            },
            case transfer_entries_to_new_node_upon_full_buffer(UpdatedSentinel) of
                {error, _} = SaveError ->
                    SaveError;
                {ok, FinalSentinel} ->
                    infinite_log_persistence:save_record(LogId, FinalSentinel)
            end;
        {error, _} = Error ->
            Error
    end.


-spec list(id(), listing_opts()) -> {ok, listing_result()} | {error, term()}.
list(LogId, Opts) ->
    case infinite_log_persistence:get_record(LogId) of
        {error, _} = GetError ->
            GetError;
        {ok, Sentinel = #sentinel{entry_count = EntryCount}} ->
            Direction = maps:get(direction, Opts, forward_from_newest),
            StartFrom = maps:get(start_from, Opts, undefined),
            Offset = maps:get(offset, Opts, 0),
            Limit = maps:get(limit, Opts, ?MAX_LISTING_BATCH),

            StartingPoint = find_starting_point(Sentinel, Direction, StartFrom),
            EffectiveOffset = snap_to_range(StartingPoint + Offset, {0, infinity}),

            StartId = case Direction of
                forward_from_oldest -> EffectiveOffset;
                backward_from_newest -> EntryCount - 1 - EffectiveOffset
            end,

            EffectiveLimit = snap_to_range(Limit, {1, ?MAX_LISTING_BATCH}),
            UnboundedEndId = calc_end_id(Direction, StartId, EffectiveLimit),
            EndId = case Direction of
                forward_from_oldest -> min(UnboundedEndId, EntryCount - 1);
                backward_from_newest -> max(UnboundedEndId, 0)
            end,

            ListingAcc = list_internal(Sentinel, #listing_state{
                direction = Direction,
                start_id = StartId,
                end_id = EndId,
                acc = []
            }),
            {ok, {gen_progress_marker(Sentinel, Direction, EndId), ListingAcc}}
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec sanitize_append_request(id(), content()) -> {ok, sentinel()} | {error, term()}.
sanitize_append_request(LogId, Content) ->
    case infinite_log_persistence:get_record(LogId) of
        {error, _} = Error ->
            Error;
        {ok, Sentinel} ->
            case byte_size(Content) > safe_log_content_size(Sentinel) of
                true -> {error, log_content_too_large};
                false -> {ok, Sentinel}
            end
    end.


%% @private
-spec list_internal(sentinel(), listing_state()) -> listing_batch().
list_internal(_, #listing_state{direction = forward_from_oldest, start_id = From, end_id = To, acc = Acc}) when From > To ->
    Acc;
list_internal(_, #listing_state{direction = backward_from_newest, start_id = From, end_id = To, acc = Acc}) when From < To ->
    Acc;
list_internal(#sentinel{entry_count = Count}, #listing_state{start_id = StartId, acc = Acc}) when StartId >= Count ->
    Acc;
list_internal(Sentinel, #listing_state{direction = Direction, start_id = StartId, end_id = EndId, acc = Acc} = State) ->
    MaxEntriesPerNode = Sentinel#sentinel.max_entries_per_node,
    RemainingEntryCount = abs(EndId - StartId) + 1,

    LocalNodeNumber = entry_id_to_node_number(Sentinel, StartId),

    LocalStartId = StartId rem MaxEntriesPerNode,
    LocalLimit = min(RemainingEntryCount, case Direction of
        forward_from_oldest -> MaxEntriesPerNode - LocalStartId;
        backward_from_newest -> LocalStartId + 1
    end),

    UnnumberedEntries = extract_entries(Sentinel, Direction, LocalNodeNumber, LocalStartId, LocalLimit),
    BatchEndId = calc_end_id(Direction, StartId, LocalLimit),
    Batch = assign_entry_ids(Direction, StartId, BatchEndId, UnnumberedEntries),

    list_internal(Sentinel, State#listing_state{
        start_id = calc_end_id(Direction, StartId, LocalLimit + 1),
        acc = Acc ++ Batch
    }).


%% @private
-spec extract_entries(sentinel(), direction(), log_node_number(), index_in_node(), limit()) -> [entry()].
extract_entries(Sentinel, Direction, NodeNumber, StartIndexInNode, Limit) ->
    #node{entries = Entries} = get_node_by_number(Sentinel, NodeNumber),
    EntriesLength = get_node_entries_length(Sentinel, NodeNumber),
    case Direction of
        forward_from_oldest ->
            EndIndexInNode = calc_end_id(Direction, StartIndexInNode, Limit),
            ReversedStartId = EntriesLength - 1 - EndIndexInNode,
            lists:reverse(lists:sublist(Entries, ReversedStartId + 1, Limit));
        backward_from_newest ->
            lists:sublist(Entries, EntriesLength - StartIndexInNode, Limit)
    end.


%% @private
-spec assign_entry_ids(direction(), entry_id(), entry_id(), [entry()]) -> listing_batch().
assign_entry_ids(forward_from_oldest, StartId, EndId, Entries) ->
    lists:zip(lists:seq(StartId, EndId), Entries);
assign_entry_ids(backward_from_newest, StartId, EndId, Entries) ->
    lists:zip(lists:seq(StartId, EndId, -1), Entries).


%% @private
-spec calc_end_id(direction(), entry_id(), limit()) -> entry_id().
calc_end_id(forward_from_oldest, StartId, Limit) ->
    StartId + Limit - 1;
calc_end_id(backward_from_newest, StartId, Limit) ->
    StartId - Limit + 1.


%% @private
-spec gen_progress_marker(sentinel(), direction(), entry_id()) -> progress_marker().
gen_progress_marker(#sentinel{entry_count = EntryCount}, forward_from_oldest, EndId) when EndId >= EntryCount - 1 ->
    done;
gen_progress_marker(_, forward_from_oldest, _) ->
    more;
gen_progress_marker(_, backward_from_newest, EndId) when EndId =< 0 ->
    done;
gen_progress_marker(_, backward_from_newest, _) ->
    more.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Every index lower than the lowest is treated as the first element.
%% Every index exceeding the length is treated as the next id after the last element.
%% @end
%%--------------------------------------------------------------------
-spec find_starting_point(sentinel(), direction(), start_from()) -> entry_id().
find_starting_point(_Sentinel, _, undefined) ->
    0;
find_starting_point(#sentinel{entry_count = EntryCount}, forward_from_oldest, {id, EntryId}) ->
    snap_to_range(EntryId, {0, EntryCount});
find_starting_point(#sentinel{entry_count = EntryCount}, backward_from_newest, {id, EntryId}) ->
    snap_to_range(EntryCount - 1 - EntryId, {0, EntryCount});
find_starting_point(Sentinel, Direction, {timestamp, Timestamp}) ->
    EntryId = find_entry_by_timestamp(
        Sentinel, Direction, Timestamp,
        0, Sentinel#sentinel.oldest_timestamp,
        Sentinel#sentinel.entry_count - 1, Sentinel#sentinel.newest_timestamp
    ),
    find_starting_point(Sentinel, Direction, {id, EntryId}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Uses a heuristic approach to find requested timestamp in a range of entries.
%% Knowing the latest entry and its timestamp as well as oldest entry and timestamp
%% in the range, approximates the target id assuming that the timestamps are
%% growing linearly. This yields two separate ranges, and the algorithm is called
%% recursively for the range which includes the timestamp.
%% Depending on the direction, the procedure finds:
%%    forward_from_oldest -> first entry with timestamp that is not lower than the target timestamp,
%%    backward_from_newest -> last entry with timestamp that is not higher than the target timestamp.
%% @end
%%--------------------------------------------------------------------
-spec find_entry_by_timestamp(sentinel(), direction(), timestamp(), timestamp(), entry_id(), timestamp(), entry_id()) ->
    entry_id().
find_entry_by_timestamp(_, forward_from_oldest, TStamp, OldestId, OldestTStamp, _, _) when TStamp =< OldestTStamp ->
    OldestId;
find_entry_by_timestamp(_, forward_from_oldest, TStamp, _, _, NewestId, NewestTStamp) when TStamp > NewestTStamp ->
    NewestId + 1;
find_entry_by_timestamp(_, backward_from_newest, TStamp, _, _, NewestId, NewestTStamp) when TStamp >= NewestTStamp ->
    NewestId;
find_entry_by_timestamp(_, backward_from_newest, TStamp, OldestId, OldestTStamp, _, _) when TStamp < OldestTStamp ->
    OldestId - 1;
find_entry_by_timestamp(Sentinel, Direction, TStamp, OldestId, OldestTStamp, NewestId, NewestTStamp) ->
    OldestNodeNum = entry_id_to_node_number(Sentinel, OldestId),
    NewestNodeNum = entry_id_to_node_number(Sentinel, NewestId),
    case NewestNodeNum - OldestNodeNum of
        0 ->
            % the considered range is included in one node
            OnlyNode = get_node_by_number(Sentinel, NewestNodeNum),
            index_in_node_to_entry_id(Sentinel, NewestNodeNum, index_in_node_of_timestamp(OnlyNode, Direction, TStamp));
        1 ->
            % the considered range covers two adjacent nodes
            OldestNode = get_node_by_number(Sentinel, OldestNodeNum),
            case locate_timestamp_in_node(OldestNode, Direction, TStamp) of
                older ->
                    OldestId;
                {included, IndexInNode} ->
                    index_in_node_to_entry_id(Sentinel, OldestNodeNum, IndexInNode);
                newer ->
                    NewestNode = get_node_by_number(Sentinel, NewestNodeNum),
                    case locate_timestamp_in_node(NewestNode, Direction, TStamp) of
                        older ->
                            FirstEntryId = index_in_node_to_entry_id(Sentinel, NewestNodeNum, 0),
                            case Direction of
                                forward_from_oldest -> FirstEntryId;
                                backward_from_newest -> FirstEntryId - 1
                            end;
                        {included, IndexInNode} ->
                            index_in_node_to_entry_id(Sentinel, NewestNodeNum, IndexInNode);
                        newer ->
                            case Direction of
                                forward_from_oldest -> NewestId;
                                backward_from_newest -> NewestId + 1
                            end
                    end
            end;
        _ ->
            % the considered range covers more than two nodes -
            % approximate target entry location assuming linearly growing timestamps
            CandidateId = OldestId + round((NewestId - OldestId) * (TStamp - OldestTStamp) / (NewestTStamp - OldestTStamp)),
            CandidateNodeNum = snap_to_range(
                entry_id_to_node_number(Sentinel, CandidateId),
                {OldestNodeNum + 1, NewestNodeNum - 1}
            ),
            CandidateNode = get_node_by_number(Sentinel, CandidateNodeNum),
            case locate_timestamp_in_node(CandidateNode, Direction, TStamp) of
                older ->
                    CandidateOldestId = index_in_node_to_entry_id(Sentinel, CandidateNodeNum, 0),
                    CandidateOldestTStamp = CandidateNode#node.oldest_timestamp,
                    find_entry_by_timestamp(
                        Sentinel, Direction, TStamp, OldestId, OldestTStamp, CandidateOldestId, CandidateOldestTStamp
                    );
                {included, IndexInNode} ->
                    index_in_node_to_entry_id(Sentinel, CandidateNodeNum, IndexInNode);
                newer ->
                    CandidateNewestId = index_in_node_to_entry_id(Sentinel, CandidateNodeNum + 1, 0) - 1,
                    CandidateNewestTStamp = CandidateNode#node.newest_timestamp,
                    find_entry_by_timestamp(
                        Sentinel, Direction, TStamp, CandidateNewestId, CandidateNewestTStamp, NewestId, NewestTStamp
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
compare_timestamp_against_node(#node{oldest_timestamp = Oldest}, forward_from_oldest, T) when T =< Oldest -> older;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, forward_from_oldest, T) when T =< Newest -> included;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, forward_from_oldest, T) when T > Newest -> newer;
compare_timestamp_against_node(#node{oldest_timestamp = Oldest}, backward_from_newest, T) when T < Oldest -> older;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, backward_from_newest, T) when T < Newest -> included;
compare_timestamp_against_node(#node{newest_timestamp = Newest}, backward_from_newest, T) when T >= Newest -> newer.


%% @private
-spec index_in_node_of_timestamp(log_node(), direction(), timestamp()) -> index_in_node().
index_in_node_of_timestamp(#node{entries = Entries}, forward_from_oldest, TargetTimestamp) ->
    lists_utils:foldl_while(fun({Timestamp, _}, Index) ->
        case TargetTimestamp > Timestamp of
            true -> {halt, Index};
            false -> {cont, Index - 1}
        end
    end, length(Entries), Entries);
index_in_node_of_timestamp(#node{entries = Entries}, backward_from_newest, TargetTimestamp) ->
    lists_utils:foldl_while(fun
        ({Timestamp, _}, Index) ->
            case TargetTimestamp >= Timestamp of
                true -> {halt, Index - 1};
                false -> {cont, Index - 1}
            end
    end, length(Entries), Entries).


%% @private
-spec index_in_node_to_entry_id(sentinel(), log_node_number(), index_in_node()) -> entry_id().
index_in_node_to_entry_id(Sentinel, NewestNodeNum, Index) ->
    NewestNodeNum * Sentinel#sentinel.max_entries_per_node + Index.


%% @private
-spec append_entry_to_node(log_node(), entry()) -> log_node().
append_entry_to_node(Node = #node{entries = Entries, oldest_timestamp = OldestEntry}, Entry = {Timestamp, _}) ->
    Node#node{
        entries = [Entry | Entries],
        newest_timestamp = Timestamp,
        oldest_timestamp = case Entries of
            [] -> Timestamp;
            _ -> OldestEntry
        end
    }.


%% @private
-spec transfer_entries_to_new_node_upon_full_buffer(sentinel()) -> ok | {error, term()}.
transfer_entries_to_new_node_upon_full_buffer(Sentinel = #sentinel{buffer = #node{entries = Entries}}) ->
    case length(Entries) == Sentinel#sentinel.max_entries_per_node of
        true ->
            case save_buffer_as_new_node(Sentinel) of
                ok ->
                    {ok, Sentinel#sentinel{buffer = #node{}}};
                {error, _} = Error ->
                    Error
            end;
        false ->
            {ok, Sentinel}
    end.


%% @private
-spec save_buffer_as_new_node(sentinel()) -> ok | {error, term()}.
save_buffer_as_new_node(Sentinel = #sentinel{buffer = Buffer}) ->
    NodeId = build_node_id(Sentinel, latest_node_number(Sentinel) - 1),
    infinite_log_persistence:save_record(NodeId, Buffer).


%% @private
-spec delete_log_nodes(sentinel()) -> ok | {error, term()}.
delete_log_nodes(#sentinel{entry_count = EntryCount} = Sentinel) ->
    MaxNodeNumber = entry_id_to_node_number(Sentinel, EntryCount - 1),
    % the newest node is included in the sentinel - no need to delete it
    case MaxNodeNumber of
        0 ->
            ok;
        _ ->
            NodeNumbersToDelete = lists:seq(0, MaxNodeNumber - 1),
            lists_utils:foldl_while(fun(NodeNumber, _) ->
                case infinite_log_persistence:delete_record(build_node_id(Sentinel, NodeNumber)) of
                    ok ->
                        {cont, ok};
                    {error, _} = Error ->
                        {halt, Error}
                end
            end, ok, NodeNumbersToDelete)
    end.


%% @private
-spec build_node_id(sentinel(), log_node_number()) -> log_node_id().
build_node_id(Sentinel, NodeNumber) ->
    datastore_key:build_adjacent(integer_to_binary(NodeNumber), Sentinel#sentinel.structure_id).


%% @private
-spec get_node_by_number(sentinel(), log_node_number()) -> log_node().
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
-spec get_node_entries_length(sentinel(), log_node_number()) -> log_node_number().
get_node_entries_length(Sentinel = #sentinel{max_entries_per_node = MaxEntriesPerNode}, NodeNumber) ->
    case latest_node_number(Sentinel) of
        NodeNumber -> Sentinel#sentinel.entry_count rem MaxEntriesPerNode;
        _ -> MaxEntriesPerNode
    end.


%% @private
-spec latest_node_number(sentinel()) -> log_node_number().
latest_node_number(Sentinel = #sentinel{entry_count = EntryCount}) ->
    entry_id_to_node_number(Sentinel, EntryCount).


%% @private
-spec entry_id_to_node_number(sentinel(), entry_id()) -> log_node_number().
entry_id_to_node_number(#sentinel{max_entries_per_node = MaxEntriesPerNode}, EntryId) ->
    EntryId div MaxEntriesPerNode.


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
