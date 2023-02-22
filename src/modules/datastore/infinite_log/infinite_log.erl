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
%%%
%%% The infinite log supports three ways of automatic cleaning:
%%%
%%%   * size based pruning - oldest nodes are pruned when the total log size
%%%     exceed a certain threshold. The threshold is soft - the pruning happens
%%%     when the log size is equal to threshold + max_elements_per_node, so that
%%%     after the pruning, the number of entries left is equal to the threshold.
%%%
%%%   * age based pruning - oldest nodes are pruned when all entries in given
%%%     node are older than the threshold. If this option is chosen, the archival
%%%     nodes are assigned a TTL so that they expire on the database level, even
%%%     if the pruning is not applied.
%%%
%%%   * expiry - the whole log is subject to expiry (if the expiry threshold is
%%%     defined), i.e. a TTL is set for all the log documents. After a certain time,
%%%     ALL the log data is deleted. For the sentinel, the expiry time is bumped
%%%     every time a new log is prepended. For archival nodes, it works just like
%%%     age based pruning.
%%%
%%% In case of size/age based pruning, only whole nodes are deleted (when all
%%% entries in the node satisfy the pruning condition). The newest node
%%% (buffered inside sentinel) is never pruned, which means that the log can
%%% still contain some entries that satisfy the pruning threshold, but will not
%%% be pruned unless the log grows.
%%%
%%% Setting an expiry causes the whole log to be completely deleted after given
%%% time (counting since the last update). In the specific case when the age-based
%%% pruning is also set, the lower of the two thresholds prevails for archival nodes,
%%% while the sentinel is affected only by the expiry.
%%% @end
%%%-------------------------------------------------------------------
-module(infinite_log).
-author("Lukasz Opiola").

-include("modules/datastore/infinite_log.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([create/4, destroy/3]).
-export([append/4]).
-export([list/4, list/5]).
-export([adjust_expiry_threshold/4]).

% unit of timestamps used across the module for stamping entries and searching
-type timestamp_millis() :: time:millis().

% id of an infinite log instance as stored in database
-type log_id() :: binary().

%% @formatter:off
-type log_opts() :: #{
    max_entries_per_node => pos_integer(),
    size_pruning_threshold => undefined | non_neg_integer(),
    age_pruning_threshold => undefined | time:seconds(),
    expiry_threshold => undefined | time:seconds()
}.
%% @formatter:on

% content of a log entry, must be a text (suitable for JSON),
% if needed may encode some arbitrary structures as a JSON or base64
-type content() :: binary().
-type entry() :: {timestamp_millis(), content()}.
% single entry in the log, numbered from 0 (oldest entry)
-type entry_index() :: non_neg_integer().

-export_type([timestamp_millis/0, log_id/0, log_opts/0, content/0, entry/0, entry_index/0]).

% Indicates if the calling process is suitable for updating the log data, or may
% only cause read only access to the documents. In case of 'readonly' access
% mode, when a log document update is required during the requested operation,
% the operation will fail with '{error, update_required}'.
-type access_mode() :: readonly | allow_updates.
-export_type([access_mode/0]).

-type ctx() :: datastore_doc:ctx().
-type batch() :: datastore_doc:batch() | undefined.
-export_type([ctx/0, batch/0]).

% macros used to determine safe log content size
% couchbase sets the document size limit at 20MB, assume 19MB as safe
-define(SAFE_NODE_DB_SIZE, 19000000).
-define(APPROXIMATE_EMPTY_ENTRY_DB_SIZE, 500).

%%=====================================================================
%% API
%%=====================================================================

-spec create(ctx(), log_id(), log_opts(), batch()) -> {ok | {error, term()}, batch()}.
create(Ctx, LogId, Opts, InitialBatch) ->
    infinite_log_sentinel:create(Ctx, LogId, #infinite_log_sentinel{
        log_id = LogId,
        max_entries_per_node = maps:get(max_entries_per_node, Opts, ?DEFAULT_MAX_ENTRIES_PER_NODE),
        size_pruning_threshold = maps:get(size_pruning_threshold, Opts, undefined),
        age_pruning_threshold = maps:get(age_pruning_threshold, Opts, undefined),
        expiry_threshold = maps:get(expiry_threshold, Opts, undefined)
    }, InitialBatch).


-spec destroy(ctx(), log_id(), batch()) -> {ok | {error, term()}, batch()}.
destroy(Ctx, LogId, InitialBatch) ->
    case infinite_log_sentinel:acquire(Ctx, LogId, skip_pruning, allow_updates, InitialBatch) of
        {{ok, Sentinel}, AcquireBatch} ->
            Callback = fun(_LogId, NodeNumber, AccBatch) -> 
                infinite_log_node:delete(Ctx, LogId, NodeNumber, AccBatch) 
            end,
            case apply_for_archival_log_nodes(Sentinel, Callback, AcquireBatch) of
                {{error, _}, _} = ErrorResponse ->
                    ErrorResponse;
                {ok, ReturnedBatch} ->
                    infinite_log_sentinel:delete(Ctx, LogId, ReturnedBatch)
            end;
        {{error, not_found}, Batch2} ->
            {ok, Batch2}
    end.


-spec append(ctx(), log_id(), content(), batch()) -> {ok | {error, term()}, batch()}.
append(Ctx, LogId, Content, InitialBatch) when is_binary(LogId) ->
    case infinite_log_sentinel:acquire(Ctx, LogId, skip_pruning, allow_updates, InitialBatch) of
        {{error, _}, _} = AcquireError ->
            AcquireError;
        {{ok, Sentinel}, AcquireBatch} ->
            case sanitize_append_request(Sentinel, Content) of
                {error, _} = SanitizeError ->
                    {SanitizeError, AcquireBatch};
                ok ->
                    infinite_log_sentinel:append(Ctx, Sentinel, Content, AcquireBatch)
            end
    end.


-spec list(ctx(), log_id(), infinite_log_browser:listing_opts(), batch()) ->
    {{ok, infinite_log_browser:listing_result()} | {error, term()}, batch()}.
list(Ctx, LogId, Opts, InitialBatch) ->
    list(Ctx, LogId, Opts, allow_updates, InitialBatch).


-spec list(ctx(), log_id(), infinite_log_browser:listing_opts(), access_mode(), batch()) ->
    {{ok, infinite_log_browser:listing_result()} | {error, term()}, batch()}.
list(Ctx, LogId, Opts, AccessMode, InitialBatch) ->
    % age based pruning must be attempted at every listing as some of
    % the log nodes may have expired
    case infinite_log_sentinel:acquire(Ctx, LogId, apply_pruning, AccessMode, InitialBatch) of
        {{error, _}, _} = AcquireError ->
            AcquireError;
        {{ok, Sentinel}, AcquireBatch} ->
            try
                {Res, FinalDatastoreBatch} = infinite_log_browser:list(Ctx, Sentinel, Opts, AcquireBatch),
                {{ok, Res}, FinalDatastoreBatch}
            catch Class:Reason:Stacktrace ->
                ?error_stacktrace("Unexpected error during infinite log listing (id: ~s) - ~w:~p", [
                    LogId, Class, Reason
                ], Stacktrace),
                {{error, internal_server_error}, AcquireBatch}
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Makes the log expire (be deleted from database) after specified threshold.
%% If a previous expiry threshold was set, it is overwritten.
%% The procedure iterates through all documents used up by the log.
%% The expiry countdown starts from the newest timestamp in every node rather than
%% the current time (making them expire as if such threshold was preset when the log
%% was created). This means that archival nodes may expire faster than the sentinel
%% node, but it's guaranteed that after the threshold (and no activity in the meantime),
%% all the data is gone.
%% @end
%%--------------------------------------------------------------------
-spec adjust_expiry_threshold(ctx(), log_id(), time:seconds(), batch()) -> {ok | {error, term()}, batch()}.
adjust_expiry_threshold(Ctx, LogId, Threshold, InitialBatch) ->
    case infinite_log_sentinel:acquire(Ctx, LogId, apply_pruning, allow_updates, InitialBatch) of
        {{error, _}, _} = AcquireError ->
            AcquireError;
        {{ok, Sentinel}, AcquiredBatch} ->
            {ArchivalNodesUpdateResult, UpdatedBatch} = case Sentinel#infinite_log_sentinel.age_pruning_threshold of
                AgeThreshold when AgeThreshold /= undefined andalso AgeThreshold < Threshold ->
                    % if the age based pruning threshold is lower, there is no point in
                    % overwriting the archival nodes (lower threshold prevails)
                    {ok, AcquiredBatch};
                _ ->
                    CurrentTimestamp = infinite_log_sentinel:current_timestamp(Sentinel),
                    SetNodeExpiryThreshold = fun(_LogId, NodeNumber, InternalBatch) ->
                        infinite_log_node:adjust_expiry_threshold(
                            Ctx, LogId, NodeNumber, Threshold, CurrentTimestamp, InternalBatch
                        )
                    end,
                    apply_for_archival_log_nodes(Sentinel, SetNodeExpiryThreshold, AcquiredBatch)
            end,
            case ArchivalNodesUpdateResult of
                {error, _} = SetExpiryThresholdError ->
                    SetExpiryThresholdError;
                ok ->
                    infinite_log_sentinel:adjust_expiry_threshold(
                        Ctx, LogId, Threshold, UpdatedBatch
                    )
            end
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec sanitize_append_request(infinite_log_sentinel:record(), content()) -> ok | {error, term()}.
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
-spec safe_log_content_size(infinite_log_sentinel:record()) -> integer().
safe_log_content_size(#infinite_log_sentinel{max_entries_per_node = MaxEntriesPerNode}) ->
    ?SAFE_NODE_DB_SIZE div MaxEntriesPerNode - ?APPROXIMATE_EMPTY_ENTRY_DB_SIZE.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies given function on all log nodes that are archival (will never be modified),
%% i.e. all nodes apart from the newest one (buffer) included in the sentinel.
%% Stops with an error if the function returns one.
%% @end
%%--------------------------------------------------------------------
-spec apply_for_archival_log_nodes(
    infinite_log_sentinel:record(),
    fun((log_id(), infinite_log_node:node_number(), batch()) -> {ok | {error, term()}, batch()}),
    batch()
) ->
    {ok | {error, term()}, batch()}.
apply_for_archival_log_nodes(Sentinel = #infinite_log_sentinel{log_id = LogId}, Callback, InitialBatch) ->
    BufferNodeNumber = infinite_log_node:newest_node_number(Sentinel),
    OldestNodeNumber = infinite_log_node:oldest_node_number(Sentinel),
    ArchivalNodeNumbers = lists:seq(OldestNodeNumber, BufferNodeNumber - 1),
    lists_utils:foldl_while(fun(NodeNumber, {ok, AccBatch}) ->
        case Callback(LogId, NodeNumber, AccBatch) of
            {ok, _} = OkResponse ->
                {cont, OkResponse};
            {{error, _}, _} = ErrorResponse ->
                {halt, ErrorResponse}
        end
    end, {ok, InitialBatch}, ArchivalNodeNumbers).
