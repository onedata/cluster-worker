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
%%%   * TTL (Time To Live) - a TTL can be explicitly set, making ALL the log
%%%     data expire after a certain time. During that time, the log can still
%%%     be read and new entries can still be appended.
%%%
%%%   * size based pruning - oldest nodes are pruned when the total log size
%%%     exceed a certain threshold. The threshold is soft - the pruning happens
%%%     when the log size is equal to threshold + max_elements_per_node, so that
%%%     after the pruning, the number of entries left is equal to the threshold.
%%%
%%%   * age based pruning - oldest nodes are pruned when all entries in given
%%%     node are older than the threshold. If this option is chosen, the nodes
%%%     are assigned a TTL so that they expire on the database level, even if
%%%     the pruning is not applied.
%%%
%%% In case of size/age based pruning, only whole nodes are deleted (when all
%%% entries in the node satisfy the pruning condition). The newest node
%%% (buffered inside sentinel) is never pruned, which means that the log can
%%% still contain some entries that satisfy the pruning threshold, but will not
%%% be pruned unless the log grows.
%%%
%%% Setting a TTL causes the whole log to be completely deleted after given
%%% time. In the specific case when the age-based pruning is also set, the TTL
%%% overrides the document's expiration time, even if pruning threshold is longer.
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
-export([set_ttl/2]).

% unit of timestamps used across the module for stamping entries and searching
-type timestamp() :: time:millis().

% id of an infinite log instance as stored in database
-type log_id() :: binary().

%% @formatter:off
-type log_opts() :: #{
    max_entries_per_node => pos_integer(),
    size_pruning_threshold => undefined | non_neg_integer(),
    age_pruning_threshold => undefined | time:seconds()
}.
%% @formatter:on

% content of a log entry, must be a text (suitable for JSON),
% if needed may encode some arbitrary structures as a JSON or base64
-type content() :: binary().
-type entry() :: {timestamp(), content()}.
% single entry in the log, numbered from 0 (oldest entry)
-type entry_index() :: non_neg_integer().

-export_type([timestamp/0, log_id/0, content/0, entry/0, entry_index/0]).

% macros used to determine safe log content size
% couchbase sets the document size limit at 20MB, assume 19MB as safe
-define(SAFE_NODE_DB_SIZE, 19000000).
-define(APPROXIMATE_EMPTY_ENTRY_DB_SIZE, 500).

%%=====================================================================
%% API
%%=====================================================================

-spec create(log_id(), log_opts()) -> ok | {error, term()}.
create(LogId, Opts) ->
    infinite_log_sentinel:save(LogId, #sentinel{
        log_id = LogId,
        max_entries_per_node = maps:get(max_entries_per_node, Opts, ?DEFAULT_MAX_ENTRIES_PER_NODE),
        size_pruning_threshold = maps:get(size_pruning_threshold, Opts, undefined),
        age_pruning_threshold = maps:get(age_pruning_threshold, Opts, undefined)
    }).


-spec destroy(log_id()) -> ok | {error, term()}.
destroy(LogId) ->
    case infinite_log_sentinel:acquire(LogId, skip_pruning) of
        {ok, Sentinel} ->
            case apply_for_archival_log_nodes(Sentinel, fun infinite_log_node:delete/2) of
                {error, _} = Error ->
                    Error;
                ok ->
                    infinite_log_sentinel:delete(LogId)
            end;
        {error, not_found} ->
            ok
    end.


-spec append(log_id(), content()) -> ok | {error, term()}.
append(LogId, Content) when is_binary(LogId) ->
    case infinite_log_sentinel:acquire(LogId, skip_pruning) of
        {error, _} = GetError ->
            GetError;
        {ok, Sentinel} ->
            case sanitize_append_request(Sentinel, Content) of
                {error, _} = SanitizeError ->
                    SanitizeError;
                ok ->
                    infinite_log_sentinel:append(Sentinel, Content)
            end
    end.


-spec list(log_id(), infinite_log_browser:listing_opts()) ->
    {ok, infinite_log_browser:listing_result()} | {error, term()}.
list(LogId, Opts) ->
    % age based pruning must be attempted at every listing as some of
    % the log nodes may have expired
    case infinite_log_sentinel:acquire(LogId, apply_pruning) of
        {error, _} = GetError ->
            GetError;
        {ok, Sentinel} ->
            {ok, infinite_log_browser:list(Sentinel, Opts)}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Makes the log expire (be deleted from database) after specified Time To Live.
%% The procedure iterates through all documents used up by the log.
%% @end
%%--------------------------------------------------------------------
-spec set_ttl(log_id(), time:seconds()) -> ok | {error, term()}.
set_ttl(LogId, Ttl) ->
    case infinite_log_sentinel:acquire(LogId, skip_pruning) of
        {error, _} = GetError ->
            GetError;
        {ok, Sentinel} ->
            SetNodeTtl = fun(LogId, NodeNumber) ->
                infinite_log_node:set_ttl(LogId, NodeNumber, Ttl)
            end,
            case apply_for_archival_log_nodes(Sentinel, SetNodeTtl) of
                {error, _} = SetTtlError ->
                    SetTtlError;
                ok ->
                    infinite_log_sentinel:set_ttl(LogId, Ttl)
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
safe_log_content_size(#sentinel{max_entries_per_node = MaxEntriesPerNode}) ->
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
    fun((log_id(), infinite_log_node:node_number()) -> ok | {error, term()})
) ->
    ok | {error, term()}.
apply_for_archival_log_nodes(Sentinel = #sentinel{log_id = LogId}, Callback) ->
    BufferNodeNumber = infinite_log_node:newest_node_number(Sentinel),
    ArchivalNodeNumbers = lists:seq(0, BufferNodeNumber - 1),
    lists_utils:foldl_while(fun(NodeNumber, _) ->
        case Callback(LogId, NodeNumber) of
            ok ->
                {cont, ok};
            {error, _} = Error ->
                {halt, Error}
        end
    end, ok, ArchivalNodeNumbers).
