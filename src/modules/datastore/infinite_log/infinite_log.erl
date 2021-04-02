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

-spec create(log_id(), pos_integer()) -> ok | {error, term()}.
create(LogId, MaxEntriesPerNode) ->
    infinite_log_sentinel:save(LogId, #sentinel{
        log_id = LogId,
        max_entries_per_node = MaxEntriesPerNode
    }).


-spec destroy(log_id()) -> ok | {error, term()}.
destroy(LogId) ->
    case infinite_log_sentinel:get(LogId) of
        {ok, Sentinel} ->
            delete_log_nodes(Sentinel),
            infinite_log_sentinel:delete(LogId);
        {error, not_found} ->
            ok
    end.


-spec append(log_id(), content()) -> ok | {error, term()}.
append(LogId, Content) when is_binary(LogId) ->
    case infinite_log_sentinel:get(LogId) of
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
    case infinite_log_sentinel:get(LogId) of
        {error, _} = GetError ->
            GetError;
        {ok, Sentinel} ->
            {ok, infinite_log_browser:list(Sentinel, Opts)}
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


%% @private
-spec delete_log_nodes(infinite_log_sentinel:record()) -> ok | {error, term()}.
delete_log_nodes(Sentinel = #sentinel{log_id = LogId}) ->
    BufferNodeNumber = infinite_log_node:latest_node_number(Sentinel),
    % the newest node (buffer) is included in the sentinel - no need to delete it
    NodeNumbersToDelete = lists:seq(0, BufferNodeNumber - 1),
    lists_utils:foldl_while(fun(NodeNumber, _) ->
        case infinite_log_node:delete(LogId, NodeNumber) of
            ok ->
                {cont, ok};
            {error, _} = Error ->
                {halt, Error}
        end
    end, ok, NodeNumbersToDelete).
