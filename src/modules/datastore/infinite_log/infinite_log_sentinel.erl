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

%% Model API
-export([get/1, save/2, delete/1]).
%% Convenience functions
-export([append/2]).
-export([get_node_by_number/2]).

-type record() :: #sentinel{}.
-export_type([record/0]).

%%=====================================================================
%% API
%%=====================================================================

-spec get(infinite_log:log_id()) -> {ok, term()} | {error, term()}.
get(LogId) ->
    case node_cache:get({?MODULE, LogId}, undefined) of
        undefined -> {error, not_found};
        Record -> {ok, Record}
    end.


-spec save(infinite_log:log_id(), term()) -> ok | {error, term()}.
save(LogId, Value) ->
    %% @TODO VFS-7411 trick dialyzer into thinking that errors can be returned
    %% they actually will after integration with datastore
    case LogId of
        <<"123">> -> {error, etmpfail};
        _ -> node_cache:put({?MODULE, LogId}, Value)
    end.


-spec delete(infinite_log:log_id()) -> ok | {error, term()}.
delete(LogId) ->
    %% @TODO VFS-7411 trick dialyzer into thinking that errors can be returned
    %% they actually will after integration with datastore
    case LogId of
        <<"123">> -> {error, etmpfail};
        _ -> node_cache:clear({?MODULE, LogId})
    end.

%%=====================================================================
%% Convenience functions
%%=====================================================================

-spec append(record(), infinite_log:content()) -> ok | {error, term()}.
append(Sentinel = #sentinel{log_id = LogId, entry_count = EntryCount, oldest_timestamp = OldestTimestamp}, Content) ->
    case transfer_entries_to_new_node_upon_full_buffer(Sentinel) of
        {error, _} = Error ->
            Error;
        {ok, UpdatedSentinel = #sentinel{buffer = Buffer}} ->
            Timestamp = current_timestamp(UpdatedSentinel),
            FinalSentinel = UpdatedSentinel#sentinel{
                entry_count = EntryCount + 1,
                buffer = infinite_log_node:append_entry(Buffer, {Timestamp, Content}),
                oldest_timestamp = case EntryCount of
                    0 -> Timestamp;
                    _ -> OldestTimestamp
                end,
                newest_timestamp = Timestamp
            },
            save(LogId, FinalSentinel)
    end.


-spec get_node_by_number(record(), infinite_log_node:node_number()) -> infinite_log_node:record().
get_node_by_number(Sentinel = #sentinel{log_id = LogId}, NodeNumber) ->
    case infinite_log_node:latest_node_number(Sentinel) of
        NodeNumber ->
            Sentinel#sentinel.buffer;
        _ ->
            {ok, Node} = infinite_log_node:get(LogId, NodeNumber),
            Node
    end.

%%=====================================================================
%% Internal functions
%%=====================================================================

%% @private
-spec transfer_entries_to_new_node_upon_full_buffer(record()) -> ok | {error, term()}.
transfer_entries_to_new_node_upon_full_buffer(Sentinel = #sentinel{max_entries_per_node = MaxEntriesPerNode}) ->
    case infinite_log_node:get_node_entries_length(Sentinel, infinite_log_node:latest_node_number(Sentinel)) of
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
-spec save_buffer_as_new_node(record()) -> ok | {error, term()}.
save_buffer_as_new_node(Sentinel = #sentinel{log_id = LogId, buffer = Buffer}) ->
    NodeNumber = infinite_log_node:latest_node_number(Sentinel),
    infinite_log_node:save(LogId, NodeNumber, Buffer).


%% @private
-spec current_timestamp(record()) -> infinite_log:timestamp().
current_timestamp(#sentinel{newest_timestamp = NewestTimestamp}) ->
    global_clock:monotonic_timestamp_millis(NewestTimestamp).
