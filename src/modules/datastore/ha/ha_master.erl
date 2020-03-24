%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used by datastore_writer and
%%% datastore_cache_writer when ha is enabled and process
%%% works as master (processing requests).
%%% For more information see ha_datastore.hrl.
%%% @end
%%%-------------------------------------------------------------------
-module(ha_master).
-author("Michał Wrzeszcz").

-include("modules/datastore/ha_datastore.hrl").
-include_lib("ctool/include/logging.hrl").

% API - process init
-export([init_data/1]).
% API - broadcasting actions to slave
-export([request_backup/4, forget_backup/2]).
% API - messages' handling by datastore_writer
-export([check_slave/2, handle_slave_message/2]).
% API - messages' handling by datastore_cache_writer
-export([handle_internal_message/2, handle_slave_lifecycle_message/2]).

-record(data, {
    backup_nodes = [] :: [node()], % currently only single backup node is supported
    slave_status = {not_linked, undefined} :: slave_status(),
    propagation_method :: ha_datastore_utils:propagation_method()
}).

-type ha_master_data() :: #data{}.
-type slave_status() :: {linked | not_linked, undefined | pid()}.

-export_type([ha_master_data/0]).

% Used messages' types:
-type failover_request_data_processed_message() :: #failover_request_data_processed{}.
-type config_changed_message() :: ?CONFIG_CHANGED.
-type unlink_request() :: ?REQUEST_UNLINK.

-export_type([failover_request_data_processed_message/0, config_changed_message/0, unlink_request/0]).

%%%===================================================================
%%% API - Process init
%%%===================================================================

-spec init_data([node()]) -> ha_master_data().
init_data(BackupNodes) ->
    #data{backup_nodes = BackupNodes, propagation_method = ha_datastore_utils:get_propagation_method()}.

%%--------------------------------------------------------------------
%% @doc
%% Verifies slave state during start of process that works as master.
%% @end
%%--------------------------------------------------------------------
-spec check_slave(datastore:key(), [node()]) ->
    {ActiveRequests :: boolean(), [datastore:key()], datastore_writer:requests_internal()}.
check_slave(Key, BackupNodes) ->
    case BackupNodes of
        [Node | _] ->
            case ha_datastore_utils:send_sync_master_message(Node, Key, #get_slave_status{answer_to = self()}, false) of
                {error, not_alive} ->
                    {false, [], []};
                #slave_status{failover_request_handling = ActiveRequests,
                    failover_pending_cache_requests = CacheRequestsMap,
                    failover_finished_memory_cache_requests = MemoryRequests,
                    failover_requests_to_handle = RequestsToHandle} ->
                    datastore_cache:save(maps:values(CacheRequestsMap)),
                    datastore_cache:save(MemoryRequests),
                    {ActiveRequests, maps:keys(CacheRequestsMap), RequestsToHandle}
            end;
        _ ->
            {false, [], []}
    end.

%%%===================================================================
%%% API - broadcasting actions to slave
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends information about request handling to backup nodes (to be used in case of failure).
%% @end
%%--------------------------------------------------------------------
-spec request_backup(datastore:key(), datastore_doc_batch:cached_keys(),
    [datastore_cache:cache_save_request()], ha_master_data()) -> ha_master_data().
request_backup(_ProcessKey, [], _CacheRequests, Data) ->
    Data;
request_backup(_ProcessKey, _Keys, _CacheRequests, #data{backup_nodes = []} = Data) ->
    Data;
request_backup(ProcessKey, Keys, CacheRequests, #data{propagation_method = ?HA_CALL_PROPAGATION,
    backup_nodes = [Node | _]} = Data) ->
    case ha_datastore_utils:send_sync_master_message(Node, ProcessKey,
        #request_backup{keys = Keys, cache_requests = CacheRequests}, true) of
        {ok, Pid} ->
            Data#data{slave_status = {not_linked, Pid}};
        Error ->
            ?warning("Cannot broadcast ha data because of error: ~p", [Error]),
            Data
    end;
request_backup(ProcessKey, Keys, CacheRequests, #data{slave_status = {not_linked, _},
    backup_nodes = [Node | _]} = Data) ->
    case ha_datastore_utils:send_sync_master_message(Node, ProcessKey,
        #request_backup{keys = Keys, cache_requests = CacheRequests, link = {true, self()}}, true) of
        {ok, Pid} ->
            Data#data{slave_status = {linked, Pid}};
        Error ->
            ?warning("Cannot broadcast ha data because of error: ~p", [Error]),
            Data
    end;
request_backup(_ProcessKey, Keys, CacheRequests, #data{slave_status = {linked, Pid}} = Data) ->
    ha_datastore_utils:send_async_master_message(Pid, #request_backup{keys = Keys, cache_requests = CacheRequests}),
    Data.

%%--------------------------------------------------------------------
%% @doc
%% Sends information about keys inactivation to backup nodes
%% (to delete data that is not needed for HA as keys are already flushed).
%% @end
%%--------------------------------------------------------------------
-spec forget_backup(datastore_doc_batch:cached_keys(), ha_master_data()) -> ok.
forget_backup([], _) ->
    ok;
forget_backup(_, #data{backup_nodes = []}) ->
    ok;
forget_backup(_, #data{slave_status = {_, undefined}}) ->
    ?warning("Inactivation request without slave defined"),
    ok;
forget_backup(Inactivated, #data{slave_status = {_, Pid}}) ->
    ha_datastore_utils:send_async_master_message(Pid, #forget_backup{keys = Inactivated}).

%%%===================================================================
%%% API - messages handling by datastore_writer
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles slave message and translates it to action by datastore_writer if needed.
%% Messages concern proxy requests or emergency requests (both sent in case of previous master failure)
%% @end
%%--------------------------------------------------------------------
-spec handle_slave_message(failover_request_data_processed_message(), pid()) -> boolean().
handle_slave_message(#failover_request_data_processed{request_handled = HandlingFinished,
    cache_requests_saved = CacheRequests} = Msg, Pid) ->
    datastore_cache:save(maps:values(CacheRequests)),
    ha_datastore_utils:send_internall_message(Pid, Msg, true),
    HandlingFinished.

%%%===================================================================
%%% API - messages handling by datastore_cache_writer
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles internal message and returns new backup data in case or reconfiguration or new keys to flush in case of
%% messages concerning emergency requests.
%% @end
%%--------------------------------------------------------------------
-spec handle_internal_message(config_changed_message() | failover_request_data_processed_message(),
    ha_master_data() | datastore_cache_writer:keys_in_flush()) ->
    ha_master_data() | datastore_cache_writer:keys_in_flush().
handle_internal_message(?CONFIG_CHANGED, Data) ->
    BackupNodes = ha_datastore_utils:get_backup_nodes(),
    PropagationMethod = ha_datastore_utils:get_propagation_method(),
    Data#data{backup_nodes = BackupNodes, propagation_method = PropagationMethod};

handle_internal_message(#failover_request_data_processed{
    cache_requests_saved = CacheRequestsSaved, keys_flushed = KeysFlushed}, KiF) ->
    NewKiF = lists:map(fun(Key) -> {Key, {slave_flush, undefined}} end, maps:keys(CacheRequestsSaved)),
    KiF2 = maps:merge(maps:from_list(NewKiF), KiF),
    maps:without(sets:to_list(KeysFlushed), KiF2).

-spec handle_slave_lifecycle_message(unlink_request(), ha_master_data()) -> ha_master_data().
handle_slave_lifecycle_message(?REQUEST_UNLINK, #data{slave_status = {_, Pid}} = Data) ->
    Data#data{slave_status = {not_linked, Pid}}.
