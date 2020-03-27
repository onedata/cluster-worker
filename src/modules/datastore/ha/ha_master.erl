%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used by datastore_writer and
%%% datastore_cache_writer when HA is enabled and process
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
-export([verify_slave_activity/2, handle_slave_message/2]).
% API - messages' handling by datastore_cache_writer
-export([handle_internal_call/2, handle_internal_cast/2, handle_slave_lifecycle_message/2]).

% status of link between master and slave (see ha_datastore.hrl)
-define(SLAVE_LINKED, linked).
-define(SLAVE_NOT_LINKED, not_linked).
-type link_status() :: ?SLAVE_LINKED | ?SLAVE_NOT_LINKED.

-record(data, {
    backup_nodes = [] :: [node()], % currently only single backup node is supported
    link_status = ?SLAVE_NOT_LINKED :: link_status(),
    slave_pid = undefined :: undefined | pid(),
    propagation_method :: ha_datastore_utils:propagation_method()
}).

-type ha_master_data() :: #data{}.
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
%% Verifies slave activity to check if slave processes any
%% requests connected to master's keys. In such a case master
%% will wait with processing for slave's processing finish.
%% Executed during start of process that works as master.
%% @end
%%--------------------------------------------------------------------
-spec verify_slave_activity(datastore:key(), [node()]) ->
    {ActiveRequests :: boolean(), [datastore:key()], datastore_writer:requests_internal()}.
verify_slave_activity(Key, BackupNodes) ->
    case BackupNodes of
        [Node | _] ->
            % TODO VFS-6168 - do not check when master wasn't down for a long time.
            case ha_datastore_utils:send_sync_master_message(Node, Key, #get_slave_status{answer_to = self()}, false) of
                {error, not_alive} ->
                    {false, [], []};
                #slave_status{failover_request_handling = ActiveRequests,
                    failover_pending_cache_requests = CacheRequestsMap,
                    failover_finished_memory_cache_requests = MemoryRequests,
                    failover_requests_to_handle = RequestsToHandle
                } ->
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
            Data#data{slave_pid = Pid};
        Error ->
            ?warning("Cannot broadcast HA data because of error: ~p", [Error]),
            Data
    end;
request_backup(ProcessKey, Keys, CacheRequests, #data{link_status = ?SLAVE_NOT_LINKED,
    backup_nodes = [Node | _]} = Data) ->
    case ha_datastore_utils:send_sync_master_message(Node, ProcessKey,
        #request_backup{keys = Keys, cache_requests = CacheRequests, link = {true, self()}}, true) of
        {ok, Pid} ->
            Data#data{link_status = ?SLAVE_LINKED, slave_pid = Pid};
        Error ->
            ?warning("Cannot broadcast HA data because of error: ~p", [Error]),
            Data
    end;
request_backup(_ProcessKey, Keys, CacheRequests, #data{slave_pid = Pid} = Data) ->
    ha_datastore_utils:send_async_master_message(Pid, #request_backup{keys = Keys, cache_requests = CacheRequests}),
    Data.

-spec forget_backup(datastore_doc_batch:cached_keys(), ha_master_data()) -> ok.
forget_backup([], _) ->
    ok;
forget_backup(_, #data{backup_nodes = []}) ->
    ok;
forget_backup(_, #data{slave_pid = undefined}) ->
    ?warning("Inactivation request without slave defined"),
    ok;
forget_backup(Inactivated, #data{slave_pid = Pid}) ->
    ha_datastore_utils:send_async_master_message(Pid, #forget_backup{keys = Inactivated}).

%%%===================================================================
%%% API - messages handling by datastore_writer
%%%===================================================================

-spec handle_slave_message(failover_request_data_processed_message(), pid()) -> boolean().
handle_slave_message(#failover_request_data_processed{request_handled = HandlingFinished,
    cache_requests_saved = CacheRequests} = Msg, Pid) ->
    datastore_cache:save(maps:values(CacheRequests)),
    ha_datastore_utils:send_async_internal_message(Pid, Msg),
    HandlingFinished.

%%%===================================================================
%%% API - messages handling by datastore_cache_writer
%%%===================================================================

-spec handle_internal_call(config_changed_message(), ha_master_data()) -> ha_master_data().
handle_internal_call(?CONFIG_CHANGED, Data) ->
    BackupNodes = ha_datastore_utils:get_backup_nodes(),
    PropagationMethod = ha_datastore_utils:get_propagation_method(),
    Data#data{backup_nodes = BackupNodes, propagation_method = PropagationMethod}.

-spec handle_internal_cast(failover_request_data_processed_message(), datastore_cache_writer:keys_in_flush()) ->
    datastore_cache_writer:keys_in_flush().
handle_internal_cast(#failover_request_data_processed{
    cache_requests_saved = CacheRequestsSaved, keys_flushed = KeysFlushed}, KiF) ->
    NewKiF = lists:map(fun(Key) -> {Key, {slave_flush, undefined}} end, maps:keys(CacheRequestsSaved)),
    KiF2 = maps:merge(maps:from_list(NewKiF), KiF),
    maps:without(sets:to_list(KeysFlushed), KiF2).

-spec handle_slave_lifecycle_message(unlink_request(), ha_master_data()) -> ha_master_data().
handle_slave_lifecycle_message(?REQUEST_UNLINK, Data) ->
    Data#data{link_status = ?SLAVE_NOT_LINKED}.
