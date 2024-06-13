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
-module(ha_datastore_master).
-author("Michał Wrzeszcz").

-include("modules/datastore/ha_datastore.hrl").
-include_lib("ctool/include/logging.hrl").

% API - process init
-export([init_data/1]).
% API - broadcasting actions to slave
-export([store_backup/4, forget_backup/2]).
% API - messages' handling by datastore_writer
-export([inspect_slave_activity/3, handle_slave_message/2]).
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
    propagation_method :: ha_datastore:propagation_method()
}).

-type ha_master_data() :: #data{}.
% Note: Reorganization uses failover handling methods as node deletion/adding handling by tp process
% is similar to node failure/recovery handling by this process
-type failover_action() :: ?REQUEST_HANDLING_ACTION | ?KEY_FLUSHING_ACTION | #preparing_reorganization{}.
-export_type([ha_master_data/0, failover_action/0]).

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
    #data{backup_nodes = BackupNodes, propagation_method = ha_datastore:get_propagation_method()}.

%%--------------------------------------------------------------------
%% @doc
%% Gets nodes where slave can work to find slave process.
%% Next, inspects slave activity to check if slave processes any
%% requests connected to master's keys. In such a case master
%% will wait with processing for slave's processing finish.
%% Executed during start of process that works as master.
%% @end
%%--------------------------------------------------------------------
-spec inspect_slave_activity(datastore:key(), [node()], ha_datastore:slave_mode()) ->
    {IsHandlingRequests :: boolean(), [datastore:key()], datastore_writer:requests_internal()}.
inspect_slave_activity(Key, _BackupNodes, ?CLUSTER_REORGANIZATION_SLAVE_MODE) ->
    Neighbors = ha_datastore:possible_neighbors_during_reconfiguration(),
    lists:foldl(fun(NodeToCheck, {IsHandlingRequests1, KeysInSlaveFlush1, RequestsToHandle1}) ->
        {IsHandlingRequests2, KeysInSlaveFlush2, RequestsToHandle2} = inspect_slave_activity(Key, NodeToCheck),
        {IsHandlingRequests1 or IsHandlingRequests2, KeysInSlaveFlush1 ++ KeysInSlaveFlush2,
                RequestsToHandle1 ++ RequestsToHandle2}
    end, {false, [], []}, Neighbors);
inspect_slave_activity(Key, BackupNodes, _Mode) ->
    case BackupNodes of
        [Node | _] ->
            % TODO VFS-6168 - do not check when master wasn't down for a long time
            inspect_slave_activity(Key, Node);
        _ ->
            {false, [], []}
    end.

%%%===================================================================
%%% API - broadcasting actions to slave
%%%===================================================================

-spec store_backup(datastore:key(), datastore_doc_batch:cached_keys(),
    [datastore_cache:cache_save_request()], ha_master_data()) -> ha_master_data().
store_backup(_ProcessKey, [], _CacheRequests, Data) ->
    Data;
store_backup(_ProcessKey, _Keys, _CacheRequests, #data{backup_nodes = []} = Data) ->
    Data;
store_backup(ProcessKey, Keys, CacheRequests, Data) ->
    % TODO VFS-6168 - delete keys connected with filtered requests
    send_store_backup_request(ProcessKey, Keys, filter_requests_with_disabled_ha(CacheRequests), Data).

-spec forget_backup(datastore_doc_batch:cached_keys(), ha_master_data()) -> ok.
forget_backup([], _) ->
    ok;
forget_backup(_, #data{backup_nodes = []}) ->
    ok;
forget_backup(_, #data{slave_pid = undefined}) ->
    % TODO VFS-6295 - log to dedicated logfile
    ?debug("Inactivation request without slave defined"),
    ok;
forget_backup(Inactivated, #data{slave_pid = Pid}) ->
    ha_datastore:send_async_master_message(Pid, #forget_backup{keys = Inactivated}).

%%%===================================================================
%%% API - messages handling by datastore_writer
%%%===================================================================

-spec handle_slave_message(failover_request_data_processed_message(), pid()) -> boolean().
handle_slave_message(#failover_request_data_processed{finished_action = FinishedAction,
    cache_requests_saved = CacheRequests} = Msg, Pid) ->
    datastore_cache:save(maps:values(CacheRequests)),
    ha_datastore:send_async_internal_message(Pid, Msg),
    FinishedAction =:= ?REQUEST_HANDLING_ACTION.

%%%===================================================================
%%% API - messages handling by datastore_cache_writer
%%%===================================================================

-spec handle_internal_call(config_changed_message(), ha_master_data()) -> ha_master_data().
handle_internal_call(?CONFIG_CHANGED, Data) ->
    BackupNodes = ha_datastore:get_backup_nodes(),
    PropagationMethod = ha_datastore:get_propagation_method(),
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Inspects slave activity to check if slave processes any
%% requests connected to master's keys. In such a case master
%% will wait with processing for slave's processing finish.
%% Executed during start of process that works as master.
%% @end
%%--------------------------------------------------------------------
-spec inspect_slave_activity(datastore:key(), node()) ->
    {IsHandlingRequests :: boolean(), [datastore:key()], datastore_writer:requests_internal()}.
inspect_slave_activity(Key, Node) ->
    case ha_datastore:send_sync_master_message(Node, Key, #get_slave_failover_status{answer_to = self()}, false) of
        {error, not_alive} ->
            {false, [], []};
        {badrpc, _} ->
            % TODO VFS-6295 - log to dedicated logfile
            {false, [], []};
        #slave_failover_status{is_handling_requests = IsHandlingRequests,
            pending_cache_requests = CacheRequestsMap,
            finished_memory_cache_requests = MemoryRequests,
            requests_to_handle = RequestsToHandle
        } ->
            datastore_cache:save(maps:values(CacheRequestsMap)),
            datastore_cache:save(MemoryRequests),
            {IsHandlingRequests, maps:keys(CacheRequestsMap), RequestsToHandle}
    end.

-spec filter_requests_with_disabled_ha([datastore_cache:cache_save_request()]) -> [datastore_cache:cache_save_request()].
filter_requests_with_disabled_ha(Requests) ->
    lists:filter(fun({Ctx, _Key, _Doc}) -> ha_datastore:is_ha_enabled(Ctx) end, Requests).

-spec send_store_backup_request(datastore:key(), datastore_doc_batch:cached_keys(),
    [datastore_cache:cache_save_request()], ha_master_data()) -> ha_master_data().
send_store_backup_request(_ProcessKey, _Keys, [], Data) ->
    Data;
send_store_backup_request(ProcessKey, Keys, CacheRequests, #data{propagation_method = ?HA_CALL_PROPAGATION,
    backup_nodes = [Node | _]} = Data) ->
    case ha_datastore:send_sync_master_message(Node, ProcessKey,
        #store_backup{keys = Keys, cache_requests = CacheRequests}, true) of
        {ok, Pid} ->
            Data#data{slave_pid = Pid};
        {badrpc, _} ->
            % TODO VFS-6295 - log to dedicated logfile
            ?debug("Cannot broadcast HA data - slave down"),
            Data;
        Error ->
            ?warning("Cannot broadcast HA data because of error: ~tp", [Error]),
            Data
    end;
send_store_backup_request(ProcessKey, Keys, CacheRequests, #data{link_status = ?SLAVE_NOT_LINKED,
    backup_nodes = [Node | _]} = Data) ->
    case ha_datastore:send_sync_master_message(Node, ProcessKey,
        #store_backup{keys = Keys, cache_requests = CacheRequests, link = {true, self()}}, true) of
        {ok, Pid} ->
            Data#data{link_status = ?SLAVE_LINKED, slave_pid = Pid};
        {badrpc, _} ->
            % TODO VFS-6295 - log to dedicated logfile
            ?debug("Cannot broadcast HA data to backup changed documents - slave down"),
            Data;
        Error ->
            ?warning("Cannot broadcast HA data because of error: ~tp", [Error]),
            Data
    end;
send_store_backup_request(_ProcessKey, Keys, CacheRequests, #data{slave_pid = Pid} = Data) ->
    % TODO VFS-6388 - maybe set slave_pid to undefined on node_down
    ha_datastore:send_async_master_message(Pid, #store_backup{keys = Keys, cache_requests = CacheRequests}),
    Data.