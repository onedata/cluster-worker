%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used by datastore_writer and
%%% datastore_cache_writer when HA is enabled and tp process
%%% works as slave. Handles two types of activities:
%%% working in failover mode (handling requests by slave when master is down)
%%% and handling backup calls (calls used to cache information from master
%%% used in case of failure).
%%% For more information see ha_datastore.hrl.
%%% @end
%%%-------------------------------------------------------------------
-module(ha_slave).
-author("Michał Wrzeszcz").

-include("modules/datastore/ha_datastore.hrl").
-include("modules/datastore/datastore_protocol.hrl").

%% API
-export([qualify_and_reverse_requests/2, get_mode/1, can_be_terminated/1]).
-export([init_failover_requests_data/0, report_failover_request_handled/4, report_keys_flushed/3]).
-export([init_data/0, handle_master_message/3, handle_internal_message/2]).
-export([set_failover_request_handling/2]).
-export([handle_management_msg/3]).

% record used by datastore_cache_writer to store information working in failover mode (when master is down)
-record(failover_requests_data, {
    keys = sets:new() :: keys_set()
}).

% record used by datastore_writer to store information about data backups and
% working in failover mode (when master is down)
-record(slave_data, {
    % Fields used for gathering backup data
    backup_keys = #{} :: datastore_doc_batch:cached_keys(),
    link_to_master = false :: link_to_master(), % TODO VFS-6197 - unlink HA slave during forced termination

    % Fields used to indicate working mode and help with transition between modes
    slave_mode :: ha_datastore:slave_mode(),
    recovered_master_pid :: pid() | undefined,

    % Fields related with work in failover mode (master is down)
    failover_request_handling = false :: boolean(), % true if failover request is currently being handled
    failover_pending_cache_requests = #{} :: cache_requests_map(),
    failover_finished_memory_cache_requests = #{} :: cache_requests_map()
}).

-type ha_failover_requests_data() :: #failover_requests_data{}.
-type ha_slave_data() :: #slave_data{}.
-type link_to_master() :: {true, pid()} | false. % status of link between master and slave (see ha_datastore.hrl)
                                                 % if processes are linked, the pid of master is part of status
-type keys_set() :: sets:set(datastore:key()).
-type cache_requests_map() :: #{datastore:key() => datastore_cache:cache_save_request()}.
-type slave_failover_status() :: #slave_failover_status{}.

-export_type([ha_failover_requests_data/0, ha_slave_data/0, link_to_master/0, keys_set/0, cache_requests_map/0]).

% Used messages types:
-type backup_message() :: #store_backup{} | #forget_backup{}.
-type get_slave_failover_status() :: #get_slave_failover_status{}.
-type master_node_status_message() :: ?MASTER_DOWN | ?MASTER_UP.

-export_type([backup_message/0, get_slave_failover_status/0, master_node_status_message/0]).

%%%===================================================================
%%% API - Working in failover mode
%%%===================================================================

-spec init_failover_requests_data() -> ha_failover_requests_data().
init_failover_requests_data() ->
    #failover_requests_data{}.


-spec report_failover_request_handled(pid(), datastore_doc_batch:cached_keys(), [datastore_cache:cache_save_request()],
    ha_failover_requests_data()) -> ha_failover_requests_data().
report_failover_request_handled(Pid, CachedKeys, CacheRequests, #failover_requests_data{keys = DataKeys} = Data) ->
    DataKeys2 = sets:union(DataKeys, sets:from_list(maps:keys(CachedKeys))),
    CacheRequests2 = lists:map(fun({_, Key, _} = Request) -> {Key, Request} end, CacheRequests),
    ha_datastore:send_async_internal_message(Pid,
        #failover_request_data_processed{request_handled = true, cache_requests_saved = maps:from_list(CacheRequests2)}),
    Data#failover_requests_data{keys = DataKeys2}.


-spec report_keys_flushed(pid(), datastore_doc_batch:cached_keys(), ha_failover_requests_data()) ->
    ha_failover_requests_data().
report_keys_flushed(Pid, Inactivated, #failover_requests_data{keys = DataKeys} = Data) ->
    KeysToReport = sets:intersection(DataKeys, sets:from_list(maps:keys(Inactivated))),
    case sets:size(KeysToReport) of
        0 ->
            Data;
        _ ->
            ha_datastore:send_async_internal_message(Pid,
                #failover_request_data_processed{keys_flushed = KeysToReport}),
            Data#failover_requests_data{keys = sets:subtract(DataKeys, KeysToReport)}
    end.

%%%===================================================================
%%% API - Helper functions to use ha_data and provide HA functionality
%%%===================================================================

-spec init_data() -> ha_slave_data().
init_data() ->
    #slave_data{slave_mode = ha_datastore:get_slave_mode()}.

-spec set_failover_request_handling(ha_slave_data(), boolean()) -> ha_slave_data().
set_failover_request_handling(Data, FailoverRequestHandling) ->
    Data#slave_data{failover_request_handling = FailoverRequestHandling}.

-spec get_mode(ha_slave_data()) -> ha_datastore:slave_mode().
get_mode(#slave_data{slave_mode = Mode}) ->
    Mode.

%%--------------------------------------------------------------------
%% @doc
%% Splits requests into local and remote requests groups.
%% Also reverses requests as requests are stored in revered list.
%% @end
%%--------------------------------------------------------------------
-spec qualify_and_reverse_requests(datastore_writer:requests_internal(), ha_datastore:slave_mode()) ->
    #qualified_datastore_requests{}.
qualify_and_reverse_requests(Requests, Mode) ->
    % TODO - VFS-6168 - maybe do not execute when HA is off
    % TODO - VFS-6169 - what if local node is broken node according to Ctx
    MyNode = node(),

    {LocalList, RemoteList, RemoteNode} = lists:foldl(fun
        (#datastore_internal_request{request = #datastore_request{
            ctx = #{failed_master := true, failed_nodes := [Master | _]}}} =
            Request, {Local, Remote, _}) when Master =/= MyNode ->
            {Local, [Request | Remote], Master};
        (Request, {Local, Remote, Node}) ->
            {[Request | Local], Remote, Node}
    end, {[], [], undefined}, Requests),

    RemoteMode = case {RemoteList, Mode} of
        {[], _} -> ?IGNORE;
        {_, ?STANDBY_SLAVE_MODE} -> ?DELEGATE;
        {_, ?FAILOVER_SLAVE_MODE} -> ?HANDLE_LOCALLY
    end,

    #qualified_datastore_requests{local_requests = LocalList, remote_requests = RemoteList, remote_node = RemoteNode,
        remote_processing_mode = RemoteMode}.

%%--------------------------------------------------------------------
%% @doc
%% Checks if slave can be terminated (no keys to protect and no links with master) and blocks termination if needed.
%% If there are no keys to protect and process is linked to master sends unlink request.
%% @end
%%--------------------------------------------------------------------
-spec can_be_terminated(ha_slave_data()) -> {terminate | retry | delay_termination, ha_slave_data()}.
can_be_terminated(#slave_data{link_to_master = false, backup_keys = Keys} = Data) ->
    case maps:size(Keys) of
        0 -> {terminate, Data};
        _ -> {delay_termination, Data}
    end;
can_be_terminated(#slave_data{link_to_master = {true, Pid}, backup_keys = Keys} = Data) ->
    case maps:size(Keys) of
        0 ->
            catch ha_datastore:send_sync_slave_message(Pid, ?REQUEST_UNLINK),
            {retry, Data#slave_data{link_to_master = false}};
        _ ->
            {delay_termination, Data}
    end.

%%%===================================================================
%%% API - messages handling by datastore_writer
%%%===================================================================

-spec handle_master_message(backup_message() | get_slave_failover_status(), ha_slave_data(),
    datastore_writer:requests_internal()) ->
    {ok | slave_failover_status(), ha_slave_data(), datastore_writer:requests_internal()}.
% Calls associated with backup creation/deletion
handle_master_message(#store_backup{keys = Keys, cache_requests = CacheRequests, link = Link},
    #slave_data{backup_keys = DataKeys} = SlaveData, WaitingRequests) ->
    datastore_cache:save(CacheRequests),
    SlaveData2 = case Link of
        {true, _} -> SlaveData#slave_data{link_to_master = Link};
        _ -> SlaveData
    end,
    {ok, SlaveData2#slave_data{backup_keys = maps:merge(DataKeys, Keys)}, WaitingRequests};
handle_master_message(#forget_backup{keys = Inactivated}, #slave_data{backup_keys = DataKeys} = SlaveData, WaitingRequests) ->
    datastore_cache:inactivate(Inactivated),
    {ok, SlaveData#slave_data{backup_keys = maps:without(maps:keys(Inactivated), DataKeys)}, WaitingRequests};

% Checking slave status
handle_master_message(#get_slave_failover_status{answer_to = Pid}, #slave_data{failover_request_handling = Status,
    failover_pending_cache_requests = CacheRequests, failover_finished_memory_cache_requests = MemoryRequests,
    slave_mode = Mode} = SlaveData,
    WaitingRequests) ->
    #qualified_datastore_requests{local_requests = LocalReversed, remote_requests = RemoteReversed} =
        qualify_and_reverse_requests(WaitingRequests, Mode),
    {#slave_failover_status{is_handling_requests = Status, ending_cache_requests = CacheRequests,
        finished_memory_cache_requests = maps:values(MemoryRequests),
        requests_to_handle = lists:reverse(RemoteReversed)},
        SlaveData#slave_data{recovered_master_pid = Pid, failover_finished_memory_cache_requests = #{}},
        lists:reverse(LocalReversed)}.


-spec handle_internal_message(ha_master:failover_request_data_processed_message(), ha_slave_data()) -> ha_slave_data().
handle_internal_message(#failover_request_data_processed{cache_requests_saved = CacheRequests,
    keys_flushed = FlushedKeys} = Msg, #slave_data{recovered_master_pid = Pid, failover_pending_cache_requests = CR,
    failover_finished_memory_cache_requests = ICR}  = SlaveData) ->
    FlushedKeysList = sets:to_list(FlushedKeys),
    CR2 = maps:without(FlushedKeysList, maps:merge(CR, CacheRequests)),
    SlaveData2 = SlaveData#slave_data{failover_pending_cache_requests = CR2},
    case Pid of
        undefined ->
            Finished = maps:with(FlushedKeysList, CR),
            FinishedMemory = maps:filter(fun
                (_, {#{disc_driver := DD}, _, _}) -> DD =:= undefined;
                (_, _) -> true
            end, Finished),
            SlaveData2#slave_data{failover_finished_memory_cache_requests = maps:merge(ICR, FinishedMemory)};
        _ ->
            ha_datastore:send_async_slave_message(Pid, Msg),
            SlaveData2
    end.

-spec handle_management_msg(master_node_status_message(), ha_slave_data(), pid()) -> ha_slave_data().
handle_management_msg(?CONFIG_CHANGED, Data, Pid) ->
    ha_datastore:send_sync_internal_message(Pid, ?CONFIG_CHANGED),
    Data;
handle_management_msg(?MASTER_DOWN, #slave_data{backup_keys = Keys} = Data, Pid) ->
    datastore_cache_writer:call(Pid, #datastore_flush_request{keys = Keys}), % VFS-6169 - mark flushed keys in case of fast master restart
    Data#slave_data{backup_keys = #{}, recovered_master_pid = undefined, slave_mode = ?FAILOVER_SLAVE_MODE};
handle_management_msg(?MASTER_UP, Data, _Pid) ->
    Data#slave_data{slave_mode = ?STANDBY_SLAVE_MODE}.