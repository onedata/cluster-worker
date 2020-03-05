%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used by datastore_writer and datastore_cache_writer when ha is enabled and process
%%% works as slaves. Handles two types of activities: emergency calls (handling requests by slave when master is down)
%%% and backup calls (calls used to cache information from master used in case of failure).
%%% @end
%%%-------------------------------------------------------------------
-module(ha_slave).
-author("Michał Wrzeszcz").

-include("modules/datastore/ha.hrl").

%% API
-export([analyse_requests/2, get_mode/1, terminate_slave/1]).
-export([new_emergency_calls_data/0, report_emergency_request_handled/4, report_emergency_keys_inactivated/3]).
-export([init_data/0, handle_master_message/3, handle_slave_internal_message/2]).
-export([set_emergency_status/2, report_cache_writer_idle/1]).
-export([handle_config_msg/3]).

% record used by datastore_cache_writer to store information about emergency requests handling (when master is down)
-record(emergency_calls_data, {
    keys = sets:new() :: emergency_keys()
}).

% record user by datastore_writer to store information about data backups and handling of emergency calls
-record(slave_data, {
    % Fields used for gathering backup data
    keys_to_protect = #{} :: datastore_doc_batch:cached_keys(),
    is_linked = false :: {true, pid()} | false, % TODO - dodac wysylanie unlinka w terminate

    % Fields used to indicate working mode and help with transition between modes
    slave_mode :: ha_management:slave_mode(),
    recovered_master_pid :: pid() | undefined,

    % Emergency calls related fields (used when master is down)
    emergency_requests_status = waiting :: waiting | handling, % Status of processing emergency calls
    emergency_cache_requests = #{} :: emergency_requests_map(),
    emergency_inactivated_memory_cache_requests = #{} :: emergency_requests_map()
}).

-type ha_slave_emergency_calls_data() :: #emergency_calls_data{}.
-type ha_slave_data() :: #slave_data{}.
-type emergency_keys() :: sets:set(datastore:key()).
-type emergency_keys_list() :: datastore:key().
-type emergency_requests_map() :: #{datastore:key() => datastore_cache:cache_save_request()}.
-type slave_status() :: {ActiveRequests :: boolean(), CacheRequestsMap :: emergency_requests_map(),
    MemoryRequests :: [datastore_cache:cache_save_request()], RequestsToHandle :: datastore_writer:requests_internal()}.

-export_type([ha_slave_emergency_calls_data/0, ha_slave_data/0, emergency_keys_list/0, emergency_requests_map/0]).

% Used messages types:
-type backup_message() :: ?BACKUP_REQUEST(datastore_doc_batch:cached_keys(), [datastore_cache:cache_save_request()]) |
    ?BACKUP_REQUEST_AND_LINK(datastore_doc_batch:cached_keys(), [datastore_cache:cache_save_request()], pid()) |
    ?KEYS_INACTIVATED(datastore_doc_batch:cached_keys()).
-type check_status_request() :: ?CHECK_SLAVE_STATUS(pid()).
-type slave_emergency_internal_request() :: ?SLAVE_INTERNAL_MSG(?EMERGENCY_REQUEST_HANDLED(emergency_requests_map()) |
    ?EMERGENCY_KEYS_INACTIVATED(emergency_keys_list())).
-type master_status_message() :: ?MASTER_DOWN | ?MASTER_UP.

%%%===================================================================
%%% API - Reporting emergency calls
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes data structure used by functions handling emergency calls.
%% @end
%%--------------------------------------------------------------------
-spec new_emergency_calls_data() -> ha_slave_emergency_calls_data().
new_emergency_calls_data() ->
    #emergency_calls_data{}.

%%--------------------------------------------------------------------
%% @doc
%% Reports to datastore_worker that emergency request was handled. 
%% @end
%%--------------------------------------------------------------------
-spec report_emergency_request_handled(pid(), datastore_doc_batch:cached_keys(), [datastore_cache:cache_save_request()],
    ha_slave_emergency_calls_data()) -> ha_slave_emergency_calls_data().
report_emergency_request_handled(Pid, CachedKeys, CacheRequests, #emergency_calls_data{keys = DataKeys} = Data) ->
    DataKeys2 = sets:union(DataKeys, sets:from_list(maps:keys(CachedKeys))),
    CacheRequests2 = lists:filtermap(fun({_, Key, _} = Request) ->
        case maps:is_key(Key, CachedKeys) of
            true -> {true, {Key, Request}};
            false -> false
        end
    end, CacheRequests),
    gen_server:cast(Pid, ?SLAVE_INTERNAL_MSG(?EMERGENCY_REQUEST_HANDLED(maps:from_list(CacheRequests2)))),
    Data#emergency_calls_data{keys = DataKeys2}.

%%--------------------------------------------------------------------
%% @doc
%% Reports to datastore_worker that keys connected with emergency request were inactivated.
%% @end
%%--------------------------------------------------------------------
-spec report_emergency_keys_inactivated(pid(), datastore_doc_batch:cached_keys(), ha_slave_emergency_calls_data()) ->
    ha_slave_emergency_calls_data().
report_emergency_keys_inactivated(Pid, Inactivated, #emergency_calls_data{keys = DataKeys} = Data) ->
    KeysToReport = sets:intersection(DataKeys, sets:from_list(maps:keys(Inactivated))),
    case sets:size(KeysToReport) of
        0 ->
            Data;
        _ ->
            gen_server:cast(Pid, ?SLAVE_INTERNAL_MSG(?EMERGENCY_KEYS_INACTIVATED(KeysToReport))),
            Data#emergency_calls_data{keys = sets:subtract(DataKeys, KeysToReport)}
    end.

%%%===================================================================
%%% API - Helper functions to use ha_data and provide ha functionality
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes data structure used by functions in this module.
%% @end
%%--------------------------------------------------------------------
-spec init_data() -> ha_slave_data().
init_data() ->
    #slave_data{slave_mode = ha_management:get_slave_mode()}.

%%--------------------------------------------------------------------
%% @doc
%% Updates emergency status - marks that emergency request is being handled now in case of emergency call.
%% @end
%%--------------------------------------------------------------------
-spec set_emergency_status(ha_slave_data(), regular | emergency_call) -> ha_slave_data().
set_emergency_status(Data, emergency_call) ->
    Data#slave_data{emergency_requests_status = handling};
set_emergency_status(Data, _HandlingType) ->
    Data. % standard request - do nothing

%%--------------------------------------------------------------------
%% @doc
%% Updates emergency status when datastore_cache_writer becomes idle
%% (if it was processing emergency request, it has finished).
%% @end
%%--------------------------------------------------------------------
-spec report_cache_writer_idle(ha_slave_data()) -> ha_slave_data().
report_cache_writer_idle(Data) ->
    Data#slave_data{emergency_requests_status = waiting}.

%%--------------------------------------------------------------------
%% @doc
%% Returns information about slave mode.
%% @end
%%--------------------------------------------------------------------
-spec get_mode(ha_slave_data()) -> ha_management:slave_mode().
get_mode(#slave_data{slave_mode = Mode}) ->
    Mode.

%%--------------------------------------------------------------------
%% @doc
%% Checks if slave can be terminated (no keys to protect and no links with master) and blocks termination if needed.
%% If there are no keys to protect and process is linked to master sends unlink request.
%% @end
%%--------------------------------------------------------------------
-spec terminate_slave(ha_slave_data()) -> {terminate | retry | schedule, ha_slave_data()}.
terminate_slave(#slave_data{is_linked = false, keys_to_protect = Keys} = Data) ->
    case maps:size(Keys) of
        0 -> {terminate, Data};
        _ -> {schedule, Data}
    end;
terminate_slave(#slave_data{is_linked = {true, Pid}, keys_to_protect = Keys} = Data) ->
    case maps:size(Keys) of
        0 ->
            catch gen_server:call(Pid, ?REQUEST_UNLINK, infinity),
            {retry, Data#slave_data{is_linked = false}};
        _ ->
            {schedule, Data}
    end.

%%%===================================================================
%%% API - messages handling by datastore_writer
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Analyses requests and provides information about expected further actions.
%% @end
%%--------------------------------------------------------------------
-spec analyse_requests(datastore_writer:requests_internal(), slave_mode) ->
    {regular, RegularReversed :: datastore_writer:requests_internal()} |
    {emergency_call, RegularReversed :: datastore_writer:requests_internal(),
        EmergencyReversed :: datastore_writer:requests_internal()} |
    {proxy_call, RegularReversed :: datastore_writer:requests_internal(),
        EmergencyReversed :: datastore_writer:requests_internal(), node()}.
analyse_requests(Requests, Mode) ->
    {RegularReversed, EmergencyReversed, ProxyNode} = clasify_requests(Requests),

    case {ProxyNode, Mode} of
        {undefined, _} ->
            {regular, RegularReversed};
        {_, backup} ->
            {proxy_call, RegularReversed, EmergencyReversed, ProxyNode};
        _ ->
            {emergency_call, RegularReversed, EmergencyReversed}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles master messages for datastore_writer.
%% @end
%%--------------------------------------------------------------------
-spec handle_master_message(backup_message() | check_status_request(), ha_slave_data(),
    datastore_writer:requests_internal()) ->
    {ok | slave_status(), ha_slave_data(), datastore_writer:requests_internal()}.
% Calls connecting with backup creation/deletion
handle_master_message(?BACKUP_REQUEST(Keys, CacheRequests), #slave_data{keys_to_protect = DataKeys} = SlaveData, WaitingRequests) ->
    datastore_cache:save(CacheRequests),
    {ok, SlaveData#slave_data{keys_to_protect = maps:merge(DataKeys, Keys)}, WaitingRequests};
handle_master_message(?BACKUP_REQUEST_AND_LINK(Keys, CacheRequests, Pid), #slave_data{keys_to_protect = DataKeys} = SlaveData, WaitingRequests) ->
    datastore_cache:save(CacheRequests),
    {ok, SlaveData#slave_data{keys_to_protect = maps:merge(DataKeys, Keys), is_linked = {true, Pid}}, WaitingRequests};
handle_master_message(?KEYS_INACTIVATED(Inactivated), #slave_data{keys_to_protect = DataKeys} = SlaveData, WaitingRequests) ->
    datastore_cache:inactivate(Inactivated),
    {ok, SlaveData#slave_data{keys_to_protect = maps:without(maps:keys(Inactivated), DataKeys)}, WaitingRequests};

% Checking slave status
handle_master_message(?CHECK_SLAVE_STATUS(Pid), #slave_data{emergency_requests_status = Status,
    emergency_cache_requests = CacheRequests, emergency_inactivated_memory_cache_requests = MemoryRequests} = SlaveData,
    WaitingRequests) ->
    {LocalReversed, RemoteReversed, _ProxyNode} = clasify_requests(WaitingRequests),
    {{Status =:= handling, CacheRequests, maps:values(MemoryRequests), lists:reverse(RemoteReversed)},
        SlaveData#slave_data{recovered_master_pid = Pid, emergency_inactivated_memory_cache_requests = #{}},
        lists:reverse(LocalReversed)}.

%%--------------------------------------------------------------------
%% @doc
%% Handles internal requests from datastore_cache_writer (by datastore_writer).
%% @end
%%--------------------------------------------------------------------
-spec handle_slave_internal_message(slave_emergency_internal_request(), ha_slave_data()) -> ha_slave_data().
handle_slave_internal_message(?SLAVE_INTERNAL_MSG(?EMERGENCY_REQUEST_HANDLED(CacheRequests)), #slave_data{
    recovered_master_pid = Pid, emergency_cache_requests = CR}  = SlaveData) ->
    SlaveData2 = SlaveData#slave_data{emergency_cache_requests = maps:merge(CR, CacheRequests)},
    case Pid of
        undefined ->
            SlaveData2;
        _ ->
            gen_server:cast(Pid, ?SLAVE_MSG(?EMERGENCY_REQUEST_HANDLED(CacheRequests))),
            SlaveData2
    end;
handle_slave_internal_message(?SLAVE_INTERNAL_MSG(?EMERGENCY_KEYS_INACTIVATED(CrashedNodeKeys)),
    #slave_data{emergency_cache_requests = CR, emergency_inactivated_memory_cache_requests = ICR,
        recovered_master_pid = Pid} = SlaveData) ->
    CrashedNodeKeysList = sets:to_list(CrashedNodeKeys),
    CR2 = maps:without(CrashedNodeKeysList, CR),
    SlaveData2 = SlaveData#slave_data{emergency_cache_requests = CR2},

    case Pid of
        undefined ->
            Finished = maps:with(CrashedNodeKeysList, CR),
            FinishedMemory = maps:filter(fun
                (_, {#{disc_driver := DD}, _, _}) -> DD =:= undefined;
                (_, _) -> true
            end, Finished),
            SlaveData2#slave_data{emergency_inactivated_memory_cache_requests = maps:merge(ICR, FinishedMemory)};
        _ ->
            gen_server:cast(Pid, ?SLAVE_MSG(?EMERGENCY_KEYS_INACTIVATED(CrashedNodeKeysList))),
            SlaveData2
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles configuration messages (changes of master status).
%% @end
%%--------------------------------------------------------------------
-spec handle_config_msg(master_status_message(), ha_slave_data(), pid()) -> ha_slave_data().
handle_config_msg(?MASTER_DOWN, #slave_data{keys_to_protect = Keys} = Data, Pid) ->
    gen_server:call(Pid, ?MASTER_DOWN(Keys), infinity), % VFS-6169 - mark flushed keys in case of fast master restart
    Data#slave_data{keys_to_protect = #{}, recovered_master_pid = undefined, slave_mode = processing};
handle_config_msg(?MASTER_UP, Data, _Pid) ->
    Data#slave_data{slave_mode = backup}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec clasify_requests(datastore_writer:requests_internal()) -> {LocalReversed :: datastore_writer:requests_internal(),
    RemoteReversed :: datastore_writer:requests_internal(), MasterNodeToBeUsed :: node() | undefined}.
clasify_requests(Requests) ->
    % TODO - VFS-6168 - maybe do not execute when HA is off
    % TODO - VFS-6169 - what if local node is broken node according to Ctx
    MyNode = node(),

    {LocalReversed, RemoteReversed, FinalMaster} = lists:foldl(fun
        ({_Pid, _Ref, {_Function, [#{broken_master := true, broken_nodes := [Master | _]} | _Args]}} =
            Request, {Local, Remote, _}) when Master =/= MyNode ->
            {Local, [Request | Remote], Master};
        (Request, {Local, Remote, Master}) ->
            {[Request | Local], Remote, Master}
    end, {[], [], undefined}, Requests),
    {LocalReversed, RemoteReversed, FinalMaster}.