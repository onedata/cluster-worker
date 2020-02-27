%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used by datastore writer and cache writer when ha is enabled and processes
%%% work as slaves (requests are processed on other node and slave is used to provide ha or when master is down).
%%% Handles two types of activities: emergency calls (handling requests by slave when master is down) and 
%%% backup calls (calls used to cache information from master used in case of failure).
%%% @end
%%%-------------------------------------------------------------------
-module(ha_slave).
-author("Michał Wrzeszcz").

-include("modules/datastore/ha.hrl").

%% API
-export([analyse_requests/3, get_mode/1]).
-export([new_emergency_calls_data/0, report_emergency_request_handled/4, report_emergency_keys_inactivated/3]).
-export([init_data/0, handle_master_message/3, handle_slave_internal_message/2]).
-export([set_emergency_status/2, report_cache_writer_idle/1]).
-export([handle_config_msg/3]).

-record(emergency_calls_data, {
    keys = sets:new() :: emergency_keys()
}).

-record(slave_data, {
    % Fields used for gathering backup data
    keys_to_protect = #{} :: datastore_doc_batch:cached_keys(),
    is_linked = false :: {true, pid()} | false, % TODO - dodac wysylanie unlinka w terminate

    % Fields used to indicate working mode and help with transition between modes
    slave_mode :: ha_management:slave_mode(),
    recovered_master_pid :: pid() | undefined,

    % Emergency calls related fields (used when master is down)
    emergency_requests_status = waiting :: waiting | handling, % Status of processing emergency calls
    emergency_keys = sets:new(),
    emergency_cache_requests = []
}).

-type ha_slave_emergency_calls_data() :: #emergency_calls_data{}.
-type ha_slave_data() :: #slave_data{}.
-type emergency_keys() :: sets:set(datastore:key()).
% Status of slave: ok, waiting for memory operation to be finished or waiting for flush.
-type slave_status() :: ok |
    {wait_cache, datastore_cache:cache_save_request(), RemoteRequests :: datastore_writer:requests_internal()} |
    {wait_disc, datastore_cache:cache_save_request(), RemoteRequests :: datastore_writer:requests_internal()}.

-export_type([ha_slave_emergency_calls_data/0, ha_slave_data/0, emergency_keys/0]).

% Used messages types:
-type backup_message() :: ?BACKUP_REQUEST(datastore_doc_batch:cached_keys(), [datastore_cache:cache_save_request()]) |
    ?BACKUP_REQUEST_AND_LINK(datastore_doc_batch:cached_keys(), [datastore_cache:cache_save_request()], pid()) |
    ?KEYS_INACTIVATED(datastore_doc_batch:cached_keys()).
-type check_status_request() :: ?CHECK_SLAVE_STATUS(pid()).
-type slave_emergency_internal_request() :: ?SLAVE_INTERNAL_MSG(
    ?EMERGENCY_REQUEST_HANDLED(emergency_keys(), [datastore_cache:cache_save_request()]) |
    ?EMERGENCY_KEYS_INACTIVATED(emergency_keys())).
-type master_status_message() :: ?MASTER_DOWN | ?MASTER_UP.

%%%===================================================================
%%% API - Emergency calls
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
report_emergency_request_handled(Pid, Keys, CacheRequests, #emergency_calls_data{keys = DataKeys} = Data) ->
    DataKeys2 = sets:union(DataKeys, sets:from_list(maps:keys(Keys))),
    gen_server:cast(Pid, ?SLAVE_INTERNAL_MSG(?EMERGENCY_REQUEST_HANDLED(DataKeys2, CacheRequests))),
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
%% Analyses requests and provides information about expected fruher acstions.
%% @end
%%--------------------------------------------------------------------
-spec analyse_requests(datastore_writer:requests_internal(), slave_mode, datastore:key()) ->
    {regular, RegularReversed :: datastore_writer:requests_internal()} |
    {emergency_call, RegularReversed :: datastore_writer:requests_internal(),
        EmergencyReversed :: datastore_writer:requests_internal()}.
analyse_requests(Requests, Mode, Key) ->
    {RegularReversed, EmergencyReversed, ProxyNode} = clasify_requests(Requests),

    case {ProxyNode, Mode} of
        {undefined, _} ->
            {regular, RegularReversed};
        {_, backup} ->
            rpc:call(ProxyNode, datastore_writer, call_async, [Key, ?PROXY_REQUESTS(lists:reverse(EmergencyReversed))]),
            {regular, RegularReversed};
        _ ->
            {emergency_call, RegularReversed, EmergencyReversed}
    end.

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
%% Updates emergency status.
%% @end
%%--------------------------------------------------------------------
-spec set_emergency_status(ha_slave_data(), regular | emergency_call) -> ha_slave_data().
set_emergency_status(Data, emergency_call) ->
    Data#slave_data{emergency_requests_status = handling};
set_emergency_status(Data, _HandlingType) ->
    Data.

%%--------------------------------------------------------------------
%% @doc
%% Updates emergency status when datastore_cache_writer becomes idle.
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

%%%===================================================================
%%% API - messages handling by datastore_writer
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles masters for datastore_writer.
%% @end
%%--------------------------------------------------------------------
-spec handle_master_message(backup_message() | check_status_request(), ha_slave_data(),
    datastore_writer:requests_internal()) ->
    {ok | {reply, slave_status()}, ha_slave_data(), datastore_writer:requests_internal()}.
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
handle_master_message(?CHECK_SLAVE_STATUS(Pid), #slave_data{emergency_requests_status = Status, emergency_keys = Keys,
    emergency_cache_requests = CacheRequests} = SlaveData, WaitingRequests) ->
    case {Status, sets:size(Keys)} of
        {handling, _} ->
            {LocalReversed, RemoteReversed, _ProxyNode} = clasify_requests(WaitingRequests),
            {{reply, {wait_cache, CacheRequests, lists:reverse(RemoteReversed)}},
                SlaveData#slave_data{recovered_master_pid = Pid}, lists:reverse(LocalReversed)};
        {_, 0} -> {{reply, ok}, SlaveData, WaitingRequests};
        _ -> {{reply, {wait_disc, CacheRequests, Keys}}, SlaveData#slave_data{recovered_master_pid = Pid}, WaitingRequests}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles internal requests from datastore_cache_writer (by datastore_writer).
%% @end
%%--------------------------------------------------------------------
-spec handle_slave_internal_message(slave_emergency_internal_request(), ha_slave_data()) -> ha_slave_data().
handle_slave_internal_message(?SLAVE_INTERNAL_MSG(?EMERGENCY_REQUEST_HANDLED(Keys, CacheRequests)), #slave_data{emergency_keys = Keys0,
    recovered_master_pid = Pid, emergency_cache_requests = CR}  = SlaveData) ->
    case Pid of
        undefined ->
            SlaveData#slave_data{emergency_keys = sets:union(Keys0, Keys),
                emergency_cache_requests = CR ++ CacheRequests}; % TODO filtrowac takie same klucze
        _ ->
            gen_server:cast(Pid, ?SLAVE_MSG(?EMERGENCY_REQUEST_HANDLED(Keys, CacheRequests))),
            SlaveData#slave_data{emergency_keys = sets:union(Keys0, Keys)}
    end;
handle_slave_internal_message(?SLAVE_INTERNAL_MSG(?EMERGENCY_KEYS_INACTIVATED(CrashedNodeKeys)), #slave_data{emergency_keys = Keys,
    emergency_cache_requests = CR, recovered_master_pid = Pid} = SlaveData) ->
    gen_server:cast(Pid, ?SLAVE_MSG(?EMERGENCY_KEYS_INACTIVATED(sets:to_list(CrashedNodeKeys)))),
    CR2 = lists:filter(fun({_, Key, _}) -> not sets:is_element(Key, CrashedNodeKeys) end, CR),
    SlaveData#slave_data{emergency_keys = sets:subtract(Keys, CrashedNodeKeys), emergency_cache_requests = CR2}.

%%--------------------------------------------------------------------
%% @doc
%% Handles configuration messages (changes of master status).
%% @end
%%--------------------------------------------------------------------
-spec handle_config_msg(master_status_message(), ha_slave_data(), pid()) -> ha_slave_data().
handle_config_msg(?MASTER_DOWN, #slave_data{keys_to_protect = Keys} = Data, Pid) ->
    gen_server:call(Pid, ?MASTER_DOWN(Keys), infinity), % TODO - trzeba zaznaczyc ze flushujemy na wypadek powrotu node;a
    Data#slave_data{keys_to_protect = #{}, recovered_master_pid = undefined, slave_mode = processing};
handle_config_msg(?MASTER_UP, Data, _Pid) ->
    Data#slave_data{slave_mode = backup}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec clasify_requests(datastore_writer:requests_internal()) -> {LocalReversed :: datastore_writer:requests_internal(),
    RemoteReversed :: datastore_writer:requests_internal(), MasterNodeToBeUsed :: node() | undefined}.
clasify_requests(Requests) ->
    % TODO - nie filtrowac jak nie ma zmienne backup_enabled (pobierac ja do stanu na poczatku)
    MyNode = node(),
    {LocalReversed, RemoteReversed, FinalMaster} = lists:foldl(fun
        ({_Pid, _Ref, {_Function, [#{broken_nodes := [Master | _]} | _Args]}} = Request, {Local, Remote, _}) when Master =/= MyNode ->
            {Local, [Request | Remote], Master};
        (Request, {Local, Remote, Master}) ->
            {[Request | Local], Remote, Master}
    end, {[], [], undefined}, Requests),
    {LocalReversed, RemoteReversed, FinalMaster}.