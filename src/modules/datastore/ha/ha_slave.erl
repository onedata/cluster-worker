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

%% API
-export([new_emergency_calls_data/0, report_emergency_request_handled/4, report_emergency_keys_inactivated/3]).

-record(emergency_calls_data, {
    keys = sets:new() :: sets:set(datastore:key())
}).

-type emergency_calls_data() :: #emergency_calls_data{}.

-export_type([emergency_calls_data/0]).

%%%===================================================================
%%% API - Emergency calls
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes data structure used by functions handling emergency calls.
%% @end
%%--------------------------------------------------------------------
-spec new_emergency_calls_data() -> emergency_calls_data().
new_emergency_calls_data() ->
    #emergency_calls_data{}.

%%--------------------------------------------------------------------
%% @doc
%% Reports to datastore_worker that emergency request was handled. 
%% @end
%%--------------------------------------------------------------------
-spec report_emergency_request_handled(pid(), datastore_doc_batch:cached_keys(), [datastore_cache:cache_save_request()],
    emergency_calls_data()) -> emergency_calls_data().
report_emergency_request_handled(Pid, Keys, CacheRequests, #emergency_calls_data{keys = DataKeys} = Data) ->
    DataKeys2 = sets:union(DataKeys, sets:from_list(maps:keys(Keys))),
    gen_server:cast(Pid, {emergency_request_handled, DataKeys2, CacheRequests}),
    Data#emergency_calls_data{keys = DataKeys2}.

%%--------------------------------------------------------------------
%% @doc
%% Reports to datastore_worker that keys connected with emergency request were inactivated.
%% @end
%%--------------------------------------------------------------------
-spec report_emergency_keys_inactivated(pid(), datastore_doc_batch:cached_keys(), emergency_calls_data()) ->
    emergency_calls_data().
report_emergency_keys_inactivated(Pid, Inactivated, #emergency_calls_data{keys = DataKeys} = Data) ->
    KeysToReport = sets:intersection(DataKeys, sets:from_list(maps:keys(Inactivated))),
    case sets:size(KeysToReport) of
        0 ->
            Data;
        _ ->
            gen_server:cast(Pid, {keys_inactivated, KeysToReport}),
            Data#emergency_calls_data{keys = sets:subtract(DataKeys, KeysToReport)}
    end.

%%%===================================================================
%%% API - Backup calls
%%%===================================================================