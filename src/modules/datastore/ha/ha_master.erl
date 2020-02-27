%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used by datastore cache writer when ha is enabled and process
%%% works as master (processes requests).
%%% @end
%%%-------------------------------------------------------------------
-module(ha_master).
-author("Michał Wrzeszcz").

-include("modules/datastore/ha.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_data/1]).
-export([broadcast_request_handled/4, broadcast_inactivation/2]).
-export([check_slave/2, handle_slave_message/2, handle_slave_lifecycle_message/2, handle_internal_message/2]).
-export([handle_config_msg/2]).

-record(data, {
    backup_nodes = [] :: [node()], % currently only single backup node is supported
    slave_status = {not_linked, undefined} :: slave_status(),
    propagation_method :: ha_management:propagation_method()
}).

-type ha_master_data() :: #data{}.
-type slave_status() :: {linked | not_linked, undefined | pid()}.

-export_type([ha_master_data/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes data structure used by functions in this module.
%% @end
%%--------------------------------------------------------------------
-spec init_data([node()]) -> ha_master_data().
init_data(BackupNodes) -> #data{backup_nodes = BackupNodes, propagation_method = ha_management:get_propagation_method()}.

%%--------------------------------------------------------------------
%% @doc
%% Sends information about request handling to backup nodes (to be used in case of failure).
%% @end
%%--------------------------------------------------------------------
-spec broadcast_request_handled(datastore:key(), datastore_doc_batch:cached_keys(),
    [datastore_cache:cache_save_request()], ha_master_data()) -> ha_master_data().
broadcast_request_handled(_ProcessKey, _Keys, _CacheRequests, #data{backup_nodes = []} = Data) ->
    Data;
broadcast_request_handled(ProcessKey, Keys, CacheRequests, #data{propagation_method = call,
    backup_nodes = [Node | _]} = Data) ->
    case rpc:call(Node, datastore_writer, custom_call, [ProcessKey, ?BACKUP_REQUEST(Keys, CacheRequests)]) of
        {ok, Pid} ->
            Data#data{slave_status = {not_linked, Pid}};
        Error ->
            ?warning("Cannot broadcast ha data because of error: ~p", [Error]),
            Data
    end;
broadcast_request_handled(ProcessKey, Keys, CacheRequests, #data{slave_status = {not_linked, _},
    backup_nodes = [Node | _]} = Data) ->
    case rpc:call(Node, datastore_writer, custom_call, [ProcessKey, ?BACKUP_REQUEST_AND_LINK(Keys, CacheRequests, self())]) of
        {ok, Pid} ->
            Data#data{slave_status = {linked, Pid}};
        Error ->
            ?warning("Cannot broadcast ha data because of error: ~p", [Error]),
            Data
    end;
broadcast_request_handled(_ProcessKey, Keys, CacheRequests, #data{slave_status = {linked, Pid}} = Data) ->
    gen_server:cast(Pid, ?BACKUP_REQUEST(Keys, CacheRequests)),
    Data.

%%--------------------------------------------------------------------
%% @doc
%% Sends information about keys inactivation to backup nodes
%% (to delete data that is not needed for HA as keys are already flushed).
%% @end
%%--------------------------------------------------------------------
-spec broadcast_inactivation(datastore_doc_batch:cached_keys(), ha_master_data()) -> ok.
broadcast_inactivation(_, #data{slave_status = {_, undefined}}) ->
    ?error("Inactivation request without slave defined"),
    ok;
broadcast_inactivation(Inactivated, #data{slave_status = {_, Pid}}) ->
    gen_server:cast(Pid, ?KEYS_INACTIVATED(Inactivated)).

%%--------------------------------------------------------------------
%% @doc
%% Verifies slave state during start of process that works as master.
%% @end
%%--------------------------------------------------------------------
-spec check_slave(datastore:key(), [node()]) -> {ActiveRequests :: {true, list()} | false, [datastore:key()]}.
check_slave(Key, BackupNodes) ->
    case BackupNodes of
        [Node | _] ->
            case rpc:call(Node, datastore_writer, custom_call, [Key, ?CHECK_SLAVE_STATUS(self())]) of
                ok ->
                    {false, []};
                {wait_cache, CacheRequests, RequestsToHandle} ->
                    datastore_cache:save(CacheRequests),
                    {{true, RequestsToHandle}, []};
                {wait_disc, CacheRequests, Keys} ->
                    datastore_cache:save(CacheRequests),
                    {false, sets:to_list(Keys)}
            end;
        _ ->
            {false, []}
    end.

handle_slave_message(?PROXY_REQUESTS(NewRequests), _Pid) ->
    {schedule, NewRequests};
handle_slave_message(?SLAVE_MSG(?EMERGENCY_REQUEST_HANDLED(Keys, CacheRequests)), Pid) ->
    datastore_cache:save(CacheRequests),
    gen_server:cast(Pid, ?MASTER_INTERNAL_MSG(?EMERGENCY_REQUEST_HANDLED(Keys))),
    idle;
handle_slave_message(?SLAVE_MSG(?EMERGENCY_KEYS_INACTIVATED(CrashedNodeKeys)), Pid) ->
    gen_server:cast(Pid, ?MASTER_INTERNAL_MSG(?EMERGENCY_KEYS_INACTIVATED(CrashedNodeKeys))),
    ignore.





handle_slave_lifecycle_message(?REQUEST_UNLINK, #data{slave_status = {_, Pid}} = Data) ->
    Data#data{slave_status = {not_linked, Pid}}.



% TODO - dodac to do handle_call
handle_internal_message(?CONFIGURE_BACKUP, Data) ->
    BackupNodes = ha_management:get_backup_nodes(),
    PropagationMethod = ha_management:get_propagation_method(),
    Data#data{backup_nodes = BackupNodes, propagation_method = PropagationMethod};

handle_internal_message(?MASTER_INTERNAL_MSG(?EMERGENCY_REQUEST_HANDLED(Keys)), KiF) ->
    NewKiF = lists:map(fun(Key) -> {Key, {slave_flush, undefined}} end, Keys),
    maps:merge(NewKiF, KiF);

handle_internal_message(?MASTER_INTERNAL_MSG(?EMERGENCY_KEYS_INACTIVATED(CrashedNodeKeys)), KiF) ->
    maps:without(CrashedNodeKeys, KiF).

handle_config_msg(?CONFIG_CHANGED, Pid) ->
    gen_server:call(Pid, ?CONFIGURE_BACKUP, infinity).
