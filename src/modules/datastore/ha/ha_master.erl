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

-include_lib("ctool/include/logging.hrl").

%% API
-export([init_data/2]).
-export([broadcast_request_handled/3, broadcast_inactivation/2]).

-record(data, {
    exemplary_key :: datastore:key(),
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
-spec init_data(datastore:key(), [node()]) -> ha_master_data().
init_data(ExemplaryKey, BackupNodes) ->
    #data{exemplary_key = ExemplaryKey, backup_nodes = BackupNodes,
        propagation_method = ha_management:get_propagation_method()}.

%%--------------------------------------------------------------------
%% @doc
%% Sends information about request handling to backup nodes (to be used in case of failure).
%% @end
%%--------------------------------------------------------------------
-spec broadcast_request_handled(datastore_doc_batch:cached_keys(), [datastore_cache:cache_save_request()],
    ha_master_data()) -> ha_master_data().
broadcast_request_handled(_Keys, _CacheRequests, #data{backup_nodes = []} = Data) ->
    Data;
broadcast_request_handled(Keys, CacheRequests, #data{propagation_method = PM, slave_status = {SS, _}, exemplary_key = Key,
    backup_nodes = [Node | _]} = Data) when PM =:= call ; SS =:= not_linked ->
    case rpc:call(Node, datastore_writer, custom_call, [Key, {backup_request, Keys, CacheRequests}]) of
        {ok, Pid} ->
            Data#data{slave_status = {linked, Pid}};
        Error ->
            ?warning("Cannot broadcast ha data because of error: ~p", [Error]),
            Data
    end;
broadcast_request_handled(Keys, CacheRequests, #data{propagation_method = cast, slave_status = {linked, Pid}} = Data) ->
    gen_server:cast(Pid, {backup_request, Keys, CacheRequests}),
    Data.

%%--------------------------------------------------------------------
%% @doc
%% Sends information about keys inactivation to backup nodes
%% (to delete data that is not needed for HA as keys are already flushed).
%% @end
%%--------------------------------------------------------------------
-spec broadcast_inactivation(datastore_doc_batch:cached_keys(), ha_master_data()) -> ok.
broadcast_inactivation(_, #data{slave_status = {_, undefined}}) ->
    ok;
broadcast_inactivation(Inactivated, #data{slave_status = {not_linked, Pid}}) ->
    gen_server:cast(Pid, {keys_inactivated, Inactivated});
broadcast_inactivation(Inactivated, #data{slave_status = {linked, Pid}}) ->
    gen_server:cast(Pid, {keys_inactivated, Inactivated}).