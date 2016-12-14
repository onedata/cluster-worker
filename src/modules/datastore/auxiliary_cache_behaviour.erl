%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Behaviour for datastore driver that implements auxiliary cache.
%%% @end
%%%-------------------------------------------------------------------
-module(auxiliary_cache_behaviour).
-author("Jakub Kudzia").


%%--------------------------------------------------------------------
%% @doc
%% Creates auxiliary tables that would allow to iterate in order by fields
%% given in list of keys of map #model_config.auxiliary_tables
%% @end
%%--------------------------------------------------------------------
-callback create_auxiliary_caches(model_behaviour:model_config(),
    Fields :: [atom()], NodeToSync :: node()) ->
    ok | datastore:generic_error() | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Deletes document with key Key in auxiliary_cache of Model connected
%% with Field.
%% @end
%%--------------------------------------------------------------------
-callback aux_delete(
    Model :: model_behaviour:model_config(),
    Field :: atom(),
    Args :: [term()]) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Saves document with key Key in auxiliary_cache of Model connected
%% with Field.
%% @end
%%--------------------------------------------------------------------
-callback aux_save(
    Model :: model_behaviour:model_config(),
    Field :: atom(),
    Args :: [term()]) -> ok.


%%--------------------------------------------------------------------
%% @doc
%% Updates document with key Key in auxiliary_cache of Model connected
%% with Field.
%% @end
%%--------------------------------------------------------------------
-callback aux_update(
    Model :: model_behaviour:model_config(),
    Field :: atom(),
    Args :: [term()]) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Creates document with key Key in auxiliary_cache of Model connected
%% with Field.
%% @end
%%--------------------------------------------------------------------
-callback aux_create(
    Model :: model_behaviour:model_config(),
    Field :: atom(),
    Args :: [term()]) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns first key to iterate over auxiliary ordered store.
%% @end
%%--------------------------------------------------------------------
-callback aux_first(model_behaviour:model_config(), Field :: atom()) ->
    datastore:aux_cache_handle().

%%--------------------------------------------------------------------
%% @doc
%% Returns next key to iterate over auxiliary ordered store.
%% @end
%%--------------------------------------------------------------------
-callback aux_next(model_behaviour:model_config(), Field :: atom(),
    Handle :: datastore:aux_cache_handle()) ->
    datastore:aux_cache_handle().

