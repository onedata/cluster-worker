%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(auxiliary_ordered_store_behaviour).
-author("Jakub Kudzia").


%%--------------------------------------------------------------------
%% @doc
%% Creates auxiliary tables that would allow to iterate in order by fields
%% given in list of keys of map #model_config.auxiliary_tables
%% @end
%%--------------------------------------------------------------------
-callback create_auxiliary_ordered_stores(model_behaviour:model_config(),
    Fields :: [atom()], NodeToSync :: node()) ->
    ok | datastore:generic_error() | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Returns first key to iterate over auxiliary ordered store.
%% @end
%%--------------------------------------------------------------------
-callback first(model_behaviour:model_config(), Field :: atom()) ->
    datastore:aux_store_handle().

%%--------------------------------------------------------------------
%% @doc
%% Returns next key to iterate over auxiliary ordered store.
%% @end
%%--------------------------------------------------------------------
-callback next(model_behaviour:model_config(), Field :: atom(),
    Handle :: datastore:aux_store_handle()) ->
    datastore:aux_store_handle().


%%--------------------------------------------------------------------
%% @doc
%% Returns next key to iterate over auxiliary ordered store.
%% @end
%%--------------------------------------------------------------------
-callback get_id(Key :: datastore:aux_store_key()) -> datastore:key().


%%--------------------------------------------------------------------
%% @doc
%% Deletes document with key Key in auxiliary_store of Model connected
%% with Field.
%% @end
%%--------------------------------------------------------------------
-callback aux_delete(
    Model :: model_behaviour:model_config(),
    Field :: atom(),
    Key :: datastore:ext_key()) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Saves document with key Key in auxiliary_store of Model connected
%% with Field.
%% @end
%%--------------------------------------------------------------------
-callback aux_save(
    Model :: model_behaviour:model_config(),
    Field :: atom(),
    Key :: datastore:ext_key()) -> ok.


%%--------------------------------------------------------------------
%% @doc
%% Updates document with key Key in auxiliary_store of Model connected
%% with Field.
%% @end
%%--------------------------------------------------------------------
-callback aux_update(
    Model :: model_behaviour:model_config(),
    Field :: atom(),
    Key :: datastore:ext_key()) -> ok.