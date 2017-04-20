%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Behaviour for datastore drivers for databases and memory stores.
%%% @end
%%%-------------------------------------------------------------------
-module(store_driver_behaviour).
-author("Rafal Slota").

-type driver_action() :: model_behaviour:model_action().

-export_type([driver_action/0]).


%%--------------------------------------------------------------------
%% @doc
%% Initializes given driver locally (this method is executed per-node).
%% @end
%%--------------------------------------------------------------------
-callback init_driver(worker_host:plugin_state()) -> {ok, worker_host:plugin_state()} | {error, Reason :: term()}.


%%--------------------------------------------------------------------
%% @doc
%% Initializes given bucket locally (this method is executed per-node).
%% @end
%%--------------------------------------------------------------------
-callback init_bucket(Bucket :: datastore:bucket(), Models :: [model_behaviour:model_config()], NodeToSync :: node()) -> ok.


%%--------------------------------------------------------------------
%% @doc
%% Saves given #document.
%% @end
%%--------------------------------------------------------------------
% TODO - opcja czy operacja linkowa w ctx
-callback save(datastore_context:driver_ctx(), datastore:document()) -> {ok, datastore:ext_key()} | datastore:generic_error().


%%--------------------------------------------------------------------
%% @doc
%% Gets #document with given key.
%% @end
%%--------------------------------------------------------------------
-callback get(datastore_context:driver_ctx(), datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().


%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-callback delete(datastore_context:driver_ctx(), datastore:ext_key(), datastore:delete_predicate()) -> ok | datastore:generic_error().


%%--------------------------------------------------------------------
%% @doc
%% Checks driver state.
%% @end
%%--------------------------------------------------------------------
-callback healthcheck(WorkerState :: term()) -> ok | {error, Reason :: term()}.
