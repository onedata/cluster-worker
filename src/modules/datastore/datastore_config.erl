%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines config of the datastore. It contains default
%%% (required by infrastructure) config, which is complemented by
%%% information from datastore_config_plugin.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_config).
-author("Michal Zmuda").

-behaviour(datastore_config_behaviour).

-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_common.hrl").
-include_lib("ctool/include/logging.hrl").

-define(DATASTORE_CONFIG_PLUGIN, datastore_config_plugin).
-define(DEFAULT_MODELS, [
    auxiliary_cache_controller,
    task_pool,
    cache_consistency_controller,
    cached_identity,
    synced_cert,
    lock,
    node_management
]).

%% datastore_config_behaviour callbacks
-export([db_nodes/0, models/0, throttled_models/0, global_caches/0,
    local_caches/0, models_with_aux_caches/0]).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of datastore nodes.
%% @end
%%--------------------------------------------------------------------
-spec db_nodes() -> [datastore:db_node()].
db_nodes() ->
    {ok, Nodes} = plugins:apply(node_manager_plugin, db_nodes, []),
    lists:map(fun(NodeString) ->
        [HostName, Port] = string:tokens(atom_to_list(NodeString), ":"),
        {list_to_binary(HostName), list_to_integer(Port)}
    end, Nodes).

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of models.
%% @end
%%--------------------------------------------------------------------
-spec models() -> Models :: [model_behaviour:model_type()].
models() ->
    ?DEFAULT_MODELS ++ plugins:apply(?DATASTORE_CONFIG_PLUGIN, models, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of throttled models.
%% @end
%%--------------------------------------------------------------------
-spec throttled_models() -> Models :: [model_behaviour:model_type()].
throttled_models() ->
    plugins:apply(?DATASTORE_CONFIG_PLUGIN, throttled_models, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of models cached globally.
%% @end
%%--------------------------------------------------------------------
-spec global_caches() -> Models :: [model_behaviour:model_type()].
global_caches() ->
    filter_models_by_level(?GLOBALLY_CACHED_LEVEL, models_potentially_cached()).

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of models cached locally.
%% @end
%%--------------------------------------------------------------------
-spec local_caches() -> Models :: [model_behaviour:model_type()].
local_caches() ->
    filter_models_by_level(?LOCALLY_CACHED_LEVEL, models_potentially_cached()).

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of models with defined auxiliary caches.
%% @end
%%--------------------------------------------------------------------
-spec models_with_aux_caches() -> Models :: [model_behaviour:model_type()].
models_with_aux_caches() ->
    case ets:info(?LOCAL_STATE) of
        undefined ->
            filter_models_with_aux_caches();
        _ ->
            case ets:lookup(?LOCAL_STATE, aux_caches_models) of
                [] ->
                    AuxCachesModels = filter_models_with_aux_caches(),
                    ets:insert_new(?LOCAL_STATE, {aux_caches_models, AuxCachesModels}),
                    AuxCachesModels;
                [{_, AuxCachesModels}] -> AuxCachesModels
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retains models with given store_level.
%% @end
%%--------------------------------------------------------------------
filter_models_by_level(Level, Models) ->
    lists:filter(fun(Model) ->
        MInit = Model:model_init(),
        (MInit#model_config.store_level == Level) and (MInit#model_config.sync_cache == false)
    end, Models).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retains models which could be cached. For instance excludes models
%% which introduce endless recursion.
%% @end
%%--------------------------------------------------------------------
models_potentially_cached() ->
    models() -- [auxiliary_cache_controller, node_management].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns list of models with defined auxiliary caches.
%% @end
%%--------------------------------------------------------------------
-spec filter_models_with_aux_caches() -> Models :: [model_behaviour:model_type()].
filter_models_with_aux_caches() ->
    lists:filter(fun(M) ->
        Config = M:model_init(),
        case map_size(Config#model_config.auxiliary_caches) of
            0 -> false;
            _ -> true
        end
    end, ?MODULE:models() -- [auxiliary_cache_controller]).