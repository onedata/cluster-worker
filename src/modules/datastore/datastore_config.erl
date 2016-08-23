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
-include_lib("ctool/include/logging.hrl").

-define(DATASTORE_CONFIG_PLUGIN, datastore_config_plugin).
-define(DEFAULT_MODELS, [
  cache_controller,
  task_pool,
  lock
]).

%% datastore_config_behaviour callbacks
-export([models/0, global_caches/0, local_caches/0]).

%%--------------------------------------------------------------------
%% @doc
%% List of models used.
%% @end
%%--------------------------------------------------------------------
-spec models() -> Models :: [model_behaviour:model_type()].
models() -> ?DEFAULT_MODELS ++ plugins:apply(?DATASTORE_CONFIG_PLUGIN, models, []).

%%--------------------------------------------------------------------
%% @doc
%% List of models cached globally.
%% @end
%%--------------------------------------------------------------------
-spec global_caches() -> Models :: [model_behaviour:model_type()].
global_caches() ->
  filter_models_by_level(?GLOBALLY_CACHED_LEVEL, models_potentially_cached()).

%%--------------------------------------------------------------------
%% @doc
%% List of models cached locally.
%% @end
%%--------------------------------------------------------------------
-spec local_caches() -> Models :: [model_behaviour:model_type()].
local_caches() ->
  filter_models_by_level(?LOCALLY_CACHED_LEVEL, models_potentially_cached()).


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
  models() -- [cache_controller].