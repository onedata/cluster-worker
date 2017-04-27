%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for document version management.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_version2).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([rename_model/2, upgrade_model/3]).

-type version() :: non_neg_integer().

-export_type([version/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Renames model to the current name.
%% @end
%%--------------------------------------------------------------------
-spec rename_model(version(), atom()) -> atom().
rename_model(Version, ModelName) ->
    RenamedModels = plugins:apply(node_manager_plugin, renamed_models, []),
    NextVersions = lists:filtermap(fun
        ({V, N}) when V >= Version, N =:= ModelName -> {true, V};
        (_) -> false
    end, maps:keys(RenamedModels)),
    case lists:sort(NextVersions) of
        [] -> ModelName;
        [Version | _] ->
            NextName = maps:get({ModelName, Version}, RenamedModels),
            rename_model(Version + 1, NextName)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model to the current version.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_model(version(), atom(), tuple()) ->
    {version(), tuple()} | no_return().
upgrade_model(Version, ModelName, Record) ->
    #model_config{version = Target} = ModelName:model_init(),
    upgrade_model(Version, Target, ModelName, Record).

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model to the provided version.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_model(version(), version(), atom(), tuple()) ->
    {version(), tuple()} | no_return().
upgrade_model(Version, Version, _ModelName, Record) ->
    {Version, Record};
upgrade_model(Version, Target, ModelName, _Record) when Version > Target ->
    ?emergency("Upgrade requested for model '~p' with future version ~p "
    "(known versions up to: ~p)", [ModelName, Version, Target]),
    error({future_version, ModelName, Version, Target});
upgrade_model(Version, Target, ModelName, Record) ->
    {Version2, Record2} = ModelName:record_upgrade(Version, Record),
    upgrade_model(Version2, Target, ModelName, Record2).