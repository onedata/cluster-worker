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
-module(datastore_versions).
-author("Krzysztof Trzepla").

-include_lib("ctool/include/logging.hrl").

%% API
-export([rename_record/2, upgrade_record/3]).

-type record() :: datastore_model:record().
-type record_name() :: datastore_model:model().
-type record_version() :: non_neg_integer().

-export_type([record_version/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Renames model to the current name.
%% @end
%%--------------------------------------------------------------------
-spec rename_record(record_version(), record_name()) -> record_name().
rename_record(Version, Name) ->
    RenamedModels = plugins:apply(node_manager_plugin, renamed_models, []),
    Versions = lists:filtermap(fun
        ({V, N}) when V >= Version andalso N == Name -> {true, V};
        (_) -> false
    end, maps:keys(RenamedModels)),
    case lists:sort(Versions) of
        [] -> Name;
        [Version2 | _] ->
            Name2 = maps:get({Version2, Name}, RenamedModels),
            rename_record(Version2 + 1, Name2)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model to the current version.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(record_version(), record_name(), record()) ->
    {record_version(), record()} | no_return().
upgrade_record(CurrentVersion, Name, Record) ->
    TargetVersion = datastore_model_default:get_record_version(Name),
    upgrade_record(TargetVersion, CurrentVersion, Name, Record).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Upgrades model to the provided version.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(record_version(), record_version(), record_name(),
    record()) -> {record_version(), record()} | no_return().
upgrade_record(Version, Version, _Name, Record) ->
    {Version, Record};
upgrade_record(TargetVersion, CurrentVersion, Name, _Record)
    when CurrentVersion > TargetVersion ->
    ?emergency("Upgrade requested for model '~tp' with future version ~tp "
    "(known versions up to: ~tp)", [Name, CurrentVersion, TargetVersion]),
    error({future_version, Name, CurrentVersion, TargetVersion});
upgrade_record(TargetVersion, CurrentVersion, Name, Record) ->
    {CurrentVersion2, Record2} = Name:upgrade_record(CurrentVersion, Record),
    upgrade_record(TargetVersion, CurrentVersion2, Name, Record2).