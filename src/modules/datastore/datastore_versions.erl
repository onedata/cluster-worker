%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc JSON encoding for datastore models
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_versions).
-author("Rafal Slota").

-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_models_def.hrl").


%%%===================================================================
%%% Types
%%%===================================================================



%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([]).

%% API
-export([stream_outdated_records/1, async_update_records/1]).


%%%===================================================================
%%% API functions
%%%===================================================================


stream_outdated_records(ModelName) ->
    #model_config{version = ModelVersion} = ModelName:model_init(),
    couchdb_datastore_driver:stream_view(ModelName, "versions", [
        {stale, false},
        {start_key, [datastore_json:encode_record(key, ModelName, atom), 1]},
        {end_key, [datastore_json:encode_record(key, ModelName, atom), ModelVersion - 1]},
        {inclusive_end, true}
    ]).

async_update_records(ModelName) ->
    #model_config{version = TargetVersion} = ModelName:model_init(),
    Ref = make_ref(),
    Host = self(),
    spawn_link(
        fun() ->
            StreamRef = stream_outdated_records(ModelName),
            Receiver = fun ReceiverFun(Keys) ->
                receive
                    {StreamRef, {stream_data, Key}} ->
%%                        {NewVersion, NewRecord} = datastore_json:record_upgrade(ModelName, TargetVersion, Version, Record),
%%                        ToSave = #document{key = Key, value = NewRecord, version = NewVersion},
                        Host ! {Ref, {updated, Key}},
                        ReceiverFun([Key | Keys]);
                    {StreamRef, {stream_ended, _Reason}} ->
                        Host ! {Ref, done};
                    {StreamRef, {stream_error, Reason}} ->
                        Host ! {Ref, {error, Reason}}
                after timer:hours(1) ->
                    Host ! {Ref, update_timeout}
                end
            end,
            Receiver([])
        end),

    Ref.
