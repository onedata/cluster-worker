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

-define(shell(F), ?shell(F, [])).
-define(shell(F, A), io:format(user, F, A)).


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
-export([shell_upgrade/0]).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec shell_upgrade() -> [{model_behaviour:model_type(), Result}] when
    Result :: {ok, Counter} | {error, Reason :: any(), Counter},
    Counter :: {OKCount :: non_neg_integer(), ErrorsCount :: non_neg_integer()}.
shell_upgrade() ->
    Models = datastore_config:models(),
    ?shell("~n~nUpdating schema of ~p datastore models...~n", [length(Models)]),

    lists:map(fun(N) ->
        Model = lists:nth(N, Models),
        ?shell("~c[~p/~p] Preparing model for update: ~p",
            [13, N, length(Models), Model]),
        Ref = async_update_records(Model),
        {Model, shell_upgrade(Ref, N, Models, 0, 0)}
    end, lists:seq(1, length(Models))).

shell_upgrade(Ref, N, Models, OKs, Errors) ->
    ?shell("~c[~p/~p] Processing model (OK: ~p, Errors: ~p): ~p",
            [13, N, length(Models), OKs, Errors, lists:nth(N, Models)]),

    receive
        {Ref, {updated, NextOKKeys}, {errors, NextErrorKeys}} ->
            shell_upgrade(Ref, N, Models, length(NextOKKeys) + OKs, length(NextErrorKeys) + Errors);
        {Ref, done} ->
            ?shell("~c[~p/~p] Successfully updated model (OK: ~p, Errors: ~p): ~p~n",
                [13, N, length(Models), OKs, Errors, lists:nth(N, Models)]),
            {ok, {OKs, Errors}};
        {Ref, {error, Reason}} ->
            ?shell("~c[~p/~p] Faild to update model (OK: ~p, Errors: ~p): ~p (reason: ~p)~n",
                [13, N, length(Models), OKs, Errors, lists:nth(N, Models), Reason]),
            {error, Reason, {OKs, Errors}}
    end.


stream_outdated_records(ModelName) ->
    #model_config{version = ModelVersion} = ModelName:model_init(),
    couchdb_datastore_driver:stream_view(ModelName, "versions", [
        {stale, false},
        {keys, [[datastore_json:encode_record(key, ModelName, atom), V] || V <- lists:seq(1, max(1, ModelVersion - 1))]},
        {inclusive_end, true}
    ]).

async_update_records(ModelName) ->
    ModelConfig = #model_config{} = ModelName:model_init(),
    Ref = make_ref(),
    Host = self(),
    spawn_link(
        fun() ->
            StreamRef = stream_outdated_records(ModelName),

            Updater = fun(Keys) ->
                KeysWithModelConfig = [{ModelConfig, Key} || Key <- Keys],
                Results = couchdb_datastore_driver:get_docs(ModelConfig, KeysWithModelConfig),
                {DocsToSave, ErrorKeys0} = lists:foldl(fun
                    ({_, {ok, #document{} = Doc}}, {OKDocs, ErrorKeysAcc}) ->
                        {[Doc | OKDocs], ErrorKeysAcc};
                    ({Key, {error, _Reason}}, {OKKeys, ErrorKeysAcc}) ->
                        {OKKeys, [Key | ErrorKeysAcc]}
                    end, {[], []}, lists:zip(Keys, Results)),
                ConfigsWithDocs = [{ModelConfig, Doc} || Doc <- DocsToSave],
                SaveRes = lists:zip(DocsToSave, couchdb_datastore_driver:save_docs(ModelConfig, ConfigsWithDocs)),
                {OKKeys, ErrorKeys} = lists:foldl(fun
                    ({#document{key = Key}, {ok, _}}, {OKKeys, ErrorKeysAcc}) ->
                        {[Key | OKKeys], ErrorKeysAcc};
                    ({#document{key = Key}, {error, _}}, {OKKeys, ErrorKeysAcc}) ->
                        {OKKeys, [Key | ErrorKeysAcc]}
                    end, {[], ErrorKeys0}, SaveRes),
                Host ! {Ref, {updated, OKKeys}, {errors, ErrorKeys}}
            end,

            Receiver = fun ReceiverFun(Keys0, LastFlush) ->
                FlushAt = LastFlush + timer:seconds(3),
                CTime = os:system_time(milli_seconds),
                {NextTime, Keys} = case length(Keys0) of
                    KeysLen when KeysLen >= 100; FlushAt =< CTime ->
                        Updater(Keys0),
                        {os:system_time(milli_seconds), []};
                    _ ->
                        {LastFlush, Keys0}
                end,

                receive
                    {StreamRef, {stream_data, Key}} ->
                        ReceiverFun([Key | Keys], NextTime);
                    {StreamRef, {stream_ended, _Reason}} ->
                        Updater(Keys),
                        Host ! {Ref, done};
                    {StreamRef, {stream_error, Reason}} ->
                        Host ! {Ref, {error, Reason}}
                after timer:hours(1) ->
                    Host ! {Ref, {error, update_timeout}}
                end
            end,
            Receiver([], os:system_time(milli_seconds))
        end),

    Ref.
