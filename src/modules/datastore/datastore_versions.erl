%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module for model schema upgrade
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_versions).
-author("Rafal Slota").

-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_models_def.hrl").

-define(shell(F), ?shell(F, [])).
-define(shell(F, A), io:format(user, F, A)).

%% Initial batch size of single fetch/save operation of update process
%% Real used value changes adapting to current average operation time
-define(UPDATE_BATCH_SIZE, 128).

%% Maximum wait time for collecting document for batch operation
-define(UPDATE_MAX_INTERVAL, timer:seconds(10)).


%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([]).

%% API
-export([shell_upgrade/0]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Performs upgrade of model's DB schema.
%% The function outputs progress on standard output and returns overall result.
%% @end
%%--------------------------------------------------------------------
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
        {Model, shell_upgrade(Ref, N, Models, 0, 0, 0, 0.0)}
    end, lists:seq(1, length(Models))).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Performs upgrade of model's DB schema. Internal support implementation for shell_upgrade/0
%% @end
%%--------------------------------------------------------------------
-spec shell_upgrade(Ref :: term(), N :: non_neg_integer(), Models :: [model_behaviour:model_type()],
    non_neg_integer(), non_neg_integer(), BatchSize :: non_neg_integer(), AvgSpeed :: float()) ->
    {ok, Counter} | {error, Reason :: any(), Counter} when
    Counter :: {OKCount :: non_neg_integer(), ErrorsCount :: non_neg_integer()}.
shell_upgrade(Ref, N, Models, OKs, Errors, BatchSize, AvgSpeed) ->
    ?shell("~c[~p/~p] Processing model (OK: ~p, Errors: ~p, Speed ~p * ~.2f): ~p",
            [13, N, length(Models), OKs, Errors, BatchSize, AvgSpeed, lists:nth(N, Models)]),

    receive
        {Ref, {updated, NextOKKeys}, {errors, NextErrorKeys}, {batch_size, NewBatchSize}, {avg_speed, NewAvgSpeed}} ->
            shell_upgrade(Ref, N, Models, length(NextOKKeys) + OKs, length(NextErrorKeys) + Errors, NewBatchSize, NewAvgSpeed);
        {Ref, done} ->
            ?shell("~c[~p/~p] Successfully updated model (OK: ~p, Errors: ~p, Speed ~p * ~.2f): ~p~n",
                [13, N, length(Models), OKs, Errors, BatchSize, AvgSpeed, lists:nth(N, Models)]),
            {ok, {OKs, Errors}};
        {Ref, {error, Reason}} ->
            ?shell("~c[~p/~p] Faild to update model (OK: ~p, Errors: ~p, Speed ~p * ~.2f): ~p (reason: ~p)~n",
                [13, N, length(Models), OKs, Errors, BatchSize, AvgSpeed, lists:nth(N, Models), Reason]),
            {error, Reason, {OKs, Errors}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Starts stream of outdated records for given model.
%% See couchdb_datastore_driver:stream_view/3 for possible stream messages.
%% @end
%%--------------------------------------------------------------------
-spec stream_outdated_records(ModelName :: model_behaviour:model_type()) ->
    StreamRef :: term().
stream_outdated_records(ModelName) ->
    #model_config{version = ModelVersion} = ModelName:model_init(),
    couchdb_datastore_driver:stream_view(ModelName, <<"versions">>, [
        {stale, false},
        {keys, [[datastore_json:encode_record(key, ModelName, atom), V] || V <- lists:seq(1, max(1, ModelVersion - 1))]},
        {inclusive_end, true}
    ]).


%%--------------------------------------------------------------------
%% @doc
%% Starts asynchronous model's schema upgrade. Reports progress via messages:
%% {Ref, {error, Reason :: any()} - error has occurred, upgrade process was interrupted,
%% {Ref, {updated, OKKeys :: [datastore:ext_key()]}, {errors, ErrorKeys :: [datastore:ext_key()]},
%%       {batch_size, integer()}, {avg_speed, float()}}
%%          - OKKeys carries list of document keys that were successfully updated since last message,
%%            while ErrorKeys holds list of document keys that were not updated.
%% {Ref, done} - upgrade process has ended normally.
%% @end
%%--------------------------------------------------------------------
-spec async_update_records(ModelName :: model_behaviour:model_type()) ->
    StreamRef :: term().
async_update_records(ModelName) ->
    ModelConfig = #model_config{} = ModelName:model_init(),
    Ref = make_ref(),
    Host = self(),
    spawn_link(
        fun() ->
            StreamRef = stream_outdated_records(ModelName),

            async_update_records(
                {ModelConfig, Host, Ref, StreamRef},
                [], os:system_time(milli_seconds), ?UPDATE_BATCH_SIZE, 0, timer:hours(24), {[], 0, os:system_time(milli_seconds)})
        end),

    Ref.
async_update_records({ModelConfig, Host, Ref, StreamRef} = Config, Keys0, LastFlush, BatchSize, LastBatchSize, LastOpTime, OpHistory) ->
    FlushAt = LastFlush + ?UPDATE_MAX_INTERVAL,
    CTime = os:system_time(milli_seconds),
    {NextTime, Keys, NewBatchSize, NewOpTime, NewOpHistory, NewLastBatchSize} =
        case length(Keys0) of
            KeysLen when KeysLen >= BatchSize; FlushAt =< CTime -> %% Should flush
                {
                    {updated, OKKeys0},
                    {errors, ErrorKeys0}
                } = batch_update_records(ModelConfig, Keys0),

                {OpTimes, OpCount, HistoryTime} = OpHistory,
                CurrentAverageOpTime = lists:sum(OpTimes) / max(1, OpCount),

                Host ! {Ref,
                    {updated, OKKeys0},
                    {errors, ErrorKeys0},
                    {batch_size, length(Keys0)},
                    {avg_speed, CurrentAverageOpTime}
                },

                CurrentFlushTime = os:system_time(milli_seconds),
                CurrentOpTime = round((CurrentFlushTime - LastFlush) / max(1, KeysLen)),

                case HistoryTime + ?UPDATE_MAX_INTERVAL < CurrentFlushTime of
                    true ->
                        NewOpHistory0 = {[], 0, CurrentFlushTime},
                        NewBatchSize0 =
                            case {CurrentAverageOpTime > LastOpTime, KeysLen >= LastBatchSize} of
                                {true, true}  -> %% Worse time, bigger batch -> make it smaller
                                    max(1, round(BatchSize * 0.5));
                                {true, false} -> %% Worse time, smaller batch -> make it bigger
                                    min(10000, round(BatchSize * 2));
                                {false, true} -> %% Better time, bigger batch -> make it even bigger
                                    min(10000, round(BatchSize * 1.5));
                                {false, false} -> %% Better time, smaller batch -> make it even smaller
                                    max(1, round(BatchSize * 0.75))
                            end,
                        {CurrentFlushTime, [], NewBatchSize0, CurrentAverageOpTime, NewOpHistory0, BatchSize};
                    false ->
                        NewOpHistory0 = {[CurrentOpTime | OpTimes], OpCount + 1, HistoryTime},
                        {CurrentFlushTime, [], BatchSize, LastOpTime, NewOpHistory0, LastBatchSize}
                end;
            _ ->
                {LastFlush, Keys0, BatchSize, LastOpTime, OpHistory, LastBatchSize}
        end,

    receive
        {StreamRef, {stream_data, Key}} ->
            async_update_records(Config, [Key | Keys], NextTime, NewBatchSize, NewLastBatchSize, NewOpTime, NewOpHistory);
        {StreamRef, {stream_ended, done}} ->
            {{updated, OKKeys}, {errors, ErrorKeys}} = batch_update_records(ModelConfig, Keys),
            {OpTimes2, OpCount2, _} = OpHistory,
            CurrentAverageOpTime2 = lists:sum(OpTimes2) / max(1, OpCount2),
            Host ! {Ref, {updated, OKKeys}, {errors, ErrorKeys}, {batch_size, BatchSize}, {avg_speed, CurrentAverageOpTime2}},
            Host ! {Ref, done};
        {StreamRef, {stream_ended, {error, Reason}}} ->
            Host ! {Ref, {error, Reason}}
    after timer:hours(1) ->
        Host ! {Ref, {error, update_timeout}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Batch updates schema of given documents. Returns number of successful and failed operations.
%% Also returns average operation time for this batch.
%% @end
%%--------------------------------------------------------------------
-spec batch_update_records(model_behaviour:model_config(), [datastore:ext_key()]) ->
    {{updated, [datastore:ext_key()]}, {errors, [datastore:ext_key()]}}.
batch_update_records(ModelConfig, Keys) ->
    T0 = os:system_time(milli_seconds),
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
    T1 = os:system_time(milli_seconds),
    AvgOpTime = round((T1 - T0) / max(length(Keys), 1)),
    {{updated, OKKeys}, {errors, ErrorKeys}}.
