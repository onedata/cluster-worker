%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for persisting view_traverse master jobs.
%%% @end
%%%-------------------------------------------------------------------
-module(view_traverse_job).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("traverse/view_traverse.hrl").
-include("modules/datastore/datastore_models.hrl").


%% API
-export([get_master_job/1, save_master_job/4, delete_master_job/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type key() :: traverse:job_id().
-type record() :: #view_traverse_job{}.
-type doc() :: datastore_doc:doc(record()).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get_master_job(key() | doc()) ->
    {ok, view_traverse:master_job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_master_job(#document{value = #view_traverse_job{
    task_id = TaskId,
    pool = Pool,
    view_processing_module = ViewProcessingModule,
    view_name = ViewName,
    token = Token,
    query_opts = QueryOpts,
    async_next_batch_job = AsyncNextBatchJob,
    info = Info
}}) ->
    Job = #view_traverse_master{
        view_name = ViewName,
        view_processing_module = ViewProcessingModule,
        token = Token,
        query_opts = QueryOpts,
        async_next_batch_job = AsyncNextBatchJob,
        info = Info
    },
    {ok, Job, Pool, TaskId};
get_master_job(Key) ->
    case datastore_model:get(?CTX#{include_deleted => true}, Key) of
        {ok, Doc} ->
            get_master_job(Doc);
        Other ->
            Other
    end.

-spec save_master_job(key(), view_traverse:master_job(), traverse:pool(), traverse:id()) ->
    {ok, key()} | {error, term()}.
save_master_job(Id, #view_traverse_master{
    view_name = ViewName,
    view_processing_module = ViewProcessingModule,
    token = Token,
    query_opts = QueryOpts,
    async_next_batch_job = AsyncNextBatchJob,
    info = Info
}, PoolName, TaskId) ->
    % setting Id to undefined will cause datastore to generate key
    Id2 = utils:ensure_defined(Id, main_job, undefined),
     Doc = #document{
        key =  Id2,
        value = #view_traverse_job{
            task_id = TaskId,
            pool = PoolName,
            view_name = ViewName,
            view_processing_module = ViewProcessingModule,
            token = Token,
            query_opts = QueryOpts,
            async_next_batch_job = AsyncNextBatchJob,
            info = Info
        }
    },
    case datastore_model:save(?CTX, Doc) of
        {ok, #document{key = Id3}} -> {ok, Id3};
        Other -> Other
    end.

-spec delete_master_job(key()) -> ok | {error, term()}.
delete_master_job(Key) ->
    datastore_model:delete(?CTX, Key).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {task_id, string},
        {pool, string},
        {view_name, string},
        {callback_module, atom},
        {token, {record, [
            {offset, integer},
            {last_doc_id, string},
            {last_start_key, term}
        ]}},
        {query_opts, {custom, json, {json_utils, encode, decode}}},
        {async_next_batch_job, boolean},
        {info, term}
    ]}.