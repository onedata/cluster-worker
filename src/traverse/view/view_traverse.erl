%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines mechanism for scheduling jobs on couchbase
%%% map-reduce views.
%%% It implements traverse_behaviour and also defines new,
%%% view_traverse behaviour.
%%% Only one callback is required by this behaviour, process_row/2
%%% which is used to process a single row from view in a slave job.
%%% @end
%%%-------------------------------------------------------------------
-module(view_traverse).
-author("Jakub Kudzia").

-include("traverse/view_traverse.hrl").
-include_lib("ctool/include/logging.hrl").

-behaviour(traverse_behaviour).

%% API
-export([
    init/1, init/4, stop/1,
    run/3, run/4, cancel/2
]).

%% traverse callbacks
-export([
    do_master_job/2, do_slave_job/2,
    get_job/1, update_job_progress/5,
    task_started/2, task_finished/2, task_canceled/2,
    to_string/1
]).

-optional_callbacks([task_started/1, task_finished/1, task_canceled/1, to_string/1]).

-type task_id() :: traverse:id().
-type master_job() :: #view_traverse_master{}.
-type slave_job() :: #view_traverse_slave{}.
-type job() :: master_job() | slave_job().
-type token() :: #view_token{}.
-type callback_module() :: module().
-type query_opts() :: #{atom() => term()}.  % opts passed to couchbase_driver:query
-type info() :: term(). % custom term passed to process_row callback

% @formatter:off
-type opts() :: #{
    query_opts => query_opts(),
    async_next_batch_job => boolean(),
    info => info()
}.
% @formatter:on

-export_type([task_id/0, master_job/0, slave_job/0, token/0, callback_module/0, query_opts/0, info/0]).

%%%===================================================================
%%% view_traverse mandatory callbacks definitions
%%%===================================================================

-callback process_row(Row :: term(), Info :: info()) -> ok.

%%%===================================================================
%%% view_traverse optional callbacks definitions
%%%===================================================================

-callback task_started(task_id()) -> ok.

-callback task_finished(task_id()) -> ok.

-callback task_canceled(task_id()) -> ok.

-callback to_string(job()) -> binary() | atom() | iolist().

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init(callback_module()) -> ok.
init(CallbackModule) ->
    init(CallbackModule, ?DEFAULT_MASTER_JOBS_LIMIT, ?DEFAULT_SLAVE_JOBS_LIMIT, ?DEFAULT_PARALLELISM_LIMIT).

-spec init(callback_module(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok.
init(CallbackModule, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) when is_atom(CallbackModule) ->
    PoolName = callback_module_to_pool_name(CallbackModule),
    traverse:init_pool(PoolName, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, #{callback_modules => [?MODULE]}).

-spec stop(callback_module()) -> ok.
stop(CallbackModule) when is_atom(CallbackModule) ->
    traverse:stop_pool(callback_module_to_pool_name(CallbackModule)).

-spec run(callback_module(), couchbase_driver:view(), opts()) -> ok.
run(CallbackModule, ViewName, Opts) ->
    run(CallbackModule, ViewName, undefined, Opts).

-spec run(callback_module(), couchbase_driver:view(), task_id() | undefined, opts()) -> ok.
run(CallbackModule, ViewName, TaskId, Opts) ->
    DefinedTaskId = ensure_defined_task_id(TaskId),
    MasterJob = #view_traverse_master{
        view_name = ViewName,
        callback_module = CallbackModule,
        query_opts = maps:merge(maps:get(query_opts, Opts, #{}), ?DEFAULT_QUERY_OPTS),
        async_next_batch_job = maps:get(async_next_batch_job, Opts, ?DEFAULT_ASYNC_NEXT_BATCH_JOB),
        info = maps:get(info, Opts, undefined)
    },
    PoolName = callback_module_to_pool_name(CallbackModule),
    traverse:run(PoolName, DefinedTaskId, MasterJob, #{callback_module => ?MODULE}).

-spec cancel(callback_module(), task_id()) -> ok.
cancel(CallbackModule, TaskId) ->
    traverse:cancel(callback_module_to_pool_name(CallbackModule), TaskId).

%%%===================================================================
%%% traverse_behaviour callbacks implementations
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link traverse_behaviour} callback do_master_job/2.
%% @end
%%--------------------------------------------------------------------
-spec do_master_job(master_job(), traverse:master_job_extended_args()) -> {ok, traverse:master_job_map()}.
do_master_job(MasterJob = #view_traverse_master{
    view_name = ViewName,
    query_opts = QueryOpts,
    view_token = ViewToken,
    async_next_batch_job = AsyncNextBatchJob
}, _Args) ->
    case query(ViewName, prepare_query_opts(ViewToken, QueryOpts)) of
        {ok, {[]}} ->
            {ok, #{}};
        {ok, {Rows}} ->
            {SlaveJobs, NewToken} = lists:foldl(fun(Row, {SlaveJobsIn, TokenIn}) ->
                {<<"key">>, Key} = lists:keyfind(<<"key">>, 1, Row),
                {<<"id">>, DocId} = lists:keyfind(<<"id">>, 1, Row),
                {[slave_job(MasterJob, Row) | SlaveJobsIn], TokenIn#view_token{
                    start_key = Key,
                    last_doc_id = DocId
                }}
            end, {[], ViewToken}, Rows),
            NextBatchJob = MasterJob#view_traverse_master{view_token = NewToken},
            case AsyncNextBatchJob of
                true -> {ok, #{slave_jobs => SlaveJobs, async_master_jobs => [NextBatchJob]}};
                false -> {ok, #{slave_jobs => SlaveJobs, master_jobs => [NextBatchJob]}}
            end;
        {error, Reason} ->
            ?error("view_traverse mechanism received error ~p when querying view ~p", [Reason, ViewName]),
            {ok, #{}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link traverse_behaviour} callback do_slave_job/2.
%% @end
%%--------------------------------------------------------------------
-spec do_slave_job(slave_job(), traverse:id()) -> ok.
do_slave_job(#view_traverse_slave{row = Row, callback_module = CallbackModule, info = Info}, _TaskId) ->
    CallbackModule:process_row(Row, Info),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link traverse_behaviour} callback get_job/1.
%% @end
%%--------------------------------------------------------------------
-spec get_job(traverse:job_id()) ->
    {ok, traverse:job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(Id) ->
    view_traverse_job:get_master_job(Id).

%%--------------------------------------------------------------------
%% @doc
%% {@link traverse_behaviour} callback update_job_progress/2.
%% @end
%%--------------------------------------------------------------------
-spec update_job_progress(undefined | main_job | traverse:job_id(),
    job(), traverse:pool(), task_id(), traverse:job_status()) ->
    {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status)
    when Status =:= waiting
    orelse Status =:= on_pool
->
    view_traverse_job:save_master_job(Id, Job, Pool, TaskId);
update_job_progress(Id, _Job, _Pool, _TaskId, _Status) ->
    ok = view_traverse_job:delete_master_job(Id),
    {ok, Id}.

%%--------------------------------------------------------------------
%% @doc
%% {@link traverse_behaviour} callback task_started/2.
%% @end
%%--------------------------------------------------------------------
-spec task_started(task_id(), traverse:pool()) -> ok.
task_started(TaskId, PoolName) ->
    task_callback(PoolName, task_started, TaskId).

%%--------------------------------------------------------------------
%% @doc
%% {@link traverse_behaviour} callback task_finished/2.
%% @end
%%--------------------------------------------------------------------
-spec task_finished(task_id(), traverse:pool()) -> ok.
task_finished(TaskId, PoolName) ->
    task_callback(PoolName, task_finished, TaskId).

%%--------------------------------------------------------------------
%% @doc
%% {@link traverse_behaviour} callback task_canceled/2.
%% @end
%%--------------------------------------------------------------------
-spec task_canceled(task_id(), traverse:pool()) -> ok.
task_canceled(TaskId, PoolName) ->
    task_callback(PoolName, task_canceled, TaskId).

%%--------------------------------------------------------------------
%% @doc
%% {@link traverse_behaviour} callback to_string/1.
%% @end
%%--------------------------------------------------------------------
-spec to_string(job()) -> binary() | atom() | iolist().
to_string(Job = #view_traverse_master{callback_module = CallbackModule}) ->
    CallbackModule:to_string(Job);
to_string(Job = #view_traverse_slave{callback_module = CallbackModule}) ->
    CallbackModule:to_string(Job).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec callback_module_to_pool_name(callback_module()) -> traverse:pool().
callback_module_to_pool_name(CallbackModule) ->
    atom_to_binary(CallbackModule, utf8).

-spec pool_name_to_callback_module(traverse:pool()) -> callback_module().
pool_name_to_callback_module(PoolName) ->
    binary_to_atom(PoolName, utf8).

-spec task_callback(traverse:pool(), atom(), task_id()) -> ok.
task_callback(PoolName, Function, TaskId) ->
    CallbackModule = pool_name_to_callback_module(PoolName),
    case erlang:function_exported(CallbackModule, Function, 1) of
        true -> CallbackModule:Function(TaskId);
        false -> ok
    end.

-spec ensure_defined_task_id(task_id() | undefined) -> task_id().
ensure_defined_task_id(undefined) ->
    datastore_utils:gen_key();
ensure_defined_task_id(TaskId) when is_binary(TaskId) ->
    TaskId.

-spec query(couchbase_driver:view(), [couchbase_driver:view_opt()]) -> {ok, term()} | {error, term()}.
query(ViewName, Opts) ->
    DiscCtx = datastore_model_default:get_default_disk_ctx(),
    couchbase_driver:query_view(DiscCtx, ViewName, ViewName, Opts).

-spec slave_job(master_job(), term()) -> slave_job().
slave_job(#view_traverse_master{callback_module = CallbackModule, info = Info}, Row) ->
    #view_traverse_slave{
        callback_module = CallbackModule,
        info = Info,
        row = Row
    }.

-spec prepare_query_opts(token(), query_opts()) -> [couchbase_driver:view_opt()].
prepare_query_opts(#view_token{last_doc_id = undefined, start_key = undefined}, Opts) ->
    maps:to_list(Opts);
prepare_query_opts(#view_token{
    last_doc_id = LastDocId,
    start_key = LastKey
}, Opts) ->
    maps:to_list(Opts#{
        startkey => LastKey,
        startkey_docid => LastDocId,
        skip => 1
    }).