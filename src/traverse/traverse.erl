%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module that allows traversing via different structures and execution of jobs on them.
%%% The basic concepts are tasks (see traverse_task.erl) and pools. Each task represents single traverse while pool
%%% represents single execution environment. The tasks can be scheduled to be executed on different environments
%%% (different clusters). Thus, concepts of task creator and executor are introduced. The creator represents the
%%% environment where the task is created while executor represents environment where the task is processed.
%%% The task is processed executing jobs. The job represents processing of single entity (e.g., dir or file).
%%% First job (called main_job) is provided to task start function, further jobs can be generated as first and next
%%% jobs result. First job is treated specially as it is the only job that exists before task start. Thus, its id is
%%% stored in a task document (see traverse_task.erl) and its special status is highlighted when it is persisted.
%%% Such special treatment is a result of the fact that task can be scheduled for other environment. Thus main job must
%%% be available there (all other jobs will be created on the environment that processes task).
%%% The jobs are divided into two groups: master and slave jobs. Master jobs process entities that define structure
%%% (e.g., directories) while slave jobs should process other elements (e.g., files). Thus, master jobs can produce
%%% further master jobs and/or slave jobs.
%%% Each pool uses two instances of worker pool for jobs execution: one instance for execution of master jobs and
%%% one for slave jobs. Each master job is blocked until all of slave jobs generated by it are finished.
%%% Worker pools can be started on several nodes. In such a case, load is balanced between nodes executing different
%%% tasks on different nodes (all jobs connected with single task are executed on the same node - see
%%% traverse_tasks_scheduler.erl).
%%% Job handling functions are provided by callback modules (see traverse_behaviour.erl). Callback modules provide also
%%% functions needed for jobs persistency (master jobs are stored only). They also can provide additional information
%%% for datastore documents synchronization and tasks sorting (see traverse_task_list.erl). Multiple callback modules
%%% can be used for single pool (different callback modules for different tasks).
%%% Task load balancing base on groups. Each task can be connected with a group and load balancing algorithm first
%%% chooses group and only than task to be executed (to prevent single group from taking all resources while other
%%% groups are waiting - see traverse_tasks_scheduler.erl).
%%%
%%% Typical task lifecycle is as follows:
%%%     - user calls run function
%%%     - the job that initializes task is persisted
%%%     - number of ongoing tasks is verified
%%%         - if the number is lower than the limit and task executor is local environment,
%%%             task will be created with status "ongoing" and number of parallel tasks is incremented
%%%         - otherwise, task will be created with status "scheduled"; if task executor is local environment group
%%%             connected with task is marked as waiting for execution
%%%     - if task is created as ongoing, it is put in master worker pool queue, next:
%%%         - workers from worker pool execute jobs connected with task (main job and jobs created as a result of master
%%%             jobs processing) until all jobs are finished
%%%         - last job connected with task updates task status to "finished"
%%%         - last job connected with task checks if there is any waiting tasks
%%%             - if some tasks are waiting, longest waiting group for execution is chosen, and oldest task from
%%%                 this group is put in master worker pool queue; chosen task status is changed to "ongoing"
%%%             - otherwise, number of parallel tasks is decremented
%%% Additionally, tasks may be started when task scheduled at other environment appears (and local environment is
%%% chosen for execution) or during environment restart. In first case, number of ongoing tasks is verified to check if
%%% task can be executed or has to wait.
%%% @end
%%%-------------------------------------------------------------------
-module(traverse).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/logging.hrl").

%% API
-export([init_pool/4, init_pool/5, stop_pool/1,
    run/3, run/4, cancel/2, cancel/3, on_task_change/2, on_job_change/5]).
%% Functions executed on pools
-export([execute_master_job/9, execute_slave_job/5]).
%% For rpc
-export([run_on_master_pool/9]).

% Basic types for execution environment
-type pool() :: binary(). % term used to identify instance of execution environment
-type callback_module() :: module().
-type environment_id() :: traverse_task_list:tree(). % environment (cluster) where task is scheduled or processed
-type pool_options() :: #{
    executor => environment_id(),
    callback_modules => [callback_module()],
    restart => boolean()
}.
% Basic types for tasks management
-type id() :: datastore:key().
-type task() :: traverse_task:doc().
-type description() :: #{atom() => integer()}.  % map describing progress of task; framework provides following counters:
                                                % slave_jobs_delegated, master_jobs_delegated, slave_jobs_done,
                                                % master_jobs_done, slave_jobs_failed, master_jobs_failed;
                                                % the user can add own counters returning map with value upgrade from
                                                % job (see traverse_behaviour.erl)
-type additional_data() :: #{binary() => binary()}.
-type status() :: atom().   % framework uses statuses: scheduled, ongoing, finished and canceled but user can set
                            % any intermediary status using traverse_task:update_status function
-type group() :: binary(). % group used for load balancing (see traverse_tasks_scheduler.erl)
-type run_options() :: #{
    executor => environment_id(),
    creator => environment_id(),
    callback_module => callback_module(),
    group_id => group(),
    additional_data => additional_data()
}.
% Basic types for jobs management
-type job() :: term().
-type job_id() :: datastore:key().
-type job_status() :: waiting | on_pool | ended | failed | canceled.
-type master_job_map() :: #{
    slave_jobs => [job()],
    sequential_slave_jobs => [job()],
    master_jobs => [job()],
    async_master_jobs => [job()],
    description => description(),
    cancel_callback => job_cancel_callback(),
    finish_callback => job_finish_callback()
}.

-type master_job_extended_args() :: #{
    task_id => id(),
    master_job_starter_callback => master_job_starter_callback()
}.
-type master_job_starter_callback() :: fun(([job()]) -> ok).
-type job_cancel_callback() :: fun((master_job_extended_args(), description()) -> ok).
-type job_finish_callback() :: fun((master_job_extended_args(), description()) -> ok).
% Types used to provide additional information to framework
-type timestamp() :: non_neg_integer(). % Timestamp used to sort tasks (usually provided by callback function)
-type sync_info() ::  #{
    mutator => datastore_doc:mutator(),
    scope => datastore_doc:scope(),
    sync_enabled => boolean(),
    local_links_tree_id => datastore:tree_id(),
    remote_driver => datastore:remote_driver(),
    remote_driver_ctx => datastore:remote_driver_ctx()
}.
% Internal types for framework
-type execution_pool() :: worker_pool:name(). % internal names of worker pools used by framework
-type ctx() :: traverse_task:ctx().

-export_type([pool/0, id/0, task/0, group/0, job/0, job_id/0, job_status/0, environment_id/0, description/0, status/0,
    additional_data/0, master_job_extended_args/0, timestamp/0, sync_info/0, master_job_map/0, callback_module/0]).

-define(MASTER_POOL_NAME(Pool), binary_to_atom(<<Pool/binary, "_master">>, utf8)).
-define(SLAVE_POOL_NAME(Pool), binary_to_atom(<<Pool/binary, "_slave">>, utf8)).
-define(CALL_TIMEOUT, timer:hours(24)).

-define(DEFAULT_GROUP, <<"main_group">>).
-define(DEFAULT_ENVIRONMENT_ID, <<"default_executor">>).

%% When to_string/1 function is not implemented by the callback module,
%% str_utils:format/1 is used to print job in logs (see to_string/1 function).
%% Unfortunately, an exception is thrown and handled inside this function
%% and because of that macro ?error_stacktrace prints wrong stacktrace.
%% This macro is used to bypass the problem.
-define(log_error_with_stacktrace(Format, Args),
    log_error_with_stacktrace(erlang:get_stacktrace(), Format, Args)
).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv init_pool(PoolName, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, #{}).
%% @end
%%--------------------------------------------------------------------
-spec init_pool(pool(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok | no_return().
init_pool(PoolName, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) ->
    init_pool(PoolName, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Inits the pool (starting appropriate worker pools and adding node to load balancing document)
%% and restarts tasks if needed.
%% @end
%%--------------------------------------------------------------------
-spec init_pool(pool(), non_neg_integer(), non_neg_integer(), non_neg_integer(), pool_options()) -> ok | no_return().
init_pool(PoolName, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, Options) ->
    MasterPool = worker_pool:start_sup_pool(?MASTER_POOL_NAME(PoolName), [{workers, MasterJobsNum}, {queue_type, lifo}]),
    SlavePool = worker_pool:start_sup_pool(?SLAVE_POOL_NAME(PoolName), [{workers, SlaveJobsNum}, {queue_type, lifo}]),
    try
        {{ok, _}, {ok, _}} = {MasterPool, SlavePool}
    catch
        error:{badmatch, {{error, {already_started, _}}, {error, {already_started, _}}}} ->
            throw({error, already_exists})
    end,

    Executor = maps:get(executor, Options, ?DEFAULT_ENVIRONMENT_ID),
    CallbackModules = maps:get(callback_modules, Options, [binary_to_atom(PoolName, utf8)]),
    ShouldRestart = maps:get(restart, Options, true),
    ok = traverse_tasks_scheduler:init(PoolName, ParallelOrdersLimit),
    IdToCtx = repair_ongoing_tasks(PoolName, Executor),

    case ShouldRestart of
        true ->
            lists:foreach(fun(CallbackModule) ->
                {ok, JobIDs} = traverse_task_list:list_local_jobs(PoolName, CallbackModule),

                lists:foreach(fun(JobID) ->
                    {ok, Job, _, TaskID} = CallbackModule:get_job(JobID),
                    case proplists:get_value(TaskID, IdToCtx, undefined) of
                        undefined ->
                            ?warning("Job: ~p (id: ~p) of undefined (probably finished) task: ~p", [Job, JobID, TaskID]),
                            ok;
                        ExtendedCtx ->
                            {ok, _, _} = traverse_task:update_description(ExtendedCtx, PoolName, TaskID, #{
                                master_jobs_delegated => 1
                            }),
                            ok = run_on_master_pool(PoolName, ?MASTER_POOL_NAME(PoolName), ?SLAVE_POOL_NAME(PoolName),
                                CallbackModule, ExtendedCtx, Executor, TaskID, Job, JobID)
                    end
                end, JobIDs)
            end, CallbackModules);
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Stops pool and prevents load balancing algorithm from scheduling tasks on node.
%% Warning: possible races with task scheduling - make sure that there are no tasks waiting to be executed.
%% @end
%%--------------------------------------------------------------------
-spec stop_pool(pool()) -> ok.
stop_pool(PoolName) ->
    ok = worker_pool:stop_sup_pool(?MASTER_POOL_NAME(PoolName)),
    ok = worker_pool:stop_sup_pool(?SLAVE_POOL_NAME(PoolName)),

    case traverse_tasks_scheduler:clear(PoolName) of
        ok -> ok;
        {error, not_found} -> ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv run(PoolName, TaskID, Job, #{}).
%% @end
%%--------------------------------------------------------------------
-spec run(pool(), id(), job()) -> ok.
run(PoolName, TaskID, Job) ->
    run(PoolName, TaskID, Job, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Initializes task. The task can be started immediately or scheduled (see traverse_tasks_scheduler.erl).
%% @end
%%--------------------------------------------------------------------
-spec run(pool(), id(), job(), run_options()) -> ok.
run(PoolName, TaskID, Job, Options) ->
    Executor = maps:get(executor, Options, ?DEFAULT_ENVIRONMENT_ID),
    Creator = maps:get(creator, Options, Executor),
    CallbackModule = maps:get(callback_module, Options, binary_to_atom(PoolName, utf8)),
    TaskGroup = maps:get(group_id, Options, ?DEFAULT_GROUP),
    AdditionalData = maps:get(additional_data, Options, #{}),
    ExtendedCtx = get_extended_ctx(CallbackModule, Job),

    {JobStatus, Node, Description} = case Creator =:= Executor of
        true ->
            case traverse_tasks_scheduler:increment_ongoing_tasks_and_choose_node(PoolName) of
                {ok, ChosenNode} ->
                    {on_pool, ChosenNode, #{master_jobs_delegated => 1}};
                {error, limit_exceeded} ->
                    {waiting, undefined, #{}}
            end;
        _ ->
            {waiting, remote, #{}}
    end,

    {ok, JobID} = CallbackModule:update_job_progress(main_job, Job, PoolName, TaskID, JobStatus),
    ok = traverse_task:create(ExtendedCtx, PoolName, CallbackModule, TaskID, Creator, Executor,
        TaskGroup, JobID, Node, Description, AdditionalData),

    case Node of
        undefined ->
            ok;
        remote ->
            ok;
        _ ->
            ok = task_callback(CallbackModule, task_started, TaskID, PoolName),
            ok = rpc:call(Node, ?MODULE, run_on_master_pool, [
                PoolName, ?MASTER_POOL_NAME(PoolName), ?SLAVE_POOL_NAME(PoolName),
                CallbackModule, ExtendedCtx, Executor, TaskID, Job, JobID])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Executes action connected with remote task change (can start task).
%% @end
%%--------------------------------------------------------------------
-spec on_task_change(task(), environment_id()) -> ok.
on_task_change(Task, Environment) ->
    case traverse_task:on_task_change(Task, Environment) of
        {remote_change, CallbackModule, MainJobID} ->
            case CallbackModule:get_job(MainJobID) of
                {ok, Job, PoolName, _} ->
                    ExtendedCtx = get_extended_ctx(CallbackModule, Job),
                    case traverse_task:on_remote_change(ExtendedCtx, Task, CallbackModule, Environment) of
                        ok ->
                            ok;
                        {ok, remote_cancel, TaskID} ->
                            task_callback(CallbackModule, on_cancel_init, TaskID, PoolName),
                            ok
                    end;
                {error, not_found} ->
                    ok
            end;
        {run, CallbackModule, MainJobID} ->
            case CallbackModule:get_job(MainJobID) of
                {ok, Job, PoolName, TaskID} ->
                    maybe_run_scheduled_task(PoolName, CallbackModule, TaskID, Task, Environment, Job, MainJobID);
                {error, not_found} ->
                    ok
            end;
        ignore ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Starts task scheduled on other environment if possible (limit of parallel tasks is not reached).
%% @end
%%--------------------------------------------------------------------
-spec on_job_change(job(), job_id(), pool(), id(), environment_id()) -> ok.
on_job_change(Job, JobID, PoolName, TaskID, Environment) ->
    case traverse_task:get(PoolName, TaskID) of
        {ok, Task} ->
            case traverse_task:is_enqueued(Task) of
                false ->
                    ok;
                true ->
                    {ok, CallbackModule, Executor, _} = traverse_task:get_execution_info(Task),
                    case Executor =:= Environment of
                        true ->
                            maybe_run_scheduled_task(PoolName, CallbackModule, TaskID, Task, Executor, Job, JobID);
                        _ ->
                            ok
                    end
            end;
        {error, not_found} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv cancel(PoolName, TaskID, ?DEFAULT_ENVIRONMENT_ID).
%% @end
%%--------------------------------------------------------------------
-spec cancel(pool(), id()) -> ok | {error, term()}.
cancel(PoolName, TaskID) ->
    cancel(PoolName, TaskID, ?DEFAULT_ENVIRONMENT_ID).

%%--------------------------------------------------------------------
%% @doc
%% Cancels task. Prevents jobs waiting in worker pools queues from execution.
%% @end
%%--------------------------------------------------------------------
-spec cancel(pool(), id(), environment_id()) -> ok | {error, term()}.
cancel(PoolName, TaskID, Environment) ->
    case traverse_task:get(PoolName, TaskID) of
        {ok, Task} ->
            {ok, CallbackModule, _, MainJobID} = traverse_task:get_execution_info(Task),
            case CallbackModule:get_job(MainJobID) of
                {ok, Job, _, _} ->
                    ExtendedCtx = get_extended_ctx(CallbackModule, Job),
                    {ok, Info} = traverse_task:cancel(ExtendedCtx, PoolName, CallbackModule, TaskID, Environment),
                    case Info of
                        local_cancel -> task_callback(CallbackModule, on_cancel_init, TaskID, PoolName);
                        _ -> ok
                    end;
                {error, not_found} ->
                    {error, main_job_not_found}
            end;
        Other ->
            Other
    end.

%%%===================================================================
%%% Functions executed on pools
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Executes master job using function provided by callback module. To be executed by worker pool process.
%% Master job is provided with function to enqueue next master jobs during the callback execution if needed.
%% After callback execution, async_master_jobs from master job answer are enqueued. Afterwards, sequential_slave_jobs
%% and next slave_jobs are executed on slave pool and the process awaits for their finish. At the end, master_jobs
%% are enqueued and finish_callback is executed.
%% @end
%%--------------------------------------------------------------------
-spec execute_master_job(pool(), execution_pool(), execution_pool(), callback_module(), ctx(), environment_id(),
    id(), job(), job_id()) -> ok.
execute_master_job(PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, Job, JobID) ->
    try
        MasterJobCallback = prepare_master_callback(PoolName, MasterPool, SlavePool, CallbackModule,
            ExtendedCtx, Executor, TaskID),
        MasterJobExtendedArgs = #{task_id => TaskID, master_job_starter_callback => MasterJobCallback},
        {ok, MasterAns} =  CallbackModule:do_master_job(Job, MasterJobExtendedArgs),
        MasterJobsList = maps:get(master_jobs, MasterAns, []),
        AsyncMasterJobsList = maps:get(async_master_jobs, MasterAns, []),
        SlaveJobsList = maps:get(slave_jobs, MasterAns, []),
        SequentialSlaveJobsList = maps:get(sequential_slave_jobs, MasterAns, []),
        SlaveJobsDelegatedNum = length(SlaveJobsList) + length(lists:flatten(SequentialSlaveJobsList)),

        Description0 = maps:get(description, MasterAns, #{}),
        Description = Description0#{
            slave_jobs_delegated => SlaveJobsDelegatedNum,
            master_jobs_delegated => length(MasterJobsList) + length(AsyncMasterJobsList)
        },

        {ok, _, Canceled} = traverse_task:update_description(ExtendedCtx, PoolName, TaskID, Description),

        {_, NewDescription, Canceled2} = case Canceled of
            true ->
                ok = traverse_task_list:delete_job_link(PoolName, CallbackModule, JobID),
                {ok, _} = CallbackModule:update_job_progress(JobID, Job, PoolName, TaskID, canceled),
                CancelDescription = #{
                    slave_jobs_delegated => -1 * (length(SlaveJobsList) + length(lists:flatten(SequentialSlaveJobsList))),
                    master_jobs_delegated => -1 * (length(MasterJobsList) + length(AsyncMasterJobsList)) - 1
                },
                CancelCallback = maps:get(cancel_callback, MasterAns, fun(_Args) -> ok end),
                CancelCallback(MasterJobExtendedArgs, CancelDescription),
                {ok, _, _} = traverse_task:update_description(ExtendedCtx, PoolName, TaskID, CancelDescription);
            _ ->
%%                CancelCallback = maps:get(init_callback, MasterAns, fun(_Args) -> ok end),
%%                CancelCallback(MasterJobExtendedArgs),
                ok = run_on_master_pool(
                    PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, AsyncMasterJobsList),

                SequentialSlaveAnswers = sequential_run_on_slave_pool(
                    PoolName, SlavePool, CallbackModule, ExtendedCtx, TaskID, SequentialSlaveJobsList),
                SlaveAnswers = run_on_slave_pool(
                    PoolName, SlavePool, CallbackModule, ExtendedCtx, TaskID, SlaveJobsList),

                ok = run_on_master_pool(
                    PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, MasterJobsList),

                {SlavesOk, SlavesErrors} = lists:foldl(fun
                    ({ok, ok}, {OkSum, ErrorSum}) -> {OkSum + 1, ErrorSum};
                    (_, {OkSum, ErrorSum}) -> {OkSum, ErrorSum + 1}
                end, {0, 0}, SequentialSlaveAnswers ++ SlaveAnswers),

                ok = traverse_task_list:delete_job_link(PoolName, CallbackModule, JobID),
                {ok, _} = CallbackModule:update_job_progress(JobID, Job, PoolName, TaskID, ended),
                Description2 = #{
                    slave_jobs_done => SlavesOk,
                    slave_jobs_failed => SlavesErrors,
                    master_jobs_done => 1
                },
                SlavesDescription = #{
                    slave_jobs_delegated => SlaveJobsDelegatedNum,
                    slave_jobs_done => SlavesOk,
                    slave_jobs_failed => SlavesErrors
                },
                FinishCallback = maps:get(finish_callback, MasterAns, fun(_Args, _SlavesDescription) -> ok end),
                FinishCallback(MasterJobExtendedArgs, SlavesDescription),
                {ok, _, _} = traverse_task:update_description(ExtendedCtx, PoolName, TaskID, Description2)
        end,

        try
            maybe_finish(PoolName, CallbackModule, ExtendedCtx, TaskID, Executor, NewDescription, Canceled2)
        catch
            E2:R2 ->
                ?log_error_with_stacktrace("Checking finish of job ~s of task ~p (module ~p) error ~p:~p",
                    [to_string(CallbackModule, Job), TaskID, CallbackModule, E2, R2])
        end
    catch
        E1:R1 ->
            ?log_error_with_stacktrace("Master job ~s of task ~s (module ~p) error ~p:~p",
                [to_string(CallbackModule, Job), TaskID, CallbackModule, E1, R1]),
            ErrorDescription = #{
                master_jobs_failed => 1
            },
            % TODO - VFS-5532
            catch traverse_task_list:delete_job_link(PoolName, CallbackModule, JobID),
            catch CallbackModule:update_job_progress(JobID, Job, PoolName, TaskID, failed),
            {ok, ErrorDescription2, Canceled3} = traverse_task:update_description(
                ExtendedCtx, PoolName, TaskID, ErrorDescription),

            try
                maybe_finish(PoolName, CallbackModule, ExtendedCtx, TaskID, Executor, ErrorDescription2, Canceled3)
            catch
                E3:R3 ->
                    ?log_error_with_stacktrace("Checking finish of job ~s of task ~p (module ~p) error ~p:~p",
                        [to_string(CallbackModule, Job), TaskID, CallbackModule, E3, R3])
            end
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Executes slave job using function provided by callback module. To be executed by worker pool process.
%% @end
%%--------------------------------------------------------------------
-spec execute_slave_job(pool(), callback_module(), ctx(), id(), job()) -> ok | error.
execute_slave_job(PoolName, CallbackModule, ExtendedCtx, TaskID, Job) ->
    try
        case CallbackModule:do_slave_job(Job, TaskID) of
            ok ->
                ok;
            {ok, Description} ->
                {ok, _, _} = traverse_task:update_description(ExtendedCtx, PoolName, TaskID, Description),
                ok;
            {error, _} ->
                error
        end
    catch
        E:R ->
            ?log_error_with_stacktrace("Slave job ~s of task ~p (module ~p) error ~p:~p",
                [to_string(CallbackModule, Job), TaskID, CallbackModule, E, R]),
            error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec run_on_slave_pool(pool(), execution_pool(), callback_module(), ctx(), id(), job() | [job()]) -> [ok | error].
run_on_slave_pool(PoolName, SlavePool, CallbackModule, ExtendedCtx, TaskID, Jobs) when is_list(Jobs) ->
    utils:pmap(fun(Job) ->
        worker_pool:call(SlavePool, {?MODULE, execute_slave_job, [PoolName, CallbackModule, ExtendedCtx, TaskID, Job]},
            worker_pool:default_strategy(), ?CALL_TIMEOUT)
    end, Jobs);
run_on_slave_pool(PoolName, SlavePool, CallbackModule, ExtendedCtx, TaskID, Job) ->
    run_on_slave_pool(PoolName, SlavePool, CallbackModule, ExtendedCtx, TaskID, [Job]).

-spec sequential_run_on_slave_pool(pool(), execution_pool(), callback_module(), ctx(), id(), [job() | [job()]]) ->
    [ok | error].
sequential_run_on_slave_pool(PoolName, SlavePool, CallbackModule, ExtendedCtx, TaskID, Jobs) ->
    Ans = lists:map(fun(ParallelJobs) ->
        run_on_slave_pool(PoolName, SlavePool, CallbackModule, ExtendedCtx, TaskID, ParallelJobs)
    end, Jobs),
    lists:flatten(Ans).

-spec run_on_master_pool(pool(), execution_pool(), execution_pool(), callback_module(), ctx(), environment_id(),
    id(), [job() | {job(), job_id()}]) -> ok.
run_on_master_pool(PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, Jobs) ->
    lists:foreach(fun
        ({Job, JobID}) ->
            run_on_master_pool(PoolName, MasterPool, SlavePool, CallbackModule,
                ExtendedCtx, Executor, TaskID, Job, JobID);
        (Job) ->
            run_on_master_pool(PoolName, MasterPool, SlavePool, CallbackModule,
                ExtendedCtx, Executor, TaskID, Job, undefined)
    end, Jobs).

-spec prepare_master_callback(pool(), execution_pool(), execution_pool(), callback_module(), ctx(), environment_id(),
    id()) -> master_job_starter_callback().
prepare_master_callback(PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID) ->
    fun(Jobs) ->
        Description = #{
            master_jobs_delegated => length(Jobs)
        },
        {ok, _, Canceled} = traverse_task:update_description(ExtendedCtx, PoolName, TaskID, Description),
        case Canceled of
            true ->
                CancelDescription = #{
                    master_jobs_delegated => -1 * length(Jobs)
                },
                {ok, _, _} = traverse_task:update_description(ExtendedCtx, PoolName, TaskID, CancelDescription);
            _ ->
                run_on_master_pool(PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, Jobs)
        end
    end.

-spec run_on_master_pool(pool(), execution_pool(), execution_pool(), callback_module(), ctx(), environment_id(),
    id(), job(), job_id()) -> ok.
run_on_master_pool(PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, Job, JobID) ->
    {ok, JobID2} = CallbackModule:update_job_progress(JobID, Job, PoolName, TaskID, on_pool),
    ok = traverse_task_list:add_job_link(PoolName, CallbackModule, JobID2),
    ok = worker_pool:cast(MasterPool, {?MODULE, execute_master_job,
        [PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, Job, JobID2]}).

-spec maybe_finish(pool(), callback_module(), ctx(), id(), environment_id(), description(), boolean()) -> ok.
maybe_finish(PoolName, CallbackModule, ExtendedCtx, TaskID, Executor, #{
    master_jobs_delegated := Delegated
} = Description, Canceled) ->
    Done = maps:get(master_jobs_done, Description, 0),
    Failed = maps:get(master_jobs_failed, Description, 0),

    case Delegated == Done + Failed of
        true ->
            % VFS-5532 - can never be equal in case of description saving error
            ok = case Canceled of
                true -> task_callback(CallbackModule, task_canceled, TaskID, PoolName);
                _ -> task_callback(CallbackModule, task_finished, TaskID, PoolName)
            end,

            ok = traverse_task:finish(ExtendedCtx, PoolName, CallbackModule, TaskID),
            check_task_list_and_run(PoolName, Executor, []);
        _ -> ok
    end.

-spec check_task_list_and_run(pool(), environment_id(), [traverse:group()]) -> ok.
check_task_list_and_run(PoolName, Executor, CheckedGroups) ->
    case traverse_tasks_scheduler:get_next_group(PoolName) of
        {error, no_groups} ->
            ok = traverse_tasks_scheduler:decrement_ongoing_tasks(PoolName),
            case traverse_tasks_scheduler:get_next_group(PoolName) of
                {error, no_groups} ->
                    ok;
                _ ->
                    % Race with task starting
                    retry_run(PoolName, Executor, 0)
            end;
        {ok, GroupID} ->
            case lists:member(GroupID, CheckedGroups) of
                true ->
                    ok = traverse_tasks_scheduler:decrement_ongoing_tasks(PoolName),
                    retry_run(PoolName, Executor, 10000);
                false ->
                    StartAns = case traverse_task_list:get_first_scheduled_link(PoolName, GroupID, Executor) of
                        {ok, not_found} ->
                            case deregister_group_and_check(PoolName, GroupID, Executor) of
                                ok ->
                                    start_interrupted;
                                {abort, TaskID} ->
                                    run_task(PoolName, TaskID, Executor)
                            end;
                        {ok, TaskID} ->
                            run_task(PoolName, TaskID, Executor)
                    end,

                    case StartAns of
                        ok ->
                            ok;
                        start_interrupted ->
                            check_task_list_and_run(PoolName, Executor, [GroupID | CheckedGroups])
                    end
            end
    end.

-spec run_task(pool(), id(), environment_id()) -> ok | start_interrupted.
run_task(PoolName, TaskID, Executor) ->
    {ok, CallbackModule, _, MainJobID} = traverse_task:get_execution_info(PoolName, TaskID),
    case  CallbackModule:get_job(MainJobID) of
        {ok, Job, _, _} ->
            ExtendedCtx = get_extended_ctx(CallbackModule, Job),
            case traverse_task:start(ExtendedCtx, PoolName, CallbackModule, TaskID, #{master_jobs_delegated => 1}) of
                ok ->
                    ok = task_callback(CallbackModule, task_started, TaskID, PoolName),
                    ok = run_on_master_pool(PoolName, ?MASTER_POOL_NAME(PoolName), ?SLAVE_POOL_NAME(PoolName),
                        CallbackModule, ExtendedCtx, Executor, TaskID, Job, MainJobID);
                {error, start_aborted} ->
                    start_interrupted;
                {error, not_found} ->
                    start_interrupted
            end;
        {error, not_found} ->
            start_interrupted
    end.

-spec retry_run(pool(), environment_id(), non_neg_integer()) -> ok.
retry_run(PoolName, Executor, Delay) ->
    spawn(fun() ->
        timer:sleep(Delay),
        check_task_list_and_run(PoolName, Executor, [])
    end),
    ok.

-spec maybe_run_scheduled_task(pool(), callback_module(), id(), task(), environment_id(), job(), job_id()) -> ok.
maybe_run_scheduled_task(PoolName, CallbackModule, TaskID, Task, Executor, Job, MainJobID) ->
    case traverse_tasks_scheduler:increment_ongoing_tasks_and_choose_node(PoolName) of
        {ok, Node} ->
            ExtendedCtx = get_extended_ctx(CallbackModule, Job),
            case traverse_task:start(ExtendedCtx, PoolName, CallbackModule, TaskID, #{master_jobs_delegated => 1}) of
                ok ->
                    ok = task_callback(CallbackModule, task_started, TaskID, PoolName),
                    ok = rpc:call(Node, ?MODULE, run_on_master_pool, [PoolName, ?MASTER_POOL_NAME(PoolName),
                        ?SLAVE_POOL_NAME(PoolName), CallbackModule, ExtendedCtx, Executor, TaskID, Job, MainJobID]);
                {error, start_aborted} ->
                    traverse_tasks_scheduler:decrement_ongoing_tasks(PoolName)
            end;
        {error, limit_exceeded} ->
            traverse_task:schedule_for_local_execution(PoolName, TaskID, Task)
    end.

-spec deregister_group_and_check(pool(), group(), environment_id()) -> ok| {abort, traverse:id()}.
deregister_group_and_check(PoolName, Group, Executor) ->
    ok = traverse_tasks_scheduler:deregister_group(PoolName, Group),
    % check for races with task creation
    case traverse_task_list:get_first_scheduled_link(PoolName, Group, Executor) of
        {ok, not_found} ->
            ok;
        {ok, Task} ->
            traverse_tasks_scheduler:register_group(PoolName, Group),
            {abort, Task}
    end.

-spec get_extended_ctx(callback_module(), job()) -> ctx().
get_extended_ctx(CallbackModule, Job) ->
    {ok, CtxExtension} = case erlang:function_exported(CallbackModule, get_sync_info, 1) of
        true ->
            CallbackModule:get_sync_info(Job);
        _ ->
            {ok, #{}}
    end,
    maps:merge(traverse_task:get_ctx(), CtxExtension).

-spec task_callback(callback_module(), task_started | task_finished | task_canceled | on_cancel_init, id(), pool()) -> ok.
task_callback(CallbackModule, Method, TaskID, PoolName) ->
    case erlang:function_exported(CallbackModule, Method, 2) of
        true ->
            ok = CallbackModule:Method(TaskID, PoolName);
        _ ->
            ok
    end.

-spec to_string(callback_module(), job()) -> term().
to_string(CallbackModule, Job) ->
    case erlang:function_exported(CallbackModule, to_string, 1) of
        true ->
            CallbackModule:to_string(Job);
        _ -> 
            str_utils:format_bin("~p", [Job])
    end.

-spec repair_ongoing_tasks(pool(), environment_id()) -> [{id(), ctx() | other_node}].
repair_ongoing_tasks(Pool, Executor) ->
    {ok, TaskIDs, _} = traverse_task_list:list(Pool, ongoing, #{tree_id => Executor}),

    lists:map(fun(Id) ->
        {ok, CallbackModule, Executor, MainJobID} = traverse_task:get_execution_info(Pool, Id),
        {ok, Job, _, _} = CallbackModule:get_job(MainJobID),
        ExtendedCtx = get_extended_ctx(CallbackModule, Job),
        case traverse_task:fix_description(ExtendedCtx, Pool, Id) of
            {ok, _} -> {Id, ExtendedCtx};
            {error, other_node} -> {Id, other_node}
        end
    end, TaskIDs).

-spec log_error_with_stacktrace(term(), string(), [term()]) -> ok.
log_error_with_stacktrace(Stacktrace, Format, Args) ->
    ?error(Format ++ "~nStacktrace:~n~p", Args ++ [Stacktrace]).
