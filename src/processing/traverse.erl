%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model that allows traversing via different structures and
%%% execution of slave jobs on children.
%%% @end
%%%-------------------------------------------------------------------
-module(traverse).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/logging.hrl").

%% API
-export([init_pool/4, init_pool/5, stop_pool/1,
    run/3, run/4, run/5, run/6, maybe_run_scheduled_task/2]).
%% Functions executed on pools
-export([execute_master_job/8, execute_slave_job/3]).
%% For rpc:
-export([run_on_master_pool/7, check_task_list_and_run/2]).

-type id() :: traverse_task:key().
-type task() :: traverse_task:doc().
-type group() :: binary().
-type job() :: term().
-type job_id() :: datastore:key().
-type executor() :: binary().
-type pool() :: atom().
-type task_module() :: atom().
-type description() :: map().
-type status() :: atom().
-type job_status() :: waiting | started | finish.

-export_type([id/0, group/0, job/0, job_id/0, executor/0, 
    pool/0, task_module/0, description/0, status/0, job_status/0]).

-define(MASTER_POOL_NAME(Pool), list_to_atom(atom_to_list(Pool) ++ "_master")).
-define(SLAVE_POOL_NAME(Pool), list_to_atom(atom_to_list(Pool) ++ "_slave")).
-define(CALL_TIMEOUT, timer:hours(24)).

-define(DEFAULT_GROUP, <<"main_group">>).
-define(DEFAULT_EXECUTOR_ID, <<"executor">>).

% co jesli plik zostanie zrename'owany albo co gorsza katalog przesuniety na przerobiony juz poziom drzewa
% musi byc load balancing puli danego typu ze jeden space nie zagladza drugiego
% mozna pomyslec o tym zeby robic load balancing per user

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv init_pool(TaskModule, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, ?DEFAULT_EXECUTOR_ID).
%% @end
%%--------------------------------------------------------------------
-spec init_pool(task_module(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok.
init_pool(TaskModule, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) ->
    init_pool(TaskModule, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, ?DEFAULT_EXECUTOR_ID).

%%--------------------------------------------------------------------
%% @doc
%% Inits the pool and restarts tasks if needed.
%% @end
%%--------------------------------------------------------------------
-spec init_pool(task_module(), non_neg_integer(), non_neg_integer(), non_neg_integer(), executor()) -> ok.
init_pool(TaskModule, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, Executor) ->
    {ok, _} = worker_pool:start_sup_pool(?MASTER_POOL_NAME(TaskModule),
        [{workers, MasterJobsNum}, {queue_type, lifo}]),
    {ok, _} = worker_pool:start_sup_pool(?SLAVE_POOL_NAME(TaskModule),
        [{workers, SlaveJobsNum}, {queue_type, lifo}]),

    ok = traverse_tasks_load_balance:init_task_module(TaskModule, ParallelOrdersLimit),
    IDsToGroups = traverse_task:repair_ongoing_tasks(TaskModule, Executor),
    {ok, JobIDs} = TaskModule:list_ongoing_jobs(),

    lists:foreach(fun(JobID) ->
        {ok, Job, TaskID} = TaskModule:get_job(JobID),
        case proplists:get_value(TaskID, IDsToGroups) of
            undefined ->
                ok;
            GR ->
                {ok, _, _} = traverse_task:update_description(TaskID, TaskModule, #{
                    master_jobs_delegated => 1
                }),
                ok = run_on_master_pool(?MASTER_POOL_NAME(TaskModule), ?SLAVE_POOL_NAME(TaskModule),
                    TaskModule, TaskID, Executor, GR, [{Job, JobID}])
        end
    end, JobIDs).

%%--------------------------------------------------------------------
%% @doc
%% Stops pool.
%% @end
%%--------------------------------------------------------------------
-spec stop_pool(task_module()) -> ok.
stop_pool(TaskModule) ->
    ok = worker_pool:stop_sup_pool(?MASTER_POOL_NAME(TaskModule)),
    ok = worker_pool:stop_sup_pool(?SLAVE_POOL_NAME(TaskModule)),

    case traverse_tasks_load_balance:clear_task_module(TaskModule) of
        ok -> ok;
        {error, not_found} -> ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv run(TaskModule, TaskID, ?DEFAULT_GROUP, Job).
%% @end
%%--------------------------------------------------------------------
-spec run(task_module(), id(), job()) -> ok.
run(TaskModule, TaskID, Job) ->
    run(TaskModule, TaskID, ?DEFAULT_GROUP, Job).

%%--------------------------------------------------------------------
%% @doc
%% @equiv run(TaskModule, TaskID, TaskGroup, Job, ?DEFAULT_EXECUTOR_ID).
%% @end
%%--------------------------------------------------------------------
-spec run(task_module(), id(), group(), job()) -> ok.
run(TaskModule, TaskID, TaskGroup, Job) ->
    run(TaskModule, TaskID, TaskGroup, Job, ?DEFAULT_EXECUTOR_ID).

%%--------------------------------------------------------------------
%% @doc
%% @equiv run(TaskModule, TaskID, TaskGroup, Job, Creator, Creator).
%% @end
%%--------------------------------------------------------------------
-spec run(task_module(), id(), group(), job(), executor()) -> ok.
run(TaskModule, TaskID, TaskGroup, Job, Creator) ->
    run(TaskModule, TaskID, TaskGroup, Job, Creator, Creator).

%%--------------------------------------------------------------------
%% @doc
%% Initializes task.
%% @end
%%--------------------------------------------------------------------
-spec run(task_module(), id(), group(), job(), executor(), executor()) -> ok.
run(TaskModule, TaskID, TaskGroup, Job, Creator, Executor) ->
    case Creator =:= Executor of
        true ->
            case traverse_tasks_load_balance:add_task(TaskModule, TaskGroup) of
                {ok, Node} ->
                    ok = traverse_task:create(TaskID, TaskModule, Executor, Creator, TaskGroup, {node, Node}, #{
                        master_jobs_delegated => 1
                    }),
                    ok = rpc:call(Node, ?MODULE, run_on_master_pool, [
                        ?MASTER_POOL_NAME(TaskModule), ?SLAVE_POOL_NAME(TaskModule),
                        TaskModule, TaskID, Executor, TaskGroup, [Job]]);
                {error, limit_exceeded} ->
                    {ok, InitialJob} = TaskModule:save_job(Job, TaskID, waiting),
                    ok = traverse_task:create(TaskID, TaskModule, Executor, Creator, TaskGroup, {job, InitialJob}, #{})
            end;
        _ ->
            {ok, InitialJob} = TaskModule:save_job(Job, TaskID, waiting),
            ok = traverse_task:create(TaskID, TaskModule, Executor, Creator, TaskGroup, {job, InitialJob}, #{})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if there is task to be executed ans starts it if possible.
%% @end
%%--------------------------------------------------------------------
%%-spec maybe_run_scheduled_task(task()) -> ok.
maybe_run_scheduled_task({task, Task}, Executor) ->
    case traverse_task:get_executor(Task) =:= Executor of
        true ->
            {TaskModule, MainJobID} = traverse_task:get_task_module_and_job(Task),

            case TaskModule:get_job(MainJobID) of
                {ok, Job, TaskID} ->
                    maybe_run_scheduled_task(TaskModule, TaskID, Task, MainJobID, Job);
                {error, not_found} ->
                    ok
            end;
        _ ->
            ok
    end;
% TODO - moze podac caly job?
maybe_run_scheduled_task({job, TaskModule, MainJobID}, Executor) ->
    case TaskModule:get_job(MainJobID) of
        {ok, Job, TaskID} ->
            case traverse_task:get(TaskID) of
                {ok, Task} ->
                    case traverse_task:get_executor(Task) =:= Executor of
                        true ->
                            maybe_run_scheduled_task(TaskModule, TaskID, Task, MainJobID, Job);
                        _ ->
                            ok
                    end;
                {error, not_found} ->
                    ok
            end;
        {error, not_found} ->
            ok
    end.

maybe_run_scheduled_task(TaskModule, TaskID, Task, MainJobID, Job) ->
    case traverse_tasks_load_balance:add_task(TaskModule) of
        {ok, Node} ->
            case traverse_task:start(Task, #{
                master_jobs_delegated => 1
            }) of
                {ok, Executor, GroupID} ->
                    ok = rpc:call(Node, ?MODULE, run_on_master_pool, [
                        ?MASTER_POOL_NAME(TaskModule), ?SLAVE_POOL_NAME(TaskModule),
                        TaskModule, TaskID, Executor, GroupID, [{Job, MainJobID}]]);
                {error, already_started} ->
                    ok
            end
    end.

%%%===================================================================
%%% Functions executed on pools
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Executes master job pool. To be executed inside pool.
%% @end
%%--------------------------------------------------------------------
-spec execute_master_job(pool(), pool(), task_module(), id(), executor(), group(), job(), job_id()) -> ok.
execute_master_job(MasterPool, SlavePool, TaskModule, TaskID, Executor, GroupID, Job, JobID) ->
    try
        % TODO - handle failures (jobow lub persystencji informacji co powinno zabic caly task)
        % TODO - przy startowaniu sprawdzamy czy task nie zostal anulowany
        {ok, SlaveJobsList, MasterJobsList, Description0} = case TaskModule:do_master_job(Job) of
            {ok, SJL, MJL} -> {ok, SJL, MJL, #{}};
            % Moze handlowac zwrocenie errora, zeby nie lecial badmatch?
            Other -> Other
        end,

        Description = Description0#{
            slave_jobs_delegated => length(SlaveJobsList),
            master_jobs_delegated => length(MasterJobsList)
        },
        {ok, _, Canceled} = UpdateAns = traverse_task:update_description(TaskID, TaskModule, Description),

        {_, NewDescription, Canceled2} = case Canceled of
            true ->
                UpdateAns;
            _ ->
                SlaveAnswers = run_on_slave_pool(SlavePool, TaskModule, TaskID, SlaveJobsList),
                ok = run_on_master_pool(MasterPool, SlavePool, TaskModule, TaskID, Executor, GroupID, MasterJobsList),

                {SlavesOk, SlavesErrors} = lists:foldl(fun
                                                           ({ok, ok}, {OkSum, ErrorSum}) -> {OkSum + 1, ErrorSum};
                                                           (_, {OkSum, ErrorSum}) -> {OkSum, ErrorSum + 1}
                                                       end, {0, 0}, SlaveAnswers),

                ok = TaskModule:update_job(JobID, Job, TaskID, finish),
                Description2 = #{
                    slave_jobs_done => SlavesOk,
                    master_jobs_failed => SlavesErrors,
                    master_jobs_done => 1
                },
                {ok, _, _} = traverse_task:update_description(TaskID, TaskModule, Description2)
        end,

        try
            maybe_finish(TaskModule, TaskID, Executor, GroupID, NewDescription, Canceled2)
        catch
            E3:E4 ->
                ?error_stacktrace("Checking finish of job ~p of task ~p (module ~p) error ~p:~p",
                    [Job, TaskID, TaskModule, E3, E4])
        end
    catch
        E1:E2 ->
            ?error_stacktrace("Master job ~p of task ~p (module ~p) error ~p:~p",
                [Job, TaskID, TaskModule, E1, E2]),
            ErrorDescription = #{
                master_jobs_failed => 1
            },
            {ok, ErrorDescription2, Canceled3} = traverse_task:update_description(TaskID, TaskModule, ErrorDescription),

            try
                maybe_finish(TaskModule, TaskID, Executor, GroupID, ErrorDescription2, Canceled3)
            catch
                E5:E6 ->
                    ?error_stacktrace("Checking finish of job ~p of task ~p (module ~p) error ~p:~p",
                        [Job, TaskID, TaskModule, E5, E6])
            end
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Executes master job pool. To be executed inside pool.
%% @end
%%--------------------------------------------------------------------
-spec execute_slave_job(task_module(), id(), job()) -> ok | error.
execute_slave_job(TaskModule, TaskID, Job) ->
    try
        case TaskModule:do_slave_job(Job) of
            ok ->
                ok;
            {ok, Description} ->
                {ok, _, _} = traverse_task:update_description(TaskID, TaskModule, Description),
                ok
        end
    catch
        E1:E2 ->
            ?error_stacktrace("Slave job ~p of task ~p (module ~p) error ~p:~p",
                [Job, TaskID, TaskModule, E1, E2]),
            error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec run_on_slave_pool(pool(), task_module(), id(), job()) -> [ok | error].
run_on_slave_pool(Pool, TaskModule, TaskID, Jobs) ->
    utils:pmap(fun(Job) ->
        worker_pool:call(Pool, {?MODULE, execute_slave_job, [TaskModule, TaskID, Job]},
            worker_pool:default_strategy(), ?CALL_TIMEOUT)
    end, Jobs).

-spec run_on_master_pool(pool(), pool(), task_module(), id(), executor(), group(), [job() | {job(), job_id()}]) -> ok.
run_on_master_pool(MasterPool, SlavePool, TaskModule, TaskID, Executor, GroupID, Jobs) ->
    lists:foreach(fun
        ({Job, JobID}) ->
            ok = TaskModule:update_job(JobID, Job, TaskID, started),
            ok = worker_pool:cast(MasterPool, {?MODULE, execute_master_job,
                [MasterPool, SlavePool, TaskModule, TaskID, Executor, GroupID, Job, JobID]});
        (Job) ->
            {ok, JobID} = TaskModule:save_job(Job, TaskID, started),
            ok = worker_pool:cast(MasterPool, {?MODULE, execute_master_job,
                [MasterPool, SlavePool, TaskModule, TaskID, Executor, GroupID, Job, JobID]})
    end, Jobs).

-spec maybe_finish(task_module(), id(), executor(), group(), description(), boolean()) -> ok.
maybe_finish(TaskModule, TaskID, Executor, GroupID, #{
    master_jobs_delegated := Delegated
} = Description, Canceled) ->
    Done = maps:get(master_jobs_done, Description, 0),
    Failed = maps:get(master_jobs_failed, Description, 0),
    case {Delegated == Done + Failed, Canceled} of
        {true, _} ->
            % Co jesli nigdy sie nie zgra (polecial blad na save'owaniu info o starcie slave'a, a slave sie nie uruchil)
            ok = TaskModule:task_finished(TaskID),
            ok = traverse_task:finish(TaskID, TaskModule, Executor, GroupID, finished),
            check_task_list_and_run(TaskModule, Executor);
        {_, true} ->
            ok = traverse_task:finish(TaskID, TaskModule, Executor, GroupID, canceled),
            check_task_list_and_run(TaskModule, Executor);
        _ -> ok
    end.

-spec check_task_list_and_run(task_module(), executor()) -> ok.
check_task_list_and_run(TaskModule, Executor) ->
    case traverse_tasks_load_balance:get_next_group(TaskModule) of
        {error, no_groups} ->
            ok = traverse_tasks_load_balance:finish_task(TaskModule);
        {ok, GroupID} ->
            case traverse_task:get_next_task(TaskModule, GroupID) of
                {ok, {TaskID, MainJobID, Creator}} ->
                    ok = run_task(TaskModule, TaskID, Executor, GroupID, MainJobID, Creator);
                {ok, no_tasks_found} ->
                    case cancel_group(TaskModule, GroupID) of
                        ok ->
                            check_task_list_and_run(TaskModule, Executor);
                        {abort, {TaskID, MainJobID, Creator}} ->
                            ok = run_task(TaskModule, TaskID, Executor, GroupID, MainJobID, Creator)
                    end
            end
    end.

-spec run_task(task_module(), id(), executor(), group(), job_id(), executor()) -> ok.
run_task(TaskModule, TaskID, Executor, GroupID, MainJobID, Creator) ->
    % TODO - zrobic tu case i obsluzyc error not found (nie zsyncowal sie dokument)
    % TODO - wywolac callback on_task_start
    % TODO - osluzyc sytuacje jak juz task zostal uruchomiony na innym node
    {ok, Job, _} = TaskModule:get_job(MainJobID), % A moze info o nodzie przechowywac w linkach?
    case traverse_task:start(TaskID, TaskModule, GroupID, Executor, Creator, #{
        master_jobs_delegated => 1
    }) of
        ok ->
            ok = run_on_master_pool(?MASTER_POOL_NAME(TaskModule), ?SLAVE_POOL_NAME(TaskModule),
                TaskModule, TaskID, Executor, GroupID, [{Job, MainJobID}]);
        {error, already_started} ->
            check_task_list_and_run(TaskModule, Executor);
        {error, not_found} ->
            check_task_list_and_run(TaskModule, Executor)
    end.

-spec cancel_group(task_module(), group()) -> ok
    | {abort, {traverse:id(), traverse:job_id(), traverse:executor()}} | {error, term()}.
cancel_group(TaskModule, Group) ->
    case traverse_tasks_load_balance:cancel_group_init(TaskModule, Group) of
        ok ->
            case traverse_task:get_next_task(TaskModule, Group) of
                {ok, no_tasks_found} ->
                    traverse_tasks_load_balance:cancel_group(TaskModule, Group);
                {ok, Task} ->
                    traverse_tasks_load_balance:register_group(TaskModule, Group),
                    {abort, Task}
            end;
        already_canceled ->
            ok
    end.