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
    run/3, run/4, cancel/2, cancel/3, maybe_run_scheduled_task/2]).
%% Functions executed on pools
-export([execute_master_job/9, execute_slave_job/5]).
%% For rpc
-export([run_on_master_pool/9]).

-type pool() :: binary().
-type callback_module() :: atom().
-type id() :: traverse_task:key().
-type task() :: traverse_task:doc().
-type group() :: binary().
-type job() :: term().
-type job_id() :: datastore:key().
-type job_status() :: waiting | on_pool | ended | failed | canceled.
-type executor() :: traverse_task_list:tree().
-type execution_pool() :: atom().
-type description() :: map().
-type status() :: atom().
-type timestamp() :: non_neg_integer().
-type sync_info() :: map().
-type finish_callback() :: fun(() -> ok).
-type master_job_map() :: #{
    slave_jobs := [job()],
    master_jobs := [job()],
    description => description(),
    finish_callback => finish_callback()
}.
-type pool_options() :: #{
    executor => executor(),
    callback_modules => [callback_module()]
}.
-type run_options() :: #{
    executor => executor(),
    creator => executor(),
    callback_module => callback_module(),
    group_id => group()
}.
-type ctx() :: traverse_task:ctx().
-type ctx_sync_info() ::  #{model := datastore_model:model(),
    mutator => datastore_doc:mutator(),
    scope => datastore_doc:scope(),
    sync_enabled => boolean(),
    local_links_tree_id => datastore:tree_id(),
    remote_driver => datastore:remote_driver(),
    remote_driver_ctx => datastore:remote_driver_ctx()
}.
-type list_job_restart_info() :: term().

-export_type([pool/0, id/0, group/0, job/0, job_id/0, job_status/0, executor/0, description/0, status/0,
    timestamp/0, sync_info/0, master_job_map/0, list_job_restart_info/0, ctx_sync_info/0]).

-define(MASTER_POOL_NAME(Pool), binary_to_atom(<<Pool/binary, "_master">>, utf8)).
-define(SLAVE_POOL_NAME(Pool), binary_to_atom(<<Pool/binary, "_slave">>, utf8)).
-define(CALL_TIMEOUT, timer:hours(24)).

-define(DEFAULT_GROUP, <<"main_group">>).
-define(DEFAULT_EXECUTOR_ID, <<"default_executor">>).

% co jesli plik zostanie zrename'owany albo co gorsza katalog przesuniety na przerobiony juz poziom drzewa
% musi byc load balancing puli danego typu ze jeden space nie zagladza drugiego
% mozna pomyslec o tym zeby robic load balancing per user

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv init_pool(PoolName, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, #{}).
%% @end
%%--------------------------------------------------------------------
-spec init_pool(pool(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok.
init_pool(PoolName, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) ->
    init_pool(PoolName, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Inits the pool and restarts tasks if needed.
%% @end
%%--------------------------------------------------------------------
-spec init_pool(pool(), non_neg_integer(), non_neg_integer(), non_neg_integer(), pool_options()) -> ok.
init_pool(PoolName, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, Options) ->
    {ok, _} = worker_pool:start_sup_pool(?MASTER_POOL_NAME(PoolName),
        [{workers, MasterJobsNum}, {queue_type, lifo}]),
    {ok, _} = worker_pool:start_sup_pool(?SLAVE_POOL_NAME(PoolName),
        [{workers, SlaveJobsNum}, {queue_type, lifo}]),

    Executor = maps:get(executor, Options, ?DEFAULT_EXECUTOR_ID),
    CallbackModules = maps:get(callback_modules, Options, [binary_to_atom(PoolName, utf8)]),

    ok = traverse_load_balance:init_pool(PoolName, ParallelOrdersLimit),
    IdToCtx = repair_ongoing_tasks(PoolName, Executor),

    lists:foreach(fun(CallbackModule) ->
        {ok, JobIDs} = traverse_task_list:list_ongoing_jobs(PoolName, CallbackModule),

        lists:foreach(fun(JobID) ->
            {ok, Job, _, TaskID} = CallbackModule:get_job(JobID),
            case proplists:get_value(TaskID, IdToCtx, undefined) of
                undefined ->
                    ?warning("Job: ~p (id: ~p) of undefined (probably finished) task: ~p", [JobID, JobID, TaskID]),
                    ok;
                ExtendedCtx ->
                    {ok, _, _} = traverse_task:update_description(ExtendedCtx, PoolName, TaskID, #{
                        master_jobs_delegated => 1
                    }),
                    ok = run_on_master_pool(PoolName, ?MASTER_POOL_NAME(PoolName), ?SLAVE_POOL_NAME(PoolName),
                        CallbackModule, ExtendedCtx, Executor, TaskID, Job, JobID)
            end
        end, JobIDs)
    end, CallbackModules).

%%--------------------------------------------------------------------
%% @doc
%% Stops pool.
%% @end
%%--------------------------------------------------------------------
-spec stop_pool(pool()) -> ok.
stop_pool(PoolName) ->
    ok = worker_pool:stop_sup_pool(?MASTER_POOL_NAME(PoolName)),
    ok = worker_pool:stop_sup_pool(?SLAVE_POOL_NAME(PoolName)),

    case traverse_load_balance:clear_pool(PoolName) of
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
%% Initializes task.
%% @end
%%--------------------------------------------------------------------
-spec run(pool(), id(), job(), run_options()) -> ok.
run(PoolName, TaskID, Job, Options) ->
    Executor = maps:get(executor, Options, ?DEFAULT_EXECUTOR_ID),
    Creator = maps:get(creator, Options, Executor),
    CallbackModule = maps:get(callback_module, Options, binary_to_atom(PoolName, utf8)),
    TaskGroup = maps:get(group_id, Options, ?DEFAULT_GROUP),
    ExtendedCtx = get_extended_ctx(CallbackModule, Job),

    {JobStatus, Node, Description} = case Creator =:= Executor of
        true ->
            case traverse_load_balance:add_task(PoolName) of
                {ok, ChosenNode} ->
                    {on_pool, ChosenNode, #{master_jobs_delegated => 1}};
                {error, limit_exceeded} ->
                    {waiting, undefined, #{}}
            end;
        _ ->
            {waiting, undefined, #{}}
    end,

    {ok, JobID} = CallbackModule:update_job_progress(main_job, Job, PoolName, TaskID, JobStatus),
    ok = traverse_task:create(ExtendedCtx, PoolName, CallbackModule, TaskID, Creator, Executor,
        TaskGroup, JobID, Node, Description),

    case Node of
        undefined ->
            ok;
        _ ->
            ok = task_callback(CallbackModule, task_started, TaskID),
            ok = rpc:call(Node, ?MODULE, run_on_master_pool, [
                PoolName, ?MASTER_POOL_NAME(PoolName), ?SLAVE_POOL_NAME(PoolName),
                CallbackModule, ExtendedCtx, Executor, TaskID, Job, JobID])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if there is task to be executed ans starts it if possible.
%% @end
%%--------------------------------------------------------------------
-spec maybe_run_scheduled_task({task, task()} | {job, callback_module(), job_id()} |
    {job, job(), job_id(), pool(), id()}, executor()) -> ok.
maybe_run_scheduled_task({task, Task}, Self) ->
    case traverse_task:get_info(Task) of
        {ok, _, _, _, _, true} ->
            ok;
        {ok, _, _, _, false, _} ->
            ok;
        {ok, _, Executor, _, _, _} when Executor =/= Self ->
            ok;
        {ok, CallbackModule, Executor, MainJobID, _, _} ->
            case CallbackModule:get_job(MainJobID) of
                {ok, Job, PoolName, TaskID} ->
                    maybe_run_scheduled_task(PoolName, CallbackModule, TaskID, Executor, Job, MainJobID);
                {error, not_found} ->
                    ok
            end
    end;
maybe_run_scheduled_task({job, Job, JobID, PoolName, TaskID}, Self) ->
    case traverse_task:get_info(PoolName, TaskID) of
        {ok, _, _, _, _, true} ->
            ok;
        {ok, _, _, _, false, _} ->
            ok;
        {ok, _, Executor, _, _, _} when Executor =/= Self ->
            ok;
        {ok, CallbackModule, Executor, _, _, _} ->
            maybe_run_scheduled_task(PoolName, CallbackModule, TaskID, Executor, Job, JobID);
        {error, not_found} ->
            ok
    end;
maybe_run_scheduled_task({job, CallbackModule, JobID}, Self) ->
    {ok, Job, PoolName, TaskID} = CallbackModule:get_job(JobID),
    maybe_run_scheduled_task({job, Job, JobID, PoolName, TaskID}, Self).

%%--------------------------------------------------------------------
%% @doc
%% @equiv cancel(PoolName, TaskID, ?DEFAULT_EXECUTOR_ID).
%% @end
%%--------------------------------------------------------------------
-spec cancel(pool(), id()) -> ok | {error, term()}.
cancel(PoolName, TaskID) ->
    cancel(PoolName, TaskID, ?DEFAULT_EXECUTOR_ID).

%%--------------------------------------------------------------------
%% @doc
%% Cancels task.
%% @end
%%--------------------------------------------------------------------
-spec cancel(pool(), id(), executor()) -> ok | {error, term()}.
cancel(PoolName, TaskID, Self) ->
    case traverse_task:get_info(PoolName, TaskID) of
        {ok, _, _, _, _, true} ->
            ok;
        {ok, CallbackModule, _, MainJobID, _, _} ->
            {ok, Job, _, _} = CallbackModule:get_job(MainJobID),
            ExtendedCtx = get_extended_ctx(CallbackModule, Job),
            traverse_task:cancel(ExtendedCtx, PoolName, TaskID, Self);
        Other ->
            Other
    end.

%%%===================================================================
%%% Functions executed on pools
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Executes master job pool. To be executed inside pool.
%% @end
%%--------------------------------------------------------------------
-spec execute_master_job(pool(), execution_pool(), execution_pool(), callback_module(), ctx(), executor(),
    id(), job(), job_id()) -> ok.
execute_master_job(PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, Job, JobID) ->
    try
        % TODO - handle failures (jobow lub persystencji informacji co powinno zabic caly task)
        % TODO - przy startowaniu sprawdzamy czy task nie zostal anulowany
        % TODO - opcjonalny handler zakonczenia slave jobow? albo zwracanie z master_joba mapy w ktorej moze byc pole finish fun
        {ok, MasterAns} =  CallbackModule:do_master_job(Job),
        MasterJobsList = maps:get(master_jobs, MasterAns),
        SlaveJobsList = maps:get(slave_jobs, MasterAns),

        Description0 = maps:get(description, MasterAns, #{}),
        Description = Description0#{
            slave_jobs_delegated => length(SlaveJobsList),
            master_jobs_delegated => length(MasterJobsList)
        },
        {ok, _, Canceled} = UpdateAns = traverse_task:update_description(ExtendedCtx, PoolName, TaskID, Description),

        {_, NewDescription, Canceled2} = case Canceled of
            true ->
                ok = traverse_task_list:delete_job_link(PoolName, CallbackModule, JobID),
                {ok, _} = CallbackModule:update_job_progress(JobID, Job, PoolName, TaskID, canceled),
                UpdateAns;
            _ ->
                SlaveAnswers = run_on_slave_pool(PoolName, SlavePool, CallbackModule, ExtendedCtx, TaskID, SlaveJobsList),
                Fun = maps:get(finish_callback, MasterAns, fun() -> ok end),
                Fun(),
                ok = run_on_master_pool(PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, MasterJobsList),

                {SlavesOk, SlavesErrors} = lists:foldl(fun
                                                           ({ok, ok}, {OkSum, ErrorSum}) -> {OkSum + 1, ErrorSum};
                                                           (_, {OkSum, ErrorSum}) -> {OkSum, ErrorSum + 1}
                                                       end, {0, 0}, SlaveAnswers),

                ok = traverse_task_list:delete_job_link(PoolName, CallbackModule, JobID),
                {ok, _} = CallbackModule:update_job_progress(JobID, Job, PoolName, TaskID, ended),
                Description2 = #{
                    slave_jobs_done => SlavesOk,
                    slave_jobs_failed => SlavesErrors,
                    master_jobs_done => 1
                },
                {ok, _, _} = traverse_task:update_description(ExtendedCtx, PoolName, TaskID, Description2)
        end,

        try
            % TODO - wiele jobow moze wykonac finish z polem canceled - sprawdzic czy to ok
            maybe_finish(PoolName, CallbackModule, ExtendedCtx, TaskID, Executor, NewDescription, Canceled2)
        catch
            E3:E4 ->
                ?error_stacktrace("Checking finish of job ~p of task ~p (module ~p) error ~p:~p",
                    [Job, TaskID, CallbackModule, E3, E4])
        end
    catch
        E1:E2 ->
            ?error_stacktrace("Master job ~p of task ~p (module ~p) error ~p:~p",
                [Job, TaskID, CallbackModule, E1, E2]),
            ErrorDescription = #{
                master_jobs_failed => 1
            },
            catch traverse_task_list:delete_job_link(PoolName, CallbackModule, JobID),
            catch CallbackModule:update_job_progress(JobID, Job, PoolName, TaskID, failed),
            % TODO - zabezpieczyc jak sie wywali
            {ok, ErrorDescription2, Canceled3} = traverse_task:update_description(ExtendedCtx, PoolName, TaskID, ErrorDescription),

            try
                maybe_finish(PoolName, CallbackModule, ExtendedCtx, TaskID, Executor, ErrorDescription2, Canceled3)
            catch
                E5:E6 ->
                    ?error_stacktrace("Checking finish of job ~p of task ~p (module ~p) error ~p:~p",
                        [Job, TaskID, CallbackModule, E5, E6])
            end
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Executes master job pool. To be executed inside pool.
%% @end
%%--------------------------------------------------------------------
-spec execute_slave_job(pool(), callback_module(), ctx(), id(), job()) -> ok | error.
execute_slave_job(PoolName, CallbackModule, ExtendedCtx, TaskID, Job) ->
    try
        case CallbackModule:do_slave_job(Job) of
            ok ->
                ok;
            {ok, Description} ->
                {ok, _, _} = traverse_task:update_description(ExtendedCtx, PoolName, TaskID, Description),
                ok
        end
    catch
        E1:E2 ->
            ?error_stacktrace("Slave job ~p of task ~p (module ~p) error ~p:~p",
                [Job, TaskID, CallbackModule, E1, E2]),
            error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec run_on_slave_pool(pool(), execution_pool(), callback_module(), ctx(), id(), job()) -> [ok | error].
run_on_slave_pool(PoolName, SlavePool, CallbackModule, ExtendedCtx, TaskID, Jobs) ->
    utils:pmap(fun(Job) ->
        worker_pool:call(SlavePool, {?MODULE, execute_slave_job, [PoolName, CallbackModule, ExtendedCtx, TaskID, Job]},
            worker_pool:default_strategy(), ?CALL_TIMEOUT)
    end, Jobs).

-spec run_on_master_pool(pool(), execution_pool(), execution_pool(), callback_module(), ctx(), executor(),
    id(), [job() | {job(), job_id()}]) -> ok.
run_on_master_pool(PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, Jobs) ->
    lists:foreach(fun
        ({Job, JobID}) ->
            run_on_master_pool(PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, Job, JobID);
        (Job) ->
            run_on_master_pool(PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, Job, undefined)
    end, Jobs).

-spec run_on_master_pool(pool(), execution_pool(), execution_pool(), callback_module(), ctx(), executor(),
    id(), job(), job_id()) -> ok.
run_on_master_pool(PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, Job, JobID) ->
    {ok, JobID2} = CallbackModule:update_job_progress(JobID, Job, PoolName, TaskID, on_pool),
    ok = traverse_task_list:add_job_link(PoolName, CallbackModule, JobID2),
    ok = worker_pool:cast(MasterPool, {?MODULE, execute_master_job,
        [PoolName, MasterPool, SlavePool, CallbackModule, ExtendedCtx, Executor, TaskID, Job, JobID2]}).

-spec maybe_finish(pool(), callback_module(), ctx(), id(), executor(), description(), boolean()) -> ok.
maybe_finish(PoolName, CallbackModule, ExtendedCtx, TaskID, Executor, #{
    master_jobs_delegated := Delegated
} = Description, Canceled) ->
    Done = maps:get(master_jobs_done, Description, 0),
    Failed = maps:get(master_jobs_failed, Description, 0),
    case {Delegated == Done + Failed, Canceled} of
        {true, _} ->
            % Co jesli nigdy sie nie zgra (polecial blad na save'owaniu info o starcie slave'a, a slave sie nie uruchil)
            ok = task_callback(CallbackModule, task_finished, TaskID),
            ok = traverse_task:finish(ExtendedCtx, PoolName, TaskID, finished),
            check_task_list_and_run(PoolName, Executor);
        {_, true} ->
            ok = traverse_task:finish(ExtendedCtx, PoolName, TaskID, canceled),
            check_task_list_and_run(PoolName, Executor);
        _ -> ok
    end.

-spec check_task_list_and_run(pool(), executor()) -> ok.
check_task_list_and_run(PoolName, Executor) ->
    case traverse_load_balance:get_next_group(PoolName) of
        {error, no_groups} ->
            ok = traverse_load_balance:finish_task(PoolName);
        {ok, GroupID} ->
            case traverse_task_list:get_first_scheduled_link(PoolName, GroupID, Executor) of
                {ok, not_found} ->
                    case cancel_group(PoolName, GroupID, Executor) of
                        ok ->
                            check_task_list_and_run(PoolName, Executor);
                        {abort, TaskID} ->
                            ok = run_task(PoolName, TaskID, Executor)
                    end;
                {ok, TaskID} ->
                    ok = run_task(PoolName, TaskID, Executor)
            end
    end.

-spec run_task(pool(), id(), executor()) -> ok.
run_task(PoolName, TaskID, Executor) ->
    % TODO - zrobic tu case i obsluzyc error not found (nie zsyncowal sie dokument)
    % TODO - wywolac callback on_task_start
    % TODO - osluzyc sytuacje jak juz task zostal uruchomiony na innym node
    case traverse_task:get_info(PoolName, TaskID) of
        {ok, _, _, _, _, true} ->
            check_task_list_and_run(PoolName, Executor);
        {ok, CallbackModule, _, MainJobID, _, _} ->
            {ok, Job, _, _} = CallbackModule:get_job(MainJobID),
            ExtendedCtx = get_extended_ctx(CallbackModule, Job),
            case traverse_task:start(ExtendedCtx, PoolName, TaskID, #{master_jobs_delegated => 1}) of
                ok ->
                    ok = task_callback(CallbackModule, task_started, TaskID),
                    ok = run_on_master_pool(PoolName, ?MASTER_POOL_NAME(PoolName), ?SLAVE_POOL_NAME(PoolName),
                        CallbackModule, ExtendedCtx, Executor, TaskID, Job, MainJobID);
                {error, already_started} ->
                    check_task_list_and_run(PoolName, Executor);
                {error, not_found} ->
                    check_task_list_and_run(PoolName, Executor)
            end
    end.

-spec maybe_run_scheduled_task(pool(), callback_module(), id(), executor(), job(), job_id()) -> ok.
maybe_run_scheduled_task(PoolName, CallbackModule, TaskID, Executor, Job, MainJobID) ->
    case traverse_load_balance:add_task(PoolName) of
        {ok, Node} ->
            ExtendedCtx = get_extended_ctx(CallbackModule, Job),
            case traverse_task:start(ExtendedCtx, PoolName, TaskID, #{master_jobs_delegated => 1}) of
                ok ->
                    ok = task_callback(CallbackModule, task_started, TaskID),
                    ok = rpc:call(Node, ?MODULE, run_on_master_pool, [PoolName, ?MASTER_POOL_NAME(PoolName),
                        ?SLAVE_POOL_NAME(PoolName), CallbackModule, ExtendedCtx, Executor, TaskID, Job, MainJobID]);
                {error, already_started} ->
                    traverse_load_balance:finish_task(PoolName)
            end
    end.

-spec cancel_group(pool(), group(), executor()) -> ok| {abort, traverse:id()}.
cancel_group(PoolName, Group, Executor) ->
    case traverse_load_balance:cancel_group(PoolName, Group) of
        ok ->
            % TODO - sprawdzic race z dodawaniem
            case traverse_task_list:get_first_scheduled_link(PoolName, Group, Executor) of
                {ok, not_found} ->
                    ok;
                {ok, Task} ->
                    traverse_load_balance:register_group(PoolName, Group),
                    {abort, Task}
            end;
        {error, already_canceled} ->
            ok
    end.

-spec get_extended_ctx(callback_module(), job()) -> ctx().
get_extended_ctx(CallbackModule, Job) ->
    {ok, CtxExtension} = try
        CallbackModule:get_sync_info(Job)
    catch
        _:undef -> {ok, #{}}
    end,
    maps:merge(traverse_task:get_ctx(), CtxExtension).

-spec task_callback(callback_module(), task_started | task_finished, id()) -> ok.
task_callback(CallbackModule, Method, TaskID) ->
    try
        ok = CallbackModule:Method(TaskID)
    catch
        _:undef -> ok
    end.

-spec repair_ongoing_tasks(pool(), executor()) -> [{id(), ctx() | other_node}].
repair_ongoing_tasks(Pool, Executor) ->
    {ok, TaskIDs, _} = traverse_task_list:list(Pool, ongoing, #{tree_id => Executor}),

    lists:map(fun(ID) ->
        {ok, CallbackModule, Executor, MainJobID, _, _} = traverse_task:get_info(Pool, ID),
        {ok, Job, _, _} = CallbackModule:get_job(MainJobID),
        ExtendedCtx = get_extended_ctx(CallbackModule, Job),
        case traverse_task:fix_description(ExtendedCtx, Pool, ID) of
            {ok, _} -> {ID, ExtendedCtx};
            {error, other_node} -> {ID, other_node}
        end
    end, TaskIDs).