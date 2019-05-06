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
-export([init_pool/4, run/5]).
%% Functions executed on pools
-export([execute_master_job/5, execute_slave_job/3]).

-type id() :: traverse_task:key().
-type group() :: traverse_task:key().
-type job() :: term().
-type pool() :: atom().
-type task_module() :: atom().
-type description() :: map().
-type status() :: atom().
-type job_status() :: start | finish.

-export_type([id/0, job/0, pool/0, task_module/0, description/0, status/0, job_status/0]).

-define(MASTER_POOL_NAME(Pool), list_to_atom(atom_to_list(Pool) ++ "_master")).
-define(SLAVE_POOL_NAME(Pool), list_to_atom(atom_to_list(Pool) ++ "_slave")).
-define(CALL_TIMEOUT, timer:hours(24)).

% co jesli plik zostanie zrename'owany albo co gorsza katalog przesuniety na przerobiony juz poziom drzewa
% musi byc load balancing puli danego typu ze jeden space nie zagladza drugiego
% mozna pomyslec o tym zeby robic load balancing per user

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes pool.
%% @end
%%--------------------------------------------------------------------
-spec init_pool(pool(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
    ok | no_return().
init_pool(Name, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) ->
    {ok, _} = worker_pool:start_sup_pool(?MASTER_POOL_NAME(Name),
        [{workers, MasterJobsNum}, {queue_type, lifo}]),
    {ok, _} = worker_pool:start_sup_pool(?SLAVE_POOL_NAME(Name),
        [{workers, SlaveJobsNum}, {queue_type, lifo}]),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Initializes pool.
%% @end
%%--------------------------------------------------------------------
-spec run(pool(), task_module(), id(), group(), job()) ->
    ok | no_return().
run(Pool, TaskModule, TaskID, TaskGroup, Job) ->
    ok = TaskModule:save_job(Job, start),
    ok = add_task(Pool, TaskModule, TaskID),
    check_task_list_and_run(),
    % job moze zostac wrzucony do kolejki kontrolera
    % kontroler opakowuje joby, aby wiedziec kiedy sie zakonczyl caly task
    ok = run_on_master_pool(?MASTER_POOL_NAME(Pool), ?SLAVE_POOL_NAME(Pool),
        TaskModule, TaskID, [Job]).

%%%===================================================================
%%% Functions executed on pools
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Executes master job pool. To be executed inside pool.
%% @end
%%--------------------------------------------------------------------
-spec execute_master_job(pool(), pool(), task_module(), id(), job()) -> ok.
execute_master_job(MasterPool, SlavePool, TaskModule, TaskID, Job) ->
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
        {ok, _} = traverse_task:update_description(TaskID, Description),

        SlaveAnswers = run_on_slave_pool(SlavePool, TaskModule, TaskID, SlaveJobsList),
        ok = run_on_master_pool(MasterPool, SlavePool, TaskModule, TaskID, MasterJobsList),

        lists:foldl(fun
            (ok, {OkSum, ErrorSum}) -> {OkSum + 1, ErrorSum};
            (_, {OkSum, ErrorSum}) -> {OkSum, ErrorSum + 1}
        end, {0, 0}, SlaveAnswers),

        ok = TaskModule:save_job(Job, finish),
        Description2 = #{
            slave_jobs_done => length(SlaveJobsList),
            master_jobs_done => 1
        },
        {ok, NewDescription} = traverse_task:update_description(TaskID, Description2),

        maybe_execute_finish_callback(TaskModule, TaskID, NewDescription)
    catch
        E1:E2 ->
            ?error_stacktrace("Master job ~p of task ~p (module ~p) error ~p:~p",
                [Job, TaskID, TaskModule, E1, E2]),
            ErrorDescription = #{
                master_jobs_failed => 1
            },
            {ok, _} = traverse_task:update_description(TaskID, ErrorDescription)
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
                {ok, _} = traverse_task:update_description(TaskID, Description),
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

-spec run_on_master_pool(pool(), pool(), task_module(), id(), job()) -> ok | no_return().
run_on_master_pool(MasterPool, SlavePool, TaskModule, TaskID, Jobs) ->
    lists:foreach(fun(Job) ->
        ok = TaskModule:save_job(Job, start),
        ok = worker_pool:cast(MasterPool, {?MODULE, execute_master_job,
            [MasterPool, SlavePool, TaskModule, TaskID, Job]})
    end, Jobs).

-spec maybe_execute_finish_callback(task_module(), id(), description()) -> ok | no_return().
maybe_execute_finish_callback(TaskModule, TaskID, #{
    master_jobs_delegated := Delegated,
    master_jobs_done := Done,
    master_jobs_failed := Failed
}) ->
    case Delegated == Done + Failed of
        true ->
            % Co jesli nigdy sie nie zgra (polecial blad na save'owaniu info o starcie slave'a, a slave sie nie uruchil)
            ok = TaskModule:task_finished(TaskID),
            check_task_list_and_run();
        _ -> ok
    end.

-spec add_task(pool(), task_module(), id()) -> ok | no_return().
add_task(Pool, TaskModule, TaskID) ->
    traverse_task:create(TaskID, Pool, TaskModule).

check_task_list_and_run() ->
    % Tutaj load balancing per user/space cokolwiek
    ok.