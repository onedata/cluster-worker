%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module coordinates tasks that needs special supervision.
%%% @end
%%% TODO - atomic update at persistent driver needed
%%%-------------------------------------------------------------------
-module(task_manager).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("elements/task_manager/task_manager.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("timeouts.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").

-type task() :: fun(() -> term()) | {fun((list()) -> term()), Args :: list()}
| {M :: atom(), F :: atom, Args :: list()} | atom(). % atom() for tests
-type level() :: ?NON_LEVEL | ?NODE_LEVEL | ?CLUSTER_LEVEL | ?PERSISTENT_LEVEL.
-type task_record() :: #task_pool{}.
-export_type([task/0, level/0]).

%% API
-export([start_task/2, start_task/3, check_and_rerun_all/0, kill_all/0, is_task_alive/1]).
-export([save_pid/3, update_pid/3]).

-define(TASK_REPEATS, 10).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts task.
%% @end
%%--------------------------------------------------------------------
-spec start_task(Task :: task() | #document{value :: #task_pool{}}, Level :: level()) -> ok.
start_task(Task, Level) ->
    start_task(Task, Level, false).

%%--------------------------------------------------------------------
%% @doc
%% Starts task.
%% @end
%%--------------------------------------------------------------------
-spec start_task(Task :: task() | #document{value :: #task_pool{}}, Level :: level(), DelaySave :: boolean()) -> ok.
start_task(Task, Level, DelaySave) ->
    start_task(Task, Level, save_pid, false, DelaySave).

%%--------------------------------------------------------------------
%% @doc
%% Starts task.
%% @end
%%--------------------------------------------------------------------
-spec start_task(Task :: task() | #document{value :: #task_pool{}}, Level :: level(),
    PersistFun :: save_pid | update_pid, Sleep :: boolean() | {boolean(), integer()}, DelaySave :: boolean()) -> ok.
start_task(Task, Level, PersistFun, Sleep, DelaySave) ->
    Pid = spawn(fun() ->
        receive
            {start, Uuid} ->
                case Sleep of
                    {true, N} ->
                        sleep_random_interval(N);
                    _ ->
                        ok
                end,
                case {do_task(Task, ?TASK_REPEATS), DelaySave} of
                    {ok, true} ->
                        ok;
                    {ok, _} ->
                        ok = delete_task(Uuid, Task, Level);
                    {task_failed, true} ->
                        {ok, _} = apply(?MODULE, save_pid, [Task, self(), Level]),
                        ?error_stacktrace("~p fails of a task ~p", [?TASK_REPEATS, Task]);
                    _ ->
                        ?error_stacktrace("~p fails of a task ~p", [?TASK_REPEATS, Task])
                end
        after
            ?TASK_SAVE_TIMEOUT ->
                ?error_stacktrace("Timeout for task ~p", [Task]),
                timeout
        end
    end),
    case DelaySave of
        true ->
            Pid ! {start, non};
        _ ->
            case apply(?MODULE, PersistFun, [Task, Pid, Level]) of
                {ok, Uuid} ->
                    Pid ! {start, Uuid};
                {error, owner_alive} ->
                    ok;
                Other ->
                    Other
            end
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Checks all tasks and reruns failed.
%% @end
%%--------------------------------------------------------------------
-spec check_and_rerun_all() -> ok.
check_and_rerun_all() ->
    check_and_rerun_all(?NODE_LEVEL),
    check_and_rerun_all(?CLUSTER_LEVEL).
% TODO - list at persistent driver needed
%%     check_and_rerun_all(?PERSISTENT_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% Checks if task is alive.
%% @end
%%--------------------------------------------------------------------
-spec is_task_alive(task_record()) -> boolean().
is_task_alive(Task) ->
    N = node(),
    case Task#task_pool.node of
        N ->
            is_process_alive(Task#task_pool.owner);
        OtherNode ->
            rpc:call(OtherNode, erlang, is_process_alive, [Task#task_pool.owner])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Kills all tasks.
%% @end
%%--------------------------------------------------------------------
-spec kill_all() -> ok.
kill_all() ->
    kill_all(?NODE_LEVEL),
    kill_all(?CLUSTER_LEVEL).
% TODO - list at persistent driver needed
%%     kill_all(?PERSISTENT_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% Saves information about the task.
%% @end
%%--------------------------------------------------------------------
-spec save_pid(Task :: task(), Pid :: pid(), Level :: level()) ->
    {ok, datastore:key()} | datastore:create_error().
save_pid(Task, Pid, Level) ->
    task_pool:create(Level, #document{value = #task_pool{task = Task, owner = Pid, node = node()}}).

%%--------------------------------------------------------------------
%% @doc
%% Updates information about the task.
%% @end
%%--------------------------------------------------------------------
-spec update_pid(Task :: #document{value :: #task_pool{}}, Pid :: pid(), Level :: level()) ->
    {ok, datastore:key()} | datastore:create_error().
update_pid(Task, Pid, Level) ->
    UpdateFun = fun(Record) ->
        case is_task_alive(Record) of
            false ->
                {ok, Record#task_pool{owner = Pid}};
            _ ->
                {error, owner_alive}
        end
    end,

    task_pool:update(Level, Task#document.key, UpdateFun).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes information about the task.
%% @end
%%--------------------------------------------------------------------
-spec delete_task(Uuid :: datastore:key(), Task :: task() | #document{value :: #task_pool{}},
    Level :: level()) -> ok.
delete_task(Uuid, Task, Level) ->
    case task_pool:delete(Level, Uuid) of
        ok ->
            ok;
        E ->
            ?error_stacktrace("Error ~p while deleting task ~p", [E, Task]),
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Executes task.
%% @end
%%--------------------------------------------------------------------
-spec do_task(Task :: task()) -> term().
do_task(Fun) when is_function(Fun) ->
    Fun();

do_task({Fun, Args}) when is_function(Fun) ->
    Fun(Args);

do_task({M, F, Args}) ->
    apply(M, F, Args);

do_task(Task) ->
    ?error_stacktrace("Not a task ~p", [Task]),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Executes task.
%% @end
%%--------------------------------------------------------------------
-spec do_task(Task :: task(), Repeats :: integer()) -> term().
do_task(Task, Num) when is_record(Task, document) ->
    V = Task#document.value,
    do_task(V#task_pool.task, Num);

do_task(_Task, 0) ->
    task_failed;

do_task(Task, Num) ->
    try
        ok = do_task(Task)
    catch
        E1:E2 ->
            ?error_stacktrace("Task ~p error: ~p:~p", [Task, E1, E2]),
            sleep_random_interval(?TASK_REPEATS - Num + 1),
            do_task(Task, Num - 1)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks tasks and reruns failed.
%% @end
%%--------------------------------------------------------------------
-spec check_and_rerun_all(Level :: level()) -> ok.
check_and_rerun_all(Level) ->
    {ok, Tasks} = task_pool:list_failed(Level),
    lists:foreach(fun(Task) ->
        start_task(Task, Level, update_pid, {true, min(20, length(Tasks))}, false)
    end, Tasks).

%%--------------------------------------------------------------------
%% @doc
%% Kills all tasks.
%% @end
%%--------------------------------------------------------------------
-spec kill_all(Level :: level()) -> ok.
kill_all(Level) ->
    {ok, Tasks} = task_pool:list(Level),
    lists:foreach(fun(Task) ->
        task_pool:delete(Level, Task#document.key),
        Value = Task#document.value,
        exit(Value#task_pool.owner, stopped_by_manager)
    end, Tasks).

%%--------------------------------------------------------------------
%% @doc
%% Sleeps random interval specified in task_fail_min_sleep_time_ms and task_fail_max_sleep_time_ms
%% variables.
%% @end
%%--------------------------------------------------------------------
-spec sleep_random_interval(Num :: integer()) -> ok.
sleep_random_interval(Num) ->
    {ok, Interval1} = application:get_env(?CLUSTER_WORKER_APP_NAME, task_fail_min_sleep_time_ms),
    {ok, Interval2} = application:get_env(?CLUSTER_WORKER_APP_NAME, task_fail_max_sleep_time_ms),
    timer:sleep(Num * crypto:rand_uniform(Interval1, Interval2 + 1)).