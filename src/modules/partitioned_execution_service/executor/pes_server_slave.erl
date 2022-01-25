%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Slave process for pes_server. Handles batches of slave tasks
%%% (they are used when working in async mode - see pes_server.erl).
%%% @end
%%%-------------------------------------------------------------------
-module(pes_server_slave).
-author("Michal Wrzeszcz").


-behaviour(gen_server).


-include("pes_protocol.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([start_link/2, send_slave_task_batch/2, send_graceful_termination_request/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


-record(state, {
    plugin :: pes:plugin(),
    executor_state :: pes:executor_state(),

    master_pid :: pid()
}).


-type state() :: #state{}.
-type pes_slave_task() :: #pes_slave_task{}.
-type pes_slave_task_batch() :: #pes_slave_task_batch{}.

-export_type([pes_slave_task/0]).


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pes:plugin(), pid()) -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(Plugin, MasterPid) ->
    gen_server2:start_link(?MODULE, [Plugin, MasterPid], []).


-spec send_slave_task_batch(pid(), pes_slave_task_batch()) -> ok.
send_slave_task_batch(Slave, Batch) ->
    gen_server2:cast(Slave, Batch).


-spec send_graceful_termination_request(pid()) -> ok.
send_graceful_termination_request(Slave) ->
    gen_server2:cast(Slave, graceful_termination_request).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(Args :: list()) -> {ok, state()} | {stop, Reason :: term()}.
init([Plugin, MasterPid]) ->
    try
        pes_process_identity:mark_as_pes_server_slave(),
        process_flag(trap_exit, true),
        {ok, #state{
            plugin = Plugin,
            executor_state = pes_plugin:init(Plugin),
            master_pid = MasterPid
        }}
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server slave init error ~p:~p for plug-in ~p",
                [Error, Reason, Plugin], Stacktrace),
            {stop, Reason}
    end.


-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: state()) ->
    {noreply, NewState :: state()}.
handle_call(Request, _From, State) ->
    % Slave process should not be called to prevent deadlocks
    ?log_bad_request(Request),
    {noreply, State}.


-spec handle_cast(pes_slave_task_batch() | pes:graceful_termination_request(), State :: state()) ->
    {noreply, NewState :: state()}.
handle_cast(graceful_termination_request, #state{
    plugin = Plugin,
    executor_state = ExecutorState,
    master_pid = MasterPid
} = State) ->
    case pes_plugin:graceful_terminate(Plugin, ExecutorState) of
        {ok, UpdatedExecutorState} ->
            MasterPid ! slave_ready_to_terminate,
            {noreply, State#state{executor_state = UpdatedExecutorState}};
        {defer, UpdatedExecutorState} ->
            MasterPid ! slave_termination_deferred,
            {noreply, State#state{executor_state = UpdatedExecutorState}};
        ?ERROR_INTERNAL_SERVER_ERROR ->
            MasterPid ! slave_termination_deferred,
            {noreply, State}
    end;

handle_cast(#pes_slave_task_batch{tasks = Tasks},
    #state{plugin = Plugin, executor_state = ExecutorState, master_pid = MasterPid} = State) ->
    UpdatedExecutorState = lists:foldl(fun(Task, ExecutorStateAcc) ->
        process_slave_task(Task, Plugin, ExecutorStateAcc)
    end, ExecutorState, Tasks),
    MasterPid ! pes_slave_task_batch_processed,
    {noreply, State#state{executor_state = UpdatedExecutorState}};

handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


-spec handle_info(pes:request() | {'EXIT', From :: pid(), Reason :: term()}, State :: state()) ->
    {noreply, NewState :: state()}.
handle_info({'EXIT', MasterPid, Reason}, State = #state{master_pid = MasterPid}) ->
    {stop, Reason, State};

handle_info(?PES_SELF_CAST(Request), #state{plugin = Plugin, executor_state = ExecutorState} = State) ->
    % self cast is sent as info (see pes:self_cast())
    UpdatedExecutorState = process_cast_request(Plugin, Request, ExecutorState),
    {noreply, State#state{executor_state = UpdatedExecutorState}};

handle_info(?SUBMIT_RESULT(_, _) = SubmitResult, #state{plugin = Plugin, executor_state = ExecutorState} = State) ->
    UpdatedExecutorState = process_cast_request(Plugin, SubmitResult, ExecutorState),
    {noreply, State#state{executor_state = UpdatedExecutorState}};

handle_info(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


-spec terminate(Reason :: pes:forced_termination_reason(), State :: state()) -> ok.
terminate({shutdown, graceful_terminate}, _State) ->
    ok; % termination after handling graceful_termination_request - executor's graceful_terminate callback
        % has already been called

terminate(Reason, #state{plugin = Plugin, executor_state = ExecutorState}) ->
    % termination as a result of supervisor request
    pes_process_identity:mark_as_terminating_pes_process(),
    pes_plugin:forced_terminate(Plugin, Reason, ExecutorState).


-spec code_change(OldVsn :: term() | {down, term()}, State :: state(), Extra :: term()) ->
    {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(OldVsn, #state{plugin = Plugin, executor_state = ExecutorState} = State, Extra) ->
    case pes_plugin:code_change(Plugin, OldVsn, ExecutorState, Extra) of
        {ok, ExecutorState2} -> {ok, State#state{executor_state = ExecutorState2}};
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec process_slave_task(pes_slave_task(), pes:plugin(), pes:executor_state()) -> pes:executor_state().
process_slave_task(#pes_slave_task{request = Request, callback = handle_call, from = From},
    Plugin, ExecutorState) ->
    try
        {RequestAns, UpdatedExecutorState} = pes_plugin:handle_call(Plugin, Request, ExecutorState),
        send_submit_result(From, RequestAns),
        UpdatedExecutorState
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server slave handle_call error ~p:~p for plug-in ~p and request ~p",
                [Error, Reason, Plugin, Request], Stacktrace),
            send_submit_result(From, ?ERROR_INTERNAL_SERVER_ERROR),
            ExecutorState
    end;

process_slave_task(#pes_slave_task{request = Request}, Plugin, ExecutorStateAcc) ->
    process_cast_request(Plugin, Request, ExecutorStateAcc).


-spec process_cast_request(pes:plugin(), pes:request(), state()) ->
    state().
process_cast_request(Plugin, Request, ExecutorState) ->
    try
        pes_plugin:handle_cast(Plugin, Request, ExecutorState)
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server slave handle_cast error ~p:~p for plug-in ~p and request ~p",
                [Error, Reason, Plugin, Request], Stacktrace),
            ExecutorState
    end.


-spec send_submit_result(From :: {pid(), Tag :: term()}, SubmitResult :: pes:execution_result()) -> ok.
send_submit_result({Caller, RequestTag} = _From, SubmitResult) ->
    Caller ! ?SUBMIT_RESULT(RequestTag, SubmitResult),
    ok.