%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Slave process for pes_server. Handles batches of slave requests
%%% (they are used when working in async mode - see pes_server.erl).
%%%
%%% NOTE: It is possible to call other pes_server from the inside of
%%% pes_server_slave. In such a case answer for call appears as slave
%%% internal message and should be handled by handle_cast callback.
%%% Synchronous waiting for massage could results in deadlock.
%%% Thus, pes:wait/1 returns error in such a case.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_server_slave).
-author("Michal Wrzeszcz").


-behaviour(gen_server).


-include("pes_protocol.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([start_link/2, send_slave_request_batch/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


-record(state, {
    executor :: pes:executor(),
    executor_state :: pes:executor_state(),

    master_pid :: pid()
}).


-type state() :: #state{}.
-type pes_slave_request() :: #pes_slave_request{}.
-type pes_slave_request_batch() :: #pes_slave_request_batch{}.

-export_type([pes_slave_request/0]).


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pes:executor(), pid()) -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(Executor, MasterPid) ->
    gen_server2:start_link(?MODULE, [Executor, MasterPid], []).


-spec send_slave_request_batch(pid(), pes_slave_request_batch()) -> ok.
send_slave_request_batch(Slave, Batch) ->
    gen_server2:cast(Slave, Batch).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(Args :: list()) -> {ok, state()} | {stop, Reason :: term()}.
init([Executor, MasterPid]) ->
    try
        pes_server_utils:mark_process_as_pes_server_slave(),
        process_flag(trap_exit, true),
        {ok, #state{
            executor = Executor,
            executor_state = Executor:init(),
            master_pid = MasterPid
        }}
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server slave init error ~p:~p for executor ~p",
                [Error, Reason, Executor], Stacktrace),
            {stop, Reason}
    end.


-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: state()) ->
    {noreply, NewState :: state()}.
handle_call(Request, _From, State) ->
    % Slave process should not be called to prevent deadlocks
    ?log_bad_request(Request),
    {noreply, State}.


-spec handle_cast(pes_slave_request_batch() | pes:termination_request(), State :: state()) ->
    {noreply, NewState :: state()}.
handle_cast(termination_request, #state{
    executor = Executor,
    executor_state = ExecutorState,
    master_pid = MasterPid
} = State) ->
    case pes_server_utils:call_optional_callback(
        Executor, terminate, [termination_request, ExecutorState],
        fun(_, _) -> {ok, ExecutorState} end, catch_and_return_error
    ) of
        {ok, UpdatedExecutorState} ->
            MasterPid ! slave_ready_to_terminate,
            {noreply, State#state{executor_state = UpdatedExecutorState}};
        {abort, UpdatedExecutorState} ->
            MasterPid ! slave_termination_aborted,
            {noreply, State#state{executor_state = UpdatedExecutorState}};
        {error, _} ->
            MasterPid ! slave_termination_aborted,
            {noreply, State}
    end;

handle_cast(#pes_slave_request_batch{requests = Requests},
    #state{executor = Executor, executor_state = ExecutorState, master_pid = Master} = State) ->
    UpdatedExecutorState = lists:foldl(fun(Request, ExecutorStateAcc) ->
        handle_slave_request(Request, Executor, ExecutorStateAcc)
    end, ExecutorState, Requests),
    Master ! pes_slave_request_batch_processed,
    {noreply, State#state{executor_state = UpdatedExecutorState}};
handle_cast(Request, State) ->
    % This request should not appear - log it
    ?log_bad_request(Request),
    {noreply, State}.


-spec handle_info(pes:request() | {'EXIT', From :: pid(), Reason :: term()}, State :: state()) ->
    {noreply, NewState :: state()}.
handle_info({'EXIT', MasterPid, Reason}, State = #state{master_pid = MasterPid}) ->
    {stop, Reason, State};
handle_info(Request, #state{executor = Executor, executor_state = ExecutorState} = State) ->
    % self cast is sent as info (see pes:self_cast())
    UpdatedExecutorState = handle_async_request(Executor, Request, ExecutorState),
    {noreply, State#state{executor_state = UpdatedExecutorState}}.


-spec terminate(Reason :: pes:termination_reason(), State :: state()) -> ok.
terminate({shutdown, termination_request}, _State) ->
    ok; % termination after handling termination_request - terminate callback has been called before - do nothing
terminate(Reason, #state{executor = Executor, executor_state = ExecutorState}) ->
    % termination as a result of supervisor request - terminate has not been called before
    pes_server_utils:mark_process_terminating(),
    pes_server_utils:call_optional_callback(Executor, terminate, [Reason, ExecutorState],
        fun(_, _) -> {ok, ExecutorState} end, execute_fallback_on_error).


-spec code_change(OldVsn :: term() | {down, term()}, State :: state(), Extra :: term()) ->
    {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(OldVsn, #state{executor = Executor, executor_state = ExecutorState} = State, Extra) ->
    CallbackAns = pes_server_utils:call_optional_callback(Executor, code_change, [OldVsn, ExecutorState, Extra],
        fun(_, _, _) -> {ok, ExecutorState} end, execute_fallback_on_error),

    case CallbackAns of
        {ok, ExecutorState2} -> {ok, State#state{executor_state = ExecutorState2}};
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec handle_slave_request(pes_slave_request(), pes:executor(), pes:executor_state()) -> pes:executor_state().
handle_slave_request(#pes_slave_request{request = Request, callback = handle_call, from = From},
    Executor, ExecutorState) ->
    try
        {RequestAns, UpdatedExecutorState} = Executor:handle_call(Request, ExecutorState),
        pes_server:respond(From, RequestAns),
        UpdatedExecutorState
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server slave handle_call error ~p:~p for executor ~p and request ~p",
                [Error, Reason, Executor, Request], Stacktrace),
            pes_server:respond(From, {error, Reason}),
            ExecutorState
    end;
handle_slave_request(#pes_slave_request{request = Request}, Executor, ExecutorStateAcc) ->
    handle_async_request(Executor, Request, ExecutorStateAcc).


-spec handle_async_request(pes:executor(), pes:request(), state()) ->
    state().
handle_async_request(Executor, Request, ExecutorState) ->
    try
        Executor:handle_cast(Request, ExecutorState)
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server slave handle_cast error ~p:~p for executor ~p and request ~p",
                [Error, Reason, Executor, Request], Stacktrace),
            ExecutorState
    end.