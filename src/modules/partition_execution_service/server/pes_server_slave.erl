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
%%% NOTE: It is possible to call other pes process from the inside of
%%% pes_server_slave. In such a case answer for call appears as slave
%%% internal message and should by handled by handle_info callback.
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
    callback_module :: pes:callback_module(),
    callback_state :: pes:callback_state(),

    master_pid :: pid()
}).


-type state() :: #state{}.
-type pes_slave_request() :: #pes_slave_request{}.
-type pes_slave_request_batch() :: #pes_slave_request_batch{}.

-export_type([pes_slave_request/0]).


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pes:callback_module(), pid()) -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(CallbackModule, MasterPid) ->
    gen_server:start_link(?MODULE, [CallbackModule, MasterPid], []).


-spec send_slave_request_batch(pid(), pes_slave_request_batch()) -> ok.
send_slave_request_batch(Slave, Batch) ->
    gen_server:cast(Slave, Batch).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(Args :: list()) -> {ok, state()} | {stop, Reason :: term()}.
init([CallbackModule, MasterPid]) ->
    try
        pes_server_utils:mark_process_as_pes_server_slave(),
        process_flag(trap_exit, true),
        {ok, #state{
            callback_module = CallbackModule,
            callback_state = CallbackModule:init(),
            master_pid = MasterPid
        }}
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server slave init error ~p:~p for callback module ~p",
                [Error, Reason, CallbackModule], Stacktrace),
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
    callback_module = CallbackModule,
    callback_state = CallbackState,
    master_pid = MasterPid
} = State) ->
    case pes_server_utils:call_optional_callback(
        CallbackModule, terminate, [termination_request, CallbackState],
        fun(_, _) -> {ok, CallbackState} end, catch_and_return_error
    ) of
        {ok, UpdatedCallbackState} ->
            MasterPid ! slave_ready_to_terminate,
            {noreply, State#state{callback_state = UpdatedCallbackState}};
        {abort, UpdatedCallbackState} ->
            MasterPid ! slave_termination_aborted,
            {noreply, State#state{callback_state = UpdatedCallbackState}};
        {error, _} ->
            MasterPid ! slave_termination_aborted,
            {noreply, State}
    end;

handle_cast(#pes_slave_request_batch{requests = Requests},
    #state{callback_module = CallbackModule, callback_state = CallbackState, master_pid = Master} = State) ->
    FinalCallbackState = lists:foldl(fun
        (#pes_slave_request{request = Request, handler = handle_call, from = From},
            CallbackStateAcc) ->
            try
                {RequestAns, UpdatedCallbackState} = CallbackModule:handle_call(Request, CallbackStateAcc),
                pes_server:respond(From, RequestAns),
                UpdatedCallbackState
            catch
                Error:Reason:Stacktrace ->
                    ?error_stacktrace("PES server slave handle_call error ~p:~p for callback module ~p and request ~p",
                        [Error, Reason, CallbackModule, Request], Stacktrace),
                    pes_server:respond(From, {error, Reason}),
                    CallbackStateAcc
            end;
        (#pes_slave_request{request = Request, handler = Handler}, CallbackStateAcc) ->
            handle_async_request(CallbackModule, Request, Handler, CallbackStateAcc)
        end, CallbackState, Requests
    ),
    Master ! pes_slave_request_batch_processed,
    {noreply, State#state{callback_state = FinalCallbackState}};
handle_cast(Request, State) ->
    % This request should not appear - log it
    ?log_bad_request(Request),
    {noreply, State}.


-spec handle_info(pes:request() | {'EXIT', From :: pid(), Reason :: term()}, State :: state()) ->
    {noreply, NewState :: state()}.
handle_info({'EXIT', MasterPid, Reason}, State = #state{master_pid = MasterPid}) ->
    {stop, Reason, State};
handle_info(Request, #state{callback_module = CallbackModule, callback_state = CallbackState} = State) ->
    UpdatedCallbackState = handle_async_request(CallbackModule, Request, handle_info, CallbackState),
    {noreply, State#state{callback_state = UpdatedCallbackState}}.


-spec terminate(Reason :: pes:termination_reason(), State :: state()) -> ok.
terminate({shutdown, termination_request}, _State) ->
    ok; % termination after handling termination_request - terminate callback has been called before - do nothing
terminate(Reason, #state{callback_module = CallbackModule, callback_state = CallbackState}) ->
    % termination as a result of supervisor request - terminate has not been called before
    pes_server_utils:mark_process_is_terminating(),
    pes_server_utils:call_optional_callback(CallbackModule, terminate, [Reason, CallbackState],
        fun(_, _) -> {ok, CallbackState} end, execute_fallback_on_error).


-spec code_change(OldVsn :: term() | {down, term()}, State :: state(), Extra :: term()) ->
    {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(OldVsn, #state{callback_module = CallbackModule, callback_state = CallbackState} = State, Extra) ->
    CallbackAns = pes_server_utils:call_optional_callback(CallbackModule, code_change, [OldVsn, CallbackState, Extra],
        fun(_, _, _) -> {ok, CallbackState} end, execute_fallback_on_error),

    case CallbackAns of
        {ok, CallbackState2} -> {ok, State#state{callback_state = CallbackState2}};
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec handle_async_request(pes:callback_module(), pes:request(), pes_server:request_handler(), state()) ->
    state().
handle_async_request(CallbackModule, Request, Handler, CallbackState) ->
    try
        CallbackModule:Handler(Request, CallbackState)
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server slave handler: ~p error ~p:~p for callback module ~p and request ~p",
                [Handler, Error, Reason, CallbackModule, Request], Stacktrace),
            CallbackState
    end.