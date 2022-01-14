%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Gen_server handling requests connected with single partition of
%%% keys (see pes_process_manager.erl for more information about keys). Requests
%%% are handled in order of appearance and share state (state returned
%%% as result of one request handling if an argument to function handling
%%% next request). It can handle requests twofold depending on chosen mode
%%% (mode is chosen using pes_executor_behaviour:get_mode/0):
%%% - sync - handling requests by pes_server process, pes_server_slave
%%%          is not started,
%%% - async - delegating requests handling to slave process.
%%% Delegating request handling to slave process allows internal
%%% calls (calls from one pes process to another pes process - see pes.erl).
%%% In such a case pes_server works as a buffer for requests processed by
%%% slave process.
%%%
%%% TECHNICAL NOTE:
%%% For sync mode callbacks are executed by pes_server and pes_server caches
%%% state provided to callbacks. For async mode, all callbacks are executed
%%% by pes_server_slave and pes_server_slave caches state provided to them.
%%%
%%% When working in async mode, request handling is as follows:
%%% 1. Client process sends request to pes_server.
%%% 2. Pes_server caches request in its state.
%%%    2a. If request is PES_SUBMIT, pes_server responds to client with
%%%        tag that can be used to wait for async response.
%%% 3. Pes_server checks pes_server_slave state.
%%%    3a. If pes_server_slave is in active state, it waits until
%%%        pes_server_slave changes its state to idle sending
%%%        pes_slave_request_batch_processed.
%%%    3b. When pes_server_slave is in idle state, it sends all cached
%%%        requests to pes_server_slave and pes_server_slave changes
%%%        state to active.
%%% 4. Pes_server_slave processes each request.
%%%    4a. For requests that should be answered (PES_SUBMIT),
%%%        pes_server_slave sends results of request processing
%%%        directly to client process.
%%%
%%% pes_server can be terminated twofold:
%%% - handling termination_request,
%%% - by supervisor.
%%% termination_request can be sent to process externally or be result of
%%% internal pes_server action when no request appear for ?IDLE_TIMEOUT.
%%% Termination is preceded by execution of pes_executor_behaviour:terminate.
%%% This callback is executed by pes_server for sync mode and
%%% pes_server_slave for async mode. The callback can abort termination
%%% initiated by termination_request. Appearance of new request also
%%% aborts termination initiated by termination_request. Termination by
%%% supervisor cannot be aborted.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_server).
-author("Michal Wrzeszcz").


-include("pes_protocol.hrl").
-include_lib("ctool/include/logging.hrl").


-behaviour(gen_server).


%% API
-export([start_link/2, respond/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


-record(state, {
    executor :: pes:executor(),
    executor_state :: pes:executor_state() | undefined, % undefined for async mode. If mode is sync - server processes
                                                        % requests, for async mode - requests are processed by
                                                        % server_slave. Thus, mode implicates which process caches
                                                        % executor state.
    key_hash :: pes_process_manager:key_hash(),

    reversed_slave_request_list = [] :: [pes_server_slave:pes_slave_request()],
    slave_pid :: pid() | undefined, % undefined when working in sync mode (there is no slave for sync mode)
    slave_state = idle :: slave_state(),

    % Fields used to init termination after ?IDLE_TIMEOUT of inactivity
    terminate_msg_ref :: reference() | undefined,
    terminate_timer_ref :: reference() | undefined
}).


-type state() :: #state{}.
-type slave_state() :: idle | active.
-type slave_status_message() :: pes_slave_request_batch_processed | slave_ready_to_terminate | slave_termination_aborted.
-type management_message() :: {terminate, reference()} | {'EXIT', From :: pid(), Reason :: term()} |
    slave_status_message().
-type execution_callback() :: handle_call | handle_cast.

-export_type([execution_callback/0]).


-define(IDLE_TIMEOUT, timer:seconds(30)).


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pes:executor(), pes:key()) -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(Executor, Key) ->
    gen_server2:start_link(?MODULE, [Executor, Key], []).


%%--------------------------------------------------------------------
%% @doc
%% Allows sending response to caller process when request processing is delegated to slave process.
%% @end
%%--------------------------------------------------------------------
-spec respond(From :: {pid(), Tag :: term()}, RequestAns :: term()) -> ok.
respond({Caller, RequestTag} = _From, RequestAns) ->
    Caller ! {submit_result, RequestTag, RequestAns},
    ok.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(Args :: list()) -> {ok, state()} | {stop, Reason :: term()} | ignore.
init([Executor, KeyHash]) ->
    try
        case pes_process_manager:register_server(Executor, KeyHash) of
            ok ->
                pes_server_utils:mark_process_as_pes_server(),
                process_flag(trap_exit, true),
                Mode = pes_server_utils:call_optional_callback(
                    Executor, get_mode, [], fun() -> sync end, do_not_catch_errors),
                State = #state{
                    executor = Executor,
                    executor_state = maybe_init_executor_state(Executor, Mode),
                    key_hash = KeyHash,
                    slave_pid = maybe_init_slave(Executor, Mode)
                },
                pes_process_manager:report_server_initialized(Executor, KeyHash),
                {ok, schedule_terminate(State)};
            {error, already_exists} ->
                ignore
        end
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server init error ~p:~p for executor ~p and hash ~p",
                [Error, Reason, Executor, KeyHash], Stacktrace),
            pes_process_manager:deregister_server(Executor, KeyHash),
            {stop, Reason}
    end.


-spec handle_call(pes:call() | pes:termination_request(), From :: {pid(), Tag :: term()}, State :: state()) ->
    {reply, pes:execution_result(), NewState :: state()} | {noreply, NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call(?PES_CALL(Request), _From, #state{
    slave_pid = undefined,
    executor = Executor,
    executor_state = ExecutorState
} = State) ->
    try
        {RequestAns, UpdatedExecutorState} = Executor:handle_call(Request, ExecutorState),
        {reply, RequestAns, schedule_terminate(State#state{executor_state = UpdatedExecutorState})}
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server handle_call error ~p:~p for executor ~p and request ~p",
                [Error, Reason, Executor, Request], Stacktrace),
            {reply, {error, Reason}, schedule_terminate(State)}
    end;

handle_call(?PES_CALL(_), _From, State) ->
    {reply, {error, not_supported}, schedule_terminate(State)};

handle_call(?PES_SUBMIT(_), _From, #state{slave_pid = undefined} = State) ->
    {reply, {error, not_supported}, schedule_terminate(State)};

handle_call(?PES_SUBMIT(Request), {_Pid, Tag} = From, State) ->
    State2 = create_internal_slave_request(From, Request, handle_call, State),
    {reply, {await, Tag, self()}, schedule_terminate(send_requests_to_slave_if_idle(State2))};

handle_call(?PES_CHECK_CAST(_), _From, #state{slave_pid = undefined} = State) ->
    {reply, {error, not_supported}, schedule_terminate(State)};

handle_call(?PES_CHECK_CAST(Request), _From, State) ->
    State2 = create_internal_slave_request(undefined, Request, handle_cast, State),
    {reply, ok, schedule_terminate(send_requests_to_slave_if_idle(State2))};

handle_call(termination_request, From, State) ->
    gen_server2:reply(From, ok),
    handle_termination_request(State).


-spec handle_cast(pes:request(), State :: state()) -> {noreply, NewState :: state()}.
handle_cast(Request, State) ->
    handle_cast_internal(Request, State).


-spec handle_info(pes:request() | management_message(), State :: state()) ->
    {noreply, NewState :: state()} | {stop, Reason :: term(), NewState :: state()}.
handle_info(pes_slave_request_batch_processed, State) ->
    {noreply, send_requests_to_slave_if_idle(State#state{slave_state = idle})};

handle_info({terminate, MsgRef}, State = #state{
    terminate_msg_ref = MsgRef
}) ->
    handle_termination_request(schedule_terminate(State));

handle_info({terminate, _}, State = #state{}) ->
    {noreply, State};

handle_info(slave_ready_to_terminate, State = #state{reversed_slave_request_list = []}) ->
    {stop, {shutdown, termination_request}, State};

handle_info(slave_ready_to_terminate, State) ->
    {noreply, send_requests_to_slave_if_idle(State#state{slave_state = idle})};

handle_info(slave_termination_aborted, State) ->
    {noreply, send_requests_to_slave_if_idle(State#state{slave_state = idle})};

handle_info({'EXIT', _, Reason}, State = #state{}) ->
    {stop, Reason, State};

handle_info(Request, State) ->
    % self cast is sent as info (see pes:self_cast())
    handle_cast_internal(Request, State).


-spec terminate(Reason :: pes:termination_reason(), State :: state()) -> ok.
terminate({shutdown, termination_request}, #state{key_hash = KeyHash, executor = Executor}) ->
    % termination after handling termination_request - terminate callback has been called before
    pes_process_manager:deregister_server(Executor, KeyHash);
terminate(Reason, #state{key_hash = KeyHash, executor = Executor, executor_state = ExecutorState}) ->
    % termination as a result of supervisor request - terminate has not been called before
    pes_server_utils:mark_process_terminating(),
    pes_server_utils:call_optional_callback(Executor, terminate, [Reason, ExecutorState],
        fun(_, _) -> {ok, ExecutorState} end, execute_fallback_on_error),
    pes_process_manager:deregister_server(Executor, KeyHash).


-spec code_change(OldVsn :: term() | {down, term()}, State :: state(), Extra :: term()) ->
    {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(OldVsn, #state{
    slave_pid = undefined,
    executor = Executor,
    executor_state = ExecutorState
} = State, Extra) ->
    CallbackAns = pes_server_utils:call_optional_callback(Executor, code_change, [OldVsn, ExecutorState, Extra],
        fun(_, _, _) -> {ok, ExecutorState} end, execute_fallback_on_error),

    case CallbackAns of
        {ok, ExecutorState2} -> {ok, State#state{executor_state = ExecutorState2}};
        {error, Reason} -> {error, Reason}
    end;
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec maybe_init_executor_state(pes:executor(), pes:mode()) -> pes:executor_state() | undefined.
maybe_init_executor_state(Executor, sync) ->
    Executor:init();
maybe_init_executor_state(_Executor, async) ->
    undefined.


%% @private
-spec maybe_init_slave(pes:executor(), pes:mode()) -> pid() | undefined.
maybe_init_slave(_Executor, sync) ->
    undefined;
maybe_init_slave(Executor, async) ->
    {ok, Pid} = pes_server_slave:start_link(Executor, self()),
    Pid.


%% @private
-spec create_internal_slave_request(From :: {pid(), Tag :: term()} | undefined, pes:request(),
    pes_server:execution_callback(), state()) -> state().
create_internal_slave_request(From, Request, Callback, #state{reversed_slave_request_list = SlaveRequests} = State) ->
    InternalRequest = #pes_slave_request{from = From, request = Request, callback = Callback},
    State#state{reversed_slave_request_list = [InternalRequest | SlaveRequests]}.


%% @private
-spec send_requests_to_slave_if_idle(state()) -> state().
send_requests_to_slave_if_idle(State = #state{reversed_slave_request_list = []}) ->
    State;
send_requests_to_slave_if_idle(State = #state{slave_state = active}) ->
    State;
send_requests_to_slave_if_idle(State = #state{reversed_slave_request_list = Requests, slave_pid = Slave}) ->
    pes_server_slave:send_slave_request_batch(Slave, #pes_slave_request_batch{requests = lists:reverse(Requests)}),
    State#state{reversed_slave_request_list = [], slave_state = active}.


%% @private
-spec handle_cast_internal(pes:request(), state()) -> {noreply, state()}.
handle_cast_internal(Request, #state{
    slave_pid = undefined,
    executor = Executor,
    executor_state = ExecutorState
} = State) ->
    UpdatedExecutorState = try
        Executor:handle_cast(Request, ExecutorState)
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server handle_cast error ~p:~p for executor ~p and request ~p",
                [Error, Reason, Executor, Request], Stacktrace),
            ExecutorState
    end,
    {noreply, schedule_terminate(State#state{executor_state = UpdatedExecutorState})};

handle_cast_internal(Request, State) ->
    State2 = create_internal_slave_request(undefined, Request, handle_cast, State),
    {noreply, schedule_terminate(send_requests_to_slave_if_idle(State2))}.


%% @private
-spec handle_termination_request(state()) -> {stop, termination_request, state()} | {noreply, state()}.
handle_termination_request(#state{
    slave_pid = undefined,
    executor = Executor,
    executor_state = ExecutorState
} = State) ->
    case pes_server_utils:call_optional_callback(
        Executor, terminate, [termination_request, ExecutorState],
        fun(_, _) -> {ok, ExecutorState} end, catch_and_return_error
    ) of
        {ok, UpdatedExecutorState} ->
            {stop, {shutdown, termination_request}, State#state{executor_state = UpdatedExecutorState}};
        {abort, UpdatedExecutorState} ->
            {noreply, State#state{executor_state = UpdatedExecutorState}};
        {error, _} ->
            {noreply, State}
    end;
handle_termination_request(#state{
    slave_state = idle,
    reversed_slave_request_list = [],
    slave_pid = SlavePid
} = State) ->
    gen_server2:cast(SlavePid, termination_request),
    {noreply, State#state{slave_state = active}};
handle_termination_request(State) ->
    {noreply, State}.


%% @private
-spec schedule_terminate(state()) -> state().
schedule_terminate(State = #state{terminate_timer_ref = undefined}) ->
    MsgRef = make_ref(),
    Msg = {terminate, MsgRef},
    State#state{
        terminate_msg_ref = MsgRef,
        terminate_timer_ref = erlang:send_after(?IDLE_TIMEOUT, self(), Msg)
    };
schedule_terminate(State = #state{terminate_timer_ref = Ref}) ->
    erlang:cancel_timer(Ref),
    schedule_terminate(State#state{terminate_timer_ref = undefined}).