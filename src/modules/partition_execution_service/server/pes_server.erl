%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Gen_server handling requests connected with single batch of
%%% keys (see pes_router.erl for more information about keys). Requests
%%% are handled in order of appearance and share state (state returned
%%% as result of one request handling if an argument to function handling
%%% next request). It can handle requests twofold depending on chosen mode
%%% (mode us chosen using pes_callback:get_mode/0):
%%% - sync - handling requests by pes_server process, pes_server_slave
%%%          is not started,,
%%% - async - delegating requests handling to slave process.
%%% Delegating request handling to slave process allows for internal
%%% calls (calls from one pes process to another pes process - see pes.erl).
%%% In such a case pes_server works as a buffer for requests processed by
%%% slave process.
%%%
%%% TECHNICAL NOTE:
%%% For sync mode callbacks are executed by pes_server and pes_server caches
%%% state provided to callbacks. For async mode, all callbacks are executed
%%% by pes_server_state and pes_server_state stores state provided to them.
%%%
%%% When working in async mode, request handling is as follows:
%%% 1. Client process sends request to pes_server.
%%% 2. Pes_server caches request in its state.
%%%    2a. If request is async call, pes_server responds to client with
%%%        tag that can be used to wait for async response.
%%% 3. Pes_server checks pes_server_slave state.
%%%    3a. If pes_server_slave is in active state, it waits until
%%%        pes_server_slave changes its state to idle sending
%%%        pes_slave_request_batch_processed.
%%%    3b. When pes_server_slave is in idle state, it sends all cached
%%%        requests to pes_server_slave and pes_server_slave changes
%%%        state to active.
%%% 4. Pes_server_slave processes each request.
%%%    4a. For requests that should be answered (async calls),
%%%        pes_server_slave sends results of request processing
%%%        directly to client process.
%%%
%%% Pes_server can be terminated twofold:
%%% - handling termination_request,
%%% - by supervisor.
%%% termination_request can be sent to process externally or be result of
%%% internal pes_server action when no request appear for ?IDLE_TIMEOUT.
%%% Termination is preceded by execution of pes_callback:terminate.
%%% This callback is executed by pes_server for sync mode and
%%% pes_server_slave for async mode. The callback can abort termination
%%% initiated by termination_request. Appearance of new request also
%%% abort termination initiated by termination_request. Termination by
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
    callback_module :: pes:callback_module(),
    callback_state :: pes:callback_state() | undefined, % undefined for async mode. If mode is sync - server processes
                                                        % requests, for async mode - requests are processed by
                                                        % server_slave. Thus, mode implicates which process stores
                                                        % callback state.
    key_hash :: pes:key_hash(),

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
-type request_handler() :: handle_call | handle_cast | handle_info.

-export_type([request_handler/0]).


-define(IDLE_TIMEOUT, timer:seconds(30)).


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pes:callback_module(), pes:key()) -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(CallbackModule, Key) ->
    gen_server:start_link(?MODULE, [CallbackModule, Key], []).


%%--------------------------------------------------------------------
%% @doc
%% Allows sending respond to caller process when request processing is delegated to slave process.
%% Some callers ignore answers. In such a case, `From` is undefined.
%% @end
%%--------------------------------------------------------------------
-spec respond(From :: {pid(), Tag :: term()} | undefined, RequestAns :: term()) -> ok.
respond(undefined = _From, _RequestAns) ->
    ok;
respond({Caller, RequestTag} = _From, RequestAns) ->
    Caller ! {RequestTag, RequestAns},
    ok.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(Args :: list()) -> {ok, state()} | {stop, Reason :: term()} | ignore.
init([CallbackModule, KeyHash]) ->
    try
        case pes_router:register(CallbackModule, KeyHash) of
            ok ->
                pes_server_utils:mark_process_as_pes_server(),
                process_flag(trap_exit, true),
                Mode = pes_server_utils:call_optional_callback(
                    CallbackModule, get_mode, [], fun() -> sync end, do_not_catch_errors),
                State = #state{
                    callback_module = CallbackModule,
                    callback_state = maybe_init_callback_state(CallbackModule, Mode),
                    key_hash = KeyHash,
                    slave_pid = maybe_init_slave(CallbackModule, Mode)
                },
                pes_router:report_process_initialized(CallbackModule, KeyHash),
                {ok, schedule_terminate(State)};
            {error, already_exists} ->
                ignore
        end
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server init error ~p:~p for callback module ~p and hash ~p",
                [Error, Reason, CallbackModule, KeyHash], Stacktrace),
            pes_router:deregister(CallbackModule, KeyHash),
            {stop, Reason}
    end.


-spec handle_call(pes:call() | pes:termination_request(), From :: {pid(), Tag :: term()}, State :: state()) ->
    {reply, pes:response(), NewState :: state()} | {noreply, NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call(?PES_SYNC_CALL(Request), _From, #state{
    slave_pid = undefined,
    callback_module = CallbackModule,
    callback_state = CallbackState
} = State) ->
    try
        case CallbackModule:handle_call(Request, CallbackState) of
            {noreply, UpdatedCallbackState} ->
                {noreply, schedule_terminate(State#state{callback_state = UpdatedCallbackState})};
            {RequestAns, UpdatedCallbackState} ->
                {reply, RequestAns, schedule_terminate(State#state{callback_state = UpdatedCallbackState})}
        end
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server handle_call error ~p:~p for callback module ~p and request ~p",
                [Error, Reason, CallbackModule, Request], Stacktrace),
            {reply, {error, Reason}, schedule_terminate(State)}
    end;

handle_call(?PES_SYNC_CALL(_), _From, State) ->
    {reply, {error, not_supported}, schedule_terminate(State)};

handle_call(?PES_ASYNC_CALL(_), _From, #state{slave_pid = undefined} = State) ->
    {reply, {error, not_supported}, schedule_terminate(State)};

handle_call(?PES_ASYNC_CALL(Request), {_Pid, Tag} = From, State) ->
    State2 = create_internal_slave_request(From, Request, handle_call, State),
    {reply, {ok, Tag}, schedule_terminate(handle_slave_requests(State2))};

handle_call(?PES_ASYNC_CALL_IGNORE_ANS(_), _From, #state{slave_pid = undefined} = State) ->
    {reply, {error, not_supported}, schedule_terminate(State)};

handle_call(?PES_ASYNC_CALL_IGNORE_ANS(Request), _From, State) ->
    State2 = create_internal_slave_request(undefined, Request, handle_call, State),
    {reply, ok, schedule_terminate(handle_slave_requests(State2))};

handle_call(termination_request, From, State) ->
    gen_server:reply(From, ok),
    handle_termination_request(State).


-spec handle_cast(pes:request(), State :: state()) -> {noreply, NewState :: state()}.
handle_cast(Request, State) ->
    handle_async_request(Request, handle_cast, State).


-spec handle_info(pes:request() | management_message(), State :: state()) ->
    {noreply, NewState :: state()} | {stop, Reason :: term(), NewState :: state()}.
handle_info(pes_slave_request_batch_processed, State) ->
    {noreply, handle_slave_requests(State#state{slave_state = idle})};

handle_info({terminate, MsgRef}, State = #state{
    terminate_msg_ref = MsgRef
}) ->
    handle_termination_request(schedule_terminate(State));

handle_info({terminate, _}, State = #state{}) ->
    {noreply, State};

handle_info(slave_ready_to_terminate, State = #state{reversed_slave_request_list = []}) ->
    {stop, {shutdown, termination_request}, State};

handle_info(slave_ready_to_terminate, State) ->
    {noreply, handle_slave_requests(State#state{slave_state = idle})};

handle_info(slave_termination_aborted, State) ->
    {noreply, handle_slave_requests(State#state{slave_state = idle})};

handle_info({'EXIT', _, Reason}, State = #state{}) ->
    {stop, Reason, State};

handle_info(Request, State) ->
    handle_async_request(Request, handle_info, State).


-spec terminate(Reason :: pes:termination_reason(), State :: state()) -> ok.
terminate({shutdown, termination_request}, #state{key_hash = KeyHash, callback_module = CallbackModule}) ->
    % termination after handling termination_request - terminate callback has been called before
    pes_router:deregister(CallbackModule, KeyHash);
terminate(Reason, #state{key_hash = KeyHash, callback_module = CallbackModule, callback_state = CallbackState}) ->
    % termination as a result of supervisor request - terminate has not been called before
    pes_server_utils:mark_process_is_terminating(),
    pes_server_utils:call_optional_callback(CallbackModule, terminate, [Reason, CallbackState],
        fun(_, _) -> {ok, CallbackState} end, execute_fallback_on_error),
    pes_router:deregister(CallbackModule, KeyHash).


-spec code_change(OldVsn :: term() | {down, term()}, State :: state(), Extra :: term()) ->
    {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(OldVsn, #state{
    slave_pid = undefined,
    callback_module = CallbackModule,
    callback_state = CallbackState
} = State, Extra) ->
    CallbackAns = pes_server_utils:call_optional_callback(CallbackModule, code_change, [OldVsn, CallbackState, Extra],
        fun(_, _, _) -> {ok, CallbackState} end, execute_fallback_on_error),

    case CallbackAns of
        {ok, CallbackState2} -> {ok, State#state{callback_state = CallbackState2}};
        {error, Reason} -> {error, Reason}
    end;
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec maybe_init_callback_state(pes:callback_module(), pes:mode()) -> pes:callback_state() | undefined.
maybe_init_callback_state(CallbackModule, sync) ->
    CallbackModule:init();
maybe_init_callback_state(_CallbackModule, async) ->
    undefined.


%% @private
-spec maybe_init_slave(pes:callback_module(), pes:mode()) -> pid() | undefined.
maybe_init_slave(_CallbackModule, sync) ->
    undefined;
maybe_init_slave(CallbackModule, async) ->
    {ok, Pid} = pes_server_slave:start_link(CallbackModule, self()),
    Pid.


%% @private
-spec create_internal_slave_request(From :: {pid(), Tag :: term()} | undefined, pes:request(),
    pes_server:request_handler(), state()) -> state().
create_internal_slave_request(From, Request, Handler, #state{reversed_slave_request_list = SlaveRequests} = State) ->
    InternalRequest = #pes_slave_request{from = From, request = Request, handler = Handler},
    State#state{reversed_slave_request_list = [InternalRequest | SlaveRequests]}.


%% @private
-spec handle_slave_requests(state()) -> state().
handle_slave_requests(State = #state{reversed_slave_request_list = []}) ->
    State;
handle_slave_requests(State = #state{slave_state = active}) ->
    State;
handle_slave_requests(State = #state{reversed_slave_request_list = Requests, slave_pid = Slave}) ->
    pes_server_slave:send_slave_request_batch(Slave, #pes_slave_request_batch{requests = lists:reverse(Requests)}),
    State#state{reversed_slave_request_list = [], slave_state = active}.


%% @private
-spec handle_async_request(pes:request(), pes_server:request_handler(), state()) -> {noreply, state()}.
handle_async_request(Request, Handler, #state{
    slave_pid = undefined,
    callback_module = CallbackModule,
    callback_state = CallbackState
} = State) ->
    UpdatedCallbackState = try
        CallbackModule:Handler(Request, CallbackState)
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server handler: ~p error ~p:~p for callback module ~p and request ~p",
                [Handler, Error, Reason, CallbackModule, Request], Stacktrace),
            CallbackState
    end,
    {noreply, schedule_terminate(State#state{callback_state = UpdatedCallbackState})};

handle_async_request(Request, Handler, State) ->
    State2 = create_internal_slave_request(undefined, Request, Handler, State),
    {noreply, schedule_terminate(handle_slave_requests(State2))}.


%% @private
-spec handle_termination_request(state()) -> {stop, termination_request, state()} | {noreply, state()}.
handle_termination_request(#state{
    slave_pid = undefined,
    callback_module = CallbackModule,
    callback_state = CallbackState
} = State) ->
    case pes_server_utils:call_optional_callback(
        CallbackModule, terminate, [termination_request, CallbackState],
        fun(_, _) -> {ok, CallbackState} end, catch_and_return_error
    ) of
        {ok, UpdatedCallbackState} ->
            {stop, {shutdown, termination_request}, State#state{callback_state = UpdatedCallbackState}};
        {abort, UpdatedCallbackState} ->
            {noreply, State#state{callback_state = UpdatedCallbackState}};
        {error, _} ->
            {noreply, State}
    end;
handle_termination_request(#state{
    slave_state = idle,
    reversed_slave_request_list = [],
    slave_pid = SlavePid
} = State) ->
    gen_server:cast(SlavePid, termination_request),
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