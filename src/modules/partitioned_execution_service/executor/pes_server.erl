%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Gen_server handling requests connected with single partition of
%%% keys (see pes_process_manager.erl for more information about keys).
%%% Requests are handled in order of appearance, and the executor state
%%% is passed between subsequent callbacks - (possibly modified) state
%%% returned as result of one request handling is passed to the function
%%% handling the next request. pes_server can handle requests in two ways
%%% depending on chosen mode (mode is chosen using
%%% pes_plugin_behaviour:get_mode/0):
%%% - sync - handling requests by pes_server process, pes_server_slave
%%%          is not started,
%%% - async - delegating requests handling to pes_server_slave process.
%%% Delegating request handling to pes_server_slave process allows internal
%%% calls (calls from one pes process to another pes process - see pes.erl).
%%% In such a case pes_server works as a buffer for requests processed by
%%% pes_server_slave process.
%%%
%%% TECHNICAL NOTE:
%%% For sync mode callbacks are executed by pes_server. pes_server keeps
%%% the executor state that is passed to callbacks and modified by them.
%%% For async mode, all callbacks are executed pes_server_slave. In such
%%% a case, pes_server_slave keeps the executor state.
%%%
%%% When working in async mode, request handling is as follows:
%%% 1. Client process sends request to pes_server.
%%% 2. Pes_server caches request in its state.
%%%    2a. If request is PES_SUBMIT, pes_server responds to client with
%%%        tag that can be used to wait for async response.
%%% 3. Pes_server checks the readiness of pes_server_slave.
%%%    3a. If pes_server_slave is actively processing requests,
%%%        pes_server recognizes its readiness as 'busy' and waits for
%%%        it to finish. When pes_server_slave sends
%%%        a 'pes_slave_task_batch_processed' message, pes_server
%%%        recognizes its readiness as 'idle' and acts as in 3b.
%%%    3b. If pes_server_slave is recognized as `idle`, pes_server sends
%%%        a batch with all cached requests to pes_server_slave and
%%%        recognizes pes_server_slave readiness as 'busy'.
%%% 4. Pes_server_slave processes each request.
%%%    4a. For requests that should be answered (PES_SUBMIT),
%%%        pes_server_slave sends results of request processing
%%%        directly to client process.
%%%
%%% pes_server can be terminated in two ways:
%%% - handling graceful_termination_request,
%%% - by supervisor.
%%% graceful_termination_request can be sent to process externally or be result
%%% of internal pes_server action when no request appear for ?IDLE_TIMEOUT.
%%% Termination is preceded by execution of pes_plugin_behaviour callback:
%%% graceful_terminate/1 for graceful_termination_request and forced_terminate/2
%%% for terminations initiated by supervisor. These callbacks are executed by
%%% pes_server for sync mode and pes_server_slave for async mode.
%%% graceful_terminate/1 callback can defer termination. Appearance of new
%%% request also defers termination initiated by graceful_termination_request.
%%% Termination by supervisor cannot be deferred.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_server).
-author("Michal Wrzeszcz").


-include("pes_protocol.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


-behaviour(gen_server).


%% API
-export([start_link/2, call/3, cast/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


-record(state, {
    plugin :: pes:plugin(),
    executor_state :: pes:executor_state() | undefined, % undefined for async mode when pes_server_slave holds the state

    key_hash :: pes_process_manager:key_hash(),
    mode :: pes:mode(),
    slave_info :: undefined | slave_info(),  % undefined for sync mode

    % Fields used to init termination after ?IDLE_TIMEOUT of inactivity
    idle_timeout_msg_ref :: reference() | undefined,
    idle_timeout_timer_ref :: reference() | undefined
}).


-record(slave_info, {
    pid :: pid(),
    readiness = idle :: slave_readiness(),
    reversed_task_queue = [] :: [pes_server_slave:pes_slave_task()]
}).


-type state() :: #state{}.
-type slave_info() :: #slave_info{}.
-type slave_readiness() :: idle | busy.
-type control_message_from_slave() :: pes_slave_task_batch_processed | slave_ready_to_terminate |
    slave_termination_deferred.
-type internal_timer_timeout_message() :: {idle_timeout_timer_expired, reference()}.
-type message() :: control_message_from_slave() | internal_timer_timeout_message() |
    {'EXIT', From :: pid(), Reason :: term()}.
-type execution_callback() :: handle_call | handle_cast.

-export_type([execution_callback/0]).


-define(IDLE_TIMEOUT, timer:seconds(30)).


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pes:plugin(), pes:key()) -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(Plugin, Key) ->
    gen_server2:start_link(?MODULE, [Plugin, Key], []).


-spec call(pid(), pes:message_with_delivery_check() | pes:graceful_termination_request(), timeout()) ->
    ok | pes:execution_result() | {ok, pes:execution_promise()}.
call(Pid, Message, Timeout) ->
    gen_server2:call(Pid, Message, Timeout).


-spec cast(pid(), ?PES_CAST(pes:request())) -> ok.
cast(Pid, Message) ->
    gen_server2:cast(Pid, Message).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(Args :: list()) -> {ok, state()} | {stop, Reason :: term()} | ignore.
init([Plugin, KeyHash]) ->
    try
        Pid = self(),
        case pes_process_manager:register_server(Plugin, KeyHash, Pid) of
            ok ->
                pes_process_identity:mark_as_pes_server(),
                process_flag(trap_exit, true),
                State = init_state(#state{
                    plugin = Plugin,
                    key_hash = KeyHash,
                    mode = pes_plugin:get_mode(Plugin)
                }),
                pes_process_manager:report_server_initialized(Plugin, KeyHash, Pid),
                {ok, reset_idle_timeout_timer(State)};
            {error, already_exists} ->
                ignore
        end
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server init error ~p:~p for plug-in ~p and hash ~p",
                [Error, Reason, Plugin, KeyHash], Stacktrace),
            pes_process_manager:deregister_server(Plugin, KeyHash),
            {stop, Reason}
    end.


-spec handle_call(pes:message_with_delivery_check() | pes:graceful_termination_request(),
    From :: {pid(), Tag :: term()}, State :: state()) -> {reply, pes:execution_result(), NewState :: state()} |
    {noreply, NewState :: state()} | {stop, Reason :: term(), NewState :: state()}.
handle_call(?PES_CALL(Request), _From, #state{
    mode = sync,
    plugin = Plugin,
    executor_state = ExecutorState
} = State) ->
    try
        {RequestAns, UpdatedExecutorState} = pes_plugin:handle_call(Plugin, Request, ExecutorState),
        {reply, RequestAns, reset_idle_timeout_timer(State#state{executor_state = UpdatedExecutorState})}
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server handle_call error ~p:~p for plug-in ~p and request ~p",
                [Error, Reason, Plugin, Request], Stacktrace),
            {reply, ?ERROR_INTERNAL_SERVER_ERROR, reset_idle_timeout_timer(State)}
    end;

handle_call(?PES_CALL(_), _From, State) ->
    {reply, ?ERROR_NOT_SUPPORTED, reset_idle_timeout_timer(State)};

handle_call(?PES_SUBMIT(_), _From, #state{mode = sync} = State) ->
    {reply, ?ERROR_NOT_SUPPORTED, reset_idle_timeout_timer(State)};

handle_call(?PES_SUBMIT(Request), {_Pid, Tag} = From, State) ->
    State2 = allocate_for_slave_processing(From, Request, handle_call, State),
    {reply, {ok, {Tag, self()}}, reset_idle_timeout_timer(State2)};

handle_call(?PES_ACKNOWLEDGED_CAST(_), _From, #state{mode = sync} = State) ->
    {reply, ?ERROR_NOT_SUPPORTED, reset_idle_timeout_timer(State)};

handle_call(?PES_ACKNOWLEDGED_CAST(Request), _From, State) ->
    State2 = allocate_for_slave_processing(undefined, Request, handle_cast, State),
    {reply, ok, reset_idle_timeout_timer(State2)};

handle_call(graceful_termination_request, From, State) ->
    gen_server2:reply(From, ok),
    handle_graceful_termination_request(State).


-spec handle_cast(?PES_CAST(pes:request()), State :: state()) -> {noreply, NewState :: state()}.
handle_cast(?PES_CAST(Request), State) ->
    handle_cast_internal(Request, State);

handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


-spec handle_info(pes:request() | message(), State :: state()) ->
    {noreply, NewState :: state()} | {stop, Reason :: term(), NewState :: state()}.
handle_info(pes_slave_task_batch_processed, State) ->
    {noreply, send_requests_to_slave_if_idle(set_slave_readiness(State, idle))};

handle_info({idle_timeout_timer_expired, MsgRef}, State = #state{
    idle_timeout_msg_ref = MsgRef
}) ->
    handle_graceful_termination_request(reset_idle_timeout_timer(State));

handle_info({idle_timeout_timer_expired, _}, State = #state{}) ->
    % Terminate message with outdated reference - ignore
    {noreply, State};

handle_info(slave_ready_to_terminate, State = #state{slave_info = #slave_info{reversed_task_queue = []}}) ->
    {stop, {shutdown, graceful_terminate}, State};

handle_info(slave_ready_to_terminate, State) ->
    {noreply, send_requests_to_slave_if_idle(set_slave_readiness(State, idle))};

handle_info(slave_termination_deferred, State) ->
    {noreply, send_requests_to_slave_if_idle(set_slave_readiness(State, idle))};

handle_info({'EXIT', _, Reason}, State = #state{}) ->
    % Catch exit of slave process
    {stop, Reason, State};

handle_info(?PES_SELF_CAST(Request), State) ->
    % self cast is sent as info (see pes:self_cast())
    handle_cast_internal(Request, State);

handle_info(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


-spec terminate(Reason :: pes:forced_termination_reason(), State :: state()) -> ok.
terminate({shutdown, graceful_terminate}, #state{key_hash = KeyHash, plugin = Plugin}) ->
    % termination after handling graceful_termination_request - executor's graceful_terminate callback
    % has already been called
    pes_process_manager:deregister_server(Plugin, KeyHash);

terminate(Reason, #state{key_hash = KeyHash, plugin = Plugin, executor_state = ExecutorState}) ->
    % termination as a result of supervisor request
    pes_process_identity:mark_as_terminating_pes_process(),
    pes_plugin:forced_terminate(Plugin, Reason, ExecutorState),
    pes_process_manager:deregister_server(Plugin, KeyHash).


-spec code_change(OldVsn :: term() | {down, term()}, State :: state(), Extra :: term()) ->
    {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(OldVsn, #state{
    mode = sync,
    plugin = Plugin,
    executor_state = ExecutorState
} = State, Extra) ->
    case pes_plugin:code_change(Plugin, OldVsn, ExecutorState, Extra) of
        {ok, ExecutorState2} -> {ok, State#state{executor_state = ExecutorState2}};
        {error, Reason} -> {error, Reason}
    end;

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec init_state(pes:executor_state()) -> pes:executor_state().
init_state(State = #state{mode = sync, plugin = Plugin}) ->
    State#state{executor_state = pes_plugin:init(Plugin)};

init_state(State = #state{mode = async, plugin = Plugin}) ->
    {ok, SlavePid} = pes_server_slave:start_link(Plugin, self()),
    State#state{slave_info = #slave_info{pid = SlavePid}}.


%% @private
-spec allocate_for_slave_processing(From :: {pid(), Tag :: term()} | undefined, pes:request(),
    pes_server:execution_callback(), state()) -> state().
allocate_for_slave_processing(From, Request, Callback, #state{
    slave_info = SlaveInfo = #slave_info{reversed_task_queue = SlaveTasks}
} = State) ->
    SlaveTask = #pes_slave_task{from = From, request = Request, callback = Callback},
    State2 = State#state{slave_info = SlaveInfo#slave_info{reversed_task_queue = [SlaveTask | SlaveTasks]}},
    send_requests_to_slave_if_idle(State2).


%% @private
-spec send_requests_to_slave_if_idle(state()) -> state().
send_requests_to_slave_if_idle(State = #state{slave_info = #slave_info{reversed_task_queue = []}}) ->
    State;

send_requests_to_slave_if_idle(State = #state{slave_info = #slave_info{readiness = busy}}) ->
    State;

send_requests_to_slave_if_idle(State = #state{
    slave_info = SlaveInfo = #slave_info{reversed_task_queue = Tasks, pid = SlavePid}
}) ->
    pes_server_slave:send_slave_task_batch(SlavePid, #pes_slave_task_batch{tasks = lists:reverse(Tasks)}),
    set_slave_readiness(State#state{slave_info = SlaveInfo#slave_info{reversed_task_queue = []}}, busy).


%% @private
-spec handle_cast_internal(pes:request(), state()) -> {noreply, state()}.
handle_cast_internal(Request, #state{
    mode = sync,
    plugin = Plugin,
    executor_state = ExecutorState
} = State) ->
    UpdatedExecutorState = try
        pes_plugin:handle_cast(Plugin, Request, ExecutorState)
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("PES server handle_cast error ~p:~p for plug-in ~p and request ~p",
                [Error, Reason, Plugin, Request], Stacktrace),
            ExecutorState
    end,
    {noreply, reset_idle_timeout_timer(State#state{executor_state = UpdatedExecutorState})};

handle_cast_internal(Request, State) ->
    State2 = allocate_for_slave_processing(undefined, Request, handle_cast, State),
    {noreply, reset_idle_timeout_timer(State2)}.


%% @private
-spec handle_graceful_termination_request(state()) ->
    {stop, graceful_termination_request, state()} | {noreply, state()}.
handle_graceful_termination_request(#state{
    mode = sync,
    plugin = Plugin,
    executor_state = ExecutorState
} = State) ->
    case pes_plugin:graceful_terminate(Plugin, ExecutorState) of
        {ok, UpdatedExecutorState} ->
            {stop, {shutdown, graceful_terminate}, State#state{executor_state = UpdatedExecutorState}};
        {defer, UpdatedExecutorState} ->
            {noreply, State#state{executor_state = UpdatedExecutorState}};
        ?ERROR_INTERNAL_SERVER_ERROR ->
            {noreply, State}
    end;

handle_graceful_termination_request(#state{slave_info = #slave_info{
    readiness = idle,
    reversed_task_queue = [],
    pid = SlavePid
}} = State) ->
    pes_server_slave:send_graceful_termination_request(SlavePid),
    {noreply, set_slave_readiness(State, busy)};

handle_graceful_termination_request(State) ->
    % ignore as there are pending requests, there will be subsequent termination retries
    {noreply, State}.


%% @private
-spec reset_idle_timeout_timer(state()) -> state().
reset_idle_timeout_timer(State = #state{idle_timeout_timer_ref = undefined}) ->
    MsgRef = make_ref(),
    State#state{
        idle_timeout_msg_ref = MsgRef,
        idle_timeout_timer_ref = erlang:send_after(?IDLE_TIMEOUT, self(), {idle_timeout_timer_expired, MsgRef})
    };

reset_idle_timeout_timer(State = #state{idle_timeout_timer_ref = Ref}) ->
    erlang:cancel_timer(Ref),
    reset_idle_timeout_timer(State#state{idle_timeout_timer_ref = undefined}).


%% @private
-spec set_slave_readiness(state(), slave_readiness()) -> state().
set_slave_readiness(State = #state{slave_info = SlaveInfo}, Readiness) ->
    State#state{slave_info = SlaveInfo#slave_info{readiness = Readiness}}.