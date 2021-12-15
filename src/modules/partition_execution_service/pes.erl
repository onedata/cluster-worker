%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface of partition execution service. Service handles
%%% requests connected with keys using callback provided by callback
%%% modules implementing pes_callback behaviour. It is guaranteed that
%%% requests connected with each key of callback module are processed
%%% sequentially. To achieve this effect partition execution service
%%% uses processes created ad-hoc. Different processes are created
%%% for different callback module even if the use the same key.
%%% Lifecycle of process created ad-hoc is as follows:
%%% 1. Process initializes state provided to oder callback using
%%%    pes_callback:init/0.
%%% 2. Process is handling requests using pes_callback.
%%% 3. Process is terminating by lack of activity for some period of
%%%    time, by external termination_request or by supervisor
%%%    (supervision tree is stopped when node is terminating).
%%%    3a. If termination is aborted (see pes_callback:terminate/2),
%%%       process returns to handling requests (2.) until next
%%%       termination attempt.
%%% Before sending first request, callback module must be initialized
%%% using init/1 or init/3 function. After handling last request
%%% terminate/1 or terminate/3 should be called.
%%%
%%% Single process can handle requests connected to several keys but it
%%% is guaranteed that two parallel requests connected with the same key
%%% are always handled by the same process. Each process is connected to
%%% supervisor and can process calls synchronously or asynchronously.
%%% Supervisors can be started/terminated by partition execution service
%%% when init/3 and terminate/3 functions are used to init/teardown callback
%%% module (otherwise it is assumed that they are managed externally).
%%%
%%% TECHNICAL NOTE:
%%% If asynchronous processing of calls is required, the process is linked
%%% with slave process. Then, it acts as a buffer for requests while
%%% processing takes place in slave process (see pes_server.erl). Such
%%% behaviour allows for calls from one process to another without risk
%%% of deadlock.
%%%
%%% Process and supervisor connected with each key are
%%% chosen using hash_key and get_supervisor_name callbacks
%%% (see pes_router.erl and pes_callback.erl).
%%%
%%% Exemplary partition execution service can be depicted as follows:
%%%
%%%                                                    +----------------+
%%%                                                    |   pes_router   |    - NOTE: single library is used to route requests
%%%    NOTE: some supervisors may not                  +----------------+\
%%%          be used at the moment but                  |                 .
%%%          they are not stopped until    CALLBACK_MODULE_1          .    \         CALLBACK_MODULE_2
%%%          callback module termination                |             .     .
%%%   +------------------+    +------------------+      .             .      \      +------------------+
%%%   | pes_supervisor 1 |    | pes_supervisor 2 |      |             .       .     | pes_supervisor 3 |
%%%   +------------------+    +------------------+      .             .        \    +------------------+
%%%                           /       \              (routing         .      (routing        |
%%%                          /         \              request)        .       request)       |
%%%                      (link)      (link)             |             .           .       (link)
%%%                        /             \              .             .            \         |
%%%                       /               \             |             .             .        |
%%%    +------------------------+      +------------------------+     .         +------------------------+
%%%    | +--------------------+ |      | +--------------------+ |     .         | +--------------------+ |
%%%    | |    pes_server 1    | |      | |    pes_server 2    | |     .         | |    pes_server 3    | |
%%%    | +--------------------+ |      | +--------------------+ |     .         | +--------------------+ |
%%%    |           |            |      |           |            |     .         |                        |
%%%    |         (link)         |      |         (link)         |     .         |                        |
%%%    |           |            |      |           |            |     .         |                        |
%%%    | +--------------------+ |      | +--------------------+ |     .         |                        |
%%%    | | pes_server_slave 1 | |      | | pes_server_slave 2 | |     .         |                        |
%%%    | +--------------------+ |      | +--------------------+ |     .         |                        |
%%%    +------------------------+      +------------------------+     .         +------------------------+
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(pes).
-author("Michal Wrzeszcz").


-include("pes_protocol.hrl").


%% API
-export([init/1, init/3, terminate/1, terminate/3]).
-export([sync_call/3, async_call/3, async_call_and_ignore_promise/3, call/5, wait/1, async_call_and_wait/3]).
-export([sync_call_if_alive/3, async_call_if_alive/3, call_if_alive/5, async_call_if_alive_and_wait/3]).
-export([cast/3, send/3]).
-export([broadcast_sync_call/2, broadcast_async_call/2, broadcast/2, broadcast_async_call_and_await_answer/2]).


-type key() :: term().
-type key_hash() :: term(). % hash of keys are used to choose process for key - see pm_callback:hash_key/1
-type request() :: term().
-type response() :: term().
-type promise() :: {wait, reference(), pid()}.
-type mode() :: sync | async.
-type sync_call() :: ?PES_SYNC_CALL(request()).
-type async_call() :: ?PES_ASYNC_CALL(request()).
-type async_call_with_ans_ignore() :: ?PES_ASYNC_CALL_IGNORE_ANS(request()).
-type call() :: sync_call() | async_call() | async_call_with_ans_ignore().
-type termination_request() :: termination_request.
-type termination_reason() :: normal | shutdown | {shutdown, term()} | term().
-type callback_module() :: module().
-type callback_state() :: term().

-export_type([key/0, key_hash/0, request/0, response/0, promise/0, mode/0, call/0,
    termination_request/0, termination_reason/0, callback_module/0, callback_state/0]).


-define(CALL_DEFAULT_TIMEOUT, infinity).
-define(CALL_DEFAULT_ATTEMPTS, 1).
-define(WAIT_DEFAULT_TIMEOUT, timer:seconds(30)).


%%%===================================================================
%%% API - init and teardown
%%% NOTE: partition execution service uses ets tables underneath and
%%% these tables are bounded to Erlang process that calls init/2 function.
%%%===================================================================

-spec init(callback_module()) -> ok.
init(CallbackModule) ->
    pes_router:init_registry(CallbackModule).


%%--------------------------------------------------------------------
%% @doc
%% Besides initialization of ets tables, this functions starts supervisors provided by 3rd argument
%% as children of root supervisor. If 3rd argument is all - it initializes all supervisors from
%% supervisor namespace (see pes_callback:supervisors_namespace/0) besides RootSupervisor (if it is
%% included in namespace).
%% @end
%%--------------------------------------------------------------------
-spec init(callback_module(), pes_router:root_sup_ref(), [pes_supervisor:name()] | all) -> ok.
init(CallbackModule, RootSupervisor, SupervisorsToInit) ->
    pes_router:init_registry(CallbackModule),
    pes_router:init_supervisors(CallbackModule, RootSupervisor, SupervisorsToInit).


-spec terminate(callback_module()) -> ok.
terminate(CallbackModule) ->
    terminate(CallbackModule, undefined, []).


%%--------------------------------------------------------------------
%% @doc
%% Besides termination of ets tables, this functions terminates supervisors provided by 3rd argument.
%% If 3rd argument is all - it terminates all supervisors from supervisor namespace besides root
%% supervisor (see pes_callback:supervisors_namespace/0).
%% @end
%%--------------------------------------------------------------------
-spec terminate(callback_module(), pes_router:root_sup_ref() | undefined, [pes_supervisor:name()] | all) -> ok.
terminate(CallbackModule, RootSupervisor, SupervisorsToTerminate) ->
    case broadcast(CallbackModule, termination_request) of
        [] ->
            case RootSupervisor of
                undefined -> ok;
                _ -> pes_router:terminate_supervisors(CallbackModule, RootSupervisor, SupervisorsToTerminate)
            end,
            pes_router:cleanup_registry(CallbackModule);
        _ ->
            timer:sleep(100),
            % Try until all processes are terminated. termination_request assumes graceful stopping of processes
            % and nothing should be forced. When application is stopping because of errors, supervisors will force
            % termination of processes and this loop will end.
            terminate(CallbackModule, RootSupervisor, SupervisorsToTerminate)
    end.


%%%===================================================================
%%% API - calls
%%% NOTE - call guarantees that request will be processed. They are
%%% protected from races between request sending to process and
%%% process termination.
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends request and hangs until the answer is returned.
%% @end
%%--------------------------------------------------------------------
-spec sync_call(callback_module(), key(), request()) -> response() | {error, Reason :: term()}.
sync_call(CallbackModule, Key, Request) ->
    call(CallbackModule, Key, ?PES_SYNC_CALL(Request), ?CALL_DEFAULT_TIMEOUT, ?CALL_DEFAULT_ATTEMPTS).


%%--------------------------------------------------------------------
%% @doc
%% Sends request and hangs only until appearance of confirmation that request is put in
%% process queue. Returns promise that can be passed to wait/1 function to return answer.
%% @end
%%--------------------------------------------------------------------
-spec async_call(callback_module(), key(), request()) -> promise() | {error, Reason :: term()}.
async_call(CallbackModule, Key, Request) ->
    call(CallbackModule, Key, ?PES_ASYNC_CALL(Request), ?CALL_DEFAULT_TIMEOUT, ?CALL_DEFAULT_ATTEMPTS).


%%--------------------------------------------------------------------
%% @doc
%% Sends request and hangs only until appearance of confirmation that request is put in
%% process queue. Does not provide promise so no answer is sent to caller.
%% @end
%%--------------------------------------------------------------------
-spec async_call_and_ignore_promise(callback_module(), key(), request()) -> ok | {error, Reason :: term()}.
async_call_and_ignore_promise(CallbackModule, Key, Request) ->
    call(CallbackModule, Key, ?PES_ASYNC_CALL_IGNORE_ANS(Request), ?CALL_DEFAULT_TIMEOUT, ?CALL_DEFAULT_ATTEMPTS).


-spec call(callback_module(), key(), call(), timeout(), non_neg_integer()) ->
    response() | promise() | {error, Reason :: term()}.
call(_CallbackModule, _Key, _Request, _Timeout, 0) ->
    {error, timeout};
call(CallbackModule, Key, Request, Timeout, Attempts) ->
    case get_or_create_pes_server(CallbackModule, Key) of
        {ok, Pid} ->
            try
                call_if_it_is_allowed(Pid, Request, Timeout)
            catch
                _:{noproc, _} ->
                    pes_router:delete_key_if_is_connected_with_pid(CallbackModule, Key, Pid),
                    call(CallbackModule, Key, Request, Timeout, Attempts); % Race with process termination - do
                                                                           % not decrement attempts
                exit:{normal, _} ->
                    pes_router:delete_key_if_is_connected_with_pid(CallbackModule, Key, Pid),
                    call(CallbackModule, Key, Request, Timeout, Attempts); % Race with process termination - do
                                                                           % not decrement attempts
                _:{timeout, _} ->
                    call(CallbackModule, Key, Request, Timeout, Attempts - 1);
                _:Reason:Stacktrace ->
                    {error, {Reason, Stacktrace}} % unknown error - do not try again and add stacktrace to error
            end;
        {error, Reason} ->
            {error, Reason} % Error starting process - do not try again
    end.


-spec wait(promise() | response() | {error, Reason :: term()}) -> response() | {error, term()}.
wait({wait, Ref, Pid}) ->
    case pes_server_utils:can_execute_wait() of
        true ->
            wait(Ref, Pid, ?WAIT_DEFAULT_TIMEOUT, true);
        false ->
            {error, potential_deadlock} % Sync waiting inside server slave or gen_server:terminate may result in deadlock
                                        % Answer should be handled asynchronously by handle_info callback
    end;
wait(Other) ->
    Other.


-spec async_call_and_wait(callback_module(), key(), request()) -> response() | {error, Reason :: term()}.
async_call_and_wait(CallbackModule, Key, Request) ->
    wait(async_call(CallbackModule, Key, Request)).


%%%===================================================================
%%% API - calls to alive processes
%%%===================================================================

-spec sync_call_if_alive(callback_module(), key(), request()) -> response() | {error, Reason :: term()}.
sync_call_if_alive(CallbackModule, Key, Request) ->
    call_if_alive(CallbackModule, Key, ?PES_SYNC_CALL(Request), ?CALL_DEFAULT_TIMEOUT, ?CALL_DEFAULT_ATTEMPTS).


-spec async_call_if_alive(callback_module(), key(), request()) -> promise() | {error, Reason :: term()}.
async_call_if_alive(CallbackModule, Key, Request) ->
    call_if_alive(CallbackModule, Key, ?PES_ASYNC_CALL(Request), ?CALL_DEFAULT_TIMEOUT, ?CALL_DEFAULT_ATTEMPTS).


-spec call_if_alive(callback_module(), key(), call(), timeout(), non_neg_integer()) -> 
    response() | promise() | {error, Reason :: term()}.
call_if_alive(_CallbackModule, _Key, _Request, _Timeout, 0) ->
    {error, timeout};
call_if_alive(CallbackModule, Key, Request, Timeout, Attempts) ->
    case pes_router:get_initialized(CallbackModule, Key) of
        {ok, Pid} ->
            try
                call_if_it_is_allowed(Pid, Request, Timeout)
            catch
                _:{noproc, _} ->
                    {error, not_alive};
                exit:{normal, _} ->
                    {error, not_alive};
                _:{timeout, _} ->
                    call_if_alive(CallbackModule, Key, Request, Timeout, Attempts - 1);
                _:Reason:Stacktrace ->
                    {error, {Reason, Stacktrace}}
            end;
        {error, not_found} ->
            {error, not_alive}
    end.


-spec async_call_if_alive_and_wait(callback_module(), key(), request()) -> response() | {error, Reason :: term()}.
async_call_if_alive_and_wait(CallbackModule, Key, Request) ->
    wait(async_call_if_alive(CallbackModule, Key, Request)).


%%%===================================================================
%%% API - requests with no guarantees of provisioning to pes_server.
%%% NOTE: these functions checks if process exist and create it if not.
%%% However, they are not protected from races between sending and
%%% process termination.
%%%===================================================================

-spec cast(callback_module(), key(), request()) -> ok | {error, Reason :: term()}.
cast(CallbackModule, Key, Request) ->
    case get_or_create_pes_server(CallbackModule, Key) of
        {ok, Pid} -> gen_server:cast(Pid, Request);
        {error, Reason} -> {error, Reason}
    end.


-spec send(callback_module(), key(), request()) -> ok | {error, Reason :: term()}.
send(CallbackModule, Key, Info) ->
    case get_or_create_pes_server(CallbackModule, Key) of
        {ok, Pid} -> Pid ! Info, ok;
        {error, Reason} -> {error, Reason}
    end.


%%%===================================================================
%%% API - broadcast
%%%===================================================================

-spec broadcast_sync_call(callback_module(), request()) -> [response() | {error, Reason :: term()}].
broadcast_sync_call(CallbackModule, Msg) ->
    broadcast(CallbackModule, ?PES_SYNC_CALL(Msg)).


-spec broadcast_async_call(callback_module(), request()) -> [promise() | {error, Reason :: term()}].
broadcast_async_call(CallbackModule, Msg) ->
    broadcast(CallbackModule, ?PES_ASYNC_CALL(Msg)).


-spec broadcast(callback_module(), call() | termination_request()) -> 
    [response() | promise() | {error, Reason :: term()}].
broadcast(CallbackModule, Msg) ->
    case pes_server_utils:can_execute_call() of
        true ->
            lists:reverse(pes_router:fold(CallbackModule, fun(Pid, Acc) ->
                try
                    [call_and_create_promise_if_async_request(Pid, Msg, ?CALL_DEFAULT_TIMEOUT) | Acc]
                catch
                    _:{noproc, _} ->
                        Acc; % Ignore terminated process
                    exit:{normal, _} ->
                        Acc; % Ignore terminated process
                    _:{timeout, _} ->
                        [{error, timeout} | Acc];
                    _:Reason:Stacktrace ->
                        [{error, {Reason, Stacktrace}} | Acc]
                end
            end, []));
        false ->
            {error, potential_deadlock}
    end.


-spec broadcast_async_call_and_await_answer(callback_module(), request()) -> [response() | {error, term()}].
broadcast_async_call_and_await_answer(CallbackModule, Msg) ->
    wait_for_all(broadcast_async_call(CallbackModule, Msg)).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_or_create_pes_server(callback_module(), key()) -> {ok, pid()} | {error, Reason :: term()}.
get_or_create_pes_server(CallbackModule, Key) ->
    case pes_router:get(CallbackModule, Key) of
        {ok, Pid} -> {ok, Pid};
        {error, not_found} -> create_pes_server(CallbackModule, Key)
    end.


%% @private
-spec create_pes_server(callback_module(), key()) -> {ok, pid()} | {error, Reason :: term()}.
create_pes_server(CallbackModule, Key) ->
    try
        case pes_router:start(CallbackModule, Key) of
            {ok, undefined} -> get_or_create_pes_server(CallbackModule, Key);
            {ok, Pid} -> {ok, Pid};
            {error, Reason} -> {error, Reason}
        end
    catch
        _:CatchReason:Stacktrace ->
            {error, {CatchReason, Stacktrace}}
    end.


%% @private
-spec call_if_it_is_allowed(pid(), request(), timeout()) -> response() | promise() | {error, Reason :: term()}.
call_if_it_is_allowed(Pid, Request, Timeout) ->
    case pes_server_utils:can_execute_call() of
        true -> call_and_create_promise_if_async_request(Pid, Request, Timeout);
        false -> {error, potential_deadlock}
    end.


%% @private
-spec call_and_create_promise_if_async_request(pid(), request(), timeout()) ->
    response() | promise() | {error, Reason :: term()}.
call_and_create_promise_if_async_request(Pid, Request, Timeout) ->
    case {Request, gen_server:call(Pid, Request, Timeout)} of
        {?PES_ASYNC_CALL(_), {ok, Ref}} -> {wait, Ref, Pid};
        {_, Other} -> Other
    end.


%% @private
-spec wait(reference(), pid(), non_neg_integer(), boolean()) -> response() | {error, term()}.
wait(Ref, Pid, Timeout, CheckAndRetry) ->
    receive
        {Ref, Response} -> Response
    after
        Timeout ->
            case {CheckAndRetry, rpc:call(node(Pid), erlang, is_process_alive, [Pid])} of
                {true, true} -> wait(Ref, Pid, Timeout, CheckAndRetry);
                {true, _} -> wait(Ref, Pid, Timeout, false); % retry last time to prevent race between
                                                             % answer sending / process terminating
                {_, {badrpc, Reason}} -> {error, Reason};
                _ -> {error, timeout}
            end
    end.


%% @private
-spec wait_for_all([promise()]) -> [response() | {error, term()}].
wait_for_all([]) ->
    [];
wait_for_all([{wait, Ref, Pid} | Tail]) ->
    [wait(Ref, Pid, ?WAIT_DEFAULT_TIMEOUT, true) | wait_for_all(Tail)];
wait_for_all([Other | Tail]) ->
    [Other | wait_for_all(Tail)].