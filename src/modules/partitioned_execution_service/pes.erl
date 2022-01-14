%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface of partitioned execution service - PES. Service handles requests
%%% associated with arbitrary keys using callbacks provided by executor
%%% implementing pes_executor_behaviour behaviour. It is guaranteed that
%%% requests connected with each key in the scope of the same executor
%%% are processed sequentially. To achieve this effect PES uses processes
%%% created ad-hoc. Different processes are created for different executors
%%% even if they use the same key. Lifecycle of process created ad-hoc is as follows:
%%% 1. pes_executor_behaviour:init/0 is used to create initial state provided
%%%    to other callbacks of executor.
%%% 2. Process is handling requests using pes_executor_behaviour.
%%% 3. Process is terminating by lack of activity for some period of
%%%    time, by external termination_request or by supervisor
%%%    (supervision tree is stopped when node is terminating).
%%%    3a. If termination is aborted (see pes_executor_behaviour:terminate/2),
%%%       process returns to handling requests (2.) until next
%%%       termination attempt.
%%% Before sending first request, executor must be initialized using start_link/1
%%% function. After handling last request stop/1 should be called.
%%%
%%% Single process can handle requests connected to several keys but it
%%% is guaranteed that two parallel requests connected with the same key
%%% are always handled by the same process. Each process is connected to
%%% supervisor and can process calls synchronously or asynchronously.
%%%
%%% TECHNICAL NOTE:
%%% If asynchronous processing of calls is required, the process is linked
%%% with slave process. Then, it acts as a buffer for requests while
%%% processing takes place in slave process (see pes_server.erl). Such
%%% behaviour allows for calls from one process to another without risk
%%% of deadlock.
%%%
%%% Process and supervisor connected with each key are chosen automatically
%%% using get_servers_count and get_server_groups_count callbacks
%%% (see pes_process_manager.erl and pes_executor_behaviour.erl).
%%%
%%% Exemplary PES can be depicted as follows:
%%%
%%%                                                    +-------------------------+
%%%                                                    |   pes_process_manager   |    - NOTE: single library is used
%%%    NOTE: some supervisors may not                  +-------------------------+            to route requests
%%%          be used at the moment but                  |                 .
%%%          they are not stopped until                 |             .    \
%%%          executor termination                |             .     .
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
%%%                                                                   .
%%%                            EXECUTOR_1                             .                 EXECUTOR_2
%%%                                                                   .
%%% @end
%%%-------------------------------------------------------------------
-module(pes).
-author("Michal Wrzeszcz").


-include("pes_protocol.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([start_link/1, stop/1]).
-export([call/3, call/4, submit/3, submit/4, await/1, submit_and_await/3, submit_and_await/4,
    check_cast/3, check_cast/4, self_cast/1]).
-export([cast/3]).
-export([multi_call/2, multi_submit/2, multi_check_cast/2, multi_submit_and_await_answers/2]).


-type key() :: term().
-type request() :: term().
-type execution_result() :: term().
-type execution_promise() :: {await, Tag :: term(), pid()}.
-type mode() :: sync | async.

-type call() :: ?PES_CALL(request()).
-type submit() :: ?PES_SUBMIT(request()).
-type check_cast() :: ?PES_CHECK_CAST(request()).
-type request_with_delivery_check() :: call() | submit() | check_cast().
-type send_request_options() :: #{
    timeout => timeout(),
    attempts => non_neg_integer(),
    expect_alive => boolean() % TODO-8523 Maybe change name of option
}.
-type send_response() :: execution_result() | execution_promise().

-type termination_request() :: termination_request.
-type termination_reason() :: normal | shutdown | {shutdown, term()} | term().
-type executor() :: module().
-type executor_state() :: term().

-export_type([key/0, request/0, execution_result/0, mode/0, call/0,
    termination_request/0, termination_reason/0, executor/0, executor_state/0]).


-define(DEFAULT_TIMEOUT, infinity).
-define(DEFAULT_ATTEMPTS, 1).
-define(AWAIT_DEFAULT_TIMEOUT, timer:seconds(30)).


%%%===================================================================
%%% API - init and teardown
%%% NOTE: process that calls start_link should be alive as long as PES is used for Executor.
%%% It should call stop when PES is not needed anymore.
%%%===================================================================


-spec start_link(executor()) -> ok.
start_link(Executor) ->
    pes_process_manager:init_registry(Executor),
    pes_process_manager:init_supervisors(Executor).


-spec stop(executor()) -> ok.
stop(Executor) ->
    case send_to_all(Executor, termination_request) of
        [] ->
            pes_process_manager:terminate_supervisors(Executor),
            pes_process_manager:cleanup_registry(Executor);
        _ ->
            timer:sleep(100),
            % Try until all processes are terminated. termination_request assumes graceful stopping of processes
            % and nothing should be forced. When application is stopping because of errors, supervisors will force
            % termination of processes and this loop will end.
            stop(Executor)
    end.


%%%===================================================================
%%% API - calls
%%% NOTE - call guarantees that request will be processed. They are protected
%%% from races between request sending to process and process termination.
%%%===================================================================

-spec call(executor(), key(), request()) -> execution_result() | {error, Reason :: term()}.
call(Executor, Key, Request) ->
    call(Executor, Key, Request, #{}).


%%--------------------------------------------------------------------
%% @doc
%% Sends request and hangs until the answer is returned.
%% @end
%%--------------------------------------------------------------------
-spec call(executor(), key(), request(), send_request_options()) -> execution_result() | {error, Reason :: term()}.
call(Executor, Key, Request, Options) ->
    send_request_and_check_delivery(Executor, Key, ?PES_CALL(Request), Options).


-spec submit(executor(), key(), request()) -> execution_promise() | {error, Reason :: term()}.
submit(Executor, Key, Request) ->
    submit(Executor, Key, Request, #{}).


%%--------------------------------------------------------------------
%% @doc
%% Sends request and hangs only until appearance of confirmation that request is put in
%% process queue. Returns promise that can be passed to await/1 function to return answer.
%% NOTE: promise should be used only by process that executed submit function.
%% @end
%%--------------------------------------------------------------------
-spec submit(executor(), key(), request(), send_request_options()) -> execution_promise() | {error, Reason :: term()}.
submit(Executor, Key, Request, Options) ->
    send_request_and_check_delivery(Executor, Key, ?PES_SUBMIT(Request), Options).


-spec await(execution_promise() | {error, Reason :: term()}) -> execution_result() | {error, term()}.
await({await, Ref, Pid}) ->
    case pes_server_utils:can_execute_wait() of
        true ->
            await(Ref, Pid, ?AWAIT_DEFAULT_TIMEOUT, true);
        false ->
            {error, potential_deadlock} % Sync waiting inside server slave or gen_server:terminate may result in deadlock
                                        % Answer should be handled asynchronously by handle_cast callback
    end;
await({error, _} = Error) ->
    Error.


-spec submit_and_await(executor(), key(), request()) -> execution_result() | {error, Reason :: term()}.
submit_and_await(Executor, Key, Request) ->
    await(submit(Executor, Key, Request)).


-spec submit_and_await(executor(), key(), request(), send_request_options()) ->
    execution_result() | {error, Reason :: term()}.
submit_and_await(Executor, Key, Request, Options) ->
    await(submit(Executor, Key, Request, Options)).


-spec check_cast(executor(), key(), request()) -> ok | {error, Reason :: term()}.
check_cast(Executor, Key, Request) ->
    check_cast(Executor, Key, Request, #{}).


%%--------------------------------------------------------------------
%% @doc
%% Sends request and hangs only until appearance of confirmation that request is put in process queue.
%% Does not provide answer or promise (no answer is sent to caller when request processing ends).
%% @end
%%--------------------------------------------------------------------
-spec check_cast(executor(), key(), request(), send_request_options()) -> ok | {error, Reason :: term()}.
check_cast(Executor, Key, Request, Options) ->
    send_request_and_check_delivery(Executor, Key, ?PES_CHECK_CAST(Request), Options).


%%--------------------------------------------------------------------
%% @doc
%% Cast function to be used from the executor handler
%% (it is faster as there is no need to resolve key).
%% @end
%%--------------------------------------------------------------------
-spec self_cast(request()) -> ok.
self_cast(Request) ->
    self() ! Request.


%%%===================================================================
%%% API - requests with no guarantees of provisioning to pes_server.
%%% NOTE: these functions checks if process exist and create it if not.
%%% However, they are not protected from races between sending and
%%% process termination.
%%%===================================================================

-spec cast(executor(), key(), request()) -> ok | {error, Reason :: term()}.
cast(Executor, Key, Request) ->
    case acquire_pes_server(Executor, Key) of
        {ok, Pid} -> gen_server2:cast(Pid, Request);
        {error, Reason} -> {error, Reason}
    end.


%%%===================================================================
%%% API - sending requests to all processes
%%%===================================================================

-spec multi_call(executor(), request()) -> [execution_result() | {error, Reason :: term()}].
multi_call(Executor, Msg) ->
    send_to_all(Executor, ?PES_CALL(Msg)).


-spec multi_submit(executor(), request()) -> [execution_promise() | {error, Reason :: term()}].
multi_submit(Executor, Msg) ->
    send_to_all(Executor, ?PES_SUBMIT(Msg)).


-spec multi_check_cast(executor(), request()) -> [ok | {error, Reason :: term()}].
multi_check_cast(Executor, Msg) ->
    send_to_all(Executor, ?PES_CHECK_CAST(Msg)).


-spec multi_submit_and_await_answers(executor(), request()) -> [execution_result() | {error, term()}].
multi_submit_and_await_answers(Executor, Msg) ->
    await_for_all(multi_submit(Executor, Msg)).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%%--------------------------------------------------------------------
%% @doc
%% Sends request and checks if request is put in process queue.
%% @end
%%--------------------------------------------------------------------
-spec send_request_and_check_delivery(executor(), key(), request_with_delivery_check(), send_request_options()) ->
    send_response() | {error, Reason :: term()}.
send_request_and_check_delivery(_Executor, _Key, _Request, #{attempts := 0}) ->
    {error, timeout};
send_request_and_check_delivery(Executor, Key, Request, #{
    expect_alive := true
} = Options) ->
    Timeout = maps:get(timeout, Options, ?DEFAULT_TIMEOUT),
    Attempts = maps:get(attempts, Options, ?DEFAULT_ATTEMPTS),

    case pes_process_manager:get_server_if_initialized(Executor, Key) of
        {ok, Pid} ->
            try
                send_request_internal(Pid, Request, Timeout)
            catch
                _:{noproc, _} ->
                    {error, not_alive};
                exit:{normal, _} ->
                    {error, not_alive};
                _:{timeout, _} ->
                    send_request_and_check_delivery(Executor, Key, Request, Options#{attempts => Attempts - 1});
                Error:Reason:Stacktrace ->
                    ?error_stacktrace("PES send_request error ~p:~p for executor ~p, key ~p and options ~p",
                        [Error, Reason, Executor, Key, Options], Stacktrace),
                    {error, Reason} % unknown error - do not try again and add stacktrace to error
            end;
        {error, not_found} ->
            {error, not_alive}
    end;
send_request_and_check_delivery(Executor, Key, Request, Options) ->
    Timeout = maps:get(timeout, Options, ?DEFAULT_TIMEOUT),
    Attempts = maps:get(attempts, Options, ?DEFAULT_ATTEMPTS),

    case acquire_pes_server(Executor, Key) of
        {ok, Pid} ->
            try
                send_request_internal(Pid, Request, Timeout)
            catch
                _:{noproc, _} ->
                    pes_process_manager:delete_key_to_server_mapping(Executor, Key, Pid),
                    % Race with process termination - do not decrement attempts
                    send_request_and_check_delivery(Executor, Key, Request, Options);
                exit:{normal, _} ->
                    pes_process_manager:delete_key_to_server_mapping(Executor, Key, Pid),
                    % Race with process termination - do not decrement attempts
                    send_request_and_check_delivery(Executor, Key, Request, Options);
                _:{timeout, _} ->
                    send_request_and_check_delivery(Executor, Key, Request, Options#{attempts => Attempts - 1});
                Error:Reason:Stacktrace ->
                    ?error_stacktrace("PES send_request error ~p:~p for executor ~p, key ~p and options ~p",
                        [Error, Reason, Executor, Key, Options], Stacktrace),
                    {error, Reason} % unknown error - do not try again and add stacktrace to error
            end;
        {error, Reason} ->
            {error, Reason} % Error starting process - do not try again
    end.


%% @private
-spec send_to_all(executor(), request_with_delivery_check() | termination_request()) ->
    [send_response() | {error, Reason :: term()}].
send_to_all(Executor, Msg) ->
    case pes_server_utils:can_execute_call() of
        true ->
            lists:reverse(pes_process_manager:fold_servers(Executor, fun(Pid, Acc) ->
                try
                    [gen_server2:call(Pid, Msg, ?DEFAULT_TIMEOUT) | Acc]
                catch
                    _:{noproc, _} ->
                        Acc; % Ignore terminated process
                    exit:{normal, _} ->
                        Acc; % Ignore terminated process
                    _:{timeout, _} ->
                        [{error, timeout} | Acc];
                    Error:Reason:Stacktrace ->
                        ?error_stacktrace("PES call error ~p:~p for executor ~p and pid ~p",
                            [Error, Reason, Executor, Pid], Stacktrace),
                        [{error, Reason} | Acc]
                end
            end, []));
        false ->
            {error, potential_deadlock}
    end.


%% @private
-spec acquire_pes_server(executor(), key()) -> {ok, pid()} | {error, Reason :: term()}.
acquire_pes_server(Executor, Key) ->
    case pes_process_manager:get_server(Executor, Key) of
        {ok, Pid} -> {ok, Pid};
        {error, not_found} -> create_pes_server(Executor, Key)
    end.


%% @private
-spec create_pes_server(executor(), key()) -> {ok, pid()} | {error, Reason :: term()}.
create_pes_server(Executor, Key) ->
    try
        case pes_process_manager:start_server(Executor, Key) of
            {ok, undefined} -> acquire_pes_server(Executor, Key);
            {ok, Pid} -> {ok, Pid};
            {error, Reason} -> {error, Reason}
        end
    catch
        Error:CatchReason:Stacktrace ->
            ?error_stacktrace("PES ~p error ~p:~p for executor ~p and key ~p",
                [?FUNCTION_NAME, Error, CatchReason, Executor, Key], Stacktrace),
            {error, CatchReason}
    end.


%% @private
-spec send_request_internal(pid(), request(), timeout()) -> send_response() | {error, Reason :: term()}.
send_request_internal(Pid, Request, Timeout) ->
    case pes_server_utils:can_execute_call() of
        true -> gen_server2:call(Pid, Request, Timeout);
        false -> {error, potential_deadlock}
    end.


%% @private
-spec await(Tag :: term(), pid(), non_neg_integer(), boolean()) -> execution_result() | {error, term()}.
await(Ref, Pid, Timeout, RetryOnFailure) ->
    receive
        {submit_result, Ref, Response} -> Response
    after
        Timeout ->
            case {RetryOnFailure, rpc:call(node(Pid), erlang, is_process_alive, [Pid])} of
                {true, true} -> await(Ref, Pid, Timeout, RetryOnFailure);
                {true, _} -> await(Ref, Pid, Timeout, false); % retry last time to prevent race between
                                                              % answer sending / process terminating
                {_, {badrpc, Reason}} -> {error, Reason};
                _ -> {error, timeout}
            end
    end.


%% @private
-spec await_for_all([execution_promise()]) -> [execution_result() | {error, term()}].
await_for_all([]) ->
    [];
await_for_all([{await, Ref, Pid} | Tail]) ->
    [await(Ref, Pid, ?AWAIT_DEFAULT_TIMEOUT, true) | await_for_all(Tail)];
await_for_all([Other | Tail]) ->
    [Other | await_for_all(Tail)].