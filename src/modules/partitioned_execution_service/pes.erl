%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface of partitioned execution service - PES. Service handles requests
%%% associated with arbitrary keys using callbacks provided by plug-ins
%%% implementing pes_plugin_behaviour behaviour. It is guaranteed that
%%% requests associated with the same key in the scope of the same plug-in
%%% are processed sequentially. To achieve this effect PES uses processes
%%% created ad-hoc call executors. Different processes are created for
%%% different plug-ins even if they use the same key. Lifecycle of executor
%%% is as follows:
%%% 1. pes_plugin_behaviour:init/0 is used to create initial state provided
%%%    to other callbacks of executor.
%%% 2. Process is handling requests using pes_plugin_behaviour.
%%% 3. Process is terminating by lack of activity for some period of
%%%    time, by external graceful_termination_request or by supervisor
%%%    (supervision tree is stopped when node is terminating).
%%%    3a. If termination is deferred (see pes_plugin_behaviour:terminate/2),
%%%       executor returns to handling requests (2.) until next
%%%       termination attempt.
%%% The service for plug-in is started using the start_link/1 function.
%%% The stop/1 function is used to stop the service for plug-in.
%%% Root supervisor has to be alive before start_link/1 is called.
%%%
%%% Single executor can handle requests connected to several keys but it
%%% is guaranteed that two parallel requests connected with the same key
%%% are always handled by the same executor. Executor can be single process
%%% or pair of processes depending on processing mode (it can process
%%% requests synchronously or asynchronously). Each process is connected to
%%% supervisor directly or via link to other process connected to supervisor.
%%%
%%% TECHNICAL NOTE:
%%% If asynchronous processing of calls is required, the process is linked
%%% with slave process. Then, it acts as a buffer for requests while
%%% processing takes place in slave process (see pes_server.erl). Such
%%% behaviour allows for calls from one process to another without risk
%%% of deadlock.
%%%
%%% Process and supervisor connected with each key are chosen automatically
%%% using get_executor_count and get_supervisor_count callbacks
%%% (see pes_process_manager.erl and pes_plugin_behaviour.erl).
%%%
%%% Exemplary PES can be depicted as follows (`pes_sup_tree_supervisor 1` and `pes_server_supervisor 3` are root
%%% supervisors for plug-ins - their linking to application supervision tree is not depicted as it is not
%%% controlled by PES and depends on plug-in's creator choice):
%%%
%%%                                                        +-------------------------+
%%%                                                        |   pes_process_manager   |      NOTE: single library is used
%%%    NOTE: some pes_server_supervisors may               +-------------------------+            to route requests
%%%          not be used at the moment but                        |            .
%%%          they are not stopped until                           .      .     |
%%%          plug-in stop                                         |      .     .
%%%                                                               .      .     |
%%%           +---------------------------+                       |      .     .
%%%           | pes_sup_tree_supervisor 1 |                       .      .     |    NOTE: If plug-in uses only one
%%%           +---------------------------+                       |      .     .          supervisor,
%%%                   /              \                            .      .     |          pes_sup_tree_supervisor
%%%                (link)           (link)                        |      .     .          is not required
%%%                 /                  \                          .      .     |
%%%                /                    \                         |      .     .
%%%  +-------------------------+   +-------------------------+    .      .      \      +-------------------------+
%%%  | pes_server_supervisor 1 |   | pes_server_supervisor 2 |    |      .       .     | pes_server_supervisor 3 |
%%%  +-------------------------+   +-------------------------+    .      .        \    +-------------------------+
%%%               |                         \                    /       .         .            |
%%%               |                          \             (routing      .       (routing       |
%%%            (link)                      (link)           request)     .        request)      |
%%%               |                            \              .          .            \      (link)
%%%               |                             \            /           .             .        |
%%%    +------------------------+      +------------------------+        .         +------------------------+
%%%    |       EXECUTOR 1       |      |       EXECUTOR 2       |        .         |       EXECUTOR 3       |
%%%    |                        |      |                        |        .         |                        |
%%%    | +--------------------+ |      | +--------------------+ |        .         | +--------------------+ |
%%%    | |    pes_server 1    | |      | |    pes_server 2    | |        .         | |    pes_server 3    | |
%%%    | +--------------------+ |      | +--------------------+ |        .         | +--------------------+ |
%%%    |           |            |      |           |            |        .         |                        |
%%%    |         (link)         |      |         (link)         |        .         |                        |
%%%    |           |            |      |           |            |        .         |                        |
%%%    | +--------------------+ |      | +--------------------+ |        .         |                        |
%%%    | | pes_server_slave 1 | |      | | pes_server_slave 2 | |        .         |                        |
%%%    | +--------------------+ |      | +--------------------+ |        .         |                        |
%%%    +------------------------+      +------------------------+        .         +------------------------+
%%%                                                                      .
%%%                             PLUG-IN_1                                .                 PLUG-IN_2
%%%                                                                      .
%%% @end
%%%-------------------------------------------------------------------
-module(pes).
-author("Michal Wrzeszcz").


-include("pes_protocol.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([start_link/1, stop/1]).
-export([start_root_supervisor/1, get_root_supervisor_flags/1, get_root_supervisor_child_specs/1]).
-export([call/3, call/4]).
-export([submit/3, submit/4, await/1, await/2, submit_and_await/3, submit_and_await/4]).
-export([acknowledged_cast/3, acknowledged_cast/4]).
-export([self_cast/1, self_cast_after/2]).
-export([cast/3, cast/4]).
-export([multi_call/2, multi_submit/2, multi_submit_and_await/2, multi_acknowledged_cast/2]).

%% Test API
-export([send_to_all/2]).


-type key() :: term().
-type request() :: term().
-type request_with_delivery_check_options() :: #{
    timeout => timeout(),
    attempts => non_neg_integer(),
    ensure_executor_alive => boolean()
}.
-type cast_options() :: #{
    ensure_executor_alive => boolean()
}.
-type execution_result() :: ok | {ok, term()} | {error, term()}.
-type execution_promise() :: {Tag :: term(), pid()}.
-type mode() :: sync | async.

-type message_with_delivery_check() :: ?PES_CALL(request()) | ?PES_SUBMIT(request()) | ?PES_ACKNOWLEDGED_CAST(request()).
-type communication_error() :: ?ERROR_INTERNAL_SERVER_ERROR | ?ERROR_TIMEOUT.
-type pes_framework_error() :: ignored | communication_error().

-type graceful_termination_request() :: graceful_termination_request.
% Reason for termination forced by supervisor (termination that is not a result of graceful_termination_request)
-type forced_termination_reason() :: normal | shutdown | {shutdown, term()} | term().
-type plugin() :: module().
-type executor_state() :: term().

-export_type([key/0, request/0, execution_result/0, execution_promise/0, mode/0, message_with_delivery_check/0,
    graceful_termination_request/0, forced_termination_reason/0, plugin/0, executor_state/0]).


-define(SEND_DEFAULT_TIMEOUT, infinity).
-define(SEND_DEFAULT_ATTEMPTS, 1).
-define(AWAIT_DEFAULT_TIMEOUT, infinity).
-define(AWAIT_CHECK_PERIOD, 30000).


%%%===================================================================
%%% API - init and teardown
%%% NOTE: process that calls start_link should be alive as long as PES is used for plug-in.
%%% It should call stop when PES is not needed anymore.
%%% NOTE: Root supervisor has to be alive before start_link/1 is called.
%%%===================================================================


-spec start_link(plugin()) -> ok.
start_link(Plugin) ->
    pes_process_manager:init_for_plugin(Plugin).


-spec stop(plugin()) -> ok.
stop(Plugin) ->
    case send_to_all(Plugin, graceful_termination_request) of
        [] ->
            pes_process_manager:teardown_for_plugin(Plugin);
        _ ->
            timer:sleep(100),
            % Try until all processes are terminated. graceful_termination_request assumes graceful stopping of
            % processes and nothing should be forced. When application is stopping because of errors, supervisors
            % will force termination of processes and this loop will end.
            stop(Plugin)
    end.


%%%===================================================================
%%% API to start root supervisor directly (start_supervisor/2) or
%%% provide spec to node_manager_plugin (get_root_supervisor_flags/1 and
%%% get_root_supervisor_child_specs/1)
%%%===================================================================

-spec start_root_supervisor(plugin()) -> {ok, pid()} | ?ERROR_INTERNAL_SERVER_ERROR.
start_root_supervisor(Plugin) ->
    SupervisorModule = resolve_root_supervisor_module(Plugin),
    case SupervisorModule:start_link(pes_plugin:get_root_supervisor_name(Plugin)) of
        {ok, Pid} ->
            {ok, Pid};
        Error ->
            ?error("PES ~p error for plug-in ~p: ~p",
                [?FUNCTION_NAME, Plugin, Error]),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


-spec get_root_supervisor_flags(plugin()) -> supervisor:sup_flags().
get_root_supervisor_flags(Plugin) ->
    SupervisorModule = resolve_root_supervisor_module(Plugin),
    SupervisorModule:sup_flags().


-spec get_root_supervisor_child_specs(plugin()) -> [supervisor:child_spec()].
get_root_supervisor_child_specs(Plugin) ->
    SupervisorModule = resolve_root_supervisor_module(Plugin),
    SupervisorModule:child_specs().


%%%===================================================================
%%% API - functions with guarantee that request will be processed.
%%% They are protected from races between request sending to process
%%% and process termination.
%%% Note: not all functions are supported for all modes:
%%%   - call - dedicated for sync mode
%%%   - submit/await - dedicated for async mode
%%%   - acknowledged_cast/self_cast - works for sync and async mode
%%%===================================================================

-spec call(plugin(), key(), request()) -> execution_result() | pes_framework_error().
call(Plugin, Key, Request) ->
    call(Plugin, Key, Request, #{}).


%%--------------------------------------------------------------------
%% @doc
%% Sends request and hangs until the answer is returned.
%% @end
%%--------------------------------------------------------------------
-spec call(plugin(), key(), request(), request_with_delivery_check_options()) ->
    execution_result() | pes_framework_error().
call(Plugin, Key, Request, Options) ->
    send_request_and_check_delivery(Plugin, Key, ?PES_CALL(Request), Options).


-spec submit(plugin(), key(), request()) -> {ok, execution_promise()} | pes_framework_error().
submit(Plugin, Key, Request) ->
    submit(Plugin, Key, Request, #{}).


%%--------------------------------------------------------------------
%% @doc
%% Sends request and hangs only until appearance of confirmation that request is put in
%% process queue. Returns promise that can be passed to await/1 function to return answer.
%% NOTE: promise can be used only by process that executed submit function. It will
%% not work properly for other processes.
%% @end
%%--------------------------------------------------------------------
-spec submit(plugin(), key(), request(), request_with_delivery_check_options()) ->
    {ok, execution_promise()} | pes_framework_error().
submit(Plugin, Key, Request, Options) ->
    send_request_and_check_delivery(Plugin, Key, ?PES_SUBMIT(Request), Options).


-spec await(execution_promise()) -> execution_result() | communication_error().
await(Promise) ->
    await(Promise, ?AWAIT_DEFAULT_TIMEOUT).


-spec await(execution_promise(), timeout()) -> execution_result() | communication_error().
await({Ref, Pid}, Timeout) ->
    pes_process_identity:ensure_eligible_for_await(),
    await(Ref, Pid, Timeout, true).


-spec submit_and_await(plugin(), key(), request()) -> execution_result() | pes_framework_error().
submit_and_await(Plugin, Key, Request) ->
    submit_and_await(Plugin, Key, Request, #{}).


-spec submit_and_await(plugin(), key(), request(), request_with_delivery_check_options()) ->
    execution_result() | pes_framework_error().
submit_and_await(Plugin, Key, Request, Options) ->
    case submit(Plugin, Key, Request, Options) of
        {ok, Promise} -> await(Promise);
        {error, Error} -> {error, Error}
    end.


-spec acknowledged_cast(plugin(), key(), request()) -> ok | pes_framework_error().
acknowledged_cast(Plugin, Key, Request) ->
    acknowledged_cast(Plugin, Key, Request, #{}).


%%--------------------------------------------------------------------
%% @doc
%% Sends request and hangs only until appearance of confirmation that request is put in process queue.
%% Does not provide answer or promise (no answer is sent to caller when request processing ends).
%% @end
%%--------------------------------------------------------------------
-spec acknowledged_cast(plugin(), key(), request(), request_with_delivery_check_options()) -> ok | pes_framework_error().
acknowledged_cast(Plugin, Key, Request, Options) ->
    send_request_and_check_delivery(Plugin, Key, ?PES_ACKNOWLEDGED_CAST(Request), Options).


%%--------------------------------------------------------------------
%% @doc
%% Cast function to be used from the executor handler
%% (it is faster as there is no need to resolve key).
%% @end
%%--------------------------------------------------------------------
-spec self_cast(request()) -> ok.
self_cast(Request) ->
    pes_process_identity:ensure_eligible_for_self_cast(),
    self() ! ?PES_SELF_CAST(Request),
    ok.


-spec self_cast_after(request(), non_neg_integer()) -> TimerRef :: reference().
self_cast_after(Request, Time) ->
    pes_process_identity:ensure_eligible_for_self_cast(),
    erlang:send_after(Time, self(), ?PES_SELF_CAST(Request)).


%%%===================================================================
%%% API - requests with no guarantees of provisioning to pes_server.
%%% NOTE: these functions checks if process exist and create it if not.
%%% However, they are not protected from races between sending and
%%% process termination.
%%%===================================================================

-spec cast(plugin(), key(), request()) -> ok | ignored | ?ERROR_INTERNAL_SERVER_ERROR.
cast(Plugin, Key, Request) ->
    cast(Plugin, Key, Request, #{}).


-spec cast(plugin(), key(), request(), cast_options()) -> ok | ignored | ?ERROR_INTERNAL_SERVER_ERROR.
cast(Plugin, Key, Request, #{ensure_executor_alive := false}) ->
    case pes_process_manager:get_server_if_initialized(Plugin, Key) of
        {ok, Pid} -> pes_server:cast(Pid, ?PES_CAST(Request));
        not_alive -> ignored
    end;
cast(Plugin, Key, Request, _Options) ->
    case acquire_pes_server(Plugin, Key) of
        {ok, Pid} -> pes_server:cast(Pid, ?PES_CAST(Request));
        {error, Reason} -> {error, Reason}
    end.


%%%===================================================================
%%% API - sending requests to all processes
%%%===================================================================

-spec multi_call(plugin(), request()) -> [execution_result() | communication_error()].
multi_call(Plugin, Msg) ->
    send_to_all(Plugin, ?PES_CALL(Msg)).


-spec multi_submit(plugin(), request()) -> [{ok, execution_promise()} | communication_error()].
multi_submit(Plugin, Msg) ->
    send_to_all(Plugin, ?PES_SUBMIT(Msg)).


-spec multi_submit_and_await(plugin(), request()) -> [execution_result() | communication_error()].
multi_submit_and_await(Plugin, Msg) ->
    pes_process_identity:ensure_eligible_for_await(),
    lists:map(fun
        ({ok, {Ref, Pid}}) -> await(Ref, Pid, ?AWAIT_DEFAULT_TIMEOUT, true);
        ({error, _} = Error) -> Error
    end, multi_submit(Plugin, Msg)).


-spec multi_acknowledged_cast(plugin(), request()) -> [ok | communication_error()].
multi_acknowledged_cast(Plugin, Msg) ->
    send_to_all(Plugin, ?PES_ACKNOWLEDGED_CAST(Msg)).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%%--------------------------------------------------------------------
%% @doc
%% Sends request and checks if request is put in process queue.
%% @end
%%--------------------------------------------------------------------
-spec send_request_and_check_delivery(plugin(), key(), message_with_delivery_check(),
    request_with_delivery_check_options()) -> ok | execution_result() | {ok, execution_promise()} | pes_framework_error().
send_request_and_check_delivery(_Plugin, _Key, _Message, #{attempts := 0}) ->
    ?ERROR_TIMEOUT;
send_request_and_check_delivery(Plugin, Key, Message, #{
    ensure_executor_alive := false
} = Options) ->
    Timeout = maps:get(timeout, Options, ?SEND_DEFAULT_TIMEOUT),
    Attempts = maps:get(attempts, Options, ?SEND_DEFAULT_ATTEMPTS),

    case pes_process_manager:get_server_if_initialized(Plugin, Key) of
        {ok, Pid} ->
            try
                send_request_internal(Pid, Message, Timeout)
            catch
                _:{noproc, _} ->
                    ignored;
                exit:{normal, _} ->
                    ignored;
                _:{timeout, _} ->
                    send_request_and_check_delivery(Plugin, Key, Message, Options#{attempts => Attempts - 1});
                Error:Reason:Stacktrace when Reason =/= potential_deadlock ->
                    ?error_stacktrace("PES send_request error ~p:~p for plug-in ~p, key ~p and options ~p",
                        [Error, Reason, Plugin, Key, Options], Stacktrace),
                    ?ERROR_INTERNAL_SERVER_ERROR
            end;
        not_alive ->
            ignored
    end;
send_request_and_check_delivery(Plugin, Key, Message, Options) ->
    Timeout = maps:get(timeout, Options, ?SEND_DEFAULT_TIMEOUT),
    Attempts = maps:get(attempts, Options, ?SEND_DEFAULT_ATTEMPTS),

    case acquire_pes_server(Plugin, Key) of
        {ok, Pid} ->
            try
                send_request_internal(Pid, Message, Timeout)
            catch
                _:{noproc, _} ->
                    pes_process_manager:delete_key_to_server_mapping(Plugin, Key, Pid),
                    % Race with process termination - do not decrement attempts
                    send_request_and_check_delivery(Plugin, Key, Message, Options);
                exit:{normal, _} ->
                    pes_process_manager:delete_key_to_server_mapping(Plugin, Key, Pid),
                    % Race with process termination - do not decrement attempts
                    send_request_and_check_delivery(Plugin, Key, Message, Options);
                _:{timeout, _} ->
                    send_request_and_check_delivery(Plugin, Key, Message, Options#{attempts => Attempts - 1});
                Error:Reason:Stacktrace when Reason =/= potential_deadlock ->
                    ?error_stacktrace("PES send_request error ~p:~p for plug-in ~p, key ~p and options ~p",
                        [Error, Reason, Plugin, Key, Options], Stacktrace),
                    ?ERROR_INTERNAL_SERVER_ERROR
            end;
        {error, Reason} ->
            {error, Reason} % Error starting process - do not try again
    end.


%% @private
-spec send_to_all(plugin(), message_with_delivery_check() | graceful_termination_request()) ->
    [ok | execution_result() | {ok, execution_promise()} | communication_error()].
send_to_all(Plugin, Message) ->
    pes_process_identity:ensure_eligible_for_sync_communication(),
    lists:reverse(pes_process_manager:fold_servers(Plugin, fun(Pid, Acc) ->
        try
            [pes_server:call(Pid, Message, ?SEND_DEFAULT_TIMEOUT) | Acc]
        catch
            _:{noproc, _} ->
                Acc; % Ignore terminated process
            exit:{normal, _} ->
                Acc; % Ignore terminated process
            _:{timeout, _} ->
                [?ERROR_TIMEOUT | Acc];
            Error:Reason:Stacktrace ->
                ?error_stacktrace("PES call error ~p:~p for plug-in ~p and pid ~p",
                    [Error, Reason, Plugin, Pid], Stacktrace),
                [?ERROR_INTERNAL_SERVER_ERROR | Acc]
        end
    end, [])).


%% @private
-spec acquire_pes_server(plugin(), key()) -> {ok, pid()} | ?ERROR_INTERNAL_SERVER_ERROR.
acquire_pes_server(Plugin, Key) ->
    case pes_process_manager:get_server(Plugin, Key) of
        {ok, Pid} -> {ok, Pid};
        not_alive -> create_pes_server(Plugin, Key)
    end.


%% @private
-spec create_pes_server(plugin(), key()) -> {ok, pid()} | ?ERROR_INTERNAL_SERVER_ERROR.
create_pes_server(Plugin, Key) ->
    try
        case pes_process_manager:start_server(Plugin, Key) of
            {ok, undefined} ->
                acquire_pes_server(Plugin, Key);
            {ok, Pid} ->
                {ok, Pid};
            {error, Reason} ->
                ?error("PES ~p error for plug-in ~p and key ~p: ~p", [?FUNCTION_NAME, Plugin, Key, Reason]),
                ?ERROR_INTERNAL_SERVER_ERROR
        end
    catch
        Error:CatchReason:Stacktrace ->
            ?error_stacktrace("PES ~p error ~p:~p for plug-in ~p and key ~p",
                [?FUNCTION_NAME, Error, CatchReason, Plugin, Key], Stacktrace),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


%% @private
-spec send_request_internal(pid(), message_with_delivery_check(), timeout()) ->
    ok | execution_result() | {ok, execution_promise()}.
send_request_internal(Pid, Message, Timeout) ->
    pes_process_identity:ensure_eligible_for_sync_communication(),
    pes_server:call(Pid, Message, Timeout).


%% @private
-spec await(Tag :: term(), pid(), timeout(), boolean()) -> execution_result() | communication_error().
await(Tag, Pid, Timeout, RetryOnFailure) ->
    {WaitingTime, NextTimeout} = case Timeout of
        infinity -> {?AWAIT_CHECK_PERIOD, infinity};
        _ when Timeout > ?AWAIT_CHECK_PERIOD -> {?AWAIT_CHECK_PERIOD, Timeout - ?AWAIT_CHECK_PERIOD};
        _ -> {Timeout, 0}
    end,
    receive
        ?SUBMIT_RESULT(Tag, Response) -> Response
    after
        WaitingTime ->
            case {NextTimeout, RetryOnFailure, erpc:call(node(Pid), erlang, is_process_alive, [Pid])} of
                {0, _, _} ->
                    ?ERROR_TIMEOUT;
                {_, true, true} ->
                    await(Tag, Pid, NextTimeout, RetryOnFailure);
                {_, true, _} ->
                    await(Tag, Pid, NextTimeout, false); % retry last time to prevent race between
                                                         % answer sending / process terminating
                {_, _, false} ->
                    ?error("PES executor ~p is not alive when awaiting for tag ~p", [Pid, Tag]),
                    ?ERROR_INTERNAL_SERVER_ERROR;
                {_, _, {badrpc, Reason}} ->
                    ?error("PES ~p badrpc for tag ~p and pid ~p: ~p", [?FUNCTION_NAME, Tag, Pid, Reason]),
                    ?ERROR_INTERNAL_SERVER_ERROR
            end
    end.


%% @private
-spec resolve_root_supervisor_module(plugin()) -> module().
resolve_root_supervisor_module(Plugin) ->
    case pes_plugin:get_supervisor_count(Plugin) of
        1 -> pes_server_supervisor;
        _ -> pes_sup_tree_supervisor
    end.