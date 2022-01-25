%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of this behaviour allows provision of application
%%% specific logic to the Partitioned Execution Service (see pes.erl).
%%%
%%% NOTE: It is possible to send requests from the inside of handle_call/2
%%% or handle_cast/2 callback only when working in async mode.
%%% In such a case answer for submit appears as ?SUBMIT_RESULT
%%% (see pes_protocol.hrl) in handle_cast/2 callback.
%%% Synchronous waiting for massage inside handle_call/2
%%% or handle_cast/2 callback could results in deadlock.
%%% Thus, pes:await/1 returns error in such a case.
%%%
%%% TECHNICAL NOTE: These callbacks are called by pes_server,
%%% pes_server_slave and pes_process_manager modules.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_plugin_behaviour).
-author("Michal Wrzeszcz").


%%%===================================================================
%%% Callbacks configuring plug-in
%%%===================================================================

%%%===================================================================
%%% Callbacks allowing customization of processes' tree structure.
%%% It provides name of supervisor that is root of supervision tree
%%% and optionally count of executors that handle requests. Usually,
%%% processes are connected to root supervisor which name is provided
%%% by get_root_supervisor_name/0 callback. If count of processes is
%%% large, get_supervisor_count/0 callback can be used to use more than
%%% one supervisor for executors (each supervisor supervises
%%% get_executor_count() / get_supervisor_count() executors). In
%%% such a case each supervisor is connected with root supervisor.
%%% Usage of more than one supervisor improves scalability as each
%%% executor can be started and stopped dynamically and supervisor
%%% is bottleneck for executor start/stop.
%%%
%%% TECHNICAL NOTE: when async mode is chosen, pes_servers are
%%% connected to supervisors while pes_server_slaves are linked to
%%% pes_servers (see pes.erl).
%%%===================================================================

-optional_callbacks([get_executor_count/0, get_supervisor_count/0]).

-callback get_root_supervisor_name() -> pes_server_supervisor:name().

-callback get_executor_count() -> pos_integer().

-callback get_supervisor_count() -> pos_integer().




%%%===================================================================
%%% Callbacks used by executor
%%%===================================================================

%%%===================================================================
%%% Optional callback setting mode.
%%% NOTE: if callback is not implemented, sync mode is used.
%%%
%%% TECHNICAL NOTE: mode determines module that handles requests - see
%%% pes_server.erl.
%%%===================================================================

-optional_callbacks([get_mode/0]).

-callback get_mode() -> pes:mode().


%%%===================================================================
%%% Callbacks for initialization and termination of executor.
%%% NOTE: termination because of termination_request
%%% (handled by graceful_terminate/1) can be deferred. New requests
%%% can also interrupt termination initiated by termination_request - in
%%% such a case state returned from graceful_terminate/1 function will
%%% be used as input for next callback execution.
%%%
%%% It is also possible to execute calls from the inside of
%%% graceful_terminate/1 function while it is forbidden from the inside
%%% of forced_terminate/1.
%%%
%%% TECHNICAL NOTE: forced_terminate/2 handles termination related to
%%% stopping of supervision tree. Thus, execution of calls from the
%%% inside of forced_terminate/2 might result in deadlock (supervisor
%%% is blocked until termination is handled in such a case) so it is
%%% forbidden.
%%%===================================================================

-optional_callbacks([graceful_terminate/1, forced_terminate/2]).

-callback init() -> pes:executor_state().

% Termination triggered by termination_request
-callback graceful_terminate(pes:executor_state()) -> {ok | defer, pes:executor_state()}.

% Termination related to stopping of supervision tree
-callback forced_terminate(pes:forced_termination_reason(), pes:executor_state()) -> ok.


%%%===================================================================
%%% Optional callbacks handling requests.
%%% NOTE: callbacks are optional because only one of them is required
%%% to be provided depending on preferred type of communication
%%% (however it is possible to provide them both).
%%%
%%% TECHNICAL NOTE: These callbacks are executed inside pes_server or
%%% pes_server_slave depending on mode (see pes_server.erl).
%%%===================================================================

-optional_callbacks([handle_call/2, handle_cast/2]).

-callback handle_call(pes:request(), pes:executor_state()) -> {pes:execution_result(), pes:executor_state()}.

-callback handle_cast(pes:request(), pes:executor_state()) -> pes:executor_state().


%%%===================================================================
%%% Optional callback allowing customization of behaviour
%%% during code change.
%%%===================================================================

-optional_callbacks([code_change/3]).

-callback code_change(OldVsn :: term() | {down, term()}, pes:executor_state(), Extra :: term()) ->
    {ok, pes:executor_state()} | {error, Reason :: term()}.