%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of this behaviour allows provision of logic to
%%% abstract pool of processes executed ad-hoc (see pes.erl).
%%% For more information see description of callbacks.
%%%
%%%
%%%
%%% TECHNICAL NOTE: This module includes callbacks that customize:
%%% - behaviour of pes_server and pes_server_slave
%%% - behaviour of pes_process_manager.
%%% See docs of above mentioned modules.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_executor_behaviour).
-author("Michal Wrzeszcz").


%%%===================================================================
%%% Callbacks for initialization and termination.
%%% NOTE: termination because of termination_request can be
%%% aborted - abort is ignored for other reasons. New requests can
%%% also interrupt termination initiated by termination_request - in
%%% such a case state returned from terminate function will be used
%%% as input for next callback execution.
%%%
%%% It is also possible to execute calls when handling
%%% termination_request while it is forbidden handling other
%%% termination reasons.
%%%
%%% TECHNICAL NOTE: other termination reasons than termination_request
%%% are related to stopping of supervision tree. Thus, execution of
%%% calls handling these reasons can result in deadlock (supervisor
%%% is blocked until termination is handled in such a case).
%%%===================================================================

-optional_callbacks([terminate/2]).

-callback init() -> pes:executor_state().

-callback terminate(pes:termination_reason(), pes:executor_state()) -> {ok | abort, pes:executor_state()}.


%%%===================================================================
%%% Optional callbacks handling requests.
%%% NOTE: callbacks are optional because only one of them is required
%%% to be provided depending of preferred type of communications
%%% (however it is possible to provide them both).
%%%
%%% TECHNICAL NOTE: These callbacks are executed inside pes_server or
%%% pes_server_slave depending on mode (see pes_server.erl).
%%%===================================================================

-optional_callbacks([handle_call/2, handle_cast/2]).

-callback handle_call(pes:request(), pes:executor_state()) -> {pes:execution_result(), pes:executor_state()}.

-callback handle_cast(pes:request(), pes:executor_state()) -> pes:executor_state().


%%%===================================================================
%%% Optional callback setting mode.
%%% NOTE: if callback is not implemented, sync mode is used.
%%%
%%% TECHNICAL NOTE: mode determines module that handles requests - see
%%% pes_server.erl).
%%%===================================================================

-optional_callbacks([get_mode/0]).

-callback get_mode() -> pes:mode().


%%%===================================================================
%%% Optional callback allowing customization of behaviour
%%% during code change.
%%%===================================================================

-optional_callbacks([code_change/3]).

-callback code_change(OldVsn :: term() | {down, term()}, pes:executor_state(), Extra :: term()) ->
    {ok, pes:executor_state()} | {error, Reason :: term()}.


%%%===================================================================
%%% Optional callbacks allowing customization of processes' tree
%%% structure. It provides name of supervisor that is root of
%%% supervision tree and optionally count of processes that handle
%%% requests. Usually, processes are connected to root supervisor
%%% which name is provided by get_root_supervisor/0 callback.
%%% If count of processes is large, get_server_groups_count/0
%%% callback can be used to divide processes into groups where each
%%% group has own supervisor. Supervisors of groups are connected with
%%% root supervisor in such a case. Usage of more than one supervisor
%%% improves scalability as each process can be started and stopped
%%% dynamically and supervisor is bottleneck for processes start/stop.
%%%
%%% TECHNICAL NOTE: when async mode is chosen, pes_servers are
%%% connected to supervisors while pes_server_slaves are linked to
%%% pes_servers (see pes.erl).
%%%===================================================================

-optional_callbacks([get_servers_count/0, get_server_groups_count/0]).

-callback get_root_supervisor() -> pes_supervisor:name().

-callback get_servers_count() -> pos_integer().

-callback get_server_groups_count() -> pos_integer().