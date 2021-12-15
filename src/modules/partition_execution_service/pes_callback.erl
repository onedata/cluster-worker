%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Callback allowing customization of partition execution service behaviour.
%%% For more information see description of callbacks.
%%%
%%%
%%%
%%% TECHNICAL NOTE: This module includes callbacks that customize:
%%% - behaviour of pes_server and pes_server_slave
%%% - behaviour of pes_router.
%%% See docs of above mentioned modules.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_callback).
-author("Michal Wrzeszcz").


%%%===================================================================
%%% Callbacks for state initialization and termination.
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

-callback init() -> pes:callback_state().

-callback terminate(pes:termination_reason(), pes:callback_state()) -> {ok | abort, pes:callback_state()}.


%%%===================================================================
%%% Callbacks providing names of supervisors used to connect each
%%% process that handles request to appropriate supervisor,
%%%
%%% NOTE: if supervisors_namespace callback returns more than one name
%%% get_supervisor_name callback has to be implemented to define list
%%% of key_hashes (processes are identified by hash) connected to each
%%% supervisor. Otherwise, all processes will be connected to first
%%% supervisor returned by supervisors_namespace and other supervisors
%%% will be unused.
%%%
%%% TECHNICAL NOTE: when async mode is chosen, pes_servers are
%%% connected to supervisors while pes_server_slaves are linked to
%%% pes_servers (see pes.erl).
%%%===================================================================

-optional_callbacks([get_supervisor_name/1]).

-callback supervisors_namespace() -> [pes_supervisor:name()].

-callback get_supervisor_name(pes:key_hash()) -> pes_supervisor:name().

%%%===================================================================
%%% Optional callbacks handling requests.
%%% NOTE: callbacks are optional because only one of them is required
%%% to be provided depending of preferred type of communications
%%% (however it is possible to provide them all).
%%%
%%% TECHNICAL NOTE: These callbacks are executed inside pes_server or
%%% pes_server_slave depending on mode (see pes_server.erl).
%%%===================================================================

-optional_callbacks([handle_call/2, handle_cast/2, handle_info/2]).

-callback handle_call(pes:request(), pes:callback_state()) -> {pes:response(), pes:callback_state()}.

-callback handle_cast(pes:request(), pes:callback_state()) -> pes:callback_state().

-callback handle_info(pes:request(), pes:callback_state()) -> pes:callback_state().


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

-callback code_change(OldVsn :: term() | {down, term()}, pes:callback_state(), Extra :: term()) ->
    {ok, pes:callback_state()} | {error, Reason :: term()}.


%%%===================================================================
%%% Optional callback allowing customization of count of processes
%%% that handle requests.
%%%
%%% TECHNICAL NOTE: this callback determines ranges of keys handled
%%% by each pes_server - see pes_router.erl. As pes_servers are started
%%% for hashes (not keys), size of hash space is equal to maximum
%%% number of pes_servers.
%%%===================================================================

-optional_callbacks([hash_key/1]).

-callback hash_key(pes:key()) -> pes:key_hash().