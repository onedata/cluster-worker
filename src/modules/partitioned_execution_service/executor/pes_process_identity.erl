%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper using process memory to save information about pes processes,
%%% which is used to verify if internal PES calls are allowed.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_process_identity).
-author("Michal Wrzeszcz").


%% API
-export([mark_as_pes_server/0, mark_as_pes_server_slave/0, mark_as_terminating_pes_process/0]).
-export([ensure_eligible_for_sync_communication/0, ensure_eligible_for_await/0, ensure_eligible_for_self_cast/0]).


-define(MARKER, pes_marker).

-define(PES_SERVER, pes_server).
-define(PES_SERVER_SLAVE, pes_server_slave).
-define(TERMINATING_PES_PROCESS, terminating_pes_process).
-define(NOT_PES_PROCESS, not_pes_process).


%%%===================================================================
%%% API
%%%===================================================================

-spec mark_as_pes_server() -> ok.
mark_as_pes_server() ->
    put(?MARKER, ?PES_SERVER),
    ok.


-spec mark_as_pes_server_slave() -> ok.
mark_as_pes_server_slave() ->
    put(?MARKER, ?PES_SERVER_SLAVE),
    ok.


-spec mark_as_terminating_pes_process() -> ok.
mark_as_terminating_pes_process() ->
    put(?MARKER, ?TERMINATING_PES_PROCESS),
    ok.


-spec ensure_eligible_for_sync_communication() -> ok | no_return().
ensure_eligible_for_sync_communication() ->
    case get(?MARKER) of
        ?PES_SERVER -> error(potential_deadlock);
        ?TERMINATING_PES_PROCESS -> error(potential_deadlock);
        _ -> ok
    end.


-spec ensure_eligible_for_await() -> ok | no_return().
ensure_eligible_for_await() ->
    case get(?MARKER) of
        ?PES_SERVER -> error(potential_deadlock);
        ?PES_SERVER_SLAVE -> error(potential_deadlock);
        ?TERMINATING_PES_PROCESS -> error(potential_deadlock);
        _ -> ok
    end.


-spec ensure_eligible_for_self_cast() -> ok | no_return().
ensure_eligible_for_self_cast() ->
    case get(?MARKER) of
        ?PES_SERVER -> ok;
        ?PES_SERVER_SLAVE -> ok;
        ?TERMINATING_PES_PROCESS -> ok;
        _ -> error(?NOT_PES_PROCESS)
    end.