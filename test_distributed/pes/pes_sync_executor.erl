%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Executor used for testing of PES operating in sync mode. The state
%%% is a number that can be incremented.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_sync_executor).
-author("Michal Wrzeszcz").


-behavior(pes_executor_behaviour).


-include("global_definitions.hrl").


%% Callbacks
-export([init/0, terminate/2,
    get_root_supervisor/0, get_server_groups_count/0,
    handle_cast/2, get_servers_count/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init() ->
    0.


terminate(Reason, State) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, termination_reason, Reason),
    case application:get_env(?CLUSTER_WORKER_APP_NAME, pes_should_stop, true) of
        true -> {ok, State};
        false -> {abort, State};
        crash -> throw(termination_error)
    end.


get_root_supervisor() ->
    pes_test_supervisor.


get_server_groups_count() ->
    5.


handle_cast({get_value, PidToAnswer}, State) ->
    PidToAnswer ! {value, State},
    State;
handle_cast(decrement_value, State) ->
    State - 1;
handle_cast(crash_cast, _State) ->
    throw(call_error);
handle_cast({send_internal_message, PidToAnswer}, State) ->
    pes:self_cast(test_internal_message),
    PidToAnswer ! internal_message_sent,
    State;
handle_cast(test_internal_message, State) ->
    State + 100;
handle_cast(_Request, State) ->
    State.


get_servers_count() ->
    50.