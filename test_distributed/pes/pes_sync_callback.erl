%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Callback module used for testing of partition execution service
%%% operating in sync mode. The state of callback is a number that
%%% can be incremented.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_sync_callback).
-author("Michal Wrzeszcz").


-behavior(pes_callback).


-include("global_definitions.hrl").


%% Callbacks
-export([init/0, terminate/2, supervisors_namespace/0, get_supervisor_name/1,
    handle_cast/2, handle_info/2, hash_key/1]).


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


supervisors_namespace() ->
    lists:map(fun(KeyHash) -> get_supervisor_name(KeyHash) end, lists:seq(1, 5)).


get_supervisor_name(KeyHash) ->
    list_to_atom("test_supervisor" ++ integer_to_list(KeyHash rem 5)).


handle_cast({get_value, PidToAnswer}, State) ->
    PidToAnswer ! {value, State},
    State;
handle_cast(decrement_value, State) ->
    State - 1;
handle_cast(crash_cast, _State) ->
    throw(call_error);
handle_cast({send_internal_message, PidToAnswer}, State) ->
    self() ! test_internal_message,
    PidToAnswer ! internal_message_sent,
    State;
handle_cast(_Request, State) ->
    State.


handle_info(increment_value, State) ->
    State + 1;
handle_info(crash_send, _State) ->
    throw(send_error);
handle_info(test_internal_message, State) ->
    State + 100;
handle_info(_Request, State) ->
    State.


hash_key(Key) ->
    Key rem 50.