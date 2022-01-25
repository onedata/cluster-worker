%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions to be used to simulate executor
%%% during tests.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_executor_mock).
-author("Michal Wrzeszcz").


-include("pes_protocol.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/test/assertions.hrl").


%% Callbacks
-export([init/0, graceful_terminate/1, forced_terminate/2,
    handle_call/2, handle_cast/2]).
%% Helper functions
-export([verify_state/3, wait_for_self_cast/0, verify_internal_submit/1]).
-export([set_simulated_termination_manner/1, set_initial_crash/1, clear_termination_reason/0, get_termination_reason/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init() ->
    case application:get_env(?CLUSTER_WORKER_APP_NAME, pes_init_should_crash, false) of
        true -> throw(init_error);
        false -> #{value => 0}
    end.


graceful_terminate(undefined = State) ->
    % For async mode State is stored by pes_server_slave and it is undefined in pes_server. Calling this callback
    % with undefined state means that it was called by pes_server when it was expected to be called by pes_server_slave.
    application:set_env(?CLUSTER_WORKER_APP_NAME, termination_reason, {error, graceful_undefined_state}),
    {ok, State};
graceful_terminate(State) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, termination_reason, graceful_termination_request),
    case application:get_env(?CLUSTER_WORKER_APP_NAME, simulated_termination_manner, terminate_successfully) of
        terminate_successfully ->
            {ok, State};
        defer_termination ->
            {defer, State};
        crash_during_termination ->
            throw(termination_error);
        {interrupt_with_request, Plugin, Key, Request} ->
            pes:submit(Plugin, Key, Request),
            {ok, State}
    end.


forced_terminate(_Reason, undefined = _State) ->
    % For async mode state is stored by pes_server_slave and is undefined in pes_server. Calling this callback
    % with undefined state means that it was called by pes_server when it was expected to be called by pes_server_slave.
    application:set_env(?CLUSTER_WORKER_APP_NAME, termination_reason, {error, forced_with_undefined_state}),
    ok;
forced_terminate(Reason, _State) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, termination_reason, {forced, Reason}),
    case application:get_env(?CLUSTER_WORKER_APP_NAME, simulated_termination_manner, terminate_successfully) of
        crash_during_termination -> throw(termination_error);
        _ -> ok
    end.


handle_call(get_value, #{value := Value} = State) ->
    {{ok, Value}, State};
handle_call(increment_value, #{value := Value} = State) ->
    {ok, State#{value => Value + 1}};
handle_call({increment_value, Change}, #{value := Value} = State) ->
    {ok, State#{value => Value + Change}};
handle_call({wait_and_increment_value, Millis}, #{value := Value} = State) ->
    timer:sleep(Millis),
    {ok, State#{value => Value + 1}};
handle_call({increment_using_self_cast, Change}, State) ->
    pes:self_cast({increment_value, Change}),
    {ok, State};
handle_call({crash_with, Class, Reason}, _State) ->
    erlang:Class(Reason);
handle_call({perform_internal_call_with_key, Module, Key}, State) ->
    Ans = pes:call(Module, Key, get_value),
    {Ans, State};
handle_call({perform_internal_submit_with_key, Module, Key, PidToAnswer}, State) ->
    case pes:submit(Module, Key, get_value) of
        {ok, {Ref, _}} -> {{ok, internal_submit_done}, State#{await_ref => Ref, notify_pid => PidToAnswer}};
        Other -> {Other, State}
    end;
handle_call({perform_internal_await, Promise}, State) ->
    Ans = pes:await(Promise),
    {Ans, State};
handle_call(_Request, State) ->
    {{ok, test_ans}, State}.


handle_cast({get_value, PidToAnswer}, #{value := Value} = State) ->
    PidToAnswer ! {value, Value},
    State;
handle_cast(decrement_value, #{value := Value} = State) ->
    State#{value => Value - 1};
handle_cast({increment_value, Change}, #{value := Value} = State) ->
    State#{value => Value + Change};
handle_cast({increment_using_self_cast, Change, PidToAnswer}, State) ->
    pes:self_cast({increment_value, Change}),
    PidToAnswer ! self_cast_called,
    State;
handle_cast({crash_with, Class, Reason}, _State) ->
    erlang:Class(Reason);
handle_cast(?SUBMIT_RESULT(Ref, Ans), #{await_ref := Ref, notify_pid := NotifyPid} = State) ->
    NotifyPid ! {internal_call_ans, Ans},
    maps:without([notify_pid, await_ref], State);
handle_cast(_Request, State) ->
    State.


%%%===================================================================
%%% Helper functions
%%%===================================================================

verify_state(pes_minimal_executor, Key, ExpectedValue) ->
    ?assertEqual({ok, ExpectedValue}, pes:call(pes_minimal_executor, Key, get_value));
verify_state(pes_async_executor, Key, ExpectedValue) ->
    ?assertEqual({ok, ExpectedValue}, pes:submit_and_await(pes_async_executor, Key, get_value));
verify_state(pes_sync_executor, Key, ExpectedValue) ->
    % pes_sync_executor does not implement handle_call callback - get state with cast
    ?assertEqual(ok, pes:cast(pes_sync_executor, Key, {get_value, self()})),
    ?assertReceivedEqual({value, ExpectedValue}, 5000).


wait_for_self_cast() ->
    % Process that sends self_cast sends message to test process to confirm that self_cast was called
    ?assertReceivedEqual(self_cast_called, 5000).


verify_internal_submit(ExpectedValue) ->
    % Process that handles internal call sends message to test process to confirm that it handled this request
    ?assertReceivedEqual({internal_call_ans, {ok, ExpectedValue}}, 5000).


set_simulated_termination_manner(TerminationManner) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, simulated_termination_manner, TerminationManner).


set_initial_crash(InitShouldCrash) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_init_should_crash, InitShouldCrash).


clear_termination_reason() ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, termination_reason, undefined).


get_termination_reason() ->
    application:get_env(?CLUSTER_WORKER_APP_NAME, termination_reason, undefined).