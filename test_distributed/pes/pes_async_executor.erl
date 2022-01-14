%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Executor used for testing of PES operating in async mode. The state
%%% is map storing integer that can be changed by requests and data
%%% needed to test internal calls.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_async_executor).
-author("Michal Wrzeszcz").


-behavior(pes_executor_behaviour).


-include("global_definitions.hrl").


%% Callbacks
-export([init/0, terminate/2, get_root_supervisor/0,
    handle_call/2, handle_cast/2, get_mode/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init() ->
    case application:get_env(?CLUSTER_WORKER_APP_NAME, pes_init_should_crash, false) of
        true -> throw(init_error);
        false -> #{value => 0}
    end.


terminate(_Reason, undefined = State) ->
    % State is undefined in pes_server - set error
    application:set_env(?CLUSTER_WORKER_APP_NAME, termination_reason, {error, undefined_state}),
    {ok, State};
terminate(Reason, State) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, termination_reason, Reason),
    case application:get_env(?CLUSTER_WORKER_APP_NAME, pes_should_stop, true) of
        true -> {ok, State};
        false -> {abort, State};
        crash -> throw(termination_error)
    end.


get_root_supervisor() ->
    pes_test_supervisor.


handle_call(get_value, #{value := Value} = State) ->
    {{ok, Value}, State};
handle_call(increment_value, #{value := Value} = State) ->
    {call_ok, State#{value => Value + 1}};
handle_call(wait_and_increment_value, #{value := Value} = State) ->
    timer:sleep(timer:seconds(65)),
    {call_ok, State#{value => Value + 1}};
handle_call(crash_call, _State) ->
    throw(call_error);
handle_call(send_internal_message, State) ->
    pes:self_cast(test_internal_message),
    {call_ok, State};
handle_call({call_key, Key}, State) ->
    Ans = pes:call(?MODULE, Key, get_value),
    {Ans, State};
handle_call({submit_for_key, Key, PidToAnswer}, State) ->
    {await, Ref, _} = pes:submit(?MODULE, Key, get_value),
    {call_ok, State#{await_ref => Ref, notify_pid => PidToAnswer}};
handle_call({await, Promise}, State) ->
    Ans = pes:await(Promise),
    {Ans, State};
handle_call(_Request, State) ->
    {call_ok, State}.


handle_cast(decrement_value, #{value := Value} = State) ->
    State#{value => Value - 1};
handle_cast(crash_cast, _State) ->
    throw(call_error);
handle_cast(test_internal_message, #{value := Value} = State) ->
    State#{value => Value + 100};
handle_cast({Ref, Ans}, #{await_ref := Ref, notify_pid := NotifyPid} = State) ->
    NotifyPid ! {internal_call_ans, Ans},
    maps:without([notify_pid, ref], State);
handle_cast(_Request, State) ->
    State.


get_mode() ->
    async.