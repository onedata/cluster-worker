%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Callback module implementing only required callbacks of
%%% pes_callback behaviour to be used during ct tests.
%%% The state of callback is a number that can be incremented.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_minimal_callback).
-author("Michal Wrzeszcz").


-behavior(pes_callback).


-include("global_definitions.hrl").


%% Callbacks
-export([init/0, supervisors_namespace/0, handle_call/2]).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init() ->
    case application:get_env(?CLUSTER_WORKER_APP_NAME, pes_init_should_crash, false) of
        true -> throw(init_error);
        false -> 0
    end.


supervisors_namespace() ->
    [pes_test_supervisor].


handle_call(get_value, State) ->
    {{ok, State}, State};
handle_call(increment_value, State) ->
    {call_ok, State + 1};
handle_call(wait_and_increment_value, State) ->
    timer:sleep(timer:seconds(65)),
    {call_ok, State + 1};
handle_call(crash_call, _State) ->
    throw(call_error);
handle_call({call_key, Key}, State) ->
    Ans = pes:sync_call(?MODULE, Key, get_value),
    {Ans, State};
handle_call({async_call_key, Key}, State) ->
    Ans = pes:async_call(?MODULE, Key, get_value),
    {Ans, State};
handle_call({wait, Promise}, State) ->
    Ans = pes:wait(Promise),
    {Ans, State};
handle_call(_Request, State) ->
    {call_ok, State}.