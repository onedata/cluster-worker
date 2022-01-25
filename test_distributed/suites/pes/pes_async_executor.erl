%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Executor used for testing of PES operating in async mode.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_async_executor).
-author("Michal Wrzeszcz").


-behavior(pes_plugin_behaviour).


%% Callbacks
-export([get_root_supervisor_name/0]).
-export([get_mode/0, init/0, graceful_terminate/1, forced_terminate/2,
    handle_call/2, handle_cast/2]).


%%%===================================================================
%%% Callbacks
%%%===================================================================

get_root_supervisor_name() ->
    pes_test_supervisor.


get_mode() ->
    async.


init() ->
    pes_executor_mock:init().


graceful_terminate(State) ->
    pes_executor_mock:graceful_terminate(State).


forced_terminate(Reason, State) ->
    pes_executor_mock:forced_terminate(Reason, State).


handle_call(Request, State) ->
    pes_executor_mock:handle_call(Request, State).


handle_cast(Request, State) ->
    pes_executor_mock:handle_cast(Request, State).