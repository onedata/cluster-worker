%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Executor used for testing of PES operating in sync mode.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_sync_executor).
-author("Michal Wrzeszcz").


-behavior(pes_plugin_behaviour).


%% Callbacks
-export([get_root_supervisor_name/0, get_executor_count/0, get_supervisor_count/0]).
-export([init/0, graceful_terminate/1, forced_terminate/2, handle_cast/2]).


%%%===================================================================
%%% Callbacks
%%%===================================================================

get_root_supervisor_name() ->
    pes_test_supervisor.


get_executor_count() ->
    50.


get_supervisor_count() ->
    5.


init() ->
    pes_executor_mock:init().


graceful_terminate(State) ->
    pes_executor_mock:graceful_terminate(State).


forced_terminate(Reason, State) ->
    pes_executor_mock:forced_terminate(Reason, State).


handle_cast(Request, State) ->
    pes_executor_mock:handle_cast(Request, State).