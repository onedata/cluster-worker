%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Executor implementing only required callbacks of
%%% pes_plugin_behaviour behaviour to be used during ct tests.
%%%
%%% NOTE: handle_call is not required but it is required that at least
%%% one of two: handle_call/2 and handle_call/2 is implemented.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_minimal_executor).
-author("Michal Wrzeszcz").


-behavior(pes_plugin_behaviour).


%% Callbacks
-export([get_root_supervisor_name/0]).
-export([init/0, handle_call/2]).


%%%===================================================================
%%% Callbacks
%%%===================================================================

get_root_supervisor_name() ->
    pes_test_supervisor.


init() ->
    pes_executor_mock:init().


handle_call(Request, State) ->
    pes_executor_mock:handle_call(Request, State).