%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements a simple worker logic used to handle
%%% starting and stopping of listeners. The purpose is correlate
%%% the listeners start/stop with supervision tree setup and teardown.
%%% @end
%%%--------------------------------------------------------------------
-module(listener_manager_worker).
-author("Lukasz Opiola").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).


%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

-spec init(Args :: term()) ->
    {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init([Listeners, BeforeListenersStartCallback, AfterListenersStopCallback]) ->
    BeforeListenersStartCallback(),

    lists:foreach(fun(Module) ->
        ok = erlang:apply(Module, start, [])
    end, Listeners),

    {ok, #{
        listeners => Listeners,
        after_listeners_stop_callback => AfterListenersStopCallback
    }}.


-spec handle(ping | healthcheck | {init_models, list()}) -> pong | ok.
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle(Request) ->
    ?log_bad_request(Request).


-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    Listeners = worker_host:state_get(?MODULE, listeners),
    ?info("Stopping listeners..."),
    Success = lists:all(fun(Module) ->
        try erlang:apply(Module, stop, []) of
            ok -> true;
            {error, _} -> false
        catch Class:Reason ->
            ?warning_stacktrace("Failed to stop listener ~w - ~w:~p", [Module, Class, Reason]),
            false
        end
    end, Listeners),

    case Success of
        true -> ?info("All listeners stopped");
        false -> ?warning("Some listeners could not be stopped, ignoring")
    end,

    AfterListenersStopCallback = worker_host:state_get(?MODULE, after_listeners_stop_callback),
    AfterListenersStopCallback().

