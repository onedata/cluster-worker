%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements a simple worker logic used to handle
%%% starting and stopping of listeners. The purpose is to correlate
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

-define(CALL_PLUGIN(Fun, Args), plugins:apply(node_manager_plugin, Fun, Args)).


%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

-spec init(Args :: term()) ->
    {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init([]) ->
    apply_before_listeners_start_procedures(),

    lists:foreach(fun(Module) ->
        ok = erlang:apply(Module, start, [])
    end, listeners()),

    {ok, #{}}.


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
    journal_logger:log("Initiating graceful stop procedures"),
    ?info("Stopping listeners..."),
    Success = lists:all(fun(Module) ->
        try erlang:apply(Module, stop, []) of
            ok -> true;
            {error, _} -> false
        catch Class:Reason:Stacktrace ->
            ?warning_exception("Failed to stop listener ~w", [Module], Class, Reason, Stacktrace),
            false
        end
    end, listeners()),

    case Success of
        true -> ?info("All listeners stopped");
        false -> ?warning("Some listeners could not be stopped, ignoring")
    end,

    apply_after_listeners_stop_procedures().

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec listeners() -> Listeners :: [atom()].
listeners() ->
    ?CALL_PLUGIN(listeners, []).


%% @private
%% @doc callback called by listener_manager_worker
-spec apply_before_listeners_start_procedures() -> ok | no_return().
apply_before_listeners_start_procedures() ->
    ?info("Executing 'before_listeners_start' procedures..."),
    try
        ok = ?CALL_PLUGIN(before_listeners_start, []),
        ?info("Successfully executed 'before_listeners_start' procedures")
    catch Class:Reason:Stacktrace ->
        ?error_exception(Class, Reason, Stacktrace),
        % this will crash the listener_manager_worker and cause an application shutdown
        error({failed_to_execute_before_listeners_start_procedures})
    end.


%% @private
%% @doc callback called by listener_manager_worker
-spec apply_after_listeners_stop_procedures() -> ok.
apply_after_listeners_stop_procedures() ->
    ?info("Executing 'after_listeners_stop' procedures..."),
    try
        ok = ?CALL_PLUGIN(after_listeners_stop, []),
        ?info("Finished executing 'after_listeners_stop' procedures")
    catch Class:Reason:Stacktrace ->
        ?error_exception(Class, Reason, Stacktrace)
        % do not crash here as we need to shut down regardless of the problems
    end.