%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for calling optional callbacks from modules
%%% implementing pes_plugin_behaviour.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_plugin).
-author("Michal Wrzeszcz").


-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([get_root_supervisor_name/1, get_executor_count/1, get_supervisor_count/1]).
-export([get_mode/1, init/1, graceful_terminate/2, forced_terminate/3, handle_call/3, handle_cast/3, code_change/4]).


-type optional_callback_error_handling_mode() :: do_not_catch_errors | catch_and_return_error |
    execute_fallback_on_error.


-define(DEFAULT_EXECUTOR_COUNT, 100).
-define(DEFAULT_SUPERVISOR_COUNT, 1).


%%%===================================================================
%%% Configuration of plug-in
%%%===================================================================

-spec get_root_supervisor_name(pes:plugin()) -> pes_server_supervisor:name().
get_root_supervisor_name(Plugin) ->
    Plugin:get_root_supervisor_name().


-spec get_executor_count(pes:plugin()) -> pos_integer().
get_executor_count(Plugin) ->
    call_optional_callback(Plugin, ?FUNCTION_NAME, [], fun() -> ?DEFAULT_EXECUTOR_COUNT end, do_not_catch_errors).


-spec get_supervisor_count(pes:plugin()) -> pos_integer().
get_supervisor_count(Plugin) ->
    call_optional_callback(Plugin, ?FUNCTION_NAME, [], fun() -> ?DEFAULT_SUPERVISOR_COUNT end, do_not_catch_errors).


%%%===================================================================
%%% Executor logic
%%%===================================================================

-spec get_mode(pes:plugin()) -> pes:mode().
get_mode(Plugin) ->
    call_optional_callback(Plugin, ?FUNCTION_NAME, [], fun() -> sync end, do_not_catch_errors).


-spec init(pes:plugin()) -> pes:executor_state().
init(Plugin) ->
    Plugin:init().


-spec graceful_terminate(pes:plugin(), pes:executor_state()) ->
    {ok | defer, pes:executor_state()} | ?ERROR_INTERNAL_SERVER_ERROR.
graceful_terminate(Plugin, PluginState) ->
    call_optional_callback(
        Plugin, ?FUNCTION_NAME, [PluginState], fun(_) -> {ok, PluginState} end, catch_and_return_error).


-spec forced_terminate(pes:plugin(), pes:forced_termination_reason(), pes:executor_state()) -> ok.
forced_terminate(Plugin, Reason, PluginState) ->
    call_optional_callback(
        Plugin, ?FUNCTION_NAME, [Reason, PluginState], fun(_, _) -> ok end, execute_fallback_on_error).


-spec handle_call(pes:plugin(), pes:request(), pes:executor_state()) -> {pes:execution_result(), pes:executor_state()}.
handle_call(Plugin, Request, ExecutorState) ->
    Plugin:handle_call(Request, ExecutorState).


-spec handle_cast(pes:plugin(), pes:request(), pes:executor_state()) -> pes:executor_state().
handle_cast(Plugin, Request, ExecutorState) ->
    Plugin:handle_cast(Request, ExecutorState).


-spec code_change(pes:plugin(), OldVsn :: term() | {down, term()}, pes:executor_state(), Extra :: term()) ->
    {ok, pes:executor_state()} | {error, Reason :: term()}.
code_change(Plugin, OldVsn, PluginState, Extra) ->
    call_optional_callback(Plugin, ?FUNCTION_NAME, [OldVsn, PluginState, Extra],
        fun(_, _, _) -> {ok, PluginState} end, execute_fallback_on_error).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec call_optional_callback(pes:plugin(), atom(), list(), function(), optional_callback_error_handling_mode()) ->
    term().
call_optional_callback(Plugin, FunName, FunArgs, FallbackFun, ErrorHandlingMode) ->
    case {erlang:function_exported(Plugin, FunName, length(FunArgs)), ErrorHandlingMode} of
        {true, do_not_catch_errors} ->
            apply(Plugin, FunName, FunArgs);
        {true, _} ->
            try
                apply(Plugin, FunName, FunArgs)
            catch
                Error:Reason:Stacktrace ->
                    ?error_stacktrace("PES optional callback ~tp error ~tp:~tp for plug-in ~tp",
                        [FunName, Error, Reason, Plugin], Stacktrace),
                    case ErrorHandlingMode of
                        execute_fallback_on_error -> apply(FallbackFun, FunArgs);
                        catch_and_return_error -> ?ERROR_INTERNAL_SERVER_ERROR
                    end
            end;
        _ ->
            apply(FallbackFun, FunArgs)
    end.