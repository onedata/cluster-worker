%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module providing utils for pes_server and pes_server_slave.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_server_utils).
-author("Michal Wrzeszcz").


-include_lib("ctool/include/logging.hrl").


%% API
-export([mark_process_as_pes_server/0, mark_process_as_pes_server_slave/0, mark_process_terminating/0]).
-export([can_execute_call/0, can_execute_wait/0]).
-export([call_optional_callback/5]).


-type error_handling_mode() :: do_not_catch_errors | catch_and_return_error | execute_fallback_on_error.


-define(MARKER, pes_marker).


%%%===================================================================
%%% API for saving in process memory information needed for
%%% verification if calling and waiting is allowed.
%%%===================================================================

-spec mark_process_as_pes_server() -> ok.
mark_process_as_pes_server() ->
    put(?MARKER, pes_server),
    ok.


-spec mark_process_as_pes_server_slave() -> ok.
mark_process_as_pes_server_slave() ->
    put(?MARKER, pes_server_slave),
    ok.


-spec mark_process_terminating() -> ok.
mark_process_terminating() ->
    put(?MARKER, pes_termination),
    ok.


-spec can_execute_call() -> boolean().
can_execute_call() ->
    case get(?MARKER) of
        pes_server -> false;
        pes_termination -> false;
        _ -> true
    end.


-spec can_execute_wait() -> boolean().
can_execute_wait() ->
    case get(?MARKER) of
        pes_server -> false;
        pes_server_slave -> false;
        pes_termination -> false;
        _ -> true
    end.


%%%===================================================================
%%% Helper API for calling optional callbacks
%%%===================================================================

-spec call_optional_callback(pes:executor(), atom(), list(), function(), error_handling_mode()) -> term().
call_optional_callback(Executor, FunName, FunArgs, FallbackFun, ErrorHandlingMode) ->
    case {erlang:function_exported(Executor, FunName, length(FunArgs)), ErrorHandlingMode} of
        {true, do_not_catch_errors} ->
            apply(Executor, FunName, FunArgs);
        {true, _} ->
            try
                apply(Executor, FunName, FunArgs)
            catch
                Error:Reason:Stacktrace ->
                    ?error_stacktrace("PES optional callback ~p error ~p:~p for executor ~p",
                        [FunName, Error, Reason, Executor], Stacktrace),
                    case ErrorHandlingMode of
                        execute_fallback_on_error -> apply(FallbackFun, FunArgs);
                        catch_and_return_error -> {error, Reason}
                    end
            end;
        _ ->
            apply(FallbackFun, FunArgs)
    end.