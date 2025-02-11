%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handles logging to the journal log file - a non-rotated log containing
%%% entries related to application stopping and starting. The log is placed
%%% in the logger's log_root, which depends on the config of the master
%%% application that uses cluster-worker.
%%% @end
%%%--------------------------------------------------------------------
-module(journal_logger).
-author("Lukasz Opiola").

%% API
-export([log/1, add_delimiter/0]).


-define(JOURNAL_LOG_FILE_NAME, "journal.log").

%%%===================================================================
%%% API
%%%===================================================================

-spec log(string()) -> ok.
log(Message) ->
    {Date, Time} = format_time(erlang:localtime()),
    write_to_log_file("[~ts ~ts] ~ts~n", [Date, Time, Message]).


% useful for better readability of the logs
-spec add_delimiter() -> ok.
add_delimiter() ->
    write_to_log_file("~n", []).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec write_to_log_file(string(), list()) -> ok.
write_to_log_file(Format, Args) ->
    ok = file:write_file(log_file(), io_lib:format(Format, Args), [append]).


%% @private
-spec log_file() -> string().
log_file() ->
    LogRoot = application:get_env(ctool, log_root, "/tmp"),
    filename:join(LogRoot, ?JOURNAL_LOG_FILE_NAME).


%% @private
-spec format_time(calendar:datetime()) -> {string(), string()}.
format_time({{Year, Month, Day}, {Hour, Minute, Second}}) ->
    DateStr = io_lib:format("~4..0B-~2..0B-~2..0B", [Year, Month, Day]),
    TimeStr = io_lib:format("~2..0B:~2..0B:~2..0B", [Hour, Minute, Second]),
    {DateStr, TimeStr}.
