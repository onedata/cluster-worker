%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of central_logger, using eunit tests.
%% @end
%% ===================================================================
-module(central_logger_tests).

%% TODO nie przetestowano inicjalizacji i sprzątania modułu, jeśli jest to zbyt trudne w eunit
%% można to zrobić w ct (jednak nie ma testu logger w ct stąd też uwaga tutaj)
%% TODO w funkcji do_log wydaje się, ze jest testowana tylko jedna główna ścieżka
%% np. brakuje testów dla których wartość wyrażenia
%% (LevelThreshold band SeverityAsInt) /= 0 orelse Destinations /= []
%% byłaby true lub Traces nie było pustą listą

%% TODO brakuje testów dispatch_log z central_logging_backend

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

central_logger_test_() ->
    {setup, fun setup/0, fun teardown/1, [fun subscribing/0]}.

setup() ->
  	ets:new(subscribers_ets, [named_table, public, bag, {read_concurrency, true}]).

teardown(_) ->
  	ets:delete(subscribers_ets).

%=================================================       
% TEST I    

% test if subscription system works properly
subscribing() ->
	% this many pids
	NumberOfPids = 20,
	% will each receive this many logs when subscribed
	NumberOfValidLogs = [56, 13, 311, 197],
	% and this many when not subscribed (in fact will NOT receive them because they are not subscribed)
	NumberOfInvalidLogs = [42, 11, 451, 93],

	Pids = lists:map
	(
		fun(_) -> spawn(fun() -> subscriber_process_loop(0) end)	end,
		lists:seq(1, NumberOfPids)
	),	

	lists:foreach(
		fun({Valid, Invalid})->
			lists:foreach(fun(Pid) -> Pid ! {self(), subscribe} end, Pids),
			wait_for_pids(NumberOfPids),
			send_n_logs(Valid),

			lists:foreach(fun(Pid) -> Pid ! {self(), unsubscribe} end, Pids),
			wait_for_pids(NumberOfPids),
			send_n_logs(Invalid)
		end,
		lists:zip(NumberOfValidLogs, NumberOfInvalidLogs)),
	

	lists:foreach(fun(Pid) -> Pid ! {self(), get_log_count} end, Pids),
	LogsTotal = lists:foldl(
		fun(_, Acc) -> 
			NumberOfLogs = receive LogsN -> LogsN after 1000 -> 0 end,
			Acc + NumberOfLogs
		end, 
		0, 
		Pids),

	% check if proper amount of logs reached their destination
	?assert(LogsTotal =:= NumberOfPids * lists:sum(NumberOfValidLogs)).

subscriber_process_loop(ReceivedLogs) ->
	ThisNode = node(),
	receive
		{Pid, subscribe} ->
			central_logger:handle(1, {subscribe, self()}),
			Pid ! done,
			subscriber_process_loop(ReceivedLogs);
		{Pid, unsubscribe} ->
			central_logger:handle(1, {unsubscribe, self()}),
			Pid ! done,
			subscriber_process_loop(ReceivedLogs);
		{log, {"log", _, info, [{node, ThisNode}]}} ->
			subscriber_process_loop(ReceivedLogs + 1);
		{log, _} ->
			subscriber_process_loop(ReceivedLogs);
		{Pid, get_log_count} ->
			Pid ! ReceivedLogs,
			finished;
		_ ->
			finished
	end.

wait_for_pids(N) when N =:= 0 -> finished;
wait_for_pids(N) -> receive done -> wait_for_pids(N - 1) end.

send_n_logs(N) when N =:= 0 -> finished;
send_n_logs(N) -> 
	central_logger:handle(1, {dispatch_log, "log", erlang:now(), info, [{node, node()}]}),
	send_n_logs(N - 1).

-endif.