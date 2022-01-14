%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains PES tests.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_test_SUITE).
-author("Michal Wrzeszcz").

% TODO - przetestowac ze wyslanie wiadomosci przerywa terminacje oraz ze request terminacji jest ignorowany jak sa wiadomosci w liscie
% TODO - dodac test gdzie robimy stop na supervisor i sprawdzic ze terminate slave sie wykona
% TODO - przetestowac zewnetrze termination_requesty
% TODO - testy dla multi_check_cast

-include("pes_protocol.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").


%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    single_call/1,
    single_call_if_alive/1,
    multiple_calls/1,
    multi_call/1,
    single_server_lifecycle/1,
    hashing/1,
    submit_not_supported/1,
    check_cast_not_supported/1,
    long_lasting_call/1,
    single_cast/1,
    call_not_supported/1,
    single_submit/1,
    single_check_cast/1,
    single_submit_if_alive/1,
    single_cast_async_mode/1,
    send_self_request_from_callback/1,
    send_self_request_from_slave_callback/1,
    multi_submit/1,
    single_server_lifecycle_async_mode/1,
    preventing_deadlock/1,
    preventing_self_call/1,
    preventing_deadlock_from_slave/1,
    preventing_self_call_from_slave/1,
    internal_slave_call/1,
    self_slave_call/1,
    multiple_supervisors/1,
    waiting_for_termination/1,
    waiting_for_termination_async_mode/1,
    long_lasting_submit/1,
    call_crash/1,
    not_implemented_call_callback_crash/1,
    cast_crash/1,
    submit_crash/1,
    cast_crash_async_mode/1,
    init_crash/1,
    init_crash_async_mode/1,
    termination_request_error/1,
    termination_request_error_async_mode/1,
    terminate_crash/1,
    slave_terminate_crash/1
]).


all() -> [
    single_call,
    single_call_if_alive,
    multiple_calls,
    multi_call,
    single_server_lifecycle,
    hashing,
    submit_not_supported,
    check_cast_not_supported,
    long_lasting_call,
    single_cast,
    call_not_supported,
    single_submit,
    single_check_cast,
    single_submit_if_alive,
    single_cast_async_mode,
    send_self_request_from_callback,
    send_self_request_from_slave_callback,
    multi_submit,
    single_server_lifecycle_async_mode,
    preventing_deadlock,
    preventing_self_call,
    preventing_deadlock_from_slave,
    preventing_self_call_from_slave,
    internal_slave_call,
    self_slave_call,
    multiple_supervisors,
    waiting_for_termination,
    waiting_for_termination_async_mode,
    long_lasting_submit,
    call_crash,
    not_implemented_call_callback_crash,
    cast_crash,
    submit_crash,
    cast_crash_async_mode,
    init_crash,
    init_crash_async_mode,
    termination_request_error,
    termination_request_error_async_mode,
    terminate_crash,
    slave_terminate_crash
].


-define(SUPERVISOR_NAME, pes_test_supervisor).
-define(EXEMPLARY_PROMISE, {await, make_ref(), self()}).


%%%===================================================================
%%% Test functions
%%%===================================================================

single_call(_Config) ->
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)).


single_call_if_alive(_Config) ->
    ?assertEqual({error, not_alive},
        pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request, #{expect_alive => true})),
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertMatch(call_ok,
        pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request, #{expect_alive => true})).


multiple_calls(_Config) ->
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key2">>, exemplary_request)),
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, increment_value)),
    ?assertEqual({ok, 1}, pes:call(pes_minimal_executor, <<"exemplary_key">>, get_value)),
    ?assertEqual({ok, 0}, pes:call(pes_minimal_executor, <<"exemplary_key2">>, get_value)).


multi_call(_Config) ->
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key2">>, exemplary_request)),
    ?assertEqual([call_ok, call_ok], pes:multi_call(pes_minimal_executor, exemplary_request)).


single_server_lifecycle(_Config) ->
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(1, ets:info(?SUPERVISOR_NAME, size)),

    timer:sleep(timer:seconds(35)),
    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(0, ets:info(?SUPERVISOR_NAME, size)),

    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(1, ets:info(?SUPERVISOR_NAME, size)).


hashing(_Config) ->
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<1>>, exemplary_request)),
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<2>>, exemplary_request)),
    ?assertEqual(call_ok,
        pes:call(pes_minimal_executor, <<3,233>>, increment_value)),
    ?assertEqual({ok, 1}, pes:call(pes_minimal_executor, <<1>>, get_value)), % call to <<3,233>> should be handled
                                                                                  % by the same process as <<1>>
    ?assertEqual({ok, 0}, pes:call(pes_minimal_executor, <<2>>, get_value)).


submit_not_supported(_Config) ->
    ?assertEqual({error, not_supported}, pes:submit(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)).


check_cast_not_supported(_Config) ->
    ?assertEqual({error, not_supported}, pes:check_cast(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)).


long_lasting_call(_Config) ->
    spawn(fun() ->
        ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, wait_and_increment_value))
    end),
    timer:sleep(timer:seconds(5)),
    Stopwatch = stopwatch:start(),
    ?assertEqual({ok, 1}, pes:call(pes_minimal_executor, <<"exemplary_key">>, get_value)),
    ?assert(stopwatch:read_seconds(Stopwatch) > 50). % second call has waited for first call ending


single_cast(_Config) ->
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, decrement_value)),
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, {get_value, self()})),
    receive_and_verify({value, -1}).


call_not_supported(_Config) ->
    ?assertEqual({error, not_supported}, pes:call(pes_async_executor, <<"exemplary_key">>, exemplary_request)).


single_submit(_Config) ->
    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)).


single_check_cast(_Config) ->
    ?assertEqual(ok, pes:check_cast(pes_async_executor, <<"exemplary_key">>, decrement_value)),
    ?assertEqual({ok, -1}, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, get_value)).


single_submit_if_alive(_Config) ->
    ?assertEqual({error, not_alive},
        pes:submit(pes_async_executor, <<"exemplary_key">>, exemplary_request, #{expect_alive => true})),
    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)),
    Promise = ?assertMatch({await, _, _},
        pes:submit(pes_async_executor, <<"exemplary_key">>, exemplary_request, #{expect_alive => true})),
    ?assertEqual(call_ok, pes:await(Promise)).


single_cast_async_mode(_Config) ->
    ?assertEqual(ok, pes:cast(pes_async_executor, <<"exemplary_key">>, decrement_value)),
    ?assertEqual({ok, -1}, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, get_value)).


send_self_request_from_callback(_Config) ->
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, {send_internal_message, self()})),
    receive_and_verify(internal_message_sent),
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, {get_value, self()})),
    receive_and_verify({value, 100}).


send_self_request_from_slave_callback(_Config) ->
    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, send_internal_message)),
    ?assertEqual({ok, 100}, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, get_value)).


multi_submit(_Config) ->
    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key2">>, exemplary_request)),
    ?assertEqual([call_ok, call_ok], pes:multi_submit_and_await_answers(pes_async_executor, exemplary_request)).


single_server_lifecycle_async_mode(_Config) ->
    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(1, ets:info(?SUPERVISOR_NAME, size)),

    timer:sleep(timer:seconds(35)),
    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(0, ets:info(?SUPERVISOR_NAME, size)),

    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(1, ets:info(?SUPERVISOR_NAME, size)).


preventing_deadlock(_Config) ->
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual({error, potential_deadlock},
        pes:call(pes_minimal_executor, <<"exemplary_key2">>, {call_key, <<"exemplary_key">>})),
    ?assertEqual({error, potential_deadlock},
        pes:call(pes_minimal_executor, <<"exemplary_key2">>, {submit_for_key, <<"exemplary_key">>})),
    ?assertEqual({error, potential_deadlock},
        pes:call(pes_minimal_executor, <<"exemplary_key2">>, {await, ?EXEMPLARY_PROMISE})).


preventing_self_call(_Config) ->
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual({error, potential_deadlock},
        pes:call(pes_minimal_executor, <<"exemplary_key">>, {call_key, <<"exemplary_key">>})),
    ?assertEqual({error, potential_deadlock},
        pes:call(pes_minimal_executor, <<"exemplary_key">>, {submit_for_key, <<"exemplary_key">>})).


preventing_deadlock_from_slave(_Config) ->
    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual({error, not_supported},
        pes:submit_and_await(pes_async_executor, <<"exemplary_key2">>, {call_key, <<"exemplary_key">>})),
    ?assertEqual({error, potential_deadlock},
        pes:submit_and_await(pes_async_executor, <<"exemplary_key2">>, {await, ?EXEMPLARY_PROMISE})).


preventing_self_call_from_slave(_Config) ->
    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual({error, not_supported},
        pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, {call_key, <<"exemplary_key">>})).


internal_slave_call(_Config) ->
    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, increment_value)),
    ?assertEqual(call_ok, pes:submit_and_await(
        pes_async_executor, <<"exemplary_key2">>, {submit_for_key, <<"exemplary_key">>, self()})),
    receive_and_verify({internal_call_ans, {ok, 1}}).


self_slave_call(_Config) ->
    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, increment_value)),
    ?assertEqual(call_ok, pes:submit_and_await(
        pes_async_executor, <<"exemplary_key">>, {submit_for_key, <<"exemplary_key">>, self()})),
    receive_and_verify({internal_call_ans, {ok, 1}}).


multiple_supervisors(_Config) ->
    lists:foreach(fun(Key) ->
        ?assertEqual(ok, pes:cast(pes_sync_executor, Key, exemplary_request))
    end, lists:seq(1, 500)),

    SupervisorNames = lists:map(fun({Name, _, supervisor, [pes_supervisor]}) ->
        Name
    end, supervisor:which_children(?SUPERVISOR_NAME)),
    ExpectedSupervisorNames = lists:map(fun(N) ->
        list_to_atom(atom_to_list(?SUPERVISOR_NAME) ++ integer_to_list(N))
    end, lists:seq(0, 4)),
    ?assertEqual(ExpectedSupervisorNames, lists:sort(SupervisorNames)),

    lists:foreach(fun(Name) ->
        ?assertEqual(10, proplists:get_value(active, supervisor:count_children(Name)))
    end, SupervisorNames).


waiting_for_termination(_Config) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_should_stop, false),
    blocking_termination_test().


waiting_for_termination_async_mode(_Config) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_should_stop, false),
    blocking_termination_async_mode_test().


long_lasting_submit(_Config) ->
    Stopwatch1 = stopwatch:start(),
    Promise1 = ?assertMatch({await, _, _},
        pes:submit(pes_async_executor, <<"exemplary_key">>, wait_and_increment_value)),
    Promise2 = ?assertMatch({await, _, _}, pes:submit(pes_async_executor, <<"exemplary_key">>, increment_value)),
    Promise3 = ?assertMatch({await, _, _}, pes:submit(pes_async_executor, <<"exemplary_key">>, increment_value)),
    Promise4 = ?assertMatch({await, _, _}, pes:submit(pes_async_executor, <<"exemplary_key">>, increment_value)),
    ?assert(stopwatch:read_seconds(Stopwatch1) < 5), % all calls should not wait for 
                                                     % wait_and_increment_value request processing finish

    Stopwatch2 = stopwatch:start(),
    ?assertEqual(call_ok, pes:await(Promise2)),
    ?assert(stopwatch:read_seconds(Stopwatch2) > 50), % long waiting for wait_and_increment_value 
                                                      % request processing finish (Promise2 will be 
                                                      % fulfilled after Promise1)

    Stopwatch3 = stopwatch:start(),
    ?assertEqual(call_ok, pes:await(Promise1)),
    ?assertEqual(call_ok, pes:await(Promise3)),
    ?assertEqual(call_ok, pes:await(Promise4)),
    ?assert(stopwatch:read_seconds(Stopwatch3) < 5), % Other promises should be fulfilled quickly
    
    ?assertEqual({ok, 4}, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, get_value)).


call_crash(_Config) ->
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, increment_value)),
    ?assertEqual({error, call_error}, pes:call(pes_minimal_executor, <<"exemplary_key">>, crash_call)),
    ?assertEqual({ok, 1}, pes:call(pes_minimal_executor, <<"exemplary_key">>, get_value)).


not_implemented_call_callback_crash(_Config) ->
    ?assertEqual({error, undef}, pes:call(pes_sync_executor, 10, exemplary_request)),
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, {get_value, self()})),
    receive_and_verify({value, 0}).


cast_crash(_Config) ->
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, decrement_value)),
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, crash_cast)),
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, {get_value, self()})),
    receive_and_verify({value, -1}).


submit_crash(_Config) ->
    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor,<<"exemplary_key">>, increment_value)),
    ?assertEqual({error, call_error}, pes:submit_and_await(pes_async_executor,<<"exemplary_key">>, crash_call)),
    ?assertEqual({ok, 1}, pes:submit_and_await(pes_minimal_executor, <<"exemplary_key">>, get_value)).


cast_crash_async_mode(_Config) ->
    ?assertEqual(ok, pes:cast(pes_async_executor, 10, decrement_value)),
    ?assertEqual(ok, pes:cast(pes_async_executor, 10, crash_cast)),
    ?assertEqual({ok, -1}, pes:submit_and_await(pes_async_executor, 10, get_value)).


init_crash(_Config) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_init_should_crash, true),
    ?assertEqual({error, init_error}, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(0, ets:info(?SUPERVISOR_NAME, size)),

    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_init_should_crash, false),
    ?assertEqual(call_ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)).


init_crash_async_mode(_Config) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_init_should_crash, true),
    ?assertEqual({error, {badmatch, {error, init_error}}},
        pes:submit(pes_async_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(0, ets:info(?SUPERVISOR_NAME, size)),

    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_init_should_crash, false),
    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)).


termination_request_error(_Config) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_should_stop, crash),
    blocking_termination_test().


termination_request_error_async_mode(_Config) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_should_stop, crash),
    blocking_termination_async_mode_test().


terminate_crash(_Config) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_should_stop, crash),
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, {get_value, self()})),
    receive_and_verify({value, 0}),
    Supervisor = pes_process_manager:get_supervisor_name(
        pes_sync_executor, pes_process_manager:hash_key(pes_sync_executor, 10)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(Supervisor))),
    ?assertEqual(1, ets:info(Supervisor, size)),

    [{_, ProcessPid, _, _}] = supervisor:which_children(Supervisor),
    ?assertEqual(ok, supervisor:terminate_child(Supervisor, ProcessPid)),
    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(Supervisor))),
    ?assertEqual(0, ets:info(Supervisor, size)),
    ?assertEqual(shutdown, application:get_env(?CLUSTER_WORKER_APP_NAME, termination_reason, undefined)).


slave_terminate_crash(_Config) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_should_stop, crash),
    ?assertEqual(call_ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(1, ets:info(?SUPERVISOR_NAME, size)),

    [{_, ProcessPid, _, _}] = supervisor:which_children(?SUPERVISOR_NAME),
    ?assertEqual(ok, supervisor:terminate_child(?SUPERVISOR_NAME, ProcessPid)),
    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(0, ets:info(?SUPERVISOR_NAME, size)),
    ?assertEqual(shutdown, application:get_env(?CLUSTER_WORKER_APP_NAME, termination_reason, undefined)).


%%%===================================================================
%%% Test skeletons
%%%===================================================================

blocking_termination_test() ->
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, decrement_value)),
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, {get_value, self()})),
    receive_and_verify({value, -1}),
    Supervisor = pes_process_manager:get_supervisor_name(
        pes_sync_executor, pes_process_manager:hash_key(pes_sync_executor, 10)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(Supervisor))),
    ?assertEqual(1, ets:info(Supervisor, size)),

    timer:sleep(timer:seconds(35)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(Supervisor))),
    ?assertEqual(1, ets:info(Supervisor, size)),
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, {get_value, self()})),
    receive_and_verify({value, -1}),
    ?assertEqual(termination_request, application:get_env(?CLUSTER_WORKER_APP_NAME, termination_reason, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_should_stop, true),
    timer:sleep(timer:seconds(35)),
    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(Supervisor))),
    ?assertEqual(0, ets:info(Supervisor, size)),
    ?assertEqual(termination_request, application:get_env(?CLUSTER_WORKER_APP_NAME, termination_reason, undefined)).


blocking_termination_async_mode_test() ->
    ?assertEqual(ok, pes:cast(pes_async_executor, <<"exemplary_key">>, decrement_value)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(1, ets:info(?SUPERVISOR_NAME, size)),

    timer:sleep(timer:seconds(35)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(1, ets:info(?SUPERVISOR_NAME, size)),
    ?assertEqual({ok, -1}, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, get_value)),
    ?assertEqual(termination_request, application:get_env(?CLUSTER_WORKER_APP_NAME, termination_reason, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_should_stop, true),
    timer:sleep(timer:seconds(35)),
    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(0, ets:info(?SUPERVISOR_NAME, size)),
    ?assertEqual(termination_request, application:get_env(?CLUSTER_WORKER_APP_NAME, termination_reason, undefined)).


%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    load_executors([pes_minimal_executor, pes_sync_executor, pes_async_executor]),
    Config.


end_per_suite(_Config) ->
    ok.


init_per_testcase(Case, Config) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_should_stop, true),
    application:set_env(?CLUSTER_WORKER_APP_NAME, pes_init_should_crash, false),
    application:set_env(?CLUSTER_WORKER_APP_NAME, termination_reason, undefined),
    Executor = case_to_executor(Case),
    {ok, SupervisorPid} = init_supervisor(case_to_executor(Case)),
    pes:start_link(Executor),
    [{supervisor_pid, SupervisorPid} | Config].


end_per_testcase(Case, Config) ->
    pes:stop(case_to_executor(Case)),
    RootSupervisorPid = ?config(supervisor_pid, Config),
    erlang:unlink(RootSupervisorPid),
    exit(RootSupervisorPid, shutdown).


%%%===================================================================
%%% Internal functions
%%%===================================================================

case_to_executor(single_call) ->
    pes_minimal_executor;
case_to_executor(single_call_if_alive) ->
    pes_minimal_executor;
case_to_executor(multiple_calls) ->
    pes_minimal_executor;
case_to_executor(multi_call) ->
    pes_minimal_executor;
case_to_executor(single_server_lifecycle) ->
    pes_minimal_executor;
case_to_executor(hashing) ->
    pes_minimal_executor;
case_to_executor(submit_not_supported) ->
    pes_minimal_executor;
case_to_executor(check_cast_not_supported) ->
    pes_minimal_executor;
case_to_executor(long_lasting_call) ->
    pes_minimal_executor;
case_to_executor(single_cast) ->
    pes_sync_executor;
case_to_executor(send_self_request_from_callback) ->
    pes_sync_executor;
case_to_executor(preventing_deadlock) ->
    pes_minimal_executor;
case_to_executor(preventing_self_call) ->
    pes_minimal_executor;
case_to_executor(multiple_supervisors) ->
    pes_sync_executor;
case_to_executor(waiting_for_termination) ->
    pes_sync_executor;
case_to_executor(call_crash) ->
    pes_minimal_executor;
case_to_executor(not_implemented_call_callback_crash) ->
    pes_sync_executor;
case_to_executor(cast_crash) ->
    pes_sync_executor;
case_to_executor(init_crash) ->
    pes_minimal_executor;
case_to_executor(termination_request_error) ->
    pes_sync_executor;
case_to_executor(terminate_crash) ->
    pes_sync_executor;
case_to_executor(_) ->
    pes_async_executor.


init_supervisor(pes_sync_executor) ->
    pes_multi_group_root_supervisor:start_link(?SUPERVISOR_NAME);
init_supervisor(_Executor) ->
    pes_supervisor:start_link(?SUPERVISOR_NAME).


load_executors([]) ->
    ok;
load_executors([Module | Modules]) ->
    {Module, Binary, Filename} = code:get_object_code(Module),
    ?assertEqual({module, Module}, code:load_binary(Module, Filename, Binary)),
    load_executors(Modules).


receive_and_verify(Expected) ->
    Received = receive
        Message -> Message
    after
        5000 -> timeout
    end,
    ?assertEqual(Expected, Received).