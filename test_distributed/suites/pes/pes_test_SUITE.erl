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


-include("pes_protocol.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").


%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    single_call/1,
    single_call_with_ensure_executor_alive_set_to_false/1,
    multiple_calls/1,
    multi_call/1,
    multi_acknowledged_cast_async_mode/1,
    single_executor_lifecycle_sync_mode/1,
    hashing/1,
    submit_not_supported/1,
    acknowledged_cast_not_supported/1,
    long_lasting_call/1,
    single_cast_sync_mode/1,
    call_not_supported/1,
    single_submit/1,
    single_acknowledged_cast/1,
    single_submit_with_ensure_executor_alive_set_to_false/1,
    single_cast_async_mode/1,
    send_self_cast_sync_mode/1,
    send_self_cast_async_mode/1,
    multi_submit/1,
    single_executor_lifecycle_async_mode/1,
    preventing_sync_mode_deadlocks/1,
    preventing_self_call/1,
    preventing_deadlock_from_slave/1,
    preventing_self_call_from_slave/1,
    internal_submit/1,
    internal_self_submit/1,
    multiple_supervisors/1,
    deferred_graceful_termination_sync_mode/1,
    deferred_graceful_termination_async_mode/1,
    long_lasting_submit/1,
    call_crash/1,
    not_implemented_call_callback_crash/1,
    cast_crash_sync_mode/1,
    submit_crash/1,
    cast_crash_async_mode/1,
    init_crash_sync_mode/1,
    init_crash_async_mode/1,
    external_termination_request_sync_mode/1,
    external_termination_request_async_mode/1,
    crashed_graceful_termination_sync_mode/1,
    crashed_graceful_termination_async_mode/1,
    server_terminate_crash/1,
    slave_terminate_crash/1,
    termination_interrupted_by_new_request/1
]).


all() -> [
    single_call,
    single_call_with_ensure_executor_alive_set_to_false,
    multiple_calls,
    multi_call,
    multi_acknowledged_cast_async_mode,
    single_executor_lifecycle_sync_mode,
    hashing,
    submit_not_supported,
    acknowledged_cast_not_supported,
    long_lasting_call,
    single_cast_sync_mode,
    call_not_supported,
    single_submit,
    single_acknowledged_cast,
    single_submit_with_ensure_executor_alive_set_to_false,
    single_cast_async_mode,
    send_self_cast_sync_mode,
    send_self_cast_async_mode,
    multi_submit,
    single_executor_lifecycle_async_mode,
    preventing_sync_mode_deadlocks,
    preventing_self_call,
    preventing_deadlock_from_slave,
    preventing_self_call_from_slave,
    internal_submit,
    internal_self_submit,
    multiple_supervisors,
    deferred_graceful_termination_sync_mode,
    deferred_graceful_termination_async_mode,
    long_lasting_submit,
    call_crash,
    not_implemented_call_callback_crash,
    cast_crash_sync_mode,
    submit_crash,
    cast_crash_async_mode,
    init_crash_sync_mode,
    init_crash_async_mode,
    external_termination_request_sync_mode,
    external_termination_request_async_mode,
    crashed_graceful_termination_sync_mode,
    crashed_graceful_termination_async_mode,
    server_terminate_crash,
    slave_terminate_crash,
    termination_interrupted_by_new_request
].


-define(SUPERVISOR_NAME, pes_test_supervisor).
-define(EXEMPLARY_PROMISE, {make_ref(), self()}).


%%%===================================================================
%%% Test functions
%%%===================================================================

single_call(_Config) ->
    ?assertEqual({ok, test_ans}, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)).


single_call_with_ensure_executor_alive_set_to_false(_Config) ->
    ?assertEqual(ignored,
        pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request, #{ensure_executor_alive => false})),
    ?assertEqual({ok, test_ans}, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertMatch({ok, test_ans},
        pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request, #{ensure_executor_alive => false})).


multiple_calls(_Config) ->
    ?assertEqual({ok, test_ans}, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual({ok, test_ans}, pes:call(pes_minimal_executor, <<"exemplary_key2">>, exemplary_request)),
    ?assertEqual(ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, increment_value)),
    pes_executor_mock:verify_state(pes_minimal_executor, <<"exemplary_key">>, 1),
    pes_executor_mock:verify_state(pes_minimal_executor, <<"exemplary_key2">>, 0).


multi_call(_Config) ->
    ?assertEqual({ok, test_ans}, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual({ok, test_ans}, pes:call(pes_minimal_executor, <<"exemplary_key2">>, exemplary_request)),
    ?assertEqual([{ok, test_ans}, {ok, test_ans}], pes:multi_call(pes_minimal_executor, exemplary_request)).


multi_acknowledged_cast_async_mode(_Config) ->
    ?assertEqual({ok, test_ans}, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual({ok, test_ans}, pes:submit_and_await(pes_async_executor, <<"exemplary_key2">>, exemplary_request)),
    ?assertEqual([ok, ok], pes:multi_acknowledged_cast(pes_async_executor, decrement_value)),
    pes_executor_mock:verify_state(pes_async_executor, <<"exemplary_key">>, -1),
    pes_executor_mock:verify_state(pes_async_executor, <<"exemplary_key2">>, -1).


single_executor_lifecycle_sync_mode(_Config) ->
    single_executor_lifecycle(pes_minimal_executor, call).


hashing(_Config) ->
    ?assertEqual({ok, test_ans}, pes:call(pes_minimal_executor, <<1>>, exemplary_request)),
    ?assertEqual({ok, test_ans}, pes:call(pes_minimal_executor, <<2>>, exemplary_request)),
    ?assertEqual(ok, pes:call(pes_minimal_executor, <<3,233>>, increment_value)),
    pes_executor_mock:verify_state(pes_minimal_executor, <<1>>, 1), % call to <<3,233>> should be handled
                                                                    % by the same process as <<1>>
    pes_executor_mock:verify_state(pes_minimal_executor, <<2>>, 0).


submit_not_supported(_Config) ->
    ?assertEqual(?ERROR_NOT_SUPPORTED, pes:submit(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)).


acknowledged_cast_not_supported(_Config) ->
    ?assertEqual(?ERROR_NOT_SUPPORTED, pes:acknowledged_cast(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)).


long_lasting_call(_Config) ->
    spawn(fun() ->
        ?assertEqual(ok,
            pes:call(pes_minimal_executor, <<"exemplary_key">>, {wait_and_increment_value, timer:seconds(65)}))
    end),
    timer:sleep(timer:seconds(5)),
    Stopwatch = stopwatch:start(),
    ?assertEqual({ok, 1}, pes:call(pes_minimal_executor, <<"exemplary_key">>, get_value)),
    ?assert(stopwatch:read_seconds(Stopwatch) > 50). % second call has waited for first call ending


single_cast_sync_mode(_Config) ->
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, decrement_value)),
    pes_executor_mock:verify_state(pes_sync_executor, 10, -1).


call_not_supported(_Config) ->
    ?assertEqual(?ERROR_NOT_SUPPORTED, pes:call(pes_async_executor, <<"exemplary_key">>, exemplary_request)).


single_submit(_Config) ->
    ?assertEqual({ok, test_ans}, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)).


single_acknowledged_cast(_Config) ->
    ?assertEqual(ok, pes:acknowledged_cast(pes_async_executor, <<"exemplary_key">>, decrement_value)),
    pes_executor_mock:verify_state(pes_async_executor, <<"exemplary_key">>, -1).


single_submit_with_ensure_executor_alive_set_to_false(_Config) ->
    ?assertEqual(ignored,
        pes:submit(pes_async_executor, <<"exemplary_key">>, exemplary_request, #{ensure_executor_alive => false})),
    ?assertEqual({ok, test_ans}, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)),
    {ok, Promise} = ?assertMatch({ok, {_, _}},
        pes:submit(pes_async_executor, <<"exemplary_key">>, exemplary_request, #{ensure_executor_alive => false})),
    ?assertEqual({ok, test_ans}, pes:await(Promise)).


single_cast_async_mode(_Config) ->
    ?assertEqual(ok, pes:cast(pes_async_executor, <<"exemplary_key">>, decrement_value)),
    pes_executor_mock:verify_state(pes_async_executor, <<"exemplary_key">>, -1).


send_self_cast_sync_mode(_Config) ->
    ?assertEqual(ok, pes:cast(pes_sync_executor, 10, {increment_using_self_cast, 100, self()})),
    pes_executor_mock:wait_for_self_cast(),
    pes_executor_mock:verify_state(pes_sync_executor, 10, 100).


send_self_cast_async_mode(_Config) ->
    ?assertEqual(ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, {increment_using_self_cast, 100})),
    pes_executor_mock:verify_state(pes_async_executor, <<"exemplary_key">>, 100).


multi_submit(_Config) ->
    ?assertEqual({ok, test_ans}, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual({ok, test_ans}, pes:submit_and_await(pes_async_executor, <<"exemplary_key2">>, exemplary_request)),
    ?assertEqual([{ok, test_ans}, {ok, test_ans}], pes:multi_submit_and_await(pes_async_executor, exemplary_request)).


single_executor_lifecycle_async_mode(_Config) ->
    single_executor_lifecycle(pes_async_executor, submit_and_await).


preventing_sync_mode_deadlocks(_Config) ->
    ?assertEqual({ok, test_ans}, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(?ERROR_INTERNAL_SERVER_ERROR, pes:call(pes_minimal_executor, <<"exemplary_key2">>,
        {perform_internal_call_with_key, pes_minimal_executor, <<"exemplary_key">>})),
    ?assertEqual(?ERROR_INTERNAL_SERVER_ERROR, pes:call(pes_minimal_executor, <<"exemplary_key2">>,
        {perform_internal_submit_with_key, pes_minimal_executor, <<"exemplary_key">>, self()})),
    ?assertEqual(?ERROR_INTERNAL_SERVER_ERROR,
        pes:call(pes_minimal_executor, <<"exemplary_key2">>, {perform_internal_await, ?EXEMPLARY_PROMISE})).


preventing_self_call(_Config) ->
    ?assertEqual({ok, test_ans}, pes:call(pes_minimal_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(?ERROR_INTERNAL_SERVER_ERROR, pes:call(pes_minimal_executor, <<"exemplary_key">>,
        {perform_internal_call_with_key, pes_minimal_executor, <<"exemplary_key">>})),
    ?assertEqual(?ERROR_INTERNAL_SERVER_ERROR, pes:call(pes_minimal_executor, <<"exemplary_key">>,
        {perform_internal_submit_with_key, pes_minimal_executor, <<"exemplary_key">>, self()})).


preventing_deadlock_from_slave(_Config) ->
    ?assertEqual({ok, test_ans}, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(?ERROR_NOT_SUPPORTED, pes:submit_and_await(pes_async_executor, <<"exemplary_key2">>,
        {perform_internal_call_with_key, pes_async_executor, <<"exemplary_key">>})),
    ?assertEqual(?ERROR_INTERNAL_SERVER_ERROR,
        pes:submit_and_await(pes_async_executor, <<"exemplary_key2">>, {perform_internal_await, ?EXEMPLARY_PROMISE})).


preventing_self_call_from_slave(_Config) ->
    ?assertEqual({ok, test_ans}, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(?ERROR_NOT_SUPPORTED, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>,
        {perform_internal_call_with_key, pes_async_executor, <<"exemplary_key">>})).


internal_submit(_Config) ->
    ?assertEqual(ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, increment_value)),
    ?assertEqual({ok, internal_submit_done}, pes:submit_and_await(pes_async_executor, <<"exemplary_key2">>,
        {perform_internal_submit_with_key, pes_async_executor, <<"exemplary_key">>, self()})),
    pes_executor_mock:verify_internal_submit(1).


internal_self_submit(_Config) ->
    ?assertEqual(ok, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>, increment_value)),
    ?assertEqual({ok, internal_submit_done}, pes:submit_and_await(pes_async_executor, <<"exemplary_key">>,
        {perform_internal_submit_with_key, pes_async_executor, <<"exemplary_key">>, self()})),
    pes_executor_mock:verify_internal_submit(1).


multiple_supervisors(_Config) ->
    lists:foreach(fun(Key) ->
        ?assertEqual(ok, pes:cast(pes_sync_executor, Key, exemplary_request))
    end, lists:seq(1, 500)),

    SupervisorNames = lists:map(fun({Name, _, supervisor, [pes_server_supervisor]}) ->
        Name
    end, supervisor:which_children(?SUPERVISOR_NAME)),
    ExpectedSupervisorNames = lists:map(fun(N) ->
        list_to_atom(atom_to_list(?SUPERVISOR_NAME) ++ integer_to_list(N))
    end, lists:seq(0, 4)),
    ?assertEqual(ExpectedSupervisorNames, lists:sort(SupervisorNames)),

    lists:foreach(fun(Name) ->
        ?assertEqual(10, proplists:get_value(active, supervisor:count_children(Name)))
    end, SupervisorNames).


deferred_graceful_termination_sync_mode(_Config) ->
    blocking_termination_sync_mode_test(defer_termination).


deferred_graceful_termination_async_mode(_Config) ->
    blocking_termination_async_mode_test(defer_termination).


long_lasting_submit(_Config) ->
    Stopwatch1 = stopwatch:start(),
    {ok, Promise1} = ?assertMatch({ok, {_, _}},
        pes:submit(pes_async_executor, <<"exemplary_key">>, {wait_and_increment_value, timer:seconds(65)})),
    {ok, Promise2} = ?assertMatch({ok, {_, _}}, pes:submit(pes_async_executor, <<"exemplary_key">>, increment_value)),
    {ok, Promise3} = ?assertMatch({ok, {_, _}}, pes:submit(pes_async_executor, <<"exemplary_key">>, increment_value)),
    {ok, Promise4} = ?assertMatch({ok, {_, _}}, pes:submit(pes_async_executor, <<"exemplary_key">>, increment_value)),
    ?assert(stopwatch:read_seconds(Stopwatch1) < 5), % all calls should not wait for
                                                     % wait_and_increment_value request processing finish

    Stopwatch2 = stopwatch:start(),
    ?assertEqual(ok, pes:await(Promise2)),
    ?assert(stopwatch:read_seconds(Stopwatch2) > 50), % long waiting for wait_and_increment_value
                                                      % request processing finish (Promise2 will be
                                                      % fulfilled after Promise1)

    Stopwatch3 = stopwatch:start(),
    ?assertEqual(ok, pes:await(Promise1)),
    ?assertEqual(ok, pes:await(Promise3)),
    ?assertEqual(ok, pes:await(Promise4)),
    ?assert(stopwatch:read_seconds(Stopwatch3) < 5), % Other promises should be fulfilled quickly

    pes_executor_mock:verify_state(pes_async_executor, <<"exemplary_key">>, 4).


call_crash(_Config) ->
    ?assertEqual(ok, pes:call(pes_minimal_executor, <<"exemplary_key">>, increment_value)),
    ?assertEqual(?ERROR_INTERNAL_SERVER_ERROR,
        pes:call(pes_minimal_executor, <<"exemplary_key">>, {crash_with, throw, call_error})),
    pes_executor_mock:verify_state(pes_minimal_executor, <<"exemplary_key">>, 1).


not_implemented_call_callback_crash(_Config) ->
    ?assertEqual(?ERROR_INTERNAL_SERVER_ERROR, pes:call(pes_sync_executor, 10, exemplary_request)),
    pes_executor_mock:verify_state(pes_sync_executor, 10, 0).


cast_crash_sync_mode(_Config) ->
    cast_crash(pes_sync_executor).


submit_crash(_Config) ->
    ?assertEqual(ok, pes:submit_and_await(pes_async_executor,<<"exemplary_key">>, increment_value)),
    ?assertEqual(?ERROR_INTERNAL_SERVER_ERROR,
        pes:submit_and_await(pes_async_executor,<<"exemplary_key">>, {crash_with, throw, call_error})),
    pes_executor_mock:verify_state(pes_async_executor, <<"exemplary_key">>, 1).


cast_crash_async_mode(_Config) ->
    cast_crash(pes_async_executor).


init_crash_sync_mode(_Config) ->
    init_crash(pes_minimal_executor, call).


init_crash_async_mode(_Config) ->
    init_crash(pes_async_executor, submit_and_await).


external_termination_request_sync_mode(_Config) ->
    Supervisor = pes_process_manager:get_supervisor_name(
        pes_sync_executor, pes_process_manager:hash_key(pes_sync_executor, 10)),
    external_termination_request(pes_sync_executor, Supervisor).


external_termination_request_async_mode(_Config) ->
    external_termination_request(pes_async_executor, ?SUPERVISOR_NAME).


crashed_graceful_termination_sync_mode(_Config) ->
    blocking_termination_sync_mode_test(crash_during_termination).


crashed_graceful_termination_async_mode(_Config) ->
    blocking_termination_async_mode_test(crash_during_termination).


server_terminate_crash(_Config) ->
    Supervisor = pes_process_manager:get_supervisor_name(
        pes_sync_executor, pes_process_manager:hash_key(pes_sync_executor, 10)),
    terminate_crash(pes_sync_executor, Supervisor).


slave_terminate_crash(_Config) ->
    terminate_crash(pes_async_executor, ?SUPERVISOR_NAME).


termination_interrupted_by_new_request(_Config) ->
    ?assertEqual({ok, test_ans}, pes:submit_and_await(pes_async_executor, 10, exemplary_request)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(1, ets:info(?SUPERVISOR_NAME, size)),

    pes_executor_mock:set_simulated_termination_manner(
        {interrupt_with_request, pes_async_executor, 10, increment_value}),
    ?assertEqual([ok], pes:send_to_all(pes_async_executor, graceful_termination_request)),
    timer:sleep(timer:seconds(2)), % Time for termination (however, it should be blocked)
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(1, ets:info(?SUPERVISOR_NAME, size)),
    ?assertEqual({ok, 1}, pes:submit_and_await(pes_async_executor, 10, get_value)),

    pes_executor_mock:set_simulated_termination_manner(terminate_successfully),
    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME)), 35),
    ?assertEqual(0, ets:info(?SUPERVISOR_NAME, size)),
    ?assertEqual(graceful_termination_request, pes_executor_mock:get_termination_reason()).


%%%===================================================================
%%% Test skeletons
%%%===================================================================

single_executor_lifecycle(Executor, Function) ->
    ?assertEqual({ok, test_ans}, pes:Function(Executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(1, ets:info(?SUPERVISOR_NAME, size)),

    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME)), 35),
    ?assertEqual(0, ets:info(?SUPERVISOR_NAME, size)),

    ?assertEqual({ok, test_ans}, pes:Function(Executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(1, ets:info(?SUPERVISOR_NAME, size)).


cast_crash(Executor) ->
    ?assertEqual(ok, pes:cast(Executor, 10, decrement_value)),
    ?assertEqual(ok, pes:cast(Executor, 10, {crash_with, throw, cast_error})),
    pes_executor_mock:verify_state(Executor, 10, -1).


init_crash(Executor, Function) ->
    pes_executor_mock:set_initial_crash(true),
    ?assertEqual(?ERROR_INTERNAL_SERVER_ERROR, pes:Function(Executor, <<"exemplary_key">>, exemplary_request)),
    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(?SUPERVISOR_NAME))),
    ?assertEqual(0, ets:info(?SUPERVISOR_NAME, size)),

    pes_executor_mock:set_initial_crash(false),
    ?assertEqual({ok, test_ans}, pes:Function(Executor, <<"exemplary_key">>, exemplary_request)).


blocking_termination_sync_mode_test(TerminationBlockingReason) ->
    Supervisor = pes_process_manager:get_supervisor_name(
        pes_sync_executor, pes_process_manager:hash_key(pes_sync_executor, 10)),
    blocking_termination_test(pes_sync_executor, Supervisor, TerminationBlockingReason).


blocking_termination_async_mode_test(TerminationBlockingReason) ->
    blocking_termination_test(pes_async_executor, ?SUPERVISOR_NAME, TerminationBlockingReason).


blocking_termination_test(Executor, Supervisor, TerminationBlockingReason) ->
    pes_executor_mock:set_simulated_termination_manner(TerminationBlockingReason),
    ?assertEqual(ok, pes:cast(Executor, 10, decrement_value)),
    pes_executor_mock:verify_state(Executor, 10, -1),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(Supervisor))),
    ?assertEqual(1, ets:info(Supervisor, size)),

    timer:sleep(timer:seconds(35)),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(Supervisor))),
    ?assertEqual(1, ets:info(Supervisor, size)),
    pes_executor_mock:verify_state(Executor, 10, -1),
    ?assertEqual(graceful_termination_request, pes_executor_mock:get_termination_reason()),

    pes_executor_mock:set_simulated_termination_manner(terminate_successfully),
    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(Supervisor)), 35),
    ?assertEqual(0, ets:info(Supervisor, size)),
    ?assertEqual(graceful_termination_request, pes_executor_mock:get_termination_reason()).


external_termination_request(Executor, Supervisor) ->
    ?assertEqual(ok, pes:cast(Executor, 10, decrement_value)),
    pes_executor_mock:verify_state(Executor, 10, -1),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(Supervisor))),
    ?assertEqual(1, ets:info(Supervisor, size)),

    ?assertEqual([ok], pes:send_to_all(Executor, graceful_termination_request)),
    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(Supervisor)), 5),
    ?assertEqual(0, ets:info(Supervisor, size)),
    ?assertEqual(graceful_termination_request, pes_executor_mock:get_termination_reason()).


terminate_crash(Executor, Supervisor) ->
    pes_executor_mock:set_simulated_termination_manner(crash_during_termination),
    pes_executor_mock:verify_state(Executor, 10, 0),
    ?assertEqual(1, proplists:get_value(active, supervisor:count_children(Supervisor))),
    ?assertEqual(1, ets:info(Supervisor, size)),

    [{_, ProcessPid, _, _}] = supervisor:which_children(Supervisor),
    ?assertEqual(ok, supervisor:terminate_child(Supervisor, ProcessPid)),
    ?assertEqual(0, proplists:get_value(active, supervisor:count_children(Supervisor))),
    ?assertEqual(0, ets:info(Supervisor, size)),
    ?assertEqual({forced, shutdown}, pes_executor_mock:get_termination_reason()).


%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [pes_minimal_executor, pes_sync_executor, pes_async_executor]} | Config].


end_per_suite(_Config) ->
    ok.


init_per_testcase(Case, Config) ->
    pes_executor_mock:set_simulated_termination_manner(terminate_successfully),
    pes_executor_mock:set_initial_crash(false),
    pes_executor_mock:clear_termination_reason(),
    Executor = case_to_executor(Case),
    {ok, SupervisorPid} = pes:start_root_supervisor(Executor),
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
case_to_executor(single_call_with_ensure_executor_alive_set_to_false) ->
    pes_minimal_executor;
case_to_executor(multiple_calls) ->
    pes_minimal_executor;
case_to_executor(multi_call) ->
    pes_minimal_executor;
case_to_executor(single_executor_lifecycle_sync_mode) ->
    pes_minimal_executor;
case_to_executor(hashing) ->
    pes_minimal_executor;
case_to_executor(submit_not_supported) ->
    pes_minimal_executor;
case_to_executor(acknowledged_cast_not_supported) ->
    pes_minimal_executor;
case_to_executor(long_lasting_call) ->
    pes_minimal_executor;
case_to_executor(single_cast_sync_mode) ->
    pes_sync_executor;
case_to_executor(send_self_cast_sync_mode) ->
    pes_sync_executor;
case_to_executor(preventing_sync_mode_deadlocks) ->
    pes_minimal_executor;
case_to_executor(preventing_self_call) ->
    pes_minimal_executor;
case_to_executor(multiple_supervisors) ->
    pes_sync_executor;
case_to_executor(deferred_graceful_termination_sync_mode) ->
    pes_sync_executor;
case_to_executor(call_crash) ->
    pes_minimal_executor;
case_to_executor(not_implemented_call_callback_crash) ->
    pes_sync_executor;
case_to_executor(cast_crash_sync_mode) ->
    pes_sync_executor;
case_to_executor(init_crash_sync_mode) ->
    pes_minimal_executor;
case_to_executor(crashed_graceful_termination_sync_mode) ->
    pes_sync_executor;
case_to_executor(external_termination_request_sync_mode) ->
    pes_sync_executor;
case_to_executor(server_terminate_crash) ->
    pes_sync_executor;
case_to_executor(_) ->
    pes_async_executor.