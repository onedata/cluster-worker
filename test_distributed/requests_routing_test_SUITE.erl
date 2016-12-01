%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This test checks requests routing inside OP cluster.
%%% @end
%%%--------------------------------------------------------------------
-module(requests_routing_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([simple_call_test/1, direct_cast_test/1, redirect_cast_test/1, mixed_cast_test/1]).
-export([mixed_cast_test_core/1]).
-export([singleton_module_test/1, simple_call_test_base/1,
  direct_cast_test_base/1, redirect_cast_test_base/1, mixed_cast_test_base/1]).

-define(TEST_CASES, [
  singleton_module_test, simple_call_test, direct_cast_test, redirect_cast_test, mixed_cast_test
]).

-define(PERFORMANCE_TEST_CASES, [
  simple_call_test, direct_cast_test, redirect_cast_test, mixed_cast_test
]).

all() ->
    ?ALL(?TEST_CASES, ?PERFORMANCE_TEST_CASES).

-define(REQUEST_TIMEOUT, timer:seconds(10)).
-define(REPEATS, 100).
-define(SUCCESS_RATE, 95).

%%%===================================================================
%%% Test functions
%%%===================================================================

%%%===================================================================

singleton_module_test(Config) ->
  [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
  lists:foreach(fun(W) ->
    ?assertEqual(ok, gen_server:call({?NODE_MANAGER_NAME, W}, {apply, node_manager, init_workers, []}))
  end, Workers),

  ?assertMatch({ok, _}, rpc:call(Worker1, supervisor, get_childspec, [?MAIN_WORKER_SUPERVISOR_NAME, sample_module])),
  ?assertEqual({error,not_found}, rpc:call(Worker2, supervisor, get_childspec, [?MAIN_WORKER_SUPERVISOR_NAME, sample_module])),

  ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [sample_module, ping, ?REQUEST_TIMEOUT])),
  ?assertEqual(pong, rpc:call(Worker2, worker_proxy, call, [sample_module, ping, ?REQUEST_TIMEOUT])),

  ok.

simple_call_test(Config) ->
    ?PERFORMANCE(Config, [
            {repeats, ?REPEATS},
            {success_rate, ?SUCCESS_RATE},
            {description, "Performs one worker_proxy call per use case"},
            {config, [{name, simple_call}, {description, "Basic config for test"}]}
        ]).

simple_call_test_base(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),

    T1 = erlang:monotonic_time(milli_seconds),
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [dns_worker, ping, ?REQUEST_TIMEOUT])),
    T2 = erlang:monotonic_time(milli_seconds),
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [{dns_worker, Worker1}, ping, ?REQUEST_TIMEOUT])),
    T3 = erlang:monotonic_time(milli_seconds),
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [{dns_worker, Worker2}, ping, ?REQUEST_TIMEOUT])),
    T4 = erlang:monotonic_time(milli_seconds),

    [
        #parameter{name = dispatcher, value = T2 - T1, unit = "ms",
            description = "Time of call without specified target node (decision made by dispatcher)"},
        #parameter{name = local_processing, value = T3 - T2, unit = "ms",
            description = "Time of call with default arguments processed locally"},
        #parameter{name = remote_processing, value = T4 - T3, unit = "ms",
            description = "Time of call with default arguments delegated to other node"}
    ].

%%%===================================================================

direct_cast_test(Config) ->
    ?PERFORMANCE(Config, [
            {repeats, ?REPEATS},
            {success_rate, ?SUCCESS_RATE},
            {parameters, [
                [{name, proc_num}, {value, 10}, {description, "Number of threads used during the test."}],
                [{name, proc_repeats}, {value, 10}, {description, "Number of operations done by single threads."}]
            ]},
            {description, "Performs many one worker_proxy calls (dispatcher decide where they will be processed), using many threads"},
            {config, [{name, direct_cast},
                {parameters, [
                    [{name, proc_num}, {value, 100}],
                    [{name, proc_repeats}, {value, 100}]
                ]},
                {description, "Basic config for test"}
            ]}
        ]).

direct_cast_test_base(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ProcSendNum = ?config(proc_repeats, Config),
    ProcNum = ?config(proc_num, Config),

    TestProc = fun() ->
        Self = self(),
        SendReq = fun(MsgId) ->
            ?assertEqual(ok, rpc:call(Worker, worker_proxy, cast, [dns_worker, ping, {proc, Self}, MsgId]))
        end,

        BeforeProcessing = erlang:monotonic_time(milli_seconds),
        for(1, ProcSendNum, SendReq),
        count_answers(ProcSendNum),
        AfterProcessing = erlang:monotonic_time(milli_seconds),
        AfterProcessing - BeforeProcessing
    end,

    Ans = spawn_and_check(TestProc, ProcNum),
    ?assertMatch({ok, _}, Ans),
    {_, Times} = Ans,
    #parameter{name = routing_time, value = Times, unit = "ms",
        description = "Aggregated time of all calls performed via dispatcher"}.
%%%===================================================================

redirect_cast_test(Config) ->
    ?PERFORMANCE(Config, [
            {repeats, ?REPEATS},
            {success_rate, ?SUCCESS_RATE},
            {parameters, [
                [{name, proc_num}, {value, 10}, {description, "Number of threads used during the test."}],
                [{name, proc_repeats}, {value, 10}, {description, "Number of operations done by single threads."}]
            ]},
            {description, "Performs many one worker_proxy calls with default arguments but delegated to other node, using many threads"},
            {config, [{name, redirect_cast},
                {parameters, [
                    [{name, proc_num}, {value, 100}],
                    [{name, proc_repeats}, {value, 100}]
                ]},
                {description, "Basic config for test"}
            ]}
        ]).

redirect_cast_test_base(Config) ->
        [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
        ProcSendNum = ?config(proc_repeats, Config),
        ProcNum = ?config(proc_num, Config),

        TestProc = fun() ->
            Self = self(),
            SendReq = fun(MsgId) ->
                ?assertEqual(ok, rpc:call(Worker1, worker_proxy, cast, [{dns_worker, Worker2}, ping, {proc, Self}, MsgId]))
            end,

            BeforeProcessing = erlang:monotonic_time(milli_seconds),
            for(1, ProcSendNum, SendReq),
            count_answers(ProcSendNum),
            AfterProcessing = erlang:monotonic_time(milli_seconds),
            AfterProcessing - BeforeProcessing
        end,

        Ans = spawn_and_check(TestProc, ProcNum),
        ?assertMatch({ok, _}, Ans),
        {_, Times} = Ans,
        #parameter{name = routing_time, value = Times, unit = "ms",
            description = "Aggregated time of all calls with default arguments but delegated to other node"}.

%%%===================================================================

mixed_cast_test(Config) ->
    ?PERFORMANCE(Config, [
            {repeats, ?REPEATS},
            {success_rate, ?SUCCESS_RATE},
            {parameters, [
                [{name, proc_num}, {value, 10}, {description, "Number of threads used during the test."}],
                [{name, proc_repeats}, {value, 10}, {description, "Number of operations done by single threads."}]
            ]},
            {description, "Performs many one worker_proxy calls with various arguments"},
            {config, [{name, short_procs},
                {parameters, [
                    [{name, proc_num}, {value, 100}],
                    [{name, proc_repeats}, {value, 1}]
                ]},
                {description, "Multiple threads, each thread does only one operation of each type"}
            ]},
            {config, [{name, one_proc},
                {parameters, [
                    [{name, proc_num}, {value, 1}],
                    [{name, proc_repeats}, {value, 100}]
                ]},
                {description, "One thread does many operations"}
            ]},
            {config, [{name, long_procs},
                {parameters, [
                    [{name, proc_num}, {value, 100}],
                    [{name, proc_repeats}, {value, 100}]
                ]},
                {description, "Many threads do many operations"}
            ]}
        ]).

mixed_cast_test_base(Config) ->
    mixed_cast_test_core(Config).

%%%===================================================================
%%% Functions cores (to be reused in stress tests)
%%%===================================================================

mixed_cast_test_core(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    ProcSendNum = ?config(proc_repeats, Config),
    ProcNum = ?config(proc_num, Config),

    TestProc = fun() ->
        Self = self(),
        SendReq = fun(MsgId) ->
            ?assertEqual(ok, rpc:call(Worker1, worker_proxy, cast, [{dns_worker, Worker1}, ping, {proc, Self}, 2 * MsgId - 1])),
            ?assertEqual(ok, rpc:call(Worker1, worker_proxy, cast, [{dns_worker, Worker2}, ping, {proc, Self}, 2 * MsgId]))
        end,

        BeforeProcessing = erlang:monotonic_time(milli_seconds),
        for(1, ProcSendNum, SendReq),
        count_answers(2 * ProcSendNum),
        AfterProcessing =erlang:monotonic_time(milli_seconds),
        AfterProcessing - BeforeProcessing
    end,

    Ans = spawn_and_check(TestProc, ProcNum),
    ?assertMatch({ok, _}, Ans),
    {_, Times} = Ans,
    #parameter{name = routing_time, value = Times, unit = "ms",
        description = "Aggregated time of all calls"}.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(singleton_module_test, Config) ->
  Workers = ?config(cluster_worker_nodes, Config),
  ok = test_node_starter:load_modules(Workers, [sample_module]),
  test_utils:mock_new(Workers, node_manager_plugin_default),
  test_utils:mock_expect(
    Workers, node_manager_plugin_default, modules_with_args,
    fun () -> [{singleton, sample_module, []}] end),
  Config;

init_per_testcase(_Case, Config) ->
  Config.

end_per_testcase(singleton_module_test, Config) ->
  Workers = ?config(cluster_worker_nodes, Config),
  test_utils:mock_unload(Workers, node_manager_plugin_default);

end_per_testcase(_Case, _Config) ->
  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

spawn_and_check(_Fun, 0) ->
    {ok, 0};

spawn_and_check(Fun, Num) ->
    Master = self(),
    spawn_link(fun() ->
        Ans = Fun(),
        Master ! {ok, Ans}
    end),
    case spawn_and_check(Fun, Num - 1) of
        {ok, Sum} ->
            receive
                {ok, Time} -> {ok, Time + Sum}
            after ?REQUEST_TIMEOUT ->
                {error, timeout}
            end;
        Err ->
            Err
    end.

for(N, N, F) ->
    F(N);
for(I, N, F) ->
    F(I),
    for(I + 1, N, F).

count_answers(Exp) ->
    count_answers(0, Exp).

count_answers(Exp, Exp) ->
    ok;

count_answers(Num, Exp) ->
    NumToBeReceived = Num + 1,
    Ans = receive
              #worker_answer{id = NumToBeReceived, response = Response} ->
                  Response
          after ?REQUEST_TIMEOUT ->
              {error, timeout}
          end,
    ?assertEqual(pong, Ans),
    count_answers(NumToBeReceived, Exp).