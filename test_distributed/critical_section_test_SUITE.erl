%%%--------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This test verifies critical section module.
%%% @end
%%%--------------------------------------------------------------------
-module(critical_section_test_SUITE).
-author("Mateusz Paciorek").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-define(TIMEOUT, timer:seconds(20)).

-export([all/0, init_per_suite/1]).
-export([critical_fun_update/3, critical_fun_increment/2, failure_test_fun/1]).
-export([
    run_and_update_test/1,
    run_and_increment_test/1,
    failure_in_critical_section_test/1,
    performance_test/1, performance_test_base/1,
    massive_test/1, massive_test_base/1]).
-export([reset_ets_counter/0, get_ets_counter/0, increment_ets_counter/1]).

-define(TEST_CASES, [
    run_and_update_test,
    run_and_increment_test,
    failure_in_critical_section_test,
    massive_test
]).

-define(PERFORMANCE_TEST_CASES, [
    performance_test, massive_test
]).

all() ->
    ?ALL(?TEST_CASES, ?PERFORMANCE_TEST_CASES).

-define(MODEL, ets_only_model).

%===================================================================
% Test functions
%===================================================================

massive_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, access_ops_num}, {value, 1000}, {description, "Number of resource access operations during single test."}],
            [{name, method}, {value, run}, {description, "Number of resource access operations during single test."}]
        ]},
        {description, "Checks critical section when massive number of threads are accessing same resource"},
        {config, [{name, global},
            {parameters, [
                [{name, method}, {value, run_on_global}]
            ]},
            {description, "Uses locks on global module"}
        ]}
    ]).
massive_test_base(Config) ->
    process_flag(trap_exit, true),
    AccessOpsNum = ?config(access_ops_num, Config),
    Method = ?config(method, Config),

    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    Master = self(),
    EtsProc = spawn(Worker, fun() ->
        ets:new(critical_section_test_ets, [named_table, public, set]),
        Master ! ets_created,
        receive
            kill -> ok
        after
            timer:minutes(10) -> ok
        end
    end),
    ?assertReceivedEqual(ets_created, timer:seconds(10)),
    ?assert(rpc:call(Worker, ?MODULE, reset_ets_counter, [])),

    Fun1 = fun() ->
        increment_ets_counter(Worker, Method, 0),
        Master ! test_fun_ok
    end,

    Fun2 = fun() ->
        increment_ets_counter(Worker, Method, 10),
        Master ! test_fun_ok
    end,

    Fun3 = fun() ->
        lists:foreach(fun(_) ->
            increment_ets_counter(Worker, Method, 0)
        end, lists:seq(1, 10)),
        Master ! test_fun_ok
    end,

    Fun4 = fun() ->
        lists:foreach(fun(I) ->
            increment_ets_counter(Worker, Method, 2 * I)
        end, lists:seq(1, 10)),
        Master ! test_fun_ok
    end,

    Funs = [{Fun1, 1}, {Fun2, 1}, {Fun3, 10}, {Fun4, 10}],
    TestCases = [{Fun, W} || Fun <- Funs, W <- [[Worker], Workers]],
    lists:foreach(fun({{Fun, Ratio}, W}) ->
        do_massive_test(Fun, W, AccessOpsNum, Ratio)
    end, TestCases),

    EtsProc ! kill,
    ok.

do_massive_test(Fun, Workers, Processes, Ratio) ->
    [Worker | _] = Workers,
    ?assert(rpc:call(Worker, ?MODULE, reset_ets_counter, [])),
    run_fun(Fun, Workers, [], round(Processes / Ratio)),
    ?assertEqual(Processes, rpc:call(Worker, ?MODULE, get_ets_counter, [])),
    ok.

reset_ets_counter() ->
    ets:insert(critical_section_test_ets, {counter, 0}).

get_ets_counter() ->
    [{counter, C}] = ets:lookup(critical_section_test_ets, counter),
    C.

increment_ets_counter(Sleep) ->
    [{counter, C}] = ets:lookup(critical_section_test_ets, counter),
    case Sleep > 0 of
        true ->
            timer:sleep(Sleep);
        _ ->
            ok
    end,
    ets:insert(critical_section_test_ets, {counter, C + 1}),
    ok.

increment_ets_counter(Worker, Method, Sleep) ->
    ok = apply(critical_section, Method, [<<"increment_ets_counter_key">>, fun() ->
        rpc:call(Worker, ?MODULE, increment_ets_counter, [Sleep])
    end]).

performance_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 100},
        {description, "Performs one critical section and transaction"},
        {config, [{name, basic_config}, {description, "Basic config for test"}]}
    ]).
performance_test_base(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),

    Master = self(),
    TestFun = fun() ->
        _X = 1 + 2,
        Master ! test_fun_ok
    end,

    TestCritical = fun() ->
        critical_section:run(<<"key">>, TestFun)
    end,
    TestCritical2 = fun() ->
        critical_section:run(rand:uniform(), TestFun)
    end,

    T11 = check_time(TestCritical, [Worker]),
    T12 = check_time(TestCritical, Workers),

    T21 = check_time(TestCritical2, [Worker]),
    T22 = check_time(TestCritical2, Workers),

    [
        #parameter{name = critical_parallel_1_node, value = T11, unit = "us",
            description = "Time of 100 executions of critical section on 1 node"},
        #parameter{name = critical_parallel_2_node, value = T12, unit = "us",
            description = "Time of 100 executions of critical section on 2 nodes"},
        #parameter{name = critical_random_1_node, value = T21, unit = "us",
            description = "Time of 100 executions of critical section with random lock key on 1 node"},
        #parameter{name = critical_random_2_node, value = T22, unit = "us",
            description = "Time of 100 executions of critical section with random lock key on 2 nodes"}
    ].

check_time(Fun, Workers) ->
    Stopwatch = stopwatch:start(),
    run_fun(Fun, Workers, [], 100),
    stopwatch:read_micros(Stopwatch).

run_fun(_Fun, _Workers1, _Workers2, 0) ->
    ok;
run_fun(Fun, [], Workers2, Count) ->
    run_fun(Fun, Workers2, [], Count);
run_fun(Fun, [W | Workers1], Workers2, Count) ->
    spawn_link(W, Fun),
    run_fun(Fun, Workers1, [W | Workers2], Count - 1),
    Ans = receive_fun_ans(),
    ?assertEqual(ok, Ans).

receive_fun_ans() ->
    receive
        test_fun_ok ->
            ok;
        {test_fun_error, Error} ->
            {error, Error};
        {'EXIT', _From, normal} ->
            receive_fun_ans();
        {'EXIT', _From, _Reason} = Err ->
            Err
    after
        30000 -> timeout
    end.

run_and_update_test(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    Self = self(),
    Key = <<"someKey">>,
    RecordId = <<"someId">>,
    ?assertMatch({ok, _}, rpc:call(Worker, ?MODEL, save, [
        #document{key = RecordId}
    ])),

    Targets = [{nth_with_modulo(N, Workers), N} || N <- lists:seq(1, 10)],

    Pids = lists:map(fun({W, V}) ->
        spawn(fun() ->
            Self ! {self(), rpc:call(W, ?MODULE, critical_fun_update, [
                RecordId, V, Key
            ])}
        end)
    end, Targets),

    lists:foreach(fun(Pid) ->
        ?assertReceivedMatch({Pid, {ok, _}}, ?TIMEOUT)
    end, Pids),
    ok.

run_and_increment_test(Config) ->
    % given
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    Self = self(),
    Key = <<"some_key">>,
    RecordId = <<"some_id">>,
    ?assertMatch({ok, _}, rpc:call(Worker, ?MODEL, save,
        [#document{key = RecordId, value = 0}])),

    % then
    TargetsNum = 10,
    Targets = [nth_with_modulo(N, Workers) || N <- lists:seq(1, TargetsNum)],

    Pids = lists:map(fun(W) ->
        spawn(fun() ->
            Self ! {self(), rpc:call(W, ?MODULE, critical_fun_increment, [
                RecordId, Key
            ])}
        end)
    end, Targets),

    lists:foreach(fun(Pid) ->
        ?assertReceivedMatch({Pid, ok}, ?TIMEOUT)
    end, Pids),

    ?assertMatch({ok, #document{key = RecordId, value = TargetsNum}},
        rpc:call(Worker, ?MODEL, get, [RecordId])),
    ok.

failure_in_critical_section_test(Config) ->
    % given
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    Key = <<"some_key">>,

    % then
    Pid = spawn(fun() ->
        Self ! {self(), rpc:call(Worker, ?MODULE, failure_test_fun, [Key])}
    end),
    ?assertReceivedEqual({Pid, ok}, ?TIMEOUT),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite([?MODEL], Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% changes value of record, sleeps and checks if value is still the same
critical_fun_update(RecordId, V1, Key) ->
    critical_section:run(Key, fun() ->
        ?MODEL:update(RecordId, fun(_) -> {ok, V1} end),
        timer:sleep(timer:seconds(1)),
        {ok, #document{value = V2}} = ?MODEL:get(RecordId),
        case V1 =:= V2 of
            true -> {ok, V1};
            false -> {error, V1, V2}
        end
    end).

%% reads value of record, sleeps and writes incremented value
critical_fun_increment(RecordId, Key) ->
    critical_section:run(Key, fun() ->
        {ok, #document{value = V}} = ?MODEL:get(RecordId),
        timer:sleep(timer:seconds(1)),
        ?MODEL:update(RecordId, fun(_) -> {ok, V + 1} end),
        ok
    end).

%% throws exception inside critical section, then tries to reacquire lock
failure_test_fun(Key) ->
    catch critical_section:run(Key, fun() -> throw(error) end),
    critical_section:run(Key, fun() -> ok end).

%% returns nth element from list, treating list as ring
nth_with_modulo(N, List) ->
    lists:nth((N rem length(List)) + 1, List).