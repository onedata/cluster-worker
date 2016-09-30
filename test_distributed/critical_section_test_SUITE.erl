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

-include("modules/datastore/datastore_models_def.hrl").
-include("datastore_test_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-define(TIMEOUT, timer:seconds(20)).

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2]).
-export([critical_fun_update/3, critical_fun_increment/2, failure_test_fun/1]).
-export([
    run_and_update_test/1,
    run_and_increment_test/1,
    failure_in_critical_section_test/1,
    performance_test/1, performance_test_base/1]).

-define(TEST_CASES, [
    run_and_update_test,
    run_and_increment_test,
    failure_in_critical_section_test]).

-define(PERFORMANCE_TEST_CASES, [
    performance_test
]).

all() ->
    ?ALL(?TEST_CASES, ?PERFORMANCE_TEST_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

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
        _X = 1+2,
        Master ! test_fun_ok
    end,

    TestCritical = fun() ->
        critical_section:run(<<"key">>, TestFun)
    end,
    TestCritical2 = fun() ->
        critical_section:run(random:uniform(), TestFun)
    end,

    TestTransaction = fun() ->
        datastore:run_transaction(cache_controller, <<"key">>, TestFun)
    end,
    TestTransaction2 = fun() ->
        datastore:run_transaction(cache_controller, float_to_binary(random:uniform()), TestFun)
    end,

    T1 = check_time(TestCritical, [Worker]),
    T2 = check_time(TestTransaction, [Worker]),
    T3 = check_time(TestCritical, Workers),
    T4 = check_time(TestTransaction, Workers),

    ct:print("~p", [{T1, T2, T3, T4}]),

    T12 = check_time(TestCritical2, [Worker]),
    T22 = check_time(TestTransaction2, [Worker]),
    T32 = check_time(TestCritical2, Workers),
    T42 = check_time(TestTransaction2, Workers),

    ct:print("~p", [{T12, T22, T32, T42}]),

    [
        #parameter{name = critical_parallel_1_node, value = T1, unit = "us",
            description = "Time of 100 executions of critical section on 1 node"},
        #parameter{name = transaction_parallel_1_node, value = T2, unit = "us",
            description = "Time of 100 executions of transaction on 1 node"},
        #parameter{name = critical_parallel_2_node, value = T3, unit = "us",
            description = "Time of 100 executions of critical section on 2 nodes"},
        #parameter{name = transaction_parallel_2_node, value = T4, unit = "us",
            description = "Time of 100 executions of transaction on 2 nodes"},
        #parameter{name = critical_random_1_node, value = T12, unit = "us",
            description = "Time of 100 executions of critical section with random lock key on 1 node"},
        #parameter{name = transaction_random_1_node, value = T22, unit = "us",
            description = "Time of 100 executions of transaction with random lock key on 1 node"},
        #parameter{name = critical_random_2_node, value = T32, unit = "us",
            description = "Time of 100 executions of critical section with random lock key on 2 nodes"},
        #parameter{name = transaction_random_2_node, value = T42, unit = "us",
            description = "Time of 100 executions of transaction with random lock key on 2 nodes"}
    ].

check_time(Fun, Workers) ->
    StartTime = os:timestamp(),
    run_fun(Fun, Workers, [], 100),
    Now = os:timestamp(),
    timer:now_diff(Now, StartTime).

run_fun(_Fun, _Workers1, _Workers2, 0) ->
    ok;
run_fun(Fun, [], Workers2, Count) ->
    run_fun(Fun, Workers2, [], Count);
run_fun(Fun, [W | Workers1], Workers2, Count) ->
    spawn(W, Fun),
    run_fun(Fun, Workers1, [W | Workers2], Count - 1),
    Ans = receive
        test_fun_ok -> ok
    after
        5000 -> timeout
    end,
    ?assertEqual(ok, Ans).

run_and_update_test(Config) ->
    % given
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    Self = self(),
    Key = <<"some_key">>,

    RecordId = <<"some_id">>,
    ?assertEqual({ok, RecordId}, rpc:call(Worker, disk_only_record, save,
        [#document{key = RecordId, value = #disk_only_record{}}])),

    % then
    Targets = [{nth_with_modulo(N, Workers), N} || N <- lists:seq(1, 10)],

    Pids = lists:map(fun({W, V}) ->
        spawn(fun() ->
            Self ! {self(), rpc:call(W, ?MODULE, critical_fun_update, [RecordId, V, Key])}
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
    ?assertEqual({ok, RecordId}, rpc:call(Worker, disk_only_record, save,
        [#document{key = RecordId, value = #disk_only_record{field1 = 0}}])),

    % then
    Targets = [nth_with_modulo(N, Workers) || N <- lists:seq(1, 10)],

    Pids = lists:map(fun(W) ->
        spawn(fun() ->
            Self ! {self(), rpc:call(W, ?MODULE, critical_fun_increment, [RecordId, Key])}
        end)
    end, Targets),

    lists:foreach(fun(Pid) ->
        ?assertReceivedMatch({Pid, ok}, ?TIMEOUT)
    end, Pids),

    ?assertEqual({ok, #document{key = RecordId, value = #disk_only_record{field1 = length(Targets)}}},
        rpc:call(Worker, disk_only_record, get, [RecordId])),
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
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:enable_datastore_models(Workers, [disk_only_record]),
    Config.

end_per_testcase(_, _) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% changes value of record, sleeps and checks if value is still the same
critical_fun_update(RecordId, V1, Key) ->
    critical_section:run(Key, fun() ->
        disk_only_record:update(RecordId, #{field1 => V1}),
        timer:sleep(timer:seconds(1)),
        {ok, #document{value = #disk_only_record{field1 = V2}}} =
            disk_only_record:get(RecordId),
        case V1 =:= V2 of
            true ->
                {ok, V1};
            false ->
                {error, V1, V2}
        end
    end).

%% reads value of record, sleeps and writes incremented value
critical_fun_increment(RecordId, Key) ->
    critical_section:run(Key, fun() ->
        {ok, #document{value = #disk_only_record{field1 = V}}} =
            disk_only_record:get(RecordId),
        timer:sleep(timer:seconds(1)),
        disk_only_record:update(RecordId, #{field1 => V+1}),
        ok
    end).

%% throws exception inside critical section, then tries to reacquire lock
failure_test_fun(Key) ->
    catch critical_section:run(Key, fun() -> throw(error) end),
    critical_section:run(Key, fun() -> ok end).

%% returns nth element from list, treating list as ring
nth_with_modulo(N, List) ->
    lists:nth((N rem length(List)) + 1, List).