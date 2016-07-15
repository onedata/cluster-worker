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
    hierarchical_lock_test/1,
    failure_in_critical_section_test/1]).

all() ->
    ?ALL([
        run_and_update_test,
        run_and_increment_test,
        hierarchical_lock_test,
        failure_in_critical_section_test]).

%%%===================================================================
%%% Test functions
%%%===================================================================

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

hierarchical_lock_test(Config) ->
    % given
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    Self = self(),

    RecordId = <<"some_id">>,
    ?assertEqual({ok, RecordId}, rpc:call(Worker, disk_only_record, save,
        [#document{key = RecordId, value = #disk_only_record{}}])),

    % then
    Targets = [{nth_with_modulo(N, Workers), N, gen_hierarchical_key(N)} || N <- lists:seq(1, 4)],

    Pids = lists:map(fun({W, V, K}) ->
        spawn(fun() ->
            Self ! {self(), rpc:call(W, ?MODULE, critical_fun_update, [RecordId, V, K])}
        end)
    end, Targets),

    lists:foreach(fun(Pid) ->
        ?assertReceivedMatch({Pid, {ok, _}}, ?TIMEOUT)
    end, Pids),
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

%% generates hierarchical key of given level
gen_hierarchical_key(N) ->
    gen_hierarchical_key(1, N).

gen_hierarchical_key(N, N) ->
    [gen_key(N)];
gen_hierarchical_key(X, N) ->
    [gen_key(X) | gen_hierarchical_key(X + 1, N)].

gen_key(X) ->
    <<"level_", (integer_to_binary(X))/binary, "_key">>.