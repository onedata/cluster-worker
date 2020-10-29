%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore base of datastore stress performance tests.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_performance_tests_base).
-author("Michal Wrzeszcz").

-include("datastore_test_utils.hrl").

%% API
-export([stress_performance_test_base/1]).

%%%===================================================================
%%% API
%%%===================================================================

stress_performance_test_base(Config) ->
    ct:timetrap({hours, 3}),
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    Repeats = ?config(proc_repeats, Config),
    ProcNum = ?config(proc_num, Config),
    ManyKeys = ?config(many_keys, Config),
    HA = ?config(ha, Config),
    HaMode = ?config(ha_mode, Config),
    MemoryOnly = ?config(memory_only, Config),
    CheckNextWorker = ?config(check_next_worker, Config),

    HANodes = case HA of
        false -> 1;
        true -> 2
    end,
    lists:foreach(fun(W) ->
        ?assertEqual(ok, rpc:call(W, ha_datastore, change_config, [HANodes, HaMode]))
    end, Workers),

    ConsistentHashingNodes = rpc:call(Worker, consistent_hashing, get_all_nodes, []),
    KeyAnsWorkersList = lists:map(fun(_) ->
        Key = datastore_key:new(),
        KeyWorker = rpc:call(Worker, datastore_key, any_responsible_node, [Key]),
        NextWorker = get_next_worker(KeyWorker, ConsistentHashingNodes ++ ConsistentHashingNodes),
        {Key, KeyWorker, NextWorker}
    end, lists:seq(1, ProcNum)),

    Ans = run_stress_procs(Repeats, ManyKeys, MemoryOnly, CheckNextWorker, KeyAnsWorkersList),
    ct:print("Test: repeats ~p, procs ~p, many keys ~p, HA ~p, HA mode ~p, result ~p",
        [Repeats, ProcNum, ManyKeys, HA, HaMode, lists:sum(Ans) / Repeats / ProcNum]),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

run_stress_procs(_, _, _, _, []) ->
    [];
run_stress_procs(Repeats, ManyKeys, MemoryOnly, CheckNextWorker, [{Key, KeyWorker, NextWorker} | Tail]) ->
    run_stress_proc(Key, KeyWorker, NextWorker, Repeats, ManyKeys, MemoryOnly, CheckNextWorker),
    Results = run_stress_procs(Repeats, ManyKeys, MemoryOnly, CheckNextWorker, Tail),
    [get_stress_proc_results() | Results].

run_stress_proc(Key, Worker, NextWorker, Repeats, ManyKeys, MemoryOnly, CheckNextWorker) ->
    Master = self(),
    spawn(Worker, fun() ->
        try
            Ctx = case MemoryOnly of
                true -> performance_test_record:get_memory_only_ctx();
                _ -> performance_test_record:get_ctx()
            end,
            Doc = #document{key = Key, value = #performance_test_record{}},

            Time0 = os:timestamp(), % @TODO VFS-6841 switch to the clock module (all occurrences in this module)
            save_loop(Doc, ManyKeys, Ctx, CheckNextWorker, NextWorker, Repeats),

            Master ! {slave_ans, {ok, timer:now_diff(os:timestamp(), Time0)}}
        catch
            E1:E2 -> Master ! {slave_ans, {error, E1, E2, erlang:get_stacktrace()}}
        end
    end).

save_loop(_, _, _, _, _, 0) ->
    ok;
save_loop(#document{key = Key} = Doc, true, Ctx, CheckNextWorker, Worker, Num) ->
    {ok, Doc2} = ?assertMatch({ok, _},  datastore_model:save(Ctx, Doc#document{key = datastore_key:new_adjacent_to(Key)})),
    check_memory_copy(CheckNextWorker, Worker, Doc2),
    save_loop(Doc, true, Ctx, CheckNextWorker, Worker, Num - 1);
save_loop(Doc, false, Ctx, CheckNextWorker, Worker, Num) ->
    {ok, Doc2} = ?assertMatch({ok, _},  datastore_model:save(Ctx, Doc)),
    check_memory_copy(CheckNextWorker, Worker, Doc2),
    save_loop(Doc2, false, Ctx, CheckNextWorker, Worker, Num - 1).

get_stress_proc_results() ->
    Ans = receive
        {slave_ans, RecAns} -> RecAns
    after
        timer:minutes(30) -> timeout
    end,
    {ok, Time} = ?assertMatch({ok, _}, Ans),
    Time.

get_next_worker(Chosen, [Chosen, Next | _]) ->
    Next;
get_next_worker(Chosen, [_ | Workers]) ->
    get_next_worker(Chosen, Workers).

check_memory_copy(false, _Worker, _Doc) ->
    ok;
check_memory_copy(true, Worker, #document{key = Key} = Doc) ->
    UniqueKey = ?UNIQUE_KEY(performance_test_record, Key),
    Ctx = datastore_multiplier:extend_name(UniqueKey, ?MEM_CTX(performance_test_record)),
    ?assertMatch({ok, Doc}, rpc:call(Worker, ets_driver, get, [Ctx, UniqueKey]), 5).