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
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").

%% export for ct
-export([all/0, init_per_suite/1]).
-export([simple_call_test/1, direct_cast_test/1, redirect_cast_test/1, mixed_cast_test/1]).
-export([mixed_cast_test_core/1]).
-export([singleton_module_test/1, simple_call_test_base/1,
    direct_cast_test_base/1, redirect_cast_test_base/1, mixed_cast_test_base/1,
    tree_test/1]).

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

singleton_module_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(W) ->
        ?assertEqual(ok, gen_server:call({?NODE_MANAGER_NAME, W}, {
            apply, node_manager, init_workers, [
                [{singleton, sample_module, []}]
            ]
        }))
    end, Workers),

    ?assertMatch({ok, _}, rpc:call(Worker1, supervisor, get_childspec, [?MAIN_WORKER_SUPERVISOR_NAME, sample_module])),
    ?assertEqual({error, not_found}, rpc:call(Worker2, supervisor, get_childspec, [?MAIN_WORKER_SUPERVISOR_NAME, sample_module])),

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

    T1 = time_utils:timestamp_millis(),
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [datastore_worker, ping, ?REQUEST_TIMEOUT])),
    T2 = time_utils:timestamp_millis(),
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [{datastore_worker, Worker1}, ping, ?REQUEST_TIMEOUT])),
    T3 = time_utils:timestamp_millis(),
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [{datastore_worker, Worker2}, ping, ?REQUEST_TIMEOUT])),
    T4 = time_utils:timestamp_millis(),

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
            ?assertEqual(ok, rpc:call(Worker, worker_proxy, cast, [datastore_worker, ping, {proc, Self}, MsgId]))
        end,

        BeforeProcessing = time_utils:timestamp_millis(),
        for(1, ProcSendNum, SendReq),
        count_answers(ProcSendNum),
        AfterProcessing = time_utils:timestamp_millis(),
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
            ?assertEqual(ok, rpc:call(Worker1, worker_proxy, cast, [{datastore_worker, Worker2}, ping, {proc, Self}, MsgId]))
        end,

        BeforeProcessing = time_utils:timestamp_millis(),
        for(1, ProcSendNum, SendReq),
        count_answers(ProcSendNum),
        AfterProcessing = time_utils:timestamp_millis(),
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
            ?assertEqual(ok, rpc:call(Worker1, worker_proxy, cast, [{datastore_worker, Worker1}, ping, {proc, Self}, 2 * MsgId - 1])),
            ?assertEqual(ok, rpc:call(Worker1, worker_proxy, cast, [{datastore_worker, Worker2}, ping, {proc, Self}, 2 * MsgId]))
        end,

        BeforeProcessing = time_utils:timestamp_millis(),
        for(1, ProcSendNum, SendReq),
        count_answers(2 * ProcSendNum),
        AfterProcessing = time_utils:timestamp_millis(),
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

init_per_suite(Config) ->
    PostHook = fun(Config2) ->
        Workers = ?config(cluster_worker_nodes, Config2),
        test_utils:mock_new(Workers, sample_module, [non_strict, no_history]),
        test_utils:mock_expect(Workers, sample_module, init, fun(_) ->
            {ok, #{}}
        end),
        test_utils:mock_expect(Workers, sample_module, handle, fun
            (ping) -> pong;
            (healthcheck) -> ok
        end),
        test_utils:mock_expect(Workers, sample_module, cleanup, fun() ->
            ok
        end),
        Config2
    end,
    [{?ENV_UP_POSTHOOK, PostHook} | Config].

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

%%%===================================================================
%%% Manual performance tests functions
%%%===================================================================

-define(SIZE, 1024).
-define(NIL, null).

tree_test(_Config) ->
    test_get(10),
    test_get(100),
    test_get(1000),
    test_get(10000),
    test_get(50000),

    put_test(10),
    put_test(100),
    put_test(1000),
    put_test(5000),

    del_test(10),
    del_test(100),
    del_test(1000),
    del_test(5000),

    test_tree(10),
    test_tree(100),
    test_tree(1000),
    test_tree(10000),
    test_tree(50000),
    ok.

test_get(Size) ->
    Seq = lists:seq(1, Size),
    Tuple = list_to_tuple(lists:duplicate(Size, ?NIL)),
    List = lists:zip(Seq, lists:duplicate(Size, ?NIL)),
    Map = maps:from_list(List),
    GBSet = gb_sets:from_ordset(List),
    GBTree = gb_trees:from_orddict(List),

    T1 = os:timestamp(),
    lists:foreach(fun(I) ->
        erlang:element(I, Tuple)
    end, Seq),
    Diff1 = timer:now_diff(os:timestamp(), T1),

    T2 = os:timestamp(),
    lists:foreach(fun(I) ->
        proplists:get_value(I, List)
    end, Seq),
    Diff2 = timer:now_diff(os:timestamp(), T2),

    T3 = os:timestamp(),
    lists:foreach(fun(I) ->
        maps:get(I, Map)
    end, Seq),
    Diff3 = timer:now_diff(os:timestamp(), T3),

    T4 = os:timestamp(),
    lists:foreach(fun(I) ->
        It = gb_sets:iterator_from({I, ?NIL}, GBSet),
        gb_sets:next(It)
    end, Seq),
    Diff4 = timer:now_diff(os:timestamp(), T4),

    T5 = os:timestamp(),
    lists:foreach(fun(I) ->
        It = gb_trees:iterator_from(I, GBTree),
        gb_trees:next(It)
    end, Seq),
    Diff5 = timer:now_diff(os:timestamp(), T5),

    ct:pal("Get test for size ~p: tuple ~p, list ~p, map ~p, gb_set ~p, gb_tree ~p",
        [Size, Diff1, Diff2, Diff3, Diff4, Diff5]).

put_test(Size) ->
    Seq = lists:seq(1, Size),
    Tuple = list_to_tuple(lists:duplicate(Size, ?NIL)),

    T1 = os:timestamp(),
    lists:foreach(fun(I) ->
        lists:foreach(fun(I) ->
            X = erlang:element(I, Tuple),
            erlang:setelement(I, Tuple, X)
        end, lists:seq(I, Size))
    end, Seq),
    Diff1 = timer:now_diff(os:timestamp(), T1),

    T2 = os:timestamp(),
    lists:foldl(fun(I, List) ->
        [{I, ?NIL} | proplists:delete(I, List)]
    end, [], Seq),
    Diff2 = timer:now_diff(os:timestamp(), T2),

    T3 = os:timestamp(),
    lists:foldl(fun(I, Map) ->
        maps:put(I, ?NIL, Map)
    end, #{}, Seq),
    Diff3 = timer:now_diff(os:timestamp(), T3),

    T4 = os:timestamp(),
    lists:foldl(fun(I, GBSet) ->
        gb_sets:add({I, ?NIL}, GBSet)
    end, gb_sets:new(), Seq),
    Diff4 = timer:now_diff(os:timestamp(), T4),

    T5 = os:timestamp(),
    lists:foldl(fun(I, GBTree) ->
        gb_trees:insert(I, ?NIL, GBTree)
    end, gb_trees:empty(), Seq),
    Diff5 = timer:now_diff(os:timestamp(), T5),

    ct:pal("Put test for size ~p: tuple ~p, list ~p, map ~p, gb_set ~p, gb_tree ~p",
        [Size, Diff1, Diff2, Diff3, Diff4, Diff5]).

del_test(Size) ->
    Seq = lists:seq(1, Size),
    List = lists:zip(Seq, lists:duplicate(Size, ?NIL)),
    Map = maps:from_list(List),
    GBSet = gb_sets:from_ordset(List),
    GBTree = gb_trees:from_orddict(List),

    T2 = os:timestamp(),
    lists:foreach(fun(I) ->
        proplists:delete(I, List)
    end, Seq),
    Diff2 = timer:now_diff(os:timestamp(), T2),

    T3 = os:timestamp(),
    lists:foreach(fun(I) ->
        maps:remove(I, Map)
    end, Seq),
    Diff3 = timer:now_diff(os:timestamp(), T3),

    T4 = os:timestamp(),
    lists:foreach(fun(I) ->
        It = gb_sets:iterator_from({I, ?NIL}, GBSet),
        E = gb_sets:next(It),
        gb_sets:delete_any(E, GBSet)
    end, Seq),
    Diff4 = timer:now_diff(os:timestamp(), T4),

    T5 = os:timestamp(),
    lists:foreach(fun(I) ->
        gb_trees:delete_any(I, GBTree)
    end, Seq),
    Diff5 = timer:now_diff(os:timestamp(), T5),

    ct:pal("Del test for size ~p: list ~p, map ~p, gb_set ~p, gb_tree ~p",
        [Size, Diff2, Diff3, Diff4, Diff5]).

test_tree(Size) ->
    Seq = lists:seq(1, Size),
    Seq2 = lists:seq(1, Size, 2),
    Seq3 = lists:seq(1, Size, 2),
    T1 = os:timestamp(),
    Tree1 = lists:foldl(fun(I, GBTree) ->
        gb_trees:insert(I, ?NIL, GBTree)
    end, gb_trees:empty(), Seq),
    Diff1 = timer:now_diff(os:timestamp(), T1),

    T2 = os:timestamp(),
    Tree2 = lists:foldl(fun(I, GBTree) ->
        gb_trees:delete_any(I, GBTree)
    end, Tree1, Seq2),
    Diff2 = timer:now_diff(os:timestamp(), T2),

    T3 = os:timestamp(),
    lists:foldl(fun(I, GBTree) ->
        gb_trees:insert(I, ?NIL, GBTree)
    end, Tree2, Seq3),
    Diff3 = timer:now_diff(os:timestamp(), T3),

    ct:pal("Tree test for size ~p: add 1: ~p, del half: ~p, add 2: ~p",
        [Size, Diff1, Diff2, Diff3]).