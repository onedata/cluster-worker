%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains CouchBase driver tests.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_driver_test_SUITE).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("datastore_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).

%% tests
-export([
    save_should_return_doc/1,
    save_should_increment_seq_counter/1,
    save_should_not_increment_seq_counter/1,
    save_should_create_change_doc/1,
    save_should_return_missing_error/1,
    save_should_return_already_exists_error/1,
    get_should_return_doc/1,
    get_should_return_missing_error/1,
    update_should_change_doc/1,
    delete_should_remove_doc/1,
    delete_should_return_missing_error/1,
    save_get_delete_should_return_success/1,
    get_counter_should_return_value/1,
    get_counter_should_return_default_value/1,
    get_counter_should_return_missing_error/1,
    update_counter_should_return_default_value/1,
    update_counter_should_update_value/1,
    save_design_doc_should_return_success/1,
    save_view_doc_should_return_success/1,
    save_spatial_view_doc_should_return_success/1,
    get_design_doc_should_return_doc/1,
    get_design_doc_should_return_missing_error/1,
    delete_design_doc_should_return_success/1,
    delete_design_doc_should_return_missing_error/1,
    query_view_should_return_empty_result/1,
    query_view_should_return_result/1,
    query_view_should_return_missing_error/1,
    query_view_should_return_result_with_string_key/1,
    query_view_should_return_result_with_integer_key/1,
    query_view_should_parse_empty_opts/1,
    query_view_should_parse_all_opts/1,
    query_view_should_parse_all_opts_spatial/1,
    query_view_should_parse_all_opts_reduce/1,
    query_view_should_parse_all_opts_reduce1/1,
    get_buckets_should_return_all_buckets/1,
    cberl_test/1,
    expired_doc_should_not_exist/1
]).

%% test_bases
-export([
    save_get_delete_should_return_success_base/1,
    cberl_test_base/1
]).

all() ->
    ?ALL([
        save_should_return_doc,
        save_should_increment_seq_counter,
        save_should_not_increment_seq_counter,
        save_should_create_change_doc,
        save_should_return_missing_error,
        save_should_return_already_exists_error,
        get_should_return_doc,
        get_should_return_missing_error,
        update_should_change_doc,
        delete_should_remove_doc,
        delete_should_return_missing_error,
        save_get_delete_should_return_success,
        get_counter_should_return_value,
        get_counter_should_return_default_value,
        get_counter_should_return_missing_error,
        update_counter_should_return_default_value,
        update_counter_should_update_value,
        save_design_doc_should_return_success,
        save_view_doc_should_return_success,
        save_spatial_view_doc_should_return_success,
        get_design_doc_should_return_doc,
        get_design_doc_should_return_missing_error,
        delete_design_doc_should_return_success,
        delete_design_doc_should_return_missing_error,
        query_view_should_return_empty_result,
        query_view_should_return_result,
        query_view_should_return_missing_error,
        query_view_should_return_result_with_integer_key,
        query_view_should_return_result_with_string_key,
        query_view_should_parse_empty_opts,
        query_view_should_parse_all_opts,
        query_view_should_parse_all_opts_spatial,
        query_view_should_parse_all_opts_reduce,
        query_view_should_parse_all_opts_reduce1,
        get_buckets_should_return_all_buckets,
        cberl_test,
        expired_doc_should_not_exist
    ], [
        save_get_delete_should_return_success,
        cberl_test
    ]).

-define(MODEL, disc_only_model).
-define(CTX, ?DISC_CTX).
-define(EMPTY_KEY(N), ?TERM("key_empty", N)).
-define(DURABLE_KEY(N), ?TERM("key_durable", N)).
-define(VALUE, ?MODEL_VALUE(?MODEL, 1)).
-define(DOC, ?DOC(1)).
-define(DOC(N), ?BASE_DOC(?KEY(N), ?VALUE)).
-define(DURABLE_DOC(N), ?BASE_DOC(?DURABLE_KEY(N), ?VALUE)).
-define(VIEW_FUNCTION, ?VIEW_FUNCTION(<<"meta.id">>)).
-define(VIEW_FUNCTION(Key), <<"function (doc, meta) {\r\n"
                              "  emit(", Key/binary, ", null);\r\n"
                              "}\r\n">>).
-define(DESIGN_EJSON(ViewFunction), {[{<<"views">>,
    {[{?VIEW,
        {[{<<"map">>, ViewFunction}]}
    }]}
}]}).
-define(DESIGN_EJSON, ?DESIGN_EJSON(?VIEW_FUNCTION)).
-define(DESIGN_EJSON_REDUCE, {[{<<"views">>,
    {[{?VIEW,
        {[{<<"map">>, ?VIEW_FUNCTION},
        {<<"reduce">>, <<"_count">>}]}
    }]}
}]}).
-define(DESIGN_EJSON_SPATIAL, {[
    {<<"spatial">>, {[
        {?VIEW_SPATIAL, ?VIEW_FUNCTION}
    ]}}
]}).

-define(DURABLE(Value), ?PERF_PARAM(durable, Value, "",
    "Perform save operation with durability check.")).

-define(ATTEMPTS, 60).

%%%===================================================================
%%% Test functions
%%%===================================================================

cberl_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 100},
        {success_rate, 95},
        {parameters, [
            ?PERF_PARAM(ops_list, [store, store_durable, get, get_empty],
                "", "List of operations on cberl"),
            ?PERF_PARAM(procs_per_op, 10, "",
                "Number of processes that execute each operation"),
            ?PERF_PARAM(batch_size, 100, "",
                "Batch size (numer of docs/keys in each batch)"),
            ?PERF_PARAM(repeats, 20, "", "List of operations on cberl"),
            ?PERF_PARAM(single_client, true, "", "Use one client for all connections")
        ]},
        {description, "Tests operations on cberl_nif"},
        ?PERF_CFG(single_client, [
            [{name, procs_per_op}, {value, 50}],
            [{name, batch_size}, {value, 1000}],
            [{name, repeats}, {value, 10}]
        ]),
        ?PERF_CFG(many_clients, [
            [{name, procs_per_op}, {value, 1}],
            [{name, batch_size}, {value, 1000}],
            [{name, repeats}, {value, 20}],
            [{name, single_client}, {value, false}]
        ])
    ]).
cberl_test_base(Config) ->
    OpsList = ?config(ops_list, Config),
    ProcsPerOperation = ?config(procs_per_op, Config),
    BatchSize = ?config(batch_size, Config),
    Repeats = ?config(repeats, Config),
    SingleClient = ?config(single_client, Config),

    Timeout = timer:seconds(180),
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),

    ClientPid = spawn(Worker, fun() ->
        Cli = case SingleClient of
            true ->
                {ok, C} = cberl_nif:new(),
                C;
            _ ->
                undefined
        end,
        client_proc(Cli)
    end),

    Fun = fun(OperationType, ProcNum) ->
        spawn_link(Worker, fun() ->
            try
                DbHosts = couchbase_config:get_hosts(),
                Host = lists:foldl(fun(DbHost, Acc) ->
                    <<Acc/binary, ";", DbHost/binary>>
                end, hd(DbHosts), tl(DbHosts)),
                Opts = get_connect_opts(),
                [Bucket] = couchbase_config:get_buckets(),

                {ok, Client} = case SingleClient of
                    false ->
                        cberl_nif:new();
                    _ ->
                        ClientPid ! {get_client, self()},
                        receive
                            {client, Cli} ->
                                {ok, Cli}
                        end
                end,

                {ok, Ref} = cberl_nif:connect(
                    self(), Client, Host, <<>>, <<>>, Bucket, Opts
                ),
                {ok, Connection} = receive
                    {Ref, {ok, C}} ->
                        {ok, C};
                    {Ref, {error, Reason}} ->
                        {error, Reason}
                after
                    Timeout -> {error, timeout}
                end,
                From = self(),

                Response2 = lists:foldl(fun(Num0, Acc) ->
                    Requests = lists:map(fun(Num) ->
                        get_request(Num, OperationType)
                    end, lists:seq(Num0 * BatchSize + 1 + ProcNum * Repeats * (BatchSize + 1),
                        Num0 * (BatchSize + 1) + ProcNum * Repeats * (BatchSize + 1))),

                    T1 = clock:timestamp_micros(),
                    {ok, Ref2} = apply(cberl_nif, map_to_cberl_req(OperationType),
                        [From, Client, Connection, Requests]),
                    {ok, Response, ResponseList} = receive
                        {Ref2, R} ->
                            Time = clock:timestamp_micros() - T1,
                            {ok, AnsList} = R,
                            lists:foreach(fun(A) ->
                                 case A of
                                     {_, {ok, _}} -> ok;
                                     {_, {ok, _, _, _}} -> ok;
                                     {_, {error, key_enoent}} -> ok
                                 end
                            end, AnsList),
                            {ok, Time, AnsList}
                    after
                        Timeout -> {error, timeout}
                    end,

                    case OperationType of
                        store_durable ->
                            DurableRequests = lists:map(fun({K, {ok, Cas}}) ->
                                {K, Cas}
                            end, ResponseList),

                            T2 = clock:timestamp_micros(),
                            {ok, Ref3} = apply(cberl_nif, durability,
                                [From, Client, Connection, DurableRequests, {1, -1}]),

                            {ok, Response2} = receive
                                {Ref3, R2} ->
                                    Time2 = clock:timestamp_micros() - T2,
                                    {ok, AnsList2} = R2,
                                    lists:foreach(fun(A) ->
                                        case A of
                                            {_, {ok, _}} -> ok;
                                            {_, {ok, _, _, _}} -> ok;
                                            {_, {error, key_enoent}} -> ok
                                        end
                                    end, AnsList2),
                                    {ok, Time2}
                            after
                                Timeout -> {error, timeout}
                            end,

                            Acc + Response + Response2;
                        _ ->
                            Acc + Response
                    end
                end, 0, lists:seq(1, Repeats)),

                Self ! {self(), {ok, Response2}}
            catch
                E1:E2 ->
                    Self ! {self(), {error, {E1, E2, erlang:get_stacktrace()}}}
            end
        end)
    end,

    Pids = lists:foldl(fun(Num, Acc1) ->
        Acc1 ++ lists:foldl(fun(Op, Acc2) ->
            [{Fun(Op, Num), Op} | Acc2]
        end, [], OpsList)
    end, [], lists:seq(1, ProcsPerOperation)),

    FinalAns0 = lists:foldl(fun({Pid, Op}, Acc) ->
        {ok, Ans} = receive
            {Pid, A} ->
                ?assertMatch({ok, _}, A),
                A
        after
            5*Timeout -> {error, timeout}
        end,
        Value = maps:get(Op, Acc),
        maps:put(Op, Value + Ans, Acc)
    end, maps:from_list(lists:map(fun(O) -> {O, 0} end, OpsList)), Pids),

    FinalAns = maps:map(fun(_K, V) -> V / Repeats/ ProcsPerOperation end, FinalAns0),

    ct:pal("Avg batch times: ~p", [FinalAns]),
    ClientPid ! stop,

    lists:foldl(fun({K, V}, Acc) ->
        [#parameter{name = K, value = V, unit = "us",
            description = "Time of operation"} | Acc]
    end, [], maps:to_list(FinalAns)).

save_should_return_doc(Config) ->
    Timestamp = datastore_config:get_timestamp(),
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, _, Doc} = ?assertMatch({ok, _, #document{}},
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY, ?DOC])
    ),
    ?assertEqual(?KEY, Doc#document.key),
    ?assertEqual(?VALUE, Doc#document.value),
    ?assertEqual(?SCOPE, Doc#document.scope),
    ?assertEqual(1, Doc#document.seq),
    ?assert(is_integer(Doc#document.timestamp)),
    ?assert(Doc#document.timestamp >= Timestamp),
    ?assertEqual(false, Doc#document.deleted),
    ?assertEqual(1, Doc#document.version).

save_should_increment_seq_counter(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY, ?DOC]),
    ?assertMatch({ok, _, 1}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_key(?SCOPE)]
    )).

save_should_not_increment_seq_counter(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, _, Doc} = ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver,
        save, [?CTX#{no_seq => true}, ?KEY, ?DOC]
    )),
    ?assertEqual(null, Doc#document.seq),
    ?assertEqual(null, Doc#document.timestamp),
    ?assertEqual({error, not_found}, rpc:call(Worker, couchbase_driver,
        get_counter, [?CTX, couchbase_changes:get_seq_key(?SCOPE)]
    )).

save_should_create_change_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY, ?DOC]),
    {ok, _, {Props}} = ?assertMatch({ok, _, _}, rpc:call(Worker,
        couchbase_driver, get, [
            ?CTX, couchbase_changes:get_change_key(?SCOPE, 1)
        ]
    )),
    ?assertEqual(<<"seq">>, proplists:get_value(<<"_record">>, Props)),
    ?assertEqual(?KEY, proplists:get_value(<<"key">>, Props)),
    Pid = binary_to_term(base64:decode(proplists:get_value(<<"pid">>, Props))),
    ?assertEqual(true, is_pid(Pid)),
    ?assertEqual(pong, gen_server:call(Pid, ping)).

save_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, not_found},
        rpc:call(Worker, couchbase_driver, save, [?CTX#{cas => 1}, ?KEY, ?DOC])
    ).

save_should_return_already_exists_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, _, _} = ?assertMatch({ok, _, #document{}},
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY, ?DOC])
    ),
    ?assertEqual({error, already_exists},
        rpc:call(Worker, couchbase_driver, save, [?CTX#{cas => 1}, ?KEY, ?DOC])
    ).

get_should_return_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, Cas, Doc} = rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY, ?DOC]),
    ?assertEqual({ok, Cas, Doc}, rpc:call(Worker, couchbase_driver, get,
        [?CTX, ?KEY]
    )).

get_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, not_found}, rpc:call(Worker, couchbase_driver, get,
        [?CTX, ?KEY]
    )).

update_should_change_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, _, Doc} = ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver,
        save, [?CTX, ?KEY, ?DOC]
    )),
    Value = ?MODEL_VALUE(?MODEL, 2),
    {ok, _, Doc2} = ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver,
        save, [?CTX, ?KEY, Doc#document{value = Value}]
    )),
    ?assertEqual(?KEY, Doc2#document.key),
    ?assertEqual(Value, Doc2#document.value),
    ?assertEqual(?SCOPE, Doc2#document.scope),
    ?assertEqual(2, Doc2#document.seq),
    ?assertEqual(false, Doc2#document.deleted),
    ?assertEqual(1, Doc2#document.version).

delete_should_remove_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver, save,
        [?CTX, ?KEY, ?DOC]
    )),
    ?assertEqual(ok, rpc:call(Worker, couchbase_driver, delete, [?CTX, ?KEY])),
    ?assertEqual({error, not_found}, rpc:call(Worker, couchbase_driver, get,
        [?CTX, ?KEY]
    )).

delete_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, not_found}, rpc:call(Worker, couchbase_driver, delete,
        [?CTX, ?KEY]
    )).

save_get_delete_should_return_success(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 3},
        {success_rate, 100},
        {parameters, [?OPS_NUM(10), ?DURABLE(true)]},
        {description, "Multiple cycles of parallel save/get/delete operations."},
        ?PERF_CFG(small_memory, [?OPS_NUM(1000), ?DURABLE(false)]),
        ?PERF_CFG(small_disk, [?OPS_NUM(1000), ?DURABLE(true)]),
        ?PERF_CFG(medium_memory, [?OPS_NUM(5000), ?DURABLE(false)]),
        ?PERF_CFG(medium_disk, [?OPS_NUM(5000), ?DURABLE(true)]),
        ?PERF_CFG(large_memory, [?OPS_NUM(10000), ?DURABLE(false)]),
        ?PERF_CFG(large_disk, [?OPS_NUM(10000), ?DURABLE(true)])
    ]).
save_get_delete_should_return_success_base(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    OpsNum = ?config(ops_num, Config),
    Durable = ?config(durable, Config),
    Self = self(),

    spawn_link(Worker, fun() ->
        Futures = lists:map(fun(N) ->
            Ctx = ?CTX#{no_durability => not Durable},
            couchbase_driver:save_async(Ctx, ?KEY(N), ?DOC(N))
        end, lists:seq(1, OpsNum)),
        lists:foreach(fun(Future) ->
            ?assertMatch({ok, _, #document{}}, couchbase_driver:wait(Future))
        end, Futures),
        ?assertEqual(
            0, couchbase_pool:get_request_queue_size(?BUCKET, write), ?ATTEMPTS
        ),

        Futures2 = lists:map(fun(N) ->
            couchbase_driver:get_async(?CTX, ?KEY(N))
        end, lists:seq(1, OpsNum)),
        lists:foreach(fun(Future) ->
            ?assertMatch({ok, _, #document{}}, couchbase_driver:wait(Future))
        end, Futures2),
        ?assertEqual(
            0, couchbase_pool:get_request_queue_size(?BUCKET, read), ?ATTEMPTS
        ),

        Futures3 = lists:map(fun(N) ->
            couchbase_driver:delete_async(?CTX, ?KEY(N))
        end, lists:seq(1, OpsNum)),
        lists:foreach(fun(Future) ->
            ?assertEqual(ok, couchbase_driver:wait(Future))
        end, Futures3),
        ?assertEqual(
            0, couchbase_pool:get_request_queue_size(?BUCKET, write), ?ATTEMPTS
        ),
        Self ! done
    end),
    receive done -> ok end.

get_counter_should_return_value(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, update_counter, [?CTX, ?KEY, 0, 10]),
    ?assertMatch({ok, _, 10}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, ?KEY]
    )).

get_counter_should_return_default_value(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, _, 0}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, ?KEY, 0]
    )).

get_counter_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, not_found}, rpc:call(Worker, couchbase_driver,
        get_counter, [?CTX, ?KEY]
    )).

update_counter_should_return_default_value(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, _, 0}, rpc:call(Worker, couchbase_driver, update_counter,
        [?CTX, ?KEY, 1, 0]
    )).

update_counter_should_update_value(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, update_counter, [?CTX, ?KEY, 0, 0]),
    ?assertMatch({ok, _, 10}, rpc:call(Worker, couchbase_driver, update_counter,
        [?CTX, ?KEY, 10, 0]
    )).

save_design_doc_should_return_success(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON]
    )).

save_view_doc_should_return_success(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, couchbase_driver, save_view_doc,
        [?CTX, ?VIEW, ?VIEW_FUNCTION]
    )).

save_spatial_view_doc_should_return_success(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, couchbase_driver, save_spatial_view_doc,
        [?CTX, ?VIEW, ?VIEW_FUNCTION]
    )).

get_design_doc_should_return_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON]
    ),
    ?assertEqual({ok, ?DESIGN_EJSON},
        rpc:call(Worker, couchbase_driver, get_design_doc, [?CTX, ?DESIGN])
    ).

get_design_doc_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, {<<"not_found">>, _}},
        rpc:call(Worker, couchbase_driver, get_design_doc, [?CTX, ?DESIGN])
    ).

delete_design_doc_should_return_success(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON]
    ),
    ?assertEqual(ok, rpc:call(Worker, couchbase_driver, delete_design_doc,
        [?CTX, ?DESIGN]
    )).

delete_design_doc_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, {<<"not_found">>, _}},
        rpc:call(Worker, couchbase_driver, delete_design_doc, [?CTX, ?DESIGN])
    ).

query_view_should_return_empty_result(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON]
    ),
    ?assertMatch({ok, #{<<"rows">> := []}},
        rpc:call(Worker, couchbase_driver, query_view,
            [?CTX, ?DESIGN, ?VIEW, [{stale, false}, {key, ?KEY}]]
        )
    ).

query_view_should_return_result(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON]
    ),
    rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY, ?DOC]),
    {ok, #{<<"rows">> := [Row]}} = ?assertMatch({ok, #{<<"rows">> := [_]}},
        rpc:call(Worker, couchbase_driver, query_view,
            [?CTX, ?DESIGN, ?VIEW, [
                {stale, false}, {key, ?KEY}
            ]]
        )
    ),
    ?assertEqual(?KEY, maps:get(<<"id">>, Row)),
    ?assertEqual(?KEY, maps:get(<<"key">>, Row)),
    ?assertEqual(null, maps:get(<<"value">>, Row)).

query_view_should_return_result_with_integer_key(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON(?VIEW_FUNCTION(<<"1">>))]
    ),
    rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY, ?DOC]),
    ?assertMatch({ok, #{<<"rows">> := [_|_]}},
        rpc:call(Worker, couchbase_driver, query_view,
            [?CTX, ?DESIGN, ?VIEW, [
                {stale, false}, {key, 1}
            ]]
        )
    ).

query_view_should_return_result_with_string_key(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON(?VIEW_FUNCTION(<<"'1'">>))]
    ),
    rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY, ?DOC]),
    ?assertMatch({ok, #{<<"rows">> := [_|_]}},
        rpc:call(Worker, couchbase_driver, query_view,
            [?CTX, ?DESIGN, ?VIEW, [
                {stale, false}, {key, <<"1">>}
            ]]
        )
    ).

query_view_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, {<<"not_found">>, _}},
        rpc:call(Worker, couchbase_driver, query_view,
            [?CTX, ?DESIGN, ?VIEW, [{stale, false}]]
        )
    ).

query_view_should_parse_empty_opts(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON]
    ),
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_driver, query_view,
        [?CTX, ?DESIGN, ?VIEW, []]
    )).

query_view_should_parse_all_opts(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ok = rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON]
    ),
    ?assertMatch({ok, _},
        rpc:call(Worker, couchbase_driver, query_view,
            [?CTX, ?DESIGN, ?VIEW, [
                {descending, true},
                {descending, false},
                {endkey, <<"key">>},
                {endkey_docid, <<"id">>},
                {full_set, true},
                {full_set, false},
                {inclusive_end, true},
                {inclusive_end, false},
                {key, <<"key">>},
                {limit, 1},
                {on_error, continue},
                {on_error, stop},
                {skip, 0},
                {stale, ok},
                {stale, false},
                {stale, update_after},
                {startkey, <<"key">>},
                {startkey_docid, <<"id">>},
                {bbox, <<"0,0,1,1">>},
                {start_range, [0,0]},
                {end_range, [1,1]},
                {spatial, false},
                {reduce, false}
            ]]
        )
    ).

query_view_should_parse_all_opts_spatial(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ok = rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON_SPATIAL]
    ),
    ?assertMatch({ok, _},
        rpc:call(Worker, couchbase_driver, query_view,
            [?CTX, ?DESIGN, ?VIEW_SPATIAL, [
                {keys, [<<"key">>]},
                {spatial, true}
            ]]
        )
    ).

query_view_should_parse_all_opts_reduce(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ok = rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON_REDUCE]
    ),
    ?assertMatch({ok, _},
        rpc:call(Worker, couchbase_driver, query_view,
            [?CTX, ?DESIGN, ?VIEW, [
                {group, true},
                {group, false},
                {reduce, true}
            ]]
        )
    ).

query_view_should_parse_all_opts_reduce1(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ok = rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON_REDUCE]
    ),
    ?assertMatch({ok, _},
        rpc:call(Worker, couchbase_driver, query_view,
            [?CTX, ?DESIGN, ?VIEW, [
                {group_level, 1},
                {reduce, true}
            ]]
        )
    ).

get_buckets_should_return_all_buckets(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual([?BUCKET], rpc:call(Worker, couchbase_config, get_buckets,
        []
    )).

expired_doc_should_not_exist(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(T) ->
        {ok, Cas, Doc} = rpc:call(Worker, couchbase_driver, save, 
            [?CTX#{expiry => T}, ?KEY, ?DOC]),
        ?assertEqual({ok, Cas, Doc}, rpc:call(Worker, couchbase_driver, get,
            [?CTX, ?KEY]
        )),
        timer:sleep(3000),
        ?assertEqual({error, not_found}, rpc:call(Worker, couchbase_driver, get,
            [?CTX, ?KEY]
        ))
    end, [os:system_time(second)+1, 1]).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite(Config).

end_per_suite(_Config) ->
    ok.

%%%===================================================================
%%% Cberl test helper functions
%%%===================================================================

client_proc(Client) ->
    receive
        {get_client, Pid} ->
            Pid ! {client, Client},
            client_proc(Client);
        stop ->
            ok
    end.

get_request(Num, store) ->
    Key = ?KEY(Num),
    Value = iolist_to_binary(jiffy:encode(datastore_json:encode(?DOC(Num)))),
    {3, Key, Value, 1, 0, 0};
get_request(Num, store_durable) ->
    Key = ?DURABLE_KEY(Num),
    Value = iolist_to_binary(jiffy:encode(datastore_json:encode(?DURABLE_DOC(Num)))),
    {3, Key, Value, 1, 0, 0};
get_request(Num, get) ->
    Key = ?KEY(Num),
    {Key, 0, false};
get_request(Num, get_empty) ->
    Key = ?EMPTY_KEY(Num),
    {Key, 0, false}.

map_to_cberl_req(store_durable) ->
    store;
map_to_cberl_req(get_empty) ->
    get;
map_to_cberl_req(R) ->
    R.

get_connect_opts() ->
    lists:map(fun({OptName, EnvName, OptDefault}) ->
        OptValue = application:get_env(
            ?CLUSTER_WORKER_APP_NAME, EnvName, OptDefault
        ),
        {OptName, 1000 * OptValue}
    end, [
        {operation_timeout, couchbase_operation_timeout, timer:seconds(60)},
        {config_total_timeout, couchbase_config_total_timeout, timer:seconds(30)},
        {view_timeout, couchbase_view_timeout, timer:seconds(120)},
        {durability_interval, couchbase_durability_interval, 500},
        {durability_timeout, couchbase_durability_timeout, timer:seconds(60)},
        {http_timeout, couchbase_http_timeout, timer:seconds(60)}
    ]).