%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains CouchBase changes tests.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_changes_test_SUITE).
-author("Krzysztof Trzepla").

-include("datastore_test_utils.hrl").
-include("global_definitions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, 
    init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    enable_should_be_idempotent/1,
    seq_counters_should_be_initialized_on_start/1,
    seq_safe_should_be_incremented_on_doc_save/1,
    seq_safe_should_be_incremented_on_multiple_same_doc_save/1,
    seq_safe_should_be_incremented_on_multiple_diff_docs_save/1,
    seq_safe_should_be_incremented_on_missing_change_doc/1,
    seq_safe_should_be_incremented_on_missing_doc/1,
    stream_should_return_all_changes/1,
    stream_should_return_last_changes/1,
    stream_should_return_all_changes_except_mutator/1,
    stream_should_return_changes_from_finite_range/1,
    stream_should_return_changes_from_infinite_range/1,
    cancel_stream_should_stop_worker/1,
    stream_should_stop_when_linked_process_is_terminated/1,
    stream_should_ignore_changes/1,
    stream_should_ignore_changes2/1,
    stream_should_ignore_changes3/1,
    stream_should_ignore_changes4/1
]).

%% test_bases
-export([
    stream_should_return_last_changes_base/1
]).

all() ->
    ?ALL([
        enable_should_be_idempotent,
        seq_counters_should_be_initialized_on_start,
        seq_safe_should_be_incremented_on_doc_save,
        seq_safe_should_be_incremented_on_multiple_same_doc_save,
        seq_safe_should_be_incremented_on_multiple_diff_docs_save,
        seq_safe_should_be_incremented_on_missing_change_doc,
        seq_safe_should_be_incremented_on_missing_doc,
        stream_should_return_all_changes,
        stream_should_return_last_changes,
        stream_should_return_all_changes_except_mutator,
        stream_should_return_changes_from_finite_range,
        stream_should_return_changes_from_infinite_range,
        cancel_stream_should_stop_worker,
        stream_should_stop_when_linked_process_is_terminated,
        stream_should_ignore_changes,
        stream_should_ignore_changes2,
        stream_should_ignore_changes3,
        stream_should_ignore_changes4
    ], [
        stream_should_return_last_changes
    ]).

-define(MODEL, disc_only_model).
-define(CTX, ?DISC_CTX).
-define(VALUE(N), ?MODEL_VALUE(?MODEL, N)).
-define(DOC, ?DOC(1)).
-define(DOC(N), ?DOC(N, ?VALUE(N))).
-define(DOC(N, Value), ?DOC(N, Value, ?MUTATOR)).
-define(DOC(N, Value, Mutator), ?BASE_DOC(?KEY(N), Value, ?SCOPE, [Mutator])).

-define(DOC_NUM(Value), ?PERF_PARAM(doc_num, Value, "",
    "Number of documents.")).
-define(CHANGE_NUM(Value), ?PERF_PARAM(change_num, Value, "",
    "Number of single document changes.")).

-define(TIMEOUT, timer:minutes(5)).
-define(ATTEMPTS, 60).

%%%===================================================================
%%% Test functions
%%%===================================================================

enable_should_be_idempotent(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, couchbase_changes, enable,
        [[?BUCKET]]
    )).

seq_counters_should_be_initialized_on_start(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, _, 0}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_key(?SCOPE)]
    )),
    ?assertMatch({ok, _, 0}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_safe_key(?SCOPE)]
    )).

seq_safe_should_be_incremented_on_doc_save(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY, ?DOC]),
    ?assertMatch({ok, _, 1}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_safe_key(?SCOPE)]
    ), ?ATTEMPTS),
    ?assertEqual({error, not_found}, rpc:call(Worker, couchbase_driver, get,
        [?CTX, couchbase_changes:get_change_key(?SCOPE, 1)]
    ), ?ATTEMPTS).

seq_safe_should_be_incremented_on_multiple_same_doc_save(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ChangesNum = 10,
    lists:foreach(fun(_) ->
        ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver, save,
            [?CTX, ?KEY, ?DOC]
        ))
    end, lists:seq(1, ChangesNum)),
    ?assertMatch({ok, _, ChangesNum}, rpc:call(Worker, couchbase_driver,
        get_counter, [?CTX, couchbase_changes:get_seq_safe_key(?SCOPE)]
    ), ?ATTEMPTS),
    lists:foreach(fun(N) ->
        ?assertEqual({error, not_found}, rpc:call(Worker, couchbase_driver,
            get, [?CTX, couchbase_changes:get_change_key(?SCOPE, N)]
        ), ?ATTEMPTS)
    end, lists:seq(1, ChangesNum)).

seq_safe_should_be_incremented_on_multiple_diff_docs_save(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    DocNum = 100,
    ?assertAllMatch({ok, _, _}, lists_utils:pmap(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY(N), ?DOC(N)])
    end, lists:seq(1, DocNum))),
    ?assertMatch({ok, _, DocNum}, rpc:call(Worker, couchbase_driver,
        get_counter, [?CTX, couchbase_changes:get_seq_safe_key(?SCOPE)]
    ), ?ATTEMPTS),
    lists:foreach(fun(N) ->
        ?assertEqual({error, not_found}, rpc:call(Worker, couchbase_driver,
            get, [?CTX, couchbase_changes:get_change_key(?SCOPE, N)]
        ), ?ATTEMPTS)
    end, lists:seq(1, DocNum)).

seq_safe_should_be_incremented_on_missing_change_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver, update_counter,
        [?CTX, couchbase_changes:get_seq_key(?SCOPE), 1, 0]
    )),
    ?assertMatch({ok, _, 1}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_safe_key(?SCOPE)]
    ), ?ATTEMPTS).

seq_safe_should_be_incremented_on_missing_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver, update_counter,
        [?CTX, couchbase_changes:get_seq_key(?SCOPE), 1, 0]
    )),
    EJson = #{
        <<"_record">> => <<"seq">>,
        <<"key">> => <<"someId">>,
        <<"pid">> => base64:encode(term_to_binary(spawn(fun() -> ok end)))
    },
    ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver, save,
        [?CTX, couchbase_changes:get_change_key(?SCOPE, 1), EJson]
    )),
    ?assertMatch({ok, _, 1}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_safe_key(?SCOPE)]
    ), ?ATTEMPTS).

stream_should_return_all_changes(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    DocNum = 100,
    Callback = fun(Any) -> Self ! Any end,
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes, stream,
        [?BUCKET, ?SCOPE, Callback]
    )),
    ?assertAllMatch({ok, _, _}, lists_utils:pmap(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY(N), ?DOC(N)])
    end, lists:seq(1, DocNum))),
    assert_all(fun(SeqList) ->
        {ok, Docs} = ?assertReceivedNextMatch({ok, _}, ?TIMEOUT),
        lists:foldl(fun(Doc, SeqList2) ->
            ?assert(lists:member(Doc#document.seq, SeqList2)),
            lists:delete(Doc#document.seq, SeqList2)
        end, SeqList, Docs)
    end, lists:seq(1, DocNum)).

stream_should_return_last_changes(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 100},
        {parameters, [?DOC_NUM(10), ?CHANGE_NUM(10)]},
        {description, "Parallel modifications of multiple documents."},
        ?PERF_CFG(small, [?DOC_NUM(100), ?CHANGE_NUM(10)]),
        ?PERF_CFG(medium, [?DOC_NUM(100), ?CHANGE_NUM(20)]),
        ?PERF_CFG(large, [?DOC_NUM(100), ?CHANGE_NUM(50)])
    ]).
stream_should_return_last_changes_base(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    DocNum = ?config(doc_num, Config),
    ChangesNum = ?config(change_num, Config),
    Value = ?VALUE(ChangesNum),
    Callback = fun
        ({ok, Docs}) ->
            lists:foreach(fun
                (Doc = #document{value = Any}) when Any =:= Value -> Self ! Doc;
                (_) -> ok
            end, Docs);
        (_Any) -> ok
    end,
    {ok, Pid} = ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes,
        stream, [?BUCKET, ?SCOPE, Callback]
    )),
    ?assertAllMatch(#document{}, lists_utils:pmap(fun(N) ->
        lists:foldl(fun(M, Doc) ->
            {ok, _, Doc2} = rpc:call(Worker, couchbase_driver, save, [
                ?CTX, Doc#document.key, Doc#document{revs = [?REV]}
            ]),
            Doc2#document{value = ?VALUE(M + 1)}
        end, ?DOC(N, ?VALUE(1)), lists:seq(1, ChangesNum))
    end, lists:seq(1, DocNum))),
    lists:foldl(fun(_, KeysList) ->
        Doc = ?assertReceivedNextMatch(#document{}, ?TIMEOUT),
        ?assert(lists:member(Doc#document.key, KeysList)),
        lists:delete(Doc#document.key, KeysList)
    end, [?KEY(N) || N <- lists:seq(1, DocNum)], lists:seq(1, DocNum)),
    ?assertEqual(ok, rpc:call(Worker, couchbase_changes, cancel_stream, [Pid])).

stream_should_return_all_changes_except_mutator(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    DocNum = 100,
    Callback = fun(Any) -> Self ! Any end,
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes, stream,
        [?BUCKET, ?SCOPE, Callback, [{except_mutator, <<"0">>}]]
    )),
    ?assertAllMatch({ok, _, _}, lists_utils:pmap(fun(N) ->
        rpc:call(Worker, couchbase_driver, save,
            [?CTX, ?KEY(N), ?DOC(N, ?VALUE(N), integer_to_binary(N rem 10))]
        )
    end, lists:seq(1, DocNum))),
    KeysExp = lists:filtermap(fun(N) ->
        case N rem 10 =/= 0 of
            true -> {true, ?KEY(N)};
            false -> false
        end
    end, lists:seq(1, DocNum)),
    assert_all(fun(Keys) ->
        {ok, Docs} = ?assertReceivedNextMatch({ok, _}, ?TIMEOUT),
        lists:foldl(fun(Doc, Keys2) ->
            ?assert(lists:member(Doc#document.key, Keys2)),
            lists:delete(Doc#document.key, Keys2)
        end, Keys, Docs)
    end, KeysExp).

stream_should_return_changes_from_finite_range(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    DocNum = 100,
    Since = 25,
    Until = 76,
    Callback = fun(Any) -> Self ! Any end,
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes, stream,
        [?BUCKET, ?SCOPE, Callback, [{since, Since}, {until, Until}]]
    )),
    ?assertAllMatch({ok, _, _}, lists_utils:pmap(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY(N), ?DOC(N)])
    end, lists:seq(1, DocNum))),
    assert_all(fun(SeqList) ->
        {ok, Docs} = ?assertReceivedNextMatch({ok, _}, ?TIMEOUT),
        lists:foldl(fun(Doc, SeqList2) ->
            ?assert(lists:member(Doc#document.seq, SeqList2)),
            lists:delete(Doc#document.seq, SeqList2)
        end, SeqList, Docs)
    end, lists:seq(Since, Until - 1)),
    ?assertReceivedNextMatch({ok, end_of_stream}, ?TIMEOUT).

stream_should_return_changes_from_infinite_range(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    DocNum = 100,
    Since = 25,
    Callback = fun(Any) -> Self ! Any end,
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes, stream,
        [?BUCKET, ?SCOPE, Callback, [{since, Since}]]
    )),
    ?assertAllMatch({ok, _, _}, lists_utils:pmap(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY(N), ?DOC(N)])
    end, lists:seq(1, DocNum))),
    assert_all(fun(SeqList) ->
        {ok, Docs} = ?assertReceivedNextMatch({ok, _}, ?TIMEOUT),
        lists:foldl(fun(Doc, SeqList2) ->
            ?assert(lists:member(Doc#document.seq, SeqList2)),
            lists:delete(Doc#document.seq, SeqList2)
        end, SeqList, Docs)
    end, lists:seq(Since, DocNum)).

cancel_stream_should_stop_worker(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    Callback = fun(Any) -> Self ! Any end,
    {ok, Pid} = ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes,
        stream, [?BUCKET, ?SCOPE, Callback]
    )),
    ?assertEqual(ok, rpc:call(Worker, couchbase_changes, cancel_stream, [Pid])),
    ?assertReceivedMatch({error, 1, shutdown}, ?TIMEOUT).

stream_should_stop_when_linked_process_is_terminated(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    Callback = fun(Any) -> Self ! Any end,
    LinkedProcess = spawn(fun Loop() -> Loop() end),
    {ok, _} = ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes,
        stream, [?BUCKET, ?SCOPE, Callback, [], [LinkedProcess]]
    )),
    exit(LinkedProcess, shutdown),
    ?assertReceivedMatch({error, 1, shutdown}, ?TIMEOUT).

stream_should_ignore_changes(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    DocNum = 50,
    SinceStart = 10,
    Since = 51,
    Until = 90,

    ?assertAllMatch({ok, _, _}, lists_utils:pmap(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY(N), ?DOC(N)])
    end, lists:seq(1, DocNum))),

    ?assertAllMatch({ok, _, _}, lists_utils:pmap(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY(N), ?DOC(N)])
    end, lists:seq(1, DocNum))),

    Callback = fun(Any) -> Self ! Any end,
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes, stream,
        [?BUCKET, ?SCOPE, Callback, [{since, SinceStart}, {until, Until}]]
    )),
    assert_all(fun(SeqList) ->
        {ok, Docs} = ?assertReceivedNextMatch({ok, _}, ?TIMEOUT),
        lists:foldl(fun(Doc, SeqList2) ->
            ?assert(lists:member(Doc#document.seq, SeqList2)),
            lists:delete(Doc#document.seq, SeqList2)
        end, SeqList, Docs)
    end, lists:seq(Since, Until - 1)),
    ?assertReceivedNextMatch({ok, end_of_stream}, ?TIMEOUT).

stream_should_ignore_changes2(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    DocNum = 50,
    Since = 10,
    Until = 40,
    WrongSeqs = [20,25,30,35],

    test_utils:mock_new(Worker, couchbase_crud, [passthrough, non_strict]),
    test_utils:mock_expect(Worker, couchbase_crud, prepare_store, fun(Requests) ->
        maps:fold(fun
            (Key, {Ctx, {ok, _, Value}}, {StoreRequests, Requests2, Responses}) ->
                case is_record(Value, document) andalso
                    lists:member(Value#document.seq, WrongSeqs) of
                    true ->
                        {
                            StoreRequests,
                            Requests2,
                            [{Key, {error, error}} | Responses]
                        };
                    _ ->
                        try
                            EJson = datastore_json:encode(Value),
                            Cas = maps:get(cas, Ctx, 0),
                            {
                                [{set, Key, EJson, json, Cas, 0} | StoreRequests],
                                maps:put(Key, {Ctx, {ok, Cas, Value}}, Requests2),
                                Responses
                            }
                        catch
                            _:Reason ->
                                Reason2 = {Reason, erlang:get_stacktrace()},
                                {
                                    StoreRequests,
                                    Requests2,
                                    [{Key, {error, Reason2}} | Responses]
                                }
                        end
                end
        end, {[], #{}, []}, Requests)
    end),

    SaveAns = lists_utils:pmap(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY(N), ?DOC(N)])
    end, lists:seq(1, DocNum)),
    ?assertAllMatch({ok, _, _},
        SaveAns -- lists:duplicate(length(WrongSeqs), {error,error})),

    Callback = fun(Any) -> Self ! Any end,
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes, stream,
        [?BUCKET, ?SCOPE, Callback, [{since, Since}, {until, Until}]]
    )),
    assert_all(fun(SeqList) ->
        {ok, Docs} = ?assertReceivedNextMatch({ok, _}, ?TIMEOUT),
        lists:foldl(fun(Doc, SeqList2) ->
            ?assert(lists:member(Doc#document.seq, SeqList2)),
            lists:delete(Doc#document.seq, SeqList2)
        end, SeqList, Docs)
    end, lists:seq(Since, Until - 1) -- WrongSeqs),
    ?assertReceivedNextMatch({ok, end_of_stream}, ?TIMEOUT).

stream_should_ignore_changes3(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    DocNum = 50,
    Since = 10,
    Until = 40,

    test_utils:mock_new(Worker, couchbase_crud, [passthrough, non_strict]),
    test_utils:mock_expect(Worker, couchbase_crud, prepare_change_store, fun(Requests) ->
        maps:fold(fun
            (_Key, {#{no_seq := true}, _}, {ChangeStoreRequests, ChangeKeys}) ->
                {ChangeStoreRequests, ChangeKeys};
            (Key, {_, {ok, _, Doc = #document{}}}, {ChangeStoreRequests, ChangeKeys}) ->
                case is_record(Doc, document) of
                    true ->
                        #document{scope = Scope, seq = Seq} = Doc,
                        ChangeKey = couchbase_changes:get_change_key(Scope, Seq + 1000),
                        EJson = #{
                            <<"_record">> => <<"seq">>,
                            <<"key">> => Key,
                            <<"pid">> => base64:encode(term_to_binary(self()))
                        },
                        {
                            [{set, ChangeKey, EJson, json, 0, 0} | ChangeStoreRequests],
                            maps:put(ChangeKey, Key, ChangeKeys)
                        };
                    _ ->
                        #document{scope = Scope, seq = Seq} = Doc,
                        ChangeKey = couchbase_changes:get_change_key(Scope, Seq),
                        EJson = #{
                            <<"_record">> => <<"seq">>,
                            <<"key">> => Key,
                            <<"pid">> => base64:encode(term_to_binary(self()))
                        },
                        {
                            [{set, ChangeKey, EJson, json, 0, 0} | ChangeStoreRequests],
                            maps:put(ChangeKey, Key, ChangeKeys)
                        }
                    end;
            (_Key, {_, _}, {ChangeStoreRequests, ChangeKeys}) ->
                {ChangeStoreRequests, ChangeKeys}
        end, {[], #{}}, Requests)
    end),

    ?assertAllMatch({ok, _, _}, lists_utils:pmap(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY(N), ?DOC(N)])
    end, lists:seq(1, DocNum))),

    test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME,
        couchbase_changes_restart_timeout, timer:minutes(1)),
    test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME,
        db_connection_timestamp, os:timestamp()),
    Callback = fun(Any) -> Self ! Any end,
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes, stream,
        [?BUCKET, ?SCOPE, Callback, [{since, Since}, {until, Until}]]
    )),
    assert_all(fun(SeqList) ->
        {ok, Docs} = ?assertReceivedNextMatch({ok, _}, ?TIMEOUT),
        lists:foldl(fun(Doc, SeqList2) ->
            ?assert(lists:member(Doc#document.seq, SeqList2)),
            lists:delete(Doc#document.seq, SeqList2)
        end, SeqList, Docs)
    end, lists:seq(Since, Until - 1)),
    ?assertReceivedNextMatch({ok, end_of_stream}, ?TIMEOUT).

stream_should_ignore_changes4(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    DocNum = 50,
    Since = 10,
    Until = 40,
    WrongSeqs = [20,25,30,35],

    DocNum2 = 100,
    Since2 = 60,
    Until2 = 90,
    WrongSeqs2 = [70,75,80,85],
    WrongSeqsSum = WrongSeqs ++ WrongSeqs2,

    test_utils:mock_new(Worker, couchbase_crud, [passthrough, non_strict]),
    test_utils:mock_expect(Worker, couchbase_crud, prepare_store, fun(Requests) ->
        maps:fold(fun
            (Key, {Ctx, {ok, _, Value}}, {StoreRequests, Requests2, Responses}) ->
                case is_record(Value, document) andalso
                    lists:member(Value#document.seq, WrongSeqsSum) of
                    true ->
                        {
                            StoreRequests,
                            Requests2,
                            [{Key, {error, error}} | Responses]
                        };
                    _ ->
                        try
                            EJson = datastore_json:encode(Value),
                            Cas = maps:get(cas, Ctx, 0),
                            {
                                [{set, Key, EJson, json, Cas, 0} | StoreRequests],
                                maps:put(Key, {Ctx, {ok, Cas, Value}}, Requests2),
                                Responses
                            }
                        catch
                            _:Reason ->
                                Reason2 = {Reason, erlang:get_stacktrace()},
                                {
                                    StoreRequests,
                                    Requests2,
                                    [{Key, {error, Reason2}} | Responses]
                                }
                        end
                end
        end, {[], #{}, []}, Requests)
    end),

    test_utils:mock_expect(Worker, couchbase_crud, prepare_change_store, fun(Requests) ->
        maps:fold(fun
            (_Key, {#{no_seq := true}, _}, {ChangeStoreRequests, ChangeKeys}) ->
                {ChangeStoreRequests, ChangeKeys};
            (Key, {_, {ok, _, Doc = #document{}}}, {ChangeStoreRequests, ChangeKeys}) ->
                case is_record(Doc, document) andalso
                    lists:member(Doc#document.seq, WrongSeqsSum) of
                    true ->
                        #document{scope = Scope, seq = Seq} = Doc,
                        ChangeKey = couchbase_changes:get_change_key(Scope, Seq + 1000),
                        EJson = #{
                            <<"_record">> => <<"seq">>,
                            <<"key">> => Key,
                            <<"pid">> => base64:encode(term_to_binary(self()))
                        },
                        {
                            [{set, ChangeKey, EJson, json, 0, 0} | ChangeStoreRequests],
                            maps:put(ChangeKey, Key, ChangeKeys)
                        };
                    _ ->
                        #document{scope = Scope, seq = Seq} = Doc,
                        ChangeKey = couchbase_changes:get_change_key(Scope, Seq),
                        EJson = #{
                            <<"_record">> => <<"seq">>,
                            <<"key">> => Key,
                            <<"pid">> => base64:encode(term_to_binary(self()))
                        },
                        {
                            [{set, ChangeKey, EJson, json, 0, 0} | ChangeStoreRequests],
                            maps:put(ChangeKey, Key, ChangeKeys)
                        }
                end;
            (_Key, {_, _}, {ChangeStoreRequests, ChangeKeys}) ->
                {ChangeStoreRequests, ChangeKeys}
        end, {[], #{}}, Requests)
    end),

    SaveAns = lists_utils:pmap(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY(N), ?DOC(N)])
    end, lists:seq(1, DocNum)),
    ?assertAllMatch({ok, _, _},
        SaveAns -- lists:duplicate(length(WrongSeqs), {error,error})),

    T1 = os:timestamp(),
    test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME,
        couchbase_changes_restart_timeout, timer:minutes(1)),
    test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME,
        db_connection_timestamp, T1),

    Callback = fun(Any) -> Self ! Any end,
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes, stream,
        [?BUCKET, ?SCOPE, Callback, [{since, Since}, {until, Until}]]
    )),
    assert_all(fun(SeqList) ->
        {ok, Docs} = ?assertReceivedNextMatch({ok, _}, ?TIMEOUT),
        lists:foldl(fun(Doc, SeqList2) ->
            ?assert(lists:member(Doc#document.seq, SeqList2)),
            lists:delete(Doc#document.seq, SeqList2)
        end, SeqList, Docs)
    end, lists:seq(Since, Until - 1) -- WrongSeqs),

    ?assert(timer:now_diff(os:timestamp(), T1) >= timer:minutes(1) * 1000),
    ?assertReceivedNextMatch({ok, end_of_stream}, ?TIMEOUT),

    SaveAns2 = lists_utils:pmap(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?KEY(N), ?DOC(N)])
    end, lists:seq(DocNum + 1, DocNum2)),
    ?assertAllMatch({ok, _, _},
        SaveAns2 -- lists:duplicate(length(WrongSeqs2), {error,error})),

    T2 = os:timestamp(),
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes, stream,
        [?BUCKET, ?SCOPE, Callback, [{since, Since2}, {until, Until2}]]
    )),
    assert_all(fun(SeqList) ->
        {ok, Docs} = ?assertReceivedNextMatch({ok, _}, ?TIMEOUT),
        lists:foldl(fun(Doc, SeqList2) ->
            ?assert(lists:member(Doc#document.seq, SeqList2)),
            lists:delete(Doc#document.seq, SeqList2)
        end, SeqList, Docs)
    end, lists:seq(Since2, Until2 - 1) -- WrongSeqs2),

    ?assert(timer:now_diff(os:timestamp(), T2) =< timer:seconds(20) * 1000),
    ?assertReceivedNextMatch({ok, end_of_stream}, ?TIMEOUT).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite([?MODEL], Config, fun(Config2) ->
        [Worker | _] = ?config(cluster_worker_nodes, Config2),
        ?assertEqual(ok, rpc:call(Worker, couchbase_changes, enable,
            [[?BUCKET]]
        )),
        Config2
    end).

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun({Key, Value}) ->
        test_utils:set_env(Worker, cluster_worker, Key, Value)
    end, [
        {couchbase_changes_update_interval, 500},
        {couchbase_changes_stream_update_interval, 500}
    ]),
    test_utils:mock_new(Workers, couchbase_pool),
    test_utils:mock_expect(Workers, couchbase_pool, get_timeout, fun() ->
        get_couchbase_pool_timeout(Case)
    end),
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes, start,
        [?BUCKET, get_scope(Case)]
    )),
    Config.

end_per_testcase(Case, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, couchbase_changes, stop,
        [?BUCKET, get_scope(Case)]
    )),
    test_utils:mock_unload(Worker, couchbase_crud).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_couchbase_pool_timeout(seq_safe_should_be_incremented_on_missing_change_doc) ->
    timer:seconds(1);
get_couchbase_pool_timeout(seq_safe_should_be_incremented_on_missing_doc) ->
    timer:seconds(1);
get_couchbase_pool_timeout(_) ->
    timer:minutes(2).

get_scope(stream_should_return_last_changes) ->
    ?SCOPE(<<"stream_should_return_last_changes_base">>);
get_scope(Case) ->
    ?SCOPE(atom_to_binary(Case, utf8)).

assert_all(_, []) ->
    ok;
assert_all(Fun, List) ->
    List1 = Fun(List),
    assert_all(Fun, List1).