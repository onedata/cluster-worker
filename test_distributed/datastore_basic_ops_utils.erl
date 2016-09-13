%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests datastore basic operations at all levels.
%%% It is utils module - it contains test functions but it is not
%%% test suite. These functions are included by suites that do tests
%%% using various environments.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_basic_ops_utils).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("datastore_basic_ops_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").

-define(REQUEST_TIMEOUT, timer:minutes(5)).

-export([create_delete_test/2, create_sync_delete_test/2, save_test/2, save_sync_test/2, update_test/2,
    update_sync_test/2, get_test/2, exists_test/2, mixed_test/2, links_test/2, links_number_test/2,
    set_env/2, clear_env/1, clear_cache/1, get_record/2, get_record/3, get_record/4]).

-define(TIMEOUT, timer:minutes(5)).
-define(call_store(Fun, Level, CustomArgs), erlang:apply(datastore, Fun, [Level] ++ CustomArgs)).
-define(rpc_store(W, Fun, Level, CustomArgs), rpc:call(W, datastore, Fun, [Level] ++ CustomArgs)).
-define(call(N, M, F, A), rpc:call(N, M, F, A, ?TIMEOUT)).

%%%===================================================================
%%% API
%%%===================================================================

links_test(Config, Level) ->
    [Worker1 | _] = Workers = ?config(cluster_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),
    TestRecord = ?config(test_record, Config),

    set_test_type(Config, Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    Key = list_to_binary("key_" ++ AnswerDesc),
    Doc =  #document{
        key = Key,
        value = get_record(TestRecord, 12345, <<"abcdef">>, {test, tuple111})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),

    save_many(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

    Create = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(add_links, Level, [Doc,
                    [{list_to_binary("link" ++ DocsSet ++ integer_to_list(I)),
                        {list_to_binary(DocsSet ++ integer_to_list(I)), TestRecord}}]]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, Create),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, OkTime, _ErrorNum, _ErrorTime, _ErrorsList} = count_answers(OpsNum),
    ?assertEqual(OpsNum, OkNum),

    Fetch = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(fetch_link, Level, [
                    Doc, list_to_binary("link" ++ DocsSet ++ integer_to_list(I))]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, Fetch),
    {OkNumF, OkTimeF, _ErrorNumF, _ErrorTimeF, _ErrorsListF} = count_answers(OpsNum),
    ?assertEqual(OpsNum, OkNumF),

    Delete = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(delete_links, Level, [
                    Doc, [list_to_binary("link" ++ DocsSet ++ integer_to_list(I))]]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, Delete),
    {OkNum2, OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum, OkNum2),

    test_with_fetch(Doc, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, false),

    ForechTestFun = fun(LinkName, LinkTarget, Acc) ->
        maps:put(LinkName, LinkTarget, Acc)
    end,
    ForeachBeforeProcessing = os:timestamp(),
    Ans = ?rpc_store(Worker1, foreach_link, Level, [Doc, ForechTestFun, #{}]),
    ForeachAfterProcessing = os:timestamp(),

    clear_with_del(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),
    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),

    [
        #parameter{name = add_time, value = OkTime / OkNum, unit = "us",
            description = "Average time of add operation"},
        #parameter{name = fetch_time, value = OkTimeF / OkNumF, unit = "us",
            description = "Average time of fetch operation"},
        #parameter{name = delete_time, value = OkTime2 / OkNum2, unit = "us",
            description = "Average time of delete operation"},
        #parameter{name = foreach_link_time, value = timer:now_diff(ForeachAfterProcessing, ForeachBeforeProcessing),
            unit = "us", description = "Time of forach_link operation"}
    ].

create_delete_test(Config, Level) ->
    create_delete_test_base(Config, Level, create, delete).

create_sync_delete_test(Config, Level) ->
    create_delete_test_base(Config, Level, create_sync, delete_sync).

create_delete_test_base(Config, Level, Fun, Fun2) ->
    Workers = ?config(cluster_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),
    TestRecord = ?config(test_record, Config),

    set_test_type(Config, Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    TestFun = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(Fun, Level, [
                    #document{
                        key = list_to_binary(DocsSet ++ integer_to_list(I)),
                        value = get_record(TestRecord, I, <<"abc">>, {test, tuple})
                    }]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, OkTime, ErrorNum, ErrorTime, _ErrorsList} = count_answers(OpsNum),
    ?assertEqual(OpsNum, OkNum + ErrorNum),
    TmpTN = trunc(ThreadsNum/ConflictedThreads),
    WLength = length(Workers),
    Multip = min(WLength, min(ThreadsNum, ConflictedThreads)),
    NewTN = case {Level, Fun} of
                {local_only, _} ->
                    TmpTN * Multip;
                {locally_cached, create} ->
                    TmpTN * Multip;
                _ ->
                    TmpTN
            end,
    ModelConfig = TestRecord:model_init(),
    case ModelConfig#model_config.transactional_global_cache of
        true ->
            CheckOpsNum = DocsPerThead * NewTN,
            ?assertEqual(CheckOpsNum, OkNum);
        _ ->
            ?assert((ThreadsNum * DocsPerThead >= OkNum) and (DocsPerThead * trunc(ThreadsNum / ConflictedThreads) =< OkNum))
    end,

    test_with_get(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

    TestFun2 = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(Fun2, Level, [
                    TestRecord, list_to_binary(DocsSet ++ integer_to_list(I))]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun2),
    {OkNum2, OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum, OkNum2),

    test_with_get(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, false),

    CreateErrorTime = case ErrorNum of
                          0 ->
                              0;
                          _ ->
                              ErrorTime / ErrorNum
                      end,

    [
        #parameter{name = create_ok_time, value = OkTime / OkNum, unit = "us",
            description = "Average time of create operation that ended successfully"},
        #parameter{name = create_error_time, value = CreateErrorTime, unit = "us",
            description = "Average time of create operation that filed (e.g. file exists)"},
        #parameter{name = create_error_num, value = ErrorNum, unit = "-",
            description = "Average numer of create operation that filed (e.g. file exists)"},
        #parameter{name = delete_time, value = OkTime2 / OkNum2, unit = "us",
            description = "Average time of delete operation"}
    ].

save_test(Config, Level) ->
    save_test_base(Config, Level, save, delete).

save_sync_test(Config, Level) ->
    save_test_base(Config, Level, save_sync, delete_sync).

save_test_base(Config, Level, Fun, Fun2) ->
    [Worker1 | _] = Workers = ?config(cluster_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),
    TestRecord = ?config(test_record, Config),

    set_test_type(Config, Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    SaveMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(Fun, Level, [
                    #document{
                        key = list_to_binary(DocsSet ++ integer_to_list(I)),
                        value = get_record(TestRecord, I, <<"abc">>, {test, tuple})
                    }]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, SaveMany),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, OkTime, _ErrorNum, _ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList),
    ?assertEqual(OpsNum, OkNum),

    test_with_get(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

    ListBeforeProcessing = os:timestamp(),
    Ans = ?rpc_store(Worker1, list, Level, [TestRecord, ?GET_ALL, []]),
    ListAfterProcessing = os:timestamp(),

    clear_with_del(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, Fun2),

    [
        #parameter{name = save_time, value = OkTime / OkNum, unit = "us",
            description = "Average time of save operation"},
        #parameter{name = list_time, value = timer:now_diff(ListAfterProcessing, ListBeforeProcessing), unit = "us",
            description = "Time of list operation"}
    ].

update_test(Config, Level) ->
    update_test_base(Config, Level, update, save, delete).

update_sync_test(Config, Level) ->
    update_test_base(Config, Level, update_sync, save_sync, delete_sync).

update_test_base(Config, Level, Fun, Fun2, Fun3) ->
    Workers = ?config(cluster_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),
    TestRecord = ?config(test_record, Config),

    set_test_type(Config, Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    UpdateMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(Fun, Level, [
                    TestRecord, list_to_binary(DocsSet ++ integer_to_list(I)),
                    #{field1 => I + J}
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, UpdateMany),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, _OkTime, ErrorNum, ErrorTime, _ErrorsList} = count_answers(OpsNum),
    ?assertEqual(0, OkNum),
    ?assertEqual(OpsNum, ErrorNum),
    ?assertEqual(OpsNum, OkNum + ErrorNum),

    test_with_get(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, false),

    save_many(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, Fun2),

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, UpdateMany),
    {OkNum3, OkTime3, _ErrorNum3, _ErrorTime3, ErrorsList3} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList3),
    ?assertEqual(OpsNum, OkNum3),

    clear_with_del(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, Fun3),

    UpdateErrorTime = ErrorTime / ErrorNum,

    [
        #parameter{name = update_ok_time, value = OkTime3 / OkNum3, unit = "us",
            description = "Average time of update operation that ended successfully"},
        #parameter{name = update_error_time, value = UpdateErrorTime, unit = "us",
            description = "Average time of update operation that failed (e.g. file does not exist)"},
        #parameter{name = update_error_num, value = ErrorNum, unit = "-",
            description = "Average number of update operation that failed (e.g. file does not exist)"}
    ].

get_test(Config, Level) ->
    Workers = ?config(cluster_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),
    TestRecord = ?config(test_record, Config),

    set_test_type(Config, Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    GetMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(_J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(get, Level, [
                    TestRecord, list_to_binary(DocsSet ++ integer_to_list(I))
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, GetMany),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, _OkTime, ErrorNum, ErrorTime, _ErrorsList} = count_answers(OpsNum),
    ?assertEqual(0, OkNum),
    ?assertEqual(OpsNum, ErrorNum),
    ?assertEqual(OpsNum, OkNum + ErrorNum),

    save_many(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, GetMany),
    {OkNum3, OkTime3, _ErrorNum3, _ErrorTime3, ErrorsList3} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList3),
    ?assertEqual(OpsNum, OkNum3),

    clear_with_del(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

    GetErrorTime = ErrorTime / ErrorNum,

    [
        #parameter{name = get_ok_time, value = OkTime3 / OkNum3, unit = "us",
            description = "Average time of get operation that ended successfully"},
        #parameter{name = get_error_time, value = GetErrorTime, unit = "us",
            description = "Average time of get operation that failed (e.g. file does not exist)"},
        #parameter{name = update_error_num, value = ErrorNum, unit = "-",
            description = "Average number of update operation that failed (e.g. file does not exist)"}
    ].

exists_test(Config, Level) ->
    Workers = ?config(cluster_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),
    TestRecord = ?config(test_record, Config),

    set_test_type(Config, Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    ExistMultiCheck = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(_J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(exists, Level, [
                    TestRecord, list_to_binary(DocsSet ++ integer_to_list(I))
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, ExistMultiCheck),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, OkTime, _ErrorNum, _ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList),
    ?assertEqual(OpsNum, OkNum),

    save_many(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, ExistMultiCheck),
    {OkNum3, OkTime3, _ErrorNum3, _ErrorTime3, ErrorsList3} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList3),
    ?assertEqual(OpsNum, OkNum3),

    clear_with_del(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

    [
        #parameter{name = exists_true_time, value = OkTime3 / OkNum3, unit = "us",
            description = "Average time of exists operation that returned true"},
        #parameter{name = exists_false_time, value = OkTime / OkNum, unit = "us",
            description = "Average time of exists operation that returned false"}
    ].

mixed_test(Config, Level) ->
    case performance:is_stress_test() of
        true ->
            LastFails = ?config(last_fails, Config),
            case LastFails of
                0 ->
                    ok;
                _ ->
                    ct:print("mixed_test: Sleep because of failures at level: ~p: ~p sek", [Level, 2 * LastFails]),
                    timer:sleep(timer:seconds(2 * LastFails))
            end;
        _ ->
            ok
    end,

    [Worker1 | _] = Workers = ?config(cluster_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),
    TestRecord = ?config(test_record, Config),

    set_test_type(Config, Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    CreateMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(create, Level, [
                    #document{
                        key = list_to_binary(DocsSet ++ integer_to_list(I)),
                        value = get_record(TestRecord, I, <<"abc">>, {test, tuple})
                    }]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    SaveMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(save, Level, [
                    #document{
                        key = list_to_binary(DocsSet ++ integer_to_list(I)),
                        value = get_record(TestRecord, I, <<"abc">>, {test, tuple})
                    }]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    UpdateMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(update, Level, [
                    TestRecord, list_to_binary(DocsSet ++ integer_to_list(I)),
                    #{field1 => I + J}
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, CreateMany),
    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, SaveMany),
    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, UpdateMany),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,

    {OkNum, OkTime, ErrorNum, ErrorTime, _ErrorsList} = count_answers(3 * OpsNum),
    ?assertEqual(3 * OpsNum, OkNum + ErrorNum),
    ?assert(OkNum >= OpsNum),

    {Key, Doc, AnsExt} = case Level of
                             local_only ->
                                 {ok, ok, []};
                             locally_cached ->
                                 {ok, ok, []};
                             _ ->
                                 K = list_to_binary("key_" ++ AnswerDesc),
                                 D =  #document{
                                     key = K,
                                     value = get_record(TestRecord, 12345, <<"abcdef">>, {test, tuple111})
                                 },
                                 ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [D])),

                                 CreateLinks = fun(DocsSet) ->
                                     for(1, DocsPerThead, fun(I) ->
                                         for(OpsPerDoc, fun() ->
                                             BeforeProcessing = os:timestamp(),
                                             Ans = ?call_store(add_links, Level, [D,
                                                 [{list_to_binary("link" ++ DocsSet ++ integer_to_list(I)),
                                                     {list_to_binary(DocsSet ++ integer_to_list(I)), TestRecord}}]]),
                                             AfterProcessing = os:timestamp(),
                                             Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
                                         end)
                                     end)
                                 end,

                                 spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, CreateLinks),
                                 {OkNumCL, OkTimeCL, _ErrorNumCL, _ErrorTimeCL, _ErrorsListCL} = count_answers(OpsNum),
                                 ?assertEqual(OpsNum, OkNumCL),
                                 {K, D,
                                     [#parameter{name = add_links_time, value = OkTimeCL / OkNumCL, unit = "us",
                                         description = "Average time of links adding"}]}
                         end,

    GetMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(_J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(get, Level, [
                    TestRecord, list_to_binary(DocsSet ++ integer_to_list(I))
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    ExistMultiCheck = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(_J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(exists, Level, [
                    TestRecord, list_to_binary(DocsSet ++ integer_to_list(I))
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    Fetch = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(fetch_link, Level, [
                    Doc, list_to_binary("link" ++ DocsSet ++ integer_to_list(I))]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, GetMany),
    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, ExistMultiCheck),
    CheckMul = case Level of
                   local_only ->
                       2;
                   locally_cached ->
                       2;
                   _ ->
                       spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, Fetch),
                       3
               end,

    {OkNum2, OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(CheckMul * OpsNum),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(CheckMul * OpsNum, OkNum2),

    ClearRatio = case performance:should_clear(Config) of
                     true -> 1;
                     _ -> 2
                 end,

    TmpTN = trunc(ThreadsNum/ConflictedThreads),
    WLength = length(Workers),
    Multip = min(WLength, min(ThreadsNum, ConflictedThreads)),
    {NewTN, NewCT} = case Level of
                         local_only ->
                             {round(TmpTN / ClearRatio) * Multip, Multip};
                         locally_cached ->
                             {round(TmpTN / ClearRatio) * Multip, Multip};
                         _ ->
                             {round(TmpTN / ClearRatio), 1}
                     end,
    DelOpsNum = DocsPerThead * NewTN,

    AnsExt2 = case Level of
                  local_only ->
                      [];
                  locally_cached ->
                      [];
                  _ ->
                      ClearManyLinks = fun(DocsSet) ->
                          for(1, DocsPerThead, fun(I) ->
                              BeforeProcessing = os:timestamp(),
                              Ans = ?call_store(delete_links, Level, [
                                  Doc, [list_to_binary("link" ++ DocsSet ++ integer_to_list(I))]]),
                              AfterProcessing = os:timestamp(),
                              Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
                          end)
                      end,

                      spawn_at_nodes(Workers, NewTN, NewCT, ClearManyLinks),
                      {DelLinkOkNum, DelLinkTime, _DelLinkErrorNum, _DelLinkErrorTime, DelLinkErrorsList} =
                          count_answers(DelOpsNum),
                      ?assertEqual([], DelLinkErrorsList),
                      ?assertEqual(DelOpsNum, DelLinkOkNum),
                      ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),

                      AnsExt ++ [#parameter{name = del_links_time, value = DelLinkTime / DelOpsNum, unit = "us",
                          description = "Average time of delete links"}
                      ]
              end,

    ClearMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(delete, Level, [
                TestRecord, list_to_binary(DocsSet ++ integer_to_list(I))]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, NewTN, NewCT, ClearMany),
    {DelOkNum, DelOkTime, _DelErrorNum, _DelErrorTime, DelErrorsList} = count_answers(DelOpsNum),
    ?assertEqual([], DelErrorsList),
    ?assertEqual(DelOpsNum, DelOkNum),

    AnsExt3 = case performance:should_clear(Config) of
                  true -> AnsExt2;
                  _ ->
                      RepNum = ?config(rep_num, Config),
                      FailedNum = ?config(failed_num, Config),
                      DocsInDB = (TmpTN - NewTN) * DocsPerThead,
                      ct:print("Docs in datastore at level ~p: ~p", [Level, DocsInDB * (RepNum - FailedNum)]),
                      AnsExt2 ++ [#parameter{name = doc_in_db, value = DocsInDB, unit = "us",
                          description = "Docs in datastore after test"},
                          #parameter{name = links_in_db, value = DocsInDB, unit = "us",
                              description = "Links in datastore after test"}
                      ]
              end,

    [
        #parameter{name = create_save_update_time, value = (OkTime + ErrorTime) / (OkNum + ErrorNum), unit = "us",
            description = "Average time of create/save/update"},
        #parameter{name = get_exist_fetch_time, value = OkTime2 / OkNum2, unit = "us",
            description = "Average time of get/exist/fetch"},
        #parameter{name = del_time, value = DelOkTime / DelOpsNum, unit = "us",
            description = "Average time of delete"}
    ] ++ AnsExt3.

links_number_test(Config, Level) ->
    LastFails = ?config(last_fails, Config),
    case LastFails of
        0 ->
            ok;
        _ ->
            ct:print("links_number_test: Sleep because of failures at level: ~p: ~p sek", [Level, 2 * LastFails]),
            timer:sleep(timer:seconds(2 * LastFails))
    end,

    [Worker1 | _] = Workers = ?config(cluster_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),
    TestRecord = ?config(test_record, Config),

    set_test_type(Config, Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    save_many(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

    Key = list_to_binary("key_with_links"),
    Doc =  #document{
        key = Key,
        value = get_record(TestRecord, 12345, <<"abcdef">>, {test, tuple111})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, save, [Doc])),

    CreateLinks = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(add_links, Level, [Doc,
                    [{list_to_binary("link" ++ DocsSet ++ integer_to_list(I)),
                        {list_to_binary(DocsSet ++ integer_to_list(I)), TestRecord}}]]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, CreateLinks),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNumCL, OkTimeCL, _ErrorNumCL, _ErrorTimeCL, _ErrorsListCL} = count_answers(OpsNum),
    ?assertEqual(OpsNum, OkNumCL),

    Fetch = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(fetch_link, Level, [
                    Doc, list_to_binary("link" ++ DocsSet ++ integer_to_list(I))]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, Fetch),
    {OkNum2, OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum, OkNum2),

    ClearRatio = 2,
    TmpTN = trunc(ThreadsNum/ConflictedThreads),
    NewTN = round(TmpTN / ClearRatio),
    NewCT = 1,
    DelOpsNum = DocsPerThead * NewTN,

    ClearManyLinks = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(delete_links, Level, [
                Doc, [list_to_binary("link" ++ DocsSet ++ integer_to_list(I))]]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, NewTN, NewCT, ClearManyLinks),
    {DelLinkOkNum, DelLinkTime, _DelLinkErrorNum, _DelLinkErrorTime, DelLinkErrorsList} =
        count_answers(DelOpsNum),
    ?assertEqual([], DelLinkErrorsList),
    ?assertEqual(DelOpsNum, DelLinkOkNum),

    clear_with_del(TestRecord, Level, Workers, DocsPerThead, NewTN, NewCT, Master, AnswerDesc),

    RepNum = ?config(rep_num, Config),
    FailedNum = ?config(failed_num, Config),
    DocsInDB = (TmpTN - NewTN) * DocsPerThead,
    ct:print("Links linked to single doc at level ~p: ~p", [Level, DocsInDB * (RepNum - FailedNum)]),

    [
        #parameter{name = links_linked_to_doc, value = DocsInDB, unit = "us",
            description = "Links linked to single doc after test"}
        #parameter{name = add_links_time, value = OkTimeCL / OkNumCL, unit = "us",
            description = "Average time of links adding"},
        #parameter{name = fetch_time, value = OkTime2 / OkNum2, unit = "us",
            description = "Average time of fetch"},
        #parameter{name = del_links_time, value = DelLinkTime / DelOpsNum, unit = "us",
            description = "Average time of delete links"}
    ].

set_env(Case, Config) ->
    timer:sleep(3000), % tmp solution until mocking is repaired (VFS-1851)
    Workers = ?config(cluster_worker_nodes, Config),
    ok = test_node_starter:load_modules(Workers, [?MODULE]),
    TestRecord = get_record_name(Case),
    test_utils:enable_datastore_models(Workers, [TestRecord]),
    [{test_record, TestRecord} | Config].

clear_env(Config) ->
    case performance:is_stress_test() of
        true ->
            ok;
        _ ->
            Workers = ?config(cluster_worker_nodes, Config),
            [W | _] = Workers,
            case ?config(test_record, Config) of
                globally_cached_record ->
                    clear_cache(W);
                locally_cached_record ->
                    lists:foreach(fun(Wr) ->
                        clear_cache(Wr)
                    end, Workers);
                _ ->
                    ok
            end
    end.

clear_cache(W) ->
    _A1 = ?call(W, caches_controller, wait_for_cache_dump, []),
    _A2 = gen_server:call({?NODE_MANAGER_NAME, W}, clear_mem_synch, 60000),
    _A3 = gen_server:call({?NODE_MANAGER_NAME, W}, force_clear_node, 60000),
%%    ?assertMatch({ok, ok, {ok, ok}}, {A1, A2, A3}).
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

for(1, F) ->
    F();
for(N, F) ->
    F(),
    for(N - 1, F).

for(N, N, F) ->
    F(N);
for(I, N, F) ->
    F(I),
    for(I + 1, N, F).

spawn_at_nodes(Nodes, Threads, ConflictedThreads, Fun) ->
    spawn_at_nodes(Nodes, [], Threads, 1, 0, ConflictedThreads, Fun, []).

spawn_at_nodes(_Nodes, _Nodes2, 0, _DocsSetNum, _DocNumInSet, _ConflictedThreads, _Fun, Pids) ->
    lists:foreach(fun(Pid) -> Pid ! start end, Pids);
spawn_at_nodes(Nodes, Nodes2, Threads, DocsSet, ConflictedThreads, ConflictedThreads, Fun, Pids) ->
    spawn_at_nodes(Nodes, Nodes2, Threads, DocsSet + 1, 0, ConflictedThreads, Fun, Pids);
spawn_at_nodes([], Nodes2, Threads, DocsSetNum, DocNumInSet, ConflictedThreads, Fun, Pids) ->
    spawn_at_nodes(Nodes2, [], Threads, DocsSetNum, DocNumInSet, ConflictedThreads, Fun, Pids);
spawn_at_nodes([N | Nodes], Nodes2, Threads, DocsSetNum, DocNumInSet, ConflictedThreads, Fun, Pids) ->
    Master = self(),
    AnswerDesc = get(file_beg),
    FileBeg = "_" ++ AnswerDesc ++ "_",
    Pid = spawn(N, fun() ->
        try
            receive start -> ok end,
            Fun(integer_to_list(DocsSetNum) ++ FileBeg)
        catch
            E1:E2 ->
                Master ! {store_ans, AnswerDesc, {uncatched_error, E1, E2, erlang:get_stacktrace()}, 0}
        end
    end),
    spawn_at_nodes(Nodes, [N | Nodes2], Threads - 1, DocsSetNum, DocNumInSet + 1, ConflictedThreads, Fun, [Pid | Pids]).

count_answers(Exp) ->
    count_answers(Exp, {0, 0, 0, 0, []}). %{OkNum, OkTime, ErrorNum, ErrorTime, ErrorsList}

count_answers(0, TmpAns) ->
    TmpAns;

count_answers(Num, {OkNum, OkTime, ErrorNum, ErrorTime, ErrorsList}) ->
    AnswerDesc = get(file_beg),
    NewAns = receive
                 {store_ans, AnswerDesc, Ans, Time} ->
                     case Ans of
                         ok ->
                             {OkNum + 1, OkTime + Time, ErrorNum, ErrorTime, ErrorsList};
                         {ok, _} ->
                             {OkNum + 1, OkTime + Time, ErrorNum, ErrorTime, ErrorsList};
                         {uncatched_error, E1, E2, ST} ->
                             ?assertEqual({ok, ok, ok}, {E1, E2, ST}),
                             error;
                         E ->
                             {OkNum, OkTime, ErrorNum + 1, ErrorTime + Time, [E | ErrorsList]}
                     end
             after ?REQUEST_TIMEOUT ->
                 {error, timeout}
             end,
    case NewAns of
        {error, timeout} ->
            {OkNum, OkTime, ErrorNum, ErrorTime, ErrorsList};
        _ ->
            count_answers(Num - 1, NewAns)
    end.

disable_cache_control(Workers) ->
    lists:foreach(fun(W) ->
        ?assertEqual(ok, gen_server:call({?NODE_MANAGER_NAME, W}, disable_cache_control))
    end, Workers).

get_record_name(Case) ->
    CStr = atom_to_list(Case),
    case {string:str(CStr, "cache") > 0, string:str(CStr, "sync") == 0, string:str(CStr, "no_transactions") > 0} of
        {true, true, _} ->
            case string:str(CStr, "global") > 0 of
                true ->
                    globally_cached_record;
                _ ->
                    locally_cached_record
            end;
        {true, false, _} ->
            case string:str(CStr, "global") > 0 of
                true ->
                    globally_cached_sync_record;
                _ ->
                    locally_cached_sync_record
            end;
        {_, _, true} ->
            global_only_no_transactions_record;
        _ ->
            case {string:str(CStr, "global") > 0, string:str(CStr, "local") > 0} of
                {true, _} ->
                    global_only_record;
                {_, true} ->
                    local_only_record;
                _ ->
                    disk_only_record
            end
    end.

set_test_type(Config, Workers) ->
    put(file_beg, get_random_string()),
    % TODO - check why cauchdb driver does not work with such elements in link_name:
%%     put(file_beg, binary_to_list(term_to_binary(os:timestamp()))),
    case performance:should_clear(Config) of
        true ->
            disable_cache_control(Workers);
        _ ->
            ok
    end.

get_random_string() ->
    get_random_string(10, "abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ").

get_random_string(Length, AllowedChars) ->
    lists:foldl(fun(_, Acc) ->
        [lists:nth(random:uniform(length(AllowedChars)),
            AllowedChars)]
        ++ Acc
    end, [], lists:seq(1, Length)).

test_with_get(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc) ->
    test_with_get(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, true).

test_with_get(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, Exists) ->
    GetMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(get, Level, [
                TestRecord, list_to_binary(DocsSet ++ integer_to_list(I))]),
            AfterProcessing = os:timestamp(),
            FinalAns = case {Exists, Ans} of
                           {false, {error, {not_found, _}}} ->
                               ok;
                           _ ->
                               Ans
                       end,
            Master ! {store_ans, AnswerDesc, FinalAns, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    TmpTN = trunc(ThreadsNum/ConflictedThreads),
    WLength = length(Workers),
    Multip = min(WLength, min(ThreadsNum, ConflictedThreads)),
    {NewTN, NewCT} = case Level of
                         local_only ->
                             {TmpTN * Multip, Multip};
                         locally_cached ->
                             {TmpTN * Multip, Multip};
                         _ ->
                             {TmpTN, 1}
                     end,

    spawn_at_nodes(Workers, NewTN, NewCT, GetMany),
    OpsNum = DocsPerThead * NewTN,
    {OkNum, _OkTime, _ErrorNum, _ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList),
    ?assertEqual(OpsNum, OkNum).

test_with_fetch(Doc, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, Exists) ->
    FetchMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(fetch_link, Level, [
                Doc, list_to_binary("link" ++ DocsSet ++ integer_to_list(I))]),
            AfterProcessing = os:timestamp(),
            FinalAns = case {Exists, Ans} of
                           {false, {error,link_not_found}} ->
                               ok;
                           _ ->
                               Ans
                       end,
            Master ! {store_ans, AnswerDesc, FinalAns, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    TmpTN = trunc(ThreadsNum/ConflictedThreads),
    WLength = length(Workers),
    Multip = min(WLength, min(ThreadsNum, ConflictedThreads)),
    {NewTN, NewCT} = case Level of
                         local_only ->
                             {TmpTN * Multip, Multip};
                         locally_cached ->
                             {TmpTN * Multip, Multip};
                         _ ->
                             {TmpTN, 1}
                     end,

    spawn_at_nodes(Workers, NewTN, NewCT, FetchMany),
    OpsNum = DocsPerThead * NewTN,
    {OkNum, _OkTime, _ErrorNum, _ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList),
    ?assertEqual(OpsNum, OkNum).

clear_with_del(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc) ->
    clear_with_del(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, delete).

clear_with_del(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, ClearFun) ->
    ClearMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(ClearFun, Level, [
                TestRecord, list_to_binary(DocsSet ++ integer_to_list(I))]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    TmpTN = trunc(ThreadsNum/ConflictedThreads),
    WLength = length(Workers),
    Multip = min(WLength, min(ThreadsNum, ConflictedThreads)),
    {NewTN, NewCT} = case Level of
                         local_only ->
                             {TmpTN * Multip, Multip};
                         locally_cached ->
                             {TmpTN * Multip, Multip};
                         _ ->
                             {TmpTN, 1}
                     end,

    spawn_at_nodes(Workers, NewTN, NewCT, ClearMany),
    OpsNum = DocsPerThead * NewTN,
    {OkNum, _OkTime, _ErrorNum, _ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList),
    ?assertEqual(OpsNum, OkNum).

save_many(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc) ->
    save_many(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, save).

save_many(TestRecord, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, SaveFun) ->
    SaveMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(SaveFun, Level, [
                #document{
                    key = list_to_binary(DocsSet ++ integer_to_list(I)),
                    value = get_record(TestRecord, I, <<"abc">>, {test, tuple})
                }]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    TmpTN = trunc(ThreadsNum/ConflictedThreads),
    WLength = length(Workers),
    Multip = min(WLength, min(ThreadsNum, ConflictedThreads)),
    {NewTN, NewCT} = case Level of
                         local_only ->
                             {TmpTN * Multip, Multip};
                         locally_cached ->
                             {TmpTN * Multip, Multip};
                         _ ->
                             {TmpTN, 1}
                     end,

    spawn_at_nodes(Workers, NewTN, NewCT, SaveMany),
    OpsNum = DocsPerThead * NewTN,
    {OkNum, _OkTime, _ErrorNum, _ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList),
    ?assertEqual(OpsNum, OkNum).

get_record(TestRecord, Field1) ->
    get_record(TestRecord, Field1, undefined).

get_record(TestRecord, Field1, Field2) ->
    get_record(TestRecord, Field1, Field2, undefined).

get_record(globally_cached_record, Field1, Field2, Field3) ->
    #globally_cached_record{field1 = Field1, field2 = Field2, field3 = Field3};
get_record(locally_cached_record, Field1, Field2, Field3) ->
    #locally_cached_record{field1 = Field1, field2 = Field2, field3 = Field3};
get_record(global_only_record, Field1, Field2, Field3) ->
    #global_only_record{field1 = Field1, field2 = Field2, field3 = Field3};
get_record(local_only_record, Field1, Field2, Field3) ->
    #local_only_record{field1 = Field1, field2 = Field2, field3 = Field3};
get_record(disk_only_record, Field1, Field2, Field3) ->
    #disk_only_record{field1 = Field1, field2 = Field2, field3 = Field3};
get_record(globally_cached_sync_record, Field1, Field2, Field3) ->
    #globally_cached_sync_record{field1 = Field1, field2 = Field2, field3 = Field3};
get_record(locally_cached_sync_record, Field1, Field2, Field3) ->
    #locally_cached_sync_record{field1 = Field1, field2 = Field2, field3 = Field3};
get_record(global_only_no_transactions_record, Field1, Field2, Field3) ->
    #global_only_no_transactions_record{field1 = Field1, field2 = Field2, field3 = Field3}.