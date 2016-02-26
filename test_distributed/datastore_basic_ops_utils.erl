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
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").

-define(REQUEST_TIMEOUT, timer:seconds(30)).

-export([create_delete_test/2, create_sync_delete_test/2, save_test/2, save_sync_test/2, update_test/2,
    update_sync_test/2, get_test/2, exists_test/2, mixed_test/2, links_test/2,
    set_hooks/2, unset_hooks/2, clear_cache/1]).

-define(TIMEOUT, timer:seconds(60)).
-define(call_store(Fun, Level, CustomArgs), erlang:apply(datastore, Fun, [Level] ++ CustomArgs)).
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

    set_test_type(Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    Key = list_to_binary("key_" ++ AnswerDesc),
    Doc =  #document{
        key = Key,
        value = #some_record{field1 = 12345, field2 = <<"abcdef">>, field3 = {test, tuple111}}
    },
    ?assertMatch({ok, _}, ?call(Worker1, some_record, create, [Doc])),

    save_many(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

    Create = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(add_links, Level, [Doc,
                    [{list_to_atom("link" ++ DocsSet ++ integer_to_list(I)),
                        {list_to_binary(DocsSet ++ integer_to_list(I)), some_record}}]]),
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
                    Doc, list_to_atom("link" ++ DocsSet ++ integer_to_list(I))]),
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
                    Doc, [list_to_atom("link" ++ DocsSet ++ integer_to_list(I))]]),
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
    clear_with_del(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),
    ?assertMatch(ok, ?call(Worker1, some_record, delete, [Key])),

    [
        #parameter{name = add_time, value = OkTime / OkNum, unit = "us",
            description = "Average time of add operation"},
        #parameter{name = fetch_time, value = OkTimeF / OkNumF, unit = "us",
            description = "Average time of fetch operation"},
        #parameter{name = delete_time, value = OkTime2 / OkNum2, unit = "us",
            description = "Average time of delete operation"}
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

    set_test_type(Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    TestFun = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(Fun, Level, [
                    #document{
                        key = list_to_binary(DocsSet ++ integer_to_list(I)),
                        value = #some_record{field1 = I, field2 = <<"abc">>, field3 = {test, tuple}}
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
    NewTN= case {Level, Fun} of
               {local_only, _} ->
                   TmpTN * WLength;
               {locally_cached, create} ->
                   TmpTN * WLength;
               _ ->
                   TmpTN
           end,
    ModelConfig = some_record:model_init(),
    case ModelConfig#model_config.transactional_global_cache of
        true ->
            CheckOpsNum = DocsPerThead * NewTN,
            ?assertEqual(CheckOpsNum, OkNum);
        _ ->
            ?assert((ThreadsNum * DocsPerThead >= OkNum) and (DocsPerThead * trunc(ThreadsNum / ConflictedThreads) =< OkNum))
    end,

    test_with_get(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

    TestFun2 = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(Fun2, Level, [
                    some_record, list_to_binary(DocsSet ++ integer_to_list(I))]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun2),
    {OkNum2, OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum, OkNum2),

    test_with_get(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, false),

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
    Workers = ?config(cluster_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),

    set_test_type(Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    SaveMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(Fun, Level, [
                    #document{
                        key = list_to_binary(DocsSet ++ integer_to_list(I)),
                        value = #some_record{field1 = I, field2 = <<"abc">>, field3 = {test, tuple}}
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

    test_with_get(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),
    clear_with_del(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, Fun2),

    #parameter{name = save_time, value = OkTime / OkNum, unit = "us",
        description = "Average time of save operation"}.

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

    set_test_type(Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    UpdateMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(Fun, Level, [
                    some_record, list_to_binary(DocsSet ++ integer_to_list(I)),
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

    test_with_get(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, false),

    save_many(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, Fun2),

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, UpdateMany),
    {OkNum3, OkTime3, _ErrorNum3, _ErrorTime3, ErrorsList3} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList3),
    ?assertEqual(OpsNum, OkNum3),

    clear_with_del(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, Fun3),

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

    set_test_type(Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    GetMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(_J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(get, Level, [
                    some_record, list_to_binary(DocsSet ++ integer_to_list(I))
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

    save_many(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, GetMany),
    {OkNum3, OkTime3, _ErrorNum3, _ErrorTime3, ErrorsList3} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList3),
    ?assertEqual(OpsNum, OkNum3),

    clear_with_del(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

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

    set_test_type(Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    ExistMultiCheck = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(_J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(exists, Level, [
                    some_record, list_to_binary(DocsSet ++ integer_to_list(I))
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

    save_many(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, ExistMultiCheck),
    {OkNum3, OkTime3, _ErrorNum3, _ErrorTime3, ErrorsList3} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList3),
    ?assertEqual(OpsNum, OkNum3),

    clear_with_del(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),

    [
        #parameter{name = exists_true_time, value = OkTime3 / OkNum3, unit = "us",
            description = "Average time of exists operation that returned true"},
        #parameter{name = exists_false_time, value = OkTime / OkNum, unit = "us",
            description = "Average time of exists operation that returned false"}
    ].

mixed_test(Config, Level) ->
    [Worker1 | _] = Workers = ?config(cluster_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),

    set_test_type(Workers),
    Master = self(),
    AnswerDesc = get(file_beg),

    CreateMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(create, Level, [
                    #document{
                        key = list_to_binary(DocsSet ++ integer_to_list(I)),
                        value = #some_record{field1 = I, field2 = <<"abc">>, field3 = {test, tuple}}
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
                        value = #some_record{field1 = I, field2 = <<"abc">>, field3 = {test, tuple}}
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
                    some_record, list_to_binary(DocsSet ++ integer_to_list(I)),
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

    {Key, Doc} = case Level of
        local_only ->
            {ok, ok};
        locally_cached ->
            {ok, ok};
        _ ->
            K = list_to_binary("key_" ++ AnswerDesc),
            D =  #document{
                key = K,
                value = #some_record{field1 = 12345, field2 = <<"abcdef">>, field3 = {test, tuple111}}
            },
            ?assertMatch({ok, _}, ?call(Worker1, some_record, create, [D])),

            CreateLinks = fun(DocsSet) ->
                for(1, DocsPerThead, fun(I) ->
                    for(OpsPerDoc, fun() ->
                        BeforeProcessing = os:timestamp(),
                        Ans = ?call_store(add_links, Level, [D,
                            [{list_to_atom("link" ++ DocsSet ++ integer_to_list(I)),
                                {list_to_binary(DocsSet ++ integer_to_list(I)), some_record}}]]),
                        AfterProcessing = os:timestamp(),
                        Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
                    end)
                end)
            end,

            spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, CreateLinks),
            {OkNumCL, OkTimeCL, _ErrorNumCL, _ErrorTimeCL, _ErrorsListCL} = count_answers(OpsNum),
            ?assertEqual(OpsNum, OkNumCL),
            {K, D}
    end,

    GetMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(_J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(get, Level, [
                    some_record, list_to_binary(DocsSet ++ integer_to_list(I))
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
                    some_record, list_to_binary(DocsSet ++ integer_to_list(I))
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
                    Doc, list_to_atom("link" ++ DocsSet ++ integer_to_list(I))]),
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

    case performance:should_clear(Config) of
        true ->
            clear_with_del(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),
            case Level of
                local_only ->
                    ok;
                locally_cached ->
                    ok;
                _ ->
                    clear_with_del_link(Doc, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc),
                    ?assertMatch(ok, ?call(Worker1, some_record, delete, [Key]))
            end;
        false ->
            ok
    end,

    [
        #parameter{name = create_save_update_time, value = (OkTime + ErrorTime) / (OkNum + ErrorNum), unit = "us",
            description = "Average time of create/save/update"},
        #parameter{name = get_exist_time, value = OkTime2 / OkNum2, unit = "us",
            description = "Average time of get/exist"}
    ].

set_hooks(Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    ok = test_node_starter:load_modules(Workers, [?MODULE]),

    Methods = [save, get, exists, delete, update, create, fetch_link, add_links, delete_links],
    ModelConfig = lists:map(fun(Method) ->
        {some_record, Method}
    end, Methods),

    case check_config_name(Case) of
        global ->
            ok;
        local ->
            test_utils:mock_new(Workers, caches_controller),
            test_utils:mock_expect(Workers, caches_controller, cache_to_datastore_level, fun(ModelName) ->
                case lists:member(ModelName, datastore_config:global_caches() -- [some_record]) of
                    true -> global_only;
                    _ -> local_only
                end
            end),
            test_utils:mock_expect(Workers, caches_controller, cache_to_task_level, fun(ModelName) ->
                case lists:member(ModelName, datastore_config:global_caches() -- [some_record]) of
                    true -> cluster;
                    _ -> node
                end
            end);
        _ ->
            lists:foreach(fun(W) ->
                lists:foreach(fun(MC) ->
                    ?assert(?call(W, ets, delete_object, [datastore_local_state, {MC, cache_controller}]))
                end, ModelConfig)
            end, Workers)
    end,

    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, 250)),
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, 1000))
    end, Workers),

    Config.

unset_hooks(Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    [W | _] = Workers,

    Methods = [save, get, exists, delete, update, create, fetch_link, add_links, delete_links],
    ModelConfig = lists:map(fun(Method) ->
        {some_record, Method}
    end, Methods),

    case check_config_name(Case) of
        global ->
            clear_cache(W);
        local ->
            try
                lists:foreach(fun(Wr) ->
                    clear_cache(Wr)
                end, Workers)
            catch
                _:_ ->
                    % Warning showed, let unload mocks
                    ok
            end,
            test_utils:mock_validate_and_unload(Workers, caches_controller);
        _ ->
            lists:foreach(fun(Wr) ->
                lists:foreach(fun(MC) ->
                    ?assert(?call(Wr, ets, insert, [datastore_local_state, {MC, cache_controller}]))
                end, ModelConfig),
                % Clear docs that may be recognized as cached in further tests
                gen_server:call({?NODE_MANAGER_NAME, Wr}, force_clear_node, 60000)
            end, Workers)
    end.

clear_cache(W) ->
    A1 = ?call(W, caches_controller, wait_for_cache_dump, []),
    A2 = gen_server:call({?NODE_MANAGER_NAME, W}, clear_mem_synch, 60000),
    A3 = gen_server:call({?NODE_MANAGER_NAME, W}, force_clear_node, 60000),
    ?assertMatch({ok, ok, {ok, ok}}, {A1, A2, A3}).

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

check_config_name(Case) ->
    CStr = atom_to_list(Case),
    case (string:str(CStr, "cache") > 0) and (string:str(CStr, "sync") == 0) of
        true ->
            case string:str(CStr, "global") > 0 of
                true ->
                    global;
                _ ->
                    local
            end;
        _ ->
            no_hooks
    end.

set_test_type(Workers) ->
    put(file_beg, binary_to_list(term_to_binary(os:timestamp()))),
    disable_cache_control(Workers).
%%     case {performance:is_stress_test(), performance:is_standard_test()} of
%%         {true, _} ->
%%             put(file_beg, binary_to_list(term_to_binary(os:timestamp())));
%%         {false, false} ->
%%             put(file_beg, binary_to_list(term_to_binary(os:timestamp()))),
%%             disable_cache_control(Workers);
%%         _ ->
%%             disable_cache_control(Workers)
%%     end.

test_with_get(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc) ->
    test_with_get(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, true).

test_with_get(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, Exists) ->
    GetMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(get, Level, [
                some_record, list_to_binary(DocsSet ++ integer_to_list(I))]),
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
    {NewTN, NewCT} = case Level of
                local_only ->
                    {TmpTN * WLength, WLength};
                locally_cached ->
                    {TmpTN * WLength, WLength};
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
                Doc, list_to_atom("link" ++ DocsSet ++ integer_to_list(I))]),
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
    {NewTN, NewCT} = case Level of
                         local_only ->
                             {TmpTN * WLength, WLength};
                         locally_cached ->
                             {TmpTN * WLength, WLength};
                         _ ->
                             {TmpTN, 1}
                     end,

    spawn_at_nodes(Workers, NewTN, NewCT, FetchMany),
    OpsNum = DocsPerThead * NewTN,
    {OkNum, _OkTime, _ErrorNum, _ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList),
    ?assertEqual(OpsNum, OkNum).

clear_with_del(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc) ->
    clear_with_del(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, delete).

clear_with_del(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, ClearFun) ->
    GetMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(ClearFun, Level, [
                some_record, list_to_binary(DocsSet ++ integer_to_list(I))]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    TmpTN = trunc(ThreadsNum/ConflictedThreads),
    WLength = length(Workers),
    {NewTN, NewCT} = case Level of
                         local_only ->
                             {TmpTN * WLength, WLength};
                         locally_cached ->
                             {TmpTN * WLength, WLength};
                         _ ->
                             {TmpTN, 1}
                     end,

    spawn_at_nodes(Workers, NewTN, NewCT, GetMany),
    OpsNum = DocsPerThead * NewTN,
    {OkNum, _OkTime, _ErrorNum, _ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList),
    ?assertEqual(OpsNum, OkNum).

clear_with_del_link(Doc, Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc) ->
    GetMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(delete_links, Level, [
                Doc, [list_to_atom("link" ++ DocsSet ++ integer_to_list(I))]]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    TmpTN = trunc(ThreadsNum/ConflictedThreads),
    WLength = length(Workers),
    {NewTN, NewCT} = case Level of
                         local_only ->
                             {TmpTN * WLength, WLength};
                         locally_cached ->
                             {TmpTN * WLength, WLength};
                         _ ->
                             {TmpTN, 1}
                     end,

    spawn_at_nodes(Workers, NewTN, NewCT, GetMany),
    OpsNum = DocsPerThead * NewTN,
    {OkNum, _OkTime, _ErrorNum, _ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList),
    ?assertEqual(OpsNum, OkNum).

save_many(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc) ->
    save_many(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, save).

save_many(Level, Workers, DocsPerThead, ThreadsNum, ConflictedThreads, Master, AnswerDesc, SaveFun) ->
    SaveMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(SaveFun, Level, [
                #document{
                    key = list_to_binary(DocsSet ++ integer_to_list(I)),
                    value = #some_record{field1 = I, field2 = <<"abc">>, field3 = {test, tuple}}
                }]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    TmpTN = trunc(ThreadsNum/ConflictedThreads),
    WLength = length(Workers),
    {NewTN, NewCT} = case Level of
                         local_only ->
                             {TmpTN * WLength, WLength};
                         locally_cached ->
                             {TmpTN * WLength, WLength};
                         _ ->
                             {TmpTN, 1}
                     end,

    spawn_at_nodes(Workers, NewTN, NewCT, SaveMany),
    OpsNum = DocsPerThead * NewTN,
    {OkNum, _OkTime, _ErrorNum, _ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList),
    ?assertEqual(OpsNum, OkNum).