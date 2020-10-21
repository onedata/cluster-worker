%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of view_traverse framework.
%%% @end
%%%-------------------------------------------------------------------
-module(view_traverse_test_SUITE).
-author("Jakub Kudzia").

-behaviour(view_traverse).

-include("datastore_test_utils.hrl").
-include("traverse/view_traverse.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    traverse_over_not_existing_view_should_return_error_test/1,
    empty_basic_traverse_test/1,
    single_row_basic_traverse_test/1,
    many_rows_single_batch_basic_traverse_test/1,
    many_batches_basic_traverse_test/1,
    many_async_batches_basic_traverse_test/1,
    job_persistence_test/1,
    traverse_token_test/1,
    cancel_traverse_test/1
]).

%% view_traverse callbacks
-export([process_row/3, batch_prehook/4, on_batch_canceled/4, task_started/1, task_finished/1, task_canceled/1]).

%% exported for RPC
-export([spawn_and_register_task_callback_receiver/1]).

-define(MODEL, disc_only_model).
-define(MODEL_BIN, atom_to_binary(?MODEL, utf8)).
-define(CTX, ?DISC_CTX).
-define(VALUE(N), ?MODEL_VALUE(?MODEL, N, ?FUNCTION_NAME)).
-define(DOC(N), ?BASE_DOC(?KEY(N), ?VALUE(N))).
-define(KEYS_AND_DOCS(DocsNum), [{?KEY(N), ?DOC(N)} || N <- lists:seq(0, DocsNum - 1)]).

-define(VIEW_FUNCTION, <<"
    function (doc, meta) {
        if (doc.field3 == \"", (?CASE)/binary, "\") {
            emit(doc.field1, doc._key);
        }
    }
">>).

-define(VIEW_PROCESSING_MODULE, ?MODULE).
-define(ATTEMPTS, 60).
-define(PROCESSED_ROW(Ref, Key, Value, RowNum), {processed_row, Ref, Key, Value, RowNum}).
-define(BATCH_TO_PROCESS(Ref, Offset, Size), {batch_to_process, Ref, Offset, Size}).
-define(DEFAULT_BATCH_SIZE, 1000).
-define(ON_BATCH_CANCELED(Ref), {on_batch_cancelled, Ref}).
-define(TASK_STARTED(TaskId), {task_started, TaskId}).
-define(TASK_CANCELED(TaskId), {task_canceled, TaskId}).
-define(TASK_FINISHED(TaskId), {task_finished, TaskId}).
-define(STOP, stop).
-define(TASK_CALLBACK_RECEIVER, task_callback_receiver).


-define(assertTaskStarted(TaskId), 
    ?assertReceivedEqual(?TASK_STARTED(TaskId), timer:seconds(?ATTEMPTS))).

-define(assertTaskFinished(TaskId),
    ?assertReceivedEqual(?TASK_FINISHED(TaskId), timer:seconds(?ATTEMPTS))).

-define(assertTaskCanceled(TaskId),
    ?assertReceivedEqual(?TASK_CANCELED(TaskId), timer:seconds(?ATTEMPTS))).

-define(assertAllRowsProcessed(Ref, KeysAndRowNums),
    ?assertEqual(true, assert_all_rows_processed(Ref, sets:from_list(KeysAndRowNums)), 60)).

-define(assertNoMoreRowsProcessed(Ref),
    ?assertNotReceivedMatch(?PROCESSED_ROW(Ref, _, _, _), timer:seconds(10))).

-define(assertBatchCancelled(Ref),
    ?assertReceivedEqual(?ON_BATCH_CANCELED(Ref), timer:seconds(?ATTEMPTS))).

-define(assertBatchPrehooksCalled(Ref, RowsNum, BatchSize, ExpectOrderedBatches),
    assert_batch_prehooks_called(Ref, RowsNum,  BatchSize, ExpectOrderedBatches)
).

all() ->
    ?ALL([
        traverse_over_not_existing_view_should_return_error_test,
        empty_basic_traverse_test,
        single_row_basic_traverse_test,
        many_rows_single_batch_basic_traverse_test,
        many_batches_basic_traverse_test,
        many_async_batches_basic_traverse_test,
        job_persistence_test,
        traverse_token_test,
        cancel_traverse_test
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

traverse_over_not_existing_view_should_return_error_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, not_found}, run_traverse(W, <<"not_existing_view">>, #{})).

empty_basic_traverse_test(Config) ->
    basic_batch_traverse_test_base(Config, 0).

single_row_basic_traverse_test(Config) ->
    basic_batch_traverse_test_base(Config, 1).

many_rows_single_batch_basic_traverse_test(Config) ->
    basic_batch_traverse_test_base(Config, 1000).

many_batches_basic_traverse_test(Config) ->
    % default batch size is set to 1000
    basic_batch_traverse_test_base(Config, 10000).

many_async_batches_basic_traverse_test(Config) ->
    % default batch size is set to 1000
    basic_batch_traverse_test_base(Config, 10000, #{async_next_batch_job => true}).

job_persistence_test(Config) ->
    % pool is started with ParallelTasksLimit set to 1 in init per testcase
    % scheduling 2 traverses simultaneously must result in persisting tha latter
    [W | _] = ?config(cluster_worker_nodes, Config),
    start_task_callback_receiver(W),
    save_view_doc(W, ?VIEW, ?VIEW_FUNCTION),
    DocsNum = 1000,
    KeysAndDocs = ?KEYS_AND_DOCS(DocsNum),
    KeysAndRowNums = save_docs(W, KeysAndDocs),

    Ref1 = make_ref(),
    Ref2 = make_ref(),
    {ok, TaskId1} = run_traverse(W, ?VIEW , #{info => #{pid => self(), ref => Ref1}}),
    {ok, TaskId2} = run_traverse(W, ?VIEW , #{info => #{pid => self(), ref => Ref2}}),
    ?assertTaskStarted(TaskId1),
    ?assertTaskStarted(TaskId2),
    ?assertAllRowsProcessed(Ref1, KeysAndRowNums),
    ?assertAllRowsProcessed(Ref2, KeysAndRowNums),
    ?assertBatchPrehooksCalled(Ref1, DocsNum, ?DEFAULT_BATCH_SIZE, true),
    ?assertTaskFinished(TaskId1),
    ?assertBatchPrehooksCalled(Ref2, DocsNum, ?DEFAULT_BATCH_SIZE, true),
    ?assertTaskFinished(TaskId2),
    SortedTasks = lists:sort([TaskId1, TaskId2]),
    ?assertMatch({ok, SortedTasks, _}, list_ended_tasks(W, ?VIEW_PROCESSING_MODULE), ?ATTEMPTS).

traverse_token_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),
    start_task_callback_receiver(W),
    save_view_doc(W, ?VIEW, ?VIEW_FUNCTION),
    RowsNum = 1000,
    StartPoint = 500,
    KeysAndDocs = ?KEYS_AND_DOCS(RowsNum),
    KeysAndRowNums = save_docs(W, KeysAndDocs),
    Ref = make_ref(),
    ExpectedKeysAndRowNums = lists:sublist(KeysAndRowNums, StartPoint + 1, length(KeysAndRowNums)),
    {StartKey, RowNum} = lists:nth(StartPoint, KeysAndRowNums),

    Token = #view_traverse_token{
        offset = RowNum + 1,
        last_doc_id = StartKey,
        last_start_key = RowNum
    },

    {ok, TaskId} = run_traverse(W, ?VIEW , #{info => #{pid => self(), ref => Ref}, token => Token}),
    ?assertTaskStarted(TaskId),
    ?assertAllRowsProcessed(Ref, ExpectedKeysAndRowNums),
    ?assertNoMoreRowsProcessed(Ref),
    ?assertTaskFinished(TaskId),
    ?assertMatch({ok, [TaskId], _}, list_ended_tasks(W, ?VIEW_PROCESSING_MODULE), ?ATTEMPTS).

cancel_traverse_test(Config) ->
    % batch size is set to 1 in this test
    RowsNum = 1000,
    [W | _] = ?config(cluster_worker_nodes, Config),
    start_task_callback_receiver(W),
    save_view_doc(W, ?VIEW, ?VIEW_FUNCTION),
    KeysAndDocs = ?KEYS_AND_DOCS(RowsNum),
    KeysAndRowNums = save_docs(W, KeysAndDocs),
    KeysAndRowNumsPart = lists:sublist(KeysAndRowNums, 1),

    Ref = make_ref(),
    {ok, TaskId} = run_traverse(W, ?VIEW , #{
        query_opts => #{limit => 1},
        info => #{pid => self(), ref => Ref}
    }),
    ?assertTaskStarted(TaskId),
    ?assertAllRowsProcessed(Ref, KeysAndRowNumsPart),

    ok = cancel_traverse(W, TaskId),

    ?assertBatchCancelled(Ref),
    ?assertTaskCanceled(TaskId),
    ?assertMatch({ok, [TaskId], _}, list_ended_tasks(W, ?VIEW_PROCESSING_MODULE), ?ATTEMPTS).

%%%===================================================================
%%% Base test functions
%%%===================================================================

basic_batch_traverse_test_base(Config, RowsNum) ->
    basic_batch_traverse_test_base(Config, RowsNum, #{}).

basic_batch_traverse_test_base(Config, RowsNum, Opts) ->
    [W | _] = ?config(cluster_worker_nodes, Config),
    start_task_callback_receiver(W),
    save_view_doc(W, ?VIEW, ?VIEW_FUNCTION),
    KeysAndDocs = ?KEYS_AND_DOCS(RowsNum),
    KeysAndRowNums = save_docs(W, KeysAndDocs),

    Ref = make_ref(),
    {ok, TaskId} = run_traverse(W, ?VIEW , Opts#{info => #{pid => self(), ref => Ref}}),
    ?assertTaskStarted(TaskId),
    ?assertAllRowsProcessed(Ref, KeysAndRowNums),
    ExpectOrderedBatches = not maps:get(async_next_batch_job, Opts, false),
    ?assertBatchPrehooksCalled(Ref, RowsNum, ?DEFAULT_BATCH_SIZE, ExpectOrderedBatches),
    ?assertTaskFinished(TaskId),
    ?assertMatch({ok, [TaskId], _}, list_ended_tasks(W, ?VIEW_PROCESSING_MODULE), ?ATTEMPTS).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    Config2 = datastore_test_utils:init_suite(Config),
    ModulesToLoad = proplists:get_value(?LOAD_MODULES, Config2),
    lists:keyreplace(?LOAD_MODULES, 1, Config2, {?LOAD_MODULES, [?MODULE | ModulesToLoad]}).

end_per_suite(_Config) ->
    ok.

init_per_testcase(job_persistence_test, Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),
    init_pool(W, 10, 20, 1),
    Config;
init_per_testcase(_Case, Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),
    init_pool(W),
    Config.

end_per_testcase(_Case, Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),
    clean_traverse_tasks(W),
    stop_task_callback_receiver(W),
    stop_pool(W).

%%%===================================================================
%%% view_traverse callbacks
%%%===================================================================

process_row(Row, #{pid := TestProcess, ref := Ref}, RowNumber) ->
    EmittedKey = maps:get(<<"key">>, Row),
    EmittedValue = maps:get(<<"value">>, Row),
    TestProcess ! ?PROCESSED_ROW(Ref, EmittedKey, EmittedValue, RowNumber),
    ok.

batch_prehook(_BatchOffset, [], _Token, _Info) ->
    ok;
batch_prehook(BatchOffset, Rows, _Token, #{pid := TestProcess, ref := Ref}) ->
    TestProcess ! ?BATCH_TO_PROCESS(Ref, BatchOffset, length(Rows)),
    ok.

on_batch_canceled(_BatchOffset, _RowJobsCancelled, _Token, #{pid := TestProcess, ref := Ref}) ->
    TestProcess ! ?ON_BATCH_CANCELED(Ref),
    ok.

task_started(TaskId) ->
    ?TASK_CALLBACK_RECEIVER ! ?TASK_STARTED(TaskId),
    ok.

task_finished(TaskId) ->
    ?TASK_CALLBACK_RECEIVER ! ?TASK_FINISHED(TaskId),
    ok.

task_canceled(TaskId) -> 
    ?TASK_CALLBACK_RECEIVER ! ?TASK_CANCELED(TaskId),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_pool(W) ->
    ok = rpc:call(W, view_traverse, init, [?MODULE]).

init_pool(W, MasterJobsNum, SlaveJobsNum, ParallelTasksNum) ->
    ok = rpc:call(W, view_traverse, init, [?MODULE, MasterJobsNum, SlaveJobsNum, ParallelTasksNum, true]).

stop_pool(W) ->
    ok = rpc:call(W, view_traverse, stop, [?MODULE]).

run_traverse(Worker, ViewName, Opts) ->
    rpc:call(Worker, view_traverse, run, [?MODULE, ViewName, Opts]).

cancel_traverse(Worker, TaskId) ->
    ok = rpc:call(Worker, view_traverse, cancel, [?MODULE, TaskId]).

save_view_doc(Worker, View, ViewFunction) ->
    (ok = rpc:call(Worker, couchbase_driver, save_view_doc, [?CTX, View, ViewFunction])).

save_doc(Worker, Ctx, Key, Doc) ->
    rpc:call(Worker, couchbase_driver, save, [Ctx, Key, Doc]).

list_ongoing_tasks(Worker, CallbackModule) ->
    list_tasks(Worker, CallbackModule, ongoing).

list_ended_tasks(Worker, CallbackModule) ->
    list_tasks(Worker, CallbackModule, ended).

delete_ended_task(Worker, CallbackModule, TaskId) ->
    Pool = atom_to_binary(CallbackModule, utf8),
    rpc:call(Worker, traverse_task, delete_ended, [Pool, TaskId]).

list_tasks(Worker, CallbackModule, Type) ->
    Pool = atom_to_binary(CallbackModule, utf8),
    case rpc:call(Worker, traverse_task_list, list, [Pool, Type]) of
        {ok, List, RestartInfo} -> {ok, lists:sort(List), RestartInfo};
        Other -> Other
    end.

clean_traverse_tasks(Worker) ->
    ?assertMatch({ok, [], _}, list_ongoing_tasks(Worker, ?VIEW_PROCESSING_MODULE), ?ATTEMPTS),
    {ok, TaskIds, _} = list_ended_tasks(Worker, ?VIEW_PROCESSING_MODULE),
    lists:foreach(fun(T) -> delete_ended_task(Worker, ?VIEW_PROCESSING_MODULE, T) end, TaskIds),
    ?assertMatch({ok, [], _}, list_ended_tasks(Worker, ?VIEW_PROCESSING_MODULE)).

save_docs(Worker, KeysAndDocs) ->
    % returns list in the form [{Key, RowNum}]
    lists:map(fun({{Key, Doc}, RowNum}) ->
        {ok, _, _} = save_doc(Worker, ?CTX, Key, Doc),
        {Key, RowNum}
    end, lists:zip(KeysAndDocs, lists:seq(0, length(KeysAndDocs) - 1))).

assert_all_rows_processed(Ref, KeysAndRowsSet) ->
    case sets:size(KeysAndRowsSet) =:= 0 of
        true ->
            true;
        false ->
            ?PROCESSED_ROW(Ref, RN, K, RN) =
                ?assertReceivedMatch(?PROCESSED_ROW(Ref, _, _, _), timer:seconds(?ATTEMPTS)),
            assert_all_rows_processed(Ref, sets:del_element({K, RN}, KeysAndRowsSet))
    end.

divide_to_batches(RowsNum, BatchSize) ->
    BatchesNum = ceil(RowsNum / BatchSize),
    lists:map(fun(BatchNum) ->
        Offset = BatchNum * BatchSize,
        Size = min(Offset + BatchSize, RowsNum) - Offset,
        {Offset, Size}
    end, lists:seq(0, BatchesNum - 1)).

assert_batch_prehooks_called(Ref, RowsNum, BatchSize, ExpectOrderedBatches) ->
    Batches = divide_to_batches(RowsNum, BatchSize),
    assert_batch_prehooks_called(Ref, Batches, ExpectOrderedBatches).

assert_batch_prehooks_called(_Ref, [], _ExpectOrderedBatches) ->
    ok;
assert_batch_prehooks_called(Ref, Batches, false) ->
    ?BATCH_TO_PROCESS(Ref, Offset, Size) =
        ?assertReceivedMatch(?BATCH_TO_PROCESS(Ref, _, _), timer:seconds(?ATTEMPTS)),
    assert_batch_prehooks_called(Ref, Batches -- [{Offset, Size}], false);
assert_batch_prehooks_called(Ref, [{ExpectedOffset, ExpectedSize} | Rest], true) ->
    % in this function clause BATCH_TO_PROCESS messages should arrived ordered
    ?BATCH_TO_PROCESS(Ref, Offset, Size) =
        ?assertReceivedMatch(?BATCH_TO_PROCESS(Ref, _, _), timer:seconds(?ATTEMPTS)),
    ?assertEqual(ExpectedOffset, Offset),
    ?assertEqual(ExpectedSize, Size),
    assert_batch_prehooks_called(Ref, Rest, true).

start_task_callback_receiver(Node) ->
    rpc:call(Node, view_traverse_test_SUITE, spawn_and_register_task_callback_receiver, [self()]).

stop_task_callback_receiver(Node) ->
    {?TASK_CALLBACK_RECEIVER, Node} ! ?STOP.

spawn_and_register_task_callback_receiver(TestProcess) ->
    register(?TASK_CALLBACK_RECEIVER, spawn(fun() -> task_callback_receiver_loop(TestProcess) end)).

task_callback_receiver_loop(TestProcess) ->
    receive
        ?STOP ->
            ok;
        Message ->
            TestProcess ! Message,
            task_callback_receiver_loop(TestProcess)
    end.