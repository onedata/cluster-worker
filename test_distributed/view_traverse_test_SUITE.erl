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
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    empty_basic_traverse_test/1,
    single_row_basic_traverse_test/1,
    many_rows_single_batch_basic_traverse_test/1,
    many_batches_basic_traverse_test/1,
    many_async_batches_basic_traverse_test/1,
    job_persistence_test/1
]).

%% view_traverse callbacks
-export([process_row/2]).

-define(MODEL, disc_only_model).
-define(MODEL_BIN, atom_to_binary(?MODEL, utf8)).
-define(CTX, ?DISC_CTX).
-define(VALUE(N), ?MODEL_VALUE(?MODEL, N, ?FUNCTION_NAME)).

-define(DOC(N), ?BASE_DOC(?KEY(N), ?VALUE(N))).
-define(VIEW_FUNCTION, <<"
    function (doc, meta) {
        if (doc.field3 == \"", (?CASE)/binary, "\") {
            emit(doc._key, doc._key);
        }
    }
">>).

-define(CALLBACK_MODULE, ?MODULE).
-define(ATTEMPTS, 10).

all() ->
    ?ALL([
        empty_basic_traverse_test,
        single_row_basic_traverse_test,
        many_rows_single_batch_basic_traverse_test,
        many_batches_basic_traverse_test,
        many_async_batches_basic_traverse_test,
        job_persistence_test
    ]).


%%%===================================================================
%%% Test functions
%%%===================================================================

empty_basic_traverse_test(Config) ->
    basic_batch_traverse_test_base(Config, 0).

single_row_basic_traverse_test(Config) ->
    basic_batch_traverse_test_base(Config, 0).

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
    save_view_doc(W, ?VIEW, ?VIEW_FUNCTION),
    RowsNum = 1000,
    Keys = lists:map(fun(N) ->
        Key = ?KEY(N),
        save_doc(W, ?CTX, Key, ?DOC(N)),
        Key
    end, lists:seq(1, RowsNum)),
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    ok = run_traverse(W, ?VIEW , #{info => #{pid => self(), ref => Ref1}}),
    ok = run_traverse(W, ?VIEW , #{info => #{pid => self(), ref => Ref2}}),

    lists:foreach(fun(K) ->
        receive {processed_row, Ref1, K} -> ok end
    end, Keys),
    lists:foreach(fun(K) ->
        receive {processed_row, Ref2, K} -> ok end
    end, Keys),

    ?assertMatch({ok, [_, _], _}, list_ended_tasks(W, ?CALLBACK_MODULE), ?ATTEMPTS).

%%%===================================================================
%%% Base test functions
%%%===================================================================

basic_batch_traverse_test_base(Config, RowsNum) ->
    basic_batch_traverse_test_base(Config, RowsNum, #{}).

basic_batch_traverse_test_base(Config, RowsNum, Opts) ->
    [W | _] = ?config(cluster_worker_nodes, Config),
    save_view_doc(W, ?VIEW, ?VIEW_FUNCTION),
    Keys = lists:map(fun(N) ->
        Key = ?KEY(N),
        save_doc(W, ?CTX, Key, ?DOC(N)),
        Key
    end, lists:seq(1, RowsNum)),
    Ref = make_ref(),
    ok = run_traverse(W, ?VIEW , Opts#{info => #{pid => self(), ref => Ref}}),
    lists:foreach(fun(K) ->
        receive {processed_row, Ref, K} -> ok end
    end, Keys),
    ?assertMatch({ok, [_], _}, list_ended_tasks(W, ?CALLBACK_MODULE), ?ATTEMPTS).

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
    stop_pool(W).

%%%===================================================================
%%% view_traverse callbacks
%%%===================================================================

process_row(Row, #{pid := TestProcess, ref := Ref}) ->
    EmittedValue = proplists:get_value(<<"value">>, Row),
    TestProcess ! {processed_row, Ref, EmittedValue},
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_pool(W) ->
    ok = rpc:call(W, view_traverse, init, [?MODULE]).

init_pool(W, MasterJobsNum, SlaveJobsNum, ParallelTasksNum) ->
    ok = rpc:call(W, view_traverse, init, [?MODULE, MasterJobsNum, SlaveJobsNum, ParallelTasksNum]).

stop_pool(W) ->
    ok = rpc:call(W, view_traverse, stop, [?MODULE]).

run_traverse(Worker, ViewName, Opts) ->
    ok = rpc:call(Worker, view_traverse, run, [?MODULE, ViewName, Opts]).

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
    rpc:call(Worker, traverse_task_list, list, [Pool, Type]).

clean_traverse_tasks(Worker) ->
    ?assertMatch({ok, [], _}, list_ongoing_tasks(Worker, ?CALLBACK_MODULE), ?ATTEMPTS),
    {ok, TaskIds, _} = list_ended_tasks(Worker, ?CALLBACK_MODULE),
    lists:foreach(fun(T) -> delete_ended_task(Worker, ?CALLBACK_MODULE, T) end, TaskIds),
    ?assertMatch({ok, [], _}, list_ended_tasks(Worker, ?CALLBACK_MODULE)).
