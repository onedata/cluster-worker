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

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    seq_counters_should_be_initialized_on_start/1,
    seq_safe_should_be_incremented_on_doc_save/1,
    seq_safe_should_be_incremented_on_multiple_same_doc_save/1,
    seq_safe_should_be_incremented_on_multiple_diff_docs_save/1,
    seq_safe_should_be_incremented_on_missing_change_doc/1,
    seq_safe_should_be_incremented_on_missing_doc/1,
    stream_should_return_all_changes/1,
    stream_should_return_all_changes_except_mutator/1,
    stream_should_return_changes_from_finite_range/1,
    stream_should_return_changes_from_infinite_range/1,
    cancel_stream_should_stop_worker/1
]).

all() ->
    ?ALL([
        seq_counters_should_be_initialized_on_start,
        seq_safe_should_be_incremented_on_doc_save,
        seq_safe_should_be_incremented_on_multiple_same_doc_save,
        seq_safe_should_be_incremented_on_multiple_diff_docs_save,
        seq_safe_should_be_incremented_on_missing_change_doc,
        seq_safe_should_be_incremented_on_missing_doc,
        stream_should_return_all_changes,
        stream_should_return_all_changes_except_mutator,
        stream_should_return_changes_from_finite_range,
        stream_should_return_changes_from_infinite_range,
        cancel_stream_should_stop_worker
    ]).

-record(test_model, {
    field1 :: integer(),
    field2 :: binary()
}).

-define(MODEL, test_model).
-define(BUCKET, <<"default">>).
-define(CTX, #{bucket => ?BUCKET, mutator => ?MUTATOR}).
-define(CASE, atom_to_binary(?FUNCTION_NAME, utf8)).
-define(KEY, ?KEY(1)).
-define(KEY(N), <<"key-", (?CASE)/binary, "-", (integer_to_binary(N))/binary>>).
-define(VALUE(N), #?MODEL{
    field1 = N,
    field2 = integer_to_binary(N)
}).
-define(SCOPE, ?SCOPE(?FUNCTION_NAME)).
-define(SCOPE(Case), <<"scope-", (atom_to_binary(Case, utf8))/binary>>).
-define(MUTATOR, ?MUTATOR(1)).
-define(MUTATOR(N), <<"mutator-", (?CASE)/binary, "-",
    (integer_to_binary(N))/binary>>).
-define(DOC, ?DOC(1)).
-define(DOC(N), #document2{
    key = ?KEY(N),
    value = ?VALUE(N),
    scope = ?SCOPE
}).

%%%===================================================================
%%% Test functions
%%%===================================================================

seq_counters_should_be_initialized_on_start(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({ok, 0}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_key(?SCOPE), 1]
    )),
    ?assertEqual({ok, 0}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_safe_key(?SCOPE), 1]
    )).

seq_safe_should_be_incremented_on_doc_save(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save, [?CTX, ?DOC]),
    ?assertEqual({ok, 1}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_safe_key(?SCOPE), 1]
    ), 20),
    ?assertEqual({error, key_enoent}, rpc:call(Worker, couchbase_driver, get,
        [?CTX, couchbase_changes:get_change_key(?SCOPE, 1)]
    ), 5).

seq_safe_should_be_incremented_on_multiple_same_doc_save(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    DocNum = 100,
    utils:pforeach(fun(_) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?DOC])
    end, lists:seq(1, DocNum)),
    ?assertEqual({ok, DocNum}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_safe_key(?SCOPE), 1]
    ), 60),
    lists:foreach(fun(N) ->
        ?assertEqual({error, key_enoent}, rpc:call(Worker, couchbase_driver, get,
            [?CTX, couchbase_changes:get_change_key(?SCOPE, N)]
        ), 5)
    end, lists:seq(1, DocNum)).

seq_safe_should_be_incremented_on_multiple_diff_docs_save(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    DocNum = 100,
    utils:pforeach(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?DOC(N)])
    end, lists:seq(1, DocNum)),
    ?assertEqual({ok, DocNum}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_safe_key(?SCOPE), 1]
    ), 60),
    lists:foreach(fun(N) ->
        ?assertEqual({error, key_enoent}, rpc:call(Worker, couchbase_driver, get,
            [?CTX, couchbase_changes:get_change_key(?SCOPE, N)]
        ), 5)
    end, lists:seq(1, DocNum)).

seq_safe_should_be_incremented_on_missing_change_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, update_counter,
        [?CTX, couchbase_changes:get_seq_key(?SCOPE), 1, 0]
    ),
    ?assertEqual({ok, 1}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_safe_key(?SCOPE), 1]
    ), 30).

seq_safe_should_be_incremented_on_missing_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, update_counter,
        [?CTX, couchbase_changes:get_seq_key(?SCOPE), 1, 0]
    ),
    rpc:call(Worker, couchbase_driver, save,
        [?CTX, couchbase_changes:get_change_key(?SCOPE, 1), <<"someId">>]
    ),
    ?assertEqual({ok, 1}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_safe_key(?SCOPE), 1]
    ), 20).

stream_should_return_all_changes(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    DocNum = 100,
    Callback = fun(Doc) -> Self ! Doc end,
    rpc:call(Worker, couchbase_changes, stream, [?BUCKET, ?SCOPE, Callback]),
    utils:pforeach(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?DOC(N)])
    end, lists:seq(1, DocNum)),
    lists:foldl(fun(_, SeqList) ->
        Doc = ?assertReceivedNextMatch(#document2{}, timer:seconds(60)),
        ?assert(lists:member(Doc#document2.seq, SeqList)),
        lists:delete(Doc#document2.seq, SeqList)
    end, lists:seq(1, DocNum), lists:seq(1, DocNum)).

stream_should_return_all_changes_except_mutator(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    DocNum = 100,
    Callback = fun(Doc) -> Self ! Doc end,
    rpc:call(Worker, couchbase_changes, stream,
        [?BUCKET, ?SCOPE, Callback, [{except_mutator, <<"0">>}]]
    ),
    utils:pforeach(fun(N) ->
        rpc:call(Worker, couchbase_driver, save,
            [?CTX#{mutator => integer_to_binary(N rem 10)}, ?DOC(N)]
        )
    end, lists:seq(1, DocNum)),
    KeysExp = lists:filtermap(fun(N) ->
        case N rem 10 =/= 0 of
            true -> {true, ?KEY(N)};
            false -> false
        end
    end, lists:seq(1, DocNum)),
    lists:foldl(fun(_, Keys) ->
        Doc = ?assertReceivedNextMatch(#document2{}, timer:seconds(60)),
        ?assert(lists:member(Doc#document2.key, Keys)),
        lists:delete(Doc#document2.key, Keys)
    end, KeysExp, KeysExp).

stream_should_return_changes_from_finite_range(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    DocNum = 100,
    Since = 25,
    Until = 75,
    Callback = fun(Doc) -> Self ! Doc end,
    rpc:call(Worker, couchbase_changes, stream,
        [?BUCKET, ?SCOPE, Callback, [{since, Since}, {until, Until}]]
    ),
    utils:pforeach(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?DOC(N)])
    end, lists:seq(1, DocNum)),
    lists:foldl(fun(_, SeqList) ->
        Doc = ?assertReceivedNextMatch(#document2{}, timer:seconds(60)),
        ?assert(lists:member(Doc#document2.seq, SeqList)),
        lists:delete(Doc#document2.seq, SeqList)
    end, lists:seq(Since, Until), lists:seq(Since, Until)),
    ?assertNotReceivedMatch(#document2{}, timer:seconds(1)).

stream_should_return_changes_from_infinite_range(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Self = self(),
    DocNum = 100,
    Since = 25,
    Callback = fun(Doc) -> Self ! Doc end,
    rpc:call(Worker, couchbase_changes, stream,
        [?BUCKET, ?SCOPE, Callback, [{since, Since}]]
    ),
    utils:pforeach(fun(N) ->
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?DOC(N)])
    end, lists:seq(1, DocNum)),
    lists:foldl(fun(_, SeqList) ->
        Doc = ?assertReceivedNextMatch(#document2{}, timer:seconds(60)),
        ?assert(lists:member(Doc#document2.seq, SeqList)),
        lists:delete(Doc#document2.seq, SeqList)
    end, lists:seq(Since, DocNum), lists:seq(Since, DocNum)).

cancel_stream_should_stop_worker(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Callback = fun(Doc) -> Doc end,
    {ok, Pid} = ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes, stream,
        [?BUCKET, ?SCOPE, Callback]
    )),
    ?assertEqual(ok, rpc:call(Worker, couchbase_changes, cancel_stream, [Pid])).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    PostHook = fun(Config2) ->
        [Worker | _] = ?config(cluster_worker_nodes, Config2),
        ?assertEqual(ok, rpc:call(Worker, couchbase_changes, enable,
            [[?BUCKET]]
        )),
        Config2
    end,
    [{?ENV_UP_POSTHOOK, PostHook} | Config].

init_per_testcase(Case, Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_new(Workers, ?MODEL, [passthrough, non_strict]),
    test_utils:mock_expect(Workers, ?MODEL, model_init, fun() ->
        #model_config{version = 1}
    end),
    test_utils:mock_expect(Workers, ?MODEL, record_struct, fun(1) ->
        {record, [
            {field1, integer},
            {field2, string}
        ]}
    end),
    ?assertMatch({ok, _}, rpc:call(Worker, couchbase_changes, start,
        [?BUCKET, ?SCOPE(Case)]
    )),
    Config.

end_per_testcase(Case, Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, couchbase_changes, stop,
        [?BUCKET, ?SCOPE(Case)]
    )),
    test_utils:mock_validate_and_unload(Workers, ?MODEL).
