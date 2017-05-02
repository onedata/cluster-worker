%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests couchdb view query api.
%%% @end
%%%-------------------------------------------------------------------
-module(couchdb_index_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("datastore_test_models_def.hrl").

-define(TIMEOUT, timer:seconds(30)).
-define(call_store(N, Model, F, A), ?call(N,
    model, execute_with_default_context, [Model, F, A])).
-define(call(N, M, F, A), ?call(N, M, F, A, ?TIMEOUT)).
-define(call(N, M, F, A, T), rpc:call(N, M, F, A, T)).

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([
    view_query_test/1,
    spatial_view_query_test/1
]).

-performance({test_cases, []}).
all() ->
    ?ALL([
        view_query_test,
        spatial_view_query_test
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

view_query_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),
    ViewId = <<"view_id">>,
    ViewFunction = <<"function (doc, meta) {emit(\"key\", null);}">>,
    ?assertEqual(ok, rpc:call(W, couchdb_datastore_driver, add_view, [test_record_1, ViewId, ViewFunction, false])),

    ?assertMatch({ok, []}, rpc:call(W, couchdb_datastore_driver, query_view, [test_record_1, ViewId, [{stale, false}]])),
    {ok, Id1} = ?assertMatch({ok, _}, ?call_store(W, test_record_1, create, [#document{value = #test_record_1{}}])),
    {ok, Id2} = ?assertMatch({ok, _}, ?call_store(W, test_record_1, create, [#document{value = #test_record_1{}}])),

    {ok, Ids} = ?assertMatch({ok, _}, rpc:call(W, couchdb_datastore_driver, query_view, [test_record_1, ViewId, [{stale, false}]])),
    ?assertEqual(lists:sort([Id1, Id2]), lists:sort(Ids)),

    %remove
    ?assertEqual(ok, rpc:call(W, couchdb_datastore_driver, delete_view, [test_record_1, ViewId])),
    ?assertMatch({error, _}, rpc:call(W, couchdb_datastore_driver, query_view, [test_record_1, ViewId, [{stale, false}]])).

spatial_view_query_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),

    ViewId = <<"view_id">>,
    ViewFunction = <<"function (doc, meta) {emit([1,1], null);}">>,
    ?assertEqual(ok, rpc:call(W, couchdb_datastore_driver, add_view, [test_record_1, ViewId, ViewFunction, true])),
    {ok, Id1} = ?assertMatch({ok, _}, ?call_store(W, test_record_1, create,
        [#document{value = #test_record_1{}}])),
    {ok, Id2} = ?assertMatch({ok, _}, ?call_store(W, test_record_1, create,
        [#document{value = #test_record_1{}}])),

    {ok, Ids} = ?assertMatch({ok, _}, rpc:call(W, couchdb_datastore_driver, query_view,
        [test_record_1, ViewId, [{stale, false}, {start_range, [1,1]}, {end_range, [2,2]}, spatial]])),
    ?assertMatch([_|_], Ids),
    ?assert(lists:member(Id1, Ids)),
    ?assert(lists:member(Id2, Ids)),

    %remove
    ?assertEqual(ok, rpc:call(W, couchdb_datastore_driver, delete_view, [test_record_1, ViewId])),
    ?assertMatch({error, _}, rpc:call(W, couchdb_datastore_driver, query_view, [test_record_1, ViewId, [{stale, false}]])).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(_, Config) ->
    [P1, P2] = ?config(cluster_worker_nodes, Config),
    Models = [test_record_1, test_record_2],

    timer:sleep(3000), % tmp solution until mocking is repaired (VFS-1851)
    test_utils:enable_datastore_models([P1], Models),
    test_utils:enable_datastore_models([P2], Models),
    Config.

end_per_testcase(_, _) ->
    flush(),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Cleans mailbox
flush() ->
    receive
        _ ->
            flush()
    after
        0 ->
            ok
    end.