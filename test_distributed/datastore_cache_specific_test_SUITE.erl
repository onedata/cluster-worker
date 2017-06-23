%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests datastore main API.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_cache_specific_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include("datastore_test_models_def.hrl").

-define(TIMEOUT, timer:minutes(5)).
-define(call_store(N, F, A), ?call(N, datastore, F, A)).
-define(call_store(N, Model, F, A), ?call(N,
    model, execute_with_default_context, [Model, F, A])).
-define(call_store(N, Model, F, A, Override), ?call(N,
    model, execute_with_default_context, [Model, F, A, Override])).
-define(call_disk(N, Model, F, A), ?call(N,
    model, execute_with_default_context, [Model, F, A, [{level, ?DIRECT_DISK_LEVEL}]])).
-define(call(N, M, F, A), ?call(N, M, F, A, ?TIMEOUT)).
-define(call(N, M, F, A, T), rpc:call(N, M, F, A, T)).

-define(test_record_f1(F), {_, F, _, _}).
-define(test_record_f2(F), {_, _, F, _}).
-define(test_record_f3(F), {_, _, _, F}).
-define(test_record_f1_2(F1, F2), {_, F1, F2, _}).
-define(test_record_f1_3(F1, F3), {_, F1, _, F3}).

-define(SCOPE_MASTER_PROC_NAME, sm_proc).

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
%%tests
-export([create_after_delete_global_cache_test/1,
    operations_sequence_global_cache_test/1,
    links_operations_sequence_global_cache_test/1,
    dump_global_cache_test/1]).

all() ->
    ?ALL([
        create_after_delete_global_cache_test,
        operations_sequence_global_cache_test,
        links_operations_sequence_global_cache_test,
        dump_global_cache_test
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

dump_global_cache_test(Config) ->
    [Node | _] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Key = <<"dgct_key">>,
    V1 = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>,
        {test, tuple}),
    ?assertMatch({ok, Key}, ?call_store(Node, TestRecord, create, [
        #document{key = Key, value = V1}
    ])),

    ?assertMatch({ok, #document{value = V1}},
        ?call_store(Node, TestRecord, get, [Key])),

    VerifyDisk = fun() ->
        GetDiskAns = ?call_disk(Node, TestRecord, get, [Key]),
        case GetDiskAns of
            {ok, _, #document{value = V, rev = R, deleted = D}} ->
                {GetDiskAns, V, R, length(R), D};
            _ ->
                GetDiskAns
        end
    end,
    ?assertMatch({{ok, _, _}, V1, _, 1, false}, VerifyDisk(), 10),
    {_, _, Rev1, _, _} = VerifyDisk(),

    ?assertMatch({ok, #document{value = V1, rev = Rev1}},
        ?call_store(Node, TestRecord, get, [Key]), 2),



    ?assertMatch(ok, ?call_store(Node, TestRecord, delete, [
        Key, ?PRED_ALWAYS
    ])),

    ?assertMatch({error, {not_found, _}},
        ?call_store(Node, TestRecord, get, [Key])),

    ?assertMatch({{ok, _, _}, V1, _, 2, true}, VerifyDisk(), 10),



    V2 = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>,
        {test, tuple}),
    ?assertMatch({ok, Key}, ?call_store(Node, TestRecord, create, [
        #document{key = Key, value = V2}
    ])),

    ?assertMatch({{ok, _, _}, V2, _, 3, false}, VerifyDisk(), 10),
    {_, _, Rev3, _, _} = VerifyDisk(),

    ?assertMatch({ok, #document{value = V2, rev = Rev3}},
        ?call_store(Node, TestRecord, get, [Key]), 2),
    ok.

operations_sequence_global_cache_test(Config) ->
    [Worker1, _Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Key = <<"key_ost">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    UpdateFun = #{field1 => 2},

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, get, [Key])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, update, [Key, UpdateFun])),
    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    timer:sleep(1000), % wait to check if any asyc opeartion hasn't changed information in cache
    ?assertMatch({error, {not_found, _}}, ?call(Worker1, TestRecord, get, [Key])),

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, update, [Key, UpdateFun])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, get, [Key])),
    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    timer:sleep(1000), % wait to check if any asyc opeartion hasn't changed information in cache
    ?assertMatch({error, {not_found, _}}, ?call(Worker1, TestRecord, get, [Key])),

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, update, [Key, UpdateFun])),
    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    ?assertMatch({error, {not_found, _}}, ?call(Worker1, TestRecord, get, [Key])),
    timer:sleep(1000), % wait to check if any asyc opeartion hasn't changed information in cache
    ?assertMatch({error, {not_found, _}}, ?call(Worker1, TestRecord, get, [Key])),

    ok.

links_operations_sequence_global_cache_test(Config) ->
    [Worker1, _Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Key = <<"key_lost">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),

    LinkedKey = <<"linked_key_lost">>,
    LinkedDoc = #document{
        key = LinkedKey,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2, <<"efg">>, {test, tuple2})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc])),

    ?assertMatch(ok, ?call_store(Worker1, TestRecord, add_links, [Doc, [{link, LinkedDoc}]])),
    ?assertMatch({ok, _}, ?call_store(Worker1, TestRecord, fetch_link, [Doc, link])),
    ?assertMatch(ok, ?call_store(Worker1, TestRecord, delete_links, [Doc, [link]])),
    timer:sleep(1000), % wait to check if any asyc opeartion hasn't changed information in cache
    ?assertMatch({error,link_not_found}, ?call_store(Worker1, TestRecord, fetch_link, [Doc, link])),

    ?assertMatch(ok, ?call_store(Worker1, TestRecord, add_links, [Doc, [{link, LinkedDoc}]])),
    ?assertMatch(ok, ?call_store(Worker1, TestRecord, delete_links, [Doc, [link]])),
    ?assertMatch({error,link_not_found}, ?call_store(Worker1, TestRecord, fetch_link, [Doc, link])),
    ?assertMatch(ok, ?call_store(Worker1, TestRecord, add_links, [Doc, [{link, LinkedDoc}]])),
    timer:sleep(1000), % wait to check if any asyc opeartion hasn't changed information in cache
    ?assertMatch({ok, _}, ?call_store(Worker1, TestRecord, fetch_link, [Doc, link])),

    ok.

create_after_delete_global_cache_test(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    LinkedKey = <<"linked_key_cadt">>,
    LinkedDoc = #document{
        key = LinkedKey,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2, <<"efg">>, {test, tuple2})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc])),

    Key = <<"key_cadt">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    ?assertMatch(ok, ?call_store(Worker2, TestRecord, add_links, [Doc, [{link, LinkedDoc}]])),
    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    ?assertMatch({ok, _}, ?call(Worker2, TestRecord, create, [Doc])),

    ?assertMatch({error,link_not_found}, ?call_store(Worker1, TestRecord, fetch_link, [Doc, link]), 2),

    ?assertMatch({ok, _, _}, ?call_disk(Worker1, TestRecord, get, [Key]), 10),

    ?assertMatch(ok, ?call_store(Worker2, TestRecord, add_links, [Doc, [{link, LinkedDoc}]])),
    ?assertMatch(ok, ?call_store(Worker1, TestRecord, delete_links, [Doc, [link]])),
    ?assertMatch(ok, ?call_store(Worker2, TestRecord, add_links, [Doc, [{link, LinkedDoc}]])),

    ?assertMatch({ok, _}, ?call_store(Worker1, TestRecord, fetch_link, [Doc, link])),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(Case, Config) ->
    datastore_basic_ops_utils:set_env(Case, Config).

end_per_testcase(_Case, Config) ->
    datastore_basic_ops_utils:clear_env(Config).
