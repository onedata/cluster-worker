%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests datastore main API.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_test_SUITE).
-author("Rafal Slota").

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
-define(call(N, M, F, A), ?call(N, M, F, A, ?TIMEOUT)).
-define(call(N, M, F, A, T), rpc:call(N, M, F, A, T)).

-define(test_record_f1(F), {_, F, _, _}).
-define(test_record_f2(F), {_, _, F, _}).
-define(test_record_f3(F), {_, _, _, F}).
-define(test_record_f1_2(F1, F2), {_, F1, F2, _}).
-define(test_record_f1_3(F1, F3), {_, F1, _, F3}).

-define(SCOPE_MASTER_PROC_NAME, sm_proc).

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
%%tests
-export([local_test/1, global_test/1, global_atomic_update_test/1, globally_cached_atomic_update_test/1,
    disk_list_test/1, global_list_test/1, persistance_test/1, local_list_test/1, globally_cached_list_test/1,
    disk_only_links_test/1, global_only_links_test/1, globally_cached_links_test/1,
    link_walk_test/1, monitoring_global_cache_test_test/1, old_keys_cleaning_global_cache_test/1,
    clearing_global_cache_test/1, link_monitoring_global_cache_test/1, create_after_delete_global_cache_test/1,
    restoring_cache_from_disk_global_cache_test/1, prevent_reading_from_disk_global_cache_test/1,
    multiple_links_creation_disk_test/1, multiple_links_creation_global_only_test/1,
    clear_and_flush_global_cache_test/1, multilevel_foreach_global_cache_test/1,
    operations_sequence_global_cache_test/1, links_operations_sequence_global_cache_test/1,
    interupt_global_cache_clearing_test/1, disk_only_many_links_test/1, global_only_many_links_test/1,
    globally_cached_many_links_test/1, create_globally_cached_test/1, disk_only_create_or_update_test/1,
    global_only_create_or_update_test/1, globally_cached_create_or_update_test/1, links_scope_test/1,
    globally_cached_foreach_link_test/1, links_scope_proc_mem_test/1,  globally_cached_consistency_test/1,
    globally_cached_consistency_without_consistency_metadata_test/1, globally_cached_consistency_with_ambigues_link_names/1]).
-export([utilize_memory/2, update_and_check/4, execute_with_link_context/4, execute_with_link_context/5]).

all() ->
    ?ALL([
        local_test, global_test, global_atomic_update_test, globally_cached_atomic_update_test, disk_list_test,
        global_list_test, persistance_test, local_list_test, globally_cached_list_test,
        disk_only_links_test, global_only_links_test, globally_cached_links_test, link_walk_test,
        monitoring_global_cache_test_test, old_keys_cleaning_global_cache_test, clearing_global_cache_test,
        link_monitoring_global_cache_test, create_after_delete_global_cache_test,
        restoring_cache_from_disk_global_cache_test, prevent_reading_from_disk_global_cache_test,
        multiple_links_creation_disk_test, multiple_links_creation_global_only_test,
        clear_and_flush_global_cache_test, multilevel_foreach_global_cache_test,
        operations_sequence_global_cache_test, links_operations_sequence_global_cache_test,
        interupt_global_cache_clearing_test,
        disk_only_many_links_test, global_only_many_links_test, globally_cached_many_links_test,
        create_globally_cached_test, disk_only_create_or_update_test, global_only_create_or_update_test,
        globally_cached_create_or_update_test, links_scope_test, globally_cached_foreach_link_test,
        links_scope_proc_mem_test, globally_cached_consistency_test,
        globally_cached_consistency_without_consistency_metadata_test,
        globally_cached_consistency_with_ambigues_link_names
    ]).


%%%===================================================================
%%% Test functions
%%%===================================================================

% TODO - add tests that clear cache_controller model and check if cache still works,
% TODO - add tests that check time refreshing by get and fetch_link operations

globally_cached_consistency_with_ambigues_link_names(Config) ->
    %% given
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results

    Key = <<"key_gccwaln">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },

    LinkName = <<"link">>,
    Scope1 = <<"scope1">>,
    Scope2 = <<"scope2">>,
    Scope3 = <<"scope3">>,
    Scope4 = <<"scope4">>,
    VHash = <<"vhash">>,
    LinkTargetKey = <<"target">>,

    {ok, _} = ?call(Worker1, TestRecord, save, [Doc]),

    %% when
    ?assertMatch(ok,
        ?call_store(Worker1, add_links, [?GLOBALLY_CACHED_LEVEL, Doc,
            [{LinkName, {1, [
                {Scope1, VHash, LinkTargetKey, TestRecord},
                {Scope2, VHash, LinkTargetKey, TestRecord},
                {Scope3, VHash, LinkTargetKey, TestRecord},
                {Scope4, VHash, LinkTargetKey, TestRecord}
                ]}}]])),

    ?assertMatch(ok, ?call(Worker1, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, LinkName])),
    ?assertMatch(ok, ?call(Worker1, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, LinkName])),

    ?assertMatch(ok,
        ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc,
            [links_utils:make_scoped_link_name(LinkName, Scope2, undefined, size(Scope2))]])),

    %% then
    ?assertMatch({error, link_not_found},
        ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc,
            links_utils:make_scoped_link_name(LinkName, Scope2, undefined, size(Scope2))])),

    %% when
    ?assertMatch(ok, ?call(Worker1, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, LinkName])),
    ?assertMatch(ok, ?call(Worker1, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, LinkName])),

    ?assertMatch(ok,
        ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc,
            [links_utils:make_scoped_link_name(LinkName, Scope4, undefined, size(Scope4))]])),

    %% when
    Res0 = ?call_store(Worker1, fetch_full_link, [?GLOBALLY_CACHED_LEVEL, Doc, LinkName]),
    ?assertMatch({ok, _}, Res0),

    %% then
    {ok, {_, LinkTargets}} = Res0,
    TargetScopes = [Scope0 || {Scope0, _, _, _} <- LinkTargets],
    ?assertMatch(true, lists:member(Scope1, TargetScopes)),
    ?assertMatch(true, lists:member(Scope3, TargetScopes)),
    ?assertMatch(false, lists:member(Scope4, TargetScopes)),
    ?assertMatch(false, lists:member(Scope2, TargetScopes)),

    ?assertMatch({ok, _},
        ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc,
            links_utils:make_scoped_link_name(LinkName, Scope1, undefined, size(Scope1))])),

    ?assertMatch({ok, _},
        ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc,
            links_utils:make_scoped_link_name(LinkName, Scope3, undefined, size(Scope3))])),


    %% when
    ?assertMatch(ok,
        ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc,
            [links_utils:make_scoped_link_name(LinkName, Scope2, undefined, size(Scope2))]])),

    %% then
    ?assertMatch({ok, _},
        ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc,
            links_utils:make_scoped_link_name(LinkName, Scope1, undefined, size(Scope1))])),
    ?assertMatch({error, link_not_found},
        ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc,
            links_utils:make_scoped_link_name(LinkName, Scope2, undefined, size(Scope2))])),

    ok.


globally_cached_consistency_without_consistency_metadata_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results

    % clean
    GetAllKeys = fun
                     ('$end_of_table', Acc) ->
                         {abort, Acc};
                     (#document{key = Uuid}, Acc) ->
                         {next, [Uuid | Acc]}
                 end,
    {_, AllKeys} = AllKeysAns = ?call_store(Worker1, list, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetAllKeys, []]),
    ?assertMatch({ok, _}, AllKeysAns),
    lists:foreach(fun(Uuid) ->
        ?assertEqual(ok, ?call_store(Worker1, delete, [?GLOBALLY_CACHED_LEVEL, TestRecord, Uuid]))
    end, AllKeys),

    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 100),
    ?assertMatch(ok, ?call(Worker1, caches_controller, delete_old_keys, [globally_cached, 0])),
    ?assertEqual(ok, ?call(Worker2, cache_consistency_controller, delete, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    % functions and definitions
    Key = <<"key_gcct">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },

    GetLinkName = fun(I) ->
        list_to_atom("linked_key_gcct" ++ integer_to_list(I))
    end,
    GetLinkKey = fun(I) ->
        list_to_binary("linked_key_gcct" ++ integer_to_list(I))
    end,

    AddDocs = fun(Start, End) ->
        for(Start, End, fun(I) ->
            LinkedDoc =  #document{
                key = GetLinkKey(I),
                value = datastore_basic_ops_utils:get_record(TestRecord, I, <<"abc">>, {test, tuple})
            },
            ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc])),
            ?assertMatch(ok, ?call_store(Worker1, add_links, [
                ?GLOBALLY_CACHED_LEVEL, Doc, [{GetLinkName(I), LinkedDoc}]
            ]))
        end)
    end,

    CheckLinks = fun(Start, End, Sum) ->
        AccFun = fun(LinkName, _, Acc) ->
            [LinkName | Acc]
        end,
        {_, ListedLinks} = FLAns = ?call_store(Worker1, foreach_link, [?GLOBALLY_CACHED_LEVEL, Doc, AccFun, []]),
        ?assertMatch({ok, _}, FLAns),

        for(Start, End, fun(I) ->
            ?assert(lists:member(GetLinkName(I), ListedLinks))
        end),
        ?assertMatch(Sum, length(ListedLinks))
    end,
    CheckLinksByFetch = fun(Start, End) ->
        for(Start, End, fun(I) ->
            ?assertMatch({ok, _}, ?call_store(Worker2, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, GetLinkName(I)]))
        end)
    end,

    CheckDocs = fun(Start, End, Sum) ->
        {_, Listed} = FLAns = ?call_store(Worker1, list, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetAllKeys, []]),
        ?assertMatch({ok, _}, FLAns),

        for(Start, End, fun(I) ->
            ?assert(lists:member(GetLinkKey(I), Listed))
        end),
        ?assertMatch(Sum, length(Listed))
    end,
    CheckDocsByGet = fun(Start, End) ->
        for(Start, End, fun(I) ->
            ?assertMatch({ok, _}, ?call_store(Worker2, get, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetLinkKey(I)]))
        end)
    end,

    FlushAndClearLink = fun(I) ->
        ?assertMatch(ok, ?call(Worker2, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, GetLinkName(I)])),
        ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, GetLinkName(I)]))
    end,

    FlushAndClearDoc = fun(I) ->
        ?assertMatch(ok, ?call(Worker2, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, GetLinkKey(I)])),
        ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, GetLinkKey(I)]))
    end,

    ClearConsistencyMetadata = fun() ->
        ?assertMatch(ok, ?call(Worker2, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
        ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),

        for(1, 10, fun(I) ->
            FlushAndClearLink(I)
        end),

        for(1, 10, fun(I) ->
            FlushAndClearDoc(I)
        end),

        {_, AllKeys1} = AllKeysAns1 = ?call_store(Worker1, list, [?GLOBAL_ONLY_LEVEL, cache_controller, GetAllKeys, []]),
        ?assertMatch({ok, _}, AllKeysAns1),
        lists:foreach(fun(Uuid) ->
            ?assertEqual(ok, ?call_store(Worker1, delete, [?GLOBAL_ONLY_LEVEL, cache_controller, Uuid]))
        end, AllKeys1),

        {_, AllKeys2} = AllKeysAns2 =
            ?call_store(Worker2, list, [?GLOBAL_ONLY_LEVEL, cache_consistency_controller, GetAllKeys, []]),
        ?assertMatch({ok, _}, AllKeysAns2),
        lists:foreach(fun(Uuid) ->
            ?assertEqual(ok, ?call_store(Worker1, delete, [?GLOBAL_ONLY_LEVEL, cache_consistency_controller, Uuid]))
        end, AllKeys2)
    end,

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    AddDocs(1, 10),

    ClearConsistencyMetadata(),
    CheckLinks(1, 10, 10),
    ClearConsistencyMetadata(),
    CheckLinksByFetch(1, 10),
    ClearConsistencyMetadata(),
    CheckDocs(1, 10, 11),
    ClearConsistencyMetadata(),
    CheckDocsByGet(1, 10),

    ok.

globally_cached_consistency_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results

    % clean
    GetAllKeys = fun
                 ('$end_of_table', Acc) ->
                     {abort, Acc};
                 (#document{key = Uuid}, Acc) ->
                     {next, [Uuid | Acc]}
             end,
    {_, AllKeys} = AllKeysAns = ?call_store(Worker1, list, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetAllKeys, []]),
    ?assertMatch({ok, _}, AllKeysAns),
    lists:foreach(fun(Uuid) ->
        ?assertEqual(ok, ?call_store(Worker1, delete, [?GLOBALLY_CACHED_LEVEL, TestRecord, Uuid]))
    end, AllKeys),

    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 100),
    ?assertMatch(ok, ?call(Worker1, caches_controller, delete_old_keys, [globally_cached, 0])),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    Key = <<"key_gcct">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    CCCUuid = caches_controller:get_cache_uuid(Key, TestRecord),

    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid]), 2),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    GetLinkName = fun(I) ->
        list_to_atom("linked_key_gcct" ++ integer_to_list(I))
    end,
    GetLinkKey = fun(I) ->
        list_to_binary("linked_key_gcct" ++ integer_to_list(I))
    end,

    FlushAndClearLink = fun(I) ->
        ?assertMatch(ok, ?call(Worker2, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, GetLinkName(I)])),
        ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, GetLinkName(I)]))
    end,

    AddDocs = fun(Start, End) ->
        for(Start, End, fun(I) ->
            LinkedDoc =  #document{
                key = GetLinkKey(I),
                value = datastore_basic_ops_utils:get_record(TestRecord, I, <<"abc">>, {test, tuple})
            },
            ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc])),
            ?assertMatch(ok, ?call_store(Worker1, add_links, [
                ?GLOBALLY_CACHED_LEVEL, Doc, [{GetLinkName(I), LinkedDoc}]
            ]))
        end)
    end,
    AddDocs(1, 200),

    CheckLinks = fun(Start, End, Sum) ->
        AccFun = fun(LinkName, _, Acc) ->
            [LinkName | Acc]
        end,
        {_, ListedLinks} = FLAns = ?call_store(Worker1, foreach_link, [?GLOBALLY_CACHED_LEVEL, Doc, AccFun, []]),
        ?assertMatch({ok, _}, FLAns),

        for(Start, End, fun(I) ->
            ?assert(lists:member(GetLinkName(I), ListedLinks))
        end),
        ?assertMatch(Sum, length(ListedLinks))
    end,
    CheckLinksByFetch = fun(Start, End) ->
        for(Start, End, fun(I) ->
            ?assertMatch({ok, _}, ?call_store(Worker2, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, GetLinkName(I)]))
        end)
    end,
    CheckLinks(1, 200, 200),
    CheckLinksByFetch(1, 200),

    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    for(10, 20, fun(I) ->
        FlushAndClearLink(I)
    end),

    for(22, 23, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [GetLinkName(I)]]))
    end),

    ?assertMatch({monitored, _, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    CheckLinks(10, 20, 198),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    for(30, 40, fun(I) ->
        FlushAndClearLink(I)
    end),

    for(42, 43, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [GetLinkName(I)]]))
    end),

    ?assertMatch({monitored, _, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    CheckLinksByFetch(30, 40),
    timer:sleep(timer:seconds(5)), % for posthooks
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    for(50, 100, fun(I) ->
        FlushAndClearLink(I)
    end),

    ?assertEqual(not_monitored, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    CheckLinksByFetch(50, 100),
    timer:sleep(timer:seconds(5)), % for posthooks

    ?assertMatch(not_monitored, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    for(150, 200, fun(I) ->
        FlushAndClearLink(I)
    end),
    CheckLinks(50, 200, 196),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),


    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    FlushAndClearDoc = fun(I) ->
        ?assertMatch(ok, ?call(Worker2, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, GetLinkKey(I)])),
        ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, GetLinkKey(I)]))
    end,

    CheckDocs = fun(Start, End, Sum) ->
        {_, Listed} = FLAns = ?call_store(Worker1, list, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetAllKeys, []]),
        ?assertMatch({ok, _}, FLAns),

        for(Start, End, fun(I) ->
            ?assert(lists:member(GetLinkKey(I), Listed))
        end),
        ?assertMatch(Sum, length(Listed))
    end,
    CheckDocsByGet = fun(Start, End) ->
        for(Start, End, fun(I) ->
            ?assertMatch({ok, _}, ?call_store(Worker2, get, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetLinkKey(I)]))
        end)
    end,
    CheckDocs(1, 200, 201),
    CheckDocsByGet(1, 200),

    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    for(10, 20, fun(I) ->
        FlushAndClearDoc(I)
    end),

    for(22, 23, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, delete, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetLinkKey(I)]))
    end),

    ?assertMatch({monitored, _, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),

    CheckDocs(10, 20, 199),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),

    for(30, 40, fun(I) ->
        FlushAndClearDoc(I)
    end),

    for(42, 43, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, delete, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetLinkKey(I)]))
    end),

    ?assertMatch({monitored, _, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),

    CheckDocsByGet(30, 40),
    timer:sleep(timer:seconds(5)), % for posthooks
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),

    for(50, 100, fun(I) ->
        FlushAndClearDoc(I)
    end),

    ?assertEqual(not_monitored, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),

    CheckDocsByGet(50, 100),
    timer:sleep(timer:seconds(5)), % for posthooks

    ?assertEqual(not_monitored, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),

    for(150, 200, fun(I) ->
        FlushAndClearDoc(I)
    end),
    CheckDocs(50, 200, 197),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, CCCUuid])),

    ok.

globally_cached_foreach_link_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),

    Key = <<"key_gcflt">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),

    AddDocs = fun(Start, End) ->
        for(Start, End, fun(I) ->
            LinkName = list_to_binary("key_gcflt" ++ integer_to_list(I)),
            LinkedDoc =  #document{
                key = LinkName,
                value = datastore_basic_ops_utils:get_record(TestRecord, I, <<"abc">>, {test, tuple})
            },
            ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc])),
            ?assertMatch(ok, ?call_store(Worker1, add_links, [
                ?GLOBALLY_CACHED_LEVEL, Doc, [{LinkName, LinkedDoc}]
            ]))
        end)
    end,
    AddDocs(1, 2),
    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, <<"key_gcflt1">>]), 6),
    ?call_store(Worker2, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [<<"key_gcflt1">>]]),

    AccFun = fun(LinkName, _, Acc) ->
        [LinkName | Acc]
    end,

    ?assertMatch({ok, [<<"key_gcflt2">>]},
        ?call_store(Worker1, foreach_link, [?GLOBALLY_CACHED_LEVEL, Doc, AccFun, []])),

    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, <<"key_gcflt2">>]), 6),
    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, <<"key_gcflt2">>])),

    ?assertMatch({ok, [<<"key_gcflt2">>]},
        ?call_store(Worker1, foreach_link, [?GLOBALLY_CACHED_LEVEL, Doc, AccFun, []])),

    ok.

links_scope_test(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    MasterLoop = spawn_link(fun() -> scope_master_loop() end),
    put(?SCOPE_MASTER_PROC_NAME, MasterLoop),
    rpc:call(Worker1, global, register_name, [?SCOPE_MASTER_PROC_NAME, MasterLoop]),
    rpc:call(Worker1, global, sync, []),

    Key = <<"key_lst">>,
    Doc =  #document{
        key = Key,
        value = #link_scopes_test_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
    },
    ?assertMatch({ok, _}, ?call(Worker1, link_scopes_test_record, create, [Doc])),

    GetLinkName = fun(I) ->
        list_to_atom("linked_key_lst" ++ integer_to_list(I))
    end,
    GetDocKey = fun(I) ->
        list_to_binary("linked_key_lst" ++ integer_to_list(I))
    end,
    GetDoc = fun(I) ->
        #document{
            key = GetDocKey(I),
            value = #link_scopes_test_record{field1 = I, field2 = <<"efg">>, field3 = {test, tuple2}}
        }
    end,
    AddLink = fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, add_links, [?GLOBAL_ONLY_LEVEL, Doc, [{GetLinkName(I), GetDoc(I)}]]))
    end,
    AddLinkWithDoc = fun(I) ->
        ?assertMatch({ok, _}, ?call(Worker2, link_scopes_test_record, create, [GetDoc(I)])),
        AddLink(I)
    end,
    CreateLink = fun(I) ->
        ?assertMatch(ok, ?call_store(Worker2, create_link, [?GLOBAL_ONLY_LEVEL, Doc, {GetLinkName(I), GetDoc(I)}]))
    end,
    CreateExistingLink = fun(I) ->
        ?assertMatch({error, already_exists}, ?call_store(Worker2, create_link, [?GLOBAL_ONLY_LEVEL, Doc, {GetLinkName(I), GetDoc(I)}]))
    end,
    CreateLinkWithDoc = fun(I) ->
        ?assertMatch({ok, _}, ?call(Worker1, link_scopes_test_record, create, [GetDoc(I)])),
        CreateLink(I)
    end,
    FetchLink = fun(I) ->
        ?assertMatch({ok, _}, ?call_store(Worker2, fetch_link, [?GLOBAL_ONLY_LEVEL, Doc, GetLinkName(I)]))
    end,
    GetAllLinks = fun(Links) ->
        AccFun = fun(LinkName, _, Acc) ->
            [LinkName | Acc]
                 end,
        {_, ListedLinks} = FLAns = ?call_store(Worker1, foreach_link, [?GLOBAL_ONLY_LEVEL, Doc, AccFun, []]),
        ?assertMatch({ok, _}, FLAns),

        lists:foreach(fun(I) ->
            ?assert(lists:member(GetLinkName(I), ListedLinks))
        end, Links),
        LinksLength = length(Links),
        ?assertMatch(LinksLength, length(ListedLinks))
    end,
    DeleteLink = fun(I) ->
        ?assertMatch(ok, ?call_store(Worker2, delete_links, [?GLOBAL_ONLY_LEVEL, Doc, [GetLinkName(I)]]))
    end,
    DeleteLinks = fun(Links) ->
        ?assertMatch(ok, ?call_store(Worker2, delete_links, [?GLOBAL_ONLY_LEVEL, Doc,
            lists:map(fun(I) -> GetLinkName(I) end, Links)]))
    end,

    ?assertMatch({ok, false}, ?call_store(Worker2, exists_link_doc, [?GLOBAL_ONLY_LEVEL, Doc, <<"scope1">>])),
    ?assertMatch({ok, false}, ?call_store(Worker2, exists_link_doc, [?GLOBAL_ONLY_LEVEL, Doc, <<"scope2">>])),

    set_mother_scope(<<"scope1">>),
    AddLinkWithDoc(1),
    AddLinkWithDoc(2),
    FetchLink(1),
    FetchLink(2),
    GetAllLinks([1,2]),
    CreateExistingLink(2),
    CreateLinkWithDoc(3),
    FetchLink(3),
    CreateLinkWithDoc(4),
    FetchLink(4),
    CreateLinkWithDoc(5),
    FetchLink(5),
    GetAllLinks([1,2,3,4,5]),
    DeleteLinks([1,5]),
    GetAllLinks([2,3,4]),
    DeleteLink(100),
    GetAllLinks([2,3,4]),

    ?assertMatch({ok, true}, ?call_store(Worker2, exists_link_doc, [?GLOBAL_ONLY_LEVEL, Doc, <<"scope1">>])),
    ?assertMatch({ok, false}, ?call_store(Worker2, exists_link_doc, [?GLOBAL_ONLY_LEVEL, Doc, <<"scope2">>])),

    set_mother_scope(<<"scope2">>),
    GetAllLinks([2,3,4]),
    DeleteLinks([2,3,4]),
    GetAllLinks([]),
    AddLink(2),
    AddLink(3),
    AddLink(4),
    GetAllLinks([2,3,4]),
    FetchLink(2),
    FetchLink(3),
    AddLink(2),
    AddLink(5),
    AddLinkWithDoc(6),
    FetchLink(5),
    FetchLink(6),
    CreateExistingLink(3),
    CreateLinkWithDoc(7),
    FetchLink(7),
    GetAllLinks([2,3,4,5,6,7]),
    DeleteLinks([3, 7, 100]),
    GetAllLinks([2,4,5,6]),

    ?assertMatch({ok, true}, ?call_store(Worker2, exists_link_doc, [?GLOBAL_ONLY_LEVEL, Doc, <<"scope1">>])),
    ?assertMatch({ok, true}, ?call_store(Worker2, exists_link_doc, [?GLOBAL_ONLY_LEVEL, Doc, <<"scope2">>])),

    set_mother_scope(<<"scope1">>),
    DeleteLinks([3, 7, 100, 5, 6]),
    GetAllLinks([2,4]),
    ?assertMatch(ok, ?call_store(Worker2, create_link, [?GLOBAL_ONLY_LEVEL, Doc, {GetLinkName(5), GetDoc(1)}])),
    AddLinkWithDoc(8),
    GetAllLinks([2,4,5,8]),
    DK1 = GetDocKey(1),
    ?assertMatch({ok, {DK1, _}}, ?call_store(Worker2, fetch_link, [?GLOBAL_ONLY_LEVEL, Doc, GetLinkName(5)])),
    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBAL_ONLY_LEVEL, Doc, [{GetLinkName(2), GetDoc(1)}]])),
    ?assertMatch({ok, {DK1, _}}, ?call_store(Worker2, fetch_link, [?GLOBAL_ONLY_LEVEL, Doc, GetLinkName(2)])),

    MasterLoop ! stop,
    ok.

links_scope_proc_mem_test(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    ModelConfig = link_scopes_test_record2:model_init(),

    Key = <<"key_lspmt">>,
    Doc =  #document{
        key = Key,
        value = #link_scopes_test_record2{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
    },
    ?assertMatch({ok, _}, ?call(Worker1, link_scopes_test_record2, create, [Doc])),

    GetLinkName = fun(I) ->
        list_to_atom("linked_key_lspmt" ++ integer_to_list(I))
    end,
    GetDocKey = fun(I) ->
        list_to_binary("linked_key_lspmt" ++ integer_to_list(I))
    end,
    GetDoc = fun(I) ->
        #document{
            key = GetDocKey(I),
            value = #link_scopes_test_record2{field1 = I, field2 = <<"efg">>, field3 = {test, tuple2}}
        }
    end,
    AddLink = fun(I, MotherScope, OtherScopes) ->
        ?assertMatch(ok, ?call(Worker1, ?MODULE, execute_with_link_context, [MotherScope, OtherScopes,
            add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{GetLinkName(I), GetDoc(I)}]]]))
    end,
    AddLinkWithDoc = fun(I, MotherScope, OtherScopes) ->
        ?assertMatch({ok, _}, ?call(Worker2, link_scopes_test_record2, create, [GetDoc(I)])),
        AddLink(I, MotherScope, OtherScopes)
    end,
    CreateLink = fun(I, MotherScope, OtherScopes) ->
        ?assertMatch(ok, ?call(Worker2, ?MODULE, execute_with_link_context, [MotherScope, OtherScopes,
            create_link, [?GLOBALLY_CACHED_LEVEL, Doc, {GetLinkName(I), GetDoc(I)}]]))
    end,
    CreateExistingLink = fun(I, MotherScope, OtherScopes) ->
        ?assertMatch({error, already_exists}, ?call(Worker2, ?MODULE, execute_with_link_context, [MotherScope, OtherScopes,
            create_link, [?GLOBALLY_CACHED_LEVEL, Doc, {GetLinkName(I), GetDoc(I)}]]))
    end,
    CreateLinkWithDoc = fun(I, MotherScope, OtherScopes) ->
        ?assertMatch({ok, _}, ?call(Worker1, link_scopes_test_record2, create, [GetDoc(I)])),
        CreateLink(I, MotherScope, OtherScopes)
    end,
    FetchLink = fun(I, MotherScope, OtherScopes) ->
        ?assertMatch({ok, _}, ?call(Worker2, ?MODULE, execute_with_link_context, [MotherScope, OtherScopes,
            fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, GetLinkName(I)]])),
        ?assertMatch({ok, _}, ?call(Worker2, ?MODULE, execute_with_link_context, [MotherScope, OtherScopes, PModule,
            fetch_link, [ModelConfig, Key, GetLinkName(I)]]), 6)
    end,
    GetAllLinks = fun(Links, MotherScope, OtherScopes) ->
        AccFun = fun(LinkName, LinkValue, Acc) ->
            maps:put(LinkName, LinkValue, Acc)
        end,
        {_, ListedLinks} = FLAns = ?call(Worker1, ?MODULE, execute_with_link_context, [MotherScope, OtherScopes,
            foreach_link, [?GLOBALLY_CACHED_LEVEL, Doc, AccFun, #{}]]),
        ?assertMatch({ok, _}, FLAns),

        lists:foreach(fun(I) ->
            ?assert(maps:is_key(GetLinkName(I), ListedLinks))
        end, Links),
        LinksLength = length(Links),
        ?assertMatch(LinksLength, maps:size(ListedLinks))
    end,
    DeleteLink = fun(I, MotherScope, OtherScopes) ->
        ?assertMatch(ok, ?call(Worker1, ?MODULE, execute_with_link_context, [MotherScope, OtherScopes,
            delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [GetLinkName(I)]]]))
    end,
    DeleteLinkAndCheck = fun(I, MotherScope, OtherScopes) ->
        DeleteLink(I, MotherScope, OtherScopes),
        ?assertMatch({error, link_not_found}, ?call(Worker2, ?MODULE, execute_with_link_context, [MotherScope, OtherScopes,
            fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, GetLinkName(I)]])),
        ?assertMatch({error, link_not_found}, ?call(Worker2, ?MODULE, execute_with_link_context, [MotherScope, OtherScopes, PModule,
            fetch_link, [ModelConfig, Key, GetLinkName(I)]]), 6)
                 end,
    DeleteLinks = fun(Links, MotherScope, OtherScopes) ->
        ?assertMatch(ok, ?call(Worker2, ?MODULE, execute_with_link_context, [MotherScope, OtherScopes,
            delete_links, [?GLOBALLY_CACHED_LEVEL, Doc,
            lists:map(fun(I) -> GetLinkName(I) end, Links)]])),
        lists:map(fun(I) ->
            ?assertMatch({error, link_not_found}, ?call(Worker2, ?MODULE, execute_with_link_context, [MotherScope, OtherScopes,
                fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, GetLinkName(I)]])),
            ?assertMatch({error, link_not_found}, ?call(Worker2, ?MODULE, execute_with_link_context, [MotherScope, OtherScopes, PModule,
                fetch_link, [ModelConfig, Key, GetLinkName(I)]]), 6)
        end, Links)
    end,

    AddLinkWithDoc(1, <<"scope1">>, []),
    AddLinkWithDoc(2, <<"scope1">>, []),
    FetchLink(1, <<"scope1">>, []),
    FetchLink(2, <<"scope1">>, []),
    GetAllLinks([1,2], <<"scope1">>, []),
    CreateExistingLink(2, <<"scope1">>, []),
    CreateLinkWithDoc(3, <<"scope1">>, []),
    FetchLink(3, <<"scope1">>, []),
    CreateLinkWithDoc(4, <<"scope1">>, []),
    FetchLink(4, <<"scope1">>, []),
    CreateLinkWithDoc(5, <<"scope1">>, []),
    FetchLink(5, <<"scope1">>, []),
    GetAllLinks([1,2,3,4,5], <<"scope1">>, []),
    DeleteLinks([1,5], <<"scope1">>, []),
    GetAllLinks([2,3,4], <<"scope1">>, []),
    DeleteLinkAndCheck(100, <<"scope1">>, []),
    GetAllLinks([2,3,4], <<"scope1">>, []),

    GetAllLinks([2,3,4], <<"scope2">>, []),
    DeleteLinks([2,3,4], <<"scope2">>, []),

    AddLink(2, <<"scope2">>, []),
    AddLink(3, <<"scope2">>, []),
    AddLink(4, <<"scope2">>, []),
    GetAllLinks([2,3,4], <<"scope2">>, []),
    FetchLink(2, <<"scope2">>, []),
    FetchLink(3, <<"scope2">>, []),
    AddLink(2, <<"scope2">>, []),
    AddLink(5, <<"scope2">>, []),
    AddLinkWithDoc(6, <<"scope2">>, []),
    FetchLink(5, <<"scope2">>, []),
    FetchLink(6, <<"scope2">>, []),
    CreateExistingLink(3, <<"scope2">>, []),
    CreateLinkWithDoc(7, <<"scope2">>, []),
    FetchLink(7, <<"scope2">>, []),
    GetAllLinks([2,3,4,5,6,7], <<"scope2">>, []),
    DeleteLinks([3, 7, 100], <<"scope2">>, []),


    GetAllLinks([2,4,5,6], <<"scope1">>, []),
    DeleteLinks([3, 7, 100, 5,6], <<"scope1">>, []),
    ?assertMatch(ok, ?call(Worker2, ?MODULE, execute_with_link_context, [<<"scope1">>, [],
        create_link, [?GLOBALLY_CACHED_LEVEL, Doc, {GetLinkName(5), GetDoc(1)}]])),
    AddLinkWithDoc(8, <<"scope1">>, []),
    GetAllLinks([2,4,5,8], <<"scope1">>, []),

    DK1 = GetDocKey(1),
    ?assertMatch({ok, {DK1, _}}, ?call(Worker2, ?MODULE, execute_with_link_context, [<<"scope1">>, [],
            fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, GetLinkName(5)]])),
    ?assertMatch(ok, ?call(Worker2, ?MODULE, execute_with_link_context, [<<"scope1">>, [],
        add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{GetLinkName(2), GetDoc(1)}]]])),
    GetAllLinks([2,4,5,8], <<"scope1">>, []),
    ?assertMatch({ok, {DK1, _}}, ?call(Worker2, ?MODULE, execute_with_link_context, [<<"scope1">>, [<<"scope2">>],
        fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, GetLinkName(2)]])),


    DeleteLinks([2,4,5,6,8], <<"scope2">>, []),
    GetAllLinks([], <<"scope2">>, []),

    ok.

execute_with_link_context(MotherScope, OtherScopes, Op, Args) ->
    execute_with_link_context(MotherScope, OtherScopes, datastore, Op, Args).

execute_with_link_context(MotherScope, OtherScopes, Module, Op, Args) ->
    put(mother_scope, MotherScope),
    put(other_scopes, OtherScopes),
    apply(Module, Op, Args).

set_mother_scope(MotherScope) ->
    Pid = get(?SCOPE_MASTER_PROC_NAME),
    Pid ! {set_mother_scope, self(), MotherScope},
    receive
        scope_changed -> ok
    end.

scope_master_loop() ->
    scope_master_loop(<<"scope1">>, []).

scope_master_loop(MotherScope, OtherScopes) ->
    Todo = receive
               {get_mother_scope, Sender} ->
                   Sender ! {mother_scope, MotherScope};
               {get_other_scopes, Sender2} ->
                   Sender2 ! {other_scopes, OtherScopes};
               {set_mother_scope, Sender3, MotherScope2} ->
                   Sender3 ! scope_changed,
                   {change_scopes, MotherScope2, OtherScopes};
               {set_other_scopes, Sender4, OtherScopes2} ->
                   Sender4 ! scope_changed,
                   {change_scopes, MotherScope, OtherScopes2};
               stop ->
                   stop
           end,
    case Todo of
        stop ->
            ok;
        {change_scopes, MotherScope3, OtherScopes3} ->
            scope_master_loop(MotherScope3, OtherScopes3);
        _ ->
            scope_master_loop(MotherScope, OtherScopes)
    end.

disk_only_create_or_update_test(Config) ->
    create_or_update_test_base(Config, ?DISK_ONLY_LEVEL).

global_only_create_or_update_test(Config) ->
    create_or_update_test_base(Config, ?GLOBAL_ONLY_LEVEL).

globally_cached_create_or_update_test(Config) ->
    UpdateMap = #{field1 => 2},
    UpdateMap2 = #{field1 => 3},
    UpdateFun = fun({R, _F1, F2, F3}) ->
        {ok, {R, 2, F2, F3}}
                end,
    UpdateFun2 = fun({R, _F1, F2, F3}) ->
        {ok, {R, 3, F2, F3}}
    end,
    globally_cached_create_or_update_test_base(Config, UpdateMap, UpdateMap2, "map"),
    globally_cached_create_or_update_test_base(Config, UpdateFun, UpdateFun2, "fun").

globally_cached_create_or_update_test_base(Config, UpdateEntity, UpdateEntity2, KeyExt) ->
    Level = ?GLOBALLY_CACHED_LEVEL,
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    ModelConfig = TestRecord:model_init(),

    Key = list_to_binary("key_coutb_" ++ atom_to_list(Level) ++ KeyExt),
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    Doc2 =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 10, <<"def">>, {test, tuple})
    },

    ?assertMatch({ok, _}, ?call_store(Worker1, create_or_update, [Level, Doc, UpdateEntity])),
    ?assertMatch({ok, #document{value = ?test_record_f1(1)}},
        ?call_store(Worker2, get, [Level,
            TestRecord, Key])),
    ?assertMatch({ok, #document{value = ?test_record_f1(1)}},
        ?call(Worker1, PModule, get, [ModelConfig, Key]), 6),

    ?assertMatch({ok, _}, ?call_store(Worker1, create_or_update, [Level, Doc2, UpdateEntity])),
    ?assertMatch({ok, #document{value = ?test_record_f1(2)}},
        ?call_store(Worker2, get, [Level,
            TestRecord, Key])),
    ?assertMatch({ok, #document{value = ?test_record_f1(2)}},
        ?call(Worker1, PModule, get, [ModelConfig, Key]), 6),

    ?assertMatch(ok, ?call(Worker1, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, _}, ?call_store(Worker1, create_or_update, [Level, Doc2, UpdateEntity2])),
    ?assertMatch({ok, #document{value = ?test_record_f1(3)}},
        ?call_store(Worker2, get, [Level,
            TestRecord, Key])),
    ?assertMatch({ok, #document{value = ?test_record_f1(3)}},
        ?call(Worker1, PModule, get, [ModelConfig, Key]), 6),
    ok.

create_or_update_test_base(Config, Level) ->
    UpdateMap = #{field1 => 2},
    UpdateFun = fun({R, _F1, F2, F3}) ->
        {ok, {R, 2, F2, F3}}
    end,
    create_or_update_test_base(Config, Level, UpdateMap, "map"),
    create_or_update_test_base(Config, Level, UpdateFun, "fun").

create_or_update_test_base(Config, Level, UpdateEntity, KeyExt) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Key = list_to_binary("key_coutb_" ++ atom_to_list(Level) ++ KeyExt),
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    Doc2 =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 10, <<"def">>, {test, tuple})
    },

    ?assertMatch({ok, _}, ?call_store(Worker1, create_or_update, [Level, Doc, UpdateEntity])),
    ?assertMatch({ok, #document{value = ?test_record_f1(1)}},
        ?call_store(Worker2, get, [Level,
            TestRecord, Key])),

    ?assertMatch({ok, _}, ?call_store(Worker1, create_or_update, [Level, Doc2, UpdateEntity])),
    ?assertMatch({ok, #document{value = ?test_record_f1(2)}},
        ?call_store(Worker2, get, [Level,
            TestRecord, Key])),
    ok.

create_globally_cached_test(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
    end, Workers),

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    Key = <<"key_cgct">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 6),

    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({error, already_exists}, ?call(Worker1, TestRecord, create, [Doc])),

    ?assertMatch(ok, ?call(Worker2, PModule, delete, [ModelConfig, Key, ?PRED_ALWAYS])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 6),

    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    timer:sleep(timer:seconds(6)), % wait to check if any async action hasn't destroyed value at disk
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 6),

    LinkedKey = "linked_key_cgct",
    LinkedDoc = #document{
        key = LinkedKey,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2, <<"efg">>, {test, tuple2})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc])),
    ?assertMatch(ok, ?call_store(Worker1, create_link, [?GLOBALLY_CACHED_LEVEL, Doc, {link, LinkedDoc}])),
    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, link]), 6),

    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, link])),
    ?assertMatch({error, already_exists}, ?call_store(Worker1, create_link, [?GLOBALLY_CACHED_LEVEL, Doc, {link, LinkedDoc}])),

    ?assertMatch(ok, ?call(Worker2, PModule, delete_links, [ModelConfig, Key, [link]])),
    ?assertMatch(ok, ?call_store(Worker1, create_link, [?GLOBALLY_CACHED_LEVEL, Doc, {link, LinkedDoc}])),
    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, link]), 6),

    ?assertMatch(ok, ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [link]])),
    ?assertMatch(ok, ?call_store(Worker1, create_link, [?GLOBALLY_CACHED_LEVEL, Doc, {link, LinkedDoc}])),
    timer:sleep(timer:seconds(6)), % wait to check if any async action hasn't destroyed value at disk
    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, link]), 6),

    ok.

disk_only_many_links_test(Config) ->
    many_links_test_base(Config, ?DISK_ONLY_LEVEL).

global_only_many_links_test(Config) ->
    many_links_test_base(Config, ?GLOBAL_ONLY_LEVEL).

globally_cached_many_links_test(Config) ->
    many_links_test_base(Config, ?GLOBALLY_CACHED_LEVEL).

many_links_test_base(Config, Level) ->
    GetLinkKey1 = fun(LinkedDocKey, I) ->
        list_to_binary(LinkedDocKey ++ integer_to_list(I))
    end,
    GetLinkKey2 = fun(LinkedDocKey, I) ->
        list_to_atom(LinkedDocKey ++ integer_to_list(I))
    end,
    KeyFuns = [GetLinkKey1, GetLinkKey2],

    GetLinkName1 = fun(_LinkedDocKey, I) ->
        integer_to_binary(I)
    end,
    GetLinkName2 = fun(LinkedDocKey, I) ->
        list_to_binary(LinkedDocKey ++ integer_to_list(I))
    end,
    GetLinkName3 = fun(LinkedDocKey, I) ->
        list_to_atom(LinkedDocKey ++ integer_to_list(I))
    end,
    GetLinkName4 = fun(LinkedDocKey, I) ->
        list_to_binary(integer_to_list(I) ++ LinkedDocKey)
    end,
    GetLinkName5 = fun(LinkedDocKey, I) ->
        list_to_binary(integer_to_list(I) ++ LinkedDocKey ++ integer_to_list(I))
    end,
    GetLinkName6 = fun(LinkedDocKey, I) ->
        list_to_binary(LinkedDocKey ++ integer_to_list(I) ++ LinkedDocKey)
    end,
    NameFuns = [GetLinkName1, GetLinkName2, GetLinkName3, GetLinkName4, GetLinkName5,
        GetLinkName6],

    KeyFunsTuples = lists:zip(lists:seq(1,2), KeyFuns),
    NameFunsTuples = lists:zip(lists:seq(1,6), NameFuns),

    LevelString = atom_to_list(Level),
    lists:foreach(fun({KNum, GLK}) ->
        LinkedDocKey = "key_fun_" ++ integer_to_list(KNum) ++ "_level_" ++ LevelString ++ "_key_mltb_link",
        GetLinkKey = fun(I) ->
            GLK(LinkedDocKey, I)
        end,
        GetLinkName = fun(I) ->
            GetLinkName1(LinkedDocKey, I)
        end,
        many_links_test_base(Config, Level, GetLinkKey, GetLinkName)
    end, KeyFunsTuples),

    lists:foreach(fun({NNum, GLN}) ->
        LinkedDocKey = "name_fun_" ++ integer_to_list(NNum) ++ "_level_" ++ LevelString ++ "_key_mltb_link",
        GetLinkKey = fun(I) ->
            GetLinkKey1(LinkedDocKey, I)
        end,
        GetLinkName = fun(I) ->
            GLN(LinkedDocKey, I)
        end,
        many_links_test_base(Config, Level, GetLinkKey, GetLinkName)
    end, NameFunsTuples).

many_links_test_base(Config, Level, GetLinkKey, GetLinkName) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Key = list_to_binary("key_mltb_" ++ atom_to_list(Level)),
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),

    AddDocs = fun(Start, End) ->
        for(Start, End, fun(I) ->
            LinkedDoc =  #document{
                key = GetLinkKey(I),
                value = datastore_basic_ops_utils:get_record(TestRecord, I, <<"abc">>, {test, tuple})
            },
            ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc]))
        end)
    end,
    AddDocs(1, 120),
    for(1, 120, fun(I) ->
        LinkedDoc =  #document{
            key = GetLinkKey(I),
            value = datastore_basic_ops_utils:get_record(TestRecord, I, <<"abc">>, {test, tuple})
        },
        ?assertMatch(ok, ?call_store(Worker1, add_links, [
            Level, Doc, [{GetLinkName(I), LinkedDoc}]
        ]))
    end),

    CheckLinks = fun(Start, End, Sum) ->
        for(Start, End, fun(I) ->
            ?assertMatch({ok, _}, ?call_store(Worker2, fetch_link, [Level, Doc, GetLinkName(I)]))
        end),

        AccFun = fun(LinkName, _, Acc) ->
            [LinkName | Acc]
        end,
        {_, ListedLinks} = FLAns = ?call_store(Worker1, foreach_link, [Level, Doc, AccFun, []]),
        ?assertMatch({ok, _}, FLAns),

        for(Start, End, fun(I) ->
            ?assert(lists:member(GetLinkName(I), ListedLinks))
        end),

        case length(ListedLinks) of
            Sum ->
                ok;
            _ ->
                ct:print("Check params ~p", [{Start, End, Sum, ListedLinks}])
        end,
        ?assertMatch(Sum, length(ListedLinks))
    end,
    CheckLinks(1, 120, 120),

    for(1, 120, fun(I) ->
        LinkedDoc =  #document{
            key = GetLinkKey(I),
            value = datastore_basic_ops_utils:get_record(TestRecord, I, <<"abc">>, {test, tuple})
        },
        ?assertMatch(ok, ?call_store(Worker1, add_links, [
            Level, Doc, [{GetLinkName(I), LinkedDoc}]
        ]))
    end),
    CheckLinks(1, 120, 120),

    AddDocs(121, 180),
    ToAddList = fun(I) ->
        LinkedDoc =  #document{
            key = GetLinkKey(I),
            value = datastore_basic_ops_utils:get_record(TestRecord, I, <<"abc">>, {test, tuple})
        },
        {GetLinkName(I), LinkedDoc}
    end,
    AddList = lists:map(ToAddList, lists:seq(121, 180)),
    ?assertMatch(ok, ?call_store(Worker1, add_links, [
        Level, Doc, AddList
    ])),
    CheckLinks(121, 180, 180),

    ?assertMatch(ok, ?call_store(Worker1, add_links, [
        Level, Doc, AddList
    ])),
    CheckLinks(121, 180, 180),

    for(1, 180, fun(I) ->
        LinkedDoc =  #document{
            key = GetLinkKey(I),
            value = datastore_basic_ops_utils:get_record(TestRecord, I, <<"abc">>, {test, tuple})
        },
        ?assertMatch({error, already_exists}, ?call_store(Worker1, create_link, [
            Level, Doc, {GetLinkName(I), LinkedDoc}
        ]))
    end),

    for(91, 110, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, delete_links, [Level, Doc, [GetLinkName(I)]]))
    end),

    DelList = lists:map(fun(I) ->
        GetLinkName(I)
    end, lists:seq(131, 150)),
    ?assertMatch(ok, ?call_store(Worker1, delete_links, [
        Level, Doc, DelList
    ])),
    CheckLinks(1, 90, 140),
    CheckLinks(111, 130, 140),
    CheckLinks(151, 180, 140),

    AddDocs(181, 300),
    AddList2 = lists:map(ToAddList, lists:seq(181, 300)),
    ?assertMatch(ok, ?call_store(Worker1, add_links, [
        Level, Doc, AddList2
    ])),
    CheckLinks(181, 300, 260),

    DelList2 = lists:map(fun(I) ->
        GetLinkName(I)
    end, lists:seq(1, 150)),
    ?assertMatch(ok, ?call_store(Worker1, delete_links, [
        Level, Doc, DelList2
    ])),
    CheckLinks(151, 300, 150),

    ?assertMatch(ok, ?call_store(Worker1, delete_links, [
        Level, Doc, DelList2
    ])),
    CheckLinks(151, 300, 150),

    ?assertMatch(ok, ?call_store(Worker1, delete_links, [Level, Doc, []])),
    CheckLinks(151, 300, 150),

    for(161, 180, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, delete_links, [Level, Doc, [GetLinkName(I)]]))
    end),
    for(211, 300, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, delete_links, [Level, Doc, [GetLinkName(I)]]))
    end),
    CheckLinks(151, 160, 40),
    CheckLinks(181, 210, 40),

    for(1, 150, fun(I) ->
        LinkedDoc =  #document{
            key = GetLinkKey(I),
            value = datastore_basic_ops_utils:get_record(TestRecord, I, <<"abc">>, {test, tuple})
        },
        ?assertMatch(ok, ?call_store(Worker1, create_link, [
            Level, Doc, {GetLinkName(I), LinkedDoc}
        ]))
    end),
    CheckLinks(1, 150, 190),

    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    ok.

interupt_global_cache_clearing_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
    end, Workers),

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),

    Key = <<"key_icct">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),

    LinkedKey = "linked_key_icct",
    LinkedDoc = #document{
        key = LinkedKey,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2, <<"efg">>, {test, tuple2})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc])),
    ?assertMatch(ok, ?call_store(Worker1, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),

    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 10),
    Self = self(),
    spawn_link(fun() ->
        timer:sleep(1),
        Ans = ?call(Worker1, caches_controller, delete_old_keys, [globally_cached, 0]),
        Self ! {del_old_key, Ans}
    end),

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, save, [Doc])),
    ?assertMatch(ok, ?call_store(Worker1, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),

    SA = receive
        {del_old_key, SpawnedAns} ->
            SpawnedAns
    after
        timer:seconds(10) ->
            timeout
    end,
    ?assertMatch(ok, SA),

    Uuid = caches_controller:get_cache_uuid(Key, TestRecord),
    Uuid2 = caches_controller:get_cache_uuid({Key, link, cache_controller_link_key}, TestRecord),
    ?assertMatch(true, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid])),
    ?assertMatch(true, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid2])),
    ?assertMatch({ok, true}, ?call_store(Worker2, exists, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),
    ?assertMatch({ok, _}, ?call_store(Worker2, fetch_link, [?GLOBAL_ONLY_LEVEL, Doc, link])),
    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, link])),


    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 10),
    ?assertMatch(ok, ?call(Worker1, caches_controller, delete_old_keys, [globally_cached, 0])),
    timer:sleep(timer:seconds(5)), % for asynch controll data clearing

    ?assertMatch(false, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid])),
    ?assertMatch(false, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid2])),
    ?assertMatch({ok, false}, ?call_store(Worker2, exists, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),
    ?assertMatch({error,link_not_found}, ?call_store(Worker2, fetch_link, [?GLOBAL_ONLY_LEVEL, Doc, link])),
    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, link])),

    ok.

operations_sequence_global_cache_test(Config) ->
    [Worker1, _Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
    end, Workers),

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
    [Worker1, _Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
    end, Workers),

    Key = <<"key_lost">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),

    LinkedKey = "linked_key_lost",
    LinkedDoc = #document{
        key = LinkedKey,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2, <<"efg">>, {test, tuple2})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc])),

    ?assertMatch(ok, ?call_store(Worker1, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),
    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    ?assertMatch(ok, ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [link]])),
    timer:sleep(1000), % wait to check if any asyc opeartion hasn't changed information in cache
    ?assertMatch({error,link_not_found}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),

    ?assertMatch(ok, ?call_store(Worker1, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),
    ?assertMatch(ok, ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [link]])),
    ?assertMatch({error,link_not_found}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    ?assertMatch(ok, ?call_store(Worker1, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),
    timer:sleep(1000), % wait to check if any asyc opeartion hasn't changed information in cache
    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),

    ok.

multilevel_foreach_global_cache_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
    end, Workers),

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    Key = "key_mlft",
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, PModule, create, [ModelConfig, Doc])),

    LinkedKey = "linked_key_mlft",
    LinkedDoc = #document{
        key = LinkedKey,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2, <<"efg">>, {test, tuple2})
    },
    ?assertMatch({ok, _}, ?call(Worker1, PModule, create, [ModelConfig, LinkedDoc])),

    LinkedKey2 = "linked_key_mlft2",
    LinkedDoc2 = #document{
        key = LinkedKey2,
        value = datastore_basic_ops_utils:get_record(TestRecord, 3, <<"hij">>, {test, tuple3})
    },
    ?assertMatch({ok, _}, ?call(Worker1, PModule, create, [ModelConfig, LinkedDoc2])),

    LinkedKey3 = "linked_key_mlft3",
    LinkedDoc3 = #document{
        key = LinkedKey3,
        value = datastore_basic_ops_utils:get_record(TestRecord, 4, <<"klm">>, {test, tuple4})
    },
    ?assertMatch({ok, _}, ?call(Worker1, PModule, create, [ModelConfig, LinkedDoc3])),

    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc,
        [{link, LinkedDoc}, {link2, LinkedDoc2}, {link3, LinkedDoc3}]])),

    ?assertMatch(ok, ?call(Worker1, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, all])),
    ?assertMatch(ok, ?call(Worker1, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, link])),
    ?assertMatch(ok, ?call(Worker1, PModule, delete_links, [ModelConfig, Key, [link3]])),
    ?assertMatch(ok, ?call_store(Worker2, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [link2]])),

    AccFun = fun(LinkName, _, Acc) ->
        [LinkName | Acc]
    end,

    {_, Listed} = FLAns = ?call_store(Worker2, foreach_link, [?GLOBALLY_CACHED_LEVEL, Doc, AccFun, []]),
    ?assertMatch({ok, _}, FLAns),
    ?assert(lists:member(link, Listed)),
    ?assert(lists:member(link3, Listed)),
    ?assertMatch(2, length(Listed)),
    ok.

clear_and_flush_global_cache_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
    end, Workers),

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    CModule = ?call_store(Worker1, driver_to_module, [?DISTRIBUTED_CACHE_DRIVER]),

    Key = <<"key_caft">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    Doc_v2 =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 100, <<"abc">>, {test, tuple})
    },

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    ?assertMatch({ok, false}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),
    ?assertMatch(ok, ?call(Worker2, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),

    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),
    ?assertMatch(ok, ?call(Worker1, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, false}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    ?assertMatch(ok, ?call(Worker2, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),
    ?assertMatch(ok, ?call(Worker1, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, false}, ?call(Worker2, CModule, exists, [ModelConfig, Key])),
    timer:sleep(timer:seconds(6)), % wait to check if any async action hasn't destroyed value at disk
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, get, [Key])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, save, [Doc_v2])),
    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, false}, ?call(Worker2, CModule, exists, [ModelConfig, Key])),
    timer:sleep(timer:seconds(6)), % wait to check if any async action hasn't destroyed value at disk

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, get, [Key])),
    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch(true, ?call(Worker1, TestRecord, exists, [Key])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, get, [Key])),

    CheckField1 = fun() ->
        {_, GetDoc} = GetAns = ?call(Worker2, PModule, get, [ModelConfig, Key]),
        ?assertMatch({ok, _}, GetAns),
        GetValue = GetDoc#document.value,
        GetValue#globally_cached_record.field1
    end,
    ?assertMatch(1, CheckField1()),

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, get, [Key])),
    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, save, [Doc_v2])),
    ?assertMatch(100, CheckField1(), 6),



    LinkedKey = <<"linked_key_caft">>,
    LinkedDoc = #document{
        key = LinkedKey,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2, <<"efg">>, {test, tuple2})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc])),
    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),
    ?assertMatch({error,link_not_found}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link])),
    ?assertMatch(ok, ?call(Worker1, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, all])),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link])),

    ?assertMatch(ok, ?call_store(Worker2, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [link]])),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link])),
    ?assertMatch(ok, ?call(Worker1, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, link])),
    ?assertMatch({error,link_not_found}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link])),

    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),
    ?assertMatch(ok, ?call(Worker1, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, link])),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link])),
    ?assertMatch(ok, ?call(Worker1, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, all])),
    ?assertMatch({error,link_not_found}, ?call(Worker1, CModule, fetch_link, [ModelConfig, Key, link])),
    timer:sleep(timer:seconds(6)), % wait to check if any async action hasn't destroyed value at disk
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link])),

    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    ?assertMatch(ok, ?call(Worker1, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, all])),
    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),

    ok.

create_after_delete_global_cache_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
    end, Workers),

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
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
    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),
    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    ?assertMatch({ok, _}, ?call(Worker2, TestRecord, create, [Doc])),

    ?assertMatch({error,link_not_found}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link]), 1),
    ?assertMatch({error,link_not_found}, ?call_store(Worker1, fetch_link, [?GLOBAL_ONLY_LEVEL, Doc, link])),
    ?assertMatch({error,link_not_found}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link])),

    Uuid = caches_controller:get_cache_uuid(Key, TestRecord),
    ?assertMatch(true, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid]), 1),
    ?assertMatch({ok, false}, ?call(Worker1, PModule, exists, [ModelConfig, Key])),
    ?assertMatch({ok, true}, ?call(Worker1, PModule, exists, [ModelConfig, Key]), 6),

    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),
    ?assertMatch(ok, ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [link]])),
    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),

    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    LinkCacheUuid = caches_controller:get_cache_uuid({Key, link, cache_controller_link_key}, TestRecord),
    ?assertMatch(true, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]), 1),
    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBAL_ONLY_LEVEL, Doc, link])),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]), 6),

    ok.

multiple_links_creation_disk_test(Config) ->
    [Worker1, _Worker2] = ?config(cluster_worker_nodes, Config),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    multiple_links_creation_test_base(Config, PModule).

multiple_links_creation_global_only_test(Config) ->
    [Worker1, _Worker2] = ?config(cluster_worker_nodes, Config),
    PModule = ?call_store(Worker1, driver_to_module, [?DISTRIBUTED_CACHE_DRIVER]),
    multiple_links_creation_test_base(Config, PModule).

multiple_links_creation_test_base(Config, PModule) ->
    [Worker1, _Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
    end, Workers),

    ModelConfig = TestRecord:model_init(),
    Key = get_key("key_mlct_", PModule),
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, PModule, create, [ModelConfig, Doc])),

    LinkedKey = get_key("linked_key_mlct_", PModule),
    LinkedDoc = #document{
        key = LinkedKey,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2, <<"efg">>, {test, tuple2})
    },
    ?assertMatch({ok, _}, ?call(Worker1, PModule, create, [ModelConfig, LinkedDoc])),

    LinkedKey2 = get_key("linked_key_mlct2_", PModule),
    LinkedDoc2 = #document{
        key = LinkedKey2,
        value = datastore_basic_ops_utils:get_record(TestRecord, 3, <<"hij">>, {test, tuple3})
    },
    ?assertMatch({ok, _}, ?call(Worker1, PModule, create, [ModelConfig, LinkedDoc2])),

    LinkedKey3 = get_key("linked_key_mlct3_", PModule),
    LinkedDoc3 = #document{
        key = LinkedKey3,
        value = datastore_basic_ops_utils:get_record(TestRecord, 4, <<"klm">>, {test, tuple4})
    },
    ?assertMatch({ok, _}, ?call(Worker1, PModule, create, [ModelConfig, LinkedDoc3])),

    Self = self(),
    AddLinks = fun(Links, Send) ->
        Ans = ?call(Worker1, PModule, add_links, [ModelConfig, Doc#document.key,
            datastore:normalize_link_target(ModelConfig, Links)]),
        case Send of
            true ->
                Self ! {link_ans, Ans};
            _ ->
                Ans
        end
    end,

    spawn_link(fun() -> AddLinks([{link, LinkedDoc}], true) end),
    spawn_link(fun() -> AddLinks([{link2, LinkedDoc2}], true) end),
    spawn_link(fun() -> AddLinks([{link3, LinkedDoc3}], true) end),

    rec_add_link_ans(),
    rec_add_link_ans(),
    rec_add_link_ans(),

    F1 = ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]),
    F2 = ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link2]),
    F3 = ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link3]),

    ?assertMatch({{ok, _}, {ok, _}, {ok, _}}, {F1, F2, F3}),

    DeleteLinks = fun(Links, Send) ->
        Ans = ?call(Worker1, PModule, delete_links, [ModelConfig, Doc#document.key, Links]),
        case Send of
            true ->
                Self ! {link_ans, Ans};
            _ ->
                Ans
        end
    end,

    spawn_link(fun() -> DeleteLinks([link], true) end),
    spawn_link(fun() -> DeleteLinks([link2], true) end),
    spawn_link(fun() -> DeleteLinks([link3], true) end),

    rec_add_link_ans(),
    rec_add_link_ans(),
    rec_add_link_ans(),

    F4 = ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]),
    F5 = ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link2]),
    F6 = ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link3]),

    ?assertMatch({{error,link_not_found}, {error,link_not_found}, {error,link_not_found}},
        {F4, F5, F6}),

    ?assertMatch(ok, AddLinks([{link, LinkedDoc}, {link2, LinkedDoc2}, {link3, LinkedDoc3}], false)),
    F7 = ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]),
    F8 = ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link2]),
    F9 = ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link3]),

    ?assertMatch({{ok, _}, {ok, _}, {ok, _}}, {F7, F8, F9}),

    ?assertMatch(ok, DeleteLinks(all, false)),
    F10 = ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]),
    F11 = ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link2]),
    F12 = ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link3]),

    ?assertMatch({{error,link_not_found}, {error,link_not_found}, {error,link_not_found}},
        {F10, F11, F12}),

    ok.

rec_add_link_ans() ->
    receive
        {link_ans, Ans} ->
            ?assertEqual(ok, Ans)
    after
        1000 ->
            timeout
    end.

get_key(Base, PModule) ->
    list_to_binary(Base ++ atom_to_list(PModule)).

link_monitoring_global_cache_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
    end, Workers),

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    Key = <<"key_lmt">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),

    LinkedKey = <<"linked_key_lmt">>,
    LinkedDoc = #document{
        key = LinkedKey,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2, <<"efg">>, {test, tuple2})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc])),

    LinkedKey2 = <<"linked_key_lmt2">>,
    LinkedDoc2 = #document{
        key = LinkedKey2,
        value = datastore_basic_ops_utils:get_record(TestRecord, 3, <<"hij">>, {test, tuple3})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc2])),

    LinkCacheUuid = caches_controller:get_cache_uuid({Key, link, cache_controller_link_key}, TestRecord),
    LinkCacheUuid2 = caches_controller:get_cache_uuid({Key, link2, cache_controller_link_key}, TestRecord),
    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),
    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    ?assertMatch(true, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]), 1),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]), 6),

    ?assertMatch(ok, ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [link]])),
    ?assertMatch({error,link_not_found}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    {ok, CC} = ?call(Worker1, cache_controller, get, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]),
    CCD = CC#document.value,
    ?assert((CCD#cache_controller.action =:= to_be_del) orelse (CCD#cache_controller.action =:= delete_links)),
    ?assertMatch({error,link_not_found}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]), 6),
    ?assertMatch(false, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]), 1),

    ?assertMatch(ok, ?call_store(Worker2, add_links,
        [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}, {link2, LinkedDoc2}]])),
    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link2])),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link2]), 6),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]), 1),

    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    ?assertMatch({error,link_not_found}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link]), 1),
    {ok, CC2} = ?call(Worker1, cache_controller, get, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]),
    CCD2 = CC2#document.value,
    ?assertEqual(delete_links, CCD2#cache_controller.action),

    ?assertMatch({error,link_not_found}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link2]), 1),
    {ok, CC3} = ?call(Worker1, cache_controller, get, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid2]),
    CCD3 = CC3#document.value,
    ?assertEqual(delete_links, CCD3#cache_controller.action),

    ?assertMatch({error,link_not_found}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]), 6),
    ?assertMatch({error,link_not_found}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link2]), 1),
    ?assertMatch(false, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]), 1),
    ?assertMatch(false, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid2]), 1),
    ok.


prevent_reading_from_disk_global_cache_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
    end, Workers),

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    Key = <<"key_prfdt">>,
    CAns = ?call(Worker1, TestRecord, create, [
        #document{
            key = Key,
            value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
        }]),
    ?assertMatch({ok, _}, CAns),
    ?assertMatch({ok, true}, ?call(Worker1, PModule, exists, [ModelConfig, Key]), 6),

    UpdateFun = #{field1 => 2},
    ?assertMatch({ok, _}, ?call(Worker2, TestRecord, update, [Key, UpdateFun])),
    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    ?assertMatch({error, {not_found, _}}, ?call(Worker1, TestRecord, get, [Key])),
    ?assertMatch(false, ?call(Worker1, TestRecord, exists, [Key])),
    ?assertMatch({ok, false}, ?call(Worker1, PModule, exists, [ModelConfig, Key]), 6),
    ok.


restoring_cache_from_disk_global_cache_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
    end, Workers),

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    CModule = ?call_store(Worker1, driver_to_module, [?DISTRIBUTED_CACHE_DRIVER]),
    Key = <<"key_rcfdr">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),

    LinkedKey = <<"linked_key_rcfdr">>,
    LinkedDoc = #document{
        key = LinkedKey,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2, <<"efg">>, {test, tuple2})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc])),
    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),

    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 6),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, LinkedKey]), 1),
    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, link]), 1),

    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, get, [Key])),
    ?assertMatch({ok, true}, ?call(Worker2, CModule, exists, [ModelConfig, Key]), 1),

    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, update, [Key, #{field1 => 2}])),
    ?assertMatch({ok, true}, ?call(Worker2, CModule, exists, [ModelConfig, Key])),

    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, link])),
    ?assertMatch({ok, _}, ?call_store(Worker2, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    ?assertMatch({ok, _}, ?call(Worker2, CModule, fetch_link, [ModelConfig, Key, link]), 1),
    ok.


% checks if cache is monitored
monitoring_global_cache_test_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
    end, Workers),

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    Key = <<"key_cmt">>,
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [
        #document{
            key = Key,
            value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
        }])),

    Uuid = caches_controller:get_cache_uuid(Key, TestRecord),
    ?assertMatch(true, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid]), 1),
    ?assertMatch(true, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid]), 1),

    ?assertMatch({ok, false}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 6),

    % Check dump delay
    Key2 = <<"key_cmt2">>,
    for(2, fun() ->
        ?assertMatch({ok, _}, ?call(Worker1, TestRecord, save, [#document{
            key = Key2,
            value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
        }])),
        timer:sleep(3500) % do not change to active waiting
    end),
    ?assertMatch({ok, false}, ?call(Worker2, PModule, exists, [ModelConfig, Key2])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key2]), 6),

    % Check forced dump
    Key3 = <<"key_cmt3">>,
    for(4, fun() ->
        ?assertMatch({ok, _}, ?call(Worker1, TestRecord, save, [#document{
            key = Key3,
            value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
        }])),
        timer:sleep(3500) % do not change to active waiting
    end),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key3])),

    ok.

% checks if caches controller clears caches
old_keys_cleaning_global_cache_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control(Workers), % Automatic cleaning may influence results

    Times = [timer:hours(7 * 24), timer:hours(24), timer:hours(1), timer:minutes(10), 0],
    {_, KeysWithTimes} = lists:foldl(fun(T, {Num, Ans}) ->
        K = list_to_binary("old_keys_cleaning_global_cache_test" ++ integer_to_list(Num)),
        {Num + 1, [{K, T} | Ans]}
    end, {1, []}, Times),

    lists:foreach(fun({K, _T}) ->
        ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [
            #document{
                key = K,
                value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
            }]))
    end, KeysWithTimes),
    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 10),
    check_clearing(TestRecord, lists:reverse(KeysWithTimes), Worker1, Worker2),

    CorruptedKey = list_to_binary("old_keys_cleaning_global_cache_test_c"),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [
        #document{
            key = CorruptedKey,
            value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
        }])),
    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 10),
    CorruptedUuid = caches_controller:get_cache_uuid(CorruptedKey, TestRecord),
    ModelConfig = TestRecord:model_init(),
    DModule = ?call(Worker1, datastore, driver_to_module, [?DISTRIBUTED_CACHE_DRIVER]),
    PModule = ?call(Worker1, datastore, driver_to_module, [?PERSISTENCE_DRIVER]),
    ?assertMatch(ok, ?call(Worker2, cache_controller, delete, [?GLOBAL_ONLY_LEVEL, CorruptedUuid])),

    ?assertMatch(ok, ?call(Worker1, caches_controller, delete_old_keys, [globally_cached, 1])),
    ?assertMatch({ok, true}, ?call(Worker2, DModule, exists, [ModelConfig, CorruptedKey])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, CorruptedKey])),

    ?assertMatch(ok, ?call(Worker1, caches_controller, delete_old_keys, [globally_cached, 0])),
    ?assertMatch({ok, false}, ?call(Worker2, DModule, exists, [ModelConfig, CorruptedKey])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, CorruptedKey])),
    ok.

% helper fun used by old_keys_cleaning_global_cache_test
check_clearing(_TestRecord, [], _Worker1, _Worker2) ->
    ok;
check_clearing(TestRecord, [{K, TimeWindow} | R] = KeysWithTimes, Worker1, Worker2) ->
    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),

    lists:foreach(fun({K2, T}) ->
        Uuid = caches_controller:get_cache_uuid(K2, TestRecord),
        UpdateFun = fun(Record) ->
            {ok, Record#cache_controller{
                timestamp = to_timestamp(from_timestamp(os:system_time(?CC_TIMEUNIT)) - T - timer:minutes(5))
            }}
        end,
        ?assertMatch({ok, _}, ?call(Worker1, cache_controller, update, [?GLOBAL_ONLY_LEVEL, Uuid, UpdateFun]), 10)
    end, KeysWithTimes),

    ?assertMatch(ok, ?call(Worker1, caches_controller, delete_old_keys, [globally_cached, TimeWindow])),
    timer:sleep(timer:seconds(5)), % for asynch controll data clearing

    Uuid = caches_controller:get_cache_uuid(K, TestRecord),
    ?assertMatch(false, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid])),
    ?assertMatch({ok, false}, ?call_store(Worker2, exists, [?GLOBAL_ONLY_LEVEL, TestRecord, K])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, K])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, get, [K])),
    ?assertMatch(true, ?call(Worker2, TestRecord, exists, [K])),

    lists:foreach(fun({K2, _}) ->
        Uuid2 = caches_controller:get_cache_uuid(K2, TestRecord),
        ?assertMatch(true, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid2])),
        ?assertMatch({ok, true}, ?call_store(Worker2, exists, [?GLOBAL_ONLY_LEVEL, TestRecord, K2])),
        ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, K2]))
    end, R),

    check_clearing(TestRecord, R, Worker1, Worker2).

% checks if node_managaer clears memory correctly
clearing_global_cache_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control(Workers), % Automatic cleaning may influence results

    [{_, Mem0}] = monitoring:get_memory_stats(),
    Mem0Node = node_mem(Worker2),
    Mem0Ets = ?call(Worker2, erlang, memory, [ets]),
    ct:print("Mem0 ~p, ~p, ~p", [Mem0, Mem0Node, Mem0Ets]),
    FreeMem = 100 - Mem0,
    ToAdd = min(10, FreeMem / 2),
    MemCheck1 = Mem0 + ToAdd / 2,
    MemUsage = Mem0 + ToAdd,

    ?assertMatch(ok, test_utils:set_env(Worker2, ?CLUSTER_WORKER_APP_NAME, node_mem_ratio_to_clear_cache, 0)),
    % at least 100MB will be added
    ?assertMatch(ok, test_utils:set_env(Worker2, ?CLUSTER_WORKER_APP_NAME, erlang_mem_to_clear_cache_mb,
        Mem0Node / 1024 / 1024 + 50)),

    ?assertMatch(ok, ?call(Worker1, ?MODULE, utilize_memory, [TestRecord, MemUsage], timer:minutes(10))),
    [{_, Mem1}] = monitoring:get_memory_stats(),
    Mem1Node = node_mem(Worker2),
    Mem1Ets = ?call(Worker2, erlang, memory, [ets]),
    ct:print("Mem1 ~p, ~p, ~p", [Mem1, Mem1Node, Mem1Ets]),
    ?assert(Mem1 > MemCheck1),
    ?assert(Mem1Node > 50 * 1024 * 1024),

    ?assertEqual(ok, ?call(Worker2, caches_controller, wait_for_cache_dump, []), 150),
    ?assertMatch(ok, gen_server:call({?NODE_MANAGER_NAME, Worker2}, check_mem_synch, ?TIMEOUT)),
    [{_, Mem2}] = monitoring:get_memory_stats(),
    Mem2Node = node_mem(Worker2),
    Mem2Ets = ?call(Worker2, erlang, memory, [ets]),
    ct:print("Mem2 ~p, ~p, ~p", [Mem2, Mem2Node, Mem2Ets]),
    % TODO Change add node memory checking when DB nodes will be run at separate machine
    ?assertMatch(true, Mem2Node < (Mem0Node + Mem1Node) / 2),
%%     ?assert(Mem2 < MemTarget),

    % clean
    GetAllKeys = fun
                     ('$end_of_table', Acc) ->
                         {abort, Acc};
                     (#document{key = Uuid}, Acc) ->
                         {next, [Uuid | Acc]}
                 end,
    {_, AllKeys} = AllKeysAns = ?call_store(Worker1, list, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetAllKeys, []]),
    ?assertMatch({ok, _}, AllKeysAns),
    lists:foreach(fun(Uuid) ->
        ?assertEqual(ok, ?call_store(Worker1, delete, [?GLOBALLY_CACHED_LEVEL, TestRecord, Uuid]))
    end, AllKeys),

    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 100),
    ?assertMatch(ok, ?call(Worker1, caches_controller, delete_old_keys, [globally_cached, 0])),

    ?assertMatch({ok, []}, ?call_store(Worker1, list, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetAllKeys, []])),
    ?assertMatch({ok, []}, ?call_store(Worker1, list, [?GLOBAL_ONLY_LEVEL, TestRecord, GetAllKeys, []])),
    timer:sleep(timer:seconds(15)), % give couch time to process deletes
    ?assertMatch({ok, []}, ?call_store(Worker2, list, [?DISK_ONLY_LEVEL, TestRecord, GetAllKeys, []])),

    ok.

node_mem(Worker) ->
    ?call(Worker, erlang, memory, [ets]) + ?call(Worker, erlang, memory, [system]).

% helper fun used by clearing_global_cache_test
utilize_memory(TestRecord, MemUsage) ->
    OneDoc = list_to_binary(prepare_list(256 * 1024)),

    Add100MB = fun(_KeyBeg) ->
        for(1, 100 * 4, fun(I) ->
            TestRecord:create(#document{value =
                datastore_basic_ops_utils:get_record(TestRecord, I, binary:copy(OneDoc), {test, tuple})
            })
        end)
    end,
    Guard = fun() ->
        [{_, Current}] = monitoring:get_memory_stats(),
        Current >= MemUsage
    end,
    while(Add100MB, Guard),
    ok.

%% Simple usage of get/update/create/exists/delete on local cache driver (on several nodes)
local_test(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Level = local_only,

    local_access_only(TestRecord, Worker1, Level),
    local_access_only(TestRecord, Worker2, Level),

    ?assertMatch({ok, _},
        ?call_store(Worker1, create, [Level,
            #document{
                key = some_other_key,
                value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
            }])),

    ?assertMatch({ok, false},
        ?call_store(Worker2, exists, [Level,
            TestRecord, some_other_key])),

    ?assertMatch({ok, true},
        ?call_store(Worker1, exists, [Level,
            TestRecord, some_other_key])),

    ok.


%% Simple usage of get/update/create/exists/delete on global cache driver (on several nodes)
global_test(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Level = ?GLOBAL_ONLY_LEVEL,

    local_access_only(TestRecord, Worker1, Level),
    local_access_only(TestRecord, Worker2, Level),

    global_access(Config, Level),

    ok.


%% Simple usage of get/update/create/exists/delete on persistamce driver (on several nodes)
persistance_test(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Level = ?DISK_ONLY_LEVEL,

    local_access_only(TestRecord, Worker1, Level),
    local_access_only(TestRecord, Worker2, Level),

    global_access(Config, Level),

    ok.


%% Atomic update on globally cached level (on several nodes)
globally_cached_atomic_update_test(Config) ->
    Level = ?GLOBALLY_CACHED_LEVEL,

    Pid = self(),
    UpdateFun1 = fun(#globally_cached_record{field1 = 0} = Record) ->
        {ok, Record#globally_cached_record{field2 = Pid}}
                 end,

    UpdateFun2 = fun(#globally_cached_record{field1 = Value} = Record) ->
        {ok, Record#globally_cached_record{field1 = Value + 1}}
    end,

    atomic_update_test_base(Config, Level, UpdateFun1, UpdateFun2).

%% Atomic update on global cache driver (on several nodes)
global_atomic_update_test(Config) ->
    Level = ?GLOBAL_ONLY_LEVEL,

    Pid = self(),
    UpdateFun1 = fun(#global_only_record{field1 = 0} = Record) ->
        {ok, Record#global_only_record{field2 = Pid}}
    end,

    UpdateFun2 = fun(#global_only_record{field1 = Value} = Record) ->
        {ok, Record#global_only_record{field1 = Value + 1}}
    end,

    atomic_update_test_base(Config, Level, UpdateFun1, UpdateFun2).

atomic_update_test_base(Config, Level, UpdateFun1, UpdateFun2) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Key = rand_key(),

    ?assertMatch({ok, _},
        ?call_store(Worker1, create, [Level,
            #document{
                key = Key,
                value = datastore_basic_ops_utils:get_record(TestRecord, 0, <<"abc">>, {test, tuple})
            }])),

    Pid = self(),
    AggregatedAns = ?call(Worker2, ?MODULE, update_and_check, [Level, TestRecord, Key, UpdateFun1]),
    ?assertMatch({{ok, Key}, {ok, #document{value = ?test_record_f1_2(0, Pid)}}}, AggregatedAns),

    ?assertMatch({ok, #document{value = ?test_record_f1_2(0, Pid)}},
        ?call_store(Worker1, get, [Level, TestRecord, Key])),

    Self = self(),
    Timeout = timer:seconds(30),
    utils:pforeach(fun(Node) ->
        ?call_store(Node, update, [Level, TestRecord, Key, UpdateFun2]),
        Self ! done
    end, lists:duplicate(100, Worker1) ++ lists:duplicate(100, Worker2)),
    [receive done -> ok after Timeout -> ok end || _ <- lists:seq(1, 200)],

    ?assertMatch({ok, #document{value = ?test_record_f1(200)}},
        ?call_store(Worker1, get, [Level,
            TestRecord, Key])),

    ok.

update_and_check(Level, TestRecord, Key, UpdateFun) ->
    UpdateAns = datastore:update(Level, TestRecord, Key, UpdateFun),
    GetAns = datastore:get(Level, TestRecord, Key),
    {UpdateAns, GetAns}.

%% list operation on global cache driver (on several nodes)
global_list_test(Config) ->
    TestRecord = ?config(test_record, Config),
    generic_list_test(TestRecord, ?config(cluster_worker_nodes, Config), ?GLOBAL_ONLY_LEVEL).

%% list operation on disk only driver (on several nodes)
disk_list_test(Config) ->
    generic_list_test(disk_only_record, ?config(cluster_worker_nodes, Config), ?DISK_ONLY_LEVEL).

%% list operation on disk only driver (on several nodes)
globally_cached_list_test(Config) ->
    [Node1 | _] = Nodes = ?config(cluster_worker_nodes, Config),
    generic_list_test(globally_cached_record, Nodes, ?GLOBALLY_CACHED_LEVEL,
        0, non, timer:seconds(10)),
    generic_list_test(globally_cached_record, Nodes, ?GLOBALLY_CACHED_LEVEL,
        timer:seconds(10), non, timer:seconds(10)),

    TestRecord = ?config(test_record, Config),
    PModule = ?call_store(Node1, driver_to_module, [?PERSISTENCE_DRIVER]),
    ModelConfig = TestRecord:model_init(),

    BeforeRetryFun = fun(Keys) ->
        lists:map(fun(Key) ->
            ?assertMatch(ok, ?call(Node1, PModule, delete, [ModelConfig, Key, ?PRED_ALWAYS]))
        end, Keys)
    end,
    generic_list_test(globally_cached_record, Nodes, ?GLOBALLY_CACHED_LEVEL,
        timer:seconds(10), BeforeRetryFun, timer:seconds(10)),

    BeforeRetryFun2 = fun(Docs) ->
        lists:map(fun(Key) ->
            ?assertMatch(ok, ?call(Node1, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key]))
        end, Docs)
    end,
    generic_list_test(globally_cached_record, Nodes, ?GLOBALLY_CACHED_LEVEL,
        timer:seconds(10), BeforeRetryFun2, timer:seconds(10)).


%% list operation on local cache driver (on several nodes)
local_list_test(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    generic_list_test(TestRecord, [Worker1], ?LOCAL_ONLY_LEVEL),
    generic_list_test(TestRecord, [Worker2], ?LOCAL_ONLY_LEVEL),
    ok.

%% Simple usege of link_walk
link_walk_test(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Level = ?DISK_ONLY_LEVEL,
    Key1 = rand_key(),
    Key2 = rand_key(),
    Key3 = rand_key(),

    Doc1 = #document{
        key = Key1,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1)
    },

    Doc2 = #document{
        key = Key2,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2)
    },

    Doc3 = #document{
        key = Key3,
        value = datastore_basic_ops_utils:get_record(TestRecord, 3)
    },

    %% Create some documents and links
    ?assertMatch({ok, _},
        ?call(Worker1, TestRecord, create, [Doc1])),

    ?assertMatch({ok, _},
        ?call(Worker1, TestRecord, create, [Doc2])),

    ?assertMatch({ok, _},
        ?call(Worker1, TestRecord, create, [Doc3])),

    ?assertMatch(ok, ?call_store(Worker2, add_links, [Level, Doc1, [{some, Doc2}, {other, Doc1}]])),
    ?assertMatch(ok, ?call_store(Worker1, add_links, [Level, Doc2, [{link, Doc3}, {parent, Doc1}]])),

    Res0 = ?call_store(Worker1, link_walk, [Level, Doc1, [some, link], get_leaf]),
    Res1 = ?call_store(Worker2, link_walk, [Level, Doc1, [some, parent], get_leaf]),

    ?assertMatch({ok, {#document{key = Key3, value = ?test_record_f1(3)}, [Key2, Key3]}}, Res0),
    ?assertMatch({ok, {#document{key = Key1, value = ?test_record_f1(1)}, [Key2, Key1]}}, Res1),

    ok.


%% Simple usege of (add/fetch/delete/foreach)_link
disk_only_links_test(Config) ->
    generic_links_test(Config, ?DISK_ONLY_LEVEL).


%% Simple usege of (add/fetch/delete/foreach)_link
global_only_links_test(Config) ->
    generic_links_test(Config, ?GLOBAL_ONLY_LEVEL).


%% Simple usege of (add/fetch/delete/foreach)_link
globally_cached_links_test(Config) ->
    generic_links_test(Config, ?GLOBALLY_CACHED_LEVEL).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    NewConfig = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [random]),
    NewConfig.

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(links_scope_test, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:enable_datastore_models(Workers, [link_scopes_test_record]),
    Config;
init_per_testcase(links_scope_proc_mem_test, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:enable_datastore_models(Workers, [link_scopes_test_record2]),
    Config;
init_per_testcase(Case, Config) ->
    datastore_basic_ops_utils:set_env(Case, Config).

end_per_testcase(_Case, Config) ->
    datastore_basic_ops_utils:clear_env(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec local_access_only(TestRecord :: atom(), Node :: atom(), Level :: datastore:store_level()) -> ok.
local_access_only(TestRecord, Node, Level) ->
    Key = rand_key(),

    ?assertMatch({ok, Key}, ?call_store(Node, create, [
        Level, #document{key = Key, value =
            datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
        }
    ])),

    ?assertMatch({error, already_exists}, ?call_store(Node, create, [
        Level, #document{key = Key, value =
            datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
        }
    ])),

    ?assertMatch({ok, true}, ?call_store(Node, exists, [Level, TestRecord, Key])),

    ?assertMatch({ok, #document{value = ?test_record_f3({test, tuple})}},
        ?call_store(Node, get, [Level, TestRecord, Key])),

    Pid = self(),
    ?assertMatch({ok, Key}, ?call_store(Node, update, [
        Level, TestRecord, Key, #{field2 => Pid}
    ])),

    ?assertMatch({ok, #document{value = ?test_record_f2(Pid)}},
        ?call_store(Node, get, [Level, TestRecord, Key])),

    ?assertMatch(ok, ?call_store(Node, delete, [
        Level, TestRecord, Key, fun() -> false end
    ])),

    ?assertMatch({ok, #document{value = ?test_record_f2(Pid)}},
        ?call_store(Node, get, [Level, TestRecord, Key])),

    ?assertMatch(ok, ?call_store(Node, delete, [
        Level, TestRecord, Key
    ])),

    ?assertMatch({error, {not_found, _}}, ?call_store(Node, get, [
        Level, TestRecord, Key
    ])),

    ?assertMatch({error, {not_found, _}}, ?call_store(Node, update, [
        Level, TestRecord, Key, #{field2 => self()}
    ])),

    ok.


-spec global_access(Config :: term(), Level :: datastore:store_level()) -> ok.
global_access(Config, Level) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Key = rand_key(),

    ?assertMatch({ok, _}, ?call_store(Worker1, create, [Level, #document{
        key = Key, value =
            datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    }])),

    ?assertMatch({ok, true}, ?call_store(Worker2, exists, [
        Level, TestRecord, Key
    ])),

    ?assertMatch({ok, true}, ?call_store(Worker1, exists, [
        Level, TestRecord, Key
    ])),

    ?assertMatch({error, already_exists}, ?call_store(Worker2, create, [
        Level, #document{key = Key, value =
            datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
        }
    ])),

    ?assertMatch({ok, #document{value = ?test_record_f1_3(1, {test, tuple})}},
        ?call_store(Worker1, get, [Level, TestRecord, Key])),

    ?assertMatch({ok, #document{value = ?test_record_f1_3(1, {test, tuple})}},
        ?call_store(Worker2, get, [Level, TestRecord, Key])),

    ?assertMatch({ok, _}, ?call_store(Worker1, update, [
        Level, TestRecord, Key, #{field1 => 2}
    ])),

    ?assertMatch({ok, #document{value = ?test_record_f1(2)}},
        ?call_store(Worker2, get, [Level,
            TestRecord, Key])),

    ok.


generic_links_test(Config, Level) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Key1 = rand_key(),
    Key2 = rand_key(),
    Key3 = rand_key(),

    Doc1 = #document{
        key = Key1,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1)
    },

    Doc2 = #document{
        key = Key2,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2)
    },

    Doc3 = #document{
        key = Key3,
        value = datastore_basic_ops_utils:get_record(TestRecord, 3)
    },

    %% Create some documents and links
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc1])),

    ?assertMatch({ok, _}, ?call(Worker2, TestRecord, create, [Doc2])),

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc3])),

    ?assertMatch(ok, ?call_store(Worker2, add_links, [
        Level, Doc1, [{link3, Doc2}, {link2, Doc3}]
    ])),

    %% Fetch all links and theirs targets
    Ret0 = ?call_store(Worker2, fetch_link_target, [Level, Doc1, link2]),
    Ret1 = ?call_store(Worker1, fetch_link_target, [Level, Doc1, link3]),
    Ret2 = ?call_store(Worker2, fetch_link, [Level, Doc1, link2]),
    Ret3 = ?call_store(Worker1, fetch_link, [Level, Doc1, link3]),

    ?assertMatch({ok, {Key3, TestRecord}}, Ret2),
    ?assertMatch({ok, {Key2, TestRecord}}, Ret3),
    ?assertMatch({ok, #document{key = Key3, value = ?test_record_f1(3)}}, Ret0),
    ?assertMatch({ok, #document{key = Key2, value = ?test_record_f1(2)}}, Ret1),

    ?assertMatch(ok, ?call_store(Worker2, set_links, [
        Level, Doc1, [{link2, Doc2}, {link3, Doc3}]
    ])),

    %% Fetch all links and theirs targets
    Ret00 = ?call_store(Worker2, fetch_link_target, [Level, Doc1, link2]),
    Ret01 = ?call_store(Worker1, fetch_link_target, [Level, Doc1, link3]),
    Ret02 = ?call_store(Worker2, fetch_link, [Level, Doc1, link2]),
    Ret03 = ?call_store(Worker1, fetch_link, [Level, Doc1, link3]),

    ?assertMatch({ok, {Key2, TestRecord}}, Ret02),
    ?assertMatch({ok, {Key3, TestRecord}}, Ret03),
    ?assertMatch({ok, #document{key = Key2, value = ?test_record_f1(2)}}, Ret00),
    ?assertMatch({ok, #document{key = Key3, value = ?test_record_f1(3)}}, Ret01),


    ?assertMatch(ok, ?call_store(Worker1, delete_links, [Level, Doc1, [link2, link3]])),

    ?assertMatch({error, link_not_found}, ?call_store(
        Worker2, fetch_link_target, [Level, Doc1, link2]
    ), 10),
    ?assertMatch({error, link_not_found}, ?call_store(
        Worker1, fetch_link_target, [Level, Doc1, link3]
    ), 10),
    ?assertMatch({error, link_not_found}, ?call_store(
        Worker2, fetch_link, [Level, Doc1, link2]
    ), 10),
    ?assertMatch({error, link_not_found}, ?call_store(
        Worker1, fetch_link, [Level, Doc1, link3]
    )),

    ?assertMatch(ok, ?call_store(Worker2, add_links, [Level, Doc1, [{link2, Doc2}, {link3, Doc3}]])),
    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key2])),
    ?assertMatch(ok, ?call_store(Worker1, delete_links, [Level, Doc1, link3]), 10),

    ?assertMatch({ok, {Key2, TestRecord}}, ?call_store(
        Worker1, fetch_link, [Level, Doc1, link2]
    ), 10),
    ?assertMatch({error, link_not_found}, ?call_store(
        Worker2, fetch_link, [Level, Doc1, link3]
    ), 10),
    ?assertMatch({error, link_not_found}, ?call_store(
        Worker2, fetch_link_target, [Level, Doc1, link3]
    ), 10),
    ?assertMatch({error, {not_found, _}}, ?call_store(
        Worker1, fetch_link_target, [Level, Doc1, link2]
    ), 10),

    %% Delete on document shall delete all its links
    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key1])),

    ?assertMatch({error, link_not_found}, ?call_store(
        Worker2, fetch_link, [Level, Doc1, link2]
    ), 10),

    ok = ?call_store(Worker2, delete, [Level, TestRecord, Key1]),
    ok = ?call_store(Worker2, delete, [Level, TestRecord, Key2]),
    ok = ?call_store(Worker2, delete, [Level, TestRecord, Key3]),

    ok.


%% generic list operation (on several nodes)
generic_list_test(TestRecord, Nodes, Level) ->
    generic_list_test(TestRecord, Nodes, Level, 0, non, 0).

generic_list_test(TestRecord, Nodes, Level, ListRetryAfter, BeforeRetryFun, DelSecondCheckAfter) ->
    Ret0 = ?call_store(rand_node(Nodes), list, [Level, TestRecord, ?GET_ALL, []]),
    ?assertMatch({ok, _}, Ret0),
    {ok, Objects0} = Ret0,
    Keys0 = lists:map(fun(#document{key = Key}) -> Key end, Objects0),

    ObjCount = 424,
    Keys = [rand_key() || _ <- lists:seq(1, ObjCount)],

    CreateDocFun = fun(Key) ->
        ?call_store(rand_node(Nodes), create, [Level,
            #document{
                key = Key,
                value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
            }])
    end,

    RemoveDocFun = fun(Key) ->
        ?call_store(rand_node(Nodes), delete, [Level, TestRecord, Key, ?PRED_ALWAYS])
    end,

    [?assertMatch({ok, _}, CreateDocFun(Key)) || Key <- Keys],

    ListFun = fun() ->
        Ret1 = ?call_store(rand_node(Nodes), list, [Level, TestRecord, ?GET_ALL, []]),
        ?assertMatch({ok, _}, Ret1),
        {ok, Objects1} = Ret1,
        ReceivedKeys = lists:map(fun(#document{key = Key}) ->
            Key end, Objects1) -- Keys0,

        ?assertMatch(ObjCount, erlang:length(Objects1) - erlang:length(Objects0)),
        ?assertMatch([], ReceivedKeys -- Keys),
        ReceivedKeys
    end,
    List1Ans = ListFun(),
    case ListRetryAfter of
        0 ->
            ok;
        _ ->
            timer:sleep(ListRetryAfter),
            case is_function(BeforeRetryFun) of
                true ->
                    BeforeRetryFun(List1Ans);
                _ ->
                    ok
            end,
            ListFun()
    end,

    [?assertMatch(ok, RemoveDocFun(Key)) || Key <- Keys],

    CheckDelFun = fun() ->
        Ret2 = ?call_store(rand_node(Nodes), list, [Level, TestRecord, ?GET_ALL, []]),
        ?assertMatch({ok, _}, Ret2),
        {ok, Objects2} = Ret2,

        ?assertMatch(0, erlang:length(Objects2) - erlang:length(Objects0))
    end,
    CheckDelFun(),
    case DelSecondCheckAfter of
        0 ->
            ok;
        _ ->
            timer:sleep(DelSecondCheckAfter),
            CheckDelFun()
    end,
    ok.

rand_key() ->
    base64:encode(crypto:rand_bytes(8)).

rand_node(Nodes) when is_list(Nodes) ->
    lists:nth(crypto:rand_uniform(1, length(Nodes) + 1), Nodes);
rand_node(Node) when is_atom(Node) ->
    Node.

prepare_list(1) ->
    "x";

prepare_list(Size) ->
    "x" ++ prepare_list(Size - 1).

for(N, N, F) ->
    F(N);
for(I, N, F) ->
    F(I),
    for(I + 1, N, F).

while(F, Guard) ->
    while(0, F, Guard).

while(Counter, F, Guard) ->
    case Guard() of
        true ->
            ok;
        _ ->
            F(Counter),
            while(Counter + 1, F, Guard)
    end.

from_timestamp(T) ->
    T / 1000.

to_timestamp(T) ->
    T * 1000.

disable_cache_control(Workers) ->
    disable_cache_control_and_set_dump_delay(Workers, 1000).

disable_cache_control_and_set_dump_delay(Workers, Delay) ->
    lists:foreach(fun(W) ->
        ?assertEqual(ok, gen_server:call({?NODE_MANAGER_NAME, W}, disable_cache_control)),
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, Delay)),
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, mem_clearing_ratio_to_stop, 0))
    end, Workers).

for(1, F) ->
    F();
for(N, F) ->
    F(),
    for(N - 1, F).
