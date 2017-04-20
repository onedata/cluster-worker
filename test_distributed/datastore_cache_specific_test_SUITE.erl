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
-export([monitoring_global_cache_test_test/1, old_keys_cleaning_global_cache_test/1, clearing_global_cache_test/1,
    link_monitoring_global_cache_test/1, create_after_delete_global_cache_test/1,
    restoring_cache_from_disk_global_cache_test/1, prevent_reading_from_disk_global_cache_test/1,
    clear_and_flush_global_cache_test/1, multilevel_foreach_global_cache_test/1,
    operations_sequence_global_cache_test/1, links_operations_sequence_global_cache_test/1,
    interupt_global_cache_clearing_test/1, create_globally_cached_test/1, globally_cached_foreach_link_test/1,
    globally_cached_consistency_test/1, globally_cached_consistency_without_consistency_metadata_test/1,
    globally_cached_consistency_with_ambigues_link_names/1]).
-export([utilize_memory/2]).

all() ->
    ?ALL([
%%        monitoring_global_cache_test_test, % TODO - enable when dumping will depend on time of last modify operation
%%        old_keys_cleaning_global_cache_test, % TODO - suite to new clearing method
        % TODO - restore when mnesia is used to store data when tp proc terminates
%%        clearing_global_cache_test,
        link_monitoring_global_cache_test, create_after_delete_global_cache_test,
        restoring_cache_from_disk_global_cache_test, prevent_reading_from_disk_global_cache_test,
        clear_and_flush_global_cache_test, multilevel_foreach_global_cache_test, operations_sequence_global_cache_test,
        links_operations_sequence_global_cache_test, interupt_global_cache_clearing_test, create_globally_cached_test,
        globally_cached_foreach_link_test, globally_cached_consistency_test,
        globally_cached_consistency_without_consistency_metadata_test
        % TODO - rewrite
        % globally_cached_consistency_with_ambigues_link_names
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

% TODO - add tests that clear cache_controller model and check if cache still works,
% TODO - add tests that check time refreshing by get and fetch_link operations

globally_cached_consistency_with_ambigues_link_names(Config) ->
    %% given
    [Worker1, _Worker2] = Workers = ?config(cluster_worker_nodes, Config),
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
    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),

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

    % TODO - create wait_for_cache_dump for new cache
    timer:sleep(30000),
%%    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 100),
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

    CheckFlushLinks = fun(Start, Stop) ->
        for(Start, Stop, fun(I) ->
            ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, GetLinkName(I)]), 10)
        end)
    end,
    ClearLinks = fun() ->
        ?assertMatch(ok, ?call(Worker2, caches_controller, clear_links, [?GLOBAL_ONLY_LEVEL, TestRecord, Key]))
    end,

    CheckFlushAndClearDoc = fun(I) ->
        ?assertMatch({ok, _}, ?call(Worker2, PModule, get, [ModelConfig, GetLinkKey(I)]), 10),
        ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, GetLinkKey(I)]))
    end,

    ClearConsistencyMetadata = fun() ->
        ?assertMatch({ok, _}, ?call(Worker2, PModule, get, [ModelConfig, Key]), 10),
        ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),

        CheckFlushLinks(1,10),


        for(1, 10, fun(I) ->
            CheckFlushAndClearDoc(I)
        end),
        ClearLinks(),

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
    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results

    % clean
    GetAllKeys = fun
                 ('$end_of_table', Acc) ->
                     {abort, Acc};
                 (#document{key = Uuid}, Acc) ->
                     {next, [Uuid | Acc]}
             end,
    {_, AllKeys} = AllKeysAns = ?call_store(Worker1, list, [?GLOBAL_ONLY_LEVEL, TestRecord, GetAllKeys, []]),
    ?assertMatch({ok, _}, AllKeysAns),
    {_, AllKeys2} = AllKeysAns2 = ?call_store(Worker1, list, [?DISK_ONLY_LEVEL, TestRecord, GetAllKeys, []]),
    ?assertMatch({ok, _}, AllKeysAns2),
    lists:foreach(fun(Uuid) ->
        ?assertEqual(ok, ?call_store(Worker1, delete, [?GLOBALLY_CACHED_LEVEL, TestRecord, Uuid]))
    end, AllKeys ++ AllKeys2),

    % TODO - create wait_for_cache_dump for new cache
    timer:sleep(30000),
%%    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 100),
    ?assertMatch(ok, ?call(Worker1, caches_controller, delete_old_keys, [globally_cached, 0])),
    % Restore consistency if needed
    ?call_store(Worker1, list, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetAllKeys, []]),

    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller, check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),
    % forget cleaning
    ?assertMatch({ok, _}, ?call(Worker1, cache_consistency_controller, save,
        [#document{key = TestRecord, value = #cache_consistency_controller{}}])),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    Key = <<"key_gcct">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    CHKey = {TestRecord, Key},
    CCCNode = ?call(Worker2, consistent_hasing, get_node, [CHKey]),
    CCCUuid = caches_controller:get_cache_uuid(Key, TestRecord),

    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller, check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),
    ?assertMatch({ok, _, _}, ?call(CCCNode, caches_controller, check_cache_consistency_direct,
        [?GLOBAL_SLAVE_DRIVER, CCCUuid, TestRecord])),

    GetLinkName = fun(I) ->
        list_to_atom("linked_key_gcct" ++ integer_to_list(I))
    end,
    GetLinkKey = fun(I) ->
        list_to_binary("linked_key_gcct" ++ integer_to_list(I))
    end,

    CheckFlushLinks = fun(Start, Stop) ->
        for(Start, Stop, fun(I) ->
            ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, GetLinkName(I)]), 10)
        end)
    end,
    CheckDelFlushLinks = fun(Start, Stop) ->
        for(Start, Stop, fun(I) ->
            ?assertMatch({error, link_not_found}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, GetLinkName(I)]), 10)
        end)
    end,
    ClearLinks = fun() ->
        ?assertMatch(ok, ?call(Worker2, caches_controller, clear_links, [?GLOBAL_ONLY_LEVEL, TestRecord, Key]))
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

    ?assertMatch({ok, _, _}, ?call(CCCNode, caches_controller,check_cache_consistency_direct,
        [?GLOBAL_SLAVE_DRIVER, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    CheckFlushLinks(1, 200),
    ClearLinks(),

    for(22, 23, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [GetLinkName(I)]]))
    end),

    ?assertMatch(not_monitored, ?call(CCCNode, caches_controller,check_cache_consistency_direct,
        [?GLOBAL_SLAVE_DRIVER, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    CheckLinks(1, 2, 198),
    ?assertMatch({ok, _, _}, ?call(CCCNode, caches_controller,check_cache_consistency_direct,
        [?GLOBAL_SLAVE_DRIVER, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    for(32, 33, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [GetLinkName(I)]]))
    end),
    CheckDelFlushLinks(32, 33),
    ClearLinks(),

    ?assertMatch(not_monitored, ?call(CCCNode, caches_controller,check_cache_consistency_direct,
        [?GLOBAL_SLAVE_DRIVER, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    CheckLinks(1, 2, 196),
    ?assertMatch({ok, _, _}, ?call(CCCNode, caches_controller,check_cache_consistency_direct,
        [?GLOBAL_SLAVE_DRIVER, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),


    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    CheckFlushAndClearDoc = fun(I) ->
        ?assertMatch({ok, _}, ?call(Worker2, PModule, get, [ModelConfig, GetLinkKey(I)]), 10),
        ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, GetLinkKey(I)]))
    end,

    FlushDelAndClearDoc = fun(I) ->
        ?assertMatch({error, {not_found, _}}, ?call(Worker2, PModule, get, [ModelConfig, GetLinkKey(I)]), 10),
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

    ?assertMatch({ok, _, _}, ?call(CCCNode, caches_controller,check_cache_consistency_direct,
        [?GLOBAL_SLAVE_DRIVER, CCCUuid])),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),

    for(10, 20, fun(I) ->
        CheckFlushAndClearDoc(I)
    end),

    for(22, 23, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, delete, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetLinkKey(I)]))
    end),

    ?assertMatch(not_monitored, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),
    ?assertMatch({ok, _, _}, ?call(CCCNode, caches_controller,check_cache_consistency_direct,
        [?GLOBAL_SLAVE_DRIVER, CCCUuid])),

    CheckDocs(1, 2, 199),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),
    ?assertMatch({ok, _, _}, ?call(CCCNode, caches_controller,check_cache_consistency_direct,
        [?GLOBAL_SLAVE_DRIVER, CCCUuid])),

    for(32, 33, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, delete, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetLinkKey(I)]))
    end),

    for(32, 32, fun(I) ->
        FlushDelAndClearDoc(I)
    end),

    ?assertMatch(not_monitored, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),
    ?assertMatch({ok, _, _}, ?call(CCCNode, caches_controller,check_cache_consistency_direct,
        [?GLOBAL_SLAVE_DRIVER, CCCUuid])),

    CheckDocs(1, 2, 197),
    ?assertMatch({ok, _, _}, ?call(Worker2, caches_controller,check_cache_consistency, [?GLOBAL_ONLY_LEVEL, TestRecord])),
    ?assertMatch({ok, _, _}, ?call(CCCNode, caches_controller,check_cache_consistency_direct,
        [?GLOBAL_SLAVE_DRIVER, CCCUuid])),

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
    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, <<"key_gcflt1">>]), 10),
    ?call_store(Worker2, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [<<"key_gcflt1">>]]),

    AccFun = fun(LinkName, _, Acc) ->
        [LinkName | Acc]
    end,

    ?assertMatch({ok, [<<"key_gcflt2">>]},
        ?call_store(Worker1, foreach_link, [?GLOBALLY_CACHED_LEVEL, Doc, AccFun, []])),

    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, <<"key_gcflt2">>]), 10),
    ?assertMatch(ok, ?call(Worker2, caches_controller, clear_links, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),

    ?assertMatch({ok, [<<"key_gcflt2">>]},
        ?call_store(Worker1, foreach_link, [?GLOBALLY_CACHED_LEVEL, Doc, AccFun, []])),

    ok.

create_globally_cached_test(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
%%    lists:foreach(fun(W) ->
%%        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
%%    end, Workers),

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    Key = <<"key_cgct">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 10),

    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({error, already_exists}, ?call(Worker1, TestRecord, create, [Doc])),

%%    ?assertMatch(ok, ?call(Worker2, PModule, delete, [ModelConfig, Key, ?PRED_ALWAYS])),
%%    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
%%    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 10),

    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    timer:sleep(timer:seconds(6)), % wait to check if any async action hasn't destroyed value at disk
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 10),

    LinkedKey = "linked_key_cgct",
    LinkedDoc = #document{
        key = LinkedKey,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2, <<"efg">>, {test, tuple2})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc])),
    ?assertMatch(ok, ?call_store(Worker1, create_link, [?GLOBALLY_CACHED_LEVEL, Doc, {link, LinkedDoc}])),
    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, link]), 10),

    ?assertMatch(ok, ?call(Worker2, caches_controller, clear_links, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({error, already_exists}, ?call_store(Worker1, create_link, [?GLOBALLY_CACHED_LEVEL, Doc, {link, LinkedDoc}])),

%%    ?assertMatch(ok, ?call(Worker2, PModule, delete_links, [ModelConfig, Key, [link]])),
%%    ?assertMatch(ok, ?call_store(Worker1, create_link, [?GLOBALLY_CACHED_LEVEL, Doc, {link, LinkedDoc}])),
%%    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, link]), 10),

    ?assertMatch(ok, ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [link]])),
    ?assertMatch(ok, ?call_store(Worker1, create_link, [?GLOBALLY_CACHED_LEVEL, Doc, {link, LinkedDoc}])),
    timer:sleep(timer:seconds(6)), % wait to check if any async action hasn't destroyed value at disk
    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, link]), 10),

    ok.

interupt_global_cache_clearing_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
%%    lists:foreach(fun(W) ->
%%        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
%%    end, Workers),

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

    % TODO - delete sleep when wait_for_cache_dump works
    timer:sleep(timer:seconds(10)),
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

%%    Uuid = caches_controller:get_cache_uuid(Key, TestRecord),
%%    Uuid2 = caches_controller:get_cache_uuid({Key, link, cache_controller_link_key}, TestRecord),
%%    ?assertMatch(true, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid])),
%%    ?assertMatch(true, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid2])),
    % TODO - GLOBALLY_CACHED_LEVEL -> GLOBAL_ONLY_LEVEL when interruption is possible
    ?assertMatch({ok, true}, ?call_store(Worker2, exists, [?GLOBALLY_CACHED_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),
    ?assertMatch({ok, _}, ?call_store(Worker2, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, link])),


    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 10),
    ?assertMatch(ok, ?call(Worker1, caches_controller, delete_old_keys, [globally_cached, 0])),
    timer:sleep(timer:seconds(5)), % for asynch controll data clearing

%%    ?assertMatch(false, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid])),
%%    ?assertMatch(false, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid2])),
    CModuleInternal = ?GLOBAL_SLAVE_DRIVER,
    ?assertMatch({ok, false}, ?call(Worker2, CModuleInternal, exists, [ModelConfig, Key])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),
    ?assertMatch({error,link_not_found}, ?call(Worker2, CModuleInternal, fetch_link, [ModelConfig, Key, link])),
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
%%    lists:foreach(fun(W) ->
%%        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
%%    end, Workers),

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

%%    ?assertMatch(ok, ?call(Worker1, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, all])),
    ?assertMatch(ok, ?call(Worker1, caches_controller, clear_links, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link]), 10),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link2]), 10),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link3]), 10),
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

% TODO - is flush needed?
clear_and_flush_global_cache_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
%%    lists:foreach(fun(W) ->
%%        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
%%    end, Workers),

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
%%    CModule = ?call_store(Worker1, driver_to_module, [?DISTRIBUTED_CACHE_DRIVER]),
    CModuleInternal = ?GLOBAL_SLAVE_DRIVER,

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
%%    ?assertMatch(ok, ?call(Worker2, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 10),

    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),
%%    ?assertMatch(ok, ?call(Worker1, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, false}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 10),

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
%%    ?assertMatch(ok, ?call(Worker2, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 10),
    ?assertMatch(ok, ?call(Worker1, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, false}, ?call(Worker2, CModuleInternal, exists, [ModelConfig, Key])),
    ?assertMatch({ok, false}, ?call(Worker1, CModuleInternal, exists, [ModelConfig, Key])),
    timer:sleep(timer:seconds(6)), % wait to check if any async action hasn't destroyed value at disk
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, get, [Key])),
    ?assert(lists:member({ok, true}, [?call(Worker1, CModuleInternal, exists, [ModelConfig, Key]),
        ?call(Worker2, CModuleInternal, exists, [ModelConfig, Key])])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, save, [Doc_v2])),
    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, false}, ?call(Worker2, CModuleInternal, exists, [ModelConfig, Key])),
    ?assertMatch({ok, false}, ?call(Worker1, CModuleInternal, exists, [ModelConfig, Key])),
    timer:sleep(timer:seconds(6)), % wait to check if any async action hasn't destroyed value at disk

    ?assertMatch(true, ?call(Worker1, TestRecord, exists, [Key])),
    ?assert(lists:member({ok, true}, [?call(Worker1, CModuleInternal, exists, [ModelConfig, Key]),
        ?call(Worker2, CModuleInternal, exists, [ModelConfig, Key])])),
    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch(true, ?call(Worker1, TestRecord, exists, [Key])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, get, [Key])),

    CheckField1 = fun() ->
        {_, GetDoc} = GetAns = ?call(Worker2, PModule, get, [ModelConfig, Key]),
        ?assertMatch({ok, _}, GetAns),
        GetValue = GetDoc#document.value,
        GetValue#globally_cached_record.field1
    end,
%%    ?assertMatch(1, CheckField1()),

    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, get, [Key])),
    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, save, [Doc_v2])),
    ?assertMatch(100, CheckField1(), 7),



    LinkedKey = <<"linked_key_caft">>,
    LinkedDoc = #document{
        key = LinkedKey,
        value = datastore_basic_ops_utils:get_record(TestRecord, 2, <<"efg">>, {test, tuple2})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [LinkedDoc])),
    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),
    ?assertMatch({error,link_not_found}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link])),
%%    ?assertMatch(ok, ?call(Worker1, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, all])),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link]), 10),

    ?assertMatch(ok, ?call_store(Worker2, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [link]])),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link])),
%%    ?assertMatch(ok, ?call(Worker1, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, link])),
    ?assertMatch({error,link_not_found}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link]), 10),

    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),
%%    ?assertMatch(ok, ?call(Worker1, caches_controller, flush, [?GLOBAL_ONLY_LEVEL, TestRecord, Key, link])),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link]), 10),
    ?assertMatch(ok, ?call(Worker1, caches_controller, clear_links, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({error,link_not_found}, ?call(Worker1, CModuleInternal, fetch_link, [ModelConfig, Key, link])),
    ?assertMatch({error,link_not_found}, ?call(Worker2, CModuleInternal, fetch_link, [ModelConfig, Key, link])),
    timer:sleep(timer:seconds(6)), % wait to check if any async action hasn't destroyed value at disk
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Key, link])),

    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    ?assert(lists:any(fun({ok, _}) -> true; (_) -> false end, [?call(Worker1, CModuleInternal, fetch_link, [ModelConfig, Key, link]),
        ?call(Worker2, CModuleInternal, fetch_link, [ModelConfig, Key, link])])),
    ?assertMatch(ok, ?call(Worker1, caches_controller, clear_links, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),

    ok.

create_after_delete_global_cache_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
%%    lists:foreach(fun(W) ->
%%        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
%%    end, Workers),

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

    ?assertMatch({error,link_not_found}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link]), 2),
    ?assertMatch({error,link_not_found}, ?call_store(Worker1, fetch_link, [?GLOBAL_ONLY_LEVEL, Doc, link])),
    ?assertMatch({error,link_not_found}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link])),

    Uuid = caches_controller:get_cache_uuid(Key, TestRecord),
%%    ?assertMatch(true, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid]), 2),
    ?assertMatch({ok, false}, ?call(Worker1, PModule, exists, [ModelConfig, Key])),
    ?assertMatch({ok, true}, ?call(Worker1, PModule, exists, [ModelConfig, Key]), 10),

    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),
    ?assertMatch(ok, ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [link]])),
    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),

    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
%%    LinkCacheUuid = caches_controller:get_cache_uuid({Key, link, cache_controller_link_key}, TestRecord),
%%    ?assertMatch(true, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]), 2),
    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBAL_ONLY_LEVEL, Doc, link])),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]), 10),

    ok.

link_monitoring_global_cache_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
%%    lists:foreach(fun(W) ->
%%        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
%%    end, Workers),

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

%%    LinkCacheUuid = caches_controller:get_cache_uuid({Key, link, cache_controller_link_key}, TestRecord),
%%    LinkCacheUuid2 = caches_controller:get_cache_uuid({Key, link2, cache_controller_link_key}, TestRecord),
    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),
    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
%%    ?assertMatch(true, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]), 2),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]), 10),

    ?assertMatch(ok, ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [link]])),
    ?assertMatch({error,link_not_found}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
%%    {ok, CC} = ?call(Worker1, cache_controller, get, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]),
%%    CCD = CC#document.value,
%%    ?assert((CCD#cache_controller.action =:= to_be_del) orelse (CCD#cache_controller.action =:= delete_links)),
    ?assertMatch({error,link_not_found}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]), 10),
%%    ?assertMatch(false, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]), 2),

    ?assertMatch(ok, ?call_store(Worker2, add_links,
        [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}, {link2, LinkedDoc2}]])),
    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link2])),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link2]), 10),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]), 10),

    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    ?assertMatch({error,link_not_found}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link]), 2),
%%    ?assertMatch({ok, #document{value = #cache_controller{action = delete_links}}},
%%        ?call(Worker1, cache_controller, get, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]), 2),

    ?assertMatch({error,link_not_found}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link2]), 2),
%%    ?assertMatch({ok, #document{value = #cache_controller{action = delete_links}}},
%%        ?call(Worker1, cache_controller, get, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid2]), 2),

    ?assertMatch({error,link_not_found}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]), 10),
    ?assertMatch({error,link_not_found}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link2]), 10),
%%    ?assertMatch(false, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]), 2),
%%    ?assertMatch(false, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid2]), 2),
    ok.


prevent_reading_from_disk_global_cache_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
%%    lists:foreach(fun(W) ->
%%        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
%%    end, Workers),

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    Key = <<"key_prfdt">>,
    CAns = ?call(Worker1, TestRecord, create, [
        #document{
            key = Key,
            value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
        }]),
    ?assertMatch({ok, _}, CAns),
    ?assertMatch({ok, true}, ?call(Worker1, PModule, exists, [ModelConfig, Key]), 10),

    UpdateFun = #{field1 => 2},
    ?assertMatch({ok, _}, ?call(Worker2, TestRecord, update, [Key, UpdateFun])),
    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    ?assertMatch({error, {not_found, _}}, ?call(Worker1, TestRecord, get, [Key])),
    ?assertMatch(false, ?call(Worker1, TestRecord, exists, [Key])),
    ?assertMatch({ok, false}, ?call(Worker1, PModule, exists, [ModelConfig, Key]), 10),
    ok.


restoring_cache_from_disk_global_cache_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
%%    lists:foreach(fun(W) ->
%%        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)))
%%    end, Workers),

    ModelConfig = TestRecord:model_init(),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    CModule = ?call_store(Worker1, driver_to_module, [?MEMORY_DRIVER]),
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

    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 10),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, LinkedKey]), 10),
    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, link]), 10),

    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, get, [Key])),
    ?assertMatch({ok, true}, ?call(Worker2, CModule, exists, [ModelConfig, Key]), 2),

    ?assertMatch(ok, ?call(Worker2, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, update, [Key, #{field1 => 2}])),
    ?assertMatch({ok, true}, ?call(Worker2, CModule, exists, [ModelConfig, Key])),

    ?assertMatch(ok, ?call(Worker2, caches_controller, clear_links, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, _}, ?call_store(Worker2, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    ?assertMatch({ok, _}, ?call(Worker2, CModule, fetch_link, [ModelConfig, Key, link]), 2),
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

%%    Uuid = caches_controller:get_cache_uuid(Key, TestRecord),
%%    ?assertMatch(true, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid]), 2),
%%    ?assertMatch(true, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid]), 2),

    ?assertMatch({ok, false}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 10),

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
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key2]), 10),

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
    timer:sleep(timer:seconds(1)), % allow start cache dumping
    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 10),
    CorruptedUuid = caches_controller:get_cache_uuid(CorruptedKey, TestRecord),
    ModelConfig = TestRecord:model_init(),
    DModule = ?call(Worker1, datastore, driver_to_module, [?MEMORY_DRIVER]),
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
            {ok, Record}
        % TODO - we cannot make value in cache older now - refactor test
%%            {ok, Record#cache_controller{
%%                timestamp = to_timestamp(from_timestamp(os:system_time(?CC_TIMEUNIT)) - T - timer:minutes(5))
%%            }}
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
    ?assertMatch(true, ?call(Worker2, TestRecord, exists, [K]), 2),

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
%%    ToAdd = min(10, FreeMem / 2),
    % TODO - revert after integration with Krzysiek
    ToAdd = min(5, FreeMem / 2),
    MemCheck1 = Mem0 + ToAdd / 2,
    MemUsage = Mem0 + ToAdd,

    {ok, MemRatioOld} = test_utils:get_env(Worker2, ?CLUSTER_WORKER_APP_NAME, node_mem_ratio_to_clear_cache),
    {ok, MemMbOld} = test_utils:get_env(Worker2, ?CLUSTER_WORKER_APP_NAME, erlang_mem_to_clear_cache_mb),

    ?assertMatch(ok, test_utils:set_env(Worker2, ?CLUSTER_WORKER_APP_NAME, node_mem_ratio_to_clear_cache, 0)),
    ?assertMatch(ok, test_utils:set_env(Worker2, ?CLUSTER_WORKER_APP_NAME, memory_store_idle_timeout_ms, 5000)),
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

    % TODO - wait_for_cache_dump works for new cache
    timer:sleep(90000),
%%    ?assertEqual(ok, ?call(Worker2, caches_controller, wait_for_cache_dump, []), 150),
%%    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 150),

    timer:sleep(1000), % to kill memory store processes

%%    ?assertMatch(ok, gen_server:call({?NODE_MANAGER_NAME, Worker2}, check_mem_synch, ?TIMEOUT)),
%%    ?assertMatch(ok, gen_server:call({?NODE_MANAGER_NAME, Worker1}, check_mem_synch, ?TIMEOUT)),
    CAns1 = gen_server:call({?NODE_MANAGER_NAME, Worker1}, check_mem_synch, ?TIMEOUT),
    CAns2 = gen_server:call({?NODE_MANAGER_NAME, Worker2}, check_mem_synch, ?TIMEOUT),
    ct:print("Clear ans ~p, ~p", [CAns1, CAns2]),

    [{_, Mem2}] = monitoring:get_memory_stats(),
    Mem2Node = node_mem(Worker2),
    Mem2Ets = ?call(Worker2, erlang, memory, [ets]),
    ct:print("Mem2 ~p, ~p, ~p", [Mem2, Mem2Node, Mem2Ets]),
    % TODO Change add node memory checking when DB nodes will be run at separate machine
    ?assertMatch(true, node_mem(Worker2) < (Mem0Node + Mem1Node) / 2, 30),
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

    % TODO - wait_for_cache_dump works for new cache
    timer:sleep(60000),
%%    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 100),
    ?assertMatch(ok, ?call(Worker1, caches_controller, delete_old_keys, [globally_cached, 0])),

    % TODO - check when batch delete is on
%%    ?assertMatch({ok, []}, ?call_store(Worker1, list, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetAllKeys, []])),
%%    ?assertMatch({ok, []}, ?call_store(Worker1, list, [?GLOBAL_ONLY_LEVEL, TestRecord, GetAllKeys, []])),
%%    timer:sleep(timer:seconds(15)), % give couch time to process deletes
%%    ?assertMatch({ok, []}, ?call_store(Worker2, list, [?DISK_ONLY_LEVEL, TestRecord, GetAllKeys, []])),


    ct:print("List cache ~p", [?call_store(Worker1, list, [?GLOBALLY_CACHED_LEVEL, TestRecord, GetAllKeys, []])]),
    ct:print("List cache ~p", [?call_store(Worker1, list, [?GLOBAL_ONLY_LEVEL, TestRecord, GetAllKeys, []])]),
    timer:sleep(timer:seconds(15)), % give couch time to process deletes
    ct:print("List cache ~p", [?call_store(Worker2, list, [?DISK_ONLY_LEVEL, TestRecord, GetAllKeys, []])]),

    ?assertMatch(ok, test_utils:set_env(Worker2, ?CLUSTER_WORKER_APP_NAME, node_mem_ratio_to_clear_cache, MemRatioOld)),
    ?assertMatch(ok, test_utils:set_env(Worker2, ?CLUSTER_WORKER_APP_NAME, erlang_mem_to_clear_cache_mb, MemMbOld)),

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


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(Case, Config) ->
    datastore_basic_ops_utils:set_env(Case, Config).

end_per_testcase(_Case, Config) ->
    datastore_basic_ops_utils:clear_env(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
        % TODO - delete next line
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, Delay)),
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, datastore_pool_batch_delay, 1000)),
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, mem_clearing_ratio_to_stop, 0))
    end, Workers).

for(1, F) ->
    F();
for(N, F) ->
    F(),
    for(N - 1, F).
