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

-define(TIMEOUT, timer:seconds(60)).
-define(call_store(N, F, A), ?call(N, datastore, F, A)).
-define(call(N, M, F, A), ?call(N, M, F, A, ?TIMEOUT)).
-define(call(N, M, F, A, T), rpc:call(N, M, F, A, T)).

-define(test_record_f1(F), {_, F, _, _}).
-define(test_record_f2(F), {_, _, F, _}).
-define(test_record_f3(F), {_, _, _, F}).
-define(test_record_f1_3(F1, F3), {_, F1, _, F3}).

-define(SCOPE_MASTER_PROC_NAME, sm_proc).

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
%%tests
-export([local_test/1, global_test/1, global_atomic_update_test/1, disk_list_test/1,
    global_list_test/1, persistance_test/1, local_list_test/1, globally_cached_list_test/1,
    disk_only_links_test/1, global_only_links_test/1, globally_cached_links_test/1,
    link_walk_test/1, monitoring_global_cache_test_test/1, old_keys_cleaning_global_cache_test/1,
    clearing_global_cache_test/1, link_monitoring_global_cache_test/1, create_after_delete_global_cache_test/1,
    restoring_cache_from_disk_global_cache_test/1, prevent_reading_from_disk_global_cache_test/1,
    multiple_links_creation_disk_test/1, multiple_links_creation_global_only_test/1,
    clear_and_flush_global_cache_test/1, multilevel_foreach_global_cache_test/1,
    operations_sequence_global_cache_test/1, links_operations_sequence_global_cache_test/1,
    interupt_global_cache_clearing_test/1, disk_only_many_links_test/1, global_only_many_links_test/1,
    globally_cached_many_links_test/1, create_globally_cached_test/1, disk_only_create_or_update_test/1,
    global_only_create_or_update_test/1, globally_cached_create_or_update_test/1, links_scope_test/1]).
-export([utilize_memory/3]).

all() ->
    ?ALL([
        local_test, global_test, global_atomic_update_test, disk_list_test,
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
        globally_cached_create_or_update_test, links_scope_test
    ]).


%%%===================================================================
%%% Test functions
%%%===================================================================

% TODO - add tests that clear cache_controller model and check if cache still works,
% TODO - add tests that check time refreshing by get and fetch_link operations

links_scope_test(Config) ->
    MasterLoop = spawn_link(fun() -> scope_master_loop() end),
    global:register_name(?SCOPE_MASTER_PROC_NAME, MasterLoop),
    global:sync(),

    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
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

    set_mother_scope(scope2),
    GetAllLinks([]),
    set_other_scopes([scope1]),
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

    set_other_scopes([]),
    GetAllLinks([2,5,6]),
    set_mother_scope(scope1),
    GetAllLinks([4]),
    ?assertMatch(ok, ?call_store(Worker2, create_link, [?GLOBAL_ONLY_LEVEL, Doc, {GetLinkName(2), GetDoc(1)}])),
    AddLinkWithDoc(8),
    GetAllLinks([2,4,8]),
    set_other_scopes([scope2]),
    GetAllLinks([2,2,4,5,6,8]),
    DK1 = GetDocKey(1),
    ?assertMatch({ok, {DK1, _}}, ?call_store(Worker2, fetch_link, [?GLOBAL_ONLY_LEVEL, Doc, GetLinkName(2)])),

    set_mother_scope(scope2),
    set_other_scopes([scope1]),
    GetAllLinks([2,2,4,5,6,8]),
    DK2 = GetDocKey(2),
    ?assertMatch({ok, {DK2, _}}, ?call_store(Worker2, fetch_link, [?GLOBAL_ONLY_LEVEL, Doc, GetLinkName(2)])),
    DeleteLink(2),
    GetAllLinks([2,4,5,6,8]),
    ?assertMatch({ok, {DK1, _}}, ?call_store(Worker2, fetch_link, [?GLOBAL_ONLY_LEVEL, Doc, GetLinkName(2)])),
    DeleteLink(2),
    GetAllLinks([4,5,6,8]),
    DeleteLinks([4,5,6,8]),
    GetAllLinks([]),
    set_mother_scope(scope1),
    set_other_scopes([scope2]),
    GetAllLinks([]),

    MasterLoop ! stop,
    ok.

set_mother_scope(MotherScope) ->
    global:send(?SCOPE_MASTER_PROC_NAME, {set_mother_scope, self(), MotherScope}),
    receive
        scope_changed -> ok
    end.

set_other_scopes(OtherScopes) ->
    global:send(?SCOPE_MASTER_PROC_NAME, {set_other_scopes, self(), OtherScopes}),
    receive
        scope_changed -> ok
    end.

scope_master_loop() ->
    scope_master_loop(scope1, []).

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
    UpdateFun = fun({R, _F1, F2, F3}) ->
        {ok, {R, 2, F2, F3}}
    end,
    globally_cached_create_or_update_test_base(Config, UpdateMap, "map"),
    globally_cached_create_or_update_test_base(Config, UpdateFun, "fun").

globally_cached_create_or_update_test_base(Config, UpdateEntity, KeyExt) ->
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
    CModule = ?call_store(Worker1, driver_to_module, [?DISTRIBUTED_CACHE_DRIVER]),
    Key = <<"key_cgct">>,
    Doc =  #document{
        key = Key,
        value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    },
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, create, [Doc])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key]), 6),

    ?assertMatch(ok, ?call(Worker2, CModule, delete, [ModelConfig, Key, ?PRED_ALWAYS])),
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

    ?assertMatch(ok, ?call(Worker2, CModule, delete_links, [ModelConfig, Key, [link]])),
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

    Key = <<"key_mltb">>,
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
            ?call_store(Worker2, fetch_link, [Level, Doc, GetLinkName(I)])
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
    Uuid2 = caches_controller:get_cache_uuid({Key, link}, TestRecord),
    ?assertMatch(true, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid])),
    ?assertMatch(true, ?call(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid2])),
    ?assertMatch({ok, true}, ?call_store(Worker2, exists, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, true}, ?call(Worker2, PModule, exists, [ModelConfig, Key])),
    ?assertMatch({ok, _}, ?call_store(Worker2, fetch_link, [?GLOBAL_ONLY_LEVEL, Doc, link])),
    ?assertMatch({ok, _}, ?call(Worker2, PModule, fetch_link, [ModelConfig, Key, link])),


    ?assertEqual(ok, ?call(Worker1, caches_controller, wait_for_cache_dump, []), 10),
    ?assertMatch(ok, ?call(Worker1, caches_controller, delete_old_keys, [globally_cached, 0])),

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
    LinkCacheUuid = caches_controller:get_cache_uuid({Key, link}, TestRecord),
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
            datastore:normalize_link_target(Links)]),
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

    LinkCacheUuid = caches_controller:get_cache_uuid({Key, link}, TestRecord),
    LinkCacheUuid2 = caches_controller:get_cache_uuid({Key, link2}, TestRecord),
    ?assertMatch(ok, ?call_store(Worker2, add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{link, LinkedDoc}]])),
    ?assertMatch({ok, _}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    ?assertMatch(true, ?call(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]), 1),
    ?assertMatch({ok, _}, ?call(Worker1, PModule, fetch_link, [ModelConfig, Doc#document.key, link]), 6),

    ?assertMatch(ok, ?call_store(Worker1, delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [link]])),
    ?assertMatch({error,link_not_found}, ?call_store(Worker1, fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, link])),
    {ok, CC} = ?call(Worker1, cache_controller, get, [?GLOBAL_ONLY_LEVEL, LinkCacheUuid]),
    CCD = CC#document.value,
    ?assertEqual(delete_links, CCD#cache_controller.action),
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

    ?assertMatch(ok, ?call(Worker2, CModule, delete, [ModelConfig, Key, ?PRED_ALWAYS])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, get, [Key])),
    ?assertMatch({ok, true}, ?call(Worker2, CModule, exists, [ModelConfig, Key]), 1),

    ?assertMatch(ok, ?call(Worker2, CModule, delete, [ModelConfig, Key, ?PRED_ALWAYS])),
    ?assertMatch({ok, _}, ?call(Worker1, TestRecord, update, [Key, #{field1 => 2}])),
    ?assertMatch({ok, true}, ?call(Worker2, CModule, exists, [ModelConfig, Key])),

    ?assertMatch(ok, ?call(Worker2, CModule, delete_links, [ModelConfig, Key, [link]])),
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
                timestamp = to_timestamp(from_timestamp(os:timestamp()) - T - timer:minutes(5))
            }}
        end,
        ?assertMatch({ok, _}, ?call(Worker1, cache_controller, update, [?GLOBAL_ONLY_LEVEL, Uuid, UpdateFun]), 10)
    end, KeysWithTimes),

    ?assertMatch(ok, ?call(Worker1, caches_controller, delete_old_keys, [globally_cached, TimeWindow]), 10),

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
    Mem0Node = ?call(Worker2, erlang, memory, [total]),
    ct:print("Mem0 ~p xxx ~p", [Mem0, Mem0Node]),
    FreeMem = 100 - Mem0,
    ToAdd = min(10, FreeMem / 2),
    MemTarget = Mem0 + ToAdd / 2,
    MemUsage = Mem0 + ToAdd,

    ?assertMatch(ok, ?call(Worker1, ?MODULE, utilize_memory, [TestRecord, MemUsage, MemTarget], timer:minutes(10))),
    ?assertMatch(ok, test_utils:set_env(Worker2, ?CLUSTER_WORKER_APP_NAME, mem_to_clear_cache, MemTarget)),
    [{_, Mem1}] = monitoring:get_memory_stats(),
    Mem1Node = ?call(Worker2, erlang, memory, [total]),
    ct:print("Mem1 ~p xxx ~p", [Mem1, Mem1Node]),
    ?assert(Mem1 > MemTarget),

    ?assertEqual(ok, ?call(Worker2, caches_controller, wait_for_cache_dump, []), 150),
    % TODO Add answer checking when DB nodes will be run at separate machine
    CheckMemAns = gen_server:call({?NODE_MANAGER_NAME, Worker2}, check_mem_synch, timer:minutes(5)),
%%     ?assertMatch(ok, gen_server:call({?NODE_MANAGER_NAME, Worker2}, check_mem_synch, ?TIMEOUT)),
    [{_, Mem2}] = monitoring:get_memory_stats(),
    Mem2Node = ?call(Worker2, erlang, memory, [total]),
    ct:print("Mem2 ~p xxx ~p, check_mem: ~p", [Mem2, Mem2Node, CheckMemAns]),
    % TODO Change to node memory checking when DB nodes will be run at separate machine
    ?assertEqual(true, ?call(Worker2, erlang, memory, [total]) < (Mem0Node + Mem1Node) / 2, 10),
%%     ?assert(Mem2 < MemTarget),

    ok.

% helper fun used by clearing_global_cache_test
utilize_memory(TestRecord, MemUsage, MemTarget) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, mem_to_clear_cache, MemTarget),

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


%% Atomic update on global cache driver (on several nodes)
global_atomic_update_test(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Level = ?GLOBAL_ONLY_LEVEL,
    Key = rand_key(),

    ?assertMatch({ok, _},
        ?call_store(Worker1, create, [Level,
            #document{
                key = Key,
                value = datastore_basic_ops_utils:get_record(TestRecord, 0, <<"abc">>, {test, tuple})
            }])),

    Pid = self(),
    ?assertMatch({ok, Key},
        ?call_store(Worker2, update, [Level,
            TestRecord, Key,
            fun(#global_only_record{field1 = 0} = Record) ->
                {ok, Record#global_only_record{field2 = Pid}}
            end])),

    ?assertMatch({ok, #document{value = #global_only_record{field1 = 0, field2 = Pid}}},
        ?call_store(Worker1, get, [Level,
            TestRecord, Key])),

    UpdateFun = fun(#global_only_record{field1 = Value} = Record) ->
        {ok, Record#global_only_record{field1 = Value + 1}}
    end,

    Self = self(),
    Timeout = timer:seconds(30),
    utils:pforeach(fun(Node) ->
        ?call_store(Node, update, [Level, TestRecord, Key, UpdateFun]),
        Self ! done
    end, lists:duplicate(100, Worker1) ++ lists:duplicate(100, Worker2)),
    [receive done -> ok after Timeout -> ok end || _ <- lists:seq(1, 200)],

    ?assertMatch({ok, #document{value = ?test_record_f1(200)}},
        ?call_store(Worker1, get, [Level,
            TestRecord, Key])),

    ok.


%% list operation on global cache driver (on several nodes)
global_list_test(Config) ->
    TestRecord = ?config(test_record, Config),
    generic_list_test(TestRecord, ?config(cluster_worker_nodes, Config), ?GLOBAL_ONLY_LEVEL).

%% list operation on disk only driver (on several nodes)
disk_list_test(Config) ->
    generic_list_test(disk_only_record, ?config(cluster_worker_nodes, Config), ?DISK_ONLY_LEVEL).

%% list operation on disk only driver (on several nodes)
globally_cached_list_test(Config) ->
    generic_list_test(globally_cached_record, ?config(cluster_worker_nodes, Config), ?GLOBALLY_CACHED_LEVEL).


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
        Level, Doc1, [{link2, Doc2}, {link3, Doc3}]
    ])),

    %% Fetch all links and theirs targets
    Ret0 = ?call_store(Worker2, fetch_link_target, [Level, Doc1, link2]),
    Ret1 = ?call_store(Worker1, fetch_link_target, [Level, Doc1, link3]),
    Ret2 = ?call_store(Worker2, fetch_link, [Level, Doc1, link2]),
    Ret3 = ?call_store(Worker1, fetch_link, [Level, Doc1, link3]),

    ?assertMatch({ok, {Key2, TestRecord}}, Ret2),
    ?assertMatch({ok, {Key3, TestRecord}}, Ret3),
    ?assertMatch({ok, #document{key = Key2, value = ?test_record_f1(2)}}, Ret0),
    ?assertMatch({ok, #document{key = Key3, value = ?test_record_f1(3)}}, Ret1),

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
    Ret0 = ?call_store(rand_node(Nodes), list, [Level, TestRecord, ?GET_ALL, []]),
    ?assertMatch({ok, _}, Ret0),
    {ok, Objects0} = Ret0,

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

    Ret1 = ?call_store(rand_node(Nodes), list, [Level, TestRecord, ?GET_ALL, []]),
    ?assertMatch({ok, _}, Ret1),
    {ok, Objects1} = Ret1,
    ReceivedKeys = lists:map(fun(#document{key = Key}) ->
        Key end, Objects1 -- Objects0),

    ?assertMatch(ObjCount, erlang:length(Objects1) - erlang:length(Objects0)),
    ?assertMatch([], ReceivedKeys -- Keys),


    [?assertMatch(ok, RemoveDocFun(Key)) || Key <- Keys],


    Ret2 = ?call_store(rand_node(Nodes), list, [Level, TestRecord, ?GET_ALL, []]),
    ?assertMatch({ok, _}, Ret2),
    {ok, Objects2} = Ret2,

    ?assertMatch(0, erlang:length(Objects2) - erlang:length(Objects0)),
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

from_timestamp({Mega, Sec, Micro}) ->
    (Mega * 1000000 + Sec) * 1000 + Micro / 1000.

to_timestamp(T) ->
    {trunc(T / 1000000000), trunc(T / 1000) rem 1000000, trunc(T * 1000) rem 1000000}.

disable_cache_control(Workers) ->
    disable_cache_control_and_set_dump_delay(Workers, 1000).

disable_cache_control_and_set_dump_delay(Workers, Delay) ->
    lists:foreach(fun(W) ->
        ?assertEqual(ok, gen_server:call({?NODE_MANAGER_NAME, W}, disable_cache_control)),
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, Delay))
    end, Workers).

for(1, F) ->
    F();
for(N, F) ->
    F(),
    for(N - 1, F).