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
-define(call_store(N, Model, F, A), ?call(N,
    model, execute_with_default_context, [Model, F, A])).
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
-export([local_test/1, global_test/1, global_atomic_update_test/1, globally_cached_atomic_update_test/1,
    disk_list_test/1, global_list_test/1, persistance_test/1, local_list_test/1, globally_cached_list_test/1,
    disk_only_links_test/1, global_only_links_test/1, globally_cached_links_test/1,
    link_walk_test/1, multiple_links_creation_disk_test/1, multiple_links_creation_global_only_test/1,
    disk_only_many_links_test/1, global_only_many_links_test/1, globally_cached_many_links_test/1,
    disk_only_create_or_update_test/1, global_only_create_or_update_test/1, globally_cached_create_or_update_test/1,
    links_scope_test/1, links_scope_proc_mem_test/1, test_models/1]).
-export([update_and_check/4, execute_with_link_context/4, execute_with_link_context/5]).

all() ->
    ?ALL([
        local_test, global_test, global_atomic_update_test, globally_cached_atomic_update_test, disk_list_test,
        global_list_test, persistance_test, local_list_test, globally_cached_list_test,
        disk_only_links_test, global_only_links_test, globally_cached_links_test, link_walk_test,
        multiple_links_creation_disk_test, multiple_links_creation_global_only_test,
        disk_only_many_links_test, global_only_many_links_test, globally_cached_many_links_test,
        disk_only_create_or_update_test, global_only_create_or_update_test, globally_cached_create_or_update_test,
        links_scope_test, links_scope_proc_mem_test, test_models
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

% TODO - add tests that clear cache_controller model and check if cache still works,
% TODO - add tests that check time refreshing by get and fetch_link operations

test_models(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Models = ?call(Worker, datastore_config, models, []),

    lists:foreach(fun(ModelName) ->
%%        ct:print("Module ~p", [ModelName]),

        #model_config{store_level = SL} = MC = ?call(Worker, ModelName, model_init, []),
        Cache = case SL of
            ?GLOBALLY_CACHED_LEVEL -> true;
            ?LOCALLY_CACHED_LEVEL -> true;
            _ -> false
        end,

        Key = list_to_binary("key_tm_" ++ atom_to_list(ModelName)),
        Doc =  #document{
            key = Key,
            value = MC#model_config.defaults
        },
        ?assertMatch({ok, _}, ?call_store(Worker, ModelName, save, [Doc])),
        ?assertMatch({ok, true}, ?call_store(Worker, ModelName, exists, [Key])),

%%        ct:print("Module ok ~p", [ModelName]),

        case Cache of
            true ->
                PModule = ?call_store(Worker, driver_to_module, [?PERSISTENCE_DRIVER]),
                ?assertMatch({ok, true}, ?call(Worker, PModule, exists, [MC, Key]), 10);
%%                ct:print("Module caching ok ~p", [ModelName]);
            _ ->
                ok
        end
    end, Models).

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
        ?assertMatch(ok, ?call_store(Worker1, link_scopes_test_record,
            add_links, [Doc, [{GetLinkName(I), GetDoc(I)}]]))
    end,
    AddLinkWithDoc = fun(I) ->
        ?assertMatch({ok, _}, ?call(Worker2, link_scopes_test_record, create, [GetDoc(I)])),
        AddLink(I)
    end,
    CreateLink = fun(I) ->
        ?assertMatch(ok, ?call_store(Worker2, link_scopes_test_record,
            create_link, [Doc, {GetLinkName(I), GetDoc(I)}]))
    end,
    CreateExistingLink = fun(I) ->
        ?assertMatch({error, already_exists}, ?call_store(Worker2,
            link_scopes_test_record, create_link, [Doc, {GetLinkName(I), GetDoc(I)}]))
    end,
    CreateLinkWithDoc = fun(I) ->
        ?assertMatch({ok, _}, ?call(Worker1, link_scopes_test_record, create, [GetDoc(I)])),
        CreateLink(I)
    end,
    FetchLink = fun(I) ->
        ?assertMatch({ok, _}, ?call_store(Worker2, link_scopes_test_record,
            fetch_link, [Doc, GetLinkName(I)]))
    end,
    GetAllLinks = fun(Links) ->
        AccFun = fun(LinkName, _, Acc) ->
            [LinkName | Acc]
                 end,
        {_, ListedLinks} = FLAns = ?call_store(Worker1, link_scopes_test_record,
            foreach_link, [Doc, AccFun, []]),
        ?assertMatch({ok, _}, FLAns),

        lists:foreach(fun(I) ->
            ?assert(lists:member(GetLinkName(I), ListedLinks))
        end, Links),
        LinksLength = length(Links),
        ?assertMatch(LinksLength, length(ListedLinks))
    end,
    DeleteLink = fun(I) ->
        ?assertMatch(ok, ?call_store(Worker2, link_scopes_test_record,
            delete_links, [Doc, [GetLinkName(I)]]))
    end,
    DeleteLinks = fun(Links) ->
        ?assertMatch(ok, ?call_store(Worker2, link_scopes_test_record,
            delete_links, [Doc, lists:map(fun(I) -> GetLinkName(I) end, Links)]))
    end,

    ?assertMatch({ok, false}, ?call_store(Worker2, link_scopes_test_record,
        exists_link_doc, [Doc, <<"scope1">>])),
    ?assertMatch({ok, false}, ?call_store(Worker2, link_scopes_test_record,
        exists_link_doc, [Doc, <<"scope2">>])),

    set_link_replica_scope(<<"scope1">>),
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

    ?assertMatch({ok, true}, ?call_store(Worker2, link_scopes_test_record,
        exists_link_doc, [Doc, <<"scope1">>])),
    ?assertMatch({ok, false}, ?call_store(Worker2, link_scopes_test_record,
        exists_link_doc, [Doc, <<"scope2">>])),

    set_link_replica_scope(<<"scope2">>),
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

    ?assertMatch({ok, true}, ?call_store(Worker2, link_scopes_test_record,
        exists_link_doc, [Doc, <<"scope1">>])),
    ?assertMatch({ok, true}, ?call_store(Worker2, link_scopes_test_record,
        exists_link_doc, [Doc, <<"scope2">>])),

    set_link_replica_scope(<<"scope1">>),
    DeleteLinks([3, 7, 100, 5, 6]),
    GetAllLinks([2,4]),
    ?assertMatch(ok, ?call_store(Worker2, link_scopes_test_record,
        create_link, [Doc, {GetLinkName(5), GetDoc(1)}])),
    AddLinkWithDoc(8),
    GetAllLinks([2,4,5,8]),
    DK1 = GetDocKey(1),
    ?assertMatch({ok, {DK1, _}}, ?call_store(Worker2, link_scopes_test_record,
        fetch_link, [Doc, GetLinkName(5)])),
    ?assertMatch(ok, ?call_store(Worker2, link_scopes_test_record, add_links,
        [Doc, [{GetLinkName(2), GetDoc(1)}]])),
    ?assertMatch({ok, {DK1, _}}, ?call_store(Worker2, link_scopes_test_record,
        fetch_link, [Doc, GetLinkName(2)])),

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
    AddLink = fun(I, LinkReplicaScope, OtherScopes) ->
        ?assertMatch(ok, ?call(Worker1, ?MODULE, execute_with_link_context, [LinkReplicaScope, OtherScopes,
            add_links, [?GLOBALLY_CACHED_LEVEL, Doc, [{GetLinkName(I), GetDoc(I)}]]]))
    end,
    AddLinkWithDoc = fun(I, LinkReplicaScope, OtherScopes) ->
        ?assertMatch({ok, _}, ?call(Worker2, link_scopes_test_record2, create, [GetDoc(I)])),
        AddLink(I, LinkReplicaScope, OtherScopes)
    end,
    CreateLink = fun(I, LinkReplicaScope, OtherScopes) ->
        ?assertMatch(ok, ?call(Worker2, ?MODULE, execute_with_link_context, [LinkReplicaScope, OtherScopes,
            create_link, [?GLOBALLY_CACHED_LEVEL, Doc, {GetLinkName(I), GetDoc(I)}]]))
    end,
    CreateExistingLink = fun(I, LinkReplicaScope, OtherScopes) ->
        ?assertMatch({error, already_exists}, ?call(Worker2, ?MODULE, execute_with_link_context, [LinkReplicaScope, OtherScopes,
            create_link, [?GLOBALLY_CACHED_LEVEL, Doc, {GetLinkName(I), GetDoc(I)}]]))
    end,
    CreateLinkWithDoc = fun(I, LinkReplicaScope, OtherScopes) ->
        ?assertMatch({ok, _}, ?call(Worker1, link_scopes_test_record2, create, [GetDoc(I)])),
        CreateLink(I, LinkReplicaScope, OtherScopes)
    end,
    FetchLink = fun(I, LinkReplicaScope, OtherScopes) ->
        ?assertMatch({ok, _}, ?call(Worker2, ?MODULE, execute_with_link_context, [LinkReplicaScope, OtherScopes,
            fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, GetLinkName(I)]])),
        ?assertMatch({ok, _}, ?call(Worker2, ?MODULE, execute_with_link_context, [LinkReplicaScope, OtherScopes, PModule,
            fetch_link, [ModelConfig, Key, GetLinkName(I)]]), 7)
    end,
    GetAllLinks = fun(Links, LinkReplicaScope, OtherScopes) ->
        AccFun = fun(LinkName, LinkValue, Acc) ->
            maps:put(LinkName, LinkValue, Acc)
        end,
        {_, ListedLinks} = FLAns = ?call(Worker1, ?MODULE, execute_with_link_context, [LinkReplicaScope, OtherScopes,
            foreach_link, [?GLOBALLY_CACHED_LEVEL, Doc, AccFun, #{}]]),
        ?assertMatch({ok, _}, FLAns),

        lists:foreach(fun(I) ->
            ?assert(maps:is_key(GetLinkName(I), ListedLinks))
        end, Links),
        LinksLength = length(Links),
        ?assertMatch(LinksLength, maps:size(ListedLinks))
    end,
    DeleteLink = fun(I, LinkReplicaScope, OtherScopes) ->
        ?assertMatch(ok, ?call(Worker1, ?MODULE, execute_with_link_context, [LinkReplicaScope, OtherScopes,
            delete_links, [?GLOBALLY_CACHED_LEVEL, Doc, [GetLinkName(I)]]]))
    end,
    DeleteLinkAndCheck = fun(I, LinkReplicaScope, OtherScopes) ->
        DeleteLink(I, LinkReplicaScope, OtherScopes),
        ?assertMatch({error, link_not_found}, ?call(Worker2, ?MODULE, execute_with_link_context, [LinkReplicaScope, OtherScopes,
            fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, GetLinkName(I)]])),
        ?assertMatch({error, link_not_found}, ?call(Worker2, ?MODULE, execute_with_link_context, [LinkReplicaScope, OtherScopes, PModule,
            fetch_link, [ModelConfig, Key, GetLinkName(I)]]), 7)
                 end,
    DeleteLinks = fun(Links, LinkReplicaScope, OtherScopes) ->
        ?assertMatch(ok, ?call(Worker2, ?MODULE, execute_with_link_context, [LinkReplicaScope, OtherScopes,
            delete_links, [?GLOBALLY_CACHED_LEVEL, Doc,
            lists:map(fun(I) -> GetLinkName(I) end, Links)]])),
        lists:map(fun(I) ->
            ?assertMatch({error, link_not_found}, ?call(Worker2, ?MODULE, execute_with_link_context, [LinkReplicaScope, OtherScopes,
                fetch_link, [?GLOBALLY_CACHED_LEVEL, Doc, GetLinkName(I)]])),
            ?assertMatch({error, link_not_found}, ?call(Worker2, ?MODULE, execute_with_link_context, [LinkReplicaScope, OtherScopes, PModule,
                fetch_link, [ModelConfig, Key, GetLinkName(I)]]), 7)
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

execute_with_link_context(LinkReplicaScope, OtherScopes, Op, Args) ->
    execute_with_link_context(LinkReplicaScope, OtherScopes, datastore, Op, Args).

execute_with_link_context(LinkReplicaScope, _OtherScopes, Module, Op, Args) ->
    put(link_replica_scope, LinkReplicaScope),
    apply(Module, Op, Args).

set_link_replica_scope(LinkReplicaScope) ->
    Pid = get(?SCOPE_MASTER_PROC_NAME),
    Pid ! {set_link_replica_scope, self(), LinkReplicaScope},
    receive
        scope_changed -> ok
    end.

scope_master_loop() ->
    scope_master_loop(<<"scope1">>, []).

% TODO - delete other scopes?
scope_master_loop(LinkReplicaScope, OtherScopes) ->
    Todo = receive
               {get_link_replica_scope, Sender} ->
                   Sender ! {link_replica_scope, LinkReplicaScope};
               {set_link_replica_scope, Sender3, LinkReplicaScope2} ->
                   Sender3 ! scope_changed,
                   {change_scopes, LinkReplicaScope2, OtherScopes};
               stop ->
                   stop
           end,
    case Todo of
        stop ->
            ok;
        {change_scopes, LinkReplicaScope3, OtherScopes3} ->
            scope_master_loop(LinkReplicaScope3, OtherScopes3);
        _ ->
            scope_master_loop(LinkReplicaScope, OtherScopes)
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

    ?assertMatch({ok, _}, ?call_store(Worker1, TestRecord, create_or_update,
        [Doc, UpdateEntity])),
    ?assertMatch({ok, #document{value = ?test_record_f1(1)}},
        ?call_store(Worker2, TestRecord, get, [Key])),
    ?assertMatch({ok, #document{value = ?test_record_f1(1)}},
        ?call(Worker1, PModule, get, [ModelConfig, Key]), 7),

    ?assertMatch({ok, _}, ?call_store(Worker1, TestRecord, create_or_update,
        [Doc2, UpdateEntity])),
    ?assertMatch({ok, #document{value = ?test_record_f1(2)}},
        ?call_store(Worker2, TestRecord, get, [Key])),
    ?assertMatch({ok, #document{value = ?test_record_f1(2)}},
        ?call(Worker1, PModule, get, [ModelConfig, Key]), 7),

    ?assertMatch(ok, ?call(Worker1, caches_controller, clear, [?GLOBAL_ONLY_LEVEL, TestRecord, Key])),
    ?assertMatch({ok, _}, ?call_store(Worker1, TestRecord, create_or_update,
        [Doc2, UpdateEntity2])),
    ?assertMatch({ok, #document{value = ?test_record_f1(3)}},
        ?call_store(Worker2, TestRecord, get, [Key])),
    ?assertMatch({ok, #document{value = ?test_record_f1(3)}},
        ?call(Worker1, PModule, get, [ModelConfig, Key]), 7),
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

    ?assertMatch({ok, _}, ?call_store(Worker1, TestRecord, create_or_update, [Doc, UpdateEntity])),
    ?assertMatch({ok, #document{value = ?test_record_f1(1)}},
        ?call_store(Worker2, TestRecord, get, [Key])),

    ?assertMatch({ok, _}, ?call_store(Worker1, TestRecord, create_or_update, [Doc2, UpdateEntity])),
    ?assertMatch({ok, #document{value = ?test_record_f1(2)}},
        ?call_store(Worker2, TestRecord, get, [Key])),
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
        ?assertMatch(ok, ?call_store(Worker1, TestRecord, add_links, [
            Doc, [{GetLinkName(I), LinkedDoc}]
        ]))
    end),

    CheckLinks = fun(Start, End, Sum) ->
        for(Start, End, fun(I) ->
            ?assertMatch({ok, _}, ?call_store(Worker1, TestRecord, fetch_link,
                [Doc, GetLinkName(I)]))
        end),
        for(Start, End, fun(I) ->
            ?assertMatch({ok, _}, ?call_store(Worker2, TestRecord, fetch_link,
                [Doc, GetLinkName(I)]))
        end),

        AccFun = fun(LinkName, _, Acc) ->
            [LinkName | Acc]
        end,
        {_, ListedLinks} = FLAns = ?call_store(Worker1, TestRecord, foreach_link,
            [Doc, AccFun, []]),
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
        ?assertMatch(ok, ?call_store(Worker1, TestRecord, add_links, [
            Doc, [{GetLinkName(I), LinkedDoc}]
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
    ?assertMatch(ok, ?call_store(Worker1, TestRecord, add_links, [
        Doc, AddList
    ])),
    CheckLinks(121, 180, 180),

    ?assertMatch(ok, ?call_store(Worker1, TestRecord, add_links, [
        Doc, AddList
    ])),
    CheckLinks(121, 180, 180),

    for(1, 180, fun(I) ->
        LinkedDoc =  #document{
            key = GetLinkKey(I),
            value = datastore_basic_ops_utils:get_record(TestRecord, I, <<"abc">>, {test, tuple})
        },
        ?assertMatch({error, already_exists}, ?call_store(Worker1, TestRecord,
            create_link, [Doc, {GetLinkName(I), LinkedDoc}
        ]))
    end),

    for(91, 110, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, TestRecord, delete_links,
            [Doc, [GetLinkName(I)]]))
    end),

    DelList = lists:map(fun(I) ->
        GetLinkName(I)
    end, lists:seq(131, 150)),
    ?assertMatch(ok, ?call_store(Worker1, TestRecord, delete_links, [
        Doc, DelList
    ])),
    CheckLinks(1, 90, 140),
    CheckLinks(111, 130, 140),
    CheckLinks(151, 180, 140),

    AddDocs(181, 300),
    AddList2 = lists:map(ToAddList, lists:seq(181, 300)),
    ?assertMatch(ok, ?call_store(Worker1, TestRecord, add_links, [
        Doc, AddList2
    ])),
    CheckLinks(181, 300, 260),

    DelList2 = lists:map(fun(I) ->
        GetLinkName(I)
    end, lists:seq(1, 150)),
    ?assertMatch(ok, ?call_store(Worker1, TestRecord, delete_links, [
        Doc, DelList2
    ])),
    CheckLinks(151, 300, 150),

    ?assertMatch(ok, ?call_store(Worker1, TestRecord, delete_links, [
        Doc, DelList2
    ])),
    CheckLinks(151, 300, 150),

    ?assertMatch(ok, ?call_store(Worker1, TestRecord, delete_links, [Doc, []])),
    CheckLinks(151, 300, 150),

    for(161, 180, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, TestRecord, delete_links,
            [Doc, [GetLinkName(I)]]))
    end),
    for(211, 300, fun(I) ->
        ?assertMatch(ok, ?call_store(Worker1, TestRecord, delete_links,
            [Doc, [GetLinkName(I)]]))
    end),
    CheckLinks(151, 160, 40),
    CheckLinks(181, 210, 40),

    for(1, 150, fun(I) ->
        LinkedDoc =  #document{
            key = GetLinkKey(I),
            value = datastore_basic_ops_utils:get_record(TestRecord, I, <<"abc">>, {test, tuple})
        },
        ?assertMatch(ok, ?call_store(Worker1, TestRecord, create_link, [
            Doc, {GetLinkName(I), LinkedDoc}
        ]))
    end),
    CheckLinks(1, 150, 190),

    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key])),
    ok.

multiple_links_creation_disk_test(Config) ->
    [Worker1, _Worker2] = ?config(cluster_worker_nodes, Config),
    PModule = ?call_store(Worker1, driver_to_module, [?PERSISTENCE_DRIVER]),
    multiple_links_creation_test_base(Config, PModule).

multiple_links_creation_global_only_test(Config) ->
    [Worker1, _Worker2] = ?config(cluster_worker_nodes, Config),
    PModule = ?call_store(Worker1, driver_to_module, [?MEMORY_DRIVER]),
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
            datastore:normalize_link_target(
                model:create_datastore_context(normalize_link_target, ModelConfig),
                Links)]),
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

%% Simple usage of get/update/create/exists/delete on local cache driver (on several nodes)
local_test(Config) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Level = local_only,

    local_access_only(TestRecord, Worker1, Level),
    local_access_only(TestRecord, Worker2, Level),

    ?assertMatch({ok, _},
        ?call_store(Worker1, TestRecord, create, [
            #document{
                key = some_other_key,
                value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
            }])),

    ?assertMatch({ok, false},
        ?call_store(Worker2, TestRecord, exists, [some_other_key])),

    ?assertMatch({ok, true},
        ?call_store(Worker1, TestRecord, exists, [some_other_key])),

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
        ?call_store(Worker1, TestRecord, create, [
            #document{
                key = Key,
                value = datastore_basic_ops_utils:get_record(TestRecord, 0, <<"abc">>, {test, tuple})
            }])),

    Pid = self(),
    AggregatedAns = ?call(Worker2, ?MODULE, update_and_check, [Level, TestRecord, Key, UpdateFun1]),
    ?assertMatch({{ok, Key}, {ok, #document{value = ?test_record_f1_2(0, Pid)}}}, AggregatedAns),

    ?assertMatch({ok, #document{value = ?test_record_f1_2(0, Pid)}},
        ?call_store(Worker1, TestRecord, get, [Key])),

    Self = self(),
    Timeout = timer:seconds(30),
    utils:pforeach(fun(Node) ->
        ?call_store(Node, TestRecord, update, [Key, UpdateFun2]),
        Self ! done
    end, lists:duplicate(100, Worker1) ++ lists:duplicate(100, Worker2)),
    [receive done -> ok after Timeout -> ok end || _ <- lists:seq(1, 200)],

    ?assertMatch({ok, #document{value = ?test_record_f1(200)}},
        ?call_store(Worker1, TestRecord, get, [Key])),

    ok.

update_and_check(Level, TestRecord, Key, UpdateFun) ->
    UpdateAns = datastore:update(
        model:create_datastore_context(update, TestRecord), Key, UpdateFun),
    GetAns = datastore:get(model:create_datastore_context(get, TestRecord), Key),
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

    ?assertMatch(ok, ?call_store(Worker2, TestRecord, add_links,
        [Doc1, [{some, Doc2}, {other, Doc1}]])),
    ?assertMatch(ok, ?call_store(Worker1, TestRecord, add_links,
        [Doc2, [{link, Doc3}, {parent, Doc1}]])),

    Res0 = ?call_store(Worker1, TestRecord, link_walk,
        [Doc1, [some, link], get_leaf]),
    Res1 = ?call_store(Worker2, TestRecord, link_walk,
        [Doc1, [some, parent], get_leaf]),

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

init_per_testcase(links_scope_test = Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:enable_datastore_models(Workers, [link_scopes_test_record]),
    datastore_basic_ops_utils:set_env(Case, Config);
init_per_testcase(links_scope_proc_mem_test = Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:enable_datastore_models(Workers, [link_scopes_test_record2]),
    datastore_basic_ops_utils:set_env(Case, Config);
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

    ?assertMatch({ok, Key}, ?call_store(Node, TestRecord, create, [
        #document{key = Key, value =
            datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
        }
    ])),

    ?assertMatch({error, already_exists}, ?call_store(Node, TestRecord, create, [
        #document{key = Key, value =
            datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
        }
    ])),

    ?assertMatch({ok, true}, ?call_store(Node, TestRecord, exists, [Key])),

    ?assertMatch({ok, #document{value = ?test_record_f3({test, tuple})}},
        ?call_store(Node, TestRecord, get, [Key])),

    Pid = self(),
    ?assertMatch({ok, Key}, ?call_store(Node, TestRecord, update, [
        Key, #{field2 => Pid}
    ])),

    ?assertMatch({ok, #document{value = ?test_record_f2(Pid)}},
        ?call_store(Node, TestRecord, get, [Key])),

    ?assertMatch(ok, ?call_store(Node, TestRecord, delete, [
        Key, fun() -> false end
    ])),

    ?assertMatch({ok, #document{value = ?test_record_f2(Pid)}},
        ?call_store(Node, TestRecord, get, [Key])),

    ?assertMatch(ok, ?call_store(Node, TestRecord, delete, [Key])),

    ?assertMatch({error, {not_found, _}}, ?call_store(Node, TestRecord, get, [
        Key
    ])),

    ?assertMatch({error, {not_found, _}}, ?call_store(Node, TestRecord, update, [
        Key, #{field2 => self()}
    ])),

    ok.


-spec global_access(Config :: term(), Level :: datastore:store_level()) -> ok.
global_access(Config, Level) ->
    [Worker1, Worker2] = ?config(cluster_worker_nodes, Config),
    TestRecord = ?config(test_record, Config),

    Key = rand_key(),

    ?assertMatch({ok, _}, ?call_store(Worker1, TestRecord, create, [#document{
        key = Key, value =
            datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
    }])),

    ?assertMatch({ok, true}, ?call_store(Worker2, TestRecord, exists, [
        Key
    ])),

    ?assertMatch({ok, true}, ?call_store(Worker1, TestRecord, exists, [
        Key
    ])),

    ?assertMatch({error, already_exists}, ?call_store(Worker2, TestRecord, create, [
        #document{key = Key, value =
            datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
        }
    ])),

    ?assertMatch({ok, #document{value = ?test_record_f1_3(1, {test, tuple})}},
        ?call_store(Worker1, TestRecord, get, [Key])),

    ?assertMatch({ok, #document{value = ?test_record_f1_3(1, {test, tuple})}},
        ?call_store(Worker2, TestRecord, get, [Key])),

    ?assertMatch({ok, _}, ?call_store(Worker1, TestRecord, update, [
        Key, #{field1 => 2}
    ])),

    ?assertMatch({ok, #document{value = ?test_record_f1(2)}},
        ?call_store(Worker2, TestRecord, get, [Key])),

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

    ?assertMatch(ok, ?call_store(Worker2, TestRecord, add_links, [
        Doc1, [{link3, Doc2}, {link2, Doc3}]
    ])),

    %% Fetch all links and theirs targets
    Ret0 = ?call_store(Worker2, TestRecord, fetch_link_target, [Doc1, link2]),
    Ret1 = ?call_store(Worker1, TestRecord, fetch_link_target, [Doc1, link3]),
    Ret2 = ?call_store(Worker2, TestRecord, fetch_link, [Doc1, link2]),
    Ret3 = ?call_store(Worker1, TestRecord, fetch_link, [Doc1, link3]),

    ?assertMatch({ok, {Key3, TestRecord}}, Ret2),
    ?assertMatch({ok, {Key2, TestRecord}}, Ret3),
    ?assertMatch({ok, #document{key = Key3, value = ?test_record_f1(3)}}, Ret0),
    ?assertMatch({ok, #document{key = Key2, value = ?test_record_f1(2)}}, Ret1),

    ?assertMatch(ok, ?call_store(Worker2, TestRecord, set_links, [
        Doc1, [{link2, Doc2}, {link3, Doc3}]
    ])),

    %% Fetch all links and theirs targets
    Ret00 = ?call_store(Worker2, TestRecord, fetch_link_target, [Doc1, link2]),
    Ret01 = ?call_store(Worker1, TestRecord, fetch_link_target, [Doc1, link3]),
    Ret02 = ?call_store(Worker2, TestRecord, fetch_link, [Doc1, link2]),
    Ret03 = ?call_store(Worker1, TestRecord, fetch_link, [Doc1, link3]),

    ?assertMatch({ok, {Key2, TestRecord}}, Ret02),
    ?assertMatch({ok, {Key3, TestRecord}}, Ret03),
    ?assertMatch({ok, #document{key = Key2, value = ?test_record_f1(2)}}, Ret00),
    ?assertMatch({ok, #document{key = Key3, value = ?test_record_f1(3)}}, Ret01),


    ?assertMatch(ok, ?call_store(Worker1, TestRecord, delete_links,
        [Doc1, [link2, link3]])),

    ?assertMatch({error, link_not_found}, ?call_store(
        Worker2, TestRecord, fetch_link_target, [Doc1, link2]
    ), 10),
    ?assertMatch({error, link_not_found}, ?call_store(
        Worker1, TestRecord, fetch_link_target, [Doc1, link3]
    ), 10),
    ?assertMatch({error, link_not_found}, ?call_store(
        Worker2, TestRecord, fetch_link, [Doc1, link2]
    ), 10),
    ?assertMatch({error, link_not_found}, ?call_store(
        Worker1, TestRecord, fetch_link, [Doc1, link3]
    )),

    ?assertMatch(ok, ?call_store(Worker2, TestRecord, add_links, [Doc1, [{link2, Doc2}, {link3, Doc3}]])),
    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key2])),
    ?assertMatch(ok, ?call_store(Worker1, TestRecord, delete_links, [Doc1, link3]), 10),

    ?assertMatch({ok, {Key2, TestRecord}}, ?call_store(
        Worker1, TestRecord, fetch_link, [Doc1, link2]
    ), 10),
    ?assertMatch({error, link_not_found}, ?call_store(
        Worker2, TestRecord, fetch_link, [Doc1, link3]
    ), 10),
    ?assertMatch({error, link_not_found}, ?call_store(
        Worker2, TestRecord, fetch_link_target, [Doc1, link3]
    ), 10),
    ?assertMatch({error, {not_found, _}}, ?call_store(
        Worker1, TestRecord, fetch_link_target, [Doc1, link2]
    ), 10),

    %% Delete on document shall delete all its links
    ?assertMatch(ok, ?call(Worker1, TestRecord, delete, [Key1])),

    ?assertMatch({error, link_not_found}, ?call_store(
        Worker2, TestRecord, fetch_link, [Doc1, link2]
    ), 10),

    ok = ?call_store(Worker2, TestRecord, delete, [Key1]),
    ok = ?call_store(Worker2, TestRecord, delete, [Key2]),
    ok = ?call_store(Worker2, TestRecord, delete, [Key3]),

    ok.


%% generic list operation (on several nodes)
generic_list_test(TestRecord, Nodes, Level) ->
    generic_list_test(TestRecord, Nodes, Level, 0, non, 0).

generic_list_test(TestRecord, Nodes, Level, ListRetryAfter, BeforeRetryFun, DelSecondCheckAfter) ->
    Ret0 = ?call_store(rand_node(Nodes), TestRecord, list, [?GET_ALL, []]),
    ?assertMatch({ok, _}, Ret0),
    {ok, Objects0} = Ret0,
    Keys0 = lists:map(fun(#document{key = Key}) -> Key end, Objects0),

    ObjCount = 424,
    Keys = [rand_key() || _ <- lists:seq(1, ObjCount)],

    CreateDocFun = fun(Key) ->
        ?call_store(rand_node(Nodes), TestRecord, create, [
            #document{
                key = Key,
                value = datastore_basic_ops_utils:get_record(TestRecord, 1, <<"abc">>, {test, tuple})
            }])
    end,

    RemoveDocFun = fun(Key) ->
        ?call_store(rand_node(Nodes), TestRecord, delete, [Key, ?PRED_ALWAYS])
    end,

    [?assertMatch({ok, _}, CreateDocFun(Key)) || Key <- Keys],

    ListFun = fun() ->
        Ret1 = ?call_store(rand_node(Nodes), TestRecord, list, [?GET_ALL, []]),
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
        Ret2 = ?call_store(rand_node(Nodes), TestRecord, list, [?GET_ALL, []]),
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

for(N, N, F) ->
    F(N);
for(I, N, F) ->
    F(I),
    for(I + 1, N, F).

disable_cache_control_and_set_dump_delay(Workers, Delay) ->
    lists:foreach(fun(W) ->
        ?assertEqual(ok, gen_server:call({?NODE_MANAGER_NAME, W}, disable_cache_control)),
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, Delay)),
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, mem_clearing_ratio_to_stop, 0))
    end, Workers).
