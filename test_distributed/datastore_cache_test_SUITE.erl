%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore cache tests.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_cache_test_SUITE).
-author("Krzysztof Trzepla").

-include("datastore_test_utils.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2]).

%% tests
-export([
    get_should_return_value_from_memory/1,
    get_should_return_missing_error/1,
    fetch_should_return_value_from_memory/1,
    fetch_should_return_value_from_disc/1,
    fetch_should_return_value_from_disc_when_memory_driver_undefined/1,
    fetch_should_return_value_from_memory_when_disc_driver_undefined/1,
    fetch_should_save_value_in_memory/1,
    fetch_should_return_value_from_memory_and_disc/1,
    fetch_should_return_missing_error/1,
    fetch_should_return_missing_error_when_disc_driver_undefined/1,
    save_should_save_value_in_memory/1,
    save_should_save_value_on_disc/1,
    save_should_save_value_on_disc_when_memory_driver_undefined/1,
    save_should_save_value_in_memory_and_on_disc/1,
    save_should_overwrite_value_in_memory/1,
    save_should_not_overwrite_value_in_memory/1,
    save_should_save_value_in_memory_when_disc_driver_undefined/1,
    save_should_return_no_memory_error_when_disc_driver_undefined/1,
    save_volatile_should_be_overwritten/1,
    parallel_save_should_not_overflow_cache_size/1,
    flush_should_save_value_on_disc/1,
    flush_should_return_missing_error/1,
    mark_active_should_activate_new_entry/1,
    mark_active_should_fail_on_full_cache/1,
    mark_active_should_ignore_active_entry/1,
    mark_inactive_should_deactivate_active_entry/1,
    mark_inactive_should_deactivate_deleted_entry/1,
    mark_inactive_should_not_deactivate_not_deleted_entry/1,
    mark_inactive_should_ignore_not_active_entry/1,
    mark_inactive_should_enable_entries_removal/1,
    mark_active_should_reactivate_inactive_entry/1,
    mark_active_should_remove_inactive_entry/1
]).

all() ->
    ?ALL([
        get_should_return_value_from_memory,
        get_should_return_missing_error,
        fetch_should_return_value_from_memory,
        fetch_should_return_value_from_disc,
        fetch_should_return_value_from_disc_when_memory_driver_undefined,
        fetch_should_return_value_from_memory_when_disc_driver_undefined,
        fetch_should_save_value_in_memory,
        fetch_should_return_value_from_memory_and_disc,
        fetch_should_return_missing_error,
        fetch_should_return_missing_error_when_disc_driver_undefined,
        save_should_save_value_in_memory,
        save_should_save_value_on_disc,
        save_should_save_value_on_disc_when_memory_driver_undefined,
        save_should_save_value_in_memory_and_on_disc,
        save_should_overwrite_value_in_memory,
        save_should_not_overwrite_value_in_memory,
        save_should_save_value_in_memory_when_disc_driver_undefined,
        save_should_return_no_memory_error_when_disc_driver_undefined,
        save_volatile_should_be_overwritten,
        parallel_save_should_not_overflow_cache_size,
        flush_should_save_value_on_disc,
        flush_should_return_missing_error,
        mark_active_should_activate_new_entry,
        mark_active_should_fail_on_full_cache,
        mark_active_should_ignore_active_entry,
        mark_inactive_should_deactivate_active_entry,
        mark_inactive_should_deactivate_deleted_entry,
        mark_inactive_should_not_deactivate_not_deleted_entry,
        mark_inactive_should_ignore_not_active_entry,
        mark_inactive_should_enable_entries_removal,
        mark_active_should_reactivate_inactive_entry,
        mark_active_should_remove_inactive_entry
    ]).

-define(MODEL, ets_cached_model).
-define(MEM_CTX, ?MEM_CTX(?MODEL)).
-define(CTX, #{
    mutator_pid => self(),
    memory_driver => ?MEM_DRV(?MODEL),
    memory_driver_ctx => ?MEM_CTX(?MODEL),
    disc_driver => ?DISC_DRV(?MODEL),
    disc_driver_ctx => ?DISC_CTX,
    remote_driver => ?REMOTE_DRV
}).
-define(VALUE, ?MODEL_VALUE(?MODEL, 1)).
-define(DOC, ?DOC(1)).
-define(DOC(N), ?BASE_DOC(?KEY(N), ?VALUE)).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_should_return_value_from_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?MEM_DRV, save, [?MEM_CTX, ?KEY, ?DOC]),
    ?assertMatch({ok, #document{}},
        rpc:call(Worker, datastore_cache, get, [?CTX, ?KEY])
    ).

get_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, not_found},
        rpc:call(Worker, datastore_cache, get, [?CTX, ?KEY])
    ),
    ?assertEqual({error, not_found},
        rpc:call(Worker, datastore_cache, get, [
            ?CTX#{memory_driver => undefined}, ?KEY
        ])
    ).

fetch_should_return_value_from_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?MEM_DRV, save, [?MEM_CTX, ?KEY, ?DOC]),
    ?assertMatch({ok, memory, #document{}},
        rpc:call(Worker, datastore_cache, fetch, [?CTX, ?KEY])
    ).

fetch_should_return_value_from_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?DISC_DRV, save, [?DISC_CTX, ?KEY, ?DOC]),
    ?assertMatch({ok, disc, #document{}},
        rpc:call(Worker, datastore_cache, fetch, [?CTX, ?KEY])
    ).

fetch_should_return_value_from_disc_when_memory_driver_undefined(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?MEM_DRV, save, [?MEM_CTX, ?KEY, ?DOC]),
    rpc:call(Worker, ?DISC_DRV, save, [?DISC_CTX, ?KEY, ?DOC]),
    ?assertMatch({ok, disc, #document{}},
        rpc:call(Worker, datastore_cache, fetch, [
            ?CTX#{memory_driver => undefined}, ?KEY
        ])
    ).

fetch_should_return_value_from_memory_when_disc_driver_undefined(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?MEM_DRV, save, [?MEM_CTX, ?KEY, ?DOC]),
    rpc:call(Worker, ?DISC_DRV, save, [?DISC_CTX, ?KEY, ?DOC]),
    ?assertMatch({ok, memory, #document{}},
        rpc:call(Worker, datastore_cache, fetch, [
            ?CTX#{disc_driver => undefined}, ?KEY
        ])
    ).

fetch_should_save_value_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?DISC_DRV, save, [?DISC_CTX, ?KEY, ?DOC]),
    ?assertMatch({ok, memory, #document{}},
        rpc:call(Worker, datastore_cache, fetch, [?CTX, ?KEY])
    ),
    ?assertMatch({ok, #document{}},
        rpc:call(Worker, datastore_cache, get, [?CTX, ?KEY])
    ).

fetch_should_return_value_from_memory_and_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?MEM_DRV, save, [?MEM_CTX, ?KEY(1), ?DOC(1)]),
    rpc:call(Worker, ?DISC_DRV, save, [?DISC_CTX, ?KEY(2), ?DOC(2)]),
    ?assertMatch([
        {ok, memory, #document{}},
        {ok, memory, #document{}},
        {error, not_found}
    ],
        rpc:call(Worker, datastore_cache, fetch, [?CTX,
            [?KEY(1), ?KEY(2), ?KEY(3)]
        ])
    ).

fetch_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, not_found},
        rpc:call(Worker, datastore_cache, fetch, [?CTX, ?KEY])
    ).

fetch_should_return_missing_error_when_disc_driver_undefined(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?DISC_DRV, save, [?DISC_CTX, ?KEY, ?DOC]),
    ?assertEqual({error, not_found},
        rpc:call(Worker, datastore_cache, fetch, [
            ?CTX#{disc_driver => undefined}, ?KEY
        ])
    ).

save_should_save_value_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, memory, #document{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?KEY, ?DOC])
    ).

save_should_save_value_on_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, disc, #document{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?KEY, ?DOC])
    ).

save_should_save_value_on_disc_when_memory_driver_undefined(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, disc, #document{}},
        rpc:call(Worker, datastore_cache, save, [
            ?CTX#{memory_driver => undefined}, ?KEY, ?DOC
        ])
    ).

save_should_save_value_in_memory_and_on_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch([
        {ok, memory, #document{}},
        {ok, disc, #document{}}
    ],
        rpc:call(Worker, datastore_cache, save, [[
            {?CTX, ?KEY(1), ?DOC(1)},
            {?CTX, ?KEY(2), ?DOC(2)}
        ]])
    ).

save_should_overwrite_value_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, memory, #document{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?KEY(1), ?DOC(1)])
    ),
    ?assertMatch({ok, disc, #document{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?KEY(2), ?DOC(2)])
    ),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [disc, ?KEY(1)]
    )),
    ?assertMatch({ok, memory, #document{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?KEY(2), ?DOC(2)])
    ),
    ?assertEqual({error, not_found},
        rpc:call(Worker, datastore_cache, get, [?CTX, ?KEY(1)])
    ),
    ?assertMatch({ok, disc, #document{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?KEY(1), ?DOC(1)])
    ).

save_should_not_overwrite_value_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, memory, #document{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?KEY(1), ?DOC(1)])
    ),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [disc, ?KEY(1)]
    )),
    ?assertMatch({ok, memory, #document{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?KEY(2), ?DOC(2)])
    ),
    ?assertMatch({ok, #document{}},
        rpc:call(Worker, datastore_cache, get, [?CTX, ?KEY(1)])
    ).

save_should_save_value_in_memory_when_disc_driver_undefined(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, memory, #document{}},
        rpc:call(Worker, datastore_cache, save, [
            ?CTX#{disc_driver => undefined}, ?KEY, ?DOC
        ])
    ).

save_should_return_no_memory_error_when_disc_driver_undefined(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, enomem},
        rpc:call(Worker, datastore_cache, save, [
            ?CTX#{disc_driver => undefined}, ?KEY, ?DOC
        ])
    ).

save_volatile_should_be_overwritten(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Ctx = ?CTX#{disc_driver => undefined},
    ?assertMatch({ok, memory, #document{}},
        rpc:call(Worker, datastore_cache, save, [
            Ctx#{volatile => true}, ?KEY(1), ?DOC(1)
        ])
    ),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [memory, ?KEY(1)]
    )),
    ?assertMatch({ok, memory, #document{}},
        rpc:call(Worker, datastore_cache, save, [Ctx, ?KEY(2), ?DOC(2)])
    ),
    ?assertMatch({error, not_found},
        rpc:call(Worker, datastore_cache, get, [Ctx, ?KEY(1)])
    ),
    ?assertMatch({ok, #document{}},
        rpc:call(Worker, datastore_cache, get, [Ctx, ?KEY(2)])
    ).

parallel_save_should_not_overflow_cache_size(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ets, delete_all_objects, [?TABLE(?MODEL)]),
    ThrNum = 1000,
    Results = utils:pmap(fun(Doc = #document{key = Key}) ->
        timer:sleep(rand:uniform(100)),
        case rpc:call(Worker, datastore_cache, save, [?CTX, Key, Doc]) of
            {ok, memory, #document{}} ->
                ?assertEqual(true, rpc:call(Worker, datastore_cache_manager,
                    mark_inactive, [disc, Key]
                )),
                ok;
            {ok, disc, #document{}} ->
                ok
        end
    end, [?DOC(N) || N <- lists:seq(1, ThrNum)]),
    ?assert(lists:all(fun(Result) -> Result =:= ok end, Results)),
    ?assertEqual(cache_size(?FUNCTION_NAME), rpc:call(Worker,
        datastore_cache_manager, get_size, [disc])),
    ?assertEqual(cache_size(?FUNCTION_NAME), length(rpc:call(Worker,
        ets, tab2list, [?TABLE(?MODEL)]))).

flush_should_save_value_on_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?MEM_DRV, save, [?MEM_CTX, ?KEY, ?DOC]),
    ?assertMatch({ok, #document{}},
        rpc:call(Worker, datastore_cache, flush, [?CTX, ?KEY])
    ).

flush_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, not_found},
        rpc:call(Worker, datastore_cache, flush, [?CTX, ?KEY])
    ).

mark_active_should_activate_new_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_active,
        [disc, ?CTX, ?KEY]
    )).

mark_active_should_fail_on_full_cache(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(false, rpc:call(Worker, datastore_cache_manager, mark_active,
        [disc, ?CTX, ?KEY]
    )).

mark_active_should_ignore_active_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_cache, save, [?CTX, ?KEY, ?DOC]),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_active,
        [disc, ?CTX, ?KEY]
    )).

mark_inactive_should_deactivate_active_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_cache, save, [?CTX, ?KEY, ?DOC]),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [disc, ?KEY]
    )).

mark_inactive_should_deactivate_deleted_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_cache, save, [
        ?CTX#{disc_driver => undefined}, ?KEY, ?DOC#document{deleted = true}
    ]),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [memory, ?KEY]
    )).

mark_inactive_should_not_deactivate_not_deleted_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_cache, save, [?CTX, ?KEY, ?DOC]),
    ?assertEqual(false, rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [memory, ?KEY]
    )).

mark_inactive_should_ignore_not_active_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(false, rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [disc, ?KEY]
    )).

mark_inactive_should_enable_entries_removal(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ok = rpc:call(Worker, erlang, apply, [fun(Items) ->
        ?assertMatch([
            {ok, memory, _},
            {ok, memory, _},
            {ok, memory, _}
        ], datastore_cache:save(Items)),
        {Ctxs, Keys, _} = lists:unzip3(Items),
        lists:foreach(fun({Ctx, Key}) ->
            ?assertEqual(true, datastore_cache:inactivate(Ctx, Key))
        end, lists:zip(Ctxs, Keys))
    end, [[
        {?CTX, ?KEY(1), ?DOC(1)},
        {?CTX, ?KEY(2), ?DOC(2)},
        {?CTX, ?KEY(3), ?DOC(3)}
    ]]]),
    lists:foreach(fun(N) ->
        ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_active,
            [disc, ?CTX, ?KEY(N)]
        ))
    end, lists:seq(4, 6)),
    lists:foreach(fun(N) ->
        ?assertEqual({error, not_found},
            rpc:call(Worker, datastore_cache, get, [?CTX, ?KEY(N)])
        )
    end, lists:seq(1, 3)).

mark_active_should_reactivate_inactive_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_cache, save, [?CTX, ?KEY, ?DOC]),
    rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [disc, ?KEY]
    ),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_active,
        [disc, ?CTX, ?KEY]
    )).

mark_active_should_remove_inactive_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_cache, save, [?CTX, ?KEY(1), ?DOC(1)]),
    rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [disc, ?KEY(1)]
    ),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_active,
        [disc, ?CTX, ?KEY(2)]
    )),
    ?assertEqual({error, not_found},
        rpc:call(Worker, datastore_cache, get, [?CTX, ?KEY(1)])
    ).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite([?MODEL], Config).

init_per_testcase(Case, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_cache_manager, reset, [memory, cache_size(Case)]),
    rpc:call(Worker, datastore_cache_manager, reset, [disc, cache_size(Case)]),
    Config.

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

cache_size(fetch_should_return_value_from_disc) -> 0;
cache_size(save_should_save_value_on_disc) -> 0;
cache_size(save_should_save_value_in_memory_and_on_disc) -> 1;
cache_size(save_should_overwrite_value_in_memory) -> 1;
cache_size(save_volatile_should_be_overwritten) -> 1;
cache_size(save_should_return_no_memory_error_when_disc_driver_undefined) -> 0;
cache_size(parallel_save_should_not_overflow_cache_size) -> 10;
cache_size(update_should_get_value_from_disc_and_save_on_disc) -> 0;
cache_size(delete_should_save_value_on_disc) -> 0;
cache_size(mark_active_should_fail_on_full_cache) -> 0;
cache_size(mark_inactive_should_enable_entries_removal) -> 3;
cache_size(mark_active_should_remove_inactive_entry) -> 1;
cache_size(_Case) -> 10000.