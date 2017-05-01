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

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    get_should_return_value_from_memory/1,
    get_should_return_missing_error/1,
    fetch_should_return_value_from_memory/1,
    fetch_should_return_value_from_disc/1,
    fetch_should_save_value_in_memory/1,
    fetch_should_return_value_from_memory_and_disc/1,
    save_should_save_value_in_memory/1,
    save_should_save_value_on_disc/1,
    save_should_save_value_in_memory_and_on_disc/1,
    save_should_overwrite_value_in_memory/1,
    save_should_not_overwrite_value_in_memory/1,
    update_should_get_value_from_memory_and_save_in_memory/1,
    update_should_get_value_from_disc_and_save_in_memory/1,
    update_should_get_value_from_disc_and_save_on_disc/1,
    update_should_return_missing_error/1,
    update_should_pass_error/1,
    delete_should_save_value_in_memory/1,
    delete_should_save_value_on_disc/1,
    flush_should_save_value_on_disc/1,
    flush_should_return_missing_error/1,
    mark_active_should_activate_new_entry/1,
    mark_active_should_fail_on_full_cache/1,
    mark_active_should_ignore_active_entry/1,
    mark_inactive_should_deactivate_active_entry/1,
    mark_inactive_should_ignore_not_active_entry/1,
    mark_active_should_reactivate_inactive_entry/1,
    mark_active_should_remove_inactive_entry/1,
    resize_should_block_mark_active/1,
    resize_should_remove_inactive_entry/1
]).

all() ->
    ?ALL([
        get_should_return_value_from_memory,
        get_should_return_missing_error,
        fetch_should_return_value_from_memory,
        fetch_should_return_value_from_disc,
        fetch_should_save_value_in_memory,
        fetch_should_return_value_from_memory_and_disc,
        save_should_save_value_in_memory,
        save_should_save_value_on_disc,
        save_should_save_value_in_memory_and_on_disc,
        save_should_overwrite_value_in_memory,
        save_should_not_overwrite_value_in_memory,
        update_should_get_value_from_memory_and_save_in_memory,
        update_should_get_value_from_disc_and_save_in_memory,
        update_should_get_value_from_disc_and_save_on_disc,
        update_should_return_missing_error,
        update_should_pass_error,
        delete_should_save_value_in_memory,
        delete_should_save_value_on_disc,
        flush_should_save_value_on_disc,
        flush_should_return_missing_error,
        mark_active_should_activate_new_entry,
        mark_active_should_fail_on_full_cache,
        mark_active_should_ignore_active_entry,
        mark_inactive_should_deactivate_active_entry,
        mark_inactive_should_ignore_not_active_entry,
        mark_active_should_reactivate_inactive_entry,
        mark_active_should_remove_inactive_entry,
        resize_should_block_mark_active,
        resize_should_remove_inactive_entry
    ]).

-record(test_model, {
    field :: binary()
}).

-define(MODEL, test_model).
-define(BUCKET, <<"default">>).
-define(MEM_DRV, ets_driver).
-define(MEM_CTX, #{table => ?FUNCTION_NAME}).
-define(DISC_DRV, couchbase_driver).
-define(DISC_CTX, #{bucket => ?BUCKET}).
-define(CTX, #{
    memory_driver => ?MEM_DRV,
    memory_driver_ctx => ?MEM_CTX,
    disc_driver => ?DISC_DRV,
    disc_driver_ctx => ?DISC_CTX
}).
-define(KEY, ?KEY(1)).
-define(KEY(N), <<"key-", (atom_to_binary(?FUNCTION_NAME, utf8))/binary,
    "-", (integer_to_binary(N))/binary>>).
-define(DOC, ?DOC(1)).
-define(DOC(N), #document2{
    key = ?KEY(N),
    value = #test_model{field = <<"1">>}
}).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_should_return_value_from_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?MEM_DRV, save, [?MEM_CTX, ?DOC]),
    ?assertMatch({ok, #document2{}},
        rpc:call(Worker, datastore_cache, get, [?CTX, ?KEY])
    ).

get_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, key_enoent},
        rpc:call(Worker, datastore_cache, get, [?CTX, ?KEY])
    ).

fetch_should_return_value_from_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?MEM_DRV, save, [?MEM_CTX, ?DOC]),
    ?assertMatch({ok, memory, #document2{}},
        rpc:call(Worker, datastore_cache, fetch, [?CTX, ?KEY])
    ).

fetch_should_return_value_from_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?DISC_DRV, save, [?DISC_CTX, ?DOC]),
    ?assertMatch({ok, disc, #document2{}},
        rpc:call(Worker, datastore_cache, fetch, [?CTX, ?KEY])
    ).

fetch_should_save_value_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?DISC_DRV, save, [?DISC_CTX, ?DOC]),
    ?assertMatch({ok, memory, #document2{}},
        rpc:call(Worker, datastore_cache, fetch, [?CTX, ?KEY])
    ),
    ?assertMatch({ok, #document2{}},
        rpc:call(Worker, datastore_cache, get, [?CTX, ?KEY])
    ).

fetch_should_return_value_from_memory_and_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?MEM_DRV, save, [?MEM_CTX, ?DOC(1)]),
    rpc:call(Worker, ?DISC_DRV, save, [?DISC_CTX, ?DOC(2)]),
    ?assertMatch([
        {ok, memory, #document2{}},
        {ok, memory, #document2{}},
        {error, key_enoent}
    ],
        rpc:call(Worker, datastore_cache, fetch, [?CTX,
            [?KEY(1), ?KEY(2), ?KEY(3)]
        ])
    ).

save_should_save_value_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, memory, #document2{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?DOC])
    ).

save_should_save_value_on_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, disc, #document2{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?DOC])
    ).

save_should_save_value_in_memory_and_on_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch([
        {ok, memory, #document2{}},
        {ok, disc, #document2{}}
    ],
        rpc:call(Worker, datastore_cache, save, [?CTX, [?DOC(1), ?DOC(2)]])
    ).

save_should_overwrite_value_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, memory, #document2{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?DOC(1)])
    ),
    ?assertMatch({ok, disc, #document2{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?DOC(2)])
    ),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [?KEY(1), ?MEM_DRV, ?MEM_CTX]
    )),
    ?assertMatch({ok, memory, #document2{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?DOC(2)])
    ),
    ?assertEqual({error, key_enoent},
        rpc:call(Worker, datastore_cache, get, [?CTX, ?KEY(1)])
    ),
    ?assertMatch({ok, disc, #document2{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?DOC(1)])
    ).

save_should_not_overwrite_value_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, memory, #document2{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?DOC(1)])
    ),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [?KEY(1), ?MEM_DRV, ?MEM_CTX]
    )),
    ?assertMatch({ok, #document2{}},
        rpc:call(Worker, datastore_cache, get, [?CTX, ?KEY(1)])
    ),
    ?assertMatch({ok, memory, #document2{}},
        rpc:call(Worker, datastore_cache, save, [?CTX, ?DOC(2)])
    ).

update_should_get_value_from_memory_and_save_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?MEM_DRV, save, [?MEM_CTX, ?DOC]),
    ?assertMatch({ok, memory, #document2{value = #test_model{field = <<"2">>}}},
        rpc:call(Worker, datastore_cache, update, [?CTX, ?KEY, fun(Doc) ->
            {ok, Doc#document2{value = #test_model{field = <<"2">>}}}
        end])
    ).

update_should_get_value_from_disc_and_save_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?DISC_DRV, save, [?DISC_CTX, ?DOC]),
    ?assertMatch({ok, memory, #document2{value = #test_model{field = <<"2">>}}},
        rpc:call(Worker, datastore_cache, update, [?CTX, ?KEY, fun(Doc) ->
            {ok, Doc#document2{value = #test_model{field = <<"2">>}}}
        end])
    ).

update_should_get_value_from_disc_and_save_on_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?DISC_DRV, save, [?DISC_CTX, ?DOC]),
    ?assertMatch({ok, disc, #document2{value = #test_model{field = <<"2">>}}},
        rpc:call(Worker, datastore_cache, update, [?CTX, ?KEY, fun(Doc) ->
            {ok, Doc#document2{value = #test_model{field = <<"2">>}}}
        end])
    ).

update_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, key_enoent},
        rpc:call(Worker, datastore_cache, update, [?CTX, ?KEY, fun(Doc) ->
            {ok, Doc#document2{value = #test_model{field = <<"2">>}}}
        end])
    ).

update_should_pass_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?DISC_DRV, save, [?DISC_CTX, ?DOC]),
    ?assertEqual({error, aborted},
        rpc:call(Worker, datastore_cache, update, [?CTX, ?KEY, fun(_Doc) ->
            {error, aborted}
        end])
    ).

delete_should_save_value_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, memory, #document2{deleted = true}},
        rpc:call(Worker, datastore_cache, delete, [?CTX, ?DOC])
    ).

delete_should_save_value_on_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, disc, #document2{deleted = true}},
        rpc:call(Worker, datastore_cache, delete, [?CTX, ?DOC])
    ).

flush_should_save_value_on_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, ?MEM_DRV, save, [?MEM_CTX, ?DOC]),
    ?assertMatch({ok, #document2{}},
        rpc:call(Worker, datastore_cache, flush, [?CTX, ?KEY])
    ).

flush_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, key_enoent},
        rpc:call(Worker, datastore_cache, flush, [?CTX, ?KEY])
    ).

mark_active_should_activate_new_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_active,
        [?KEY]
    )).

mark_active_should_fail_on_full_cache(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(false, rpc:call(Worker, datastore_cache_manager, mark_active,
        [?KEY]
    )).

mark_active_should_ignore_active_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_cache, save, [?CTX, ?DOC]),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_active,
        [?KEY]
    )).

mark_inactive_should_deactivate_active_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_cache, save, [?CTX, ?DOC]),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [?KEY, ?MEM_DRV, ?MEM_CTX]
    )).

mark_inactive_should_ignore_not_active_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(false, rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [?KEY, ?MEM_DRV, ?MEM_CTX]
    )).

mark_active_should_reactivate_inactive_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_cache, save, [?CTX, ?DOC]),
    rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [?KEY, ?MEM_DRV, ?MEM_CTX]
    ),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_active,
        [?KEY]
    )).

mark_active_should_remove_inactive_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_cache, save, [?CTX, ?DOC(1)]),
    rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [?KEY(1), ?MEM_DRV, ?MEM_CTX]
    ),
    ?assertEqual(true, rpc:call(Worker, datastore_cache_manager, mark_active,
        [?KEY(2)]
    )),
    ?assertEqual({error, key_enoent},
        rpc:call(Worker, datastore_cache, get, [?CTX, ?KEY(1)])
    ).

resize_should_block_mark_active(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, datastore_cache_manager, resize, [0])),
    ?assertEqual(false, rpc:call(Worker, datastore_cache_manager, mark_active,
        [?KEY]
    )).

resize_should_remove_inactive_entry(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_cache, save, [?CTX, [
        ?DOC(1), ?DOC(2), ?DOC(3)
    ]]),
    rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [?KEY(1), ?MEM_DRV, ?MEM_CTX]
    ),
    rpc:call(Worker, datastore_cache_manager, mark_inactive,
        [?KEY(3), ?MEM_DRV, ?MEM_CTX]
    ),
    ?assertEqual(ok, rpc:call(Worker, datastore_cache_manager, resize, [0])),
    ?assertMatch([
        {error, key_enoent},
        {ok, #document2{}},
        {error, key_enoent}
    ],
        rpc:call(Worker, datastore_cache, get, [?CTX, [
            ?KEY(1), ?KEY(2), ?KEY(3)
        ]])
    ).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_testcase(Case, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Ctx = #{table => proplists:get_value(table, Config, Case)},
    spawn_link(Worker, fun() ->
        ?MEM_DRV:init(Ctx, []),
        receive _ -> ok end
    end),
    rpc:call(Worker, datastore_cache_manager, reset, []),
    rpc:call(Worker, datastore_cache_manager, resize, [cache_size(Case)]),
    test_utils:mock_new(Worker, ?MODEL, [passthrough, non_strict]),
    test_utils:mock_expect(Worker, ?MODEL, model_init, fun() ->
        #model_config{version = 1}
    end),
    test_utils:mock_expect(Worker, ?MODEL, record_struct, fun(1) ->
        {record, [{field, string}]}
    end),
    Config.

end_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Worker, ?MODEL).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

cache_size(fetch_should_return_value_from_disc) -> 0;
cache_size(save_should_save_value_on_disc) -> 0;
cache_size(save_should_save_value_in_memory_and_on_disc) -> 1;
cache_size(save_should_overwrite_value_in_memory) -> 1;
cache_size(update_should_get_value_from_disc_and_save_on_disc) -> 0;
cache_size(delete_should_save_value_on_disc) -> 0;
cache_size(mark_active_should_fail_on_full_cache) -> 0;
cache_size(mark_active_should_remove_inactive_entry) -> 1;
cache_size(_Case) -> 10000.