%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains Mnesia driver tests.
%%% @end
%%%-------------------------------------------------------------------
-module(mnesia_driver_test_SUITE).
-author("Krzysztof Trzepla").

-include("datastore_test_utils.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2]).

%% tests
-export([
    save_should_return_doc/1,
    get_should_return_doc/1,
    get_should_return_missing_error/1,
    update_should_change_doc/1,
    delete_should_remove_doc/1,
    save_get_delete_should_return_success/1
]).

%% test_bases
-export([
    save_get_delete_should_return_success_base/1
]).

all() ->
    ?ALL([
        save_should_return_doc,
        get_should_return_doc,
        get_should_return_missing_error,
        update_should_change_doc,
        delete_should_remove_doc,
        save_get_delete_should_return_success
    ], [
        save_get_delete_should_return_success
    ]).

-define(MODEL, mnesia_only_model).
-define(CTX, ?MEM_CTX(?MODEL)).
-define(CTX(Key), datastore_multiplier:extend_name(?KEY, ?CTX)).
-define(VALUE, ?VALUE(1)).
-define(VALUE(N), ?MODEL_VALUE(?MODEL, N)).
-define(DOC, ?DOC(1)).
-define(DOC(N), ?BASE_DOC(?KEY(N), ?VALUE)).

%%%===================================================================
%%% Test functions
%%%===================================================================

save_should_return_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, Doc} = ?assertMatch({ok, _}, rpc:call(Worker, mnesia_driver, save, [
        ?CTX(?KEY), ?KEY, ?DOC
    ])),
    ?assertEqual(?KEY, Doc#document.key),
    ?assertEqual(?VALUE, Doc#document.value),
    ?assertEqual(?SCOPE, Doc#document.scope),
    ?assertEqual([], Doc#document.mutators),
    ?assertMatch([<<_/binary>>], Doc#document.revs),
    ?assertEqual(null, Doc#document.seq),
    ?assertEqual(null, Doc#document.timestamp),
    ?assertEqual(false, Doc#document.deleted),
    ?assertEqual(1, Doc#document.version).

get_should_return_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, Doc} = ?assertMatch({ok, _}, rpc:call(Worker, mnesia_driver, save, [
        ?CTX(?KEY), ?KEY, ?DOC
    ])),
    ?assertEqual({ok, Doc}, rpc:call(Worker, mnesia_driver, get, [
        ?CTX(?KEY), ?KEY
    ])).

get_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, not_found}, rpc:call(Worker, mnesia_driver, get, [
        ?CTX(?KEY), ?KEY
    ])).

update_should_change_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, Doc} = ?assertMatch({ok, _}, rpc:call(Worker, mnesia_driver, save, [
        ?CTX(?KEY), ?KEY, ?DOC
    ])),
    Value = ?VALUE(2),
    {ok, Doc2} = ?assertMatch({ok, _}, rpc:call(Worker, mnesia_driver, save, [
        ?CTX(?KEY), ?KEY, ?DOC#document{value = Value}
    ])),
    ?assertEqual(?KEY, Doc2#document.key),
    ?assertEqual(Value, Doc2#document.value),
    ?assertEqual(?SCOPE, Doc2#document.scope),
    ?assertEqual([], Doc#document.mutators),
    ?assertMatch([<<_/binary>>], Doc#document.revs),
    ?assertEqual(null, Doc#document.seq),
    ?assertEqual(null, Doc#document.timestamp),
    ?assertEqual(false, Doc2#document.deleted),
    ?assertEqual(1, Doc2#document.version).

delete_should_remove_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, _}, rpc:call(Worker, mnesia_driver, save, [
        ?CTX(?KEY), ?KEY, ?DOC
    ])),
    ?assertEqual(ok, rpc:call(Worker, mnesia_driver, delete, [?CTX(?KEY), ?KEY])),
    ?assertEqual({error, not_found}, rpc:call(Worker, mnesia_driver, get, [
        ?CTX(?KEY), ?KEY
    ])).

save_get_delete_should_return_success(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [?OPS_NUM(100)]},
        {description, "Multiple cycles of parallel save/get/delete operations."},
        ?PERF_CFG(small, [?OPS_NUM(10000)]),
        ?PERF_CFG(medium, [?OPS_NUM(100000)]),
        ?PERF_CFG(large, [?OPS_NUM(1000000)])
    ]).
save_get_delete_should_return_success_base(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    OpsNum = ?config(ops_num, Config),

    lists:foreach(fun(N) ->
        ?assertMatch({ok, #document{}}, rpc:call(Worker, mnesia_driver, save, [
            ?CTX(?KEY(N)), ?KEY(N), ?DOC(N)
        ]))
    end, lists:seq(1, OpsNum)),

    lists:foreach(fun(N) ->
        ?assertMatch({ok, #document{}}, rpc:call(Worker, mnesia_driver, get, [
            ?CTX(?KEY(N)), ?KEY(N)
        ]))
    end, lists:seq(1, OpsNum)),

    lists:foreach(fun(N) ->
        ?assertEqual(ok, rpc:call(Worker, mnesia_driver, delete, [
            ?CTX(?KEY(N)), ?KEY(N)
        ]))
    end, lists:seq(1, OpsNum)).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite([?MODEL], Config).

init_per_testcase(_, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    application:load(cluster_worker),
    application:set_env(cluster_worker, tp_subtrees_number, 1),
    test_utils:set_env(Worker, cluster_worker, tp_subtrees_number, 1),
    Config.
