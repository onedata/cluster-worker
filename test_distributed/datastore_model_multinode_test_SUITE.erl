%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore models multi-node tests.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_model_multinode_test_SUITE).
-author("Michał Wrzeszcz").

-include("datastore_test_utils.hrl").
-include("global_definitions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    save_should_succeed/1,
    disk_fetch_should_succeed/1
]).

all() ->
    ?ALL([
        save_should_succeed,
        disk_fetch_should_succeed
    ]).

-define(DOC(Model), ?DOC(?KEY, Model)).
-define(DOC(Key, Model), ?BASE_DOC(Key, ?MODEL_VALUE(Model))).

-define(ATTEMPTS, 30).

%%%===================================================================
%%% Test functions
%%%===================================================================

save_should_succeed(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, save, [?DOC(Model)])
        ),
        lists:foreach(fun(W) -> assert_in_memory(W, Model, Key) end, Workers),
        assert_on_disc(Worker, Model, Key)
    end, ?TEST_MODELS).

disk_fetch_should_succeed(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, save, [?DOC(Model)])
        ),

        assert_on_disc(Worker, Model, Key),

        UniqueKey = ?UNIQUE_KEY(Model, Key),
        MemCtx = datastore_multiplier:extend_name(UniqueKey, ?MEM_CTX(Model)),
        lists:foreach(fun(W) ->
            ?assertMatch({ok, _}, rpc:call(W, ?MEM_DRV(Model), get, [MemCtx, UniqueKey])),
            ?assertEqual(ok, rpc:call(W, ?MEM_DRV(Model), delete, [MemCtx, UniqueKey])),
            ?assertMatch({error, not_found}, rpc:call(W, ?MEM_DRV(Model), get, [MemCtx, UniqueKey]))
        end, Workers),

        lists:foreach(fun(W) ->
            ?assertMatch({ok, #document{}}, rpc:call(W, Model, get, [Key])),
            ?assertMatch({ok, _}, rpc:call(W, ?MEM_DRV(Model), get, [MemCtx, UniqueKey]))
        end, Workers),

        lists:foreach(fun(W) ->
            ?assertEqual(ok, rpc:call(W, ?MEM_DRV(Model), delete, [MemCtx, UniqueKey])),
            ?assertMatch({error, not_found}, rpc:call(W, ?MEM_DRV(Model), get, [MemCtx, UniqueKey])),
            ?assertMatch({ok, #document{}}, rpc:call(W, Model, get, [Key])),
            ?assertMatch({ok, _}, rpc:call(W, ?MEM_DRV(Model), get, [MemCtx, UniqueKey]))
        end, lists:reverse(Workers))
    end, ?TEST_CACHED_MODELS).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite(Config).

init_per_testcase(_, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    application:load(cluster_worker),
    application:set_env(cluster_worker, tp_subtrees_number, 10),
    test_utils:set_env(Workers, cluster_worker, tp_subtrees_number, 10),
    test_utils:set_env(Workers, cluster_worker, test_ctx_base, #{memory_copies => all}),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

end_per_suite(_Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

assert_in_memory(Worker, Model, Key) ->
    assert_in_memory(Worker, Model, Key, false).

assert_in_memory(Worker, Model, Key, Deleted) ->
    case ?MEM_DRV(Model) of
        undefined ->
            ok;
        Driver ->
            Ctx = datastore_multiplier:extend_name(?UNIQUE_KEY(Model, Key),
                ?MEM_CTX(Model)),
            ?assertMatch({ok, #document{deleted = Deleted}},
                rpc:call(Worker, Driver, get, [
                    Ctx, ?UNIQUE_KEY(Model, Key)
                ])
            )
    end.

assert_not_in_memory(Worker, Model, Key) ->
    case ?MEM_DRV(Model) of
        undefined ->
            ok;
        Driver ->
            Ctx = datastore_multiplier:extend_name(?UNIQUE_KEY(Model, Key),
                ?MEM_CTX(Model)),
            ?assertMatch({error, not_found},
                rpc:call(Worker, Driver, get, [
                    Ctx, ?UNIQUE_KEY(Model, Key)
                ])
            )
    end.

assert_on_disc(Worker, Model, Key) ->
    assert_on_disc(Worker, Model, Key, false).

assert_on_disc(Worker, Model, Key, Deleted) ->
    assert_key_on_disc(Worker, Model, ?UNIQUE_KEY(Model, Key), Deleted).

assert_key_on_disc(Worker, Model, Key, Deleted) ->
    case ?DISC_DRV(Model) of
        undefined ->
            ok;
        Driver ->
            ?assertMatch({ok, _, #document{deleted = Deleted}},
                rpc:call(Worker, Driver, get, [
                    ?DISC_CTX, Key
                ]), ?ATTEMPTS
            )
    end.