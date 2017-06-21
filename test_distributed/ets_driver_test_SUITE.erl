%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains ETS driver tests.
%%% @end
%%%-------------------------------------------------------------------
-module(ets_driver_test_SUITE).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_testcase/2]).

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

-record(test_model, {
    field1 :: integer(),
    field2 :: binary(),
    filed3 :: atom()
}).

-define(MODEL, test_model).
-define(KEY, ?KEY(1)).
-define(KEY(N), <<"key-", (integer_to_binary(N))/binary>>).
-define(VALUE, #?MODEL{
    field1 = 1,
    field2 = <<"2">>,
    filed3 = '3'
}).
-define(SCOPE, <<"scope">>).
-define(DOC, ?DOC(1)).
-define(DOC(N), #document{
    key = ?KEY(N),
    value = ?VALUE,
    scope = ?SCOPE
}).
-define(PERF_PARAM(Name, Value, Unit, Description), [
    {name, Name},
    {value, Value},
    {description, Description},
    {unit, Unit}
]).
-define(PERF_CFG(Name, Params), {config, [
    {name, Name},
    {description, atom_to_list(Name)},
    {parameters, Params}
]}).
-define(OPS_NUM(Value), ?PERF_PARAM(ops_num, Value, "",
    "Number of operations.")).

%%%===================================================================
%%% Test functions
%%%===================================================================

save_should_return_doc(Config) ->
    Ctx = ?config(ctx, Config),
    {ok, Doc} = ?assertMatch({ok, _}, ets_driver:save(Ctx, ?DOC)),
    ?assertEqual(?KEY, Doc#document.key),
    ?assertEqual(?VALUE, Doc#document.value),
    ?assertEqual(?SCOPE, Doc#document.scope),
    ?assertEqual([], Doc#document.mutator),
    ?assertMatch([], Doc#document.rev),
    ?assertEqual(0, Doc#document.seq),
    ?assertEqual(false, Doc#document.deleted),
    ?assertEqual(1, Doc#document.version).

get_should_return_doc(Config) ->
    Ctx = ?config(ctx, Config),
    {ok, Doc} = ets_driver:save(Ctx, ?DOC),
    ?assertEqual({ok, Doc}, ets_driver:get(Ctx, ?KEY)).

get_should_return_missing_error(Config) ->
    Ctx = ?config(ctx, Config),
    ?assertEqual({error, key_enoent}, ets_driver:get(Ctx, ?KEY)).

update_should_change_doc(Config) ->
    Ctx = ?config(ctx, Config),
    {ok, Doc} = ets_driver:save(Ctx, ?DOC),
    Value = #?MODEL{
        field1 = 2,
        field2 = <<"3">>,
        filed3 = '4'
    },
    {ok, Doc2} = ?assertMatch({ok, _}, ets_driver:save(
        Ctx, Doc#document{value = Value}
    )),
    ?assertEqual(?KEY, Doc2#document.key),
    ?assertEqual(Value, Doc2#document.value),
    ?assertEqual(?SCOPE, Doc2#document.scope),
    ?assertEqual([], Doc#document.mutator),
    ?assertMatch([], Doc#document.rev),
    ?assertEqual(0, Doc#document.seq),
    ?assertEqual(false, Doc2#document.deleted),
    ?assertEqual(1, Doc2#document.version).

delete_should_remove_doc(Config) ->
    Ctx = ?config(ctx, Config),
    ?assertMatch({ok, _}, ets_driver:save(Ctx, ?DOC)),
    ?assertEqual(ok, ets_driver:delete(Ctx, ?KEY)),
    ?assertEqual({error, key_enoent}, ets_driver:get(Ctx, ?KEY)).

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
    Ctx = ?config(ctx, Config),
    OpsNum = ?config(ops_num, Config),

    lists:foreach(fun(N) ->
        ?assertMatch({ok, #document{}}, ets_driver:save(Ctx, ?DOC(N)))
    end, lists:seq(1, OpsNum)),

    lists:foreach(fun(N) ->
        ?assertMatch({ok, #document{}}, ets_driver:get(Ctx, ?KEY(N)))
    end, lists:seq(1, OpsNum)),

    lists:foreach(fun(N) ->
        ?assertMatch(ok, ets_driver:delete(Ctx, ?KEY(N)))
    end, lists:seq(1, OpsNum)).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_testcase(Case, Config) ->
    Ctx = #{table => Case},
    ets_driver:init(Ctx, []),
    [{ctx, Ctx} | Config].
