%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains CouchBase driver tests.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_driver_test_SUITE).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    save_should_return_doc/1,
    save_should_increment_seq_counter/1,
    save_should_not_increment_seq_counter/1,
    save_should_not_set_mutator/1,
    save_should_not_set_revision/1,
    save_should_create_change_doc/1,
    save_should_return_missing_error/1,
    save_should_return_already_exists_error/1,
    get_should_return_doc/1,
    get_should_return_missing_error/1,
    update_should_change_doc/1,
    delete_should_remove_doc/1,
    delete_should_return_missing_error/1,
    save_get_delete_should_return_success/1,
    get_counter_should_return_value/1,
    get_counter_should_return_default_value/1,
    get_counter_should_return_missing_error/1,
    update_counter_should_return_default_value/1,
    update_counter_should_update_value/1,
    save_design_doc_should_return_success/1,
    save_view_doc_should_return_success/1,
    save_spatial_view_doc_should_return_success/1,
    get_design_doc_should_return_doc/1,
    get_design_doc_should_return_missing_error/1,
    delete_design_doc_should_return_success/1,
    delete_design_doc_should_return_missing_error/1,
    query_view_should_return_empty_result/1,
    query_view_should_return_result/1,
    query_view_should_return_missing_error/1,
    query_view_should_parse_empty_opts/1,
    query_view_should_parse_all_opts/1,
    get_buckets_should_return_all_buckets/1
]).

%% test_bases
-export([
    save_get_delete_should_return_success_base/1
]).

all() ->
    ?ALL([
        save_should_return_doc,
        save_should_increment_seq_counter,
        save_should_not_increment_seq_counter,
        save_should_not_set_mutator,
        save_should_not_set_revision,
        save_should_create_change_doc,
        save_should_return_missing_error,
        save_should_return_already_exists_error,
        get_should_return_doc,
        get_should_return_missing_error,
        update_should_change_doc,
        delete_should_remove_doc,
        delete_should_return_missing_error,
        save_get_delete_should_return_success,
        get_counter_should_return_value,
        get_counter_should_return_default_value,
        get_counter_should_return_missing_error,
        update_counter_should_return_default_value,
        update_counter_should_update_value,
        save_design_doc_should_return_success,
        save_view_doc_should_return_success,
        save_spatial_view_doc_should_return_success,
        get_design_doc_should_return_doc,
        get_design_doc_should_return_missing_error,
        delete_design_doc_should_return_success,
        delete_design_doc_should_return_missing_error,
        query_view_should_return_empty_result,
        query_view_should_return_result,
        query_view_should_return_missing_error,
        query_view_should_parse_empty_opts,
        query_view_should_parse_all_opts,
        get_buckets_should_return_all_buckets
    ], [
        save_get_delete_should_return_success
    ]).

-record(test_model, {
    field1 :: integer(),
    field2 :: binary(),
    filed3 :: atom()
}).

-define(MODEL, test_model).
-define(BUCKET, <<"default">>).
-define(CTX, #{bucket => ?BUCKET, mutator => ?MUTATOR, prefix => <<"prefix">>}).
-define(CASE, atom_to_binary(?FUNCTION_NAME, utf8)).
-define(KEY, ?KEY(1)).
-define(KEY(N), <<"key-", (?CASE)/binary, "-", (integer_to_binary(N))/binary>>).
-define(VALUE, #?MODEL{
    field1 = 1,
    field2 = <<"2">>,
    filed3 = '3'
}).
-define(SCOPE, <<"scope-", (?CASE)/binary>>).
-define(MUTATOR, ?MUTATOR(1)).
-define(MUTATOR(N), <<"mutator-", (?CASE)/binary, "-",
    (integer_to_binary(N))/binary>>).
-define(DOC, ?DOC(1)).
-define(DOC(N), #document{
    key = ?KEY(N),
    value = ?VALUE,
    scope = ?SCOPE
}).
-define(DESIGN, <<"design-", (?CASE)/binary>>).
-define(VIEW, <<"view-", (?CASE)/binary>>).
-define(VIEW_FUNCTION, <<"function (doc, meta) {\r\n"
"  emit(meta.id, null);\r\n"
"}\r\n">>).
-define(DESIGN_EJSON, {[{<<"views">>,
    {[{?VIEW,
        {[{<<"map">>, ?VIEW_FUNCTION}]}
    }]}
}]}).
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
-define(DURABLE(Value), ?PERF_PARAM(durable, Value, "",
    "Perform save operation with durability check.")).
-define(ATTEMPTS, 60).

%%%===================================================================
%%% Test functions
%%%===================================================================

save_should_return_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, _, Doc} = ?assertMatch({ok, _, #document{}},
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?DOC])
    ),
    ?assertEqual(?KEY, Doc#document.key),
    ?assertEqual(?VALUE, Doc#document.value),
    ?assertEqual(?SCOPE, Doc#document.scope),
    ?assertEqual([?MUTATOR], Doc#document.mutator),
    ?assertEqual(1, Doc#document.seq),
    ?assertEqual(false, Doc#document.deleted),
    ?assertEqual(1, Doc#document.version).

save_should_increment_seq_counter(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save, [?CTX, ?DOC]),
    ?assertMatch({ok, _, 1}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, couchbase_changes:get_seq_key(?SCOPE)]
    )).

save_should_not_increment_seq_counter(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, _, Doc} = ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver,
        save, [?CTX#{no_seq => true}, ?DOC]
    )),
    ?assertEqual(0, Doc#document.seq),
    ?assertEqual({error, key_enoent}, rpc:call(Worker, couchbase_driver,
        get_counter, [?CTX, couchbase_changes:get_seq_key(?SCOPE)]
    )).

save_should_not_set_mutator(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, _, Doc} = ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver,
        save, [maps:remove(mutator, ?CTX), ?DOC]
    )),
    ?assertEqual([], Doc#document.mutator).

save_should_not_set_revision(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, _, Doc} = ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver,
        save, [?CTX, ?DOC]
    )),
    ?assertEqual([], Doc#document.rev).

save_should_create_change_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save, [?CTX, ?DOC]),
    Key = couchbase_doc:set_prefix(?CTX, ?KEY),
    {ok, _, Json} = ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver,
        get, [
            maps:remove(prefix, ?CTX),
            couchbase_changes:get_change_key(?SCOPE, 1)
        ]
    )),
    {Props} = jiffy:decode(Json),
    ?assertEqual(Key, proplists:get_value(<<"key">>, Props)),
    Pid = binary_to_term(base64:decode(proplists:get_value(<<"pid">>, Props))),
    ?assertEqual(true, is_pid(Pid)),
    ?assertEqual(pong, gen_server:call(Pid, ping)).

save_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, key_enoent},
        rpc:call(Worker, couchbase_driver, save, [?CTX#{cas => 1}, ?DOC])
    ).

save_should_return_already_exists_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, _, _} = ?assertMatch({ok, _, #document{}},
        rpc:call(Worker, couchbase_driver, save, [?CTX, ?DOC])
    ),
    ?assertEqual({error, key_eexists},
        rpc:call(Worker, couchbase_driver, save, [?CTX#{cas => 1}, ?DOC])
    ).

get_should_return_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, Cas, Doc} = rpc:call(Worker, couchbase_driver, save, [?CTX, ?DOC]),
    ?assertEqual({ok, Cas, Doc}, rpc:call(Worker, couchbase_driver, get,
        [?CTX, ?KEY]
    )).

get_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, key_enoent}, rpc:call(Worker, couchbase_driver, get,
        [?CTX, ?KEY]
    )).

update_should_change_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, _, Doc} = ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver,
        save, [?CTX, ?DOC]
    )),
    Value = #?MODEL{
        field1 = 2,
        field2 = <<"3">>,
        filed3 = '4'
    },
    {ok, _, Doc2} = ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver,
        save, [?CTX#{mutator => ?MUTATOR(2)}, Doc#document{value = Value}]
    )),
    ?assertEqual(?KEY, Doc2#document.key),
    ?assertEqual(Value, Doc2#document.value),
    ?assertEqual(?SCOPE, Doc2#document.scope),
    ?assertEqual([?MUTATOR(2), ?MUTATOR(1)], Doc2#document.mutator),
    ?assertEqual(2, Doc2#document.seq),
    ?assertEqual(false, Doc2#document.deleted),
    ?assertEqual(1, Doc2#document.version).

delete_should_remove_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, _, _}, rpc:call(Worker, couchbase_driver, save,
        [?CTX, ?DOC]
    )),
    ?assertEqual(ok, rpc:call(Worker, couchbase_driver, delete, [?CTX, ?KEY])),
    ?assertEqual({error, key_enoent}, rpc:call(Worker, couchbase_driver, get,
        [?CTX, ?KEY]
    )).

delete_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual({error, key_enoent}, rpc:call(Worker, couchbase_driver, delete,
        [?CTX, ?KEY]
    )).

save_get_delete_should_return_success(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 3},
        {success_rate, 100},
        {parameters, [?OPS_NUM(10), ?DURABLE(true)]},
        {description, "Multiple cycles of parallel save/get/delete operations."},
        ?PERF_CFG(small_memory, [?OPS_NUM(1000), ?DURABLE(false)]),
        ?PERF_CFG(small_disk, [?OPS_NUM(1000), ?DURABLE(true)]),
        ?PERF_CFG(medium_memory, [?OPS_NUM(5000), ?DURABLE(false)]),
        ?PERF_CFG(medium_disk, [?OPS_NUM(5000), ?DURABLE(true)]),
        ?PERF_CFG(large_memory, [?OPS_NUM(10000), ?DURABLE(false)]),
        ?PERF_CFG(large_disk, [?OPS_NUM(10000), ?DURABLE(true)])
    ]).
save_get_delete_should_return_success_base(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    OpsNum = ?config(ops_num, Config),
    Durable = ?config(durable, Config),
    Self = self(),

    spawn_link(Worker, fun() ->
        Futures = lists:map(fun(N) ->
            Ctx = ?CTX#{no_durability => not Durable},
            couchbase_driver:save_async(Ctx, ?DOC(N))
        end, lists:seq(1, OpsNum)),
        lists:foreach(fun(Future) ->
            ?assertMatch({ok, _, #document{}}, couchbase_driver:wait(Future))
        end, Futures),
        ?assertEqual(
            0, couchbase_pool:get_request_queue_size(?BUCKET, write), ?ATTEMPTS
        ),

        Futures2 = lists:map(fun(N) ->
            couchbase_driver:get_async(?CTX, ?KEY(N))
        end, lists:seq(1, OpsNum)),
        lists:foreach(fun(Future) ->
            ?assertMatch({ok, _, #document{}}, couchbase_driver:wait(Future))
        end, Futures2),
        ?assertEqual(
            0, couchbase_pool:get_request_queue_size(?BUCKET, read), ?ATTEMPTS
        ),

        Futures3 = lists:map(fun(N) ->
            couchbase_driver:delete_async(?CTX, ?KEY(N))
        end, lists:seq(1, OpsNum)),
        lists:foreach(fun(Future) ->
            ?assertEqual(ok, couchbase_driver:wait(Future))
        end, Futures3),
        ?assertEqual(
            0, couchbase_pool:get_request_queue_size(?BUCKET, write), ?ATTEMPTS
        ),
        Self ! done
    end),
    receive done -> ok end.

get_counter_should_return_value(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, update_counter, [?CTX, ?KEY, 0, 10]),
    ?assertMatch({ok, _, 10}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, ?KEY]
    )).

get_counter_should_return_default_value(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, _, 0}, rpc:call(Worker, couchbase_driver, get_counter,
        [?CTX, ?KEY, 0]
    )).

get_counter_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, key_enoent}, rpc:call(Worker, couchbase_driver,
        get_counter, [?CTX, ?KEY]
    )).

update_counter_should_return_default_value(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({ok, _, 0}, rpc:call(Worker, couchbase_driver, update_counter,
        [?CTX, ?KEY, 1, 0]
    )).

update_counter_should_update_value(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, update_counter, [?CTX, ?KEY, 0, 0]),
    ?assertMatch({ok, _, 10}, rpc:call(Worker, couchbase_driver, update_counter,
        [?CTX, ?KEY, 10, 0]
    )).

save_design_doc_should_return_success(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON]
    )).

save_view_doc_should_return_success(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, couchbase_driver, save_view_doc,
        [?CTX, ?VIEW, ?VIEW_FUNCTION]
    )).

save_spatial_view_doc_should_return_success(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, couchbase_driver, save_spatial_view_doc,
        [?CTX, ?VIEW, ?VIEW_FUNCTION]
    )).

get_design_doc_should_return_doc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON]
    ),
    ?assertEqual({ok, ?DESIGN_EJSON},
        rpc:call(Worker, couchbase_driver, get_design_doc, [?CTX, ?DESIGN])
    ).

get_design_doc_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, {<<"not_found">>, _}},
        rpc:call(Worker, couchbase_driver, get_design_doc, [?CTX, ?DESIGN])
    ).

delete_design_doc_should_return_success(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON]
    ),
    ?assertEqual(ok, rpc:call(Worker, couchbase_driver, delete_design_doc,
        [?CTX, ?DESIGN]
    )).

delete_design_doc_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, {<<"not_found">>, _}},
        rpc:call(Worker, couchbase_driver, delete_design_doc, [?CTX, ?DESIGN])
    ).

query_view_should_return_empty_result(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON]
    ),
    ?assertMatch({ok, {[]}},
        rpc:call(Worker, couchbase_driver, query_view,
            [?CTX, ?DESIGN, ?VIEW, [{stale, false}, {key, ?KEY}]]
        )
    ).

query_view_should_return_result(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, couchbase_driver, save_design_doc,
        [?CTX, ?DESIGN, ?DESIGN_EJSON]
    ),
    rpc:call(Worker, couchbase_driver, save, [?CTX, ?DOC]),
    {ok, {[Result]}} = ?assertMatch({ok, {[_]}},
        rpc:call(Worker, couchbase_driver, query_view,
            [?CTX, ?DESIGN, ?VIEW, [
                {stale, false}, {key, couchbase_doc:set_prefix(?CTX, ?KEY)}
            ]]
        )
    ),
    ?assertEqual(couchbase_doc:set_prefix(?CTX, ?KEY),
        proplists:get_value(<<"id">>, Result)),
    ?assertEqual(couchbase_doc:set_prefix(?CTX, ?KEY),
        proplists:get_value(<<"key">>, Result)),
    ?assertEqual(null, proplists:get_value(<<"value">>, Result)).

query_view_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, {<<"not_found">>, _}},
        rpc:call(Worker, couchbase_driver, query_view,
            [?CTX, ?DESIGN, ?VIEW, [{stale, false}]]
        )
    ).

query_view_should_parse_empty_opts(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, {_, _}}, rpc:call(Worker, couchbase_driver, query_view,
        [?CTX, ?DESIGN, ?VIEW, []]
    )).

query_view_should_parse_all_opts(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({error, {_, _}},
        rpc:call(Worker, couchbase_driver, query_view,
            [?CTX, ?DESIGN, ?VIEW, [
                {descending, true},
                {descending, false},
                {endkey, <<"key">>},
                {endkey_docid, <<"id">>},
                {full_set, true},
                {full_set, false},
                {group, true},
                {group, false},
                {group_level, 1},
                {inclusive_end, true},
                {inclusive_end, false},
                {key, <<"key">>},
                {keys, [<<"key">>]},
                {limit, 1},
                {on_error, continue},
                {on_error, stop},
                {reduce, true},
                {reduce, false},
                {spatial, true},
                {spatial, false},
                {skip, 0},
                {stale, ok},
                {stale, false},
                {stale, update_after},
                {startkey, <<"key">>},
                {startkey_docid, <<"id">>},
                {bbox, <<"bbox">>},
                {start_range, <<"range">>},
                {end_range, <<"range">>}
            ]]
        )
    ).

get_buckets_should_return_all_buckets(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertEqual([?BUCKET], rpc:call(Worker, couchbase_config, get_buckets,
        []
    )).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_testcase(_Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_new(Workers, ?MODEL, [passthrough, non_strict]),
    test_utils:mock_expect(Workers, ?MODEL, model_init, fun() ->
        #model_config{version = 1}
    end),
    test_utils:mock_expect(Workers, ?MODEL, record_struct, fun(1) ->
        {record, [
            {field1, integer},
            {field2, string},
            {field3, atom}
        ]}
    end),

    Config.

end_per_testcase(_Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, ?MODEL).
