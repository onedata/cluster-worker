%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore models tests.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_model_test_SUITE).
-author("Krzysztof Trzepla").

-include("datastore_test_utils.hrl").
-include("global_definitions.hrl").
-include("datastore_performance_tests_base.hrl").
-include("modules/datastore/histogram.hrl").
-include_lib("ctool/include/aai/aai.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    create_should_succeed/1,
    delete_all_should_succeed/1,
    create_should_return_already_exists_error/1,
    save_should_succeed/1,
    update_should_succeed/1,
    update_should_return_missing_error/1,
    update_should_save_default_when_missing/1,
    get_should_succeed/1,
    get_should_return_missing_error/1,
    exists_should_return_false/1,
    exists_should_return_true/1,
    delete_should_ignore_missing_value/1,
    delete_should_mark_value_deleted/1,
    fold_should_return_all_values/1,
    secure_fold_should_return_empty_list/1,
    secure_fold_should_return_not_empty_list/1,
    fold_should_return_all_values2/1,
    fold_keys_should_return_all_keys/1,
    add_links_should_succeed/1,
    check_and_add_links_test/1,
    get_links_should_succeed/1,
    get_links_after_expiration_time_should_succeed/1,
    disk_fetch_links_should_succeed/1,
    get_links_should_return_missing_error/1,
    delete_links_should_succeed/1,
    delete_links_should_ignore_missing_links/1,
    mark_links_deleted_should_succeed/1,
    fold_links_should_succeed/1,
    fold_links_token_should_succeed/1,
    fold_links_token_should_return_error_on_db_error/1,
    get_links_trees_should_return_all_trees/1,
    fold_links_token_should_succeed_after_token_timeout/1,
    links_performance/1,
    links_performance_base/1,
    create_get_performance/1,
    expired_doc_should_not_exist/1,
    deleted_doc_should_expire/1,
    link_doc_should_expire/1,
    link_del_should_delay_inactivate/1,
    fold_links_id_should_succeed/1,
    fold_links_token_and_id_should_succeed/1,
    stress_performance_test/1,
    stress_performance_test_base/1,
    memory_only_stress_performance_test/1,
    memory_only_stress_performance_test_base/1,
    infinite_log_create_test/1,
    infinite_log_destroy_test/1,
    infinite_log_operations_test/1,
    infinite_log_operations_direct_access_test/1,
    infinite_log_set_ttl_test/1,
    infinite_log_age_pruning_test/1,
    histogram_test/1,
    multinode_histogram_test/1,
    histogram_document_fetch_test/1
]).

% for rpc
-export([test_create_get/0, del_one_by_one/4]).

all() ->
    ?ALL([
        get_links_after_expiration_time_should_succeed,
        create_should_succeed,
        delete_all_should_succeed,
        create_should_return_already_exists_error,
        save_should_succeed,
        update_should_succeed,
        update_should_return_missing_error,
        update_should_save_default_when_missing,
        get_should_succeed,
        get_should_return_missing_error,
        exists_should_return_false,
        exists_should_return_true,
        delete_should_ignore_missing_value,
        delete_should_mark_value_deleted,
        fold_should_return_all_values,
        secure_fold_should_return_empty_list,
        secure_fold_should_return_not_empty_list,
        fold_should_return_all_values2,
        fold_keys_should_return_all_keys,
        add_links_should_succeed,
        check_and_add_links_test,
        get_links_should_succeed,
        disk_fetch_links_should_succeed,
        get_links_should_return_missing_error,
        delete_links_should_succeed,
        delete_links_should_ignore_missing_links,
        mark_links_deleted_should_succeed,
        fold_links_should_succeed,
        fold_links_token_should_succeed,
        fold_links_token_should_return_error_on_db_error,
        get_links_trees_should_return_all_trees,
        fold_links_token_should_succeed_after_token_timeout,
        links_performance,
        expired_doc_should_not_exist,
        deleted_doc_should_expire,
        link_doc_should_expire,
        link_del_should_delay_inactivate,
        fold_links_id_should_succeed,
        fold_links_token_and_id_should_succeed,
        memory_only_stress_performance_test,
        stress_performance_test,
        infinite_log_create_test,
        infinite_log_destroy_test,
        infinite_log_operations_test,
        infinite_log_operations_direct_access_test,
        infinite_log_set_ttl_test,
        infinite_log_age_pruning_test,
        histogram_test,
        multinode_histogram_test,
        histogram_document_fetch_test
    ], [
        links_performance,
        create_get_performance,
        memory_only_stress_performance_test,
        stress_performance_test
    ]).

-define(DOC(Model), ?DOC(?KEY, Model)).
-define(DOC(Key, Model), ?BASE_DOC(Key, ?MODEL_VALUE(Model))).

-define(ATTEMPTS, 30).

-define(REPEATS, 1).
-define(HA_REPEATS, 5).
-define(SUCCESS_RATE, 100).

%%%===================================================================
%%% Test functions
%%%===================================================================

create_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, create, [?DOC(Model)])
        ),
        assert_in_memory(Worker, Model, Key),
        assert_on_disc(Worker, Model, Key)
    end, ?TEST_MODELS).

delete_all_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Model = ets_only_model,
    {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
        rpc:call(Worker, Model, create, [?DOC(Model)])
    ),
    ?assertMatch(ok, rpc:call(Worker, Model, delete_all, [])),
    assert_not_in_memory(Worker, Model, Key).


create_should_return_already_exists_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, create, [?DOC(Model)])
        ),
        ?assertEqual({error, already_exists},
            rpc:call(Worker, Model, create, [?DOC(Key, Model)])
        )
    end, ?TEST_MODELS).

save_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, save, [?DOC(Model)])
        ),
        assert_in_memory(Worker, Model, Key),
        assert_on_disc(Worker, Model, Key)
    end, ?TEST_MODELS).

update_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, save, [?DOC(Model)])
        ),
        Value = ?MODEL_VALUE(Model, 2),
        Diff = fun(_) -> {ok, Value} end,
        {ok, #document{value = Value}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, update, [Key, Diff])
        ),
        assert_in_memory(Worker, Model, Key),
        assert_on_disc(Worker, Model, Key)
    end, ?TEST_MODELS).

update_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        ?assertEqual({error, not_found}, rpc:call(Worker, Model, update, [
            ?RND_KEY, fun(Value) -> {ok, Value} end
        ]))
    end, ?TEST_MODELS).

update_should_save_default_when_missing(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        Key = ?RND_KEY,
        ?assertMatch({ok, #document{}}, rpc:call(Worker, Model, update, [
            Key, fun(Value) -> {ok, Value} end, ?MODEL_VALUE(Model, 2)
        ]))
    end, ?TEST_MODELS).

get_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, save, [?DOC(Model)])
        ),
        ?assertMatch({ok, #document{}}, rpc:call(Worker, Model, get, [Key]))
    end, ?TEST_MODELS).

get_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        ?assertEqual({error, not_found}, rpc:call(Worker, Model, get, [
            ?RND_KEY
        ]))
    end, ?TEST_MODELS).

exists_should_return_true(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, save, [?DOC(Model)])
        ),
        ?assertMatch({ok, true}, rpc:call(Worker, Model, exists, [Key]))
    end, ?TEST_MODELS).

exists_should_return_false(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        ?assertEqual({ok, false}, rpc:call(Worker, Model, exists, [?RND_KEY]))
    end, ?TEST_MODELS).

delete_should_ignore_missing_value(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        ?assertEqual(ok, rpc:call(Worker, Model, delete, [?RND_KEY]))
    end, ?TEST_MODELS).

delete_should_mark_value_deleted(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, save, [?DOC(Model)])
        ),
        ?assertEqual(ok, rpc:call(Worker, Model, delete, [Key])),
        assert_in_memory(Worker, Model, Key, true),
        assert_on_disc(Worker, Model, Key, true),
        ?assertEqual({error, not_found}, rpc:call(Worker, Model, get, [Key]))
    end, ?TEST_MODELS).

fold_should_return_all_values(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = ExpectedKey}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, save, [?DOC(Model)])
        ),
        {ok, Docs} = ?assertMatch({ok, [_ | _]}, rpc:call(Worker, Model, fold, [
            fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []
        ])),
        Keys = [Doc#document.key || Doc <- Docs],
        ?assert(lists:member(ExpectedKey, Keys))
    end, ?TEST_MODELS).

secure_fold_should_return_empty_list(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    Master = self(),
    ok = test_utils:mock_expect(Workers, datastore_model, create,
        fun(Ctx, Doc) ->
            put(model_ctx, Ctx), % remember for use in datastore:create mock
            meck:passthrough([Ctx, Doc])
        end),

    ok = test_utils:mock_expect(Workers, datastore, create,
        fun(Ctx, Key, Doc) ->
            Ans = meck:passthrough([Ctx, Key, #document{key = DocKey} = Doc]),
            ModelCtx = get(model_ctx), % get Ctx from process memory as Ctx in first arg is changed by datastore
            spawn(fun() ->
                AnsToSend = datastore_model:delete(ModelCtx, DocKey),
                Master ! {del_ans, AnsToSend}
            end),
            timer:sleep(1000),
            Ans
        end),

    lists:foreach(fun(Model) ->
        {ok, #document{key = ExpectedKey}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, create, [?DOC(Model)])
        ),

        DelAns = receive
            {del_ans, Received} -> Received
        after
            5000 -> timeout
        end,
        ?assertEqual(ok, DelAns),

        {ok, Keys} = ?assertMatch({ok, _}, rpc:call(Worker, Model, fold_keys, [
            fun(Key, Acc) -> {ok, [Key | Acc]} end, []
        ])),
        ?assertNot(lists:member(ExpectedKey, Keys))
    end, ?TEST_MODELS).

secure_fold_should_return_not_empty_list(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    Master = self(),
    ok = test_utils:mock_expect(Workers, datastore_model, delete,
        fun(Ctx, Key) ->
            put(model_ctx, Ctx), % remember for use in datastore:create mock
            put(model_key, Key), % remember for use in datastore:create mock
            meck:passthrough([Ctx, Key])
        end),
    ok = test_utils:mock_expect(Workers, datastore, delete,
        fun(#{model := Model} = Ctx, Key, Pred) ->
            Ans = meck:passthrough([Ctx, Key, Pred]),
            ModelCtx = get(model_ctx), % get Ctx from process memory as Ctx in first arg is changed by datastore
            ModelKey = get(model_key), % get key from process memory as key in second arg is changed by datastore
            spawn(fun() ->
                AnsToSend = datastore_model:create(ModelCtx, ?DOC(ModelKey, Model)),
                Master ! {create_ans, AnsToSend}
            end),
            timer:sleep(1000),
            Ans
        end),

    lists:foreach(fun(Model) ->
        ?assertMatch(ok, rpc:call(Worker, Model, delete, [?KEY])),

        DelAns = receive
            {create_ans, Received} -> Received
        after
            5000 -> timeout
        end,
        ?assertMatch({ok, _}, DelAns),

        {ok, Keys} = ?assertMatch({ok, _}, rpc:call(Worker, Model, fold_keys, [
            fun(Key, Acc) -> {ok, [Key | Acc]} end, []
        ])),
        ?assert(lists:member(?KEY, Keys))
    end, ?TEST_MODELS).

fold_should_return_all_values2(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        ExpectedKeys = lists:foldl(fun(Num, Acc) ->
            {ok, #document{key = ExpectedKey}} = ?assertMatch({ok, #document{}},
                rpc:call(Worker, Model, save, [?DOC(?KEY(Num), Model)])
            ),
            [ExpectedKey | Acc]
        end, [], lists:seq(1, 10)),
        {ok, Docs} = ?assertMatch({ok, [_ | _]}, rpc:call(Worker, Model, fold, [
            fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []
        ])),
        Keys = [Doc#document.key || Doc <- Docs],
        lists:foreach(fun(ExpectedKey) ->
            ?assert(lists:member(ExpectedKey, Keys))
        end, ExpectedKeys)
    end, ?TEST_MODELS).

fold_keys_should_return_all_keys(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        {ok, #document{key = ExpectedKey}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, Model, save, [?DOC(Model)])
        ),
        {ok, Keys} = ?assertMatch({ok, [_ | _]}, rpc:call(Worker, Model,
            fold_keys, [fun(Key, Acc) -> {ok, [Key | Acc]} end, []]
        )),
        ?assert(lists:member(ExpectedKey, Keys))
    end, ?TEST_MODELS).

add_links_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        LinksNum = 1000,
        Links = lists:sort(lists:map(fun(N) ->
            {?LINK_NAME(N), ?LINK_TARGET(N)}
        end, lists:seq(1, LinksNum))),
        Results = rpc:call(Worker, Model, add_links, [
            ?KEY, ?LINK_TREE_ID, Links
        ]),
        lists:foreach(fun({Result, {LinkName, LinkTarget}}) ->
            {ok, Link} = ?assertMatch({ok, #link{}}, Result),
            ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
            ?assertEqual(LinkName, Link#link.name),
            ?assertEqual(LinkTarget, Link#link.target),
            ?assertEqual(undefined, Link#link.rev)
        end, lists:zip(Results, Links))
    end, ?TEST_MODELS).

check_and_add_links_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        LinksNum = 1000,

        Links = lists:sort(lists:map(fun(N) ->
            {?LINK_NAME(N), ?LINK_TARGET(N)}
        end, lists:seq(1, LinksNum, 2))),
        Results = rpc:call(Worker, Model, add_links, [
            ?KEY, ?LINK_TREE_ID(1), Links
        ]),
        lists:foreach(fun({Result, {LinkName, LinkTarget}}) ->
            {ok, Link} = ?assertMatch({ok, #link{}}, Result),
            ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
            ?assertEqual(LinkName, Link#link.name),
            ?assertEqual(LinkTarget, Link#link.target),
            ?assertEqual(undefined, Link#link.rev)
        end, lists:zip(Results, Links)),

        Links2 = lists:sort(lists:map(fun(N) ->
            {?LINK_NAME(N), ?LINK_TARGET(N)}
        end, lists:seq(1, LinksNum))),
        Results2 = rpc:call(Worker, Model, check_and_add_links, [
            ?KEY, ?LINK_TREE_ID(2), all, Links2
        ]),

        lists:foreach(fun({Result, {LinkName, LinkTarget} = LinkDef}) ->
            case lists:member(LinkDef, Links) of
                false ->
                    {ok, Link} = ?assertMatch({ok, #link{}}, Result),
                    ?assertEqual(?LINK_TREE_ID(2), Link#link.tree_id),
                    ?assertEqual(LinkName, Link#link.name),
                    ?assertEqual(LinkTarget, Link#link.target),
                    ?assertEqual(undefined, Link#link.rev);
                _ ->
                    ?assertMatch({error, already_exists}, Result)
            end
        end, lists:zip(Results2, Links2))
    end, ?TEST_MODELS).

get_links_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        LinksNum = 1000,
        Links = lists:sort(lists:map(fun(N) ->
            {?LINK_NAME(N), ?LINK_TARGET(N)}
        end, lists:seq(1, LinksNum))),
        {LinksNames, _} = lists:unzip(Links),
        ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
            ?KEY, ?LINK_TREE_ID, Links
        ])),
        Results = rpc:call(Worker, Model, get_links, [
            ?KEY, ?LINK_TREE_ID, LinksNames
        ]),
        lists:foreach(fun({Result, {LinkName, LinkTarget}}) ->
            {ok, [Link]} = ?assertMatch({ok, [#link{}]}, Result),
            ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
            ?assertEqual(LinkName, Link#link.name),
            ?assertEqual(LinkTarget, Link#link.target),
            ?assertEqual(undefined, Link#link.rev)
        end, lists:zip(Results, Links))
    end, ?TEST_MODELS).

% Test if documents' expiration does not result in links' trees/forests inconsistency.
% If everything works properly, only deleted documents (parts of links' trees) should expire.
% However, in case of a bug in expiration parameters setting, necessary documents may be deleted from
% couchbase. As a result link operations executed after time longer than expiration time will fail.
% The test performs link operations waiting longer than expiration time between operations
% to find possible expiration parameters setting bugs.
get_links_after_expiration_time_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Model = disc_only_model,

    LinksNum = 1000,
    Links = lists:sort(lists:map(fun(N) ->
        {?LINK_NAME(N), ?LINK_TARGET(N)}
    end, lists:seq(1, LinksNum))),
    {LinksNames, _} = lists:unzip(Links),



    % Check links deletion
    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        ?KEY, ?LINK_TREE_ID, Links
    ])),

    timer:sleep(timer:seconds(5)), % Allow documents expire
    % delete_links will fail if any link document expired
    ?assertAllMatch(ok, rpc:call(Worker, Model, delete_links, [
        ?KEY, ?LINK_TREE_ID, LinksNames
    ])),

    timer:sleep(timer:seconds(5)), % Allow documents expire
    Results = rpc:call(Worker, Model, get_links, [
        ?KEY, ?LINK_TREE_ID, LinksNames
    ]),
    lists:foreach(fun({Result, _}) ->
        ?assertMatch({error, not_found}, Result)
    end, lists:zip(Results, Links)),

    ?assertEqual({ok, []}, rpc:call(Worker, Model, fold_links,
        [?KEY, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], #{}]
    )),



    % Check links adding after deletion
    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        ?KEY, ?LINK_TREE_ID, Links
    ])),

    timer:sleep(timer:seconds(5)), % Allow documents expire
    Results2 = rpc:call(Worker, Model, get_links, [
        ?KEY, ?LINK_TREE_ID, LinksNames
    ]),
    lists:foreach(fun({Result, {LinkName, LinkTarget}}) ->
        {ok, [Link]} = ?assertMatch({ok, [#link{}]}, Result),
        ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
        ?assertEqual(LinkName, Link#link.name),
        ?assertEqual(LinkTarget, Link#link.target),
        ?assertEqual(undefined, Link#link.rev)
    end, lists:zip(Results2, Links)),

    {ok, Results3} = ?assertMatch({ok, _}, rpc:call(Worker, Model, fold_links,
        [?KEY, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], #{}]
    )),
    ?assertEqual(LinksNum, length(Results3)),



    % Test fold links
    ?assertMatch({ok, #document{}},
        rpc:call(Worker, Model, save, [?DOC(?KEY(2), Model)])
    ),

    ?assertMatch({ok, #document{}},
        rpc:call(Worker, Model, save, [?DOC(?KEY(3), Model)])
    ),

    timer:sleep(timer:seconds(5)), % Allow documents expire
    ?assertMatch({ok, [_, _]}, rpc:call(Worker, Model, fold, [fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []])),

    ?assertEqual(ok, rpc:call(Worker, Model, delete, [?KEY(2)])),

    timer:sleep(timer:seconds(5)), % Allow documents expire
    ?assertMatch({ok, [_]}, rpc:call(Worker, Model, fold, [fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []])).

disk_fetch_links_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),

    Master = self(),
    test_utils:mock_expect(Worker, links_tree, update_node, fun(NodeID, Node, State) ->
        Master ! {link_node_id, NodeID},
        meck:passthrough([NodeID, Node, State])
    end),

    lists:foreach(fun(Model) ->
        Links = [{?LINK_NAME, ?LINK_TARGET}],
        {LinksNames, _} = lists:unzip(Links),
        ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
            ?KEY, ?LINK_TREE_ID, Links
        ])),
        Results = rpc:call(Worker, Model, get_links, [
            ?KEY, ?LINK_TREE_ID, LinksNames
        ]),
        [{ok, [Link]}] = ?assertMatch([{ok, [#link{}]}], Results),
        ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
        ?assertEqual(?LINK_NAME, Link#link.name),
        ?assertEqual(?LINK_TARGET, Link#link.target),
        ?assertEqual(undefined, Link#link.rev),

        [LinkNode |__] = get_link_nodes(),

        MemCtx = datastore_multiplier:extend_name(?UNIQUE_KEY(Model, ?KEY), ?MEM_CTX(Model)),
        ?assertMatch({ok, _}, rpc:call(Worker, ?MEM_DRV(Model), get, [MemCtx, LinkNode])),
        ?assertMatch({ok, _, _}, rpc:call(Worker, ?DISC_DRV(Model), get, [?DISC_CTX, LinkNode]), 15),
        ?assertEqual(ok, rpc:call(Worker, ?MEM_DRV(Model), delete, [MemCtx, LinkNode])),
        ?assertEqual({error, not_found}, rpc:call(Worker, ?MEM_DRV(Model), get, [MemCtx, LinkNode])),

        Results2 = rpc:call(Worker, Model, get_links, [
            ?KEY, ?LINK_TREE_ID, LinksNames
        ]),
        ?assertEqual(Results, Results2),
        ?assertMatch({ok, _}, rpc:call(Worker, ?MEM_DRV(Model), get, [MemCtx, LinkNode]))
    end, ?TEST_CACHED_MODELS).

get_links_should_return_missing_error(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        ?assertEqual([{error, not_found}], rpc:call(Worker, Model, get_links, [
            ?KEY, ?LINK_TREE_ID, [?LINK_NAME]
        ]))
    end, ?TEST_MODELS).

delete_links_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        LinksNum = 1000,
        Links = lists:sort(lists:map(fun(N) ->
            {?LINK_NAME(N), ?LINK_TARGET(N)}
        end, lists:seq(1, LinksNum))),
        {LinksNames, _} = lists:unzip(Links),
        ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
            ?KEY, ?LINK_TREE_ID, Links
        ])),
        ?assertAllMatch({ok, [#link{}]}, rpc:call(Worker, Model, get_links, [
            ?KEY, ?LINK_TREE_ID, LinksNames
        ])),
        ?assertAllMatch(ok, rpc:call(Worker, Model, delete_links, [
            ?KEY, ?LINK_TREE_ID, LinksNames
        ])),
        ?assertAllMatch({error, not_found}, rpc:call(Worker, Model, get_links, [
            ?KEY, ?LINK_TREE_ID, LinksNames
        ]))
    end, ?TEST_MODELS).

delete_links_should_ignore_missing_links(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        ?assertEqual([ok], rpc:call(Worker, Model, delete_links,
            [?KEY, ?LINK_TREE_ID, [?LINK_NAME]]
        ))
    end, ?TEST_MODELS).

mark_links_deleted_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        LinksNum = 1000,
        Links = lists:sort(lists:map(fun(N) ->
            {?LINK_NAME(N), ?LINK_TARGET(N)}
        end, lists:seq(1, LinksNum))),
        {LinksNames, _} = lists:unzip(Links),
        ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
            ?KEY, ?LINK_TREE_ID, Links
        ])),
        Results = rpc:call(Worker, Model, get_links, [
            ?KEY, ?LINK_TREE_ID, LinksNames
        ]),
        LinkNamesAndRevs = lists:map(fun(Result) ->
            {ok, [#link{
                name = LinkName, rev = LinkRev
            }]} = ?assertMatch({ok, [#link{}]}, Result),
            {LinkName, LinkRev}
        end, Results),
        ?assertAllMatch(ok, rpc:call(Worker, Model, mark_links_deleted, [
            ?KEY, ?LINK_TREE_ID, LinkNamesAndRevs
        ])),
        ?assertAllMatch({error, not_found}, rpc:call(Worker, Model, get_links, [
            ?KEY, ?LINK_TREE_ID, LinksNames
        ]))
    end, ?TEST_MODELS).

fold_links_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        LinksNum = 1000,
        ExpectedLinks = lists:sort(lists:map(fun(N) ->
            {?LINK_NAME(N), ?LINK_TARGET(N)}
        end, lists:seq(1, LinksNum))),
        ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
            ?KEY, ?LINK_TREE_ID, ExpectedLinks
        ])),
        ExpectedLinks2 = lists:sort(ExpectedLinks),
        {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, Model, fold_links,
            [?KEY, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], #{}]
        )),
        lists:foreach(fun({{Name, Target}, Link = #link{}}) ->
            ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
            ?assertEqual(Name, Link#link.name),
            ?assertEqual(Target, Link#link.target)
        end, lists:zip(ExpectedLinks2, lists:reverse(Links)))
    end, ?TEST_MODELS).

fold_links_token_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        LinksNum = 1000,
        ExpectedLinks = lists:sort(lists:map(fun(N) ->
            {?LINK_NAME(N), ?LINK_TARGET(N)}
        end, lists:seq(1, LinksNum))),
        ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
            ?KEY, ?LINK_TREE_ID, ExpectedLinks
        ])),
        ExpectedLinks2 = lists:sort(ExpectedLinks),
        Links = fold_links_token(?KEY, Worker, Model,
            #{token => #link_token{}, size => 100}),
        lists:foreach(fun({{Name, Target}, Link = #link{}}) ->
            ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
            ?assertEqual(Name, Link#link.name),
            ?assertEqual(Target, Link#link.target)
        end, lists:zip(ExpectedLinks2, Links))
    end, ?TEST_MODELS).

fold_links_token_should_return_error_on_db_error(Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    Model = ets_only_model,
    LinksNum = 10,

    Links = lists:sort(lists:map(fun(N) ->
        {?LINK_NAME(N), ?LINK_TARGET(N)}
    end, lists:seq(1, LinksNum))),
    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        ?KEY, ?LINK_TREE_ID, Links
    ])),

    test_utils:mock_expect(Workers, datastore_links, get_links_trees,
        fun(_Ctx, _Key, Batch) -> {{error, etmpfail}, Batch} end),
    ?assertMatch({error, _}, rpc:call(Worker, Model, fold_links,
        [?KEY, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], #{token => #link_token{}}]
    )).

fold_links_token_should_succeed_after_token_timeout(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME,
        fold_cache_timeout, timer:seconds(0)),

    lists:foreach(fun(Model) ->
        LinksNum = 1000,
        ExpectedLinks = lists:sort(lists:map(fun(N) ->
            {?LINK_NAME(N), ?LINK_TARGET(N)}
        end, lists:seq(1, LinksNum))),
        ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
            ?KEY, ?LINK_TREE_ID, ExpectedLinks
        ])),
        ExpectedLinks2 = lists:sort(ExpectedLinks),

        Links = fold_links_token_sleep(?KEY, Worker, Model,
            #{token => #link_token{}, size => 500, offset => 0}),
        lists:foreach(fun({{Name, Target}, Link = #link{}}) ->
            ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
            ?assertEqual(Name, Link#link.name),
            ?assertEqual(Target, Link#link.target)
        end, lists:zip(ExpectedLinks2, Links))
    end, ?TEST_MODELS).

fold_links_id_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        LinksNum = 1000,
        ExpectedLinks = lists:sort(lists:map(fun(N) ->
            {?LINK_NAME(N), ?LINK_TARGET(N)}
        end, lists:seq(1, LinksNum))),
        ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
            ?KEY, ?LINK_TREE_ID, ExpectedLinks
        ])),
        ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
            ?KEY, ?LINK_TREE_ID(2), ExpectedLinks
        ])),

        AddTree = fun(TreeID) ->
            lists:map(fun({Name, Target}) ->
                {Name, Target, TreeID}
            end, ExpectedLinks)
        end,
        ExpectedLinks2 = lists:sort(AddTree(?LINK_TREE_ID) ++ AddTree(?LINK_TREE_ID(2))),

        Links = fold_links_id_and_tree(?KEY, Worker, Model,
            #{prev_link_name => <<>>, size => 101}),
        lists:foreach(fun({{Name, Target, TreeID}, Link = #link{}}) ->
            ?assertEqual(TreeID, Link#link.tree_id),
            ?assertEqual(Name, Link#link.name),
            ?assertEqual(Target, Link#link.target)
        end, lists:zip(ExpectedLinks2, Links))
    end, ?TEST_MODELS).

fold_links_token_and_id_should_succeed(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME,
        fold_cache_timeout, timer:seconds(0)),
    lists:foreach(fun(Model) ->
        LinksNum = 1000,
        ExpectedLinks = lists:sort(lists:map(fun(N) ->
            {?LINK_NAME(N), ?LINK_TARGET(N)}
        end, lists:seq(1, LinksNum))),
        ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
            ?KEY, ?LINK_TREE_ID, ExpectedLinks
        ])),
        ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
            ?KEY, ?LINK_TREE_ID(2), ExpectedLinks
        ])),

        AddTree = fun(TreeID) ->
            lists:map(fun({Name, Target}) ->
                {Name, Target, TreeID}
            end, ExpectedLinks)
        end,
        ExpectedLinks2 = lists:sort(AddTree(?LINK_TREE_ID) ++ AddTree(?LINK_TREE_ID(2))),

        Links = fold_links_token_id_and_tree(?KEY, Worker, Model,
            #{token => #link_token{}, size => 501}),
        lists:foreach(fun({{Name, Target, TreeID}, Link = #link{}}) ->
            ?assertEqual(TreeID, Link#link.tree_id),
            ?assertEqual(Name, Link#link.name),
            ?assertEqual(Target, Link#link.target)
        end, lists:zip(ExpectedLinks2, Links))
    end, ?TEST_MODELS).

get_links_trees_should_return_all_trees(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        ExpectedTreeIds = [?LINK_TREE_ID(N) || N <- lists:seq(1, 10)],
        lists:foreach(fun(TreeId) ->
            ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
                ?KEY, TreeId, [{?LINK_NAME, ?LINK_TARGET}]
            ]))
        end, ExpectedTreeIds),
        {ok, TreeIds} = ?assertMatch({ok, [_ | _]}, rpc:call(Worker, Model,
            get_links_trees, [?KEY]
        )),
        ?assertEqual(length(ExpectedTreeIds), length(TreeIds)),
        lists:foreach(fun(TreeId) ->
            ?assert(lists:member(TreeId, ExpectedTreeIds))
        end, TreeIds)
    end, ?TEST_MODELS).

expired_doc_should_not_exist(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(T) ->
        Model = ets_cached_model,
        Ctx = (datastore_test_utils:get_ctx(Model))#{expiry => T},
        {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
            rpc:call(Worker, datastore_model, create, [Ctx, ?DOC(?KEY(T), Model)])
        ),
        assert_on_disc(Worker, Model, Key),
        timer:sleep(8000),
        assert_not_on_disc(Worker, Model, Key)
    end, [global_clock:timestamp_seconds()+5, 5]).

deleted_doc_should_expire(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Model = ets_cached_model,
    Key = ?KEY,
    Ctx = datastore_test_utils:get_ctx(Model),
    {ok, #document{key = Key}} = ?assertMatch({ok, #document{}},
        rpc:call(Worker, datastore_model, create, [Ctx, ?DOC(Key, Model)])
    ),
    assert_on_disc(Worker, Model, Key),

    ?assertMatch(ok,
        rpc:call(Worker, datastore_model, delete, [Ctx, Key])
    ),
    assert_on_disc(Worker, Model, Key, false),
    timer:sleep(8000),
    assert_not_on_disc(Worker, Model, Key).

link_doc_should_expire(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Model = ets_cached_model,
    LinksNum = 1,
    Links = lists:sort(lists:map(fun(N) ->
        {?LINK_NAME(N), ?LINK_TARGET(N)}
    end, lists:seq(1, LinksNum))),
    {LinksNames, _} = lists:unzip(Links),
    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        ?KEY, ?LINK_TREE_ID, Links
    ])),
    ?assertAllMatch({ok, [#link{}]}, rpc:call(Worker, Model, get_links, [
        ?KEY, ?LINK_TREE_ID, LinksNames
    ])),

    CreatedNodes = get_link_nodes(create_link_node),
    lists:foreach(fun(Node) -> assert_key_on_disc(Worker, Model, Node) end,
        CreatedNodes),

    ?assertAllMatch(ok, rpc:call(Worker, Model, delete_links, [
        ?KEY, ?LINK_TREE_ID, LinksNames
    ])),
    ?assertAllMatch({error, not_found}, rpc:call(Worker, Model, get_links, [
        ?KEY, ?LINK_TREE_ID, LinksNames
    ])),

    DeletedNodes = get_link_nodes(delete_link_node),
    ?assertEqual(1, length(DeletedNodes)),
    [DeletedNode] = DeletedNodes,
    assert_key_on_disc(Worker, Model, DeletedNode, false),
    timer:sleep(8000),
    assert_key_not_on_disc(Worker, Model, DeletedNode).

link_del_should_delay_inactivate(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Model = ets_only_model,
    LinksNum = 1,
    Links = lists:sort(lists:map(fun(N) ->
        {?LINK_NAME(N), ?LINK_TARGET(N)}
    end, lists:seq(1, LinksNum))),
    {LinksNames, _} = lists:unzip(Links),
    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        ?KEY, ?LINK_TREE_ID, Links
    ])),
    ?assertAllMatch({ok, [#link{}]}, rpc:call(Worker, Model, get_links, [
        ?KEY, ?LINK_TREE_ID, LinksNames
    ])),
    Now = global_clock:timestamp_millis(),
    ?assertAllMatch(ok, rpc:call(Worker, Model, delete_links, [
        ?KEY, ?LINK_TREE_ID, LinksNames
    ])),
    ?assertAllMatch({error, not_found}, rpc:call(Worker, Model, get_links, [
        ?KEY, ?LINK_TREE_ID, LinksNames
    ])),

    DeletedNodes = get_link_nodes(delete_link_node),
    ?assertEqual(1, length(DeletedNodes)),
    [DeletedNode] = DeletedNodes,
    timer:sleep(timer:seconds(15)),
    Inactivated = get_link_nodes(inactivate),

    Timestamp = lists:foldl(fun({Map, T}, Acc) ->
        case maps:is_key(DeletedNode, Map) of
            true -> T;
            _ -> Acc
        end
    end, undefined, Inactivated),

    case Timestamp of
        undefined -> ct:print("Inactivated ~p", [Inactivated]);
        _ -> ok
    end,

    ?assertNotEqual(undefined, Timestamp),
    ?assert(Timestamp - Now > timer:seconds(5)).


infinite_log_create_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        ?assertMatch(ok,
            rpc:call(Worker, Model, infinite_log_create, [?KEY, #{max_entries_per_node => 8}])
        ),
        assert_in_memory(Worker, Model, ?KEY),
        assert_on_disc(Worker, Model, ?KEY)
    end, ?TEST_MODELS).


infinite_log_destroy_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        ok = rpc:call(Worker, Model, infinite_log_create, [?KEY, #{max_entries_per_node => 8}]),
        ?assertMatch(ok,
            rpc:call(Worker, Model, infinite_log_destroy, [?KEY])
        ),
        assert_in_memory(Worker, Model, ?KEY, true),
        assert_on_disc(Worker, Model, ?KEY, true)
    end, ?TEST_MODELS).


infinite_log_operations_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        ok = rpc:call(Worker, Model, infinite_log_create, [?KEY, #{max_entries_per_node => 8}]),
        
        ?assertEqual(ok, rpc:call(Worker, Model, infinite_log_append, [?KEY, <<"some_binary">>])),
        ?assertMatch({ok, {done, [{0, {_, <<"some_binary">>}}]}}, rpc:call(Worker, Model,
            infinite_log_list, [?KEY, #{}])),
        ?assertEqual(ok, rpc:call(Worker, Model, infinite_log_append, [?KEY, <<"another_binary">>])),
        ?assertMatch({ok, {done, [{1, {_, <<"another_binary">>}}]}}, rpc:call(Worker, Model,
            infinite_log_list, [?KEY, #{limit => 1, start_from => {index, 1}}])),
        
        ok = rpc:call(Worker, Model, infinite_log_destroy, [?KEY])
    end, ?TEST_MODELS).


infinite_log_operations_direct_access_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        MemoryOnlyCtx = (datastore_test_utils:get_ctx(Model))#{disc_driver => undefined, disc_driver_ctx => #{}}, 
        ok = rpc:call(Worker, Model, infinite_log_create, [?KEY, #{max_entries_per_node => 8}]),
        ExtendedMemTableCtx = datastore_multiplier:extend_name(?UNIQUE_KEY(Model, ?KEY), ?MEM_CTX(Model)),
        
        ?assertEqual(ok, rpc:call(Worker, Model, infinite_log_append, [?KEY, <<"some_binary">>])),
        ?assertMatch(
            {ok, {done, [{0, {_, <<"some_binary">>}}]}},
                check_direct_access_operation(Worker, Model, datastore_infinite_log, list, [?KEY, #{}], ExtendedMemTableCtx)),
    
        clean_cache(Worker, Model, ExtendedMemTableCtx),
        ?assertEqual({error, not_found}, rpc:call(Worker, datastore_infinite_log, append, [MemoryOnlyCtx, ?KEY, <<"another_binary">>])),
        ?assertEqual(ok, rpc:call(Worker, Model, infinite_log_append, [?KEY, <<"another_binary">>])),
        
        ?assertMatch(
            {ok, {done, [{1, {_, <<"another_binary">>}}]}},
            check_direct_access_operation(Worker, Model, datastore_infinite_log, list, [?KEY, #{limit => 1, start_from => {index, 1}}], ExtendedMemTableCtx)),
        
        clean_cache(Worker, Model, ExtendedMemTableCtx),
        ok = rpc:call(Worker, Model, infinite_log_destroy, [?KEY])
    end, ?TEST_CACHED_MODELS).


infinite_log_set_ttl_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        Ctx = datastore_multiplier:extend_name(?UNIQUE_KEY(Model, ?KEY),
            ?MEM_CTX(Model)),
        ok = rpc:call(Worker, Model, infinite_log_create, [?KEY, #{max_entries_per_node => 1}]),
        
        ?assertEqual(ok, rpc:call(Worker, Model, infinite_log_append, [?KEY, <<"some_binary1">>])),
        ?assertEqual(ok, rpc:call(Worker, Model, infinite_log_append, [?KEY, <<"some_binary2">>])),
        ?assertMatch({ok, {done, [
            {0, {_, <<"some_binary1">>}},
            {1, {_, <<"some_binary2">>}}
        ]}}, rpc:call(Worker, Model, infinite_log_list, [?KEY, #{}])),
        ?assertEqual(ok, rpc:call(Worker, Model, infinite_log_set_ttl, [?KEY, 2])),
        clean_cache(Worker, Model, Ctx),
        timer:sleep(timer:seconds(3)),
        
        ?assertEqual({error, not_found}, rpc:call(Worker, Model,
            infinite_log_append, [?KEY, <<"another_binary">>])),
        ?assertMatch({error, not_found}, rpc:call(Worker, Model,
            infinite_log_list, [?KEY, #{}])),
        
        ok = rpc:call(Worker, Model, infinite_log_destroy, [?KEY])
    end, ?TEST_MODELS).


infinite_log_age_pruning_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        Ctx = datastore_multiplier:extend_name(?UNIQUE_KEY(Model, ?KEY),
            ?MEM_CTX(Model)),
        ok = rpc:call(Worker, Model, infinite_log_create, [?KEY, #{max_entries_per_node => 1, age_pruning_threshold => 2}]),
        
        ?assertEqual(ok, rpc:call(Worker, Model, infinite_log_append, [?KEY, <<"some_binary1">>])),
        ?assertEqual(ok, rpc:call(Worker, Model, infinite_log_append, [?KEY, <<"some_binary2">>])),
        ?assertEqual(ok, rpc:call(Worker, Model, infinite_log_append, [?KEY, <<"some_binary3">>])),
        ?assertEqual(ok, rpc:call(Worker, Model, infinite_log_append, [?KEY, <<"some_binary4">>])),
        ?assertMatch({ok, {done, [
            {0, {_, <<"some_binary1">>}},
            {1, {_, <<"some_binary2">>}},
            {2, {_, <<"some_binary3">>}},
            {3, {_, <<"some_binary4">>}}
        ]}}, rpc:call(Worker, Model, infinite_log_list, [?KEY, #{}])),
        clean_cache(Worker, Model, Ctx),
        timer:sleep(timer:seconds(3)),
        
        ?assertMatch({ok, {done, [
            {3, {_, <<"some_binary4">>}}
        ]}}, rpc:call(Worker, Model, infinite_log_list, [?KEY, #{}])),
    
        ?assertEqual(ok, rpc:call(Worker, Model, infinite_log_append, [?KEY, <<"another_binary">>])),
        ?assertMatch({ok, {done, [
            {4, {_, <<"another_binary">>}}
        ]}}, rpc:call(Worker, Model, infinite_log_list, [?KEY, #{}])),
        
        ok = rpc:call(Worker, Model, infinite_log_destroy, [?KEY])
    end, ?TEST_PERSISTENT_MODELS).


histogram_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        Id = datastore_key:new(),
        ConfigMap = lists:foldl(fun(N, Acc) ->
            TimeSeries = <<"TS", (N rem 2)>>,
            MetricsMap = maps:get(TimeSeries, Acc, #{}),
            MetricsConfig = #histogram_config{window_size = N, max_windows_count = 600 div N + 10, apply_function = sum},
            Acc#{TimeSeries => MetricsMap#{<<"M", (N div 2)>> => MetricsConfig}}
        end, #{}, lists:seq(1, 5)),
        ?assertEqual(ok, rpc:call(Worker, Model, histogram_init, [Id, ConfigMap])),

        PointsCount = 1199,
        Points = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(0, PointsCount)),

        lists:foreach(fun({NewTimestamp, NewValue}) ->
            ?assertEqual(ok, rpc:call(Worker, Model, histogram_update, [Id, NewTimestamp, NewValue]))
        end, Points),

        ExpectedMap = maps:fold(fun(TimeSeriesId, MetricsConfigs, Acc) ->
            maps:fold(fun(MetricsId, #histogram_config{window_size = WindowSize, max_windows_count = MaxWindowsCount}, InternalAcc) ->
                InternalAcc#{{TimeSeriesId, MetricsId} => lists:sublist(lists:reverse(lists:map(fun(N) ->
                    {N, {WindowSize, (N + N + WindowSize - 1) * WindowSize}}
                end, lists:seq(0, PointsCount, WindowSize))), MaxWindowsCount)}
            end, Acc, MetricsConfigs)
        end, #{}, ConfigMap),
        ?assertMatch({ok, ExpectedMap}, rpc:call(Worker, Model, histogram_get, [Id, maps:keys(ExpectedMap), #{}]))
    end, ?TEST_MODELS).


multinode_histogram_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        Id = datastore_key:new(),
        ConfigMap = lists:foldl(fun(N, Acc) ->
            TimeSeries = <<"TS", (N rem 2)>>,
            MetricsMap = maps:get(TimeSeries, Acc, #{}),
            MetricsConfig = #histogram_config{window_size = 1, max_windows_count = 50000 * N, apply_function = last},
            Acc#{TimeSeries => MetricsMap#{<<"M", (N div 2)>> => MetricsConfig}}
        end, #{}, lists:seq(1, 5)),
        ?assertEqual(ok, rpc:call(Worker, Model, histogram_init, [Id, ConfigMap])),

        PointsCount = 300000,
        Points = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, PointsCount)),

        ?assertEqual(ok, rpc:call(Worker, Model, histogram_update, [Id, Points])),

        ExpectedMap = maps:fold(fun(TimeSeriesId, MetricsConfigs, Acc) ->
            maps:fold(fun(MetricsId, #histogram_config{max_windows_count = MaxWindowsCount}, InternalAcc) ->
                InternalAcc#{{TimeSeriesId, MetricsId} => lists:sublist(lists:reverse(Points), MaxWindowsCount)}
            end, Acc, MetricsConfigs)
        end, #{}, ConfigMap),
        ?assertMatch({ok, ExpectedMap}, rpc:call(Worker, Model, histogram_get, [Id, maps:keys(ExpectedMap), #{}]))
    end, ?TEST_MODELS).


histogram_document_fetch_test(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    lists:foreach(fun(Model) ->
        InitialKeys = get_all_keys(Worker, ?MEM_DRV(Model), ?MEM_CTX(Model)),
        Id = datastore_key:new(),
        ConfigMap = lists:foldl(fun(N, Acc) ->
            TimeSeries = <<"TS", (N rem 2)>>,
            MetricsMap = maps:get(TimeSeries, Acc, #{}),
            MetricsConfig = #histogram_config{window_size = 1, max_windows_count = 50000 * N, apply_function = last},
            Acc#{TimeSeries => MetricsMap#{<<"M", (N div 2)>> => MetricsConfig}}
        end, #{}, lists:seq(1, 3)),
        ?assertEqual(ok, rpc:call(Worker, Model, histogram_init, [Id, ConfigMap])),

        PointsCount = 300000,
        Points = lists:map(fun(I) -> {I, 2 * I} end, lists:seq(1, PointsCount)),
        ExpectedMap = maps:fold(fun(TimeSeriesId, MetricsConfigs, Acc) ->
            maps:fold(fun(MetricsId, #histogram_config{max_windows_count = MaxWindowsCount}, InternalAcc) ->
                InternalAcc#{{TimeSeriesId, MetricsId} => lists:sublist(lists:reverse(Points), MaxWindowsCount)}
            end, Acc, MetricsConfigs)
        end, #{}, ConfigMap),

        ?assertEqual(ok, rpc:call(Worker, Model, histogram_update, [Id, Points])),
        Keys = get_all_keys(Worker, ?MEM_DRV(Model), ?MEM_CTX(Model)) -- InitialKeys,

        lists:foreach(fun(Key) ->
            assert_key_on_disc(Worker, Model, Key, false),
            MemCtx = datastore_multiplier:extend_name(Key, ?MEM_CTX(Model)),
            ?assertEqual(ok, rpc:call(Worker, ?MEM_DRV(Model), delete, [MemCtx, Key])),
            assert_key_not_in_memory(Worker, Model, Key),

            ?assertMatch({ok, ExpectedMap}, rpc:call(Worker, Model, histogram_get, [Id, maps:keys(ExpectedMap), #{}]))
        end, Keys)
    end, ?TEST_CACHED_MODELS).


%%%===================================================================
%%% Stress tests
%%%===================================================================

memory_only_stress_performance_test(Config) ->
    ?PERFORMANCE(Config, [
        ?SINGLENODE_TEST(true, 1)
    ]).
memory_only_stress_performance_test_base(Config) ->
    datastore_performance_tests_base:stress_performance_test_base(Config).

stress_performance_test(Config) ->
    ?PERFORMANCE(Config, [
        ?SINGLENODE_TEST(false, ?HA_REPEATS)
    ]).
stress_performance_test_base(Config) ->
    datastore_performance_tests_base:stress_performance_test_base(Config).

links_performance(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, ?SUCCESS_RATE},
        {parameters, [
            [{name, links_num}, {value, 50000},
                {description, "Number of links listed during the test."}],
            [{name, orders}, {value, [1024]},
                {description, "Tree orders used during test."}]
        ]},
        {description, "Lists large number of links"},
        {config, [{name, small},
            {parameters, [
                [{name, links_num}, {value, 5000}],
                [{name, orders}, {value, [128, 1024, 5120, 10240]}]
            ]},
            {description, "Small number of links"}
        ]},
        {config, [{name, medium},
            {parameters, [
                [{name, links_num}, {value, 20000}],
                [{name, orders}, {value, [128, 1024, 5120, 10240]}]
            ]},
            {description, "Medium number of links"}
        ]},
        {config, [{name, big},
            {parameters, [
                [{name, links_num}, {value, 50000}],
                [{name, orders}, {value, [128, 1024, 5120, 10240]}]
            ]},
            {description, "High number of links"}
        ]}
        % TODO - VFS-4937
        %%        {config, [{name, large},
        %%            {parameters, [
        %%                [{name, links_num}, {value, 100000}],
        %%                [{name, orders}, {value, [128, 1024, 5120, 10240]}]
        %%            ]},
        %%            {description, "Very high number of links"}
        %%        ]}
    ]).
links_performance_base(Config) ->
    ct:timetrap({hours, 2}),
    Orders = ?config(orders, Config),
    lists:foreach(fun(Order) ->
        links_performance_base(Config, Order)
    end, Orders).

links_performance_base(Config, Order) ->
    % TODO VFS-4743 - test fetch

    % Init test variables
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Workers, cluster_worker, datastore_links_tree_order, Order),
    test_utils:set_env(Workers, ?CLUSTER_WORKER_APP_NAME,
        fold_cache_timeout, timer:seconds(30)),

    Model = ets_only_model,
    LinksNum = ?config(links_num, Config),

    KeyNum = case get(key_num) of
        undefined ->
            0;
        N ->
            N
    end,
    put(key_num, KeyNum + 2),
    Key = ?KEY(KeyNum),
    Key2 = ?KEY(KeyNum + 1),

    % Init tp
    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        Key, ?LINK_TREE_ID, [{?LINK_NAME, ?LINK_TARGET}]
    ])),
    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        Key2, ?LINK_TREE_ID, [{?LINK_NAME, ?LINK_TARGET}]
    ])),
    ?assertAllMatch(ok, rpc:call(Worker, Model, delete_links, [
        Key, ?LINK_TREE_ID, [?LINK_NAME]
    ])),
    ?assertAllMatch(ok, rpc:call(Worker, Model, delete_links, [
        Key2, ?LINK_TREE_ID, [?LINK_NAME]
    ])),
    ?assertMatch({ok, []}, rpc:call(Worker, Model, fold_links,
        [Key, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], #{size => 1}]
    )),
    ?assertMatch({ok, []}, rpc:call(Worker, Model, fold_links,
        [Key2, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], #{size => 1}]
    )),

    % Test add
    ExpectedLinks = lists:sort(lists:map(fun(N) ->
        {?LINK_NAME(N), ?LINK_TARGET(N)}
    end, lists:seq(1, LinksNum))),
    ExpectedLinksHalf = lists:sort(lists:map(fun(N) ->
        {?LINK_NAME(N), ?LINK_TARGET(N)}
    end, lists:seq(1, LinksNum, 2))),
    ExpectedLinksHalf2 = ExpectedLinks -- ExpectedLinksHalf,

    Stopwatch = stopwatch:start(),
    T0Add = stopwatch:read_micros(Stopwatch),
    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        Key, ?LINK_TREE_ID, ExpectedLinks
    ])),
    T1Add = stopwatch:read_micros(Stopwatch),

    T2Add = stopwatch:read_micros(Stopwatch),
    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        Key2, ?LINK_TREE_ID, ExpectedLinksHalf
    ])),
    T3Add = stopwatch:read_micros(Stopwatch),

    T4Add = stopwatch:read_micros(Stopwatch),
    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        Key2, ?LINK_TREE_ID, ExpectedLinksHalf2
    ])),
    T5Add = stopwatch:read_micros(Stopwatch),

    % Test list
    T0List = stopwatch:read_micros(Stopwatch),
    {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, Model, fold_links,
        [Key, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], #{}]
    )),
    T1List = stopwatch:read_micros(Stopwatch),
    ?assertEqual(LinksNum, length(Links)),

    T2List = stopwatch:read_micros(Stopwatch),
    Links2 = fold_links_offset(Key2, Worker, Model,
        #{size => 100, offset => 0}, LinksNum),
    T3List = stopwatch:read_micros(Stopwatch),
    ?assertEqual(LinksNum, length(Links2)),

    T4List = stopwatch:read_micros(Stopwatch),
    Links3 = fold_links_offset(Key2, Worker, Model,
        #{size => 2000, offset => 0}, LinksNum),
    T5List = stopwatch:read_micros(Stopwatch),
    ?assertEqual(LinksNum, length(Links3)),

    T6List = stopwatch:read_micros(Stopwatch),
    Links4 = fold_links_token(Key, Worker, Model,
        #{size => 100, offset => 0, token => #link_token{}}),
    T7List = stopwatch:read_micros(Stopwatch),
    ?assertEqual(LinksNum, length(Links4)),

    T8List = stopwatch:read_micros(Stopwatch),
    Links5 = fold_links_token(Key, Worker, Model,
        #{size => 2000, offset => 0, token => #link_token{}}),
    T9List = stopwatch:read_micros(Stopwatch),
    ?assertEqual(LinksNum, length(Links5)),

    T10List = stopwatch:read_micros(Stopwatch),
    LinksByOffset = fold_links_id(Key, Worker, Model,
        #{size => 2000, prev_link_name => <<>>}),
    T11List = stopwatch:read_micros(Stopwatch),
    ?assertEqual(LinksNum, length(LinksByOffset)),

    timer:sleep(500),
    T12List = stopwatch:read_micros(Stopwatch),
    LinksByOffset = fold_links_id_and_neg_offset(Key, Worker, Model,
        #{size => 2000, prev_link_name => <<>>, offset => -100}, []),
    T13List = stopwatch:read_micros(Stopwatch),
    ?assertEqual(LinksNum, length(LinksByOffset)),

    % Test del
    ExpectedLinkNames = lists:sort(lists:map(fun(N) ->
        ?LINK_NAME(N)
    end, lists:seq(1, LinksNum))),
    ExpectedLinkNamesReversed = lists:reverse(ExpectedLinkNames),

    T0Del = stopwatch:read_micros(Stopwatch),
    ?assertAllMatch(ok, rpc:call(Worker, Model, delete_links, [
        Key, ?LINK_TREE_ID, ExpectedLinkNames
    ])),
    T1Del = stopwatch:read_micros(Stopwatch),
    Links6 = fold_links_token(Key, Worker, Model,
        #{size => 2000, offset => 0, token => #link_token{}}),
    ?assertEqual(0, length(Links6)),

    T2Del = stopwatch:read_micros(Stopwatch),
    ?assertAllMatch(ok, rpc:call(Worker, Model, delete_links, [
        Key2, ?LINK_TREE_ID, ExpectedLinkNamesReversed
    ])),
    T3Del = stopwatch:read_micros(Stopwatch),
    Links7 = fold_links_token(Key2, Worker, Model,
        #{size => 2000, offset => 0, token => #link_token{}}),
    ?assertEqual(0, length(Links7)),

    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        Key, ?LINK_TREE_ID, ExpectedLinks
    ])),

    ExpectedLinkNames2 = lists:sort(lists:map(fun(N) ->
        ?LINK_NAME(N)
    end, lists:seq(1, LinksNum, 3))),
    ExpectedLinkNames3 = ExpectedLinkNames -- ExpectedLinkNames2,
    T4Del = stopwatch:read_micros(Stopwatch),
    ?assertAllMatch(ok, rpc:call(Worker, Model, delete_links, [
        Key, ?LINK_TREE_ID, ExpectedLinkNames2
    ])),
    T5Del = stopwatch:read_micros(Stopwatch),

    ?assertAllMatch(ok, rpc:call(Worker, Model, delete_links, [
        Key, ?LINK_TREE_ID, ExpectedLinkNames3
    ])),
    Links8 = fold_links_token(Key2, Worker, Model,
        #{size => 2000, offset => 0, token => #link_token{}}),
    ?assertEqual(0, length(Links8)),

    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        Key, ?LINK_TREE_ID, ExpectedLinks
    ])),
    ExpectedLinkNames4 = lists:map(fun(N) ->
        ?LINK_NAME(N)
    end, lists:seq(1, LinksNum)),

    T6Del = stopwatch:read_micros(Stopwatch),
    ?assertAllMatch(ok, rpc:call(Worker, ?MODULE, del_one_by_one, [
        Model, Key, ?LINK_TREE_ID, ExpectedLinkNames4
    ])),
    T7Del = stopwatch:read_micros(Stopwatch),
    Links9 = fold_links_token(Key, Worker, Model,
        #{size => 2000, offset => 0, token => #link_token{}}),
    ?assertEqual(0, length(Links9)),

    % Print results
    AddTime1Diff = T1Add - T0Add,
    AddTime2Diff = T3Add - T2Add,
    AddTime3Diff = T5Add - T4Add,
    ListTimeDiff1 = T1List - T0List,
    ListTimeDiff2 = T3List - T2List,
    ListTimeDiff3 = T5List - T4List,
    ListTimeDiff4 = T7List - T6List,
    ListTimeDiff5 = T9List - T8List,
    ListTimeDiff6 = T11List - T10List,
    ListTimeDiff7 = T13List - T12List,
    DelTime1Diff = T1Del - T0Del,
    DelTime2Diff = T3Del - T2Del,
    DelTime3Diff = T5Del - T4Del,
    DelTime4Diff = T7Del - T6Del,
    ct:pal("Results for order ~p, links num ~p:~n"
    "add all ~p, add half ~p, add second half ~p~n"
    "list all ~p, list offset (batch 100) ~p, list offset (batch 2000) ~p~n"
    "list token (batch 100) ~p, list token (batch 2000) ~p~n"
    "list by id (batch 2000) ~p, list by id with neg offest (batch 2000) ~p~n"
    "dell all ~p, dell all reversed ~p, dell 1/3 ~p, dell one by one ~p~n",
        [Order, LinksNum, AddTime1Diff, AddTime2Diff, AddTime3Diff,
            ListTimeDiff1, ListTimeDiff2, ListTimeDiff3,
            ListTimeDiff4, ListTimeDiff5, ListTimeDiff6, ListTimeDiff7,
            DelTime1Diff, DelTime2Diff, DelTime3Diff, DelTime4Diff]).

create_get_performance(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, Times} = ?assertMatch({ok, _},
        rpc:call(Worker, ?MODULE, test_create_get, [])),
    ct:print("Times: ~p", [Times]),
    ok.

test_create_get() ->
    NonExistingId = ?KEY,
    % Use gs_subscriber as example of existing model
    % (model emulation affects results).
    Stopwatch = stopwatch:start(),
    Time0 = stopwatch:read_micros(Stopwatch),
    ?assertEqual({error, not_found}, gs_session:get(NonExistingId)),
    Time1 = stopwatch:read_micros(Stopwatch),
    ?assertEqual({error, not_found}, gs_session:get(NonExistingId)),
    Time2 = stopwatch:read_micros(Stopwatch),
    #gs_session{id = ExistingId} = ?assertMatch(
        #gs_session{}, gs_session:create(?USER(<<"123">>), self(), 4, dummyTranslator)
    ),
    Time3 = stopwatch:read_micros(Stopwatch),
    ?assertMatch({ok, _}, gs_session:get(ExistingId)),
    Time4 = stopwatch:read_micros(Stopwatch),
    ?assertMatch({ok, _}, gs_session:get(ExistingId)),
    Time5 = stopwatch:read_micros(Stopwatch),

    Diff1 = Time1 - Time0,
    Diff2 = Time2 - Time1,
    Diff3 = Time3 - Time2,
    Diff4 = Time4 - Time3,
    Diff5 = Time5 - Time4,
    {ok, {Diff1, Diff2, Diff3, Diff4, Diff5}}.

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite(?TEST_MODELS, Config,
        fun(Config2) -> Config2 end, [datastore_test_utils, datastore_performance_tests_base]).

init_per_testcase(fold_links_token_should_return_error_on_db_error = Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_new(Workers, [datastore_links]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(deleted_doc_should_expire = Case, Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    {ok, Expiry} = test_utils:get_env(Worker, cluster_worker, document_expiry),
    test_utils:set_env(Workers, cluster_worker, document_expiry, 5),
    [{expiry, Expiry} | init_per_testcase(?DEFAULT_CASE(Case), Config)];
init_per_testcase(link_doc_should_expire = Case, Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    {ok, Expiry} = test_utils:get_env(Worker, cluster_worker, document_expiry),
    test_utils:set_env(Workers, cluster_worker, link_disk_expiry, 5),

    Master = self(),
    ok = test_utils:mock_new(Workers, links_tree),
    ok = test_utils:mock_expect(Workers, links_tree, delete_node,
        fun(NodeID, State) ->
            Master ! {delete_link_node, NodeID},
            meck:passthrough([NodeID, State])
        end),
    ok = test_utils:mock_expect(Workers, links_tree, create_node,
        fun(Node, State) ->
            {{ok, NodeID}, _} = Ans = meck:passthrough([Node, State]),
            Master ! {create_link_node, NodeID},
            Ans
        end),

    [{expiry, Expiry} | init_per_testcase(?DEFAULT_CASE(Case), Config)];
init_per_testcase(link_del_should_delay_inactivate = Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    Master = self(),
    ok = test_utils:mock_new(Workers, links_tree),
    ok = test_utils:mock_expect(Workers, links_tree, delete_node,
        fun(NodeID, State) ->
            Master ! {delete_link_node, NodeID},
            meck:passthrough([NodeID, State])
        end),

    ok = test_utils:mock_new(Workers, datastore_cache),
    ok = test_utils:mock_expect(Workers, datastore_cache, inactivate,
        fun(ToInactivate) ->
            Master ! {inactivate, {ToInactivate, global_clock:timestamp_millis()}},
            meck:passthrough([ToInactivate])
        end),

    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(disk_fetch_links_should_succeed = Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_new(Workers, links_tree),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(get_links_after_expiration_time_should_succeed = Case, Config) ->
    [Worker | _] = Workers = ?config(cluster_worker_nodes, Config),
    {ok, DocExpiry} = test_utils:get_env(Worker, cluster_worker, document_expiry),
    {ok, LinkExpiry} = test_utils:get_env(Worker, cluster_worker, link_disk_expiry),
    test_utils:set_env(Workers, cluster_worker, document_expiry, 1),
    test_utils:set_env(Workers, cluster_worker, link_disk_expiry, 1),
    [{doc_expiry, DocExpiry}, {link_expiry, LinkExpiry} | init_per_testcase(?DEFAULT_CASE(Case), Config)];
init_per_testcase(Case, Config) when Case =:= secure_fold_should_return_empty_list orelse
    Case =:= secure_fold_should_return_not_empty_list ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Workers, cluster_worker, test_ctx_base, #{secure_fold_enabled => true}),
    ok = test_utils:mock_new(Workers, datastore_model),
    ok = test_utils:mock_new(Workers, datastore),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(_, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    application:load(cluster_worker),
    application:set_env(cluster_worker, tp_subtrees_number, 10),
    test_utils:set_env(Worker, cluster_worker, tp_subtrees_number, 10),
    Config.

end_per_testcase(fold_links_token_should_return_error_on_db_error, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_unload(Workers, [datastore_links]);
end_per_testcase(deleted_doc_should_expire, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    Expiry = ?config(expiry, Config),
    test_utils:set_env(Workers, cluster_worker, document_expiry, Expiry);
end_per_testcase(link_doc_should_expire, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    Expiry = ?config(expiry, Config),
    test_utils:set_env(Workers, cluster_worker, link_disk_expiry, Expiry),

    test_utils:mock_unload(Workers, links_tree);
end_per_testcase(link_del_should_delay_inactivate, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_unload(Workers, [links_tree, datastore_cache]);
end_per_testcase(disk_fetch_links_should_succeed, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_unload(Workers, [links_tree]);
end_per_testcase(get_links_after_expiration_time_should_succeed, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    DocExpiry = ?config(doc_expiry, Config),
    LinkExpiry = ?config(link_expiry, Config),
    test_utils:set_env(Workers, cluster_worker, document_expiry, DocExpiry),
    test_utils:set_env(Workers, cluster_worker, link_disk_expiry, LinkExpiry);
end_per_testcase(Case, Config) when Case =:= secure_fold_should_return_empty_list orelse
    Case =:= secure_fold_should_return_not_empty_list ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:set_env(Workers, cluster_worker, test_ctx_base, #{}),
    test_utils:mock_unload(Workers, [datastore_model, datastore]);
end_per_testcase(_Case, _Config) ->
    ok.

end_per_suite(_Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_link_nodes(Message) ->
    receive
        {Message, NodeID} ->
            [NodeID | get_link_nodes(Message)]
    after
        0 -> []
    end.

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
    assert_not_in_memory(Worker, Model, ?UNIQUE_KEY(Model, Key)).

assert_key_not_in_memory(Worker, Model, Key) ->
    case ?MEM_DRV(Model) of
        undefined ->
            ok;
        Driver ->
            Ctx = datastore_multiplier:extend_name(Key,
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

assert_key_on_disc(Worker, Model, Key) ->
    assert_key_on_disc(Worker, Model, Key, false).

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

assert_not_on_disc(Worker, Model, Key) ->
    assert_key_not_on_disc(Worker, Model, ?UNIQUE_KEY(Model, Key)).

assert_key_not_on_disc(Worker, Model, Key) ->
    case ?DISC_DRV(Model) of
        undefined ->
            ok;
        Driver ->
            ?assertMatch({error, not_found},
                rpc:call(Worker, Driver, get, [
                    ?DISC_CTX, Key
                ]), ?ATTEMPTS
            )
    end.

get_all_keys(Worker, ets_driver, MemoryDriverCtx) ->
    lists:foldl(fun(#{table := Table}, AccOut) ->
        AccOut ++ lists:filtermap(fun
            ({_Key, #document{deleted = true}}) -> false;
            ({Key, #document{deleted = false}}) -> {true, Key}
        end, rpc:call(Worker, ets, tab2list, [Table]))
    end, [], rpc:call(Worker, datastore_multiplier, get_names, [MemoryDriverCtx]));
get_all_keys(Worker, mnesia_driver, MemoryDriverCtx) ->
    lists:foldl(fun(#{table := Table}, AccOut) ->
        AccOut ++ rpc:call(Worker, mnesia, async_dirty, [fun() ->
            mnesia:foldl(fun
                ({entry, _Key, #document{deleted = true}}, Acc) -> Acc;
                ({entry, Key, #document{deleted = false}}, Acc) -> [Key | Acc]
            end, [], Table)
        end])
    end, [], rpc:call(Worker, datastore_multiplier, get_names, [MemoryDriverCtx])).

fold_links_token(Key, Worker, Model, Opts) ->
    {{ok, Links}, Token} = ?assertMatch({{ok, _}, _}, rpc:call(Worker, Model, fold_links,
        [Key, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], Opts]
    )),
    Reversed = lists:reverse(Links),
    case Token#link_token.is_last of
        true ->
            Reversed;
        _ ->
            Opts2 = Opts#{token => Token},
            Reversed ++ fold_links_token(Key, Worker, Model, Opts2)
    end.

fold_links_token_sleep(Key, Worker, Model, Opts) ->
    {{ok, Links}, Token} = ?assertMatch({{ok, _}, _}, rpc:call(Worker, Model, fold_links,
        [Key, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], Opts]
    )),
    Reversed = lists:reverse(Links),
    case Token#link_token.is_last of
        true ->
            Reversed;
        _ ->
            Offset = maps:get(offset, Opts),
            Opts2 = Opts#{token => Token, offset => Offset + length(Reversed)},
            timer:sleep(timer:seconds(10)),
            Reversed ++ fold_links_token_sleep(Key, Worker, Model, Opts2)
    end.

fold_links_offset(Key, Worker, Model, Opts, ExpectedSize) ->
    {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, Model, fold_links,
        [Key, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], Opts]
    )),
    LinksLength = length(Links),
    case LinksLength >= ExpectedSize of
        true ->
            Links;
        _ ->
            Offset = maps:get(offset, Opts),
            Opts2 = Opts#{offset => Offset + LinksLength},
            Links ++ fold_links_offset(Key, Worker, Model, Opts2,
                ExpectedSize - LinksLength)
    end.

fold_links_id(Key, Worker, Model, #{prev_link_name := StartLink} = Opts) ->
    {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, Model, fold_links,
        [Key, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], Opts]
    )),

    Filtered = lists:filter(fun(#link{name = Name}) ->
        Name =/= StartLink
    end, Links),
    case Filtered of
        [Last | _] ->
            Opts2 = Opts#{prev_link_name => Last#link.name, prev_tree_id => Last#link.tree_id},
            lists:reverse(Filtered) ++ fold_links_id(Key, Worker, Model, Opts2);
        _ ->
            []
    end.

fold_links_id_and_tree(Key, Worker, Model, Opts) ->
    {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, Model, fold_links,
        [Key, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], Opts]
    )),

    case Links of
        [Last | _] ->
            Opts2 = Opts#{prev_link_name => Last#link.name, prev_tree_id => Last#link.tree_id},
            lists:reverse(Links) ++ fold_links_id_and_tree(Key, Worker, Model, Opts2);
        _ ->
            []
    end.

fold_links_token_id_and_tree(Key, Worker, Model, Opts) ->
    {{ok, Links}, Token} = ?assertMatch({{ok, _}, _}, rpc:call(Worker, Model, fold_links,
        [Key, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], Opts]
    )),
    Reversed = lists:reverse(Links),
    case Token#link_token.is_last of
        true ->
            Reversed;
        _ ->
            [Last | _] = Links,
            Opts2 = Opts#{token => Token, prev_link_name => Last#link.name,
                prev_tree_id => Last#link.tree_id},
            timer:sleep(timer:seconds(10)), % sleep to allow token cache cleaning
            Reversed ++ fold_links_token_id_and_tree(Key, Worker, Model, Opts2)
    end.

fold_links_id_and_neg_offset(Key, Worker, Model, Opts, TmpAns) ->
    {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, Model, fold_links,
        [Key, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], Opts]
    )),

    Links2 = Links -- TmpAns,
    Links2Reversed = lists:reverse(Links2),

    case Links2 of
        [Last | _] ->
            Opts2 = Opts#{prev_link_name => Last#link.name},
            fold_links_id_and_neg_offset(Key, Worker, Model, Opts2,
                TmpAns ++ Links2Reversed);
        _ ->
            TmpAns
    end.

get_link_nodes() ->
    receive
        {link_node_id, NodeID} -> [NodeID | get_link_nodes()]
    after
        1000 -> []
    end.

del_one_by_one(Model, Key, Tree, ExpectedLinkNames) ->
    lists:map(fun(Name) ->
        apply(Model, delete_links, [Key, Tree, Name])
    end, ExpectedLinkNames).


check_direct_access_operation(Worker, Model, Module, Function, Args, ExtendedMemTableCtx) ->
    Ctx = datastore_test_utils:get_ctx(Model),
    MemoryOnlyCtx = Ctx#{disc_driver => undefined, disc_driver_ctx => #{}},
    {ok, Res} = ?assertMatch({ok, _}, rpc:call(Worker, Module, Function, [MemoryOnlyCtx | Args])),
    clean_cache(Worker, Model, ExtendedMemTableCtx),
    ?assertMatch({error, not_found}, rpc:call(Worker, Module, Function, [MemoryOnlyCtx | Args])),
    ?assertMatch({ok, Res}, rpc:call(Worker, Module, Function, [Ctx | Args])),
    ?assertMatch({ok, Res}, rpc:call(Worker, Module, Function, [MemoryOnlyCtx | Args])).


clean_cache(Worker, Model, Ctx) when 
    Model =:= ets_only_model;
    Model =:= ets_cached_model ->
    % wait for documents to be saved on disc
    timer:sleep(timer:seconds(1)),
    rpc:call(Worker, ets_driver, delete_all, [Ctx]);
clean_cache(Worker, Model, Ctx) when
    Model =:= mnesia_only_model;
    Model =:= mnesia_cached_model ->
    % wait for documents to be saved on disc
    timer:sleep(timer:seconds(1)),
    {ok, Keys} = rpc:call(Worker, mnesia_driver, fold, [Ctx, fun(Key, _, Acc) -> {ok, [Key | Acc]} end, []]),
    lists:foreach(fun(Key) ->
        ok = rpc:call(Worker, mnesia_driver, delete, [Ctx, Key])
    end, Keys);
clean_cache(_Worker, _Model, _Ctx) ->
    ok.
    