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

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2]).

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
    fold_should_return_all_values2/1,
    fold_keys_should_return_all_keys/1,
    add_links_should_succeed/1,
    get_links_should_succeed/1,
    get_links_should_return_missing_error/1,
    delete_links_should_succeed/1,
    delete_links_should_ignore_missing_links/1,
    mark_links_deleted_should_succeed/1,
    fold_links_should_succeed/1,
    fold_links_token_should_succeed/1,
    get_links_trees_should_return_all_trees/1,
    fold_links_token_should_succeed_after_token_timeout/1,
    links_performance/1,
    links_performance_base/1,
    create_get_performance/1,
    expired_doc_should_not_exist/1
]).

% for rpc
-export([test_create_get/0, del_one_by_one/4]).

all() ->
    ?ALL([
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
        fold_should_return_all_values2,
        fold_keys_should_return_all_keys,
        add_links_should_succeed,
        get_links_should_succeed,
        get_links_should_return_missing_error,
        delete_links_should_succeed,
        delete_links_should_ignore_missing_links,
        mark_links_deleted_should_succeed,
        fold_links_should_succeed,
        fold_links_token_should_succeed,
        get_links_trees_should_return_all_trees,
        fold_links_token_should_succeed_after_token_timeout,
        links_performance,
        expired_doc_should_not_exist
    ], [
        links_performance,
        create_get_performance
    ]).

-define(DOC(Model), ?DOC(?KEY, Model)).
-define(DOC(Key, Model), ?BASE_DOC(Key, ?MODEL_VALUE(Model))).

-define(ATTEMPTS, 30).

-define(REPEATS, 1).
-define(SUCCESS_RATE, 100).

%%%===================================================================
%%% Test functions
%%%===================================================================

create_get_performance(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {ok, Times} = ?assertMatch({ok, _},
        rpc:call(Worker, ?MODULE, test_create_get, [])),
    ct:print("Times: ~p", [Times]),
    ok.

test_create_get() ->
    ID = ?KEY,
    % Use gs_subscription as example of existing model
    % (model emulation affects results).
    Doc = #document{value = #gs_subscription{}},
    Time0 = os:timestamp(),
    ?assertEqual({error, not_found}, gs_subscription:get(test, ID)),
    Time1 = os:timestamp(),
    ?assertEqual({error, not_found}, gs_subscription:get(test, ID)),
    Time2 = os:timestamp(),
    ?assertMatch({ok, _}, gs_subscription:create(test, ID, Doc)),
    Time3 = os:timestamp(),
    ?assertMatch({ok, _}, gs_subscription:get(test, ID)),
    Time4 = os:timestamp(),
    ?assertMatch({ok, _}, gs_subscription:get(test, ID)),
    Time5 = os:timestamp(),

    Diff1 = timer:now_diff(Time1, Time0),
    Diff2 = timer:now_diff(Time2, Time1),
    Diff3 = timer:now_diff(Time3, Time2),
    Diff4 = timer:now_diff(Time4, Time3),
    Diff5 = timer:now_diff(Time5, Time4),
    {ok, {Diff1, Diff2, Diff3, Diff4, Diff5}}.

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

    T0Add = os:timestamp(),
    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        Key, ?LINK_TREE_ID, ExpectedLinks
    ])),
    T1Add = os:timestamp(),

    T2Add = os:timestamp(),
    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        Key2, ?LINK_TREE_ID, ExpectedLinksHalf
    ])),
    T3Add = os:timestamp(),

    T4Add = os:timestamp(),
    ?assertAllMatch({ok, #link{}}, rpc:call(Worker, Model, add_links, [
        Key2, ?LINK_TREE_ID, ExpectedLinksHalf2
    ])),
    T5Add = os:timestamp(),

    % Test list
    T0List = os:timestamp(),
    {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, Model, fold_links,
        [Key, all, fun(Link, Acc) -> {ok, [Link | Acc]} end, [], #{}]
    )),
    T1List = os:timestamp(),
    ?assertEqual(LinksNum, length(Links)),

    T2List = os:timestamp(),
    Links2 = fold_links_offset(Key2, Worker, Model,
        #{size => 100, offset => 0}, LinksNum),
    T3List = os:timestamp(),
    ?assertEqual(LinksNum, length(Links2)),

    T4List = os:timestamp(),
    Links3 = fold_links_offset(Key2, Worker, Model,
        #{size => 2000, offset => 0}, LinksNum),
    T5List = os:timestamp(),
    ?assertEqual(LinksNum, length(Links3)),

    T6List = os:timestamp(),
    Links4 = fold_links_token(Key, Worker, Model,
        #{size => 100, offset => 0, token => #link_token{}}),
    T7List = os:timestamp(),
    ?assertEqual(LinksNum, length(Links4)),

    T8List = os:timestamp(),
    Links5 = fold_links_token(Key, Worker, Model,
        #{size => 2000, offset => 0, token => #link_token{}}),
    T9List = os:timestamp(),
    ?assertEqual(LinksNum, length(Links5)),

    T10List = os:timestamp(),
    LinksByOffset = fold_links_id(Key, Worker, Model,
        #{size => 2000, prev_link_name => <<>>}),
    T11List = os:timestamp(),
    ?assertEqual(LinksNum, length(LinksByOffset)),

    timer:sleep(500),
    T12List = os:timestamp(),
    LinksByOffset = fold_links_id_and_neg_offset(Key, Worker, Model,
        #{size => 2000, prev_link_name => <<>>, offset => -100}, []),
    T13List = os:timestamp(),
    ?assertEqual(LinksNum, length(LinksByOffset)),

    % Test del
    ExpectedLinkNames = lists:sort(lists:map(fun(N) ->
        ?LINK_NAME(N)
    end, lists:seq(1, LinksNum))),
    ExpectedLinkNamesReversed = lists:reverse(ExpectedLinkNames),

    T0Del = os:timestamp(),
    ?assertAllMatch(ok, rpc:call(Worker, Model, delete_links, [
        Key, ?LINK_TREE_ID, ExpectedLinkNames
    ])),
    T1Del = os:timestamp(),
    Links6 = fold_links_token(Key, Worker, Model,
        #{size => 2000, offset => 0, token => #link_token{}}),
    ?assertEqual(0, length(Links6)),

    T2Del = os:timestamp(),
    ?assertAllMatch(ok, rpc:call(Worker, Model, delete_links, [
        Key2, ?LINK_TREE_ID, ExpectedLinkNamesReversed
    ])),
    T3Del = os:timestamp(),
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
    T4Del = os:timestamp(),
    ?assertAllMatch(ok, rpc:call(Worker, Model, delete_links, [
        Key, ?LINK_TREE_ID, ExpectedLinkNames2
    ])),
    T5Del = os:timestamp(),

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

    T6Del = os:timestamp(),
    ?assertAllMatch(ok, rpc:call(Worker, ?MODULE, del_one_by_one, [
        Model, Key, ?LINK_TREE_ID, ExpectedLinkNames4
    ])),
    T7Del = os:timestamp(),
    Links9 = fold_links_token(Key, Worker, Model,
        #{size => 2000, offset => 0, token => #link_token{}}),
    ?assertEqual(0, length(Links9)),

    % Print results
    AddTime1Diff = timer:now_diff(T1Add, T0Add),
    AddTime2Diff = timer:now_diff(T3Add, T2Add),
    AddTime3Diff = timer:now_diff(T5Add, T4Add),
    ListTimeDiff1 = timer:now_diff(T1List, T0List),
    ListTimeDiff2 = timer:now_diff(T3List, T2List),
    ListTimeDiff3 = timer:now_diff(T5List, T4List),
    ListTimeDiff4 = timer:now_diff(T7List, T6List),
    ListTimeDiff5 = timer:now_diff(T9List, T8List),
    ListTimeDiff6 = timer:now_diff(T11List, T10List),
    ListTimeDiff7 = timer:now_diff(T13List, T12List),
    DelTime1Diff = timer:now_diff(T1Del, T0Del),
    DelTime2Diff = timer:now_diff(T3Del, T2Del),
    DelTime3Diff = timer:now_diff(T5Del, T4Del),
    DelTime4Diff = timer:now_diff(T7Del, T6Del),
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

del_one_by_one(Model, Key, Tree, ExpectedLinkNames) ->
    lists:map(fun(Name) ->
        apply(Model, delete_links, [Key, Tree, Name])
    end, ExpectedLinkNames).


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
    end, [os:system_time(second)+5, 5]).
    

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite(Config).

init_per_testcase(_, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    application:load(cluster_worker),
    application:set_env(cluster_worker, tp_subtrees_number, 10),
    test_utils:set_env(Worker, cluster_worker, tp_subtrees_number, 10),
    Config.

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
    case ?DISC_DRV(Model) of
        undefined ->
            ok;
        Driver ->
            ?assertMatch({ok, _, #document{deleted = Deleted}},
                rpc:call(Worker, Driver, get, [
                    ?DISC_CTX, ?UNIQUE_KEY(Model, Key)
                ]), ?ATTEMPTS
            )
    end.

assert_not_on_disc(Worker, Model, Key) ->
    case ?DISC_DRV(Model) of
        undefined ->
            ok;
        Driver ->
            ?assertMatch({error, not_found},
                rpc:call(Worker, Driver, get, [
                    ?DISC_CTX, ?UNIQUE_KEY(Model, Key)
                ]), ?ATTEMPTS
            )
    end.

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
            Opts2 = Opts#{prev_link_name => Last#link.name},
            lists:reverse(Filtered) ++ fold_links_id(Key, Worker, Model, Opts2);
        _ ->
            []
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