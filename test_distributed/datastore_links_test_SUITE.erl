%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore links tests.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_links_test_SUITE).
-author("Krzysztof Trzepla").

-include("datastore_test_utils.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2]).

%% tests
-export([
    add_link_should_save_link_in_memory/1,
    add_link_should_save_link_on_disc/1,
    get_link_should_return_target_from_memory/1,
    get_link_should_return_target_from_disc/1,
    delete_link_should_delete_link_in_memory/1,
    delete_link_should_delete_link_on_disc/1,
    tree_fold_should_return_all_targets/1,
    tree_fold_should_return_targets_from_offset/1,
    tree_fold_should_return_targets_from_link/1,
    tree_fold_should_return_targets_limited_by_size/1,
    tree_fold_should_return_targets_from_offset_and_limited_by_size/1,
    multi_tree_fold_should_return_all_targets/1,
    multi_tree_fold_should_return_targets_from_link/1
]).

all() ->
    ?ALL([
        add_link_should_save_link_in_memory,
        add_link_should_save_link_on_disc,
        get_link_should_return_target_from_memory,
        get_link_should_return_target_from_disc,
        delete_link_should_delete_link_in_memory,
        delete_link_should_delete_link_on_disc,
        tree_fold_should_return_all_targets,
        tree_fold_should_return_targets_from_offset,
        tree_fold_should_return_targets_from_link,
        tree_fold_should_return_targets_limited_by_size,
        tree_fold_should_return_targets_from_offset_and_limited_by_size,
        multi_tree_fold_should_return_all_targets,
        multi_tree_fold_should_return_targets_from_link
    ]).

-define(MODEL, ets_cached_model).
-define(MEM_CTX, ?MEM_CTX(?MODEL)).
-define(CTX, #{
    model => ?MODEL,
    mutator_pid => self(),
    memory_driver => ?MEM_DRV(?MODEL),
    memory_driver_ctx => ?MEM_CTX(?MODEL),
    disc_driver => ?DISC_DRV(?MODEL),
    disc_driver_ctx => ?DISC_CTX,
    remote_driver => ?REMOTE_DRV
}).

%%%===================================================================
%%% Test functions
%%%===================================================================

add_link_should_save_link_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {{ok, Link}, _} = ?assertMatch({{ok, #link{}}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX, ?KEY, ?LINK_TREE_ID, add, [
            ?LINK_NAME, ?LINK_TARGET
        ]]
    )),
    ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
    ?assertEqual(?LINK_NAME, Link#link.name),
    ?assertEqual(?LINK_TARGET, Link#link.target),
    ?assertMatch(<<_/binary>>, Link#link.tree_id).

add_link_should_save_link_on_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    {{ok, Link}, _} = ?assertMatch({{ok, #link{}}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX, ?KEY, ?LINK_TREE_ID, add, [
            ?LINK_NAME, ?LINK_TARGET
        ]]
    )),
    ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
    ?assertEqual(?LINK_NAME, Link#link.name),
    ?assertEqual(?LINK_TARGET, Link#link.target),
    ?assertMatch(<<_/binary>>, Link#link.tree_id).

get_link_should_return_target_from_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({{ok, #link{}}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX, ?KEY, ?LINK_TREE_ID, add, [
            ?LINK_NAME, ?LINK_TARGET
        ]]
    )),
    {{ok, Link}, _} = ?assertMatch({{ok, #link{}}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX, ?KEY, ?LINK_TREE_ID, get, [
            ?LINK_NAME
        ]]
    )),
    ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
    ?assertEqual(?LINK_NAME, Link#link.name),
    ?assertEqual(?LINK_TARGET, Link#link.target),
    ?assertMatch(<<_/binary>>, Link#link.tree_id).

get_link_should_return_target_from_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({{ok, #link{}}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX, ?KEY, ?LINK_TREE_ID, add, [
            ?LINK_NAME, ?LINK_TARGET
        ]]
    )),
    {{ok, Link}, _} = ?assertMatch({{ok, #link{}}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX, ?KEY, ?LINK_TREE_ID, get, [
            ?LINK_NAME
        ]]
    )),
    ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
    ?assertEqual(?LINK_NAME, Link#link.name),
    ?assertEqual(?LINK_TARGET, Link#link.target),
    ?assertMatch(<<_/binary>>, Link#link.tree_id).

delete_link_should_delete_link_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({{ok, #link{}}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX, ?KEY, ?LINK_TREE_ID, add, [
            ?LINK_NAME, ?LINK_TARGET
        ]]
    )),
    ?assertMatch({ok, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX, ?KEY, ?LINK_TREE_ID, delete, [
            ?LINK_NAME
        ]]
    )).

delete_link_should_delete_link_on_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    ?assertMatch({{ok, #link{}}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX, ?KEY, ?LINK_TREE_ID, add, [
            ?LINK_NAME, ?LINK_TARGET
        ]]
    )),
    ?assertMatch({ok, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX, ?KEY, ?LINK_TREE_ID, delete, [
            ?LINK_NAME
        ]]
    )).

tree_fold_should_return_all_targets(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinksNum = 1000,
    AllLinks = add_links(Worker, ?CTX, ?KEY, ?LINK_TREE_ID, LinksNum),
    Links = fold_links(Worker, ?CTX, ?KEY, #{}),
    ?assertEqual(get_expected_links(AllLinks), Links).

tree_fold_should_return_targets_from_offset(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinksNum = 1000,
    AllLinks = add_links(Worker, ?CTX, ?KEY, ?LINK_TREE_ID, LinksNum),
    lists:foreach(fun(Offset) ->
        Links = fold_links(Worker, ?CTX, ?KEY, #{offset => Offset}),
        ?assertEqual(get_expected_links(AllLinks, Offset), Links)
    end, lists:seq(0, LinksNum)).

tree_fold_should_return_targets_from_link(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinksNum = 1000,
    AllLinks = add_links(Worker, ?CTX, ?KEY, ?LINK_TREE_ID, LinksNum),
    Names = lists:map(fun(#link{name = Name}) -> Name end, AllLinks),
    lists:foreach(fun({Offset, Name}) ->
        Links = fold_links(Worker, ?CTX, ?KEY, #{
            prev_tree_id => ?LINK_TREE_ID,
            prev_link_name => Name
        }),
        ?assertEqual(get_expected_links(AllLinks, Offset), Links)
    end, lists:zip(lists:seq(1, LinksNum), lists:sort(Names))).

tree_fold_should_return_targets_limited_by_size(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinksNum = 1000,
    AllLinks = add_links(Worker, ?CTX, ?KEY, ?LINK_TREE_ID, LinksNum),
    lists:foreach(fun(Size) ->
        Links = fold_links(Worker, ?CTX, ?KEY, #{size => Size}),
        ?assertEqual(get_expected_links(AllLinks, 0, Size), Links)
    end, lists:seq(0, LinksNum)).

tree_fold_should_return_targets_from_offset_and_limited_by_size(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinksNum = 100,
    AllLinks = add_links(Worker, ?CTX, ?KEY, ?LINK_TREE_ID, LinksNum),
    lists:foreach(fun(Offset) ->
        lists:foreach(fun(Size) ->
            Links = fold_links(Worker, ?CTX, ?KEY, #{
                offset => Offset,
                size => Size
            }),
            ?assertEqual(get_expected_links(AllLinks, Offset, Size), Links)
        end, lists:seq(0, LinksNum - Offset))
    end, lists:seq(0, LinksNum)).

multi_tree_fold_should_return_all_targets(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 500,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX, ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),
    Links = fold_links(Worker, ?CTX, ?KEY, #{}),
    ?assertEqual(get_expected_links(AllLinks), Links).

multi_tree_fold_should_return_targets_from_link(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 500,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX, ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),
    LinkPoints = lists:map(fun(#link{tree_id = TreeId, name = Name}) ->
        {Name, TreeId}
    end, AllLinks),
    lists:foreach(fun({Offset, {Name, TreeId}}) ->
        Links = fold_links(Worker, ?CTX, ?KEY, #{
            prev_tree_id => TreeId,
            prev_link_name => Name
        }),
        ?assertEqual(get_expected_links(AllLinks, Offset), Links)
    end, lists:zip(lists:seq(1, TreesNum * LinksNum), lists:sort(LinkPoints))).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite([?MODEL], Config).

init_per_testcase(Case, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    rpc:call(Worker, datastore_cache_manager, reset, [disc]),
    rpc:call(Worker, datastore_cache_manager, resize, [disc, cache_size(Case)]),
    Config.

%%%===================================================================
%%% Internal functions
%%%===================================================================

cache_size(add_link_should_save_link_on_disc) -> 0;
cache_size(get_link_should_return_target_from_disc) -> 0;
cache_size(delete_link_should_delete_link_on_disc) -> 0;
cache_size(_Case) -> 10000.

add_links(Worker, Ctx, Key, TreeId, LinksNum) ->
    lists:map(fun(N) ->
        {{ok, Link}, _} = ?assertMatch({{ok, #link{}}, _}, rpc:call(Worker,
            datastore_links_crud, apply, [Ctx, Key, TreeId, add, [
                ?LINK_NAME(N), ?LINK_TARGET(N)
            ]]
        )),
        Link
    end, lists:seq(1, LinksNum)).

fold_links(Worker, Ctx, Key, Opts) ->
    {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, datastore_links_iter,
        fold, [Ctx, Key, all, fun(Link, Acc) ->
            {ok, [Link | Acc]}
        end, [], Opts]
    )),
    lists:reverse(Links).

get_expected_links(Links) ->
    get_expected_links(Links, 0).

get_expected_links(Links, Offset) ->
    get_expected_links(Links, Offset, length(Links)).

get_expected_links(Links, Offset, Size) ->
    SortedLinks = lists:sort(fun(
        #link{tree_id = TreeId1, name = Name1},
        #link{tree_id = TreeId2, name = Name2}
    ) ->
        {Name1, TreeId1} < {Name2, TreeId2}
    end, Links),
    lists:sublist(SortedLinks, Offset + 1, Size).