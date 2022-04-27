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
-export([all/0, init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2]).

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
    tree_fold_should_return_targets_inclusive_from_link/1,
    tree_fold_should_return_targets_from_not_existing_id/1,
    tree_fold_should_return_targets_from_link_after_delete/1,
    tree_fold_should_return_targets_using_token_after_delete/1,
    tree_fold_should_return_targets_limited_by_size/1,
    tree_fold_should_return_targets_from_offset_and_limited_by_size/1,
    multi_tree_fold_should_return_all_targets/1,
    multi_tree_fold_should_return_targets_from_offset_and_limited_by_size/1,
    multi_tree_fold_should_return_targets_from_link/1,
    multi_tree_fold_should_return_all_using_ids/1,
    multi_tree_fold_should_return_all_using_ids_with_tree/1,
    multi_tree_fold_should_return_all_using_not_existing_ids/1,
    multi_tree_fold_should_return_all_using_id_with_offset/1,
    multi_tree_fold_should_return_all_using_id_with_neg_offset/1,
    multi_tree_fold_should_return_all_using_id_and_tree_id_with_neg_offset/1,
    multi_tree_fold_should_return_all_using_id_and_tree_id_inclusive_with_neg_offset/1,
    multi_tree_fold_should_return_all_using_empty_id_with_neg_offset/1,
    multi_tree_fold_should_return_non_with_neg_offset/1,
    multi_tree_fold_should_return_all_using_not_existsing_id_with_neg_offset/1,
    multi_sorted_tree_fold_should_return_all_using_not_existing_ids/1,
    multi_sorted_tree_fold_should_return_all_using_id_with_neg_offset/1,
    multi_sorted_tree_fold_should_return_all_using_not_existsing_id_with_neg_offset/1,
    multi_sorted_tree_fold_should_return_all_using_not_existsing_middle_id_with_neg_offset/1,
    multi_tree_fold_should_return_targets_with_token/1,
    multi_tree_fold_should_return_targets_with_token_id_and_tree/1
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
        tree_fold_should_return_targets_inclusive_from_link,
        tree_fold_should_return_targets_from_not_existing_id,
        tree_fold_should_return_targets_from_link_after_delete,
        tree_fold_should_return_targets_using_token_after_delete,
        tree_fold_should_return_targets_limited_by_size,
        tree_fold_should_return_targets_from_offset_and_limited_by_size,
        multi_tree_fold_should_return_all_targets,
        multi_tree_fold_should_return_targets_from_offset_and_limited_by_size,
        multi_tree_fold_should_return_targets_from_link,
        multi_tree_fold_should_return_all_using_ids,
        multi_tree_fold_should_return_all_using_ids_with_tree,
        multi_tree_fold_should_return_all_using_not_existing_ids,
        multi_tree_fold_should_return_all_using_id_with_offset,
        multi_tree_fold_should_return_all_using_id_with_neg_offset,
        multi_tree_fold_should_return_all_using_id_and_tree_id_with_neg_offset,
        multi_tree_fold_should_return_all_using_id_and_tree_id_inclusive_with_neg_offset,
        multi_tree_fold_should_return_all_using_empty_id_with_neg_offset,
        multi_tree_fold_should_return_non_with_neg_offset,
        multi_tree_fold_should_return_all_using_not_existsing_id_with_neg_offset,
        multi_sorted_tree_fold_should_return_all_using_not_existing_ids,
        multi_sorted_tree_fold_should_return_all_using_id_with_neg_offset,
        multi_sorted_tree_fold_should_return_all_using_not_existsing_id_with_neg_offset,
        multi_sorted_tree_fold_should_return_all_using_not_existsing_middle_id_with_neg_offset,
        multi_tree_fold_should_return_targets_with_token,
        multi_tree_fold_should_return_targets_with_token_id_and_tree
    ]).

-define(MODEL, ets_cached_model).
-define(CTX, #{
    model => ?MODEL,
    mutator_pid => self(),
    memory_driver => ?MEM_DRV(?MODEL),
    memory_driver_ctx => ?MEM_CTX(?MODEL),
    disc_driver => ?DISC_DRV(?MODEL),
    disc_driver_ctx => ?DISC_CTX,
    remote_driver => ?REMOTE_DRV
}).
-define(CTX(Key), datastore_multiplier:extend_name(?KEY, ?CTX)).

%%%===================================================================
%%% Test functions
%%%===================================================================

add_link_should_save_link_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinkName = ?LINK_NAME,
    ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, add, [
            [{LinkName, {?LINK_TARGET, undefined}}]
        ]]
    )).

add_link_should_save_link_on_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinkName = ?LINK_NAME,
    ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, add, [
            [{LinkName, {?LINK_TARGET, undefined}}]
        ]]
    )).

get_link_should_return_target_from_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinkName = ?LINK_NAME,
    ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, add, [
            [{LinkName, {?LINK_TARGET, undefined}}]
        ]]
    )),
    {{ok, Link}, _} = ?assertMatch({{ok, #link{}}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, get, [
            ?LINK_NAME
        ]]
    )),
    ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
    ?assertEqual(?LINK_NAME, Link#link.name),
    ?assertEqual(?LINK_TARGET, Link#link.target),
    ?assertMatch(<<_/binary>>, Link#link.tree_id).

get_link_should_return_target_from_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinkName = ?LINK_NAME,
    ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, add, [
            [{LinkName, {?LINK_TARGET, undefined}}]
        ]]
    )),
    {{ok, Link}, _} = ?assertMatch({{ok, #link{}}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, get, [
            ?LINK_NAME
        ]]
    )),
    ?assertEqual(?LINK_TREE_ID, Link#link.tree_id),
    ?assertEqual(?LINK_NAME, Link#link.name),
    ?assertEqual(?LINK_TARGET, Link#link.target),
    ?assertMatch(<<_/binary>>, Link#link.tree_id).

delete_link_should_delete_link_in_memory(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinkName = ?LINK_NAME,
    ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, add, [
            [{LinkName, {?LINK_TARGET, undefined}}]
        ]]
    )),
    ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, delete, [
            [{LinkName, fun(_) -> true end}]
        ]]
    )).

delete_link_should_delete_link_on_disc(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinkName = ?LINK_NAME,
    ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, add, [
            [{LinkName, {?LINK_TARGET, undefined}}]
        ]]
    )),
    ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
        datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, delete, [
            [{LinkName, fun(_) -> true end}]
        ]]
    )).

tree_fold_should_return_all_targets(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinksNum = 1000,
    AllLinks = add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID, LinksNum),
    Links = fold_links(Worker, ?CTX(?KEY), ?KEY, #{}),
    ?assertEqual(get_expected_links(AllLinks), Links).

tree_fold_should_return_targets_from_offset(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinksNum = 1000,
    AllLinks = add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID, LinksNum),
    lists:foreach(fun(Offset) ->
        Links = fold_links(Worker, ?CTX(?KEY), ?KEY, #{offset => Offset}),
        ?assertEqual(get_expected_links(AllLinks, Offset), Links)
    end, lists:seq(0, LinksNum)).

tree_fold_should_return_targets_from_link(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinksNum = 1000,
    AllLinks = add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID, LinksNum),
    Names = lists:map(fun(#link{name = Name}) -> Name end, AllLinks),
    SortedNames = lists:sort(Names),
    lists:foreach(fun({Offset, Name}) ->
        Links = fold_links(Worker, ?CTX(?KEY), ?KEY, #{
            prev_tree_id => ?LINK_TREE_ID,
            prev_link_name => Name
        }),
        ?assertEqual(get_expected_links(AllLinks, Offset), Links)
    end, lists_utils:enumerate(SortedNames)).

tree_fold_should_return_targets_inclusive_from_link(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinksNum = 1000,
    AllLinks = add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID, LinksNum),
    Names = lists:map(fun(#link{name = Name}) -> Name end, AllLinks),
    SortedNames = lists:sort(Names),
    lists:foreach(fun({Offset, Name}) ->
        Links = fold_links(Worker, ?CTX(?KEY), ?KEY, #{
            prev_tree_id => ?LINK_TREE_ID,
            prev_link_name => Name,
            inclusive => true
        }),
        ?assertEqual(get_expected_links(AllLinks, Offset - 1), Links)
    end, lists_utils:enumerate(SortedNames)).

tree_fold_should_return_targets_from_not_existing_id(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinksNum = 1000,

    NamesToAdd = lists:map(fun(N) -> ?LINK_NAME(N) end, lists:seq(1, LinksNum, 2)),
    NamesToCheck = lists:map(fun(N) -> ?LINK_NAME(N) end, lists:seq(0, LinksNum, 2)),
    SortedNamesToAdd = lists:sort(NamesToAdd),

    lists:foreach(fun(LinkName) ->
        ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
            datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, add, [
                [{LinkName, {LinkName, undefined}}]
            ]]
        ))
    end, NamesToAdd),

    lists:foreach(fun(Name) ->
        ReturnedLinks = fold_links(Worker, ?CTX(?KEY), ?KEY, #{
            prev_tree_id => ?LINK_TREE_ID,
            prev_link_name => Name
        }),

        ReturnedNames = lists:map(fun(#link{name = LinkName}) -> LinkName end, ReturnedLinks),
        ExpectedNames = lists:dropwhile(fun(ExpectedName) -> ExpectedName < Name end, SortedNamesToAdd),
        ?assertEqual(ExpectedNames, ReturnedNames)
    end, NamesToCheck).

tree_fold_should_return_targets_from_link_after_delete(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    % Links number and test cases are selected to reproduce error that has been observed when
    % listing after link's delete started near ending of document storing links inside bp_tree.
    LinksNum = 1650,

    % Prepare test - create links
    Names = lists:map(fun(N) -> ?LINK_NAME(N) end, lists:seq(1, LinksNum)),
    lists:foreach(fun(LinkName) ->
        ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
            datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, add, [
                [{LinkName, {LinkName, undefined}}]
            ]]
        ))
    end, Names),

    % Prepare test cases
    SortedNames = lists:sort(Names),
    BorderNames = [?LINK_NAME(412), ?LINK_NAME(432), ?LINK_NAME(641), ?LINK_NAME(815)],

    % Execute each test case
    lists:foldl(fun(DeleteUpTo, RemainingNames) ->
        % Remove links before listing
        ToDelete = lists:takewhile(fun(Name) -> Name =< DeleteUpTo end, RemainingNames),
        lists:foreach(fun(LinkName) ->
            ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
                datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, delete, [
                    [{LinkName, fun(_) -> true end}]
                ]]
            ))
        end, ToDelete),
        NamesAfterDelete = RemainingNames -- ToDelete,

        % List links
        LinksToCheck = fold_links(Worker, ?CTX(?KEY), ?KEY, #{
            prev_tree_id => ?LINK_TREE_ID,
            prev_link_name => DeleteUpTo
        }),

        % Verify answer
        NamesToCheck = lists:map(fun(#link{name = Name}) -> Name end, LinksToCheck),
        ?assertEqual(NamesAfterDelete, NamesToCheck),
        NamesAfterDelete
    end, SortedNames, BorderNames).

tree_fold_should_return_targets_using_token_after_delete(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    % Links number and test cases are selected to reproduce error that has been observed when
    % listing after link's delete started near ending of document storing links inside bp_tree.
    LinksNum = 1650,

    % Prepare test - create links
    Names = lists:map(fun(N) -> ?LINK_NAME(N) end, lists:seq(1, LinksNum)),
    lists:foreach(fun(LinkName) ->
        ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
            datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, add, [
                [{LinkName, {LinkName, undefined}}]
            ]]
        ))
    end, Names),

    % Prepare test cases
    SortedNames = lists:sort(Names),
    TestCases = [{<<>>, ?LINK_NAME(412), false}, {?LINK_NAME(412), ?LINK_NAME(631), false},
        {?LINK_NAME(641), ?LINK_NAME(825), true}, {?LINK_NAME(815), ?LINK_NAME(999), false}],

    % Execute each test case
    lists:foldl(fun({DeleteUpTo, ListUpTo, AdditionalLinksPossible}, {RemainingNames, Token, ListedTo}) ->
        % Remove links before listing
        ToDelete = lists:takewhile(fun(Name) -> Name =< DeleteUpTo end, RemainingNames),
        lists:foreach(fun(LinkName) ->
            ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
                datastore_links_crud, apply, [?CTX(?KEY), ?KEY, ?LINK_TREE_ID, delete, [
                    [{LinkName, fun(_) -> true end}]
                ]]
            ))
        end, ToDelete),
        NamesAfterDelete = RemainingNames -- ToDelete,

        % List links
        ExpectedNames = lists:takewhile(fun(Name) -> Name =< ListUpTo end,
            lists:dropwhile(fun(Name) -> Name =< ListedTo end, NamesAfterDelete)),
        {{ok, ReversedLinksToCheck}, NewToken} = ?assertMatch({{ok, _}, _}, rpc:call(Worker, datastore_links_iter,
            fold, [?CTX(?KEY), ?KEY, all, fun(Link, Acc) ->
                {ok, [Link | Acc]}
            end, [], #{token => Token, size => length(ExpectedNames)}]
        )),

        % Verify answer
        case AdditionalLinksPossible of
            true ->
                % Listing can return additional links cached in token
                % Check possible number of additional links and fold once more
                NamesToCheck = lists:map(fun(#link{name = Name}) -> Name end, lists:reverse(ReversedLinksToCheck)),
                MissingLinks = ExpectedNames -- NamesToCheck,
                {{ok, ReversedLinksToCheck2}, NewToken2} = ?assertMatch({{ok, _}, _}, rpc:call(Worker, datastore_links_iter,
                    fold, [?CTX(?KEY), ?KEY, all, fun(Link, Acc) ->
                        {ok, [Link | Acc]}
                    end, [], #{token => NewToken, size => length(MissingLinks)}]
                )),

                % All expected links should be present in answer together with additional links
                NamesToCheck2 = lists:map(fun(#link{name = Name}) -> Name end, lists:reverse(ReversedLinksToCheck2)),
                ?assertEqual([], (ExpectedNames -- NamesToCheck) -- NamesToCheck2),
                {NamesAfterDelete, NewToken2, ListUpTo};
            false ->
                % Listing should return expected links without any additional links
                NamesToCheck = lists:map(fun(#link{name = Name}) -> Name end, lists:reverse(ReversedLinksToCheck)),
                ?assertEqual(ExpectedNames, NamesToCheck),
                {NamesAfterDelete, NewToken, ListUpTo}
        end
    end, {SortedNames, #link_token{}, <<>>}, TestCases).

tree_fold_should_return_targets_limited_by_size(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinksNum = 1000,
    AllLinks = add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID, LinksNum),
    lists:foreach(fun(Size) ->
        Links = fold_links(Worker, ?CTX(?KEY), ?KEY, #{size => Size}),
        ?assertEqual(get_expected_links(AllLinks, 0, Size), Links)
    end, lists:seq(0, LinksNum)).

tree_fold_should_return_targets_from_offset_and_limited_by_size(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    LinksNum = 100,
    AllLinks = add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID, LinksNum),
    lists:foreach(fun(Offset) ->
        lists:foreach(fun(Size) ->
            Links = fold_links(Worker, ?CTX(?KEY), ?KEY, #{
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
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),
    Links = fold_links(Worker, ?CTX(?KEY), ?KEY, #{}),
    ?assertEqual(get_expected_links(AllLinks), Links).


multi_tree_fold_should_return_targets_from_offset_and_limited_by_size(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 500,
    Incr = 100,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),

    Links = lists:foldl(fun(Offset, Acc) ->
        Acc ++ fold_links(Worker, ?CTX(?KEY), ?KEY, #{offset => Offset, size => Incr})
    end, [], lists:seq(0, TreesNum * LinksNum, Incr)),
    ?assertEqual(get_expected_links(AllLinks), Links).

multi_tree_fold_should_return_targets_with_token(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 500,
    Incr = 100,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),

    Links = fold_links_token(Worker, ?CTX(?KEY), ?KEY,
            #{token => #link_token{}, offset => 0, size => Incr}),
    ?assertEqual(get_expected_links(AllLinks), Links).

multi_tree_fold_should_return_targets_with_token_id_and_tree(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 500,
    Incr = 100,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),

    Links = fold_links_token_id_and_tree(Worker, ?CTX(?KEY), ?KEY,
        #{token => #link_token{}, size => Incr}),
    ?assertEqual(get_expected_links(AllLinks), Links).

multi_tree_fold_should_return_targets_from_link(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 500,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),
    LinkPoints = lists:map(fun(#link{tree_id = TreeId, name = Name}) ->
        {Name, TreeId}
    end, AllLinks),
    LinkPointsSorted = lists:sort(LinkPoints),
    lists:foreach(fun({Offset, {Name, TreeId}}) ->
        Links = fold_links(Worker, ?CTX(?KEY), ?KEY, #{
            prev_tree_id => TreeId,
            prev_link_name => Name
        }),
        ?assertEqual(get_expected_links(AllLinks, Offset), Links)
    end, lists_utils:enumerate(LinkPointsSorted)).

multi_tree_fold_should_return_all_using_ids(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 500,
    Incr = 100,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),

    Links = fold_links_id(Worker, ?CTX(?KEY), ?KEY, #{size => Incr,
        prev_link_name => <<>>}),
    ?assertEqual(get_expected_links(AllLinks), Links).

multi_tree_fold_should_return_all_using_ids_with_tree(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 500,
    Incr = 100,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),

    Links = fold_links_id_and_tree(Worker, ?CTX(?KEY), ?KEY, #{size => Incr,
        prev_link_name => <<>>}),
    ?assertEqual(get_expected_links(AllLinks), Links).

multi_tree_fold_should_return_all_using_not_existing_ids(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 1000,
    Incr = 100,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),

    Links = fold_links_non_existing_id(Worker, ?CTX(?KEY), ?KEY, #{size => Incr,
        prev_link_name => <<>>}),

    ?assertEqual(get_expected_links(AllLinks), Links).

multi_tree_fold_should_return_all_using_id_with_offset(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 500,
    Incr = 100,
    Offset = 50,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),

    Links = fold_links_id_and_tree(Worker, ?CTX(?KEY), ?KEY, #{size => Incr,
        prev_link_name => <<>>, offset => Offset}),
    Check = filter_expected_links(get_expected_links(AllLinks), [], Offset, Incr),
    ?assertEqual(Check, Links).

multi_tree_fold_should_return_all_using_id_with_neg_offset(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 1000,
    Incr = 450,
    Offset = -250,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),

    ExpectedLinks = get_expected_links(AllLinks),
    #link{name = StartName} = lists:nth(abs(Offset) + 1, ExpectedLinks),
    Links = fold_links_id_and_neg_offset(Worker, ?CTX(?KEY), ?KEY, #{size => Incr,
        prev_link_name => StartName, offset => Offset}, [], false, ExpectedLinks),
    ?assertEqual(ExpectedLinks, Links),

    #link{name = StartName2} = lists:nth(abs(Offset) + 6, ExpectedLinks),
    Links2 = fold_links_id_and_neg_offset(Worker, ?CTX(?KEY), ?KEY, #{size => Incr,
        prev_link_name => StartName2, offset => Offset}, [], false, undefined),
    ?assert(length(ExpectedLinks) > length(Links2)).


multi_tree_fold_should_return_all_using_id_and_tree_id_with_neg_offset(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 1000,
    Offset = -250,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),

    [_ | ExpectedLinks] = get_expected_links(AllLinks),
    #link{name = StartName, tree_id = StartTreeId} = lists:nth(abs(Offset), ExpectedLinks),
    {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, datastore_links_iter, fold, [
        ?CTX(?KEY), ?KEY, all,
        fun(Link, Acc) ->
            {ok, [Link | Acc]}
        end,
        [], #{size => 10000, prev_link_name => StartName, prev_tree_id => StartTreeId, offset => Offset}
    ])),
    ?assertEqual(ExpectedLinks, lists:reverse(Links)).


multi_tree_fold_should_return_all_using_id_and_tree_id_inclusive_with_neg_offset(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 1000,
    Offset = -250,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),

    ExpectedLinks = get_expected_links(AllLinks),
    #link{name = StartName, tree_id = StartTreeId} = lists:nth(abs(Offset) + 1, ExpectedLinks),
    {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, datastore_links_iter, fold, [
        ?CTX(?KEY), ?KEY, all,
        fun(Link, Acc) ->
            {ok, [Link | Acc]}
        end,
        [],
        #{size => 10000, prev_link_name => StartName, prev_tree_id => StartTreeId, offset => Offset, inclusive => true}
    ])),
    ?assertEqual(ExpectedLinks, lists:reverse(Links)).


multi_tree_fold_should_return_all_using_empty_id_with_neg_offset(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 3,
    LinksNum = 1,
    Size = 1000,
    Offset = -100,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),

    ExpectedLinks = get_expected_links(AllLinks),
    Links = fold_links_id_and_neg_offset(Worker, ?CTX(?KEY), ?KEY, #{size => Size,
        prev_link_name => <<>>, offset => Offset}, [], false, ExpectedLinks),
    ?assertEqual(ExpectedLinks, Links).

multi_tree_fold_should_return_non_with_neg_offset(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    Size = 1000,
    Offset = -100,

    ExpectedLinks = [],
    Links = fold_links_id_and_neg_offset(Worker, ?CTX(?KEY), ?KEY, #{size => Size,
        prev_link_name => <<>>, offset => Offset}, [], false, ExpectedLinks),
    ?assertEqual(ExpectedLinks, Links).

multi_tree_fold_should_return_all_using_not_existsing_id_with_neg_offset(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 1000,
    Incr = 100,
    Offset = -50,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links(Worker, ?CTX(?KEY), ?KEY, ?LINK_TREE_ID(N), LinksNum)
    end, lists:seq(1, TreesNum))),

    ExpectedLinks = get_expected_links(AllLinks),
    #link{name = StartName} = lists:nth(abs(Offset), ExpectedLinks),
    Links = fold_links_id_and_neg_offset(Worker, ?CTX(?KEY), ?KEY, #{size => Incr,
        prev_link_name => <<StartName/binary, ".....">>, offset => Offset},
        [], true, ExpectedLinks),
    ?assertEqual(ExpectedLinks, Links).

multi_sorted_tree_fold_should_return_all_using_not_existing_ids(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 500,
    Incr = 100,
    Offset = 50,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links_with_ids(Worker, ?CTX(?KEY), ?KEY, N, LinksNum)
    end, lists:seq(1, TreesNum))),

    Links = fold_links_non_existing_id(Worker, ?CTX(?KEY), ?KEY, #{size => Incr,
        prev_link_name => <<>>, offset => Offset}),
    Check = filter_expected_links(get_expected_links(AllLinks), [], Offset, Incr),
    ?assertEqual(Check, Links).

multi_sorted_tree_fold_should_return_all_using_id_with_neg_offset(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 1000,
    Incr = 450,
    Offset = -250,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links_with_ids(Worker, ?CTX(?KEY), ?KEY, N, LinksNum)
    end, lists:seq(1, TreesNum))),

    ExpectedLinks = get_expected_links(AllLinks),
    #link{name = StartName} = lists:nth(abs(Offset) + 1, ExpectedLinks),
    Links = fold_links_id_and_neg_offset(Worker, ?CTX(?KEY), ?KEY, #{size => Incr,
        prev_link_name => StartName, offset => Offset}, [], false, ExpectedLinks),
    ?assertEqual(ExpectedLinks, Links).

multi_sorted_tree_fold_should_return_all_using_not_existsing_id_with_neg_offset(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 1000,
    Incr = 100,
    Offset = -50,
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links_with_ids(Worker, ?CTX(?KEY), ?KEY, N, LinksNum)
    end, lists:seq(1, TreesNum))),

    ExpectedLinks = get_expected_links(AllLinks),
    #link{name = StartName} = lists:nth(abs(Offset), ExpectedLinks),
    Links = fold_links_id_and_neg_offset(Worker, ?CTX(?KEY), ?KEY, #{size => Incr,
        prev_link_name => <<StartName/binary, ".....">>, offset => Offset},
        [], true, ExpectedLinks),
    ?assertEqual(ExpectedLinks, Links).

multi_sorted_tree_fold_should_return_all_using_not_existsing_middle_id_with_neg_offset(Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),
    TreesNum = 5,
    LinksNum = 1000,
    Incr = 600,
    Offset = -450,
    Start = 128 * (2 * TreesNum + 1),
    AllLinks = lists:flatten(lists:map(fun(N) ->
        add_links_with_ids(Worker, ?CTX(?KEY), ?KEY, N, LinksNum)
    end, lists:seq(1, TreesNum))),

    ExpectedLinks0 = get_expected_links(AllLinks),
    #link{name = StartName} = lists:nth(Start, ExpectedLinks0),
    ExpectedLinks = lists:sublist(ExpectedLinks0, Start + Offset + 1,
        length(ExpectedLinks0) - Start - Offset),
    Links = fold_links_id_and_neg_offset(Worker, ?CTX(?KEY), ?KEY, #{size => Incr,
        prev_link_name => <<StartName/binary, ".....">>, offset => Offset},
        [], true, ExpectedLinks),
    ?assertEqual(ExpectedLinks, Links).

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite([?MODEL], Config).

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    [Worker | _] = ?config(cluster_worker_nodes, Config),

    application:load(cluster_worker),
    application:set_env(cluster_worker, tp_subtrees_number, 1),
    test_utils:set_env(Worker, cluster_worker, tp_subtrees_number, 1),
    % Use low tree order for better test splitting links to multiple documents
    test_utils:set_env(Worker, cluster_worker, datastore_links_tree_order, 64),

    rpc:call(Worker, datastore_cache_manager, reset, [disc1]),
    rpc:call(Worker, datastore_cache_manager, resize, [disc1, cache_size(Case)]),
    Config.

end_per_testcase(_Case, _Config) ->
    timer:sleep(5000),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

cache_size(add_link_should_save_link_on_disc) -> 0;
cache_size(get_link_should_return_target_from_disc) -> 0;
cache_size(delete_link_should_delete_link_on_disc) -> 0;
cache_size(_Case) -> 10000.

add_links(Worker, Ctx, Key, TreeId, LinksNum) ->
    lists:map(fun(N) ->
        LinkName = ?LINK_NAME(N),
        ?assertMatch({{ok, [LinkName]}, _}, rpc:call(Worker,
            datastore_links_crud, apply, [Ctx, Key, TreeId, add, [
                [{LinkName, {?LINK_TARGET(N), undefined}}]
            ]]
        )),
        {{ok, Link}, _} = ?assertMatch({{ok, #link{}}, _}, rpc:call(Worker,
            datastore_links_crud, apply, [Ctx, Key, TreeId, get, [
                LinkName
            ]]
        )),
        Link
    end, lists:seq(1, LinksNum)).

add_links_with_ids(Worker, Ctx, Key, TreeId, LinksNum) ->
    lists:map(fun(N) ->
        Name = <<"link-", (?CASE)/binary, "-",
            (integer_to_binary(10-TreeId))/binary, "-",
            (integer_to_binary(N))/binary>>,

        ?assertMatch({{ok, [Name]}, _}, rpc:call(Worker,
            datastore_links_crud, apply, [Ctx, Key, ?LINK_TREE_ID(TreeId), add, [
                [{Name, {?LINK_TARGET(N), undefined}}]
            ]]
        )),
        {{ok, Link}, _} = ?assertMatch({{ok, #link{}}, _}, rpc:call(Worker,
            datastore_links_crud, apply, [Ctx, Key, ?LINK_TREE_ID(TreeId), get, [
                Name
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

fold_links_id(Worker, Ctx, Key, #{prev_link_name := StartLink} = Opts) ->
    {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, datastore_links_iter,
        fold, [Ctx, Key, all, fun(Link, Acc) ->
            {ok, [Link | Acc]}
        end, [], Opts]
    )),

    Filtered = lists:filter(fun(#link{name = Name}) ->
        Name =/= StartLink
    end, Links),
    case Filtered of
        [Last | _] ->
            Opts2 = Opts#{prev_link_name => Last#link.name},
            lists:reverse(Filtered) ++ fold_links_id(Worker, Ctx, Key, Opts2);
        _ ->
            []
    end.

fold_links_non_existing_id(Worker, Ctx, Key, Opts) ->
    {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, datastore_links_iter,
        fold, [Ctx, Key, all, fun(Link, Acc) ->
            {ok, [Link | Acc]}
        end, [], Opts]
    )),

    case Links of
        [Last | _] ->
            LastBin = Last#link.name,
            Opts2 = Opts#{prev_link_name => <<LastBin/binary, ".....">>},
            lists:reverse(Links) ++ fold_links_non_existing_id(Worker, Ctx, Key, Opts2);
        _ ->
            []
    end.

fold_links_id_and_neg_offset(Worker, Ctx, Key, Opts, TmpAns, ExtendName, ExpectedLinks) ->
    {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, datastore_links_iter,
        fold, [Ctx, Key, all, fun(Link, Acc) ->
            {ok, [Link | Acc]}
        end, [], Opts]
    )),

    Links2 = Links -- TmpAns,
    Links2Reversed = lists:reverse(Links2),

    ExpectedLinks2 = case ExpectedLinks of
        undefined ->
            ExpectedLinks;
        _ ->
            L2Len = length(Links2),
            ?assertEqual(lists:sublist(ExpectedLinks, 1, L2Len), Links2Reversed),
            lists:sublist(ExpectedLinks, L2Len + 1, length(ExpectedLinks) - L2Len)

    end,

    case Links2 of
        [Last | _] ->
            Opts2 = case ExtendName of
                true ->
                    LastBin = Last#link.name,
                    Opts#{prev_link_name => <<LastBin/binary, ".....">>};
                _ ->
                    Opts#{prev_link_name => Last#link.name}
            end,
            fold_links_id_and_neg_offset(Worker, Ctx, Key, Opts2,
                TmpAns ++ Links2Reversed, ExtendName, ExpectedLinks2);
        _ ->
            TmpAns
    end.

fold_links_id_and_tree(Worker, Ctx, Key, Opts) ->
    {ok, Links} = ?assertMatch({ok, _}, rpc:call(Worker, datastore_links_iter,
        fold, [Ctx, Key, all, fun(Link, Acc) ->
            {ok, [Link | Acc]}
        end, [], Opts]
    )),

    case Links of
        [Last | _] ->
            Reversed = lists:reverse(Links),
            Opts2 = Opts#{prev_link_name => Last#link.name,
                prev_tree_id => Last#link.tree_id},
            Reversed ++ fold_links_id_and_tree(Worker, Ctx, Key, Opts2);
        _ ->
            []
    end.

fold_links_token(Worker, Ctx, Key, Opts) ->
    {{ok, Links}, Token} = ?assertMatch({{ok, _}, _}, rpc:call(Worker, datastore_links_iter,
        fold, [Ctx, Key, all, fun(Link, Acc) ->
            {ok, [Link | Acc]}
        end, [], Opts]
    )),
    Reversed = lists:reverse(Links),
    case Token#link_token.is_last of
        true ->
            Reversed;
        _ ->
            Opts2 = Opts#{token => Token},
            Reversed ++ fold_links_token(Worker, Ctx, Key, Opts2)
    end.

fold_links_token_id_and_tree(Worker, Ctx, Key, Opts) ->
    {{ok, Links}, Token} = ?assertMatch({{ok, _}, _}, rpc:call(Worker, datastore_links_iter,
        fold, [Ctx, Key, all, fun(Link, Acc) ->
            {ok, [Link | Acc]}
        end, [], Opts]
    )),
    Reversed = lists:reverse(Links),
    case Token#link_token.is_last of
        true ->
            Reversed;
        _ ->
            [Last | _] = Links,
            Opts2 = Opts#{token => Token#link_token{restart_token = undefined},
                prev_link_name => Last#link.name, prev_tree_id => Last#link.tree_id},
            Reversed ++ fold_links_token_id_and_tree(Worker, Ctx, Key, Opts2)
    end.

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

filter_expected_links(Links, Acc, Offset, Size) ->
    Length = length(Links),
    case Length =< Offset of
        true ->
            Acc;
        _ ->
            Links2 = lists:sublist(Links, Offset + 1, Length),
            case length(Links2) =< Size of
                true ->
                    Acc ++ Links2;
                _ ->
                    Links3 = lists:sublist(Links2, Size),
                    Links4 = lists:sublist(Links2, Size + 1, Length),
                    filter_expected_links(Links4, Acc ++ Links3, Offset, Size)
            end
    end.