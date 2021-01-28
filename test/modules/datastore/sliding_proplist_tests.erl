%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Eunit tests for the sliding proplist module.
%%% @end
%%%-------------------------------------------------------------------
-module(sliding_proplist_tests).
-author("Michal Stanisz").

-ifdef(TEST).

-include("modules/datastore/sliding_proplist.hrl").
-include_lib("eunit/include/eunit.hrl").


%%%===================================================================
%%% Setup and teardown
%%%===================================================================

sliding_proplist_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            {"test_create_and_destroy", fun test_create_and_destroy/0},
            {"test_insert_elements_one_node", fun test_insert_elements_one_node/0},
            {"test_insert_elements_multi_nodes", fun test_insert_elements_multi_nodes/0},
            {"test_insert_elements_only_existing", fun test_insert_elements_only_existing_elements/0},
            {"test_insert_elements_with_overwrite", fun test_insert_elements_with_overwrite/0},
            {"test_list_with_listing_state", fun test_list_with_listing_state/0},
            {"test_list_with_listing_state_start_from_last", fun test_list_with_listing_state_start_from_last/0},
            {"test_fold_elements", fun test_fold_elements/0},
            {"test_fold_elements_stop", fun test_fold_elements_stop/0},
            {"test_remove_consecutive_elements_between_nodes", fun test_remove_consecutive_elements_between_nodes/0},
            {"test_remove_non_consecutive_elements_between_nodes", fun test_remove_non_consecutive_elements_between_nodes/0},
            {"test_remove_all_elements_in_first_node", fun test_remove_all_elements_in_first_node/0},
            {"test_remove_elements_all_but_first_node", fun test_remove_elements_all_but_first_node/0},
            {"test_first_node_merge_during_remove", fun test_first_node_merge_during_remove/0},
            {"test_remove_elements_one_by_one_descending", fun test_remove_elements_one_by_one_descending/0},
            {"test_remove_elements_one_by_one_ascending", fun test_remove_elements_one_by_one_ascending/0},
            {"test_remove_elements_structure_not_sorted", fun test_remove_elements_structure_not_sorted/0},
            {"test_merge_nodes_during_remove_structure_not_sorted", fun test_merge_nodes_during_remove_structure_not_sorted/0},
            {"test_remove_elements_between_listings", fun test_remove_elements_between_listings/0},
            {"test_get_elements", fun test_get_elements/0},
            {"test_get_elements_structure_not_sorted", fun test_get_elements_structure_not_sorted/0},
            {"test_get_elements_forward_from_oldest", fun test_get_elements_forward_from_oldest/0},
            {"test_get_structure_not_sorted_forward_from_oldest", fun test_get_structure_not_sorted_forward_from_oldest/0},
            {"test_get_highest", fun test_get_highest/0},
            {"test_get_highest_structure_not_sorted", fun test_get_highest_structure_not_sorted/0},
            {"test_get_max_key", fun test_get_max_key/0},
            {"test_get_max_key_structure_not_sorted", fun test_get_max_key_structure_not_sorted/0},
    
            {"test_nodes_created_after_insert_elements", fun test_nodes_created_after_insert_elements/0},
            {"test_min_in_newer_after_insert_elements", fun test_min_in_newer_after_insert_elements/0},
            {"test_min_in_newer_after_insert_elements_reversed", fun test_min_in_newer_after_insert_elements_reversed/0},
            {"test_max_in_older_after_insert_elements", fun test_max_in_older_after_insert_elements/0},
            {"test_max_in_older_after_insert_elements_reversed", fun test_max_in_older_after_insert_elements_reversed/0},
            {"test_node_num_after_insert_elements", fun test_node_num_after_insert_elements/0},
            {"test_nodes_elements_after_insert_elements", fun test_nodes_elements_after_insert_elements/0},
            {"test_nodes_elements_after_insert_elements_reversed", fun test_nodes_elements_after_insert_elements_reversed/0},
            {"test_nodes_removed_after_remove_elements", fun test_nodes_removed_after_remove_elements/0},
            {"test_nodes_after_remove_elements_from_last_node", fun test_nodes_after_remove_elements_from_last_node/0},
            {"test_min_in_newer_after_remove_elements", fun test_min_in_newer_after_remove_elements/0},
            {"test_max_in_older_after_remove_elements", fun test_max_in_older_after_remove_elements/0},
            {"test_min_in_node", fun test_min_in_node/0},
            {"test_min_in_node_after_remove_elements", fun test_min_in_node_after_remove_elements/0},
            {"test_max_in_node", fun test_max_in_node/0},
            {"test_max_in_node_after_remove_elements", fun test_max_in_node_after_remove_elements/0},
            {"test_min_max_in_node_after_remove_elements_merge_nodes", fun test_min_max_in_node_after_remove_elements_merge_nodes/0}
        ]
    }.

setup() ->
    sliding_proplist_persistence:init(),
    ok.

teardown(_) ->
    sliding_proplist_persistence:destroy_ets().

%%%===================================================================
%%% Tests
%%%===================================================================

test_create_and_destroy() ->
    ?assertEqual(ok, sliding_proplist:destroy(<<"dummy_id">>)),
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(10, 30)),
    ?assertEqual(ok, sliding_proplist:destroy(Id)),
    ?assertEqual([], ets:tab2list(sliding_proplist_persistence)).


test_insert_elements_one_node() ->
    ?assertEqual({error, not_found}, sliding_proplist:insert_unique_sorted_elements(<<"dummy_id">>, prepare_batch(1, 100))),
    ?assertMatch({error, not_found}, sliding_proplist:list(<<"dummy_id">>, 100)),
    ?assertEqual({error, not_found}, sliding_proplist:insert_unique_sorted_elements(<<"dummy_id">>, [])),
    {ok, Id} = sliding_proplist:create(10),
    ?assertEqual({ok, []}, sliding_proplist:insert_unique_sorted_elements(Id, [{1, <<"1">>}])),
    ?assertMatch({done, [{1, <<"1">>}]}, sliding_proplist:list(Id, 100)).


test_insert_elements_multi_nodes() ->
    {ok, Id} = sliding_proplist:create(10),
    Batch = prepare_batch(10, 30),
    sliding_proplist:insert_unique_sorted_elements(Id, Batch),
    ?assertMatch({done, Batch}, sliding_proplist:list(Id, 100)).


test_insert_elements_only_existing_elements() ->
    {ok, Id} = sliding_proplist:create(10),
    Batch = prepare_batch(10, 30),
    ?assertEqual({ok, []}, sliding_proplist:insert_unique_sorted_elements(Id, Batch)),
    ?assertEqual({ok, lists:reverse(lists:seq(10, 30))}, sliding_proplist:insert_unique_sorted_elements(Id, Batch)),
    ?assertMatch({done, Batch}, sliding_proplist:list(Id, 100)).


test_insert_elements_with_overwrite() ->
    {ok, Id} = sliding_proplist:create(10),
    Batch1 = prepare_batch(10, 30),
    ?assertEqual({ok, []}, sliding_proplist:insert_unique_sorted_elements(Id, Batch1)),
    Batch2 = prepare_batch(10, 32, fun(A) -> integer_to_binary(2*A) end),
    ?assertEqual({ok, lists:reverse(lists:seq(10, 30))}, sliding_proplist:insert_unique_sorted_elements(Id, Batch2)),
    ?assertMatch({done, Batch2}, sliding_proplist:list(Id, 100)),
    ?assertEqual({ok, [20]}, sliding_proplist:insert_unique_sorted_elements(Id, [{20, <<"40">>}])),
    ?assertMatch({done, Batch2}, sliding_proplist:list(Id, 100)).


test_list_with_listing_state() ->
    ?assertMatch({error, not_found}, sliding_proplist:list(<<"dummy_id">>, 1000)),
    {ok, Id} = sliding_proplist:create(10),
    ?assertMatch({done, []}, sliding_proplist:list(Id, 0)),
    ?assertMatch({done, []}, sliding_proplist:list(Id, 10)),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(10, 30)),
    ?assertMatch({more, [], _}, sliding_proplist:list(Id, 0)),
    FinalListingState = lists:foldl(fun(X, ListingState) ->
        {more, Res, NewListingState} = sliding_proplist:list(ListingState, 1),
        ?assertEqual([{X, integer_to_binary(X)}], Res),
        NewListingState
    end, Id, lists:seq(30, 11, -1)),
    ?assertMatch({done, [{10, <<"10">>}]}, sliding_proplist:list(FinalListingState, 1)).


test_list_with_listing_state_start_from_last() ->
    ?assertMatch({error, not_found}, sliding_proplist:list(<<"dummy_id">>, 1000)),
    {ok, Id} = sliding_proplist:create(10),
    ?assertMatch({done, []}, sliding_proplist:list(Id, 0, forward_from_oldest)),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(10, 30)),
    FinalListingState = lists:foldl(
        fun (X, Id) when is_binary(Id) ->
                {more, Res, NewListingState} = sliding_proplist:list(Id, 1, forward_from_oldest),
                ?assertEqual([{X, integer_to_binary(X)}], Res),
                NewListingState;
            (X, ListingState) ->
                {more, Res, NewListingState} = sliding_proplist:list(ListingState, 1),
                ?assertEqual([{X, integer_to_binary(X)}], Res),
                NewListingState
    end, Id, lists:seq(10, 29)),
    ?assertMatch({done, [{30, <<"30">>}]}, sliding_proplist:list(FinalListingState, 1)).


test_fold_elements() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(10, 30)),
    FinalListingState = lists:foldl(fun(X, ListingState) ->
        {more, Res, NewListingState} = sliding_proplist:fold_elements(
            ListingState, 1, fun({_Key, Value}, Acc) -> {ok, [Value | Acc]} end, []),
        ?assertEqual([integer_to_binary(X)], Res),
        NewListingState
    end, Id, lists:seq(30, 11, -1)),
    ?assertMatch({done, [<<"10">>]}, sliding_proplist:fold_elements(
        FinalListingState, 1, fun({_Key, Value}, Acc) -> {ok, [Value | Acc]} end, [])).


test_fold_elements_stop() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 30)),
    Expected1 = lists:seq(1, 7),
    ?assertMatch({done, Expected1},  sliding_proplist:fold_elements(Id, 100, forward_from_oldest,
        fun ({8, _Value}, _Acc) -> stop;
            ({Key, _Value}, Acc) -> {ok, Acc ++ [Key]} 
        end, 
        [])
    ),
    Expected2 = lists:seq(30, 9, -1),
    ?assertMatch({done, Expected2},  sliding_proplist:fold_elements(Id, 100, 
        fun ({8, _Value}, _Acc) -> stop;
            ({Key, _Value}, Acc) -> {ok, Acc ++ [Key]}
        end,
        [])
    ).


test_remove_consecutive_elements_between_nodes() ->
    ?assertEqual({error, not_found}, sliding_proplist:remove_elements(<<"dummy_id">>, [])),
    {ok, Id} = sliding_proplist:create(10),
    ?assertEqual(ok, sliding_proplist:remove_elements(Id, [])),
    ?assertEqual(ok, sliding_proplist:remove_elements(Id, [1,2,3,4,5])),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(10, 30)),
    sliding_proplist:remove_elements(Id, lists:seq(10, 20)),
    Expected = prepare_batch(21, 30),
    ?assertMatch({done, Expected}, sliding_proplist:list(Id, 100)),
    sliding_proplist:remove_elements(Id, lists:seq(25, 30)),
    Expected1 = prepare_batch(21, 24),
    ?assertMatch({done, Expected1}, sliding_proplist:list(Id, 100)).


test_remove_non_consecutive_elements_between_nodes() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 100)),
    sliding_proplist:remove_elements(Id, lists:seq(1,100, 2)),
    Expected = prepare_batch(2, 100 ,2),
    ?assertMatch({done, Expected}, sliding_proplist:list(Id, 100)).


test_remove_all_elements_in_first_node() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(10, 30)),
    sliding_proplist:remove_elements(Id, [30]),
    Expected = prepare_batch(10, 29),
    ?assertMatch({done, Expected}, sliding_proplist:list(Id, 100)),
    sliding_proplist:remove_elements(Id, lists:seq(20, 30)),
    Expected1 = prepare_batch(10, 19),
    ?assertMatch({done, Expected1}, sliding_proplist:list(Id, 100)).


test_remove_elements_all_but_first_node() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(10, 30)),
    sliding_proplist:remove_elements(Id, lists:seq(10, 29)),
    Expected = lists:reverse(prepare_batch(30, 30)),
    ?assertMatch({done, Expected}, sliding_proplist:list(Id, 100)).


test_first_node_merge_during_remove() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(10, 31)),
    sliding_proplist:remove_elements(Id, lists:seq(10, 30)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({done, Expected}, sliding_proplist:list(Id, 100)).


test_remove_elements_one_by_one_descending() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(10, 31)),
    lists:foreach(fun(Elem) ->
        sliding_proplist:remove_elements(Id, [Elem])
    end, lists:seq(30, 10, -1)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({done, Expected}, sliding_proplist:list(Id, 100)).


test_remove_elements_one_by_one_ascending() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(10, 31)),
    lists:foreach(fun(Elem) ->
        sliding_proplist:remove_elements(Id, [Elem])
    end, lists:seq(10, 30)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({done, Expected}, sliding_proplist:list(Id, 100)).


test_remove_elements_structure_not_sorted() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(20, 31)),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(10, 20)),
    lists:foreach(fun(Elem) ->
        sliding_proplist:remove_elements(Id, [Elem])
    end, lists:seq(30, 10, -1)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({done, Expected}, sliding_proplist:list(Id, 100)).


test_merge_nodes_during_remove_structure_not_sorted() ->
    {ok, Id} = sliding_proplist:create(1),
    lists:foreach(fun(Elem) ->
        sliding_proplist:insert_unique_sorted_elements(Id, Elem)
    end, prepare_batch(5, 1, -1)),
    sliding_proplist:remove_elements(Id, [5]),
    Expected = prepare_batch(4, 1, -1),
    ?assertMatch({done, Expected}, sliding_proplist:list(Id, 100)).


test_remove_elements_between_listings() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(10, 30)),
    {more, _, ListingState} = sliding_proplist:list(Id, 2),
    sliding_proplist:remove_elements(Id, [28, 27]),
    {more, Res, _} = sliding_proplist:list(ListingState, 1),
    ?assertMatch([{26, <<"26">>}], Res),
    sliding_proplist:remove_elements(Id, lists:seq(20, 29)),
    Expected = prepare_batch(10, 19),
    ?assertMatch({done, Expected}, sliding_proplist:list(ListingState, 100)).


test_get_highest() ->
    ?assertEqual({error, not_found}, sliding_proplist:get_highest(<<"dummy_id">>)),
    {ok, Id} = sliding_proplist:create(10),
    ?assertEqual({error, not_found}, sliding_proplist:get_highest(Id)),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 100)),
    ?assertEqual({ok, {100, <<"100">>}}, sliding_proplist:get_highest(Id)),
    sliding_proplist:remove_elements(Id, lists:seq(2, 99)),
    ?assertEqual({ok, {100, <<"100">>}}, sliding_proplist:get_highest(Id)),
    sliding_proplist:remove_elements(Id, [100]),
    ?assertEqual({ok, {1, <<"1">>}}, sliding_proplist:get_highest(Id)),
    sliding_proplist:remove_elements(Id, [1]),
    ?assertEqual({error, not_found}, sliding_proplist:get_highest(Id)).

    
test_get_highest_structure_not_sorted() ->
    {ok, Id} = sliding_proplist:create(10),
    lists:foreach(fun(Elem) ->
        sliding_proplist:insert_unique_sorted_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual({ok, {100, <<"100">>}}, sliding_proplist:get_highest(Id)),
    sliding_proplist:remove_elements(Id, lists:seq(2, 99)),
    ?assertEqual({ok, {100, <<"100">>}}, sliding_proplist:get_highest(Id)).


test_get_max_key() ->
    ?assertEqual({error, not_found}, sliding_proplist:get_max_key(<<"dummy_id">>)),
    {ok, Id} = sliding_proplist:create(10),
    ?assertEqual({error, not_found}, sliding_proplist:get_max_key(Id)),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 100)),
    ?assertEqual({ok, 100}, sliding_proplist:get_max_key(Id)),
    sliding_proplist:remove_elements(Id, lists:seq(2, 99)),
    ?assertEqual({ok, 100}, sliding_proplist:get_max_key(Id)),
    sliding_proplist:remove_elements(Id, [100]),
    ?assertEqual({ok, 1}, sliding_proplist:get_max_key(Id)),
    sliding_proplist:remove_elements(Id, [1]),
    ?assertEqual({error, not_found}, sliding_proplist:get_max_key(Id)).


test_get_max_key_structure_not_sorted() ->
    {ok, Id} = sliding_proplist:create(10),
    lists:foreach(fun(Elem) ->
        sliding_proplist:insert_unique_sorted_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual({ok, 100}, sliding_proplist:get_max_key(Id)),
    sliding_proplist:remove_elements(Id, lists:seq(2, 99)),
    ?assertEqual({ok, 100}, sliding_proplist:get_max_key(Id)).


test_get_elements() ->
    ?assertEqual({error, not_found}, sliding_proplist_persistence:get_node(<<"dummy_id">>), 8),
    {ok, Id} = sliding_proplist:create(10),
    ?assertEqual({ok, []}, sliding_proplist:get_elements(Id, 8)),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 100)),
    ?assertEqual({ok, [{8, <<"8">>}]}, sliding_proplist:get_elements(Id, 8)),
    {ok, Result} = sliding_proplist:get_elements(Id, lists:seq(8,50)),
    ?assertEqual(prepare_batch(8, 50), lists:sort(Result)),
    sliding_proplist:remove_elements(Id, lists:seq(2,99)),
    ?assertEqual({ok, []}, sliding_proplist:get_elements(Id, 8)),
    {ok, Result1} = sliding_proplist:get_elements(Id, lists:seq(1,100)),
    ?assertEqual([{1, <<"1">>}, {100, <<"100">>}], lists:sort(Result1)).


test_get_elements_structure_not_sorted() ->
    {ok, Id} = sliding_proplist:create(10),
    lists:foreach(fun(Elem) ->
        sliding_proplist:insert_unique_sorted_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual({ok, [{8, <<"8">>}]}, sliding_proplist:get_elements(Id, 8)),
    {ok, Result} = sliding_proplist:get_elements(Id, lists:seq(8,50)),
    ?assertEqual(prepare_batch(8, 50), lists:sort(Result)),
    sliding_proplist:remove_elements(Id, lists:seq(2, 99)),
    ?assertEqual({ok, []}, sliding_proplist:get_elements(Id, 8)),
    {ok, Result1} = sliding_proplist:get_elements(Id, lists:seq(1,100)),
    ?assertEqual([{1, <<"1">>}, {100, <<"100">>}], lists:sort(Result1)).


test_get_elements_forward_from_oldest() ->
    ?assertEqual({error, not_found}, sliding_proplist_persistence:get_node(<<"dummy_id">>), 8),
    {ok, Id} = sliding_proplist:create(10),
    ?assertEqual({ok, []}, sliding_proplist:get_elements(Id, 8, forward_from_oldest)),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 100)),
    ?assertEqual({ok, [{8, <<"8">>}]}, sliding_proplist:get_elements(Id, 8, forward_from_oldest)),
    {ok, Result} = sliding_proplist:get_elements(Id, lists:seq(8,50), forward_from_oldest),
    ?assertEqual(prepare_batch(8, 50), lists:sort(Result)),
    sliding_proplist:remove_elements(Id, lists:seq(2,99)),
    ?assertEqual({ok, []}, sliding_proplist:get_elements(Id, 8, forward_from_oldest)),
    {ok, Result1} = sliding_proplist:get_elements(Id, lists:seq(1,100), forward_from_oldest),
    ?assertEqual([{1, <<"1">>}, {100, <<"100">>}], lists:sort(Result1)).


test_get_structure_not_sorted_forward_from_oldest() ->
    {ok, Id} = sliding_proplist:create(10),
    lists:foreach(fun(Elem) ->
        sliding_proplist:insert_unique_sorted_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual({ok, [{8, <<"8">>}]}, sliding_proplist:get_elements(Id, 8, forward_from_oldest)),
    {ok, Result} = sliding_proplist:get_elements(Id, lists:seq(8,50), forward_from_oldest),
    ?assertEqual(prepare_batch(8, 50), lists:sort(Result)),
    sliding_proplist:remove_elements(Id, lists:seq(2, 99)),
    ?assertEqual({ok, []}, sliding_proplist:get_elements(Id, 8, forward_from_oldest)),
    {ok, Result1} = sliding_proplist:get_elements(Id, lists:seq(1,100), forward_from_oldest),
    ?assertEqual([{1, <<"1">>}, {100, <<"100">>}], lists:sort(Result1)).


test_nodes_created_after_insert_elements() ->
    {ok, Id} = sliding_proplist:create(1),
    ?assertMatch({ok, #sentinel{first = undefined, last = undefined}}, sliding_proplist_persistence:get_node(Id)),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 1)),
    ?assertNotMatch({ok, #sentinel{last = undefined}}, sliding_proplist_persistence:get_node(Id)),
    {ok, #sentinel{last = LastNodeId}} = sliding_proplist_persistence:get_node(Id),
    ?assertMatch({ok, #sentinel{first = LastNodeId}}, sliding_proplist_persistence:get_node(Id)),
    ?assertMatch({ok, #node{next = undefined, prev = undefined}}, sliding_proplist_persistence:get_node(LastNodeId)),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(2, 2)),
    ?assertNotMatch({ok, #sentinel{first = LastNodeId}}, sliding_proplist_persistence:get_node(Id)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    ?assertMatch({ok, #node{next = FirstNodeId, prev = undefined}}, sliding_proplist_persistence:get_node(LastNodeId)),
    ?assertMatch({ok, #node{next = undefined, prev = LastNodeId}}, sliding_proplist_persistence:get_node(FirstNodeId)).


test_min_in_newer_after_insert_elements() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 100)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMinsOnLeft = [undefined] ++ lists:seq(91, 11, -10),
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{min_in_newer = Expected}}, sliding_proplist_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsOnLeft)).


test_min_in_newer_after_insert_elements_reversed() ->
    {ok, Id} = sliding_proplist:create(10),
    lists:foreach(fun(Elem) ->
        sliding_proplist:insert_unique_sorted_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMinsOnLeft = [undefined] ++ lists:duplicate(9, 1),
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{min_in_newer = Expected}}, sliding_proplist_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsOnLeft)).


test_max_in_older_after_insert_elements() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 100)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMaxInOlder = lists:seq(90, 10, -10) ++ [undefined],
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{max_in_older = Expected}}, sliding_proplist_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMaxInOlder)).


test_max_in_older_after_insert_elements_reversed() ->
    {ok, Id} = sliding_proplist:create(10),
    lists:foreach(fun(Elem) ->
        sliding_proplist:insert_unique_sorted_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMinsOnLeft = lists:duplicate(9, 100) ++ [undefined],
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{max_in_older = Expected}}, sliding_proplist_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsOnLeft)).


test_node_num_after_insert_elements() ->
    {ok, Id} = sliding_proplist:create(1),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 10)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    {ok, #node{prev = PrevNodeId, node_number = FirstNodeNum}} = sliding_proplist_persistence:get_node(FirstNodeId),
    NodeIds = get_nodes_ids(PrevNodeId),
    lists:foldl(fun(NodeId, NextNodeNum) ->
        {ok, #node{node_number = Num}} = sliding_proplist_persistence:get_node(NodeId),
        ?assert(Num < NextNodeNum),
        Num
    end, FirstNodeNum, NodeIds).


test_nodes_elements_after_insert_elements() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 100)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedElementsPerNode = lists:map(fun(A) -> maps:from_list(prepare_batch(10*(A-1) + 1, 10*A)) end, lists:seq(10,1, -1)),
    lists:foreach(fun({NodeId, Expected}) ->
        {ok, #node{elements = ElementsInNode}} = sliding_proplist_persistence:get_node(NodeId),
        ?assertEqual(Expected, ElementsInNode)
    end, lists:zip(NodesIds, ExpectedElementsPerNode)).


test_nodes_elements_after_insert_elements_reversed() ->
    {ok, Id} = sliding_proplist:create(10),
    lists:foreach(fun(Elem) ->
        sliding_proplist:insert_unique_sorted_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedElementsPerNode = lists:map(fun(A) -> maps:from_list(prepare_batch(10*(A-1) + 1, 10*A)) end, lists:seq(1,10)),
    lists:foreach(fun({NodeId, Expected}) ->
        {ok, #node{elements = ElementsInNode}} = sliding_proplist_persistence:get_node(NodeId),
        ?assertEqual(Expected, ElementsInNode)
    end, lists:zip(NodesIds, ExpectedElementsPerNode)).


test_nodes_removed_after_remove_elements() ->
    {ok, Id} = sliding_proplist:create(1),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 3)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    [Node1, Node2, Node3] = get_nodes_ids(FirstNodeId),
    sliding_proplist:remove_elements(Id, [2]),
    ?assertEqual({error, not_found}, sliding_proplist_persistence:get_node(Node2)),
    ?assertMatch({ok, #node{next = Node1}}, sliding_proplist_persistence:get_node(Node3)),
    ?assertMatch({ok, #node{prev = Node3}}, sliding_proplist_persistence:get_node(Node1)),
    sliding_proplist:remove_elements(Id, [3]),
    ?assertEqual({error, not_found}, sliding_proplist_persistence:get_node(Node1)),
    ?assertMatch({ok, #node{next = undefined}}, sliding_proplist_persistence:get_node(Node3)),
    ?assertMatch({ok, #sentinel{first = Node3, last = Node3}}, sliding_proplist_persistence:get_node(Id)).

    
test_nodes_after_remove_elements_from_last_node() ->
    {ok, Id} = sliding_proplist:create(1),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 3)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    [Node1, Node2, Node3] = get_nodes_ids(FirstNodeId),
    sliding_proplist:remove_elements(Id, [1]),
    ?assertMatch({ok, #node{prev = undefined, next = Node1}}, sliding_proplist_persistence:get_node(Node2)),
    ?assertEqual({error, not_found}, sliding_proplist_persistence:get_node(Node3)),
    ?assertMatch({ok, #node{prev = Node2, next = undefined}}, sliding_proplist_persistence:get_node(Node1)),
    sliding_proplist:remove_elements(Id, [3]),
    ?assertEqual({error, not_found}, sliding_proplist_persistence:get_node(Node3)),
    ?assertEqual({error, not_found}, sliding_proplist_persistence:get_node(Node1)),
    ?assertMatch({ok, #node{next = undefined, prev = undefined}}, sliding_proplist_persistence:get_node(Node2)),
    ?assertMatch({ok, #sentinel{first = Node2, last = Node2}}, sliding_proplist_persistence:get_node(Id)).


test_min_in_newer_after_remove_elements() ->
    {ok, Id} = sliding_proplist:create(1),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(2, 10)),
    sliding_proplist:insert_unique_sorted_elements(Id, {1, <<"1">>}),
    sliding_proplist:remove_elements(Id, [1]),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMinsOnLeft = [undefined] ++ lists:seq(10, 3, -1),
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{min_in_newer = Expected}}, sliding_proplist_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsOnLeft)).


test_max_in_older_after_remove_elements() ->
    {ok, Id} = sliding_proplist:create(1),
    sliding_proplist:insert_unique_sorted_elements(Id, {10, <<"10">>}),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 9)),
    sliding_proplist:remove_elements(Id, [10]),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMaxInOlder = lists:seq(8, 1, -1) ++ [undefined],
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{max_in_older = Expected}}, sliding_proplist_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMaxInOlder)).


test_min_in_node() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 100)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMinsInNode = lists:seq(91, 1, -10),
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{min_in_node = Expected}}, sliding_proplist_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsInNode)).


test_min_in_node_after_remove_elements() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 100)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    sliding_proplist:remove_elements(Id, lists:seq(1, 91, 10)),
    ExpectedMinsInNode = lists:seq(92, 2, -10),
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{min_in_node = Expected}}, sliding_proplist_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsInNode)).


test_max_in_node() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 100)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMaxsInNode = lists:seq(100, 10, -10),
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{max_in_node = Expected}}, sliding_proplist_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMaxsInNode)).


test_max_in_node_after_remove_elements() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 100)),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    sliding_proplist:remove_elements(Id, lists:seq(10, 100, 10)),
    ExpectedMaxsInNode = lists:seq(99, 9, -10),
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{max_in_node = Expected}}, sliding_proplist_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMaxsInNode)).


test_min_max_in_node_after_remove_elements_merge_nodes() ->
    {ok, Id} = sliding_proplist:create(10),
    sliding_proplist:insert_unique_sorted_elements(Id, prepare_batch(1, 100)),
    sliding_proplist:remove_elements(Id, lists:seq(1, 100) -- [8]),
    {ok, #sentinel{first = FirstNodeId}} = sliding_proplist_persistence:get_node(Id),
    {ok, #node{min_in_node = MinInNode, max_in_node = MaxInNode}} = sliding_proplist_persistence:get_node(FirstNodeId),
    ?assertEqual(8, MinInNode),
    ?assertEqual(8, MaxInNode).

-endif.


%%=====================================================================
%% Internal functions
%%=====================================================================

prepare_batch(Start, End) ->
    prepare_batch(Start, End, 1).

prepare_batch(Start, End, Step) when is_integer(Step)->
    prepare_batch(Start, End, Step, fun(A) -> integer_to_binary(A) end);
prepare_batch(Start, End, ValueFun) when is_function(ValueFun, 1)->
    prepare_batch(Start, End, 1, ValueFun).

prepare_batch(Start, End, Step, ValueFun) ->
    lists:map(fun(A) -> {A, ValueFun(A)} end, lists:seq(Start, End, Step)).


get_nodes_ids(undefined) -> [];
get_nodes_ids(NodeId) ->
    {ok, #node{prev = PrevNodeId}} = sliding_proplist_persistence:get_node(NodeId),
    [NodeId] ++ get_nodes_ids(PrevNodeId).
    