%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Eunit tests for the append list module.
%%% @end
%%%-------------------------------------------------------------------
-module(append_list_tests).
-author("Michal Stanisz").

-ifdef(TEST).

-include("modules/datastore/append_list.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/errors.hrl").


%%%===================================================================
%%% Setup and teardown
%%%===================================================================

append_list_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            {"test_create_and_destroy", fun test_create_and_destroy/0},
            {"test_add_elements_one_node", fun test_add_elements_one_node/0},
            {"test_add_elements_multi_nodes", fun test_add_elements_multi_nodes/0},
            {"test_add_only_existing", fun test_add_only_existing_elements/0},
            {"test_add_with_overwrite", fun test_add_with_overwrite/0},
            {"test_list_with_listing_state", fun test_list_with_listing_state/0},
            {"test_list_with_listing_state_start_from_last", fun test_list_with_listing_state_start_from_last/0},
            {"test_list_with_fold_fun", fun test_list_with_fold_fun/0},
            {"test_list_with_fold_fun_stop", fun test_list_with_fold_fun_stop/0},
            {"test_delete_consecutive_elems_between_nodes", fun test_delete_consecutive_elems_between_nodes/0},
            {"test_delete_non_consecutive_elems_between_nodes", fun test_delete_non_consecutive_elems_between_nodes/0},
            {"test_delete_all_elems_in_first_node", fun test_delete_all_elems_in_first_node/0},
            {"test_delete_elems_all_but_first_node", fun test_delete_elems_all_but_first_node/0},
            {"test_first_node_merge_during_delete", fun test_first_node_merge_during_delete/0},
            {"test_delete_elems_one_by_one_descending", fun test_delete_elems_one_by_one_descending/0},
            {"test_delete_elems_one_by_one_ascending", fun test_delete_elems_one_by_one_ascending/0},
            {"test_delete_elems_structure_not_sorted", fun test_delete_elems_structure_not_sorted/0},
            {"test_merge_nodes_during_delete_structure_not_sorted", fun test_merge_nodes_during_delete_structure_not_sorted/0},
            {"test_delete_between_listings", fun test_delete_between_listings/0},
            {"test_get_elements", fun test_get_elements/0},
            {"test_get_elements_structure_not_sorted", fun test_get_elements_structure_not_sorted/0},
            {"test_get_elements_forward_from_oldest", fun test_get_elements_forward_from_oldest/0},
            {"test_get_structure_not_sorted_forward_from_oldest", fun test_get_structure_not_sorted_forward_from_oldest/0},
            {"test_get_highest", fun test_get_highest/0},
            {"test_get_highest_structure_not_sorted", fun test_get_highest_structure_not_sorted/0},
            {"test_get_max_key", fun test_get_max_key/0},
            {"test_get_max_key_structure_not_sorted", fun test_get_max_key_structure_not_sorted/0},
    
            {"test_nodes_created_after_add", fun test_nodes_created_after_add/0},
            {"test_min_on_left_after_add", fun test_min_on_left_after_add/0},
            {"test_min_on_left_after_add_reversed", fun test_min_on_left_after_add_reversed/0},
            {"test_max_on_right_after_add", fun test_max_on_right_after_add/0},
            {"test_max_on_right_after_add_reversed", fun test_max_on_right_after_add_reversed/0},
            {"test_node_num_after_add", fun test_node_num_after_add/0},
            {"test_nodes_elements_after_add", fun test_nodes_elements_after_add/0},
            {"test_nodes_elements_after_add_reversed", fun test_nodes_elements_after_add_reversed/0},
            {"test_nodes_deleted_after_delete_elems", fun test_nodes_deleted_after_delete_elems/0},
            {"test_nodes_after_delete_elems_from_last_node", fun test_nodes_after_delete_elems_from_last_node/0},
            {"test_min_on_left_after_delete_elems", fun test_min_on_left_after_delete_elems/0},
            {"test_max_on_right_after_delete_elems", fun test_max_on_right_after_delete_elems/0}
        ]
    }.

setup() ->
    append_list_persistence:init(),
    ok.

teardown(_) ->
    append_list_persistence:destroy_ets().

%%%===================================================================
%%% Tests
%%%===================================================================

test_create_and_destroy() ->
    ?assertEqual(ok, append_list:destroy(<<"dummy_id">>)),
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(10, 30)),
    ?assertEqual(ok, append_list:destroy(Id)),
    ?assertEqual([], ets:tab2list(append_list_persistence)).


test_add_elements_one_node() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:insert_elements(<<"dummy_id">>, prepare_batch(1, 100))),
    ?assertMatch(?ERROR_NOT_FOUND, append_list:fold_elements(<<"dummy_id">>, 100)),
    ?assertEqual(ok, append_list:insert_elements(<<"dummy_id">>, [])),
    {ok, Id} = append_list:create(10),
    ?assertEqual({ok, []}, append_list:insert_elements(Id, [{1, <<"1">>}])),
    ?assertMatch({done, [{1, <<"1">>}]}, append_list:fold_elements(Id, 100)).


test_add_elements_multi_nodes() ->
    {ok, Id} = append_list:create(10),
    Batch = prepare_batch(10, 30),
    append_list:insert_elements(Id, Batch),
    Expected = lists:reverse(Batch),
    ?assertMatch({done, Expected}, append_list:fold_elements(Id, 100)).


test_add_only_existing_elements() ->
    {ok, Id} = append_list:create(10),
    Batch = prepare_batch(10, 30),
    ?assertEqual({ok, []}, append_list:insert_elements(Id, Batch)),
    ?assertEqual({ok, lists:reverse(lists:seq(10, 30))}, append_list:insert_elements(Id, Batch)),
    Expected = lists:reverse(Batch),
    ?assertMatch({done, Expected}, append_list:fold_elements(Id, 100)).


test_add_with_overwrite() ->
    {ok, Id} = append_list:create(10),
    Batch1 = prepare_batch(10, 30),
    ?assertEqual({ok, []}, append_list:insert_elements(Id, Batch1)),
    Batch2 = prepare_batch(10, 32, fun(A) -> integer_to_binary(2*A) end),
    ?assertEqual({ok, lists:reverse(lists:seq(10, 30))}, append_list:insert_elements(Id, Batch2)),
    Expected = lists:reverse(Batch2),
    ?assertMatch({done, Expected}, append_list:fold_elements(Id, 100)),
    ?assertEqual({ok, [20]}, append_list:insert_elements(Id, [{20, <<"40">>}])),
    ?assertMatch({done, Expected}, append_list:fold_elements(Id, 100)).


test_list_with_listing_state() ->
    ?assertMatch(?ERROR_NOT_FOUND, append_list:fold_elements(<<"dummy_id">>, 1000)),
    {ok, Id} = append_list:create(10),
    ?assertMatch({done, []}, append_list:fold_elements(Id, 0)),
    ?assertMatch({done, []}, append_list:fold_elements(Id, 10)),
    append_list:insert_elements(Id, prepare_batch(10, 30)),
    FinalListingState = lists:foldl(fun(X, ListingState) ->
        {more, Res, NewListingState} = append_list:fold_elements(ListingState, 1),
        ?assertEqual([{X, integer_to_binary(X)}], Res),
        NewListingState
    end, Id, lists:seq(30, 11, -1)),
    ?assertMatch({done, [{10, <<"10">>}]}, append_list:fold_elements(FinalListingState, 1)).


test_list_with_listing_state_start_from_last() ->
    ?assertMatch(?ERROR_NOT_FOUND, append_list:fold_elements(<<"dummy_id">>, 1000)),
    {ok, Id} = append_list:create(10),
    ?assertMatch({done, []}, append_list:fold_elements(Id, 0, forward_from_oldest)),
    append_list:insert_elements(Id, prepare_batch(10, 30)),
    FinalListingState = lists:foldl(
        fun (X, Id) when is_binary(Id) ->
                {more, Res, NewListingState} = append_list:fold_elements(Id, 1, forward_from_oldest),
                ?assertEqual([{X, integer_to_binary(X)}], Res),
                NewListingState;
            (X, ListingState) ->
                {more, Res, NewListingState} = append_list:fold_elements(ListingState, 1),
                ?assertEqual([{X, integer_to_binary(X)}], Res),
                NewListingState
    end, Id, lists:seq(10, 29)),
    ?assertMatch({done, [{30, <<"30">>}]}, append_list:fold_elements(FinalListingState, 1)).


test_list_with_fold_fun() ->
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(10, 30)),
    FinalListingState = lists:foldl(fun(X, ListingState) ->
        {more, Res, NewListingState} = append_list:fold_elements(ListingState, 1, fun({_Key, Value}) -> {ok, Value} end),
        ?assertEqual([integer_to_binary(X)], Res),
        NewListingState
    end, Id, lists:seq(30, 11, -1)),
    ?assertMatch({done, [<<"10">>]}, append_list:fold_elements(FinalListingState, 1, fun({_Key, Value}) -> {ok, Value} end)).


test_list_with_fold_fun_stop() ->
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(1, 30)),
    Expected1 = lists:seq(1, 7),
    ?assertMatch({done, Expected1},  append_list:fold_elements(Id, 100, forward_from_oldest,
        fun ({8, _Value}) -> stop;
            ({Key, _Value}) -> {ok, Key} 
        end)
    ),
    Expected2 = lists:seq(30, 9, -1),
    ?assertMatch({done, Expected2},  append_list:fold_elements(Id, 100, 
        fun ({8, _Value}) -> stop;
            ({Key, _Value}) -> {ok, Key}
        end)
    ).


test_delete_consecutive_elems_between_nodes() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:remove_elements(<<"dummy_id">>, [])),
    {ok, Id} = append_list:create(10),
    ?assertEqual(ok, append_list:remove_elements(Id, [])),
    ?assertEqual(ok, append_list:remove_elements(Id, [1,2,3,4,5])),
    append_list:insert_elements(Id, prepare_batch(10, 30)),
    append_list:remove_elements(Id, lists:seq(10, 20)),
    Expected = lists:reverse(prepare_batch(21, 30)),
    ?assertMatch({done, Expected}, append_list:fold_elements(Id, 100)),
    append_list:remove_elements(Id, lists:seq(25, 30)),
    Expected1 = lists:reverse(prepare_batch(21, 24)),
    ?assertMatch({done, Expected1}, append_list:fold_elements(Id, 100)).


test_delete_non_consecutive_elems_between_nodes() ->
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(1, 100)),
    append_list:remove_elements(Id, lists:seq(1,100, 2)),
    Expected = lists:reverse(prepare_batch(2, 100 ,2)),
    ?assertMatch({done, Expected}, append_list:fold_elements(Id, 100)).


test_delete_all_elems_in_first_node() ->
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(10, 30)),
    append_list:remove_elements(Id, [30]),
    Expected = lists:reverse(prepare_batch(10, 29)),
    ?assertMatch({done, Expected}, append_list:fold_elements(Id, 100)),
    append_list:remove_elements(Id, lists:seq(20, 30)),
    Expected1 = lists:reverse(prepare_batch(10, 19)),
    ?assertMatch({done, Expected1}, append_list:fold_elements(Id, 100)).


test_delete_elems_all_but_first_node() ->
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(10, 30)),
    append_list:remove_elements(Id, lists:seq(10, 29)),
    Expected = lists:reverse(prepare_batch(30, 30)),
    ?assertMatch({done, Expected}, append_list:fold_elements(Id, 100)).


test_first_node_merge_during_delete() ->
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(10, 31)),
    append_list:remove_elements(Id, lists:seq(10, 30)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({done, Expected}, append_list:fold_elements(Id, 100)).


test_delete_elems_one_by_one_descending() ->
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(10, 31)),
    lists:foreach(fun(Elem) ->
        append_list:remove_elements(Id, [Elem])
    end, lists:seq(30, 10, -1)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({done, Expected}, append_list:fold_elements(Id, 100)).


test_delete_elems_one_by_one_ascending() ->
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(10, 31)),
    lists:foreach(fun(Elem) ->
        append_list:remove_elements(Id, [Elem])
    end, lists:seq(10, 30)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({done, Expected}, append_list:fold_elements(Id, 100)).


test_delete_elems_structure_not_sorted() ->
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(20, 31)),
    append_list:insert_elements(Id, prepare_batch(10, 20)),
    lists:foreach(fun(Elem) ->
        append_list:remove_elements(Id, [Elem])
    end, lists:seq(30, 10, -1)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({done, Expected}, append_list:fold_elements(Id, 100)).


test_merge_nodes_during_delete_structure_not_sorted() ->
    {ok, Id} = append_list:create(1),
    lists:foreach(fun(Elem) ->
        append_list:insert_elements(Id, Elem)
    end, prepare_batch(5, 1, -1)),
    append_list:remove_elements(Id, [5]),
    Expected = lists:reverse(prepare_batch(4, 1, -1)),
    ?assertMatch({done, Expected}, append_list:fold_elements(Id, 100)).


test_delete_between_listings() ->
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(10, 30)),
    {more, _, ListingState} = append_list:fold_elements(Id, 2),
    append_list:remove_elements(Id, [28, 27]),
    {more, Res, _} = append_list:fold_elements(ListingState, 1),
    ?assertMatch([{26, <<"26">>}], Res),
    append_list:remove_elements(Id, lists:seq(20, 29)),
    Expected = lists:reverse(prepare_batch(10, 19)),
    ?assertMatch({done, Expected}, append_list:fold_elements(ListingState, 100)).


test_get_highest() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_highest(<<"dummy_id">>)),
    {ok, Id} = append_list:create(10),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_highest(Id)),
    append_list:insert_elements(Id, prepare_batch(1, 100)),
    ?assertEqual({ok, {100, <<"100">>}}, append_list:get_highest(Id)),
    append_list:remove_elements(Id, lists:seq(2, 99)),
    ?assertEqual({ok, {100, <<"100">>}}, append_list:get_highest(Id)),
    append_list:remove_elements(Id, [100]),
    ?assertEqual({ok, {1, <<"1">>}}, append_list:get_highest(Id)),
    append_list:remove_elements(Id, [1]),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_highest(Id)).

    
test_get_highest_structure_not_sorted() ->
    {ok, Id} = append_list:create(10),
    lists:foreach(fun(Elem) ->
        append_list:insert_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual({ok, {100, <<"100">>}}, append_list:get_highest(Id)),
    append_list:remove_elements(Id, lists:seq(2, 99)),
    ?assertEqual({ok, {100, <<"100">>}}, append_list:get_highest(Id)).


test_get_max_key() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_max_key(<<"dummy_id">>)),
    {ok, Id} = append_list:create(10),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_max_key(Id)),
    append_list:insert_elements(Id, prepare_batch(1, 100)),
    ?assertEqual({ok, 100}, append_list:get_max_key(Id)),
    append_list:remove_elements(Id, lists:seq(2, 99)),
    ?assertEqual({ok, 100}, append_list:get_max_key(Id)),
    append_list:remove_elements(Id, [100]),
    ?assertEqual({ok, 1}, append_list:get_max_key(Id)),
    append_list:remove_elements(Id, [1]),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_max_key(Id)).


test_get_max_key_structure_not_sorted() ->
    {ok, Id} = append_list:create(10),
    lists:foreach(fun(Elem) ->
        append_list:insert_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual({ok, 100}, append_list:get_max_key(Id)),
    append_list:remove_elements(Id, lists:seq(2, 99)),
    ?assertEqual({ok, 100}, append_list:get_max_key(Id)).


test_get_elements() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(<<"dummy_id">>), 8),
    {ok, Id} = append_list:create(10),
    ?assertEqual({ok, []}, append_list:get_elements(Id, 8)),
    append_list:insert_elements(Id, prepare_batch(1, 100)),
    ?assertEqual({ok, [{8, <<"8">>}]}, append_list:get_elements(Id, 8)),
    {ok, Result} = append_list:get_elements(Id, lists:seq(8,50)),
    ?assertEqual(prepare_batch(8, 50), lists:sort(Result)),
    append_list:remove_elements(Id, lists:seq(2,99)),
    ?assertEqual({ok, []}, append_list:get_elements(Id, 8)),
    {ok, Result1} = append_list:get_elements(Id, lists:seq(1,100)),
    ?assertEqual([{1, <<"1">>}, {100, <<"100">>}], lists:sort(Result1)).


test_get_elements_structure_not_sorted() ->
    {ok, Id} = append_list:create(10),
    lists:foreach(fun(Elem) ->
        append_list:insert_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual({ok, [{8, <<"8">>}]}, append_list:get_elements(Id, 8)),
    {ok, Result} = append_list:get_elements(Id, lists:seq(8,50)),
    ?assertEqual(prepare_batch(8, 50), lists:sort(Result)),
    append_list:remove_elements(Id, lists:seq(2, 99)),
    ?assertEqual({ok, []}, append_list:get_elements(Id, 8)),
    {ok, Result1} = append_list:get_elements(Id, lists:seq(1,100)),
    ?assertEqual([{1, <<"1">>}, {100, <<"100">>}], lists:sort(Result1)).


test_get_elements_forward_from_oldest() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(<<"dummy_id">>), 8),
    {ok, Id} = append_list:create(10),
    ?assertEqual({ok, []}, append_list:get_elements(Id, 8, forward_from_oldest)),
    append_list:insert_elements(Id, prepare_batch(1, 100)),
    ?assertEqual({ok, [{8, <<"8">>}]}, append_list:get_elements(Id, 8, forward_from_oldest)),
    {ok, Result} = append_list:get_elements(Id, lists:seq(8,50), forward_from_oldest),
    ?assertEqual(prepare_batch(8, 50), lists:sort(Result)),
    append_list:remove_elements(Id, lists:seq(2,99)),
    ?assertEqual({ok, []}, append_list:get_elements(Id, 8, forward_from_oldest)),
    {ok, Result1} = append_list:get_elements(Id, lists:seq(1,100), forward_from_oldest),
    ?assertEqual([{1, <<"1">>}, {100, <<"100">>}], lists:sort(Result1)).


test_get_structure_not_sorted_forward_from_oldest() ->
    {ok, Id} = append_list:create(10),
    lists:foreach(fun(Elem) ->
        append_list:insert_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual({ok, [{8, <<"8">>}]}, append_list:get_elements(Id, 8, forward_from_oldest)),
    {ok, Result} = append_list:get_elements(Id, lists:seq(8,50), forward_from_oldest),
    ?assertEqual(prepare_batch(8, 50), lists:sort(Result)),
    append_list:remove_elements(Id, lists:seq(2, 99)),
    ?assertEqual({ok, []}, append_list:get_elements(Id, 8, forward_from_oldest)),
    {ok, Result1} = append_list:get_elements(Id, lists:seq(1,100), forward_from_oldest),
    ?assertEqual([{1, <<"1">>}, {100, <<"100">>}], lists:sort(Result1)).


test_nodes_created_after_add() ->
    {ok, Id} = append_list:create(1),
    ?assertMatch({ok, #sentinel{first = undefined, last = undefined}}, append_list_persistence:get_node(Id)),
    append_list:insert_elements(Id, prepare_batch(1, 1)),
    ?assertNotMatch({ok, #sentinel{last = undefined}}, append_list_persistence:get_node(Id)),
    {ok, #sentinel{last = LastNodeId}} = append_list_persistence:get_node(Id),
    ?assertMatch({ok, #sentinel{first = LastNodeId}}, append_list_persistence:get_node(Id)),
    ?assertMatch({ok, #node{next = undefined, prev = undefined}}, append_list_persistence:get_node(LastNodeId)),
    append_list:insert_elements(Id, prepare_batch(2, 2)),
    ?assertNotMatch({ok, #sentinel{first = LastNodeId}}, append_list_persistence:get_node(Id)),
    {ok, #sentinel{first = FirstNodeId}} = append_list_persistence:get_node(Id),
    ?assertMatch({ok, #node{next = FirstNodeId, prev = undefined}}, append_list_persistence:get_node(LastNodeId)),
    ?assertMatch({ok, #node{next = undefined, prev = LastNodeId}}, append_list_persistence:get_node(FirstNodeId)).


test_min_on_left_after_add() ->
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(1, 100)),
    {ok, #sentinel{first = FirstNodeId}} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMinsOnLeft = [undefined] ++ lists:seq(91, 11, -10),
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{min_on_left = Expected}}, append_list_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsOnLeft)).


test_min_on_left_after_add_reversed() ->
    {ok, Id} = append_list:create(10),
    lists:foreach(fun(Elem) ->
        append_list:insert_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    {ok, #sentinel{first = FirstNodeId}} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMinsOnLeft = [undefined] ++ lists:duplicate(9, 1),
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{min_on_left = Expected}}, append_list_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsOnLeft)).


test_max_on_right_after_add() ->
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(1, 100)),
    {ok, #sentinel{first = FirstNodeId}} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMaxOnRight = lists:seq(90, 10, -10) ++ [undefined],
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{max_on_right = Expected}}, append_list_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMaxOnRight)).


test_max_on_right_after_add_reversed() ->
    {ok, Id} = append_list:create(10),
    lists:foreach(fun(Elem) ->
        append_list:insert_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    {ok, #sentinel{first = FirstNodeId}} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMinsOnLeft = lists:duplicate(9, 100) ++ [undefined],
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{max_on_right = Expected}}, append_list_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsOnLeft)).


test_node_num_after_add() ->
    {ok, Id} = append_list:create(1),
    append_list:insert_elements(Id, prepare_batch(1, 10)),
    {ok, #sentinel{first = FirstNodeId}} = append_list_persistence:get_node(Id),
    {ok, #node{prev = PrevNodeId, node_number = FirstNodeNum}} = append_list_persistence:get_node(FirstNodeId),
    NodeIds = get_nodes_ids(PrevNodeId),
    lists:foldl(fun(NodeId, NextNodeNum) ->
        {ok, #node{node_number = Num}} = append_list_persistence:get_node(NodeId),
        ?assert(Num < NextNodeNum),
        Num
    end, FirstNodeNum, NodeIds).


test_nodes_elements_after_add() ->
    {ok, Id} = append_list:create(10),
    append_list:insert_elements(Id, prepare_batch(1, 100)),
    {ok, #sentinel{first = FirstNodeId}} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedElementsPerNode = lists:map(fun(A) -> maps:from_list(prepare_batch(10*(A-1) + 1, 10*A)) end, lists:seq(10,1, -1)),
    lists:foreach(fun({NodeId, Expected}) ->
        {ok, #node{elements = ElementsInNode}} = append_list_persistence:get_node(NodeId),
        ?assertEqual(Expected, ElementsInNode)
    end, lists:zip(NodesIds, ExpectedElementsPerNode)).


test_nodes_elements_after_add_reversed() ->
    {ok, Id} = append_list:create(10),
    lists:foreach(fun(Elem) ->
        append_list:insert_elements(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    {ok, #sentinel{first = FirstNodeId}} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedElementsPerNode = lists:map(fun(A) -> maps:from_list(prepare_batch(10*(A-1) + 1, 10*A)) end, lists:seq(1,10)),
    lists:foreach(fun({NodeId, Expected}) ->
        {ok, #node{elements = ElementsInNode}} = append_list_persistence:get_node(NodeId),
        ?assertEqual(Expected, ElementsInNode)
    end, lists:zip(NodesIds, ExpectedElementsPerNode)).


test_nodes_deleted_after_delete_elems() ->
    {ok, Id} = append_list:create(1),
    append_list:insert_elements(Id, prepare_batch(1, 3)),
    {ok, #sentinel{first = FirstNodeId}} = append_list_persistence:get_node(Id),
    [Node1, Node2, Node3] = get_nodes_ids(FirstNodeId),
    append_list:remove_elements(Id, [2]),
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(Node2)),
    ?assertMatch({ok, #node{next = Node1}}, append_list_persistence:get_node(Node3)),
    ?assertMatch({ok, #node{prev = Node3}}, append_list_persistence:get_node(Node1)),
    append_list:remove_elements(Id, [3]),
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(Node1)),
    ?assertMatch({ok, #node{next = undefined}}, append_list_persistence:get_node(Node3)),
    ?assertMatch({ok, #sentinel{first = Node3, last = Node3}}, append_list_persistence:get_node(Id)).

    
test_nodes_after_delete_elems_from_last_node() ->
    {ok, Id} = append_list:create(1),
    append_list:insert_elements(Id, prepare_batch(1, 3)),
    {ok, #sentinel{first = FirstNodeId}} = append_list_persistence:get_node(Id),
    [Node1, Node2, Node3] = get_nodes_ids(FirstNodeId),
    append_list:remove_elements(Id, [1]),
    ?assertMatch({ok, #node{prev = undefined, next = Node1}}, append_list_persistence:get_node(Node2)),
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(Node3)),
    ?assertMatch({ok, #node{prev = Node2, next = undefined}}, append_list_persistence:get_node(Node1)),
    append_list:remove_elements(Id, [3]),
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(Node3)),
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(Node1)),
    ?assertMatch({ok, #node{next = undefined, prev = undefined}}, append_list_persistence:get_node(Node2)),
    ?assertMatch({ok, #sentinel{first = Node2, last = Node2}}, append_list_persistence:get_node(Id)).


test_min_on_left_after_delete_elems() ->
    {ok, Id} = append_list:create(1),
    append_list:insert_elements(Id, prepare_batch(2, 10)),
    append_list:insert_elements(Id, {1, <<"1">>}),
    append_list:remove_elements(Id, [1]),
    {ok, #sentinel{first = FirstNodeId}} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMinsOnLeft = [undefined] ++ lists:seq(10, 3, -1),
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{min_on_left = Expected}}, append_list_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsOnLeft)).


test_max_on_right_after_delete_elems() ->
    {ok, Id} = append_list:create(1),
    append_list:insert_elements(Id, {10, <<"10">>}),
    append_list:insert_elements(Id, prepare_batch(1, 9)),
    append_list:remove_elements(Id, [10]),
    {ok, #sentinel{first = FirstNodeId}} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMaxOnRight = lists:seq(8, 1, -1) ++ [undefined],
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch({ok, #node{max_on_right = Expected}}, append_list_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMaxOnRight)).
    
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
    {ok, #node{prev = PrevNodeId}} = append_list_persistence:get_node(NodeId),
    [NodeId] ++ get_nodes_ids(PrevNodeId).
    