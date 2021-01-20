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

-include("modules/datastore/datastore_append_list.hrl").
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
            {"test_delete_struct", fun test_delete_struct/0},
            {"test_add_elements_multi_nodes", fun test_add_elements_multi_nodes/0},
            {"test_add_elements_one_node", fun test_add_elements_one_node/0},
            {"test_add_only_existing", fun test_add_only_existing_elements/0},
            {"test_add_with_overwrite", fun test_add_with_overwrite/0},
            {"test_list_with_listing_info", fun test_list_with_listing_info/0},
            {"test_list_start_from_last", fun test_list_start_from_last/0},
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
            {"test_get", fun test_get/0},
            {"test_get_structure_not_sorted", fun test_get_structure_not_sorted/0},
            {"test_get_start_from_last", fun test_get_start_from_last/0},
            {"test_get_structure_not_sorted_start_from_last", fun test_get_structure_not_sorted_start_from_last/0},
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

test_delete_struct() ->
    ?assertEqual(ok, append_list:delete_structure(<<"dummy_id">>)),
    {ok, Id} = append_list:create_structure(10),
    append_list:add(Id, prepare_batch(10, 30)),
    ?assertEqual(ok, append_list:delete_structure(Id)),
    ?assertEqual([], ets:tab2list(append_list_persistence)).


test_add_elements_one_node() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:add(<<"dummy_id">>, prepare_batch(1, 100))),
    ?assertMatch({[], _}, append_list:list(<<"dummy_id">>, 100)),
    ?assertEqual(ok, append_list:add(<<"dummy_id">>, [])),
    {ok, Id} = append_list:create_structure(10),
    ?assertEqual({ok, []}, append_list:add(Id, [{1, <<"1">>}])),
    ?assertMatch({[{1, <<"1">>}], _}, append_list:list(Id, 100)).


test_add_elements_multi_nodes() ->
    {ok, Id} = append_list:create_structure(10),
    Batch = prepare_batch(10, 30),
    append_list:add(Id, Batch),
    Expected = lists:reverse(Batch),
    ?assertMatch({Expected, _}, append_list:list(Id, 100)).


test_add_only_existing_elements() ->
    {ok, Id} = append_list:create_structure(10),
    Batch = prepare_batch(10, 30),
    ?assertEqual({ok, []}, append_list:add(Id, Batch)),
    ?assertEqual({ok, lists:reverse(lists:seq(10, 30))}, append_list:add(Id, Batch)),
    Expected = lists:reverse(Batch),
    ?assertMatch({Expected, _}, append_list:list(Id, 100)).


test_add_with_overwrite() ->
    {ok, Id} = append_list:create_structure(10),
    Batch1 = prepare_batch(10, 30),
    ?assertEqual({ok, []}, append_list:add(Id, Batch1)),
    Batch2 = prepare_batch(10, 32, fun(A) -> integer_to_binary(2*A) end),
    ?assertEqual({ok, lists:reverse(lists:seq(10, 30))}, append_list:add(Id, Batch2)),
    Expected = lists:reverse(Batch2),
    ?assertMatch({Expected, _}, append_list:list(Id, 100)),
    ?assertEqual({ok, [20]}, append_list:add(Id, [{20, <<"40">>}])),
    ?assertMatch({Expected, _}, append_list:list(Id, 100)).


test_list_with_listing_info() ->
    ?assertMatch({[], _}, append_list:list(<<"dummy_id">>, 1000)),
    {ok, Id} = append_list:create_structure(10),
    ?assertMatch({[], _}, append_list:list(Id, 0)),
    append_list:add(Id, prepare_batch(10, 30)),
    lists:foldl(fun(X, ListingInfo) ->
        {Res, NewListingInfo} = append_list:list(ListingInfo, 1),
        ?assertEqual([{X, integer_to_binary(X)}], Res),
        NewListingInfo
    end, Id, lists:seq(30, 10, -1)),
    {_, ListingInfo} = append_list:list(Id, 1000),
    ?assertMatch({[], _}, append_list:list(ListingInfo, 100)).


test_list_start_from_last() ->
    ?assertMatch({[], _}, append_list:list(<<"dummy_id">>, 1000)),
    {ok, Id} = append_list:create_structure(10),
    ?assertMatch({[], _}, append_list:list(Id, 0, last)),
    append_list:add(Id, prepare_batch(10, 30)),
    lists:foldl(fun(X, ListingInfo) ->
        {Res, NewListingInfo} = append_list:list(ListingInfo, 1, last),
        ?assertEqual([{X, integer_to_binary(X)}], Res),
        NewListingInfo
    end, Id, lists:seq(10, 30)),
    {Res, ListingInfo} = append_list:list(Id, 1000, last),
    ?assertEqual(Res, prepare_batch(10, 30)),
    ?assertMatch({[], _}, append_list:list(ListingInfo, 100)).


test_delete_consecutive_elems_between_nodes() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:delete(<<"dummy_id">>, [])),
    {ok, Id} = append_list:create_structure(10),
    ?assertEqual(ok, append_list:delete(Id, [])),
    ?assertEqual(ok, append_list:delete(Id, [1,2,3,4,5])),
    append_list:add(Id, prepare_batch(10, 30)),
    append_list:delete(Id, lists:seq(10, 20)),
    Expected = lists:reverse(prepare_batch(21, 30)),
    ?assertMatch({Expected, _}, append_list:list(Id, 100)),
    append_list:delete(Id, lists:seq(25, 30)),
    Expected1 = lists:reverse(prepare_batch(21, 24)),
    ?assertMatch({Expected1, _}, append_list:list(Id, 100)).


test_delete_non_consecutive_elems_between_nodes() ->
    {ok, Id} = append_list:create_structure(10),
    append_list:add(Id, prepare_batch(1, 100)),
    append_list:delete(Id, lists:seq(1,100, 2)),
    Expected = lists:reverse(prepare_batch(2, 100 ,2)),
    ?assertMatch({Expected, _}, append_list:list(Id, 100)).


test_delete_all_elems_in_first_node() ->
    {ok, Id} = append_list:create_structure(10),
    append_list:add(Id, prepare_batch(10, 30)),
    append_list:delete(Id, [30]),
    Expected = lists:reverse(prepare_batch(10, 29)),
    ?assertMatch({Expected, _}, append_list:list(Id, 100)),
    append_list:delete(Id, lists:seq(20, 30)),
    Expected1 = lists:reverse(prepare_batch(10, 19)),
    ?assertMatch({Expected1, _}, append_list:list(Id, 100)).


test_delete_elems_all_but_first_node() ->
    {ok, Id} = append_list:create_structure(10),
    append_list:add(Id, prepare_batch(10, 30)),
    append_list:delete(Id, lists:seq(10, 29)),
    Expected = lists:reverse(prepare_batch(30, 30)),
    ?assertMatch({Expected, _}, append_list:list(Id, 100)).


test_first_node_merge_during_delete() ->
    {ok, Id} = append_list:create_structure(10),
    append_list:add(Id, prepare_batch(10, 31)),
    append_list:delete(Id, lists:seq(10, 30)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({Expected, _}, append_list:list(Id, 100)).


test_delete_elems_one_by_one_descending() ->
    {ok, Id} = append_list:create_structure(10),
    append_list:add(Id, prepare_batch(10, 31)),
    lists:foreach(fun(Elem) ->
        append_list:delete(Id, [Elem])
    end, lists:seq(30, 10, -1)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({Expected, _}, append_list:list(Id, 100)).


test_delete_elems_one_by_one_ascending() ->
    {ok, Id} = append_list:create_structure(10),
    append_list:add(Id, prepare_batch(10, 31)),
    lists:foreach(fun(Elem) ->
        append_list:delete(Id, [Elem])
    end, lists:seq(10, 30)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({Expected, _}, append_list:list(Id, 100)).


test_delete_elems_structure_not_sorted() ->
    {ok, Id} = append_list:create_structure(10),
    append_list:add(Id, prepare_batch(20, 31)),
    append_list:add(Id, prepare_batch(10, 20)),
    lists:foreach(fun(Elem) ->
        append_list:delete(Id, [Elem])
    end, lists:seq(30, 10, -1)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({Expected, _}, append_list:list(Id, 100)).


test_merge_nodes_during_delete_structure_not_sorted() ->
    {ok, Id} = append_list:create_structure(1),
    lists:foreach(fun(Elem) ->
        append_list:add(Id, Elem)
    end, prepare_batch(5, 1, -1)),
    append_list:delete(Id, [5]),
    Expected = lists:reverse(prepare_batch(4, 1, -1)),
    ?assertMatch({Expected, _}, append_list:list(Id, 100)).


test_delete_between_listings() ->
    {ok, Id} = append_list:create_structure(10),
    append_list:add(Id, prepare_batch(10, 30)),
    {_, ListingInfo} = append_list:list(Id, 2),
    append_list:delete(Id, [28, 27]),
    {Res, _} = append_list:list(ListingInfo, 1),
    ?assertMatch([{26, <<"26">>}], Res),
    append_list:delete(Id, lists:seq(20, 29)),
    Expected = lists:reverse(prepare_batch(10, 19)),
    ?assertMatch({Expected, _}, append_list:list(ListingInfo, 100)).


test_get_highest() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_highest(<<"dummy_id">>)),
    {ok, Id} = append_list:create_structure(10),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_highest(Id)),
    append_list:add(Id, prepare_batch(1, 100)),
    ?assertEqual({100, <<"100">>}, append_list:get_highest(Id)),
    append_list:delete(Id, lists:seq(2, 99)),
    ?assertEqual({100, <<"100">>}, append_list:get_highest(Id)),
    append_list:delete(Id, [100]),
    ?assertEqual({1, <<"1">>}, append_list:get_highest(Id)),
    append_list:delete(Id, [1]),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_highest(Id)).

    
test_get_highest_structure_not_sorted() ->
    {ok, Id} = append_list:create_structure(10),
    lists:foreach(fun(Elem) ->
        append_list:add(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual({100, <<"100">>}, append_list:get_highest(Id)),
    append_list:delete(Id, lists:seq(2, 99)),
    ?assertEqual({100, <<"100">>}, append_list:get_highest(Id)).


test_get_max_key() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_max_key(<<"dummy_id">>)),
    {ok, Id} = append_list:create_structure(10),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_max_key(Id)),
    append_list:add(Id, prepare_batch(1, 100)),
    ?assertEqual(100, append_list:get_max_key(Id)),
    append_list:delete(Id, lists:seq(2, 99)),
    ?assertEqual(100, append_list:get_max_key(Id)),
    append_list:delete(Id, [100]),
    ?assertEqual(1, append_list:get_max_key(Id)),
    append_list:delete(Id, [1]),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_max_key(Id)).


test_get_max_key_structure_not_sorted() ->
    {ok, Id} = append_list:create_structure(10),
    lists:foreach(fun(Elem) ->
        append_list:add(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual(100, append_list:get_max_key(Id)),
    append_list:delete(Id, lists:seq(2, 99)),
    ?assertEqual(100, append_list:get_max_key(Id)).


test_get() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(<<"dummy_id">>), 8),
    {ok, Id} = append_list:create_structure(10),
    ?assertEqual([], append_list:get(Id, 8)),
    append_list:add(Id, prepare_batch(1, 100)),
    ?assertEqual([{8, <<"8">>}], append_list:get(Id, 8)),
    ?assertEqual(prepare_batch(8, 50), lists:sort(append_list:get(Id, lists:seq(8,50)))),
    append_list:delete(Id, lists:seq(2,99)),
    ?assertEqual([], append_list:get(Id, 8)),
    ?assertEqual([{1, <<"1">>}, {100, <<"100">>}], lists:sort(append_list:get(Id, lists:seq(1,100)))).


test_get_structure_not_sorted() ->
    {ok, Id} = append_list:create_structure(10),
    lists:foreach(fun(Elem) ->
        append_list:add(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual([{8, <<"8">>}], append_list:get(Id, 8)),
    ?assertEqual(prepare_batch(8, 50), lists:sort(append_list:get(Id, lists:seq(8,50)))),
    append_list:delete(Id, lists:seq(2, 99)),
    ?assertEqual([], append_list:get(Id, 8)),
    ?assertEqual([{1, <<"1">>}, {100, <<"100">>}], lists:sort(append_list:get(Id, lists:seq(1,100)))).


test_get_start_from_last() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(<<"dummy_id">>), 8),
    {ok, Id} = append_list:create_structure(10),
    ?assertEqual([], append_list:get(Id, 8, last)),
    append_list:add(Id, prepare_batch(1, 100)),
    ?assertEqual([{8, <<"8">>}], append_list:get(Id, 8, last)),
    ?assertEqual(prepare_batch(8, 50), lists:sort(append_list:get(Id, lists:seq(8,50), last))),
    append_list:delete(Id, lists:seq(2,99)),
    ?assertEqual([], append_list:get(Id, 8, last)),
    ?assertEqual([{1, <<"1">>}, {100, <<"100">>}], lists:sort(append_list:get(Id, lists:seq(1,100), last))).


test_get_structure_not_sorted_start_from_last() ->
    {ok, Id} = append_list:create_structure(10),
    lists:foreach(fun(Elem) ->
        append_list:add(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual([{8, <<"8">>}], append_list:get(Id, 8, last)),
    ?assertEqual(prepare_batch(8, 50), lists:sort(append_list:get(Id, lists:seq(8,50), last))),
    append_list:delete(Id, lists:seq(2, 99)),
    ?assertEqual([], append_list:get(Id, 8, last)),
    ?assertEqual([{1, <<"1">>}, {100, <<"100">>}], lists:sort(append_list:get(Id, lists:seq(1,100), last))).


test_nodes_created_after_add() ->
    {ok, Id} = append_list:create_structure(1),
    ?assertMatch(#sentinel{first = undefined, last = undefined}, append_list_persistence:get_node(Id)),
    append_list:add(Id, prepare_batch(1, 1)),
    ?assertNotMatch(#sentinel{last = undefined}, append_list_persistence:get_node(Id)),
    #sentinel{last = LastNodeId} = append_list_persistence:get_node(Id),
    ?assertMatch(#sentinel{first = LastNodeId}, append_list_persistence:get_node(Id)),
    ?assertMatch(#node{next = undefined, prev = undefined}, append_list_persistence:get_node(LastNodeId)),
    append_list:add(Id, prepare_batch(2, 2)),
    ?assertNotMatch(#sentinel{first = LastNodeId}, append_list_persistence:get_node(Id)),
    #sentinel{first = FirstNodeId} = append_list_persistence:get_node(Id),
    ?assertMatch(#node{next = FirstNodeId, prev = undefined}, append_list_persistence:get_node(LastNodeId)),
    ?assertMatch(#node{next = undefined, prev = LastNodeId}, append_list_persistence:get_node(FirstNodeId)).


test_min_on_left_after_add() ->
    {ok, Id} = append_list:create_structure(10),
    append_list:add(Id, prepare_batch(1, 100)),
    #sentinel{first = FirstNodeId} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMinsOnLeft = [undefined] ++ lists:seq(91, 11, -10),
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch(#node{min_on_left = Expected}, append_list_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsOnLeft)).


test_min_on_left_after_add_reversed() ->
    {ok, Id} = append_list:create_structure(10),
    lists:foreach(fun(Elem) ->
        append_list:add(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    #sentinel{first = FirstNodeId} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMinsOnLeft = [undefined] ++ lists:duplicate(9, 1),
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch(#node{min_on_left = Expected}, append_list_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsOnLeft)).


test_max_on_right_after_add() ->
    {ok, Id} = append_list:create_structure(10),
    append_list:add(Id, prepare_batch(1, 100)),
    #sentinel{first = FirstNodeId} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMaxOnRight = lists:seq(90, 10, -10) ++ [undefined],
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch(#node{max_on_right = Expected}, append_list_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMaxOnRight)).


test_max_on_right_after_add_reversed() ->
    {ok, Id} = append_list:create_structure(10),
    lists:foreach(fun(Elem) ->
        append_list:add(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    #sentinel{first = FirstNodeId} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMinsOnLeft = lists:duplicate(9, 100) ++ [undefined],
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch(#node{max_on_right = Expected}, append_list_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsOnLeft)).


test_node_num_after_add() ->
    {ok, Id} = append_list:create_structure(1),
    append_list:add(Id, prepare_batch(1, 10)),
    #sentinel{first = FirstNodeId} = append_list_persistence:get_node(Id),
    #node{prev = PrevNodeId, node_num = FirstNodeNum} = append_list_persistence:get_node(FirstNodeId),
    NodeIds = get_nodes_ids(PrevNodeId),
    lists:foldl(fun(NodeId, NextNodeNum) ->
        #node{node_num = Num} = append_list_persistence:get_node(NodeId),
        ?assert(Num < NextNodeNum),
        Num
    end, FirstNodeNum, NodeIds).

% fixme test elems per node after add


test_nodes_deleted_after_delete_elems() ->
    {ok, Id} = append_list:create_structure(1),
    append_list:add(Id, prepare_batch(1, 3)),
    #sentinel{first = FirstNodeId} = append_list_persistence:get_node(Id),
    [Node1, Node2, Node3] = get_nodes_ids(FirstNodeId),
    append_list:delete(Id, [2]),
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(Node2)),
    ?assertMatch(#node{next = Node1}, append_list_persistence:get_node(Node3)),
    ?assertMatch(#node{prev = Node3}, append_list_persistence:get_node(Node1)),
    append_list:delete(Id, [3]),
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(Node1)),
    ?assertMatch(#node{next = undefined}, append_list_persistence:get_node(Node3)),
    ?assertMatch(#sentinel{first = Node3, last = Node3}, append_list_persistence:get_node(Id)).

    
test_nodes_after_delete_elems_from_last_node() ->
    {ok, Id} = append_list:create_structure(1),
    append_list:add(Id, prepare_batch(1, 3)),
    #sentinel{first = FirstNodeId} = append_list_persistence:get_node(Id),
    [Node1, Node2, Node3] = get_nodes_ids(FirstNodeId),
    append_list:delete(Id, [1]),
    ?assertMatch(#node{prev = undefined, next = Node1}, append_list_persistence:get_node(Node2)),
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(Node3)),
    ?assertMatch(#node{prev = Node2, next = undefined}, append_list_persistence:get_node(Node1)),
    append_list:delete(Id, [3]),
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(Node3)),
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(Node1)),
    ?assertMatch(#node{next = undefined, prev = undefined}, append_list_persistence:get_node(Node2)),
    ?assertMatch(#sentinel{first = Node2, last = Node2}, append_list_persistence:get_node(Id)).


test_min_on_left_after_delete_elems() ->
    {ok, Id} = append_list:create_structure(1),
    append_list:add(Id, prepare_batch(2, 10)),
    append_list:add(Id, {1, <<"1">>}),
    append_list:delete(Id, [1]),
    #sentinel{first = FirstNodeId} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMinsOnLeft = [undefined] ++ lists:seq(10, 3, -1),
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch(#node{min_on_left = Expected}, append_list_persistence:get_node(NodeId))
    end, lists:zip(NodesIds, ExpectedMinsOnLeft)).


test_max_on_right_after_delete_elems() ->
    {ok, Id} = append_list:create_structure(1),
    append_list:add(Id, {10, <<"10">>}),
    append_list:add(Id, prepare_batch(1, 9)),
    append_list:delete(Id, [10]),
    #sentinel{first = FirstNodeId} = append_list_persistence:get_node(Id),
    NodesIds = get_nodes_ids(FirstNodeId),
    ExpectedMaxOnRight = lists:seq(8, 1, -1) ++ [undefined],
    lists:foreach(fun({NodeId, Expected}) ->
        ?assertMatch(#node{max_on_right = Expected}, append_list_persistence:get_node(NodeId))
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
    #node{prev = PrevNodeId} = append_list_persistence:get_node(NodeId),
    [NodeId] ++ get_nodes_ids(PrevNodeId).
    