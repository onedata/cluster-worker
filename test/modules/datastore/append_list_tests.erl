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
            % fixme implement tests checking structure (nodes, elems in node, min_on_left, etc.)
            {"test_delete_struct", fun test_delete_struct/0},
            {"test_add_elements_multi_nodes", fun test_add_elements_multi_nodes/0},
            {"test_add_elements_one_node", fun test_add_elements_one_node/0},
            {"test_add_only_existing", fun test_add_only_existing_elements/0},
            {"test_add_with_overwrite", fun test_add_with_overwrite/0},
            {"test_get_many_with_listing_info", fun test_get_many_with_listing_info/0},
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
            {"test_get_highest", fun test_get_highest/0},
            {"test_get_highest_structure_not_sorted", fun test_get_highest_structure_not_sorted/0}
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
    ?assertEqual(ok, append_list:delete(<<"dummy_id">>)),
    {ok, Id} = append_list:create(10),
    append_list:add(Id, prepare_batch(10, 30)),
    ?assertEqual(ok, append_list:delete(Id)),
    ?assertEqual([], ets:tab2list(append_list_persistence)).


test_add_elements_one_node() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:add(<<"dummy_id">>, prepare_batch(1, 100))),
    ?assertMatch({[], _}, append_list:get_many(<<"dummy_id">>, 100)),
    ?assertEqual(ok, append_list:add(<<"dummy_id">>, [])),
    {ok, Id} = append_list:create(10),
    ?assertEqual({ok, []}, append_list:add(Id, [{1, <<"1">>}])),
    ?assertMatch({[{1, <<"1">>}], _}, append_list:get_many(Id, 100)).


test_add_elements_multi_nodes() ->
    {ok, Id} = append_list:create(10),
    Batch = prepare_batch(10, 30),
    append_list:add(Id, Batch),
    Expected = lists:reverse(Batch),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


test_add_only_existing_elements() ->
    {ok, Id} = append_list:create(10),
    Batch = prepare_batch(10, 30),
    ?assertEqual({ok, []}, append_list:add(Id, Batch)),
    ?assertEqual({ok, lists:reverse(lists:seq(10, 30))}, append_list:add(Id, Batch)),
    Expected = lists:reverse(Batch),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


test_add_with_overwrite() ->
    {ok, Id} = append_list:create(10),
    Batch1 = prepare_batch(10, 30),
    ?assertEqual({ok, []}, append_list:add(Id, Batch1)),
    Batch2 = prepare_batch(10, 32, fun(A) -> integer_to_binary(2*A) end),
    ?assertEqual({ok, lists:reverse(lists:seq(10, 30))}, append_list:add(Id, Batch2)),
    Expected = lists:reverse(Batch2),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)),
    ?assertEqual({ok, [20]}, append_list:add(Id, [{20, <<"40">>}])),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


test_get_many_with_listing_info() ->
    ?assertMatch({[], _}, append_list:get_many(<<"dummy_id">>, 1000)),
    {ok, Id} = append_list:create(10),
    ?assertMatch({[], _}, append_list:get_many(Id, 0)),
    append_list:add(Id, prepare_batch(10, 30)),
    lists:foldl(fun(X, ListingInfo) ->
        {Res, NewListingInfo} = append_list:get_many(ListingInfo, 1),
        ?assertEqual([{X, integer_to_binary(X)}], Res),
        NewListingInfo
    end, Id, lists:seq(30, 10, -1)),
    {_, ListingInfo} = append_list:get_many(Id, 1000),
    ?assertMatch({[], _}, append_list:get_many(ListingInfo, 100)).


test_delete_consecutive_elems_between_nodes() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:delete_elems(<<"dummy_id">>, [])),
    {ok, Id} = append_list:create(10),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:delete_elems(Id, [])),
    append_list:add(Id, prepare_batch(10, 30)),
    append_list:delete_elems(Id, lists:seq(10, 20)),
    Expected = lists:reverse(prepare_batch(21, 30)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)),
    append_list:delete_elems(Id, lists:seq(25, 30)),
    Expected1 = lists:reverse(prepare_batch(21, 24)),
    ?assertMatch({Expected1, _}, append_list:get_many(Id, 100)).


test_delete_non_consecutive_elems_between_nodes() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, prepare_batch(1, 100)),
    append_list:delete_elems(Id, lists:seq(1,100, 2)),
    Expected = lists:reverse(prepare_batch(2, 100 ,2)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


test_delete_all_elems_in_first_node() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, prepare_batch(10, 30)),
    append_list:delete_elems(Id, [30]),
    Expected = lists:reverse(prepare_batch(10, 29)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)),
    append_list:delete_elems(Id, lists:seq(20, 30)),
    Expected1 = lists:reverse(prepare_batch(10, 19)),
    ?assertMatch({Expected1, _}, append_list:get_many(Id, 100)).


test_delete_elems_all_but_first_node() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, prepare_batch(10, 30)),
    append_list:delete_elems(Id, lists:seq(10, 29)),
    Expected = lists:reverse(prepare_batch(30, 30)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


test_first_node_merge_during_delete() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, prepare_batch(10, 31)),
    append_list:delete_elems(Id, lists:seq(10, 30)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


test_delete_elems_one_by_one_descending() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, prepare_batch(10, 31)),
    lists:foreach(fun(Elem) ->
        append_list:delete_elems(Id, [Elem])
    end, lists:seq(30, 10, -1)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


test_delete_elems_one_by_one_ascending() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, prepare_batch(10, 31)),
    lists:foreach(fun(Elem) ->
        append_list:delete_elems(Id, [Elem])
    end, lists:seq(10, 30)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


test_delete_elems_structure_not_sorted() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, prepare_batch(20, 31)),
    append_list:add(Id, prepare_batch(10, 20)),
    lists:foreach(fun(Elem) ->
        append_list:delete_elems(Id, [Elem])
    end, lists:seq(30, 10, -1)),
    Expected = lists:reverse(prepare_batch(31, 31)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


test_merge_nodes_during_delete_structure_not_sorted() ->
    {ok, Id} = append_list:create(1),
    lists:foreach(fun(Elem) ->
        append_list:add(Id, Elem)
    end, prepare_batch(5, 1, -1)),
    append_list:delete_elems(Id, [5]),
    Expected = lists:reverse(prepare_batch(4, 1, -1)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


test_delete_between_listings() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, prepare_batch(10, 30)),
    {_, ListingInfo} = append_list:get_many(Id, 2),
    append_list:delete_elems(Id, [28, 27]),
    {Res, _} = append_list:get_many(ListingInfo, 1),
    ?assertMatch([{26, <<"26">>}], Res),
    append_list:delete_elems(Id, lists:seq(20, 29)),
    Expected = lists:reverse(prepare_batch(10, 19)),
    ?assertMatch({Expected, _}, append_list:get_many(ListingInfo, 100)).


test_get_highest() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_highest(<<"dummy_id">>)),
    {ok, Id} = append_list:create(10),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_highest(Id)),
    append_list:add(Id, prepare_batch(1, 100)),
    ?assertEqual({100, <<"100">>}, append_list:get_highest(Id)),
    append_list:delete_elems(Id, lists:seq(2, 99)),
    ?assertEqual({100, <<"100">>}, append_list:get_highest(Id)),
    append_list:delete_elems(Id, [100]),
    ?assertEqual({1, <<"1">>}, append_list:get_highest(Id)),
    append_list:delete_elems(Id, [1]),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_highest(Id)).

    
test_get_highest_structure_not_sorted() ->
    {ok, Id} = append_list:create(10),
    lists:foreach(fun(Elem) ->
        append_list:add(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual({100, <<"100">>}, append_list:get_highest(Id)),
    append_list:delete_elems(Id, lists:seq(2, 99)),
    ?assertEqual({100, <<"100">>}, append_list:get_highest(Id)).


test_get() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(<<"dummy_id">>), 8),
    {ok, Id} = append_list:create(10),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get(Id, 8)),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(1,100))),
    ?assertEqual({ok, 8}, append_list:get(Id, 8)),
    append_list:delete_elems(Id, lists:seq(2,99)),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get(Id, 8)).


test_get_structure_not_sorted() ->
    {ok, Id} = append_list:create(10),
    lists:foreach(fun(Elem) ->
        append_list:add(Id, Elem)
    end, prepare_batch(100, 1, -1)),
    ?assertEqual({ok, <<"8">>}, append_list:get(Id, 8)),
    append_list:delete_elems(Id, lists:seq(2, 99)),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get(Id, 8)).

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