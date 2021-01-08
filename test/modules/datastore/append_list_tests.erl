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
            % fixme fun names
            % fixme max_on_right and min_on_left tests
            {"", fun delete_struct/0},
            {"", fun add/0},
            {"", fun add1/0},
            {"8", fun add2/0},
            {"", fun add_existing/0},
            {"123213", fun add_overwrite_existing/0},
            {"", fun list/0},
            {"", fun delete_elems/0},
            {"13", fun delete_elems1/0},
            {"", fun delete_elems2/0},
            {"9999", fun delete_elems3/0},
            {"", fun delete_elems4/0},
            {"", fun delete_elems_one_by_one_descending/0},
            {"123", fun delete_elems6/0},
            {"", fun delete_elems7/0},
            {"", fun delete_elems8/0},
            {"aaaa", fun delete_between_listings/0},
            {"", fun get_highest/0},
            {"", fun get/0}
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

delete_struct() ->
    ?assertEqual(ok, append_list:delete(<<"dummy_id">>)),
    {ok, Id} = append_list:create(10),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    ?assertEqual(ok, append_list:delete(Id)),
    ?assertEqual([], ets:tab2list(append_list_persistence)).


add() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10,30)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


add1() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(100, 1, -1))),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(100, 1, -1)),
    {Res, _} = append_list:get_many(Id, 1000),
    ?assertMatch(Expected, lists:sort(Res)).


add2() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:add(<<"dummy_id">>, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(100, 1, -1)))),
    ?assertEqual(ok, append_list:add(<<"dummy_id">>, [])),
    {ok, Id} = append_list:create(10),
    append_list:add(Id, [{1,1}]),
    ?assertMatch({[{1,1}], _}, append_list:get_many(Id, 100)).


add_existing() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10,30)),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


add_overwrite_existing() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    Expected = lists:foldl(fun(A, Acc) -> [{A, 2*A} | Acc] end, [], lists:seq(10, 32)),
    append_list:add(Id, Expected),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)),
    append_list:add(Id, [{20,40}]),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


list() ->
    ?assertMatch({[], _}, append_list:get_many(<<"dummy_id">>, 1000)),
    {ok, Id} = append_list:create(10),
    ?assertMatch({[], _}, append_list:get_many(Id, 0)),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    lists:foldl(fun(X, LS) ->
        {Res, NewLS} = append_list:get_many(LS, 1),
        ?assertEqual([{X, X}], Res),
        NewLS
    end, Id, lists:seq(30, 10, -1)),
    {_, ListingInfo} = append_list:get_many(Id, 1000),
    ?assertMatch({[], _}, append_list:get_many(ListingInfo, 100)).


delete_elems() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:delete_elems(<<"dummy_id">>, [])),
    {ok, Id} = append_list:create(10),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:delete_elems(Id, [])),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    append_list:delete_elems(Id, lists:seq(10, 20)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(21,30)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)),
    append_list:delete_elems(Id, lists:seq(25, 30)),
    Expected1 = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(21,24)),
    ?assertMatch({Expected1, _}, append_list:get_many(Id, 100)).

delete_elems1() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(1, 100))),
    append_list:delete_elems(Id, lists:seq(1,100, 2)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(2,100, 2)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


delete_elems2() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    append_list:delete_elems(Id, [30]),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10,29)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)),
    append_list:delete_elems(Id, lists:seq(20,30)),
    Expected1 = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10,19)),
    ?assertMatch({Expected1, _}, append_list:get_many(Id, 100)).


delete_elems3() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    append_list:delete_elems(Id, lists:seq(10, 29)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(30, 30)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


delete_elems4() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 31))),
    append_list:delete_elems(Id, lists:seq(10, 30)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(31, 31)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


delete_elems_one_by_one_descending() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 31))),
    lists:foreach(fun(Elem) ->
        append_list:delete_elems(Id, [Elem])
    end, lists:seq(30, 10, -1)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(31, 31)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


delete_elems6() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 31))),
    lists:foreach(fun(Elem) ->
        append_list:delete_elems(Id, [Elem])
    end, lists:seq(10, 30)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(31, 31)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


delete_elems7() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(31, 20, -1))),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(20, 10, -1))),
    lists:foreach(fun(Elem) ->
        append_list:delete_elems(Id, [Elem])
    end, lists:seq(30, 10, -1)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(31, 31)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


delete_elems8() ->
    {ok, Id} = append_list:create(1),
    lists:foreach(fun(Elem) ->
        append_list:add(Id, [{Elem, Elem}])
    end, lists:seq(5, 1, -1)),
    append_list:delete_elems(Id, [5]),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(4, 1, -1)),
    ?assertMatch({Expected, _}, append_list:get_many(Id, 100)).


delete_between_listings() ->
    {ok, Id} = append_list:create(10),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 30))),
    {_, ListingInfo} = append_list:get_many(Id, 2),
    append_list:delete_elems(Id, [28,27]),
    {Res, _} = append_list:get_many(ListingInfo, 1),
    ?assertMatch([{26,26}], Res),
    append_list:delete_elems(Id, lists:seq(20,29)),
    Expected = lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(10, 19)),
    ?assertMatch({Expected, _}, append_list:get_many(ListingInfo, 100)).


get_highest() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_highest(<<"dummy_id">>)),
    {ok, Id} = append_list:create(10),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_highest(Id)),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(1,100))),
    ?assertEqual({100, 100}, append_list:get_highest(Id)),
    append_list:delete_elems(Id, lists:seq(2,99)),
    ?assertEqual({100, 100}, append_list:get_highest(Id)),
    append_list:delete_elems(Id, [100]),
    ?assertEqual({1,1}, append_list:get_highest(Id)),
    append_list:delete_elems(Id, [1]),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get_highest(Id)),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(100,1, -1))),
    ?assertEqual({100, 100}, append_list:get_highest(Id)),
    append_list:delete_elems(Id, lists:seq(2,99)),
    ?assertEqual({100, 100}, append_list:get_highest(Id)).


get() ->
    ?assertEqual(?ERROR_NOT_FOUND, append_list_persistence:get_node(<<"dummy_id">>), 8),
    {ok, Id} = append_list:create(10),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get(Id, 8)),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(1,100))),
    ?assertEqual({ok, 8}, append_list:get(Id, 8)),
    append_list:delete_elems(Id, lists:seq(2,99)),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get(Id, 8)),
    append_list:add(Id, lists:foldl(fun(A, Acc) -> [{A, A} | Acc] end, [], lists:seq(100,1, -1))),
    ?assertEqual({ok, 8}, append_list:get(Id, 8)),
    append_list:delete_elems(Id, lists:seq(2,99)),
    ?assertEqual(?ERROR_NOT_FOUND, append_list:get(Id, 8)).

-endif.

