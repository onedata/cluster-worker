%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module tests the functionality of couchbase_batch, using eunit tests.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_batch_tests).

-ifdef(TEST).

-include("global_definitions.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Tests description
%%%===================================================================

%% This test generator tests the functionalities.
couchbase_batch_verification_test_() ->
    {foreach,
        fun setup/0,
        fun cleanup/1,
        [
            {"check_timeout_should_return_ok_for_empty_list",
                fun check_timeout_should_return_ok_for_empty_list/0},
            {"check_timeout_should_return_ok_for_list_without_error",
                fun check_timeout_should_return_ok_for_list_without_error/0},
            {"check_timeout_should_return_ok_for_list_with_non_timeout_error",
                fun check_timeout_should_return_ok_for_list_with_non_timeout_error/0},
            {"check_timeout_should_return_timeout_for_list_with_timeout",
                fun check_timeout_should_return_timeout_for_list_with_timeout/0},
            {"check_timeout_should_return_timeout_for_list_with_etimedout",
                fun check_timeout_should_return_timeout_for_list_with_etimedout/0},

            {"decrease_batch_size_should_change_size",
                fun decrease_batch_size_should_change_size/0},

            {"batch_size_should_be_increased",
                fun batch_size_should_be_increased/0},
            {"batch_size_should_not_be_increased_for_timeout",
                fun batch_size_should_not_be_increased_for_timeout/0},
            {"batch_size_should_not_be_increased_for_high_execution_time",
                fun batch_size_should_not_be_increased_for_high_execution_time/0},
            {"batch_size_should_not_be_increased_over_max_batch_size",
                fun batch_size_should_not_be_increased_over_max_batch_size/0}
        ]
    }.

%%%===================================================================
%%% Test functions
%%%===================================================================

check_timeout_should_return_ok_for_empty_list() ->
    ?assertEqual(ok, couchbase_batch:check_timeout([], test, 0)),

    ?assertEqual(undefined, get({[thread, 0, mod, couchbase_batch, submod, none, times], reset})),
    ?assertEqual(undefined, get({[thread, 0, mod, couchbase_batch, submod, none, sizes], reset})),
    ?assertEqual(undefined, get([thread, 0, mod, couchbase_batch, submod, none, sizes_config])).

check_timeout_should_return_ok_for_list_without_error() ->
    ?assertEqual(ok, couchbase_batch:check_timeout(
        [{key, ok}, {key2, ok}, {key3, ok}], test, 0)).

check_timeout_should_return_ok_for_list_with_non_timeout_error() ->
    ?assertEqual(ok, couchbase_batch:check_timeout(
        [{key, ok}, {key2, ok}, {key3, {error, error}}], test, 0)).

check_timeout_should_return_timeout_for_list_with_timeout() ->
    ?assertEqual(timeout, couchbase_batch:check_timeout(
        [{key, ok}, {key2, {error, etimedout}}, {key3, ok}], test, 0)).

check_timeout_should_return_timeout_for_list_with_etimedout() ->
    ?assertEqual(timeout, couchbase_batch:check_timeout(
        [{key, ok}, {key2, {error, timeout}}, {key3, ok}], test, 0)).

decrease_batch_size_should_change_size() ->
    couchbase_batch:decrease_batch_size([]),
    ?assertEqual(25, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    ?assertEqual(ok, get({[thread, 0, mod, couchbase_batch, submod, none, times], reset})),
    ?assertEqual(ok, get({[thread, 0, mod, couchbase_batch, submod, none, sizes], reset})).
%    ?assertEqual([25], get([thread, 0, mod, couchbase_batch, submod, none, sizes_config])).

batch_size_should_be_increased() ->
    put({[thread, 0, mod, couchbase_batch, submod, none, timeouts], [count]}, {ok, [{count, 0}]} ),
    put({[thread, 0, mod, couchbase_batch, submod, none, sizes], [mean]}, {ok, [{mean, 50}]}),
    put({[thread, 0, mod, couchbase_batch, submod, none, times], [max, mean]}, {ok, [{mean, 1500}, {max, 1500}, {n, 50}]}),
    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(100),
        [4, 50, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(500, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)).

batch_size_should_not_be_increased_for_timeout() ->
    put({[thread, 0, mod, couchbase_batch, submod, none, timeouts], [count]}, {ok, [{count, 1}]} ),
    put({[thread, 0, mod, couchbase_batch, submod, none, sizes], [mean]}, {ok, [{mean, 50}]}),
    put({[thread, 0, mod, couchbase_batch, submod, none, times], [max, mean]}, {ok, [{mean, 1500}, {max, 1500}, {n, 50}]}),
    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(100),
        [4, 50, 1, 5], [ok, ok, timeout, ok])),
    ?assertEqual(100, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)).

batch_size_should_not_be_increased_for_high_execution_time() ->
    put({[thread, 0, mod, couchbase_batch, submod, none, timeouts], [count]}, {ok, [{count, 0}]} ),
    put({[thread, 0, mod, couchbase_batch, submod, none, sizes], [mean]}, {ok, [{mean, 50}]}),
    put({[thread, 0, mod, couchbase_batch, submod, none, times], [max, mean]}, {ok, [{mean, 1500}, {max, 15000}, {n, 50}]}),
    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(100),
        [4, 50, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(100, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)).

batch_size_should_not_be_increased_over_max_batch_size() ->
    put({[thread, 0, mod, couchbase_batch, submod, none, timeouts], [count]}, {ok, [{count, 0}]} ),
    put({[thread, 0, mod, couchbase_batch, submod, none, sizes], [mean]}, {ok, [{mean, 50}]}),
    put({[thread, 0, mod, couchbase_batch, submod, none, times], [max, mean]}, {ok, [{mean, 150}, {max, 1500}, {n, 50}]}),
    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(100),
        [4, 50, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(2000, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)).

-endif.

%%%===================================================================
%%% Test setup and cleanup
%%%===================================================================

setup() ->
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, 100),
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_min_batch_size, 25),
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_max_batch_size, 2000),
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_operation_timeout, 60000),
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_durability_timeout, 300000),

    meck:new(exometer_utils),
    meck:expect(exometer_utils, reset,
        fun(Name) -> put({Name, reset}, ok) end),
    meck:expect(exometer_utils, update_counter,
        fun(Name, Value) -> add_to_list(Name, Value), ok end),
    meck:expect(exometer_utils, get_value,
        fun(Name, Value) -> get({Name, Value}) end),
    ok.

cleanup(_) ->
    meck:unload(exometer_utils),
    ok.

%%%===================================================================
%%% Helper function
%%%===================================================================

get_response_map(Size) ->
    maps:from_list(lists:zip(lists:seq(1, Size), lists:seq(1, Size))).

add_to_list(Name, Element) ->
    Current = case get(Name) of
        undefined ->
            [];
        List ->
            List
    end,
    put(Name, [Element | Current]).
