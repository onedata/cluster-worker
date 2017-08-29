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
        fun(_) -> ok end,
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

            {"decrease_batch_size_should_change_size_for_old_check_time",
                fun decrease_batch_size_should_change_size_for_old_check_time/0},
            {"decrease_batch_size_should_not_change_size_for_min_batch_size",
                fun decrease_batch_size_should_not_change_size_for_min_batch_size/0},

            {"batch_size_should_be_increased_for_old_check_time",
                fun batch_size_should_be_increased_for_old_check_time/0},
            {"batch_size_should_not_be_increased_for_wrong_response_list_size",
                fun batch_size_should_not_be_increased_for_wrong_response_list_size/0},
            {"batch_size_should_not_be_increased_for_timeout",
                fun batch_size_should_not_be_increased_for_timeout/0},
            {"batch_size_should_not_be_increased_for_high_execution_time",
                fun batch_size_should_not_be_increased_for_high_execution_time/0},
            {"batch_size_should_not_be_increased_for_max_batch_size",
                fun batch_size_should_not_be_increased_for_max_batch_size/0}
        ]
    }.

%%%===================================================================
%%% Test functions
%%%===================================================================

check_timeout_should_return_ok_for_empty_list() ->
    ?assertEqual(ok, couchbase_batch:check_timeout([])).

check_timeout_should_return_ok_for_list_without_error() ->
    ?assertEqual(ok, couchbase_batch:check_timeout(
        [{key, ok}, {key2, ok}, {key3, ok}])).

check_timeout_should_return_ok_for_list_with_non_timeout_error() ->
    ?assertEqual(ok, couchbase_batch:check_timeout(
        [{key, ok}, {key2, ok}, {key3, {error, error}}])).

check_timeout_should_return_timeout_for_list_with_timeout() ->
    ?assertEqual(timeout, couchbase_batch:check_timeout(
        [{key, ok}, {key2, {error, etimedout}}, {key3, ok}])).

check_timeout_should_return_timeout_for_list_with_etimedout() ->
    ?assertEqual(timeout, couchbase_batch:check_timeout(
        [{key, ok}, {key2, {error, timeout}}, {key3, ok}])).

decrease_batch_size_should_change_size_for_old_check_time() ->
    couchbase_batch:decrease_batch_size(),
    ?assertEqual(50, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    couchbase_batch:decrease_batch_size(),
    ?assertEqual(50, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    couchbase_batch:decrease_batch_size(),
    ?assertEqual(25, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)).

decrease_batch_size_should_not_change_size_for_min_batch_size() ->
    couchbase_batch:decrease_batch_size(),
    ?assertEqual(50, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    couchbase_batch:decrease_batch_size(),
    ?assertEqual(25, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    couchbase_batch:decrease_batch_size(),
    ?assertEqual(25, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)).

batch_size_should_be_increased_for_old_check_time() ->
    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(100),
        [4, 15000, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(200, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(200),
        [4, 0, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(200, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(200),
        [4, 0, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(400, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)).

batch_size_should_not_be_increased_for_wrong_response_list_size() ->
    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(100),
        [4, 15000, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(200, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(100),
        [4, 0, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(200, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)).

batch_size_should_not_be_increased_for_timeout() ->
    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(100),
        [4, 0, 1, 5], [ok, ok, timeout, ok])),
    ?assertEqual(100, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)).

batch_size_should_not_be_increased_for_high_execution_time() ->
    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(100),
        [1000000, 0, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(100, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)).

batch_size_should_not_be_increased_for_max_batch_size() ->
    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(100),
        [4, 15000, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(200, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(200),
        [4, 0, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(400, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    ?assertEqual(ok, couchbase_batch:verify_batch_size_increase(get_response_map(400),
        [4, 0, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(400, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)).

-endif.

%%%===================================================================
%%% Test setup
%%%===================================================================

setup() ->
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, 100),
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_min_batch_size, 25),
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_max_batch_size, 400),
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_operation_timeout, 60000),
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_durability_timeout, 60000),
    ok.

%%%===================================================================
%%% Helper function
%%%===================================================================

get_response_map(Size) ->
    maps:from_list(lists:zip(lists:seq(1, Size), lists:seq(1, Size))).