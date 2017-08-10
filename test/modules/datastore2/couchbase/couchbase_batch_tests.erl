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
batch_size_verification_test_() ->
    {foreach,
        fun setup/0,
        fun(_) -> ok end,
        [
            {"validate_analyse_answer", fun validate_analyse_answer/0},
            {"validate_timeout", fun validate_timeout/0},
            {"validate_size_increase", fun validate_size_increase/0}
        ]
    }.

%%%===================================================================
%%% Test functions
%%%===================================================================

validate_analyse_answer() ->
    ?assertEqual(ok, couchbase_batch:analyse_answer([])),
    ?assertEqual(ok, couchbase_batch:analyse_answer(
        [{key, ok}, {key2, ok}, {key3, ok}])),
    ?assertEqual(timeout, couchbase_batch:analyse_answer(
        [{key, ok}, {key2, {error, etimedout}}, {key3, ok}])),
    ?assertEqual(timeout, couchbase_batch:analyse_answer(
        [{key, ok}, {key2, {error, timeout}}, {key3, ok}])),

    ?assertEqual(ok, couchbase_batch:analyse_answer(#{})),
    ?assertEqual(ok, couchbase_batch:analyse_answer(
        #{key => {ctx, ok}, key2 => {ctx, ok}, key3 => {ctx, ok}})),
    ?assertEqual(timeout, couchbase_batch:analyse_answer(
        #{key => {ctx, ok}, key2 => {ctx, {error, etimedout}}, key3 => {ctx, ok}})),
    ?assertEqual(timeout, couchbase_batch:analyse_answer(
        #{key => {ctx, ok}, key2 => {ctx, {error, timeout}}, key3 => {ctx, ok}})).

validate_timeout() ->
    couchbase_batch:timeout(),
    ?assertEqual(50, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    couchbase_batch:timeout(),
    ?assertEqual(50, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    couchbase_batch:timeout(),
    ?assertEqual(25, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    couchbase_batch:timeout(),
    ?assertEqual(25, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)).

validate_size_increase() ->
    ?assertEqual(ok, couchbase_batch:analyse_times(get_response_map(100),
        [1000000, 0, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(100, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    ?assertEqual(ok, couchbase_batch:analyse_times(get_response_map(100),
        [4, 15000, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(200, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    ?assertEqual(ok, couchbase_batch:analyse_times(get_response_map(200),
        [4, 0, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(200, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    ?assertEqual(ok, couchbase_batch:analyse_times(get_response_map(100),
        [4, 0, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(200, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    ?assertEqual(ok, couchbase_batch:analyse_times(get_response_map(200),
        [4, 0, 1, 5], [ok, ok, ok, ok])),
    ?assertEqual(400, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    ?assertEqual(ok, couchbase_batch:analyse_times(get_response_map(400),
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