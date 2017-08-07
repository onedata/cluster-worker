%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module tests the functionality of couchbase_crud, using eunit tests.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_crud_tests).

-ifdef(TEST).

-include("global_definitions.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([store_fun/0]).

%%%===================================================================
%%% Tests description
%%%===================================================================

%% This test generator tests the functionalities.
batch_size_verification_test_() ->
    {foreach,
        fun setup/0,
        fun(_) -> ok end,
        [
            {"validate_init", fun validate_init/0},
            {"validate_timeout", fun validate_timeout/0},
            {"validate_size_increase", fun validate_size_increase/0},
            {"validate_size_increase_trigger",
                fun validate_size_increase_trigger/0}
        ]
    }.

%%%===================================================================
%%% Test functions
%%%===================================================================

validate_init() ->
    couchbase_crud:init_batch_size_check(lists:seq(1, 10)),
    ?assertEqual(false, get(batch_size_check)),

    couchbase_crud:init_batch_size_check(lists:seq(1, 100)),
    ?assertEqual([], get(batch_size_check)).

validate_timeout() ->
    couchbase_crud:timeout(),
    ?assertEqual(50, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    couchbase_crud:timeout(),
    ?assertEqual(50, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    couchbase_crud:timeout(),
    ?assertEqual(25, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    couchbase_crud:timeout(),
    ?assertEqual(25, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)).

validate_size_increase_trigger() ->
    put(batch_size_check, false),
    ?assertEqual(ok,
        couchbase_crud:execute_and_check_batch_size(?MODULE, store_fun, [])),
    ?assertEqual(false, get(batch_size_check)),

    put(batch_size_check, []),
    ?assertEqual(ok,
        couchbase_crud:execute_and_check_batch_size(?MODULE, store_fun, [])),
    L1 = get(batch_size_check),
    ?assertEqual(1, length(L1)),

    ?assertEqual(ok,
        couchbase_crud:execute_and_check_batch_size(?MODULE, store_fun, [])),
    L2 = get(batch_size_check),
    ?assertEqual(2, length(L2)),

    ?assertEqual(ok,
        couchbase_crud:execute_and_check_batch_size(?MODULE, store_fun, [])),
    L3 = get(batch_size_check),
    ?assertEqual(3, length(L3)),

    ?assertEqual(ok,
        couchbase_crud:execute_and_check_batch_size(?MODULE, store_fun, [])),
    L4 = get(batch_size_check),
    ?assertEqual(4, length(L4)),
    lists:foreach(fun(T) ->
        ?assert(T < 100)
    end, L4).


validate_size_increase() ->
    put(batch_size_check, [1000000, 0, 1]),
    ?assertEqual(ok,
        couchbase_crud:execute_and_check_batch_size(?MODULE, store_fun, [])),
    ?assertEqual(100, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    put(batch_size_check, [4, 15000, 1]),
    ?assertEqual(ok,
        couchbase_crud:execute_and_check_batch_size(?MODULE, store_fun, [])),
    ?assertEqual(200, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    put(batch_size_check, [4, 0, 1]),
    ?assertEqual(ok,
        couchbase_crud:execute_and_check_batch_size(?MODULE, store_fun, [])),
    ?assertEqual(200, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    put(batch_size_check, [4, 0, 1]),
    ?assertEqual(ok,
        couchbase_crud:execute_and_check_batch_size(?MODULE, store_fun, [])),
    ?assertEqual(400, application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, undefined)),

    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    put(batch_size_check, [4, 0, 1]),
    ?assertEqual(ok,
        couchbase_crud:execute_and_check_batch_size(?MODULE, store_fun, [])),
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
%%% Helper fun
%%%===================================================================
store_fun() ->
    ok.