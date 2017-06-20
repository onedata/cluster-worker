%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This test checks requests routing inside OP cluster.
%%% @end
%%%--------------------------------------------------------------------
-module(worker_db_stress_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
%%tests
-export([stress_test/1,
    datastore_mixed_db_test/1, datastore_links_number_db_test/1
]).
%%test_bases
-export([stress_test_base/1,
    datastore_mixed_db_test_base/1, datastore_links_number_db_test_base/1
]).

-define(STRESS_CASES, [
        datastore_mixed_db_test
    ]).

-define(STRESS_NO_CLEARING_CASES, [
        datastore_mixed_db_test, datastore_links_number_db_test
    ]).

all() ->
    ?STRESS_ALL(?STRESS_CASES, ?STRESS_NO_CLEARING_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

stress_test(Config) ->
    ?STRESS(Config, [
            {description, "Main stress test function. Links together all cases to be done multiple times as one continous test."},
            {success_rate, 95},
            {config, [{name, stress}, {description, "Basic config for stress test"}]}
        ]).
stress_test_base(Config) ->
    ?STRESS_TEST_BASE(Config).

%%%===================================================================

datastore_mixed_db_test(Config) ->
    ?PERFORMANCE(Config, [
            {parameters, [
                [{name, threads_num}, {value, 20}, {description, "Number of threads used during the test."}],
                [{name, docs_per_thead}, {value, 10}, {description, "Number of documents used by single threads."}],
                [{name, ops_per_doc}, {value, 2}, {description, "Number of oprerations on each document."}],
                [{name, conflicted_threads}, {value, 2}, {description, "Number of threads that work with the same documents set."}]
            ]},
            {description, "Performs multiple datastore operations using many threads. Level - database."}
        ]).
datastore_mixed_db_test_base(Config) ->
    datastore_basic_ops_utils:mixed_test(Config, disk_only).

datastore_links_number_db_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, threads_num}, {value, 20}, {description, "Number of threads used during the test."}],
            [{name, docs_per_thead}, {value, 10}, {description, "Number of documents used by single threads."}],
            [{name, ops_per_doc}, {value, 2}, {description, "Number of oprerations on each document."}],
            [{name, conflicted_threads}, {value, 2}, {description, "Number of threads that work with the same documents set."}]
        ]},
        {description, "Performs multiple datastore links operations using many threads. Level - database."}
    ]).
datastore_links_number_db_test_base(Config) ->
    datastore_basic_ops_utils:links_number_test(Config, disk_only).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(Case, Config) when
    Case =:= datastore_mixed_db_test;
    Case =:= datastore_links_number_db_test ->
    datastore_basic_ops_utils:set_env(Case, Config);

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Case, Config) when
    Case =:= datastore_mixed_db_test;
    Case =:= datastore_links_number_db_test ->
    datastore_basic_ops_utils:clear_env(Config);

end_per_testcase(_Case, _Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
