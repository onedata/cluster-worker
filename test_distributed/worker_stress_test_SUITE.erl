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
-module(worker_stress_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([stress_test/1,
    datastore_mixed_db_test/1, datastore_mixed_global_store_test/1, datastore_mixed_local_store_test/1,
    datastore_mixed_global_cache_test/1, datastore_mixed_local_cache_test/1, mixed_cast_test/1
]).

-performance([
    {stress, [
        datastore_mixed_db_test, datastore_mixed_global_store_test, datastore_mixed_local_store_test,
        datastore_mixed_global_cache_test, datastore_mixed_local_cache_test, mixed_cast_test
    ]}, {stress_no_clearing, [
        datastore_mixed_db_test, datastore_mixed_global_cache_test, datastore_mixed_local_cache_test
    ]}
]).
all() ->
    [].

%%%===================================================================
%%% Test functions
%%%===================================================================

-performance([
    {description, "Main stress test function. Links together all cases to be done multiple times as one continous test."},
    {success_rate, 99.9},
    {config, [{name, stress}, {description, "Basic config for stress test"}]}
]).
stress_test(Config) ->
    performance:stress_test(Config).

%%%===================================================================

-performance([
    {parameters, [
        [{name, threads_num}, {value, 20}, {description, "Number of threads used during the test."}],
        [{name, docs_per_thead}, {value, 3}, {description, "Number of documents used by single threads."}],
        [{name, ops_per_doc}, {value, 5}, {description, "Number of oprerations on each document."}],
        [{name, conflicted_threads}, {value, 10}, {description, "Number of threads that work with the same documents set."}]
    ]},
    {description, "Performs multiple datastore operations using many threads. Level - database."}
]).
datastore_mixed_db_test(Config) ->
    datastore_basic_ops_utils:mixed_test(Config, disk_only).

-performance([
    {parameters, [
        [{name, threads_num}, {value, 20}, {description, "Number of threads used during the test."}],
        [{name, docs_per_thead}, {value, 3}, {description, "Number of documents used by single threads."}],
        [{name, ops_per_doc}, {value, 5}, {description, "Number of oprerations on each document."}],
        [{name, conflicted_threads}, {value, 10}, {description, "Number of threads that work with the same documents set."}]
    ]},
    {description, "Performs multiple datastore operations using many threads. Level - global store."}
]).
datastore_mixed_global_store_test(Config) ->
    datastore_basic_ops_utils:mixed_test(Config, global_only).

-performance([
    {parameters, [
        [{name, threads_num}, {value, 20}, {description, "Number of threads used during the test."}],
        [{name, docs_per_thead}, {value, 3}, {description, "Number of documents used by single threads."}],
        [{name, ops_per_doc}, {value, 5}, {description, "Number of oprerations on each document."}],
        [{name, conflicted_threads}, {value, 10}, {description, "Number of threads that work with the same documents set."}]
    ]},
    {description, "Performs multiple datastore operations using many threads. Level - local store."}
]).
datastore_mixed_local_store_test(Config) ->
    datastore_basic_ops_utils:mixed_test(Config, local_only).

-performance([
    {parameters, [
        [{name, threads_num}, {value, 20}, {description, "Number of threads used during the test."}],
        [{name, docs_per_thead}, {value, 3}, {description, "Number of documents used by single threads."}],
        [{name, ops_per_doc}, {value, 5}, {description, "Number of oprerations on each document."}],
        [{name, conflicted_threads}, {value, 10}, {description, "Number of threads that work with the same documents set."}]
    ]},
    {description, "Performs multiple datastore operations using many threads. Level - global cache."}
]).
datastore_mixed_global_cache_test(Config) ->
    datastore_basic_ops_utils:mixed_test(Config, globally_cached).

-performance([
    {parameters, [
        [{name, threads_num}, {value, 20}, {description, "Number of threads used during the test."}],
        [{name, docs_per_thead}, {value, 3}, {description, "Number of documents used by single threads."}],
        [{name, ops_per_doc}, {value, 5}, {description, "Number of oprerations on each document."}],
        [{name, conflicted_threads}, {value, 10}, {description, "Number of threads that work with the same documents set."}]
    ]},
    {description, "Performs multiple datastore operations using many threads. Level - local cache."}
]).
datastore_mixed_local_cache_test(Config) ->
    datastore_basic_ops_utils:mixed_test(Config, locally_cached).

%%%===================================================================

-performance([
    {parameters, [
        [{name, proc_num}, {value, 10}, {description, "Number of threads used during the test."}],
        [{name, proc_repeats}, {value, 10}, {description, "Number of operations done by single threads."}]
    ]},
    {description, "Performs many one worker_proxy calls with various arguments"}
]).
mixed_cast_test(Config) ->
    requests_routing_test_SUITE:mixed_cast_test_core(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(Case, Config) when
    Case =:= datastore_mixed_db_test;
    Case =:= datastore_mixed_global_store_test;
    Case =:= datastore_mixed_local_store_test;
    Case =:= datastore_mixed_local_cache_test;
    Case =:= datastore_mixed_global_cache_test ->
    datastore_basic_ops_utils:set_hooks(Case, Config);

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Case, Config) when
    Case =:= datastore_mixed_db_test;
    Case =:= datastore_mixed_global_store_test;
    Case =:= datastore_mixed_local_store_test;
    Case =:= datastore_mixed_local_cache_test;
    Case =:= datastore_mixed_global_cache_test ->
    datastore_basic_ops_utils:unset_hooks(Case, Config);

end_per_testcase(_Case, _Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
