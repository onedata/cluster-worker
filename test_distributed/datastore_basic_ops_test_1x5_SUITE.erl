%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests datastore basic operations at all levels.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_basic_ops_test_1x5_SUITE).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").
-include("datastore_basic_ops_utils.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([create_delete_db_test/1, save_db_test/1, update_db_test/1, get_db_test/1, exists_db_test/1,
    create_delete_global_cache_test/1, save_global_cache_test/1, update_global_cache_test/1,
    get_global_cache_test/1, exists_global_cache_test/1,
    create_delete_local_cache_test/1, save_local_cache_test/1, update_local_cache_test/1,
    get_local_cache_test/1, exists_local_cache_test/1
]).

-performance({test_cases,
    [create_delete_db_test, save_db_test, update_db_test, get_db_test, exists_db_test,
        create_delete_global_cache_test, save_global_cache_test, update_global_cache_test,
        get_global_cache_test, exists_global_cache_test,
        create_delete_local_cache_test, save_local_cache_test, update_local_cache_test,
        get_local_cache_test, exists_local_cache_test
    ]
}).
all() -> [].

%%%===================================================================
%%% Test function
%% ====================================================================

?create_delete_test_def.
create_delete_db_test(Config) ->
    datastore_basic_ops_utils:create_delete_test(Config, disk_only).

?save_test_def.
save_db_test(Config) ->
    datastore_basic_ops_utils:save_test(Config, disk_only).

?update_test_def.
update_db_test(Config) ->
    datastore_basic_ops_utils:update_test(Config, disk_only).

?get_test.
get_db_test(Config) ->
    datastore_basic_ops_utils:get_test(Config, disk_only).

?exists_test.
exists_db_test(Config) ->
    datastore_basic_ops_utils:exists_test(Config, disk_only).

%% ====================================================================

?create_delete_test_def.
create_delete_global_cache_test(Config) ->
    datastore_basic_ops_utils:create_delete_test(Config, globally_cached).

?save_test_def.
save_global_cache_test(Config) ->
    datastore_basic_ops_utils:save_test(Config, globally_cached).

?update_test_def.
update_global_cache_test(Config) ->
    datastore_basic_ops_utils:update_test(Config, globally_cached).

?get_test.
get_global_cache_test(Config) ->
    datastore_basic_ops_utils:get_test(Config, globally_cached).

?exists_test.
exists_global_cache_test(Config) ->
    datastore_basic_ops_utils:exists_test(Config, globally_cached).

%% ====================================================================

?create_delete_test_def.
create_delete_local_cache_test(Config) ->
    datastore_basic_ops_utils:create_delete_test(Config, locally_cached).

?save_test_def.
save_local_cache_test(Config) ->
    datastore_basic_ops_utils:save_test(Config, locally_cached).

?update_test_def.
update_local_cache_test(Config) ->
    datastore_basic_ops_utils:update_test(Config, locally_cached).

?get_test.
get_local_cache_test(Config) ->
    datastore_basic_ops_utils:get_test(Config, locally_cached).

?exists_test.
exists_local_cache_test(Config) ->
    datastore_basic_ops_utils:exists_test(Config, locally_cached).

%% ====================================================================



%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).
