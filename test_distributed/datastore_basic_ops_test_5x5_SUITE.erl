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
-module(datastore_basic_ops_test_5x5_SUITE).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include("datastore_basic_ops_utils.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_models_def.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
%%tests
-export([create_delete_db_test/1, save_db_test/1, update_db_test/1, get_db_test/1, exists_db_test/1,
    create_delete_global_store_test/1, no_transactions_create_delete_global_store_test/1,
    save_global_store_test/1, no_transactions_save_global_store_test/1, update_global_store_test/1,
    no_transactions_update_global_store_test/1, get_global_store_test/1, exists_global_store_test/1,
    create_delete_local_store_test/1, save_local_store_test/1, update_local_store_test/1,
    get_local_store_test/1, exists_local_store_test/1,
    create_delete_global_cache_test/1, save_global_cache_test/1, update_global_cache_test/1,
    create_sync_delete_global_cache_test/1, save_sync_global_cache_test/1, update_sync_global_cache_test/1,
    get_global_cache_test/1, exists_global_cache_test/1,
    create_delete_local_cache_test/1, save_local_cache_test/1, update_local_cache_test/1,
    create_sync_delete_local_cache_test/1, save_sync_local_cache_test/1, update_sync_local_cache_test/1,
    get_local_cache_test/1, exists_local_cache_test/1,
    mixed_db_test/1, mixed_global_store_test/1, mixed_local_store_test/1,
    mixed_global_cache_test/1, mixed_local_cache_test/1
]).
%%test_bases
-export([create_delete_db_test_base/1, save_db_test_base/1, update_db_test_base/1,
	get_db_test_base/1, exists_db_test_base/1, create_delete_global_store_test_base/1,
	no_transactions_create_delete_global_store_test_base/1,
	save_global_store_test_base/1, no_transactions_save_global_store_test_base/1,
	update_global_store_test_base/1,
	no_transactions_update_global_store_test_base/1, get_global_store_test_base/1,
	exists_global_store_test_base/1, create_delete_local_store_test_base/1,
	save_local_store_test_base/1, update_local_store_test_base/1,
	get_local_store_test_base/1, exists_local_store_test_base/1,
	create_delete_global_cache_test_base/1, save_global_cache_test_base/1,
	update_global_cache_test_base/1, create_sync_delete_global_cache_test_base/1,
	save_sync_global_cache_test_base/1, update_sync_global_cache_test_base/1,
	get_global_cache_test_base/1, exists_global_cache_test_base/1,
	create_delete_local_cache_test_base/1, save_local_cache_test_base/1,
	update_local_cache_test_base/1, create_sync_delete_local_cache_test_base/1,
	save_sync_local_cache_test_base/1, update_sync_local_cache_test_base/1,
	get_local_cache_test_base/1, exists_local_cache_test_base/1,
	mixed_db_test_base/1, mixed_global_store_test_base/1, mixed_local_store_test_base/1,
	mixed_global_cache_test_base/1, mixed_local_cache_test_base/1
]).

-define(PERFORMANCE_CASES, [
    create_delete_db_test, save_db_test, update_db_test, get_db_test, exists_db_test,
    create_delete_global_store_test, no_transactions_create_delete_global_store_test,
    save_global_store_test, no_transactions_save_global_store_test, update_global_store_test,
    no_transactions_update_global_store_test, get_global_store_test, exists_global_store_test,
    create_delete_local_store_test, save_local_store_test, update_local_store_test,
    get_local_store_test, exists_local_store_test,
    create_delete_global_cache_test, save_global_cache_test, update_global_cache_test,
    create_sync_delete_global_cache_test, save_sync_global_cache_test, update_sync_global_cache_test,
    get_global_cache_test, exists_global_cache_test,
    create_delete_local_cache_test, save_local_cache_test, update_local_cache_test,
    create_sync_delete_local_cache_test, save_sync_local_cache_test, update_sync_local_cache_test,
    get_local_cache_test, exists_local_cache_test,
    mixed_db_test, mixed_global_store_test, mixed_local_store_test,
    mixed_global_cache_test, mixed_local_cache_test
]).

-define(TEST_CASES, [
    create_delete_db_test, save_db_test, update_db_test, get_db_test, exists_db_test,
    create_delete_global_store_test, no_transactions_create_delete_global_store_test,
    save_global_store_test, no_transactions_save_global_store_test, update_global_store_test,
    no_transactions_update_global_store_test, get_global_store_test, exists_global_store_test,
    create_delete_local_store_test, save_local_store_test, update_local_store_test,
    get_local_store_test, exists_local_store_test,
    create_delete_global_cache_test, save_global_cache_test, update_global_cache_test,
    create_sync_delete_global_cache_test, save_sync_global_cache_test, update_sync_global_cache_test,
    get_global_cache_test, exists_global_cache_test,
    create_delete_local_cache_test, save_local_cache_test, update_local_cache_test,
    create_sync_delete_local_cache_test, save_sync_local_cache_test, update_sync_local_cache_test,
    get_local_cache_test, exists_local_cache_test
]).

all() ->
    ?ALL(?TEST_CASES, ?PERFORMANCE_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

create_delete_db_test(Config) ->
	?PERFORMANCE(Config, ?create_delete_test_def).

create_delete_db_test_base(Config) ->
	datastore_basic_ops_utils:create_delete_test(Config, disk_only).

save_db_test(Config) ->
	?PERFORMANCE(Config, ?save_test_def).

save_db_test_base(Config) ->
	datastore_basic_ops_utils:save_test(Config, disk_only).

update_db_test(Config) ->
	?PERFORMANCE(Config, ?update_test_def).

update_db_test_base(Config) ->
	datastore_basic_ops_utils:update_test(Config, disk_only).

get_db_test(Config) ->
	?PERFORMANCE(Config, ?get_test_def).

get_db_test_base(Config) ->
	datastore_basic_ops_utils:get_test(Config, disk_only).

exists_db_test(Config) ->
	?PERFORMANCE(Config, ?exists_test_def).

exists_db_test_base(Config) ->
	datastore_basic_ops_utils:exists_test(Config, disk_only).

%% ====================================================================

create_delete_global_store_test(Config) ->
	?PERFORMANCE(Config, ?create_delete_test_def).

create_delete_global_store_test_base(Config) ->
	datastore_basic_ops_utils:create_delete_test(Config, global_only).

no_transactions_create_delete_global_store_test(Config) ->
	?PERFORMANCE(Config, ?no_transactions_create_delete_test_def).

no_transactions_create_delete_global_store_test_base(Config) ->
	datastore_basic_ops_utils:create_delete_test(Config, global_only).

save_global_store_test(Config) ->
	?PERFORMANCE(Config, ?save_test_def).

save_global_store_test_base(Config) ->
	datastore_basic_ops_utils:save_test(Config, global_only).

no_transactions_save_global_store_test(Config) ->
	?PERFORMANCE(Config, ?no_transactions_save_test_def).

no_transactions_save_global_store_test_base(Config) ->
	datastore_basic_ops_utils:save_test(Config, global_only).

update_global_store_test(Config) ->
	?PERFORMANCE(Config, ?update_test_def).

update_global_store_test_base(Config) ->
	datastore_basic_ops_utils:update_test(Config, global_only).

no_transactions_update_global_store_test(Config) ->
	?PERFORMANCE(Config, ?no_transactions_update_test_def).

no_transactions_update_global_store_test_base(Config) ->
	datastore_basic_ops_utils:update_test(Config, global_only).

get_global_store_test(Config) ->
	?PERFORMANCE(Config, ?get_test_def).

get_global_store_test_base(Config) ->
	datastore_basic_ops_utils:get_test(Config, global_only).

exists_global_store_test(Config) ->
	?PERFORMANCE(Config, ?exists_test_def).

exists_global_store_test_base(Config) ->
	datastore_basic_ops_utils:exists_test(Config, global_only).

%% ====================================================================

create_delete_local_store_test(Config) ->
	?PERFORMANCE(Config, ?create_delete_test_def).

create_delete_local_store_test_base(Config) ->
	datastore_basic_ops_utils:create_delete_test(Config, local_only).

save_local_store_test(Config) ->
	?PERFORMANCE(Config, ?save_test_def).

save_local_store_test_base(Config) ->
	datastore_basic_ops_utils:save_test(Config, local_only).

update_local_store_test(Config) ->
	?PERFORMANCE(Config, ?update_test_def).

update_local_store_test_base(Config) ->
	datastore_basic_ops_utils:update_test(Config, local_only).

get_local_store_test(Config) ->
	?PERFORMANCE(Config, ?get_test_def).

get_local_store_test_base(Config) ->
	datastore_basic_ops_utils:get_test(Config, local_only).

exists_local_store_test(Config) ->
	?PERFORMANCE(Config, ?exists_test_def).

exists_local_store_test_base(Config) ->
	datastore_basic_ops_utils:exists_test(Config, local_only).

%% ====================================================================

create_delete_global_cache_test(Config) ->
	?PERFORMANCE(Config, ?create_delete_test_def).

create_delete_global_cache_test_base(Config) ->
	datastore_basic_ops_utils:create_delete_test(Config, globally_cached).

save_global_cache_test(Config) ->
	?PERFORMANCE(Config, ?save_test_def).

save_global_cache_test_base(Config) ->
	datastore_basic_ops_utils:save_test(Config, globally_cached).

update_global_cache_test(Config) ->
	?PERFORMANCE(Config, ?update_test_def).

update_global_cache_test_base(Config) ->
	datastore_basic_ops_utils:update_test(Config, globally_cached).

create_sync_delete_global_cache_test(Config) ->
	?PERFORMANCE(Config, ?create_sync_delete_test_def).

create_sync_delete_global_cache_test_base(Config) ->
	datastore_basic_ops_utils:create_sync_delete_test(Config, globally_cached).

save_sync_global_cache_test(Config) ->
	?PERFORMANCE(Config, ?save_sync_test_def).

save_sync_global_cache_test_base(Config) ->
	datastore_basic_ops_utils:save_sync_test(Config, globally_cached).

update_sync_global_cache_test(Config) ->
	?PERFORMANCE(Config, ?update_sync_test_def).

update_sync_global_cache_test_base(Config) ->
	datastore_basic_ops_utils:update_sync_test(Config, globally_cached).

get_global_cache_test(Config) ->
	?PERFORMANCE(Config, ?get_test_def).

get_global_cache_test_base(Config) ->
	datastore_basic_ops_utils:get_test(Config, globally_cached).

exists_global_cache_test(Config) ->
	?PERFORMANCE(Config, ?exists_test_def).

exists_global_cache_test_base(Config) ->
	datastore_basic_ops_utils:exists_test(Config, globally_cached).

%% ====================================================================

create_delete_local_cache_test(Config) ->
	?PERFORMANCE(Config, ?create_delete_test_def).

create_delete_local_cache_test_base(Config) ->
	datastore_basic_ops_utils:create_delete_test(Config, locally_cached).

save_local_cache_test(Config) ->
	?PERFORMANCE(Config, ?save_test_def).

save_local_cache_test_base(Config) ->
	datastore_basic_ops_utils:save_test(Config, locally_cached).

update_local_cache_test(Config) ->
	?PERFORMANCE(Config, ?update_test_def).

update_local_cache_test_base(Config) ->
	datastore_basic_ops_utils:update_test(Config, locally_cached).

create_sync_delete_local_cache_test(Config) ->
	?PERFORMANCE(Config, ?create_sync_delete_test_def).

create_sync_delete_local_cache_test_base(Config) ->
	datastore_basic_ops_utils:create_sync_delete_test(Config, locally_cached).

save_sync_local_cache_test(Config) ->
	?PERFORMANCE(Config, ?save_sync_test_def).

save_sync_local_cache_test_base(Config) ->
	datastore_basic_ops_utils:save_sync_test(Config, locally_cached).

update_sync_local_cache_test(Config) ->
	?PERFORMANCE(Config, ?update_sync_test_def).

update_sync_local_cache_test_base(Config) ->
	datastore_basic_ops_utils:update_sync_test(Config, locally_cached).

get_local_cache_test(Config) ->
	?PERFORMANCE(Config, ?get_test_def).

get_local_cache_test_base(Config) ->
	datastore_basic_ops_utils:get_test(Config, locally_cached).

exists_local_cache_test(Config) ->
	?PERFORMANCE(Config, ?exists_test_def).

exists_local_cache_test_base(Config) ->
	datastore_basic_ops_utils:exists_test(Config, locally_cached).

%% ====================================================================

mixed_db_test(Config) ->
	?PERFORMANCE(Config, ?long_test_def).

mixed_db_test_base(Config) ->
	datastore_basic_ops_utils:mixed_test(Config, disk_only).

mixed_global_store_test(Config) ->
	?PERFORMANCE(Config, ?long_test_def).

mixed_global_store_test_base(Config) ->
	datastore_basic_ops_utils:mixed_test(Config, global_only).

mixed_local_store_test(Config) ->
	?PERFORMANCE(Config, ?long_test_def).

mixed_local_store_test_base(Config) ->
	datastore_basic_ops_utils:mixed_test(Config, local_only).

mixed_global_cache_test(Config) ->
	?PERFORMANCE(Config, ?long_test_def).

mixed_global_cache_test_base(Config) ->
	datastore_basic_ops_utils:mixed_test(Config, globally_cached).

mixed_local_cache_test(Config) ->
	?PERFORMANCE(Config, ?long_test_def).

mixed_local_cache_test_base(Config) ->
	datastore_basic_ops_utils:mixed_test(Config, locally_cached).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [random]).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(Case, Config) when
    Case =:= no_transactions_create_delete_global_store_test;
    Case =:= no_transactions_save_global_store_test;
    Case =:= no_transactions_update_global_store_test ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_new(Workers, some_record),
    test_utils:mock_expect(Workers, some_record, model_init, fun() ->
        #model_config{name = some_record,
            size = record_info(size, some_record),
            fields = record_info(fields, some_record),
            defaults = #some_record{},
            bucket = test_bucket,
            hooks = [{some_record, update}],
            store_level = globally_cached, % use of macro results in test errors
            link_store_level = globally_cached,
            transactional_global_cache = false
        }
    end),
    datastore_basic_ops_utils:set_hooks(Case, Config);

init_per_testcase(Case, Config) ->
    datastore_basic_ops_utils:set_hooks(Case, Config).

end_per_testcase(Case, Config) when
    Case =:= no_transactions_create_delete_global_store_test;
    Case =:= no_transactions_save_global_store_test;
    Case =:= no_transactions_update_global_store_test ->
    Workers = ?config(cluster_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, some_record),
    datastore_basic_ops_utils:unset_hooks(Case, Config);

end_per_testcase(Case, Config) ->
    datastore_basic_ops_utils:unset_hooks(Case, Config).