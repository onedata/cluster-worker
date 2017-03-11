%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% This suite contains tests of mechanism of auxiliary cache tables.
%%% @end
%%%-------------------------------------------------------------------
-module(auxiliary_cache_test_SUITE).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include("datastore_test_models_def.hrl").

% TODO - extend test with second worker after refactoring of aux cache in mnesia driver

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
%% tests
-export([local_only_record_with_local_aux_cache_creation_test/1,
    local_only_record_with_local_aux_cache_save_test/1,
    local_only_record_with_local_aux_cache_deletion_test/1,
    local_only_record_with_local_aux_cache_update_test/1,
    local_only_record_with_local_aux_cache_create_or_update_test/1,
    global_only_record_with_local_aux_cache_creation_test/1,
    global_only_record_with_local_aux_cache_save_test/1,
    global_only_record_with_local_aux_cache_deletion_test/1,
    global_only_record_with_local_aux_cache_update_test/1,
    global_only_record_with_local_aux_cache_create_or_update_test/1,
    global_only_record_with_global_aux_cache_dirty_creation_test/1,
    global_only_record_with_global_aux_cache_dirty_save_test/1,
    global_only_record_with_global_aux_cache_dirty_deletion_test/1,
    global_only_record_with_global_aux_cache_dirty_update_test/1,
    global_only_record_with_global_aux_cache_dirty_create_or_update_test/1,
    global_only_record_with_global_aux_cache_transaction_creation_test/1,
    global_only_record_with_global_aux_cache_transaction_save_test/1,
    global_only_record_with_global_aux_cache_transaction_deletion_test/1,
    global_only_record_with_global_aux_cache_transaction_update_test/1,
    global_only_record_with_global_aux_cache_transaction_create_or_update_test/1]).

-define(POSTHOOK_METHODS, [save, delete, update, create, create_or_update]).
-define(FIELD(Number, Id), binary_to_atom(
    <<"field", (integer_to_binary(Number))/binary, "_",
        (str_utils:format_bin("~4..0B", [Id]))/binary>>, utf8)).

-define(TIMEOUT, timer:minutes(5)).
-define(call_datastore(N, F, A), ?call(N, datastore, F, A)).
-define(call(N, M, F, A), rpc:call(N, M, F, A)).
-define(TEST_RECORDS, [
    local_only_record_with_local_aux_cache,
    global_only_record_with_local_aux_cache,
    global_only_record_with_global_aux_cache_dirty,
    global_only_record_with_global_aux_cache_transaction
]).


all() -> ?ALL([
    local_only_record_with_local_aux_cache_creation_test,
    local_only_record_with_local_aux_cache_save_test,
    local_only_record_with_local_aux_cache_deletion_test,
    local_only_record_with_local_aux_cache_update_test,
    local_only_record_with_local_aux_cache_create_or_update_test,
    global_only_record_with_local_aux_cache_creation_test,
    global_only_record_with_local_aux_cache_save_test,
    global_only_record_with_local_aux_cache_deletion_test,
    global_only_record_with_local_aux_cache_update_test,
    global_only_record_with_local_aux_cache_create_or_update_test,
    global_only_record_with_global_aux_cache_dirty_creation_test,
    global_only_record_with_global_aux_cache_dirty_save_test,
    global_only_record_with_global_aux_cache_dirty_deletion_test,
    global_only_record_with_global_aux_cache_dirty_update_test,
    global_only_record_with_global_aux_cache_dirty_create_or_update_test,
    global_only_record_with_global_aux_cache_transaction_creation_test,
    global_only_record_with_global_aux_cache_transaction_save_test,
    global_only_record_with_global_aux_cache_transaction_deletion_test,
    global_only_record_with_global_aux_cache_transaction_update_test,
    global_only_record_with_global_aux_cache_transaction_create_or_update_test
]).



%%%===================================================================
%%% Test functions
%%%===================================================================

local_only_record_with_local_aux_cache_creation_test(Config) ->
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?LOCAL_ONLY_LEVEL,
    TestModel = local_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),

    create_records(Worker1, ShuffledRecords, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords, Level, TestModel, field1).

local_only_record_with_local_aux_cache_save_test(Config) ->
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?LOCAL_ONLY_LEVEL,
    TestModel = local_only_record_with_local_aux_cache,

    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),
    RecordsAndKeys = create_records(Worker1, ShuffledRecords, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords, Level, TestModel, field1),

    OrderedRecords2 = create_test_records(TestModel, 11, 20),
    RecordsAndKeys2 = [{NewR, K} || {{_R, K}, NewR} <- lists:zip(RecordsAndKeys, OrderedRecords2)],
    ShuffledRecordsAndKeys = shuffle(RecordsAndKeys2),
    save_records(Worker1, ShuffledRecordsAndKeys, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1).

local_only_record_with_local_aux_cache_deletion_test(Config) ->
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?LOCAL_ONLY_LEVEL,
    TestModel = local_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),
    RecordsAndKeys = create_records(Worker1, ShuffledRecords, Level),
    SortedKeys = sort_keys(RecordsAndKeys, OrderedRecords),
    timer:sleep(timer:seconds(1)),
    {RecordToDelete, Key} = choose_random_element_from_list(OrderedRecords, SortedKeys),
    OrderedRecords2 = OrderedRecords -- [RecordToDelete],

    delete(Worker1, TestModel, Key, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1).

local_only_record_with_local_aux_cache_update_test(Config) ->
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?LOCAL_ONLY_LEVEL,
    TestModel = local_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),
    RecordsAndKeys = create_records(Worker1, ShuffledRecords, Level),
    SortedKeys = sort_keys(RecordsAndKeys, OrderedRecords),
    timer:sleep(timer:seconds(1)),
    {RecordToUpdate, Key} = choose_random_element_from_list(OrderedRecords, SortedKeys),
    UpdateFun = fun(OldRecord) ->
        {ok, OldRecord#local_only_record_with_local_aux_cache{field1=field1_0020}}
    end,
    {ok, UpdatedRecord} = UpdateFun(RecordToUpdate),
    OrderedRecords2 = (OrderedRecords ++ [UpdatedRecord]) -- [RecordToUpdate],

    update(Worker1, TestModel, Level, Key, UpdateFun),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1).

local_only_record_with_local_aux_cache_create_or_update_test(Config) ->
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?LOCAL_ONLY_LEVEL,
    TestModel = local_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),
    RecordsAndKeys = create_records(Worker1, ShuffledRecords, Level),
    SortedKeys = sort_keys(RecordsAndKeys, OrderedRecords),
    {RecordToUpdate, Key} = choose_random_element_from_list(OrderedRecords, SortedKeys),
    UpdateFun = fun(OldRecord) ->
        {ok, OldRecord#local_only_record_with_local_aux_cache{field1=field1_0020}}
    end,
    {ok, UpdatedRecord} = UpdateFun(RecordToUpdate),
    OrderedRecords2 = (OrderedRecords ++ [UpdatedRecord]) -- [RecordToUpdate],
    create_or_update(Worker1, Key, UpdatedRecord, Level, UpdateFun),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1),
    NewRecord = create_test_record(TestModel, 0),

    create_or_update(Worker1, <<"non_existing_key">>, NewRecord, Level, UpdateFun),
    OrderedRecords3 = [NewRecord | OrderedRecords2],
    check_list_ordered(Worker1, OrderedRecords3, Level, TestModel, field1).

global_only_record_with_local_aux_cache_creation_test(Config) ->
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = global_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),

    create_records(Worker1, ShuffledRecords, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords, Level, TestModel, field1).

global_only_record_with_local_aux_cache_save_test(Config) ->
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = global_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),
    RecordsAndKeys = create_records(Worker1, ShuffledRecords, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords, Level, TestModel, field1),

    OrderedRecords2 = create_test_records(TestModel, 11, 20),
    RecordsAndKeys2 = [{NewR, K} || {{_R, K}, NewR} <- lists:zip(RecordsAndKeys, OrderedRecords2)],

    ShuffledRecordsAndKeys = shuffle(RecordsAndKeys2),
    save_records(Worker1, ShuffledRecordsAndKeys, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1).

global_only_record_with_local_aux_cache_deletion_test(Config) ->
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = global_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),

    RecordsAndKeys = create_records(Worker1, ShuffledRecords, Level),
    SortedKeys = sort_keys(RecordsAndKeys, OrderedRecords),

    timer:sleep(timer:seconds(1)),
    {RecordToDelete, Key} = choose_random_element_from_list(OrderedRecords, SortedKeys),
    OrderedRecords2 = OrderedRecords -- [RecordToDelete],

    delete(Worker1, TestModel, Key, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1).

global_only_record_with_local_aux_cache_update_test(Config) ->
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = global_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),
    RecordsAndKeys = create_records(Worker1, ShuffledRecords, Level),
    SortedKeys = sort_keys(RecordsAndKeys, OrderedRecords),

    {RecordToUpdate, Key} = choose_random_element_from_list(OrderedRecords, SortedKeys),
    UpdateFun = fun(OldRecord) ->
        {ok, OldRecord#global_only_record_with_local_aux_cache{field1=field1_0020}}
    end,
    {ok, UpdatedRecord} = UpdateFun(RecordToUpdate),
    OrderedRecords2 = (OrderedRecords ++ [UpdatedRecord]) -- [RecordToUpdate] ,
    update(Worker1, TestModel, Level, Key, UpdateFun),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1).

global_only_record_with_local_aux_cache_create_or_update_test(Config) ->
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = global_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),
    RecordsAndKeys = create_records(Worker1, ShuffledRecords, Level),
    SortedKeys = sort_keys(RecordsAndKeys, OrderedRecords),
    {RecordToUpdate, Key} = choose_random_element_from_list(OrderedRecords, SortedKeys),
    UpdateFun = fun(OldRecord) ->
        {ok, OldRecord#global_only_record_with_local_aux_cache{field1=field1_0020}}
    end,
    {ok, UpdatedRecord} = UpdateFun(RecordToUpdate),
    OrderedRecords2 = (OrderedRecords ++ [UpdatedRecord]) -- [RecordToUpdate],

    create_or_update(Worker1, Key, UpdatedRecord, Level, UpdateFun),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1),

    NewRecord = create_test_record(TestModel, 0),
    create_or_update(Worker1, <<"non_existing_key">>, NewRecord, Level, UpdateFun),
    OrderedRecords3 = [NewRecord | OrderedRecords2],
    check_list_ordered(Worker1, OrderedRecords3, Level, TestModel, field1).

global_only_record_with_global_aux_cache_dirty_creation_test(Config) ->
    global_only_record_with_global_aux_cache_creation_test(Config, dirty).

global_only_record_with_global_aux_cache_dirty_save_test(Config) ->
    global_only_record_with_global_aux_cache_save_test(Config, dirty).

global_only_record_with_global_aux_cache_dirty_deletion_test(Config) ->
    global_only_record_with_global_aux_cache_deletion_test(Config, dirty).

global_only_record_with_global_aux_cache_dirty_update_test(Config) ->
    global_only_record_with_global_aux_cache_update_test(Config, dirty).

global_only_record_with_global_aux_cache_dirty_create_or_update_test(Config) ->
    global_only_record_with_global_aux_cache_create_or_update_test(Config, dirty).

global_only_record_with_global_aux_cache_transaction_creation_test(Config) ->
    global_only_record_with_global_aux_cache_creation_test(Config, transaction).

global_only_record_with_global_aux_cache_transaction_save_test(Config) ->
    global_only_record_with_global_aux_cache_save_test(Config, transaction).

global_only_record_with_global_aux_cache_transaction_deletion_test(Config) ->
    global_only_record_with_global_aux_cache_deletion_test(Config, transaction).

global_only_record_with_global_aux_cache_transaction_update_test(Config) ->
    global_only_record_with_global_aux_cache_update_test(Config, transaction).

global_only_record_with_global_aux_cache_transaction_create_or_update_test(Config) ->
    global_only_record_with_global_aux_cache_create_or_update_test(Config, transaction).

%%%===================================================================
%%% Base functions
%%%===================================================================

global_only_record_with_global_aux_cache_creation_test(Config, AccessContext) ->
% TODO - restore Worker2
%%    [Worker1, Worker2 | _] = ?config(cluster_worker_nodes, Config),
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = join_atoms([global_only_record_with_global_aux_cache, AccessContext], '_'),
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),

    create_records(Worker1, ShuffledRecords, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords, Level, TestModel, field1).
% TODO - restore Worker2
%%    check_list_ordered(Worker2, OrderedRecords, Level, TestModel, field1).

global_only_record_with_global_aux_cache_save_test(Config, AccessContext) ->
% TODO - restore Worker2
% %%    [Worker1, Worker2| _] = ?config(cluster_worker_nodes, Config),
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = join_atoms([global_only_record_with_global_aux_cache, AccessContext], '_'),
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),
    RecordsAndKeys = create_records(Worker1, ShuffledRecords, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords, Level, TestModel, field1),

    OrderedRecords2 = create_test_records(TestModel, 11, 20),
    RecordsAndKeys2 = [{NewR, K} || {{_R, K}, NewR} <- lists:zip(RecordsAndKeys, OrderedRecords2)],
    ShuffledRecordsAndKeys = shuffle(RecordsAndKeys2),

    save_records(Worker1, ShuffledRecordsAndKeys, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1).
% TODO - restore Worker2
%%    check_list_ordered(Worker2, OrderedRecords2, Level, TestModel, field1).

global_only_record_with_global_aux_cache_deletion_test(Config, AccessContext) ->
% TODO - restore Worker2
%%    [Worker1, Worker2| _] = ?config(cluster_worker_nodes, Config),
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = join_atoms([global_only_record_with_global_aux_cache, AccessContext], '_'),
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),
    RecordsAndKeys = create_records(Worker1, ShuffledRecords, Level),
    SortedKeys = sort_keys(RecordsAndKeys, OrderedRecords),

    timer:sleep(timer:seconds(1)),
    {RecordToDelete, Key} = choose_random_element_from_list(OrderedRecords, SortedKeys),
    OrderedRecords2 = OrderedRecords -- [RecordToDelete],

    delete(Worker1, TestModel, Key, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1).
% TODO - restore Worker2
%%    check_list_ordered(Worker2, OrderedRecords2, Level, TestModel, field1).

global_only_record_with_global_aux_cache_update_test(Config, AccessContext) ->
%%    [Worker1, Worker2| _] = ?config(cluster_worker_nodes, Config),
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = join_atoms([global_only_record_with_global_aux_cache, AccessContext], '_'),
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),
    RecordsAndKeys = create_records(Worker1, ShuffledRecords, Level),
    SortedKeys = sort_keys(RecordsAndKeys, OrderedRecords),
    {RecordToUpdate, Key} = choose_random_element_from_list(OrderedRecords, SortedKeys),
    UpdateFun = fun(OldRecord) ->
        case OldRecord of
            #global_only_record_with_global_aux_cache_dirty{} ->
                {ok, OldRecord#global_only_record_with_global_aux_cache_dirty{field1=field1_0020}};
            #global_only_record_with_global_aux_cache_transaction{} ->
                {ok, OldRecord#global_only_record_with_global_aux_cache_transaction{field1=field1_0020}}
        end
    end,
    {ok, UpdatedRecord} = UpdateFun(RecordToUpdate),
    OrderedRecords2 = (OrderedRecords ++ [UpdatedRecord]) -- [RecordToUpdate] ,
    update(Worker1, TestModel, Level, Key, UpdateFun),
    timer:sleep(timer:seconds(1)),

    update(Worker1, TestModel, Level, Key, UpdateFun),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1).
% TODO - restore Worker2
%%    check_list_ordered(Worker2, OrderedRecords2, Level, TestModel, field1).

global_only_record_with_global_aux_cache_create_or_update_test(Config, AccessContext) ->
% TODO - restore Worker2
%%    [Worker1, Worker2 | _] = ?config(cluster_worker_nodes, Config),
    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = join_atoms([global_only_record_with_global_aux_cache, AccessContext], '_'),
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),
    RecordsAndKeys = create_records(Worker1, ShuffledRecords, Level),
    SortedKeys = sort_keys(RecordsAndKeys, OrderedRecords),
    {RecordToUpdate, Key} = choose_random_element_from_list(OrderedRecords, SortedKeys),
    UpdateFun = fun(OldRecord) ->
        case OldRecord of
            #global_only_record_with_global_aux_cache_dirty{} ->
                {ok, OldRecord#global_only_record_with_global_aux_cache_dirty{field1=field1_0020}};
            #global_only_record_with_global_aux_cache_transaction{} ->
                {ok, OldRecord#global_only_record_with_global_aux_cache_transaction{field1=field1_0020}}
        end
    end,
    {ok, UpdatedRecord} = UpdateFun(RecordToUpdate),
    OrderedRecords2 = (OrderedRecords ++ [UpdatedRecord]) -- [RecordToUpdate],
    create_or_update(Worker1, Key, UpdatedRecord, Level, UpdateFun),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1),
% TODO - restore Worker2
%%    check_list_ordered(Worker2, OrderedRecords2, Level, TestModel, field1),

    NewRecord = create_test_record(TestModel, 0),
    create_or_update(Worker1, <<"non_existing_key">>, NewRecord, Level, UpdateFun),
    OrderedRecords3 = [NewRecord | OrderedRecords2],
    check_list_ordered(Worker1, OrderedRecords3, Level, TestModel, field1).
% TODO - restore Worker2
%%    check_list_ordered(Worker2, OrderedRecords3, Level, TestModel, field1).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [random]} | Config].

init_per_testcase(Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    enable_datastore_models_with_hooks(Workers, [test_to_record(Case)], ?POSTHOOK_METHODS),
    Config.

end_per_testcase(Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    clear_env(Case, Workers).

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_test_record(RecordName, N) ->
    datastore_basic_ops_utils:get_record(RecordName, ?FIELD(1, N), ?FIELD(2, N), ?FIELD(3, N)).

create_test_records(RecordName, Number) ->
    lists:map(fun(N) ->
        create_test_record(RecordName, N)
    end, lists:seq(1, Number)).

create_test_records(RecordName, From, To) ->
    lists:map(fun(N) ->
        datastore_basic_ops_utils:get_record(RecordName, ?FIELD(1, N), ?FIELD(2, N), ?FIELD(3, N))
    end, lists:seq(From, To)).

shuffle(List) ->
    [ X || {_,X} <- lists:sort([ {rand:uniform(), N} || N <- List])].

create_records(Worker, Records, Level) ->
    lists:map(fun(R) ->
        Doc = #document{key=random_key(), value=R},
        {ok, Key} = ?call_datastore(Worker, create, [Level, Doc]),
        {R, Key}
    end, Records).

save_records(Worker, RecordsAndKeys2, Level) ->
    lists:foreach(fun({R, K}) ->
        Doc = #document{key=K, value=R},
        {ok, K} = ?call_datastore(Worker, save, [Level, Doc])
    end, RecordsAndKeys2).

create_or_update(Worker, Key, Record, Level, UpdateFun) ->
    Doc = #document{key=Key, value=Record},
    {ok, Key} = ?call_datastore(Worker, create_or_update, [Level, Doc, UpdateFun]).

update(Worker, TestModel, Level, Key, UpdateFun) ->
    {ok, Key} = ?call_datastore(Worker, update, [Level ,TestModel, Key, UpdateFun]).

delete(Worker, ModelName, Key, Level) ->
    ?call_datastore(Worker, delete, [Level, ModelName, Key]).

random_key() ->
    base64:encode(crypto:strong_rand_bytes(8)).

check_list_ordered(Worker, ExpectedRecords, Level, Model, OrderBy) ->
    ListFun = fun
        ('$end_of_table', AccIn) ->
            {abort, AccIn};
        (#document{key=_Key, value=Record}, AccIn) ->
            {next, AccIn ++ [Record]}
    end,
    Args = [Level, Model, ListFun, OrderBy, []],
    ?assertMatch({ok, ExpectedRecords}, ?call_datastore(Worker, list_ordered, Args), 2).

choose_random_element_from_list(List, KeyList) ->
    Size = length(List),
    Rand = rand:uniform(Size),
    {lists:nth(Rand, List), lists:nth(Rand, KeyList)}.

sort_keys(RecordsAndKeys, OrderedRecords) ->
    lists:map(fun(R) ->
        {R, Key} = lists:keyfind(R, 1, RecordsAndKeys),
        Key
    end, OrderedRecords).


%%--------------------------------------------------------------------
%% @doc
%% Enables given local models in datastore that runs on given nodes.
%% All given nodes should be form one single provider.
%% @end
%%--------------------------------------------------------------------
enable_datastore_models_with_hooks([H | _] = Nodes, Models, HooksMethods) ->
    lists:foreach(
        fun(Model) ->
            {Module, Binary, Filename} = code:get_object_code(Model),
            {_, []} = rpc:multicall(Nodes, code, load_binary, [Module, Filename, Binary])
        end, Models),

    test_utils:mock_unload(Nodes, [plugins]),
        catch test_utils:mock_new(Nodes, [plugins, auxiliary_cache_controller]),
    ok = test_utils:mock_expect(Nodes, plugins, apply,
        fun
            (datastore_config_plugin, models, []) ->
                meck:passthrough([datastore_config_plugin, models, []]) ++ Models;
            (A1, A2, A3) ->
                meck:passthrough([A1, A2, A3])
        end),

    lists:foreach(fun(Model) ->
        HooksConfig = [{Model, Method} || Method <- HooksMethods],
        ok = test_utils:mock_expect(Nodes, auxiliary_cache_controller,
            get_hooks_config, fun() -> HooksConfig end)
    end, Models),


    lists:foreach(
        fun(Node) ->
            ok = rpc:call(Node, gen_server, call,
                [node_manager, {apply, datastore, initialize_state, [H]}],
                timer:seconds(30))
        end, Nodes).


test_to_record(Case) ->
    CaseStr = atom_to_list(Case),
    [Record] = [R || R <- ?TEST_RECORDS, string:str(CaseStr, atom_to_list(R)) > 0 ],
    Record.


clear_env(Case, Workers) ->
    TestModel = test_to_record(Case),
    ModelConfig = TestModel:model_init(),
    lists:foreach(fun(Field) ->
        clear_model_tables(Workers, ModelConfig#model_config.name, Field)
    end, ModelConfig#model_config.fields).


clear_model_tables(Workers, ModelName, Field) ->
    lists:foreach(fun(W) ->
        Aux1 = mnesia_aux_table_name(extend_table_name_with_node(ModelName, W), Field),
        Aux2 = ets_aux_table_name(ModelName, Field),
        Tab1 = mnesia_table_name(extend_table_name_with_node(ModelName, W)),
        Tab2 = ets_table_name(ModelName),

        rpc:call(W, mnesia, activity, [async_dirty, fun() -> mnesia:clear_table(Aux1)end]),
        rpc:call(W, mnesia, activity, [async_dirty, fun() -> mnesia:clear_table(Tab1)end]),
        rpc:call(W, ets, delete_all_objects, [Aux2]),
        rpc:call(W, ets, delete_all_objects, [Tab2])
    end, Workers).

mnesia_aux_table_name(ModelName, Field) ->
    binary_to_atom(<<(atom_to_binary(mnesia_table_name(ModelName), utf8))/binary, "_",
        (atom_to_binary(Field, utf8))/binary>>, utf8).

ets_aux_table_name(ModelName, Field) ->
    binary_to_atom(<<(atom_to_binary(ets_table_name(ModelName), utf8))/binary, "_",
        (atom_to_binary(Field, utf8))/binary>>, utf8).

mnesia_table_name(TabName) ->
    binary_to_atom(<<"dc_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).

ets_table_name(TabName) ->
    binary_to_atom(<<"lc_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).

extend_table_name_with_node(TabName, _Worker) when TabName =:= lock ->
    TabName;
extend_table_name_with_node(TabName, Worker) ->
    list_to_atom(atom_to_list(TabName) ++ atom_to_list(Worker)).

join_atoms(Atoms, Sep) ->
    Bins = [atom_to_binary(A, latin1) || A <- Atoms],
    binary_to_atom(str_utils:join_binary(Bins, atom_to_binary(Sep, latin1)), latin1).
