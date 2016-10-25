%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% WRITEME
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



%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
%% tests
-export([local_only_record_with_local_aux_cache_creation_test/1,
    global_only_record_with_local_aux_cache_creation_test/1,
    global_only_record_with_global_aux_cache_creation_test/1,
    local_only_record_with_local_aux_cache_deletion_test/1,
    global_only_record_with_local_aux_cache_deletion_test/1,
    global_only_record_with_global_aux_cache_deletion_test/1,
    local_only_record_with_local_aux_cache_save_test/1]).

-define(POSTHOOK_METHODS, [save, delete, update, create, create_or_update]).

all() -> ?ALL([
    local_only_record_with_local_aux_cache_creation_test,
    local_only_record_with_local_aux_cache_save_test,
    local_only_record_with_local_aux_cache_deletion_test,
    global_only_record_with_local_aux_cache_creation_test,
    global_only_record_with_local_aux_cache_deletion_test,
    global_only_record_with_global_aux_cache_creation_test,
    global_only_record_with_global_aux_cache_deletion_test
]).

-define(FIELD(Number, Id), binary_to_atom(
    <<"field", (integer_to_binary(Number))/binary, "_",
        (str_utils:format_bin("~4..0B", [Id]))/binary>>, utf8)).

-define(TIMEOUT, timer:minutes(5)).
-define(call_datastore(N, F, A), ?call(N, datastore, F, A)).
-define(call(N, M, F, A), rpc:call(N, M, F, A)).


%%%===================================================================
%%% Test functions
%%%===================================================================

local_only_record_with_local_aux_cache_creation_test(Config) ->

    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?LOCAL_ONLY_LEVEL,
    TestModel = local_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),

    create(Worker1, ShuffledRecords, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords, Level, TestModel, field1).

local_only_record_with_local_aux_cache_deletion_test(Config) ->

    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?LOCAL_ONLY_LEVEL,
    TestModel = local_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),

    RecordsAndKeys = create(Worker1, ShuffledRecords, Level),
    SortedKeys = sort_keys(RecordsAndKeys, OrderedRecords),

    timer:sleep(timer:seconds(1)),
    {RecordToDelete, Key} = choose_random_element_from_list(OrderedRecords, SortedKeys),
    OrderedRecords2 = OrderedRecords -- [RecordToDelete],

    delete(Worker1, TestModel, Key, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1).

local_only_record_with_local_aux_cache_save_test(Config) ->

    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?LOCAL_ONLY_LEVEL,
    TestModel = local_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),
    RecordsAndKeys = create(Worker1, ShuffledRecords, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords, Level, TestModel, field1),

    OrderedRecords2 = create_test_records(TestModel, 11, 20),
    RecordsAndKeys2 = [{NewR, K} || {{_R, K}, NewR} <- lists:zip(RecordsAndKeys, OrderedRecords2)],

    ShuffledRecordsAndKeys = shuffle(RecordsAndKeys2),

    ct:pal("shuffled: ~p", [ShuffledRecordsAndKeys]),

    save(Worker1, ShuffledRecordsAndKeys, Level),
    timer:sleep(timer:seconds(5)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1).




global_only_record_with_local_aux_cache_creation_test(Config) ->

    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = global_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),

    create(Worker1, ShuffledRecords, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords, Level, TestModel, field1).

global_only_record_with_local_aux_cache_deletion_test(Config) ->

    [Worker1 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = global_only_record_with_local_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),

    RecordsAndKeys = create(Worker1, ShuffledRecords, Level),
    SortedKeys = sort_keys(RecordsAndKeys, OrderedRecords),

    timer:sleep(timer:seconds(1)),
    {RecordToDelete, Key} = choose_random_element_from_list(OrderedRecords, SortedKeys),
    OrderedRecords2 = OrderedRecords -- [RecordToDelete],

    delete(Worker1, TestModel, Key, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1).

global_only_record_with_global_aux_cache_creation_test(Config) ->

    [Worker1, Worker2 | _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = global_only_record_with_global_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),

    create(Worker1, ShuffledRecords, Level),
    timer:sleep(timer:seconds(1)),
    check_list_ordered(Worker1, OrderedRecords, Level, TestModel, field1),
    check_list_ordered(Worker2, OrderedRecords, Level, TestModel, field1).

global_only_record_with_global_aux_cache_deletion_test(Config) ->

    [Worker1, Worker2| _] = ?config(cluster_worker_nodes, Config),
    Level = ?GLOBAL_ONLY_LEVEL,
    TestModel = global_only_record_with_global_aux_cache,
    OrderedRecords = create_test_records(TestModel, 10),
    ShuffledRecords = shuffle(OrderedRecords),

    RecordsAndKeys = create(Worker1, ShuffledRecords, Level),
    SortedKeys = sort_keys(RecordsAndKeys, OrderedRecords),

    timer:sleep(timer:seconds(1)),
    {RecordToDelete, Key} = choose_random_element_from_list(OrderedRecords, SortedKeys),
    OrderedRecords2 = OrderedRecords -- [RecordToDelete],

    tracer:start(Worker1),
    tracer:trace_calls(mnesia_cache_driver, aux_delete),
    delete(Worker1, TestModel, Key, Level),
    timer:sleep(timer:seconds(10)),
    check_list_ordered(Worker1, OrderedRecords2, Level, TestModel, field1),
    check_list_ordered(Worker2, OrderedRecords2, Level, TestModel, field1).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    NewConfig = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [random]),
    NewConfig.

end_per_suite(Config) ->
%%    ok.
    test_node_starter:clean_environment(Config).


init_per_testcase(Case, Config) ->
    Workers = ?config(cluster_worker_nodes, Config),
    enable_datastore_models_with_hooks(Workers, [test_to_record(Case)], ?POSTHOOK_METHODS),
    Config.

end_per_testcase(Case, Config) ->
%%    ok.
    Workers = ?config(cluster_worker_nodes, Config),
    clear_env(Case, Workers).

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_test_records(RecordName, Number) ->
    lists:map(fun(N) ->
        datastore_basic_ops_utils:get_record(RecordName, ?FIELD(1, N), ?FIELD(2, N), ?FIELD(3, N))
    end, lists:seq(1, Number)).

create_test_records(RecordName, From, To) ->
    lists:map(fun(N) ->
        datastore_basic_ops_utils:get_record(RecordName, ?FIELD(1, N), ?FIELD(2, N), ?FIELD(3, N))
    end, lists:seq(From, To)).

shuffle(List) ->
    [ X || {_,X} <- lists:sort([ {rand:uniform(), N} || N <- List])].

create(Worker, Records, Level) ->
    lists:map(fun(R) ->
        Doc = #document{key=random_key(), value=R},
        {ok, Key} = ?call_datastore(Worker, create, [Level, Doc]),
        {R, Key}
    end, Records).

save(Worker, RecordsAndKeys2, Level) ->
    lists:foreach(fun({R, K}) ->
        Doc = #document{key=K, value=R},
        {ok, K} = ?call_datastore(Worker, save, [Level, Doc])
    end, RecordsAndKeys2).


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
    {ok, ListedRecords} = ?call_datastore(Worker, list_ordered, Args),
    ct:pal("~p", [ExpectedRecords]),
    ct:pal("~p", [ListedRecords]),

    ?assertMatch(ExpectedRecords, ListedRecords).

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
            ok = rpc:call(Node, gen_server, call, [node_manager, {apply, datastore, initialize_state, [H]}], timer:seconds(30))
        end, Nodes).


test_to_record(local_only_record_with_local_aux_cache_creation_test) ->
    local_only_record_with_local_aux_cache;
test_to_record(local_only_record_with_local_aux_cache_save_test) ->
    local_only_record_with_local_aux_cache;
test_to_record(local_only_record_with_local_aux_cache_deletion_test) ->
    local_only_record_with_local_aux_cache;
test_to_record(global_only_record_with_local_aux_cache_creation_test) ->
    global_only_record_with_local_aux_cache;
test_to_record(global_only_record_with_local_aux_cache_deletion_test) ->
    global_only_record_with_local_aux_cache;
test_to_record(global_only_record_with_global_aux_cache_creation_test) ->
    global_only_record_with_global_aux_cache;
test_to_record(global_only_record_with_global_aux_cache_deletion_test) ->
    global_only_record_with_global_aux_cache.


clear_env(Case, Workers) ->
    TestModel = test_to_record(Case),
    ModelConfig = TestModel:model_init(),
    lists:foreach(fun(Field) ->
        clear_model_tables(Workers, ModelConfig#model_config.name, Field)
    end, ModelConfig#model_config.fields).


clear_model_tables(Workers, ModelName, Field) ->
    Aux1 = mnesia_aux_table_name(ModelName, Field),
    Aux2 = ets_aux_table_name(ModelName, Field),
    Tab1 = mnesia_table_name(ModelName),
    Tab2 = ets_table_name(ModelName),
    lists:foreach(fun(W) ->
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



%% TODO:
%% TODO: * local-local
%% TODO: * global-local
%% TODO: * global-global
%% TODO: * test for all posthooks
%% TODO: * cleaning
%% TODO: * bound test case with model
