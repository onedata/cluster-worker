%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests couchdb changes using test_record_1 and test_record_2.
%%% @end
%%%-------------------------------------------------------------------
-module(couchdb_changes_test_SUITE).
-author("Mateusz Paciorek").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include("datastore_test_models_def.hrl").

-define(getFirstSeq(W, Config),
    begin
        {_, LastSeqInDb, _} = ?assertMatch(
            {ok, _, _},
            rpc:call(W, couchdb_datastore_driver, db_run,
                [couchbeam_changes, follow_once, [], 3])
        ),
        binary_to_integer(LastSeqInDb)
    end).

-define(TIMEOUT, timer:seconds(30)).

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([
    record_saving_test/1,
    revision_numbering_test/1,
    multiple_records_saving_test/1,
    force_save_test/1,
    force_save_gc_test/1,
    force_save_gc_link_test/1,
    finite_stream_test/1,
    record_deletion_test/1,
    delete_conflict_test/1,
    delete_force_save_test/1,
    delete_double_conflict_test/1,
    force_save_after_delete_test/1]).

-performance({test_cases, []}).
all() ->
    ?ALL([
        % TODO - simmilar tests
%%        record_deletion_test,
%%        record_saving_test,
%%        revision_numbering_test,
%%        multiple_records_saving_test,
%%        force_save_test,
%%        force_save_gc_test,
%%        force_save_gc_link_test,
%%        finite_stream_test,
%%        delete_conflict_test,
%%        delete_force_save_test,
%%        delete_double_conflict_test,
%%        force_save_after_delete_test
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

%% Test saving records of different models
record_saving_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),

    Doc1Key = <<"doc1_key">>,
    Doc1Val = #test_record_1{field1 = 1, field2 = 2, field3 = 3},
    Doc1 = #document{key = Doc1Key, value = Doc1Val},
    ?assertEqual({ok, Doc1Key}, rpc:call(W, test_record_1, save, [Doc1])),
    {_, {_, DocR1, ModR1}} = ?assertReceivedMatch({record_saving_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1, value = ValR1} = DocR1,
    ?assertEqual({Doc1Key, Doc1Val, test_record_1}, {KeyR1, ValR1, ModR1}),

    Doc2Key = <<"doc2_key">>,
    Doc2Val = #test_record_2{field1 = 4, field2 = 5, field3 = 6},
    Doc2 = #document{key = Doc2Key, value = Doc2Val},
    ?assertEqual({ok, Doc2Key}, rpc:call(W, test_record_2, save, [Doc2])),
    {_, {_, DocR2, ModR2}} = ?assertReceivedMatch({record_saving_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR2, value = ValR2} = DocR2,
    ?assertEqual({Doc2Key, Doc2Val, test_record_2}, {KeyR2, ValR2, ModR2}),

    ok.

%% Test contents of 'deleted' field
record_deletion_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),

    %% given
    Doc1Key = <<"doc1_key">>,
    Doc1Val = #test_record_1{field1 = 1, field2 = 2, field3 = 3},
    Doc1 = #document{key = Doc1Key, value = Doc1Val},
    ?assertEqual({ok, Doc1Key}, rpc:call(W, test_record_1, save, [Doc1])),
    {_, {_, DocR1, ModR1}} = ?assertReceivedMatch({record_deletion_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1, value = ValR1, deleted = DeletedR1} = DocR1,
    ?assertEqual(
        {false, Doc1Key, Doc1Val, test_record_1},
        {DeletedR1, KeyR1, ValR1, ModR1}
    ),

    %% when
    ?assertEqual(ok, rpc:call(W, test_record_1, delete, [Doc1Key])),

    %% then
    {_, {_, DocR2, ModR2}} = ?assertReceivedMatch({record_deletion_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR2, value = ValR2, deleted = DeletedR2} = DocR2,
    ?assertEqual(
        {true, Doc1Key, Doc1Val, test_record_1},
        {DeletedR2, KeyR2, ValR2, ModR2}
    ).

%% Test incrementing number of subsequent revisions
revision_numbering_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),

    Key = <<"key">>,
    Revs = lists:map(
        fun(N) ->
            Val = #test_record_1{field1 = N, field2 = N, field3 = N},
            Doc = #document{key = Key, value = Val},
            ?assertEqual({ok, Key}, rpc:call(W, test_record_1, save, [Doc])),
            {_, {_, DocR, ModR}} = ?assertReceivedMatch({revision_numbering_test,
                {_, #document{}, _}}, ?TIMEOUT),
            #document{key = KeyR, rev = RevR, value = ValR} = DocR,
            ?assertEqual({Key, Val, test_record_1}, {KeyR, ValR, ModR}),
            RevR
        end,
        lists:seq(1, 20)
    ),
    ?assertEqual(Revs, lists:sort(Revs)),
    ok.

%% Test saving multiple records in short time
multiple_records_saving_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),

    Docs = lists:map(
        fun(Key) ->
            Mod = case Key rem 2 of
                0 -> test_record_1;
                1 -> test_record_2
            end,
            Vals = lists:map(
                fun(N) ->
                    Val = case Mod of
                        test_record_1 ->
                            #test_record_1{field1 = N, field2 = N, field3 = N};
                        test_record_2 ->
                            #test_record_2{field1 = N, field2 = N, field3 = N}
                    end,
                    Doc = #document{key = Key, value = Val},
                    ?assertEqual({ok, Key}, rpc:call(W, Mod, save, [Doc])),
                    Val
                end,
                lists:seq(1, 10)
            ),
            LastVal = lists:last(Vals),
            {Key, LastVal, Mod}
        end,
        lists:seq(1, 10)
    ),

    lists:map(
        fun({Key, Val, Mod}) ->
            ?assertReceivedMatch({multiple_records_saving_test,
                {_, #document{key = Key, value = Val}, Mod}}, ?TIMEOUT)
        end,
        Docs
    ),
    ok.

%% Test overwriting specific revision
force_save_test(Config) ->
    [W1, W2] = ?config(cluster_worker_nodes, Config),

    Key = <<"key_fst">>,
    Docs = lists:map(
        fun(N) ->
            Val = #test_record_1{field1 = N, field2 = N, field3 = N},
            Doc = #document{key = Key, value = Val},
            ?assertEqual({ok, Key}, rpc:call(W1, test_record_1, save, [Doc])),
            {_, {_, DocR, _}} = ?assertReceivedMatch({force_save_test,
                {_, #document{key = Key, value = Val}, test_record_1}}, ?TIMEOUT),
            DocR
        end,
        lists:seq(1, 10)
    ),
    ModelConfig = test_record_1:model_init(),

    lists:map(
        fun(Doc) ->
            ?assertMatch(
                {{ok, Key}, Doc},
                rpc:call(W2, couchdb_datastore_driver, force_save,
                    [ModelConfig, Doc])
            )
        end,
        Docs
    ),

    [#document{value = Check} | _] = lists:reverse(Docs),
    ?assertMatch({{ok, #document{value = Check}}, {ok, #document{value = Check}}},
        {rpc:call(W1, couchdb_datastore_driver, get, [ModelConfig, Key]),
        rpc:call(W2, couchdb_datastore_driver, get, [ModelConfig, Key])}),

    ?assertEqual(
        rpc:call(W1, couchdb_datastore_driver, get, [ModelConfig, Key]),
        rpc:call(W2, couchdb_datastore_driver, get, [ModelConfig, Key])
    ),

    ok.

force_save_gc_test(Config) ->
    [W1, W2] = ?config(cluster_worker_nodes, Config),

    Key = <<"key_gc">>,
    Docs = lists:map(
        fun(N) ->
            Val = #globally_cached_record{field1 = N, field2 = N, field3 = N},
            Doc = #document{key = Key, value = Val},
            ?assertEqual({ok, Key}, rpc:call(W1, globally_cached_record, save, [Doc])),
            {_, {_, DocR, _}} = ?assertReceivedMatch({force_save_gc_test,
                {_, #document{key = Key, value = Val}, globally_cached_record}}, ?TIMEOUT),
            DocR
        end,
        lists:seq(1, 10)
    ),
    ModelConfig = globally_cached_record:model_init(),

    lists:map(
        fun(Doc) ->
            ?assertEqual(
                ok,
                rpc:call(W2, model, execute_with_default_context,
                    [ModelConfig, save, [Doc], [{resolve_conflicts, doc}]])
            )
        end,
        Docs
    ),

    [#document{value = Check} | _] = lists:reverse(Docs),
    ?assertMatch({{ok, #document{value = Check}}, {ok, #document{value = Check}}},
        {rpc:call(W1, ?MEMORY_DRIVER, get, [ModelConfig, Key]),
        rpc:call(W2, ?MEMORY_DRIVER, get, [ModelConfig, Key])}),

    ?assertEqual(
        rpc:call(W1, ?MEMORY_DRIVER, get, [ModelConfig, Key]),
        rpc:call(W2, ?MEMORY_DRIVER, get, [ModelConfig, Key])
    ),
    ok.

force_save_gc_link_test(Config) ->
    [W1, W2] = ?config(cluster_worker_nodes, Config),

    Key = <<"key_l">>,
    ModelConfig = globally_cached_record:model_init(),
    Docs = lists:map(
        fun(N) ->
            ?assertEqual(ok, rpc:call(W1, model, execute_with_default_context,
                [ModelConfig, add_links, [Key, {N, {Key, globally_cached_record}}],
                    [{level, ?DISK_ONLY_LEVEL}]])),
            {_, {_, DocR, _}} = ?assertReceivedMatch({force_save_gc_link_test,
                {_, #document{}, globally_cached_record}}, ?TIMEOUT),
            DocR
        end,
        lists:seq(1, 10)
    ),

    lists:map(
        fun(Doc) ->
            ?assertEqual(
                ok,
                rpc:call(W2, model, execute_with_default_context,
                    [ModelConfig, save, [Doc], [{resolve_conflicts, {links, Key}}]])
            )
        end,
        Docs
    ),

    [#document{key = CheckKey, value = Check} | _] = lists:reverse(Docs),
    ?assertMatch({ok, #document{value = Check}},
        rpc:call(W2, ?GLOBAL_SLAVE_DRIVER, get_link_doc, [ModelConfig, CheckKey])),

    ok.

%% Test stream with finite until value
finite_stream_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),
    Pid = self(),

    BaseVal = #test_record_1{field1 = 1, field2 = 2, field3 = 3},
    BaseMod = test_record_1,

    save_docs(W, BaseVal, BaseMod, 1, 10),
    receive_all(finite_stream_test, []),

    save_docs(W, BaseVal, BaseMod, 11, 20),
    ReceivedFromInfinite = receive_all(finite_stream_test, []),
    Since = element(1, lists:nth(1, ReceivedFromInfinite)) - 1,
    Until = element(1, lists:last(ReceivedFromInfinite)),

    save_docs(W, BaseVal, BaseMod, 21, 30),
    receive_all(finite_stream_test, []),

    {_, DriverPid} = ?assertMatch(
        {ok, _},
        rpc:call(W, couchdb_datastore_driver, changes_start_link,
            [
                fun(Seq, Doc, Mod) ->
                    Pid ! {finite, {Seq, Doc, Mod}}
                end,
                Since,
                Until
            ]
        )
    ),
    ReceivedFromFinite = receive_all(finite, []),
    ?assertEqual(ReceivedFromInfinite, ReceivedFromFinite),

    ?assertEqual(ok, rpc:call(W, gen_changes, stop, [DriverPid])),
    ok.

delete_force_save_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),

    %% given
    Doc1Key = <<"dfst_key">>,

    Doc1Val = #test_record_1{field1 = 1, field2 = 2, field3 = 3},
    Doc1 = #document{key = Doc1Key, value = Doc1Val},
    ?assertEqual({ok, Doc1Key}, rpc:call(W, test_record_1, save, [Doc1])),
    {_, {_, DocR1, ModR1}} = ?assertReceivedMatch({delete_force_save_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1, value = ValR1, deleted = DeletedR1} = DocR1,
    ?assertEqual(
        {false, Doc1Key, Doc1Val, test_record_1},
        {DeletedR1, KeyR1, ValR1, ModR1}
    ),

    Doc1Val2 = #test_record_1{field1 = 2, field2 = 2, field3 = 3},
    Doc1_2 = #document{key = Doc1Key, value = Doc1Val2},
    ?assertEqual({ok, Doc1Key}, rpc:call(W, test_record_1, save, [Doc1_2])),
    {_, {_, DocR1_2, ModR1_2}} = ?assertReceivedMatch({delete_force_save_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1_2, value = ValR1_2, deleted = DeletedR1_2, rev = Rev} = DocR1_2,
    ?assertEqual(
        {false, Doc1Key, Doc1Val2, test_record_1},
        {DeletedR1_2, KeyR1_2, ValR1_2, ModR1_2}
    ),

    {RNum, [H1, _H2] = H} = Rev,
    RevCheck = <<"2-", H1/binary>>,
    ?assertMatch({ok, #document{rev = RevCheck}}, rpc:call(W, test_record_1, get, [Doc1Key])),

    Doc1_3 = #document{key = Doc1Key, value = Doc1Val2, rev = {RNum + 1, [<<"0">> | H]}, deleted = true},
    ?assertMatch({{ok, Doc1Key}, #document{}},
        rpc:call(W, couchdb_datastore_driver, force_save, [test_record_1:model_init(), Doc1_3])),

    %% then
    ?assertMatch({error, {not_found, _}}, rpc:call(W, test_record_1, get, [Doc1Key])),
    {_, {_, DocR2, ModR2}} = ?assertReceivedMatch({delete_force_save_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR2, value = ValR2, deleted = DeletedR2} = DocR2,
    ?assertEqual(
        {true, Doc1Key, Doc1Val2, test_record_1},
        {DeletedR2, KeyR2, ValR2, ModR2}
    ).

delete_conflict_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),

    %% given
    Doc1Key = <<"dct_key">>,

    Doc1Val = #test_record_1{field1 = 1, field2 = 2, field3 = 3},
    Doc1 = #document{key = Doc1Key, value = Doc1Val},
    ?assertEqual({ok, Doc1Key}, rpc:call(W, test_record_1, save, [Doc1])),
    {_, {_, DocR1, ModR1}} = ?assertReceivedMatch({delete_conflict_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1, value = ValR1, deleted = DeletedR1} = DocR1,
    ?assertEqual(
        {false, Doc1Key, Doc1Val, test_record_1},
        {DeletedR1, KeyR1, ValR1, ModR1}
    ),

    Doc1Val2 = #test_record_1{field1 = 2, field2 = 2, field3 = 3},
    Doc1_2 = #document{key = Doc1Key, value = Doc1Val2},
    ?assertEqual({ok, Doc1Key}, rpc:call(W, test_record_1, save, [Doc1_2])),
    {_, {_, DocR1_2, ModR1_2}} = ?assertReceivedMatch({delete_conflict_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1_2, value = ValR1_2, deleted = DeletedR1_2, rev = Rev} = DocR1_2,
    ?assertEqual(
        {false, Doc1Key, Doc1Val2, test_record_1},
        {DeletedR1_2, KeyR1_2, ValR1_2, ModR1_2}
    ),

    {RNum, [H1, H2]} = Rev,
    RevCheck = <<"2-", H1/binary>>,
    ?assertMatch({ok, #document{rev = RevCheck}}, rpc:call(W, test_record_1, get, [Doc1Key])),

    Doc1Val3 = #test_record_1{field1 = 3, field2 = 2, field3 = 3},
    Doc1_3 = #document{key = Doc1Key, value = Doc1Val3, rev = {RNum, [<<"0">>, H2]}},
    ?assertEqual({{ok, Doc1Key}, not_changed}, rpc:call(W, couchdb_datastore_driver, force_save, [test_record_1:model_init(), Doc1_3])),

    ?assertEqual(timeout, receive
                              {delete_conflict_test,
                                  {_, #document{}, _}} = __Result__ -> __Result__
                          after
                              ?TIMEOUT ->
                                  timeout
                          end),

    ?assertMatch({ok, #document{rev = RevCheck}}, rpc:call(W, test_record_1, get, [Doc1Key])),

    Doc1Val4 = #test_record_1{field1 = 4, field2 = 2, field3 = 3},
    Doc1_4 = #document{key = Doc1Key, value = Doc1Val4, rev = {RNum, [<<"z">>, H2]}},
    ?assertMatch({{ok, Doc1Key}, #document{}}, rpc:call(W, couchdb_datastore_driver, force_save, [test_record_1:model_init(), Doc1_4])),

    {_, {_, DocR1_4, ModR1_4}} = ?assertReceivedMatch({delete_conflict_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1_4, value = ValR1_4, deleted = DeletedR1_4, rev = Rev4} = DocR1_4,
    ?assertEqual(
        {false, Doc1Key, Doc1Val4, test_record_1},
        {DeletedR1_4, KeyR1_4, ValR1_4, ModR1_4}
    ),

    {_, [H4 | _]} = Rev4,
    RevCheck4 = <<"2-", H4/binary>>,

    ?assertMatch({ok, #document{rev = RevCheck4}}, rpc:call(W, test_record_1, get, [Doc1Key])),

    %% when
    ?assertEqual(ok, rpc:call(W, test_record_1, delete, [Doc1Key])),

    %% then
    ?assertMatch({error, {not_found, _}}, rpc:call(W, test_record_1, get, [Doc1Key])),
    {_, {_, DocR2, ModR2}} = ?assertReceivedMatch({delete_conflict_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR2, value = ValR2, deleted = DeletedR2} = DocR2,
    ?assertEqual(
        {true, Doc1Key, test_record_1},
        {DeletedR2, KeyR2, ModR2}
    ).

delete_double_conflict_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),

    %% given
    Doc1Key = <<"ddct_key">>,

    Doc1Val = #test_record_1{field1 = 1, field2 = 2, field3 = 3},
    Doc1 = #document{key = Doc1Key, value = Doc1Val},
    ?assertEqual({ok, Doc1Key}, rpc:call(W, test_record_1, save, [Doc1])),
    {_, {_, DocR1, ModR1}} = ?assertReceivedMatch({delete_double_conflict_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1, value = ValR1, deleted = DeletedR1} = DocR1,
    ?assertEqual(
        {false, Doc1Key, Doc1Val, test_record_1},
        {DeletedR1, KeyR1, ValR1, ModR1}
    ),

    Doc1Val2 = #test_record_1{field1 = 2, field2 = 2, field3 = 3},
    Doc1_2 = #document{key = Doc1Key, value = Doc1Val2},
    ?assertEqual({ok, Doc1Key}, rpc:call(W, test_record_1, save, [Doc1_2])),
    {_, {_, DocR1_2, ModR1_2}} = ?assertReceivedMatch({delete_double_conflict_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1_2, value = ValR1_2, deleted = DeletedR1_2} = DocR1_2,
    ?assertEqual(
        {false, Doc1Key, Doc1Val2, test_record_1},
        {DeletedR1_2, KeyR1_2, ValR1_2, ModR1_2}
    ),

    Doc1Val22 = #test_record_1{field1 = 2, field2 = 2, field3 = 32},
    Doc1_22 = #document{key = Doc1Key, value = Doc1Val22},
    ?assertEqual({ok, Doc1Key}, rpc:call(W, test_record_1, save, [Doc1_22])),
    {_, {_, DocR1_22, ModR1_22}} = ?assertReceivedMatch({delete_double_conflict_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1_22, value = ValR1_22, deleted = DeletedR1_22, rev = Rev} = DocR1_22,
    ?assertEqual(
        {false, Doc1Key, Doc1Val22, test_record_1},
        {DeletedR1_22, KeyR1_22, ValR1_22, ModR1_22}
    ),

    {RNum, [H1, _H2, H3]} = Rev,
    RevCheck = <<"3-", H1/binary>>,
    ?assertMatch({ok, #document{rev = RevCheck}}, rpc:call(W, test_record_1, get, [Doc1Key])),

    Doc1Val3 = #test_record_1{field1 = 3, field2 = 2, field3 = 3},
    Doc1_3 = #document{key = Doc1Key, value = Doc1Val3, rev = {RNum, [<<"001">>, <<"002">>, H3]}},
    ?assertEqual({{ok, Doc1Key}, not_changed}, rpc:call(W, couchdb_datastore_driver, force_save, [test_record_1:model_init(), Doc1_3])),

    ?assertEqual(timeout, receive
                              {delete_double_conflict_test,
                                  {_, #document{}, _}} = __Result__ -> __Result__
                          after
                              ?TIMEOUT ->
                                  timeout
                          end),

    Doc1_33 = #document{key = Doc1Key, value = Doc1Val3, rev = {RNum, [<<"001">>, <<"zzz">>, H3]}},
    ?assertEqual({{ok, Doc1Key}, not_changed}, rpc:call(W, couchdb_datastore_driver, force_save, [test_record_1:model_init(), Doc1_33])),

    ?assertEqual(timeout, receive
                              {delete_double_conflict_test,
                                  {_, #document{}, _}} = __Result__ -> __Result__
                          after
                              ?TIMEOUT ->
                                  timeout
                          end),

    ?assertMatch({ok, #document{rev = RevCheck}}, rpc:call(W, test_record_1, get, [Doc1Key])),

    Doc1Val4 = #test_record_1{field1 = 4, field2 = 2, field3 = 3},
    Doc1_4 = #document{key = Doc1Key, value = Doc1Val4, rev = {RNum, [<<"zz1">>, <<"zz2">>, H3]}},
    ?assertMatch({{ok, Doc1Key}, #document{}}, rpc:call(W, couchdb_datastore_driver, force_save, [test_record_1:model_init(), Doc1_4])),

    {_, {_, DocR1_4, ModR1_4}} = ?assertReceivedMatch({delete_double_conflict_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1_4, value = ValR1_4, deleted = DeletedR1_4, rev = Rev4} = DocR1_4,
    ?assertEqual(
        {false, Doc1Key, Doc1Val4, test_record_1},
        {DeletedR1_4, KeyR1_4, ValR1_4, ModR1_4}
    ),

    {_, [H4 | _]} = Rev4,
    RevCheck4 = <<"3-", H4/binary>>,

    ?assertMatch({ok, #document{rev = RevCheck4}}, rpc:call(W, test_record_1, get, [Doc1Key])),

    %% when
    ?assertEqual(ok, rpc:call(W, test_record_1, delete, [Doc1Key])),

    %% then
    ?assertMatch({error, {not_found, _}}, rpc:call(W, test_record_1, get, [Doc1Key])),
    {_, {_, DocR2, ModR2}} = ?assertReceivedMatch({delete_double_conflict_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR2, value = ValR2, deleted = DeletedR2} = DocR2,

    ?assertEqual(
        {true, Doc1Key, Doc1Val4, test_record_1},
        {DeletedR2, KeyR2, ValR2, ModR2}
    ).

force_save_after_delete_test(Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),

    %% given
    Doc1Key = <<"fsadt_key">>,

    Doc1Val = #test_record_1{field1 = 1, field2 = 2, field3 = 3},
    Doc1 = #document{key = Doc1Key, value = Doc1Val},
    ?assertEqual({ok, Doc1Key}, rpc:call(W, test_record_1, save, [Doc1])),
    {_, {_, DocR1, ModR1}} = ?assertReceivedMatch({force_save_after_delete_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1, value = ValR1, deleted = DeletedR1} = DocR1,
    ?assertEqual(
        {false, Doc1Key, Doc1Val, test_record_1},
        {DeletedR1, KeyR1, ValR1, ModR1}
    ),

    Doc1Val2 = #test_record_1{field1 = 2, field2 = 2, field3 = 3},
    Doc1_2 = #document{key = Doc1Key, value = Doc1Val2},
    ?assertEqual({ok, Doc1Key}, rpc:call(W, test_record_1, save, [Doc1_2])),
    {_, {_, DocR1_2, ModR1_2}} = ?assertReceivedMatch({force_save_after_delete_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1_2, value = ValR1_2, deleted = DeletedR1_2} = DocR1_2,
    ?assertEqual(
        {false, Doc1Key, Doc1Val2, test_record_1},
        {DeletedR1_2, KeyR1_2, ValR1_2, ModR1_2}
    ),

    ?assertEqual(ok, rpc:call(W, test_record_1, delete, [Doc1Key])),
    {_, {_, DocR1_3, ModR1_3}} = ?assertReceivedMatch({force_save_after_delete_test,
        {_, #document{}, _}}, ?TIMEOUT),
    #document{key = KeyR1_3, value = ValR1_3, deleted = DeletedR1_3, rev = Rev} = DocR1_3,
    ?assertEqual(
        {true, Doc1Key, Doc1Val2, test_record_1},
        {DeletedR1_3, KeyR1_3, ValR1_3, ModR1_3}
    ),

    {RNum, [_H1, _H2, H3]} = Rev,

    Doc1Val3 = #test_record_1{field1 = 3, field2 = 2, field3 = 3},
    Doc1_3 = #document{key = Doc1Key, value = Doc1Val3, rev = {RNum - 1, [<<"z">>, H3]}, deleted = false},
    ?assertEqual({{ok, Doc1Key}, not_changed}, rpc:call(W, couchdb_datastore_driver, force_save, [test_record_1:model_init(), Doc1_3])),

    %% then
    ?assertMatch({error, {not_found, _}}, rpc:call(W, test_record_1, get, [Doc1Key])),
    ?assertEqual(timeout, receive
        {force_save_after_delete_test,
            {_, #document{}, _}} = __Result__ -> __Result__
    after
        ?TIMEOUT ->
            timeout
    end).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================



init_per_testcase(CaseName, Config) ->
    [W | _] = Workers = ?config(cluster_worker_nodes, Config),
    [P1, P2] = ?config(cluster_worker_nodes, Config),
    Models = [test_record_1, test_record_2, globally_cached_record],

    timer:sleep(3000), % tmp solution until mocking is repaired (VFS-1851)
    test_utils:enable_datastore_models([P1], Models),
    test_utils:enable_datastore_models([P2], Models),

    FirstSeq = ?getFirstSeq(W, Config),
    Pid = self(),
    {_, DriverPid} = ?assertMatch(
        {ok, _},
        rpc:call(W, couchdb_datastore_driver, changes_start_link,
            [
                fun(Seq, Doc, Mod) ->
                    Pid ! {CaseName, {Seq, Doc, Mod}}
                end,
                FirstSeq,
                infinity
            ]
        )
    ),

    lists:foreach(fun(W) ->
        ?assertEqual(ok, test_utils:set_env(W, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(3)))
    end, Workers),

    [{driver_pid, DriverPid} | Config].

end_per_testcase(_, Config) ->
    [W | _] = ?config(cluster_worker_nodes, Config),
    DriverPid = ?config(driver_pid, Config),
    ?assertEqual(ok, rpc:call(W, gen_changes, stop, [DriverPid])),
    flush(),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Saves docs for given value, model and range of keys
save_docs(Worker, Value, Model, FirstKey, LastKey) ->
    lists:map(
        fun(Key) ->
            Doc = #document{key = Key, value = Value},
            ?assertEqual({ok, Key}, rpc:call(Worker, Model, save, [Doc])),
            {Key, Value, Model}
        end,
        lists:seq(FirstKey, LastKey)
    ).

%% Receives all messages with given prefix
receive_all(Prefix, Received) ->
    receive
        {Prefix, {_, stream_ended, _}} ->
            lists:usort(Received);
        {Prefix, Data} ->
            receive_all(Prefix, [Data | Received])
    after
        timer:seconds(5) ->
            lists:usort(Received)
    end.

%% Cleans mailbox
flush() ->
    receive
        _ ->
            flush()
    after
        0 ->
            ok
    end.