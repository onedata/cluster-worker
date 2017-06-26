%%%--------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for memory_store_driver module.
%%% @end
%%%--------------------------------------------------------------------
-module(memory_store_driver_test).

-ifdef(TEST).

-include("global_definitions.hrl").
-include("modules/datastore/memory_store_driver.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_doc.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test functions
%%%===================================================================

merge_link_ops_test() ->
    Key = k,

    Pred1 = fun() -> true end,
    Pred2 = fun() -> true end,

    Msg = {add_links, [Key, [l1, l2]]},
    Msg2 = {add_links, [Key, [l4, l5]]},
    Msg3 = {delete_links, [Key, [l2, l4], Pred1]},
    Msg4 = {delete_links, [Key, [l2, l6], Pred1]},
    Msg5 = {delete_links, [Key, [l7, l6], Pred2]},
    Msg6 = {create_link, [Key, l1]},
    Msg7 = {create_link, [Key, l5]},
    Msg8 = {add_links, [Key, [l4, l7]]},

    ?assertEqual({merged, {add_links, [Key, [l1, l2, l4, l5]]}},
        memory_store_driver_links:merge_link_ops(Msg, Msg2)),
    ?assertEqual(different, memory_store_driver_links:merge_link_ops(Msg, Msg3)),
    ?assertEqual({merged, {delete_links, [Key, [l2, l4, l6], Pred1]}},
        memory_store_driver_links:merge_link_ops(Msg3, Msg4)),
    ?assertEqual(different, memory_store_driver_links:merge_link_ops(Msg, Msg5)),
    ?assertEqual(different, memory_store_driver_links:merge_link_ops(Msg6, Msg7)),

    Msgs = [{x, Msg}, {x, Msg2}, {x, Msg3}, {x, Msg4}, {x, Msg5}, {x, Msg6},
        {x, Msg7}, {x, Msg8}, {y, Msg}, {y, Msg8}],
    Ans = [{{x, {add_links, [Key, [l1, l2, l4, l5]]}}, 2},
        {{x, {delete_links, [Key, [l2, l4, l6], Pred1]}}, 2},
        {{x, Msg5}, 1}, {{x, Msg6}, 1}, {{x, Msg7}, 1}, {{x, Msg8}, 1},
        {{y, {add_links, [Key, [l1, l2, l4, l7]]}}, 2}],

    ?assertEqual(Ans, memory_store_driver_links:merge_link_ops(Msgs)).

opts_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            {"single operations on doc", fun single_op/0},
            {"many operations on doc", fun many_op/0},
            {"single link operations", fun single_op_link/0},
            {"many link operations", fun many_op_link/0},
            {"flush doc", fun flush_doc/0},
            {"flush links", fun flush_links/0}
        ]
    }.

single_op() ->
    Key = k,
    Link = false,
    application:set_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, 5000),

    Ans0 = memory_store_driver:init([Key, Link, false]),
    ?assertMatch({ok, #datastore_doc_init{min_commit_delay = infinity,
        max_commit_delay = infinity}}, Ans0),
    Ans1 = memory_store_driver:init([Key, Link, true]),
    ?assertMatch({ok, #datastore_doc_init{}}, Ans1),
    {ok, #datastore_doc_init{data = State}} = Ans1,

    Ctx = #{get_method => fetch, resolve_conflicts => false, model_name => mn,
        mutator_pid => self()},
    CheckState = State#state{last_ctx = Ctx},
    set_response({ok, #document{key = Key, value = "g"}}),
    Msg = {save, [#document{key = Key, value = "v"}]},
    Ans2 = memory_store_driver:modify([{Ctx, Msg}], State, []),
    ?assertMatch({[{ok, Key}], {true, [{Key, Ctx}]}, CheckState}, Ans2),
    ?assertMatch(#document{value = "v", rev = [<<"1-rev">>]}, get_doc()),

    set_response({ok, #document{key = Key, value = "v"}}),
    Msg1 = {save, [#document{key = Key, value = "error"}]},
    Ans2_2 = memory_store_driver:modify([{Ctx, Msg1}], State, []),
    ?assertMatch({[{datastore_cache_error, {error, error}}], false, State}, Ans2_2),
    ?assertMatch(#document{value = "v", rev = [<<"1-rev">>]}, get_doc()),

    set_response(),
    Msg2 = {update, [Key, diff]},
    Ans3 = memory_store_driver:modify([{Ctx, Msg2}], State, []),
    ?assertMatch({[{ok, Key}], {true, [{Key, Ctx}]}, CheckState}, Ans3),
    ?assertMatch(#document{value = "vu", rev = [<<"1-rev">>]}, get_doc()),

    set_response({ok, #document{key = Key, value = "vu"}}),
    Msg3 = {create_or_update, [#document{key = Key, value = "v"}, fun() -> true end]},
    Ans4 = memory_store_driver:modify([{Ctx, Msg3}], State, []),
    ?assertMatch({[{ok, Key}], {true, [{Key, Ctx}]}, CheckState}, Ans4),
    ?assertMatch(#document{value = "vuu", rev = [<<"1-rev">>]}, get_doc()),

    set_response({ok, #document{key = Key, value = "vuu"}}),
    Msg4 = {create, [#document{key = Key, value = "v"}]},
    Ans5 = memory_store_driver:modify([{Ctx, Msg4}], State, []),
    ?assertMatch({[{error, already_exists}], false, State}, Ans5),
    ?assertMatch(#document{value = "vuu", rev = [<<"1-rev">>]}, get_doc()),

    set_response(),
    Msg5 = {delete, [Key, fun() -> true end]},
    Ans6 = memory_store_driver:modify([{Ctx, Msg5}], State, []),
    ?assertMatch({[ok], {true, [{Key, Ctx}]}, CheckState}, Ans6),
    ?assertMatch(#document{value = "vuu", rev = [<<"1-rev">>], deleted = true},
        get_doc()),

    set_response({ok, #document{key = Key, value = "vuu", deleted = true}}),
    Msg6 = {create, [#document{key = Key, value = "v2"}]},
    Ans7 = memory_store_driver:modify([{Ctx, Msg6}], State, []),
    ?assertMatch({[{ok, Key}], {true, [{Key, Ctx}]}, CheckState}, Ans7),
    ?assertMatch(#document{value = "v2", rev = [<<"1-rev">>]}, get_doc()),

    set_response({ok, #document{key = Key, value = "v2"}}),
    Msg7 = {delete, [Key, fun() -> true end]},
    Ans8 = memory_store_driver:modify([{Ctx, Msg7}], State, []),
    ?assertMatch({[ok], {true, [{Key, Ctx}]}, CheckState}, Ans8),
    ?assertMatch(#document{value = "v2", rev = [<<"1-rev">>], deleted = true},
        get_doc()),

    set_response({ok, #document{key = Key, value = "v2", deleted = true}}),
    Msg8 = {delete, [Key, fun() -> true end]},
    Ans9 = memory_store_driver:modify([{Ctx, Msg8}], State, []),
    ?assertMatch({[ok], false, State}, Ans9),
    ?assertMatch(#document{value = "v2", rev = [<<"1-rev">>], deleted = true},
        get_doc()),

    set_response(),
    Msg9 = {update, [Key, diff]},
    Ans10 = memory_store_driver:modify([{Ctx, Msg9}], State, []),
    ?assertMatch({[{error, {not_found, _}}], false, State}, Ans10),
    ?assertMatch(#document{value = "v2", rev = [<<"1-rev">>], deleted = true},
        get_doc()),

    set_response(),
    Msg10 = {create_or_update, [#document{key = Key, value = "v3"}, fun() -> true end]},
    Ans11 = memory_store_driver:modify([{Ctx, Msg10}], State, []),
    ?assertMatch({[{ok, Key}], {true, [{Key, Ctx}]}, CheckState}, Ans11),
    ?assertMatch(#document{value = "v3", rev = [<<"1-rev">>]}, get_doc()),

    ?assertEqual(ok, memory_store_driver:terminate(State, [])),

    erase(),
    set_response({error, key_enoent}),
    Msg11 = {update, [Key, diff]},
    Ans12 = memory_store_driver:modify([{Ctx, Msg11}], State, []),
    ?assertMatch({[{error, {not_found, _}}], false, State}, Ans12),
    ?assertMatch(undefined, get_doc()),

    set_response(),
    Msg13 = {create, [#document{key = Key, value = "v4"}]},
    Ans14 = memory_store_driver:modify([{Ctx, Msg13}], State, []),
    ?assertMatch({[{ok, Key}], {true, [{Key, Ctx}]}, CheckState}, Ans14),
    ?assertMatch(#document{value = "v4", rev = [<<"1-rev">>]}, get_doc()),

    set_response(),
    Msg14 = {create_or_update, [#document{key = Key, value = "v5"}, fun() -> true end]},
    Ans15 = memory_store_driver:modify([{Ctx, Msg14}], State, []),
    ?assertMatch({[{ok, Key}], {true, [{Key, Ctx}]}, CheckState}, Ans15),
    ?assertMatch(#document{value = "v5", rev = [<<"1-rev">>]}, get_doc()).


many_op() ->
    Key = k,
    Link = false,
    application:set_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, 5000),

    Ans1 = memory_store_driver:init([Key, Link, true]),
    ?assertMatch({ok, #datastore_doc_init{}}, Ans1),
    {ok, #datastore_doc_init{data = State}} = Ans1,

    Ctx = #{get_method => fetch, resolve_conflicts => false, model_name => mn,
        mutator_pid => self()},
    Ctx2 = #{get_method => fetch, resolve_conflicts => false, model_name => mn2,
        mutator_pid => self()},
    Ctx3 = #{get_method => fetch, resolve_conflicts => true, model_name => mn,
        mutator_pid => self()},
    set_response({ok, #document{key = Key, value = "g"}}),

    Msg = {save, [#document{key = Key, value = "v"}]},
    Msg1 = {save, [#document{key = Key, value = "error"}]},
    Msg2 = {update, [Key, diff]},
    Msg3 = {create_or_update, [#document{key = Key, value = "v"}, fun() -> true end]},
    Msg4 = {create, [#document{key = Key, value = "v"}]},
    Msg5 = {delete, [Key, fun() -> true end]},
    Msg6 = {create, [#document{key = Key, value = "v2"}]},
    Msg7 = {delete, [Key, fun() -> true end]},
    Msg8 = {delete, [Key, fun() -> true end]},
    Msg9 = {update, [Key, diff]},
    Msg10 = {create_or_update, [#document{key = Key, value = "v3"}, fun() -> true end]},

    Msgs = [{Ctx, Msg}, {Ctx, Msg1}, {Ctx, Msg2}, {Ctx, Msg3}, {Ctx, Msg4},
        {Ctx, Msg5}, {Ctx, Msg6}, {Ctx, Msg7}, {Ctx, Msg8}, {Ctx, Msg9},
        {Ctx, Msg10}],

    CheckState = State#state{last_ctx = Ctx},
    CheckState2 = State#state{last_ctx = Ctx2},

    Ans2 = memory_store_driver:modify(Msgs, State, []),
    ?assertMatch({[{ok, Key}, {ok, Key}, {ok, Key}, {ok, Key}, {error, already_exists}, ok,
        {ok, Key}, ok, ok, {error, {not_found, _}}, {ok, Key}], {true, [{Key, Ctx}]}, CheckState}, Ans2),
    ?assertMatch(#document{value = "v3", rev = [<<"1-rev">>]}, get_doc()),

    set_response(),
    Msg10_2 = {save, [#document{key = Key, value = "v2"}]},
    Msgs2 = [{Ctx, Msg}, {Ctx, Msg}, {Ctx2, Msg10_2}],
    Ans3 = memory_store_driver:modify(Msgs2, State, []),
    ?assertMatch({[{ok, Key}, {ok, Key}, {ok, Key}], {true, [{Key, Ctx2}]}, CheckState2}, Ans3),
    ?assertMatch(#document{value = "v2", rev = [<<"1-rev">>, <<"1-rev">>]}, get_doc()),
    ?assertMatch([{#document{value = "v2", rev = [<<"1-rev">>, <<"1-rev">>]}, Ctx2},
        {#document{value = "v", rev = [<<"1-rev">>]}, Ctx}], get_docs_with_ctx()),

    set_response(),
    Msg11 = {save, [#document{key = Key, value = "v2", rev = [<<"0-rev">>]}]},
    Msgs3 = [{Ctx, Msg}, {Ctx, Msg}, {Ctx3, Msg11}],
    Ans4 = memory_store_driver:modify(Msgs3, State, []),
    ?assertMatch({[{ok, Key}, {ok, Key}, ok], {true, [{Key, Ctx}]}, CheckState}, Ans4),
    ?assertMatch(#document{value = "v", rev = [<<"1-rev">>]}, get_doc()),
    ?assertMatch([{#document{value = "v", rev = [<<"1-rev">>]}, Ctx}],
        get_docs_with_ctx()),

    ?assertEqual(ok, memory_store_driver:terminate(State, [])).

single_op_link() ->
    Key = k,
    Link = true,
    application:set_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, 5000),

    Ans0 = memory_store_driver:init([Key, Link, false]),
    ?assertMatch({ok, #datastore_doc_init{
        min_commit_delay = infinity,
        max_commit_delay = infinity
    }}, Ans0),
    Ans1 = memory_store_driver:init([Key, Link, true]),
    ?assertMatch({ok, #datastore_doc_init{}}, Ans1),
    {ok, #datastore_doc_init{data = State}} = Ans1,

    Ctx = #{get_method => fetch, resolve_conflicts => false,
        mutator_pid => self()},
    CheckState = State#state{last_ctx = Ctx},

    erase(),
    set_response(),
    V = [l1, l2],
    Msg = {add_links, [Key, V]},
    Ans2 = memory_store_driver:modify([{Ctx, Msg}], State, []),
    Check = #document{key = {x, V}, value = V, rev = [<<"1-rev">>]},
    ?assertMatch({[ok], {true, [{{x, V}, Ctx}]}, CheckState}, Ans2),
    ?assertMatch([Check], get_docs()),

    set_response(),
    V2 = [l4, l5],
    Msg2 = {add_links, [Key, V2]},
    Ans3 = memory_store_driver:modify([{Ctx, Msg2}], State, []),
    Check2 = #document{key = {x, V2}, value = V2, rev = [<<"1-rev">>]},
    ?assertMatch({[ok], {true, [{{x, V2}, Ctx}]}, CheckState}, Ans3),
    ?assertEqual([Check2, Check], get_docs()),

    set_response(),
    V3 = [l2, l4],
    Msg3 = {delete_links, [Key, V3, fun() -> true end]},
    Ans4 = memory_store_driver:modify([{Ctx, Msg3}], State, []),
    Check3 = #document{key = {y, V3}, value = V3, rev = [<<"1-rev">>],
        deleted = true},
    ?assertMatch({[ok], {true, [{{y, V3}, Ctx}]}, CheckState}, Ans4),
    ?assertEqual([Check3, Check2, Check], get_docs()),

    set_response(),
    Msg4 = {add_links, [Key, [error]]},
    Ans5 = memory_store_driver:modify([{Ctx, Msg4}], State, []),
    ?assertMatch({[{error, error}], false, State}, Ans5),
    ?assertEqual([Check3, Check2, Check], get_docs()),

    set_response(),
    V5 = [l4, l6],
    Msg5 = {add_links, [Key, V5]},
    Ans6 = memory_store_driver:modify([{Ctx, Msg5}], State, []),
    Check6 = #document{key = {x, V5}, value = V5, rev = [<<"1-rev">>]},
    ?assertMatch({[ok], {true, [{{x, V5}, Ctx}]}, CheckState}, Ans6),
    ?assertEqual([Check6, Check3, Check2, Check], get_docs()),

    set_response(),
    Ans7 = memory_store_driver:modify([{Ctx, Msg}], State, []),
    ?assertMatch({[ok], {true, [{{x, V}, Ctx}]}, CheckState}, Ans7),
    ?assertEqual([Check, Check6, Check3, Check2], get_docs()),

    set_response(),
    V7 = [l1, error],
    Msg7 = {add_links, [Key, V7]},
    Ans8 = memory_store_driver:modify([{Ctx, Msg7}], State, []),
    ?assertMatch({[{datastore_cache_error, {error, error2}}], false, State}, Ans8),
    ?assertEqual([Check, Check6, Check3, Check2], get_docs()),

    ?assertEqual(ok, memory_store_driver:terminate(State, [])).

many_op_link() ->
    Key = k,
    Link = true,
    application:set_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, 5000),

    Ans1 = memory_store_driver:init([Key, Link, true]),
    ?assertMatch({ok, #datastore_doc_init{}}, Ans1),
    {ok, #datastore_doc_init{data = State}} = Ans1,

    Ctx = #{get_method => fetch, resolve_conflicts => false,
        mutator_pid => self()},
    Ctx2 = #{get_method => fetch, resolve_conflicts => true, model_name => mn,
        mutator_pid => self()},
    Ctx3 = #{get_method => fetch, resolve_conflicts => true, model_name => mn2,
        mutator_pid => self()},
    erase(),
    set_response(),

    Msg = {add_links, [Key, [l1, l2]]},
    Msg2 = {add_links, [Key, [l4, l5]]},
    Msg3 = {delete_links, [Key, [l2, l4], fun() -> true end]},
    Msg4 = {add_links, [Key, [error]]},
    Msg5 = {add_links, [Key, [l4, l6]]},
    Msg6 = {delete_links, [Key, [l2, l6], fun() -> true end]},
    Msg7 = {add_links, [Key, [l1, l7]]},

    Msgs = [{Ctx, Msg}, {Ctx, Msg2}, {Ctx, Msg3}, {Ctx, Msg4}, {Ctx, Msg5},
        {Ctx, Msg6}, {Ctx, Msg7}, {Ctx, Msg6}, {Ctx, Msg7}],

    CheckState = State#state{last_ctx = Ctx},
    CheckState3 = State#state{last_ctx = Ctx3},

    Ans2 = memory_store_driver:modify(Msgs, State, []),
    K1 = [l1,l2,l4,l5],
    K2 = [l2, l4],
    K3 = [l2,l6],
    K4 = [l1,l7],
    Check = [{{x, K1}, Ctx}, {{y, K2}, Ctx}, {{y, K3}, Ctx}, {{x, K4}, Ctx}],
    ?assertMatch({[ok, ok, ok, {error, error}, {error, error}, ok, ok, ok, ok],
        {true, Check}, CheckState}, Ans2),
    CheckV = [#document{key = {x, K1}, value = K1, rev = [<<"1-rev">>]},
        #document{key = {y, K2}, value = K2, rev = [<<"1-rev">>], deleted = true},
        #document{key = {y, K3}, value = K3, rev = [<<"1-rev">>], deleted = true},
        #document{key = {x, K4}, value = K4, rev = [<<"1-rev">>]}],
    ?assertEqual(CheckV, get_docs()),

    set_response(),
    Msg8 = {add_links, [Key, [l1, error]]},
    Msgs2 = [{Ctx, Msg}, {Ctx, Msg2}, {Ctx, Msg3}, {Ctx, Msg4}, {Ctx, Msg5},
        {Ctx, Msg6}, {Ctx, Msg7}, {Ctx, Msg6}, {Ctx, Msg8}],

    Ans3 = memory_store_driver:modify(Msgs2, State, []),
    CheckList = lists:duplicate(9, {datastore_cache_error, {error, error2}}),
    ?assertMatch({CheckList, false, State}, Ans3),
% TODO - check
%%    ?assertEqual([], get_docs()),

    set_response(),
    Msgs3 = [{Ctx, Msg8}, {Ctx, Msg6}, {Ctx, Msg}, {Ctx, Msg2}, {Ctx, Msg3},
        {Ctx, Msg4}, {Ctx, Msg5}, {Ctx, Msg6}, {Ctx, Msg7}],

    Ans4 = memory_store_driver:modify(Msgs3, State, []),
    ?assertMatch({CheckList, {true, Check}, CheckState}, Ans4),
% TODO - check
%%    ?assertEqual(CheckV, get_docs()),

    erase(),
    set_response({ok, #document{key = Key, deleted = true}}),
    Msg9 = {save, [#document{key = Key, value = "v", rev = [<<"1-rev">>]}]},
    Msg10 = {save, [#document{key = Key, value = "v", rev = [<<"2-rev">>]}]},
    Msgs4 = [{Ctx2, Msg9}, {Ctx2, Msg9}, {Ctx3, Msg10}],
    Ans5 = memory_store_driver:modify(Msgs4, State, []),
    ?assertMatch({[ok, ok, ok], {true, [{Key, Ctx3}]}, CheckState3}, Ans5),
    ?assertMatch(#document{value = "v", rev = [<<"2-rev">>]}, get_doc()),
    ?assertMatch([{#document{value = "v", rev = [<<"2-rev">>]}, Ctx3},
        {#document{value = "v", rev = [<<"1-rev">>]}, Ctx2}], get_docs_with_ctx()),

    ?assertEqual(ok, memory_store_driver:terminate(State, [])).

flush_doc() ->
    Key = k,
    Link = false,
    Ctx = #{get_method => fetch, resolve_conflicts => false, model_name => mn,
        mutator_pid => self()},
    set_response({error, key_enoent}),
    put(get_flush_response, {error, {not_found, mc}}),

    Ans1 = memory_store_driver:init([Key, Link, true]),
    ?assertMatch({ok, #datastore_doc_init{min_commit_delay = 5000,
        max_commit_delay = 10000}}, Ans1),
    {ok, #datastore_doc_init{data = State}} = Ans1,

    CheckState = State#state{last_ctx = Ctx},

    Msg = {update, [Key, diff]},
    Ans2 = memory_store_driver:modify([{Ctx, Msg}], State, []),
    ?assertMatch({[{error, {not_found, _}}], false, State}, Ans2),
    ?assertEqual(undefined, get_doc()),

    set_response({error, key_enoent}),
    Msg2 = {create, [#document{key = Key, value = "v4"}]},
    Ans3 = memory_store_driver:modify([{Ctx, Msg2}], State, []),
    ?assertMatch({[{ok, Key}], {true, [{Key, Ctx}]}, CheckState}, Ans3),
    ?assertEqual(#document{key = Key, value = "v4", rev = [<<"1-rev">>]},
        get_doc()),

    set_response({error, key_enoent}),
    Msg3 = {create_or_update, [#document{key = Key, value = "v5"}, fun() -> true end]},
    Ans4 = memory_store_driver:modify([{Ctx, Msg3}], State, []),
    ?assertMatch({[{ok, Key}], {true, [{Key, Ctx}]}, CheckState}, Ans4),
    ?assertEqual(#document{key = Key, value = "v5", rev = [<<"1-rev">>]},
        get_doc()),

    put(get_flush_response, {ok, #document{key = Key, value = "fg"}}),
    set_response({ok, #document{key = Key, value = "fg"}}),

    Ans5 = memory_store_driver:modify([{Ctx, Msg}], State, []),
    ?assertMatch({[{ok, Key}], {true, [{Key, Ctx}]}, CheckState}, Ans5),
    {_, {true, Changes}, _} = Ans5,
    ?assertEqual(#document{key = Key, value = "fgu", rev = [<<"1-rev">>]},
        get_doc()),

    set_response({ok, #document{key = Key, value = "fgu"}}),
    Ans6 = memory_store_driver:modify([{Ctx, Msg3}], State, []),
    ?assertMatch({[{ok, Key}], {true, [{Key, Ctx}]}, CheckState}, Ans6),
    {_, {true, Changes2}, CheckState} = Ans6,
    ?assertEqual(#document{key = Key, value = "fguu", rev = [<<"1-rev">>]},
        get_doc()),

    ?assertMatch(Changes2, memory_store_driver:merge_changes(Changes, Changes2)),

    ?assertEqual({true, []}, memory_store_driver:commit(Changes2, State)),
    ?assertEqual([Key], get_flush()),
    set_response({error, error}),
    set_get_flush_response(Key, {error, error}),
    ?assertMatch({{false, Changes2}, []}, memory_store_driver:commit(Changes2,
        State)),
    ?assertEqual([], get_flush()),

    set_response({ok, #document{key = Key, value = "fgu"}}),
    set_get_flush_response(Key, ok),
    Msg7 = {delete, [Key, fun() -> true end]},
    Ans8 = memory_store_driver:modify([{Ctx, Msg7}], State, []),
    ?assertMatch({[ok], {true, [{Key, Ctx}]}, CheckState}, Ans8),
    {_, {true, Changes4}, CheckState} = Ans8,
    ?assertEqual(#document{key = Key, value = "fgu", rev = [<<"1-rev">>],
        deleted = true}, get_doc()),

    ?assertEqual({true, []}, memory_store_driver:commit(Changes4, State)),
    ?assertEqual([Key], get_flush()),

    ?assertEqual(ok, memory_store_driver:terminate(State, [])).

flush_links() ->
    Key = k,
    Link = true,
    Ctx = #{get_method => fetch, resolve_conflicts => false, model_name => mn,
        mutator_pid => self()},
    set_response({error, key_enoent}),
    put(get_links, true),

    Ans1 = memory_store_driver:init([Key, Link, true]),
    ?assertMatch({ok, #datastore_doc_init{min_commit_delay = 5000,
        max_commit_delay = 10000}}, Ans1),
    {ok, #datastore_doc_init{data = State}} = Ans1,

    CheckState = State#state{last_ctx = Ctx},

    V = [l1, l2],
    Msg = {add_links, [Key, V]},
    Ans2 = memory_store_driver:modify([{Ctx, Msg}], State, []),
    Check = #document{key = {x, V}, value = V, rev = [<<"1-rev">>]},
    ?assertMatch({[ok], {true, [{{x, V}, Ctx}]}, CheckState}, Ans2),
    ?assertEqual([Check], get_docs()),

    set_response({ok, #document{value = [ggg]}}),
    put(get_links, true),

    V2 = [l1, l2, ggg],
    Ans3 = memory_store_driver:modify([{Ctx, Msg}], State, []),
    Check2 = #document{key = {x, V}, value = V2, rev = [<<"1-rev">>]},
    ?assertMatch({[ok], {true, [{{x, V}, Ctx}]}, CheckState}, Ans3),
    ?assertEqual([Check2], get_docs()),

    set_response({ok, #document{value = [ggg2]}}),
    put(get_links, true),

    V4 = [l1, l2, ggg2],
    Ans5 = memory_store_driver:modify([{Ctx, Msg}], State, []),
    Check4 = #document{key = {x, V}, value = V4, rev = [<<"1-rev">>]},
    ?assertMatch({[ok], {true, [{{x, V}, Ctx}]}, CheckState}, Ans5),
    {_, {true, Changes}, CheckState} = Ans5,
    ?assertEqual([Check4], get_docs()),

    set_response({ok, #document{value = [l1, l2, ggg2]}}),
    put(get_links, true),

    V5 = [l1, l2, l1, l2, ggg2],
    Ans6 = memory_store_driver:modify([{Ctx, Msg}], State, []),
    Check5 = #document{key = {x, V}, value = V5, rev = [<<"1-rev">>]},
    ?assertMatch({[ok], {true, [{{x, V}, Ctx}]}, CheckState}, Ans6),
    {_, {true, Changes2}, CheckState} = Ans6,
    ?assertEqual([Check5], get_docs()),

    ?assertMatch(Changes2, memory_store_driver:merge_changes(Changes, Changes2)),

    set_response({error, key_enoent}),
    put(get_links, true),

    V6 = [l3, l4],
    Msg6 = {add_links, [Key, V6]},
    Ans7 = memory_store_driver:modify([{Ctx, Msg6}], State, []),
    Check6 = #document{key = {x, V6}, value = V6, rev = [<<"1-rev">>]},
    ?assertMatch({[ok], {true, [{{x, V6}, Ctx}]}, CheckState}, Ans7),
    {_, {true, Changes3}, CheckState} = Ans7,
    ?assertEqual([Check6, Check5], get_docs()),

    ?assertMatch([{{x, V6}, Ctx}, {{x, V}, Ctx}],
        memory_store_driver:merge_changes(Changes2, Changes3)),
    MChanges = memory_store_driver:merge_changes(Changes2, Changes3),

    set_response({error, key_enoent}),
    put(get_links, true),

    V7 = [l2, l4],
    Msg7 = {delete_links, [Key, V7, fun() -> true end]},
    Ans8 = memory_store_driver:modify([{Ctx, Msg7}], State, []),
    Check7 = #document{key = {y, V7}, value = V7, rev = [<<"1-rev">>],
        deleted = true},
    ?assertMatch({[ok], {true, [{{y, V7}, Ctx}]}, CheckState}, Ans8),
    {_, {true, Changes4}, CheckState} = Ans8,
    ?assertEqual([Check7, Check6, Check5], get_docs()),

    ?assertMatch([{{y, V7}, Ctx}, {{x, V6}, Ctx}, {{x, V}, Ctx}],
        memory_store_driver:merge_changes(MChanges, Changes4)),
    MChanges2 = memory_store_driver:merge_changes(MChanges, Changes4),

    ?assertEqual({true, []}, memory_store_driver:commit(MChanges2, State)),
    ?assertEqual([{y, V7}, {x, V6}, {x, V}], get_flush()),

    set_response({error, error}),
    set_get_flush_response({x, V6}, {error, error}),
    ?assertMatch({{false, [{{x, V6}, Ctx}]}, []},
        memory_store_driver:commit(MChanges2, State)),
    ?assertEqual([{y, V7}, {x, V}], get_flush()).

%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

setup() ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, 5000),
    application:set_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, 10000),
    application:set_env(?CLUSTER_WORKER_APP_NAME, memory_store_idle_timeout_ms, 60000),

    meck:new(datastore_cache, [passthrough]),
    meck:expect(datastore_cache, fetch, fun(A1, A2) -> fetch(A1, A2) end),
    meck:expect(datastore_cache, get, fun(A1, A2) -> fetch(A1, A2) end),
    meck:expect(datastore_cache, save, fun(A1, A2) -> save(A1, A2) end),
    meck:expect(datastore_cache, flush, fun(List) ->
        lists:map(fun({Key, Ctx}) -> flush(Ctx, Key) end, List)
    end),

    meck:new(memory_store_driver_links, [passthrough]),
    meck:expect(memory_store_driver_links, add_links, fun(A1, A2, A3) ->
        add_links(A1, A2, A3) end),
    meck:expect(memory_store_driver_links, get, fun(A1, A2) -> get(A1, A2) end),
    meck:expect(memory_store_driver_links, delete_links, fun(A1, A2, A3, A4) ->
        delete_links(A1, A2, A3, A4) end),
    meck:new(consistent_hasing),
    meck:expect(consistent_hasing, get_node, fun(_) -> node() end),

    meck:new(memory_store_driver_docs, [passthrough]),
    meck:expect(memory_store_driver_docs, update,
        fun(OldValue, _Diff) -> {ok, OldValue ++ "u"} end),

    meck:new(memory_store_driver, [passthrough]),
    meck:expect(memory_store_driver, increment_rev,
        fun
            (#{resolve_conflicts := true}, Doc) ->
                Doc;
            (#{persistence := false}, Doc) ->
                Doc;
            (_, #document{rev = Rev} = Doc) ->
                Doc#document{rev = [<<"1-rev">> | Rev]}
        end),

    ok.

teardown(_) ->
    meck:unload(memory_store_driver),
    meck:unload(consistent_hasing),
    meck:unload(memory_store_driver_links),
    meck:unload(datastore_cache),
    meck:unload(memory_store_driver_docs),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

set_response() ->
    set_response(get(get_response)).
set_response(Response) ->
    DCDoc = get(dc_doc),
    erase(),
    put(dc_doc, DCDoc),
    put(get_response, Response).

set_get_flush_response(Key, Response) ->
    memory_store_driver:add_to_proc_mem(get_flush_response, Key, Response).

save_doc(#document{key = Key} = Doc) ->
    memory_store_driver:add_to_proc_mem(dc_doc, Key, Doc).

get_docs() ->
    lists:map(fun({_K, V}) -> V end, get(dc_doc)).

get_doc() ->
    case get(dc_doc) of
        [{_, Doc}] -> Doc;
        Other -> Other
    end.

get_flush() ->
    case get(dc_flush) of
        undefined ->
            [];
        List ->
            lists:map(fun({K, _V}) -> K end, List)
    end.

save_doc_with_ctx(Ctx, #document{key = Key} = Doc) ->
    memory_store_driver:add_to_proc_mem(dc_ctx, {Key, Ctx}, Doc).

get_docs_with_ctx() ->
    lists:map(fun({{_K, C}, V}) -> {V, C} end, get(dc_ctx)).

%%%===================================================================
%%% Helper functions for meck
%%%===================================================================

save(_, #document{value = "error"}) ->
    {error, error};
save(_, #document{value = [l1, error]}) ->
    {error, error2};
save(Ctx, Document) ->
    save_doc(Document),
    save_doc_with_ctx(Ctx, Document),
    {ok, memory, Document}.

get(_, _Key) ->
    get(get_response).

fetch(A1, A2) ->
    case get(A1, A2) of
        {ok, Doc} ->
            {ok, memory, Doc};
        Error ->
            Error
    end.

flush(Ctx, Key) ->
    case memory_store_driver:get_from_proc_mem(get_flush_response, Key) of
        {error, _} ->
            {error, error};
        _ ->
            memory_store_driver:add_to_proc_mem(dc_flush, Key, Ctx),
            {ok, #document{key = Key}}
    end.


add_links(_, _Key, [error]) ->
    {error, error};
add_links(_, _Key, [error, l4, l6]) ->
    {error, error};
add_links(Ctx, _Key, Links) ->
    case get(get_links) of
        undefined ->
            memory_store_driver_links:save_link_doc(Ctx,
                #document{key = {x, Links}, value = Links});
        true ->
            case memory_store_driver_links:get_link_doc(Ctx, {x, Links}) of
                {error, {not_found, _}} ->
                    memory_store_driver_links:save_link_doc(Ctx,
                        #document{key = {x, Links}, value = Links});
                {ok, #document{value = L2}} ->
                    memory_store_driver_links:save_link_doc(Ctx,
                        #document{key = {x, Links}, value = Links ++ L2})
            end
    end,
    ok.

delete_links(_, _Key, LinkNames, _Pred) ->
    memory_store_driver_links:delete_link_doc(mc,
        #document{key = {y, LinkNames}, value = LinkNames}),
    ok.

-endif.