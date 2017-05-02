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

% Driver mock functions
-export([save/2, get/2, delete/3]).
-export([add_links/3, delete_links/4, get_link_doc/2, save_link_doc/2, delete_link_doc/2]).

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

    ?assertEqual({merged, {add_links, [Key, [l1, l2, l4, l5]]}}, memory_store_driver_links:merge_link_ops(Msg, Msg2)),
    ?assertEqual(different, memory_store_driver_links:merge_link_ops(Msg, Msg3)),
    ?assertEqual({merged, {delete_links, [Key, [l2, l4, l6], Pred1]}},
        memory_store_driver_links:merge_link_ops(Msg3, Msg4)),
    ?assertEqual(different, memory_store_driver_links:merge_link_ops(Msg, Msg5)),
    ?assertEqual(different, memory_store_driver_links:merge_link_ops(Msg6, Msg7)),

    Msgs = [Msg, Msg2, Msg3, Msg4, Msg5, Msg6, Msg7, Msg8],
    Ans = [{{add_links, [Key, [l1, l2, l4, l5]]}, 2}, {{delete_links, [Key, [l2, l4, l6], Pred1]}, 2},
        {Msg5, 1}, {Msg6, 1}, {Msg7, 1}, {Msg8, 1}],

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
    MC = #model_config{},
    Key = k,
    Link = false,
    application:set_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, 5000),
    put(get_response, {ok, #document{key = Key, value = "g"}}),

    Ans0 = memory_store_driver:init([memory_store_driver_test, MC, Key, undefined, Link]),
    ?assertMatch({ok, #datastore_doc_init{min_commit_delay = infinity, max_commit_delay = infinity}}, Ans0),
    Ans1 = memory_store_driver:init([memory_store_driver_test, MC, Key, memory_store_flush_driver, Link]),
    ?assertMatch({ok, #datastore_doc_init{}}, Ans1),
    {ok, #datastore_doc_init{data = State}} = Ans1,

    Msg = {save, [#document{key = Key, value = "v"}]},
    Ans2 = memory_store_driver:modify([Msg], State, []),
    ?assertMatch({[{ok, Key}], {true, to_save}, _}, Ans2),
    {_, _, #state{current_value = CV} = NewState} = Ans2,
    ?assertEqual("v", CV#document.value),

    Msg1 = {save, [#document{key = Key, value = "error"}]},
    Ans2_2 = memory_store_driver:modify([Msg1], NewState, []),
    ?assertMatch({[{error, error}], false, _}, Ans2_2),
    {_, _, #state{current_value = CV1} = NewState1} = Ans2_2,
    ?assertEqual("v", CV1#document.value),

    Msg2 = {update, [Key, diff]},
    Ans3 = memory_store_driver:modify([Msg2], NewState1, []),
    ?assertMatch({[{ok, Key}], {true, to_save}, _}, Ans3),
    {_, _, #state{current_value = CV2} = NewState2} = Ans3,
    ?assertEqual("vu", CV2#document.value),

    Msg3 = {create_or_update, [#document{key = Key, value = "v"}, fun() -> true end]},
    Ans4 = memory_store_driver:modify([Msg3], NewState2, []),
    ?assertMatch({[{ok, Key}], {true, to_save}, _}, Ans4),
    {_, _, #state{current_value = CV3} = NewState3} = Ans4,
    ?assertEqual("vuu", CV3#document.value),

    Msg4 = {create, [#document{key = Key, value = "v"}]},
    Ans5 = memory_store_driver:modify([Msg4], NewState3, []),
    ?assertMatch({[{error, already_exists}], false, _}, Ans5),
    {_, _, #state{current_value = CV4} = NewState4} = Ans5,
    ?assertEqual("vuu", CV4#document.value),

    Msg5 = {delete, [Key, fun() -> true end]},
    Ans6 = memory_store_driver:modify([Msg5], NewState4, []),
    ?assertMatch({[ok], {true, to_save}, _}, Ans6),
    {_, _, #state{current_value = CV5} = NewState5} = Ans6,
    ?assertEqual(not_found, CV5),

    Msg6 = {create, [#document{key = Key, value = "v2"}]},
    Ans7 = memory_store_driver:modify([Msg6], NewState5, []),
    ?assertMatch({[{ok, Key}], {true, to_save}, _}, Ans7),
    {_, _, #state{current_value = CV6} = NewState6} = Ans7,
    ?assertEqual("v2", CV6#document.value),

    Msg7 = {delete, [Key, fun() -> true end]},
    Ans8 = memory_store_driver:modify([Msg7], NewState6, []),
    ?assertMatch({[ok], {true, to_save}, _}, Ans8),
    {_, _, #state{current_value = CV7} = NewState7} = Ans8,
    ?assertEqual(not_found, CV7),

    Msg8 = {delete, [Key, fun() -> true end]},
    Ans9 = memory_store_driver:modify([Msg8], NewState7, []),
    ?assertMatch({[ok], false, _}, Ans9),
    {_, _, #state{current_value = CV8} = NewState8} = Ans9,
    ?assertEqual(not_found, CV8),

    Msg9 = {update, [Key, diff]},
    Ans10 = memory_store_driver:modify([Msg9], NewState8, []),
    ?assertMatch({[{error, {not_found, _}}], false, _}, Ans10),
    {_, _, #state{current_value = CV9} = NewState9} = Ans10,
    ?assertEqual(not_found, CV9),

    Msg10 = {create_or_update, [#document{key = Key, value = "v3"}, fun() -> true end]},
    Ans11 = memory_store_driver:modify([Msg10], NewState9, []),
    ?assertMatch({[{ok, Key}], {true, to_save}, _}, Ans11),
    {_, _, #state{current_value = CV10} = NewState10} = Ans11,
    ?assertEqual("v3", CV10#document.value),

    ?assertEqual(ok, memory_store_driver:terminate(NewState10, [])),

    put(get_response, {error, {not_found, model}}),

    Msg11 = {update, [Key, diff]},
    Ans12 = memory_store_driver:modify([Msg11], State, []),
    ?assertMatch({[{error, {not_found, _}}], false, _}, Ans12),
    {_, _, #state{current_value = CV11}} = Ans12,
    ?assertEqual(not_found, CV11),

    Msg13 = {create, [#document{key = Key, value = "v4"}]},
    Ans14 = memory_store_driver:modify([Msg13], State, []),
    ?assertMatch({[{ok, Key}], {true, to_save}, _}, Ans14),
    {_, _, #state{current_value = CV13}} = Ans14,
    ?assertEqual("v4", CV13#document.value),

    Msg14 = {create_or_update, [#document{key = Key, value = "v5"}, fun() -> true end]},
    Ans15 = memory_store_driver:modify([Msg14], State, []),
    ?assertMatch({[{ok, Key}], {true, to_save}, _}, Ans15),
    {_, _, #state{current_value = CV14}} = Ans15,
    ?assertEqual("v5", CV14#document.value).

many_op() ->
    MC = #model_config{},
    Key = k,
    Link = false,
    application:set_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, 5000),
    put(get_response, {ok, #document{key = Key, value = "g"}}),

    Ans1 = memory_store_driver:init([memory_store_driver_test, MC, Key, memory_store_flush_driver, Link]),
    ?assertMatch({ok, #datastore_doc_init{}}, Ans1),
    {ok, #datastore_doc_init{data = State}} = Ans1,

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

    Msgs = [Msg, Msg1, Msg2, Msg3, Msg4, Msg5, Msg6, Msg7, Msg8, Msg9, Msg10],

    Ans2 = memory_store_driver:modify(Msgs, State, []),
    ?assertMatch({[{ok, Key}, {ok, Key}, {ok, Key}, {ok, Key}, {error, already_exists}, ok,
        {ok, Key}, ok, ok, {error, {not_found, _}}, {ok, Key}], {true, to_save}, _}, Ans2),
    {_, _, #state{current_value = CV} = NewState} = Ans2,
    ?assertEqual("v3", CV#document.value),

    ?assertEqual(ok, memory_store_driver:terminate(NewState, [])).

single_op_link() ->
    MC = #model_config{},
    Key = k,
    Link = true,
    application:set_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, 5000),
    erase(),

    Ans0 = memory_store_driver:init([memory_store_driver_test, MC, Key, undefined, Link]),
    ?assertMatch({ok, #datastore_doc_init{
        min_commit_delay = infinity,
        max_commit_delay = infinity
    }}, Ans0),
    Ans1 = memory_store_driver:init([memory_store_driver_test, MC, Key, memory_store_flush_driver, Link]),
    ?assertMatch({ok, #datastore_doc_init{}}, Ans1),
    {ok, #datastore_doc_init{data = State}} = Ans1,

    V = [l1, l2],
    Msg = {add_links, [Key, V]},
    Ans2 = memory_store_driver:modify([Msg], State, []),
    Check = {{x, V}, #document{key = {x, V}, value = V}},
    ?assertMatch({[ok], {true, {[{x, V}], []}}, _}, Ans2),
    {_, _, #state{current_value = CV} = NewState} = Ans2,
    ?assertMatch([Check], CV),

    V2 = [l4, l5],
    Msg2 = {add_links, [Key, V2]},
    Ans3 = memory_store_driver:modify([Msg2], NewState, []),
    Check2 = {{x, V2}, #document{key = {x, V2}, value = V2}},
    ?assertMatch({[ok], {true, {[{x, V2}], []}}, _}, Ans3),
    {_, _, #state{current_value = CV2} = NewState2} = Ans3,
    ?assertEqual([Check2, Check], CV2),

    V3 = [l2, l4],
    Msg3 = {delete_links, [Key, V3, fun() -> true end]},
    Ans4 = memory_store_driver:modify([Msg3], NewState2, []),
    Check3 = {{y, V3}, not_found},
    ?assertMatch({[ok], {true, {[{y, V3}], []}}, _}, Ans4),
    {_, _, #state{current_value = CV3} = NewState3} = Ans4,
    ?assertEqual([Check3, Check2, Check], CV3),

    Msg4 = {add_links, [Key, [error]]},
    Ans5 = memory_store_driver:modify([Msg4], NewState3, []),
    ?assertMatch({[{error, error}], false, _}, Ans5),
    {_, _, #state{current_value = CV4} = NewState4} = Ans5,
    ?assertEqual([Check3, Check2, Check], CV4),

    V5 = [l4, l6],
    Msg5 = {add_links, [Key, V5]},
    Ans6 = memory_store_driver:modify([Msg5], NewState4, []),
    Check6 = {{x, V5}, #document{key = {x, V5}, value = V5}},
    ?assertMatch({[ok], {true, {[{x, V5}], []}}, _}, Ans6),
    {_, _, #state{current_value = CV5} = NewState5} = Ans6,
    ?assertEqual([Check6, Check3, Check2, Check], CV5),

    Ans7 = memory_store_driver:modify([Msg], NewState5, []),
    ?assertMatch({[ok], {true, {[{x, V}], []}}, _}, Ans7),
    {_, _, #state{current_value = CV6} = NewState6} = Ans7,
    ?assertEqual([Check, Check6, Check3, Check2], CV6),

    V7 = [l1, error],
    Msg7 = {add_links, [Key, V7]},
    Ans8 = memory_store_driver:modify([Msg7], NewState6, []),
    ?assertMatch({[{error, error2}], false, _}, Ans8),
    {_, _, #state{current_value = CV7} = NewState7} = Ans8,
    ?assertEqual([Check, Check6, Check3, Check2], CV7),

    ?assertEqual(ok, memory_store_driver:terminate(NewState7, [])).

many_op_link() ->
    MC = #model_config{},
    Key = k,
    Link = true,
    application:set_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, 5000),

    Ans1 = memory_store_driver:init([memory_store_driver_test, MC, Key, memory_store_flush_driver, Link]),
    ?assertMatch({ok, #datastore_doc_init{}}, Ans1),
    {ok, #datastore_doc_init{data = State}} = Ans1,

    Msg = {add_links, [Key, [l1, l2]]},
    Msg2 = {add_links, [Key, [l4, l5]]},
    Msg3 = {delete_links, [Key, [l2, l4], fun() -> true end]},
    Msg4 = {add_links, [Key, [error]]},
    Msg5 = {add_links, [Key, [l4, l6]]},
    Msg6 = {delete_links, [Key, [l2, l6], fun() -> true end]},
    Msg7 = {add_links, [Key, [l1, l7]]},

    Msgs = [Msg, Msg2, Msg3, Msg4, Msg5, Msg6, Msg7, Msg6, Msg7],

    Ans2 = memory_store_driver:modify(Msgs, State, []),
    K1 = [l1,l2,l4,l5],
    K2 = [l2, l4],
    K3 = [l2,l6],
    K4 = [l1,l7],
    Check = [{x, K1}, {y, K2}, {y, K3}, {x, K4}],
    ?assertMatch({[ok, ok, ok, {error, error}, {error, error}, ok, ok, ok, ok], {true, {Check, []}}, _}, Ans2),
    {_, _, #state{current_value = CV} = NewState} = Ans2,
    CheckV = [{{x, K1}, #document{key = {x, K1}, value = K1}}, {{y, K2}, not_found}, {{y, K3}, not_found},
        {{x, K4}, #document{key = {x, K4}, value = K4}}],
    ?assertEqual(lists:reverse(CheckV), CV), % reverse due to del error value

    Msg8 = {add_links, [Key, [l1, error]]},
    Msgs2 = [Msg, Msg2, Msg3, Msg4, Msg5, Msg6, Msg7, Msg6, Msg8],

    Ans3 = memory_store_driver:modify(Msgs2, State, []),
    ?assertMatch({[{error, error2}, {error, error2}, {error, error2}, {error, error2}, {error, error2}, {error, error2},
        {error, error2}, {error, error2}, {error, error2}], false, _}, Ans3),
    {_, _, #state{current_value = CV2}} = Ans3,
% TODO - check
%%    ?assertEqual([], CV2),

    Msgs3 = [Msg8, Msg6, Msg, Msg2, Msg3, Msg4, Msg5, Msg6, Msg7],

    Ans4 = memory_store_driver:modify(Msgs3, State, []),
    ?assertMatch({[{error, error2}, {error, error2}, {error, error2}, {error, error2}, {error, error2}, {error, error2},
        {error, error2}, {error, error2}, {error, error2}], {true, {Check, []}}, _}, Ans4),
    {_, _, #state{current_value = CV3}} = Ans4,
% TODO - check
%%    ?assertEqual(CheckV, CV3),

    ?assertEqual(ok, memory_store_driver:terminate(NewState, [])).

flush_doc() ->
    MC = #model_config{},
    Key = k,
    Link = false,
    put(get_response, {error, {not_found, mc}}),
    put(get_flush_response, {error, {not_found, mc}}),

    Ans1 = memory_store_driver:init([memory_store_driver_test, MC, Key, memory_store_flush_driver, Link]),
    ?assertMatch({ok, #datastore_doc_init{min_commit_delay = 5000, max_commit_delay = 10000}}, Ans1),
    {ok, #datastore_doc_init{data = State}} = Ans1,

    Msg = {update, [Key, diff]},
    Ans2 = memory_store_driver:modify([Msg], State, []),
    ?assertMatch({[{error, {not_found, _}}], false, _}, Ans2),
    {_, _, #state{current_value = CV}} = Ans2,
    ?assertEqual(not_found, CV),

    Msg2 = {create, [#document{key = Key, value = "v4"}]},
    Ans3 = memory_store_driver:modify([Msg2], State, []),
    ?assertMatch({[{ok, Key}], {true, to_save}, _}, Ans3),
    {_, _, #state{current_value = CV2}} = Ans3,
    ?assertEqual("v4", CV2#document.value),

    Msg3 = {create_or_update, [#document{key = Key, value = "v5"}, fun() -> true end]},
    Ans4 = memory_store_driver:modify([Msg3], State, []),
    ?assertMatch({[{ok, Key}], {true, to_save}, _}, Ans4),
    {_, _, #state{current_value = CV3}} = Ans4,
    ?assertEqual("v5", CV3#document.value),

    put(get_flush_response, {ok, #document{key = Key, value = "fg"}}),

    Ans5 = memory_store_driver:modify([Msg], State, []),
    ?assertMatch({[{error, {not_found, _}}], false, _}, Ans5),
    {_, _, #state{current_value = CV4}} = Ans5,
    ?assertEqual(not_found, CV4),

    meck:expect(caches_controller, check_cache_consistency, fun(_, _) -> not_monitored end),
    meck:expect(caches_controller, check_cache_consistency_direct, fun(_, _) -> not_monitored end),
    Ans5_2 = memory_store_driver:modify([Msg], State, []),
    ?assertMatch({[{ok, Key}], {true, to_save}, _}, Ans5_2),
    {_, {true, Changes}, #state{current_value = CV4_2} = NewState2} = Ans5_2,
    ?assertEqual("fgu", CV4_2#document.value),

    Ans6 = memory_store_driver:modify([Msg3], NewState2, []),
    ?assertMatch({[{ok, Key}], {true, to_save}, _}, Ans6),
    {_, {true, Changes2}, #state{current_value = CV5} = NewState3} = Ans6,
    ?assertEqual("fguu", CV5#document.value),

    ?assertMatch(Changes2, memory_store_driver:merge_changes(Changes, Changes2)),

    Ans7 = memory_store_driver:modify([Msg2], NewState3, []),
    ?assertMatch({[{error, already_exists}], false, _}, Ans7),
    {_, _Changes3, #state{current_value = CV6} = NewState4} = Ans7,
    ?assertEqual("fguu", CV6#document.value),

    ?assertEqual({true, undefined}, memory_store_driver:commit(Changes2, NewState4)),
    ?assertMatch({{false, Changes2}, undefined}, memory_store_driver:commit(Changes2,
        NewState4#state{current_value = #document{key = Key, value = "error"}})),

    Msg7 = {delete, [Key, fun() -> true end]},
    Ans8 = memory_store_driver:modify([Msg7], NewState4, []),
    ?assertMatch({[ok], {true, to_save}, _}, Ans8),
    {_, {true, Changes4}, #state{current_value = CV7} = NewState5} = Ans8,
    ?assertEqual(not_found, CV7),

    ?assertEqual({true, undefined}, memory_store_driver:commit(Changes4, NewState5)),

    ?assertEqual(ok, memory_store_driver:terminate(NewState5, [])),

    put(get_last_response, {error, {not_found, model}}),
    Msg15 = {force_save, [#document{key = Key, value = "fsv", rev = {1, [1]}}]},
    Ans16 = memory_store_driver:modify([Msg15], State, []),
    ?assertMatch({[ok], {true, {to_save,
        {#document{key = Key, value = "fsv"}, b, false}}}, _}, Ans16),
    {_, {true, Changes16}, #state{current_value = CV15} = FlushState16} = Ans16,
    ?assertEqual("fsv", CV15#document.value),
    ?assertEqual({true, undefined}, memory_store_driver:commit(Changes16, FlushState16)),

    put(get_last_response, {ok, #document{key = Key, value = "fgu", rev = <<"2-2">>}}),
    Ans17 = memory_store_driver:modify([Msg15], NewState2, []),
    ?assertMatch({[ok], false, _}, Ans17),
    {_, _, #state{current_value = CV16}} = Ans17,
    ?assertEqual("fgu", CV16#document.value),

    Msg17 = {force_save, [#document{key = Key, value = "fsv", rev = {2, [<<"3">>]}}]},
    Ans18 = memory_store_driver:modify([Msg17], NewState2, []),
    ?assertMatch({[ok], {true, {to_save, {#document{key = Key, value = "fsv"},
        b, #document{key = Key, value = "fgu", rev = <<"2-2">>}}}}, _}, Ans18),
    {_, {true, Changes18}, #state{current_value = CV17} = FlushState18} = Ans18,
    ?assertEqual("fsv", CV17#document.value),
    ?assertEqual({true, undefined}, memory_store_driver:commit(Changes18, FlushState18)).

flush_links() ->
    MC = #model_config{},
    Key = k,
    Link = true,
    put(get_links, true),
    put(get_response, {error, {not_found, mc}}),
    put(get_flush_response, {error, {not_found, mc}}),

    Ans1 = memory_store_driver:init([memory_store_driver_test, MC, Key, memory_store_flush_driver, Link]),
    ?assertMatch({ok, #datastore_doc_init{min_commit_delay = 5000, max_commit_delay = 10000}}, Ans1),
    {ok, #datastore_doc_init{data = State}} = Ans1,

    V = [l1, l2],
    Msg = {add_links, [Key, V]},
    Ans2 = memory_store_driver:modify([Msg], State, []),
    Check = {{x, V}, #document{key = {x, V}, value = V}},
    ?assertMatch({[ok], {true, {[{x, V}], []}}, _}, Ans2),
    {_, _, #state{current_value = CV}} = Ans2,
    ?assertEqual([Check], CV),

    put(get_links, true),
    put(get_response, {ok, #document{value = [ggg]}}),
    put(get_flush_response, {ok, #document{value = [ggg2]}}),

    V2 = [l1, l2, ggg],
    Ans3 = memory_store_driver:modify([Msg], State, []),
    Check2 = {{x, V}, #document{key = {x, V}, value = V2}},
    ?assertMatch({[ok], {true, {[{x, V}], []}}, _}, Ans3),
    {_, _, #state{current_value = CV2}} = Ans3,
    ?assertEqual([Check2], CV2),

    put(get_links, true),
    put(get_response, {error, {not_found, mc}}),
    put(get_flush_response, {ok, #document{value = [ggg2]}}),

    Ans4 = memory_store_driver:modify([Msg], State, []),
    ?assertMatch({[ok], {true, {[{x, V}], []}}, _}, Ans4),
    {_, _, #state{current_value = CV3}} = Ans4,
    ?assertEqual([Check], CV3),

    put(get_links, true),
    put(get_response, {error, {not_found, mc}}),
    put(get_flush_response, {ok, #document{value = [ggg2]}}),
    meck:expect(caches_controller, check_cache_consistency, fun(_, _, _) -> not_monitored end),
    meck:expect(caches_controller, check_cache_consistency_direct, fun(_, _, _) -> not_monitored end),

    V4 = [l1, l2, ggg2],
    Ans5 = memory_store_driver:modify([Msg], State, []),
    Check4 = {{x, V}, #document{key = {x, V}, value = V4}},
    ?assertMatch({[ok], {true, {[{x, V}], []}}, _}, Ans5),
    {_, {true, Changes}, #state{current_value = CV4} = NewState} = Ans5,
    ?assertEqual([Check4], CV4),

    put(get_links, true),

    V5 = [l1, l2, l1, l2, ggg2],
    Ans6 = memory_store_driver:modify([Msg], NewState, []),
    Check5 = {{x, V}, #document{key = {x, V}, value = V5}},
    ?assertMatch({[ok], {true, {[{x, V}], []}}, _}, Ans6),
    {_, {true, Changes2}, #state{current_value = CV5} = NewState2} = Ans6,
    ?assertEqual([Check5], CV5),

    ?assertMatch(Changes2, memory_store_driver:merge_changes(Changes, Changes2)),

    put(get_links, true),
    put(get_response, {error, {not_found, mc}}),
    put(get_flush_response, {error, {not_found, mc}}),

    V6 = [l3, l4],
    Msg6 = {add_links, [Key, V6]},
    Ans7 = memory_store_driver:modify([Msg6], NewState2, []),
    Check6 = {{x, V6}, #document{key = {x, V6}, value = V6}},
    ?assertMatch({[ok], {true, {[{x, V6}], []}}, _}, Ans7),
    {_, {true, Changes3}, #state{current_value = CV6} = NewState3} = Ans7,
    ?assertEqual([Check6, Check5], CV6),

    ?assertMatch({[{x, V6}, {x, V}], []}, memory_store_driver:merge_changes(Changes2, Changes3)),
    MChanges = memory_store_driver:merge_changes(Changes2, Changes3),

    V7 = [l2, l4],
    Msg7 = {delete_links, [Key, V7, fun() -> true end]},
    Ans8 = memory_store_driver:modify([Msg7], NewState3, []),
    Check7 = {{y, V7}, not_found},
    ?assertMatch({[ok], {true, {[{y, V7}], []}}, _}, Ans8),
    {_, {true, Changes4}, #state{current_value = CV7} = NewState4} = Ans8,
    ?assertEqual([Check7, Check6, Check5], CV7),

    ?assertMatch({[{y, V7}, {x, V6}, {x, V}], []}, memory_store_driver:merge_changes(MChanges, Changes4)),
    MChanges2 = memory_store_driver:merge_changes(MChanges, Changes4),

    put(get_flush_response, {error, {not_found, mc}}),

    ?assertEqual({true, []}, memory_store_driver:commit(MChanges2, NewState4)),
    ?assertMatch({{false, {[{x, V6}], []}}, []}, memory_store_driver:commit(MChanges2,
        NewState4#state{current_value = [Check7, {{x, V6}, #document{value = error}}, Check5]})),

    put(get_last_response, {error, {not_found, model}}),
    V15 = [llll],
    Check15 = {k, #document{key = k, value = V15}},
    Msg15 = {force_save, [#document{key = Key, value = V15, rev = {1, [1]}}]},
    Ans16 = memory_store_driver:modify([Msg15], State, []),
    ?assertMatch({[ok], {true, {[Key], [{Key,
        {#document{key = Key, value = V15}, b, false}}]}}, _}, Ans16),
    {_, {true, Changes16}, #state{current_value = CV15} = FlushState16} = Ans16,
    ?assertEqual([Check15], CV15),
    ?assertEqual({true, []}, memory_store_driver:commit(Changes16, FlushState16)),

    put(get_last_response, {ok, #document{key = Key, value = "fgu", rev = <<"2-2">>}}),
    Ans17 = memory_store_driver:modify([Msg15], NewState2, []),
    ?assertMatch({[ok], false, _}, Ans17),
    {_, _, #state{current_value = CV16}} = Ans17,
    ?assertEqual([Check5], CV16),

    put(get_last_response, {ok, #document{key = Key, value = "fgu", rev = <<"2-2">>}}),
    Msg17 = {force_save, [#document{key = Key, value = V15, rev = {2, [<<"3">>]}}]},
    Ans18 = memory_store_driver:modify([Msg17], NewState2, []),
    ?assertMatch({[ok], {true, {[Key], [{Key, {#document{key = Key, value = V15},
        b, #document{key = Key, value = "fgu", rev = <<"2-2">>}}}]}}, _}, Ans18),
    {_, {true, Changes18}, #state{current_value = CV17} = FlushState18} = Ans18,
    ?assertEqual([Check15, Check5], CV17),
    ?assertEqual({true, []}, memory_store_driver:commit(Changes18, FlushState18)).

%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

setup() ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, 5000),
    application:set_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, 10000),
    application:set_env(?CLUSTER_WORKER_APP_NAME, memory_store_idle_timeout_ms, 60000),

    meck:new(caches_controller, [passthrough]),
    meck:expect(caches_controller, check_cache_consistency, fun(_, _) -> {ok, ok, ok} end),
    meck:expect(caches_controller, check_cache_consistency, fun(_, _, _) -> {ok, ok, ok} end),
    meck:expect(caches_controller, check_cache_consistency_direct, fun(_, _) -> {ok, ok, ok} end),
    meck:expect(caches_controller, check_cache_consistency_direct, fun(_, _, _) -> {ok, ok, ok} end),
    meck:expect(caches_controller, get_cache_uuid, fun(_, _) -> cuuid end),

    meck:new(consistent_hasing),
    meck:expect(consistent_hasing, get_node, fun(_) -> node() end),

    meck:new(memory_store_driver_docs, [passthrough]),
    meck:expect(memory_store_driver_docs, update, fun(OldValue, _Diff) -> {ok, OldValue ++ "u"} end),

    ok.

teardown(_) ->
    meck:unload(consistent_hasing),
    meck:unload(caches_controller),
    meck:unload(memory_store_driver_docs),
    ok.

%%%===================================================================
%%% Driver mock functions
%%%===================================================================

save(_ModelConfig, #document{value = "error"}) ->
    {error, error};
save(_ModelConfig, Document) ->
    {ok, Document#document.key}.

get(_ModelConfig, _Key) ->
    get(get_response).

delete(_ModelConfig, _Key, _Pred) ->
    ok.

add_links(_ModelConfig, _Key, [error]) ->
    {error, error};
add_links(_ModelConfig, _Key, [error, l4, l6]) ->
    {error, error};
add_links(_ModelConfig, _Key, Links) ->
    case get(get_links) of
        undefined ->
            memory_store_driver_links:save_link_doc(mc, #document{key = {x, Links}, value = Links});
        true ->
            case memory_store_driver_links:get_link_doc(#model_config{name = mc}, {x, Links}) of
                {error, {not_found, _}} ->
                    memory_store_driver_links:save_link_doc(mc, #document{key = {x, Links}, value = Links});
                {ok, #document{value = L2}} ->
                    memory_store_driver_links:save_link_doc(mc, #document{key = {x, Links}, value = Links ++ L2})
            end
    end,
    ok.

get_link_doc(_ModelConfig, _Key) ->
    get(get_response).

delete_links(_ModelConfig, _Key, LinkNames, _Pred) ->
    memory_store_driver_links:delete_link_doc(mc, #document{key = {y, LinkNames}, value = LinkNames}),
    ok.

save_link_doc(_ModelConfig, #document{value = [l1, error]}) ->
    {error, error2};
save_link_doc(_ModelConfig, #document{}) ->
    {ok, ok}.

delete_link_doc(_ModelConfig, _Key) ->
    ok.

-endif.