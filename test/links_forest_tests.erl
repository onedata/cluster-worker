%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Eunit tests for the links_forest module.
%%% @end
%%%-------------------------------------------------------------------
-module(links_forest_tests).
-author("Michal Stanisz").

-ifdef(TEST).

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_links.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TREE_ID, <<"tree">>).
-define(LOWER_REV, <<"1-a">>).
-define(HIGHER_REV, <<"2-b">>).

%%%===================================================================
%%% Tests
%%%===================================================================

local_not_synced_flag_is_kept_when_remote_tree_wins_test() ->
    LocalDoc = forest_doc(<<"local_root">>, ?LOWER_REV, true),
    RemoteDoc = forest_doc(<<"remote_root">>, ?HIGHER_REV, false),
    ?assertMatch({false, #document{ignore_in_changes = true}},
        links_forest:resolve_conflict(#{}, RemoteDoc, LocalDoc)).


local_not_synced_flag_is_kept_when_local_tree_wins_test() ->
    LocalDoc = forest_doc(<<"local_root">>, ?HIGHER_REV, true),
    RemoteDoc = forest_doc(<<"remote_root">>, ?LOWER_REV, false),
    ?assertMatch({true, #document{ignore_in_changes = true}},
        links_forest:resolve_conflict(#{}, RemoteDoc, LocalDoc)).


remote_flag_is_taken_when_local_forest_is_synced_test() ->
    lists:foreach(fun(RemoteIgnoreInChanges) ->
        LocalDoc = forest_doc(<<"local_root">>, ?LOWER_REV, false),
        RemoteDoc = forest_doc(<<"remote_root">>, ?HIGHER_REV, RemoteIgnoreInChanges),
        ?assertMatch({false, #document{ignore_in_changes = RemoteIgnoreInChanges}},
            links_forest:resolve_conflict(#{}, RemoteDoc, LocalDoc))
    end, [true, false]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
forest_doc(TreeRootId, Rev, IgnoreInChanges) ->
    #document{
        key = <<"forest">>,
        value = #links_forest{model = test_model, key = <<"forest">>, trees = #{?TREE_ID => {TreeRootId, Rev}}},
        ignore_in_changes = IgnoreInChanges
    }.

-endif.
