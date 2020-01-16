%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for running custom model hooks before and after
%%% datastore calls.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_hooks).
-author("Krzysztof Trzepla").

%% API
-export([wrap/5]).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type wrapped() :: fun((Function :: atom(), Args :: list()) -> term()).
-type prehook() :: fun((Function :: atom(), Args :: list()) ->
                       ok | {error, term()}).
-type posthook() :: fun((Function :: atom(), Args :: list(), Result :: term()) ->
                       term()).

-export_type([prehook/0, posthook/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Runs custom model hooks before and after datastore calls.
%% @end
%%--------------------------------------------------------------------
-spec wrap(ctx(), key(), atom(), list(), wrapped()) -> term().
wrap(Ctx0, Key, Function, Args0, Fun) ->
    Ctx = Ctx0#{routing_key => Key},
    Args = [Ctx, Key | Args0],
    Result = case run_prehooks(Ctx, Function, Args) of
        ok -> Fun(Function, Args);
        {error, Reason} -> {error, Reason}
    end,
    run_posthooks(Ctx, Function, Args, Result).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs custom model hooks before datastore call.
%% @end
%%--------------------------------------------------------------------
-spec run_prehooks(ctx(), atom(), list()) -> ok | {error, term()}.
run_prehooks(#{hooks_disabled := true}, _Function, _Args) ->
    ok;
run_prehooks(#{model := Model}, Function, Args) ->
    lists:foldl(fun
        (PreHook, ok) -> PreHook(Function, Args);
        (_PreHook, {error, Reason}) -> {error, Reason}
    end, ok, datastore_model_default:get_prehooks(Model)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs custom model hooks after datastore call.
%% @end
%%--------------------------------------------------------------------
-spec run_posthooks(ctx(), atom(), list(), term()) -> term().
run_posthooks(#{hooks_disabled := true}, _Function, _Args, Result) ->
    Result;
run_posthooks(#{model := Model}, Function, Args, Result) ->
    lists:foldl(fun(PostHook, Result2) ->
        PostHook(Function, Args, Result2)
    end, Result, datastore_model_default:get_posthooks(Model)).