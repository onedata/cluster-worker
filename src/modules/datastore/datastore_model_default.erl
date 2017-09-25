%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides default implementation of datastore model behaviour.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_model_default).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([get_ctx/1, get_record_version/1, get_prehooks/1, get_posthooks/1]).
-export([resolve_conflict/4]).
-export([set_defaults/1]).

-type model() :: datastore_model:model().
-type version() :: datastore_model:version().
-type doc() :: datastore:doc().
-type ctx() :: datastore:ctx().
-type ctx_default() :: {[atom()], term()}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model context by calling model's 'get_ctx/0' function and sets
%% defaults. If the function is not provided by the model, runs base context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx(model()) -> ctx().
get_ctx(Model) ->
    Ctx = model_apply(Model, {get_ctx, []}, fun() -> #{model => Model} end),
    set_defaults(Ctx).

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version by calling model's 'get_record_version/0'
%% function. If the function is not provided by the model, runs default version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version(model()) -> version().
get_record_version(Model) ->
    model_apply(Model, {get_record_version, []}, fun() -> 1 end).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of callbacks that will be called before each datastore model
%% operation.
%% @end
%%--------------------------------------------------------------------
-spec get_prehooks(model()) -> [datastore_hooks:prehook()].
get_prehooks(Model) ->
    model_apply(Model, {get_prehooks, []}, fun() -> [] end).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of callbacks that will be called after each datastore model
%% operation. Result of datastore model call will be passed to the first
%% callback which outcome will be passed to the next callback. The last
%% callback outcome will be the final result of datastore model call.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks(model()) -> [datastore_hooks:posthook()].
get_posthooks(Model) ->
    model_apply(Model, {get_posthooks, []}, fun() -> [] end).

%%--------------------------------------------------------------------
%% @doc
%% Provides custom resolution of remote, concurrent modification conflicts.
%% Should return 'default' if default conflict resolution should be applied.
%% Should return 'ignore' if new change is obsolete.
%% Should return '{Modified, Doc}' when custom conflict resolution has been
%% applied, where Modified defines whether next revision should be generated.
%% If Modified is set to 'false' conflict resolution outcome will be saved as
%% it is.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(model(), ctx(), doc(), doc()) ->
    {boolean(), doc()} | ignore | default.
resolve_conflict(Model, Ctx, Doc, PrevDoc) ->
    model_apply(Model, {resolve_conflict, [Ctx, Doc, PrevDoc]}, fun() ->
        default
    end).

%%--------------------------------------------------------------------
%% @doc
%% Sets defaults for a datastore model context.
%% @end
%%--------------------------------------------------------------------
-spec set_defaults(ctx()) -> ctx().
set_defaults(Ctx) ->
    Ctx2 = set_memory_driver(Ctx),
    Ctx3 = set_disc_driver(Ctx2),
    set_remote_driver(Ctx3).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to call custom model function with provided arguments. If such function
%% is not exported fallbacks to the default implementation.
%% @end
%%--------------------------------------------------------------------
-spec model_apply(model(), {atom(), list()}, fun()) -> term().
model_apply(Model, {Function, Args}, DefaultFun) ->
    Exports = Model:module_info(functions),
    Arity = length(Args),
    case lists:keyfind(Function, 1, Exports) of
        {Function, Arity} -> erlang:apply(Model, Function, Args);
        _ -> DefaultFun()
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets default memory driver.
%% @end
%%--------------------------------------------------------------------
-spec set_memory_driver(ctx()) -> ctx().
set_memory_driver(Ctx = #{memory_driver := undefined}) ->
    Ctx;
set_memory_driver(Ctx = #{model := Model}) ->
    Name = atom_to_list(Model),
    set_defaults(Ctx, [
        {[memory_driver], ets_driver},
        {[memory_driver_ctx, table], list_to_atom(Name ++ "_table")},
        {[memory_driver_opts], []}
    ]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets default disc driver.
%% @end
%%--------------------------------------------------------------------
-spec set_disc_driver(ctx()) -> ctx().
set_disc_driver(Ctx = #{disc_driver := undefined}) ->
    Ctx;
set_disc_driver(Ctx) ->
    set_defaults(Ctx, [
        {[disc_driver], couchbase_driver},
        {[disc_driver_ctx, bucket], <<"onedata">>},
        {[disc_driver_ctx, no_seq], not maps:get(sync_enabled, Ctx, false)}
    ]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets default remote driver.
%% @end
%%--------------------------------------------------------------------
-spec set_remote_driver(ctx()) -> ctx().
set_remote_driver(Ctx = #{remote_driver := undefined}) ->
    Ctx;
set_remote_driver(Ctx) ->
    RemoteDriver = application:get_env(cluster_worker, datastore_remote_driver,
        undefined),
    set_defaults(Ctx, [
        {[remote_driver], RemoteDriver}
    ]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets default context parameters.
%% @end
%%--------------------------------------------------------------------
-spec set_defaults(ctx(), [ctx_default()]) -> ctx().
set_defaults(Ctx, Defaults) ->
    lists:foldl(fun({Keys, Default}, Ctx2) ->
        set_default(Keys, Default, Ctx2)
    end, Ctx, Defaults).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets default context parameter.
%% @end
%%--------------------------------------------------------------------
-spec set_default([atom()], term(), ctx()) -> ctx().
set_default([Key], Default, Ctx) ->
    maps:put(Key, maps:get(Key, Ctx, Default), Ctx);
set_default([Key | Keys], Default, Ctx) ->
    Ctx2 = set_default(Keys, Default, maps:get(Key, Ctx, #{})),
    maps:put(Key, Ctx2, Ctx).