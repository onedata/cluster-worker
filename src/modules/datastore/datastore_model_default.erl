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
-export([get_ctx/1, get_basic_ctx/1, get_record_version/1, get_prehooks/1, get_posthooks/1,
    get_default_disk_ctx/0]).
-export([resolve_conflict/4]).
-export([set_defaults/1, set_defaults/2]).

-type model() :: datastore_model:model().
-type version() :: datastore_model:record_version().
-type doc() :: datastore:doc().
-type key() :: datastore:key().
-type ctx() :: datastore:ctx().

-define(DEFAULT_BUCKET, <<"onedata">>).
-define(EXTEND_TABLE_NAME(Model), list_to_atom(atom_to_list(Model) ++ "_table")).
-define(EXTEND_TABLE_NAME(UniqueKey, Model), list_to_atom(
    datastore_multiplier:extend_name(UniqueKey, atom_to_list(Model) ++ "_table"))).

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
%% Returns model context by calling model's 'get_ctx/0' function.
%% Does not sets defaults.
%% @end
%%--------------------------------------------------------------------
-spec get_basic_ctx(model()) -> ctx().
get_basic_ctx(Model) ->
    model_apply(Model, {get_ctx, []}, fun() -> #{model => Model} end).

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

%%--------------------------------------------------------------------
%% @doc
%% Sets defaults for a datastore model context.
%% @end
%%--------------------------------------------------------------------
-spec set_defaults(key(), ctx()) -> ctx().
set_defaults(UniqueKey, Ctx) ->
    Ctx2 = set_memory_driver(UniqueKey, Ctx),
    Ctx3 = set_disc_driver(Ctx2),
    Ctx4 = set_remote_driver(Ctx3),
    ensure_routing_key(UniqueKey, Ctx4).

%%-------------------------------------------------------------------
%% @doc
%% Returns default disk ctx.
%% @end
%%-------------------------------------------------------------------
-spec get_default_disk_ctx() -> datastore:disc_driver_ctx().
get_default_disk_ctx() ->
    #{bucket => ?DEFAULT_BUCKET}.

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
set_memory_driver(Ctx = #{model := Model, memory_driver := _MemDriver}) ->
    Ctx#{memory_driver_ctx => #{table => ?EXTEND_TABLE_NAME(Model)},
        memory_driver_opts => []};
set_memory_driver(Ctx) ->
    set_memory_driver(Ctx#{memory_driver => ets_driver}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets default memory driver.
%% @end
%%--------------------------------------------------------------------
-spec set_memory_driver(key(), ctx()) -> ctx().
set_memory_driver(_UniqueKey, Ctx = #{memory_driver := undefined}) ->
    Ctx;
set_memory_driver(UniqueKey, Ctx = #{model := Model, memory_driver := _MemDriver}) ->
    Ctx#{memory_driver_ctx => #{table => ?EXTEND_TABLE_NAME(UniqueKey, Model)},
        memory_driver_opts => []};
set_memory_driver(UniqueKey, Ctx) ->
    set_memory_driver(UniqueKey, Ctx#{memory_driver => ets_driver}).


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
    Ctx#{
        disc_driver => couchbase_driver,
        disc_driver_ctx => #{
            bucket => ?DEFAULT_BUCKET,
            no_seq => not maps:get(sync_enabled, Ctx, false),
            expiry => maps:get(expiry, Ctx, 0)
        }
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets default remote driver.
%% @end
%%--------------------------------------------------------------------
-spec set_remote_driver(ctx()) -> ctx().
set_remote_driver(Ctx = #{remote_driver := _RD}) ->
    Ctx;
set_remote_driver(Ctx) ->
    Ctx#{remote_driver => undefined}.

-spec ensure_routing_key(key(), ctx()) -> ctx().
ensure_routing_key(_, Ctx = #{routing_key := _}) ->
    Ctx;
ensure_routing_key(Key, Ctx) ->
    Ctx#{routing_key => Key}.