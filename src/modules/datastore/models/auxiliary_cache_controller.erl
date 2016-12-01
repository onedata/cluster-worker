%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Model responsible for updating state of auxiliary tables.
%%% This module doesn't use save, update, create, get, delete and exists
%%% functions, they're only implemented because module implements model_behaviour.
%%% Store operations are performed directly on drivers in foreach_aux_cache.
%%% @end
%%%-------------------------------------------------------------------
-module(auxiliary_cache_controller).
-author("Jakub Kudzia").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_internal_model.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").


%% model_behaviour callbacks and API
-export([save/1, update/2, create/1, get/1, delete/1, exists/1,
    model_init/0, 'after'/5, before/4, get_hooks_config/0]).


%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(acc_bucket, ?MODULE:get_hooks_config(), ?GLOBAL_ONLY_LEVEL).


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(
    ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, ?DISK_ONLY_LEVEL, _Context, _ReturnValue) ->
    ok;
'after'(ModelName, delete, _Level, [Key, DelPred], ok) ->
    case DelPred() of
        true ->
            foreach_aux_cache(ModelName, delete, [Key]);
        _ -> ok
    end;
'after'(ModelName, save, _Level, [Doc], {ok, Key}) ->
    foreach_aux_cache(ModelName, save, [Key, Doc]);
'after'(ModelName, update, Level, [Key, _Diff], {ok, Key}) ->
    foreach_aux_cache(ModelName, update, [Key, Level]);
'after'(ModelName, create, _Level, [Doc], {ok, Key}) ->
    foreach_aux_cache(ModelName, create, [Key, Doc]);
'after'(ModelName, create_or_update, Level, [_Doc, _Diff], {ok, Key}) ->
    foreach_aux_cache(ModelName, update, [Key, Level]);
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Provides hooks configuration.
%% @end
%%--------------------------------------------------------------------
-spec get_hooks_config() -> list().
get_hooks_config() ->
    get_hooks_config(datastore:models_with_aux_caches()).

%%%===================================================================
%%% Internal functions
%%%===================================================================



%%--------------------------------------------------------------------
%% @private
%% @doc
%% Provides hooks configuration on the basis of models list.
%% @end
%%--------------------------------------------------------------------
-spec get_hooks_config(Models :: [#model_config{}]) -> list().
get_hooks_config(Models) ->
    Methods = [save, delete, update, create, create_or_update],
    lists:foldl(fun(Model, AccIn) ->
        ModelConfig = [{Model, Method} || Method <- Methods],
        ModelConfig ++ AccIn
    end, [], Models).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Performs given action (Method) on each auxiliary cache for
%% given model.
%% @end
%%--------------------------------------------------------------------
-spec foreach_aux_cache(
    ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(), term()) -> ok.
foreach_aux_cache(#model_config{auxiliary_caches = AuxCaches}=ModelConfig, Method, Args) ->
    AuxMethod = method_to_aux_method(Method),
    lists:foreach(fun({Field, #aux_cache_config{level = StoreLevel}}) ->
        Driver = datastore:level_to_driver(StoreLevel),
        ok = Driver:AuxMethod(ModelConfig, Field, Args)
    end, maps:to_list(AuxCaches));
foreach_aux_cache(ModelName, Method, Args) ->
    foreach_aux_cache(ModelName:model_init(), Method, Args).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns given method name with prefix "aux_"
%% @end
%%--------------------------------------------------------------------
-spec method_to_aux_method(Method :: model_behaviour:model_action()) -> atom().
method_to_aux_method(Method) ->
    binary_to_atom(<<"aux_", (atom_to_binary(Method, utf8))/binary>>, utf8).