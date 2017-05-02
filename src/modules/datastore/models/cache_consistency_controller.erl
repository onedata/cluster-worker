%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model that is used to control cache consistency with disk.
%%% @end
%%%-------------------------------------------------------------------
-module(cache_consistency_controller).
-author("Michal Wrzeszcz").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_internal_model.hrl").
-include_lib("ctool/include/logging.hrl").

%% model_behaviour callbacks and API
-export([save/1, get/1, list/0, list/1, exists/1, delete/1, delete/2, update/2, create/1,
    save/2, get/2, exists/2, delete/3, update/3, create/2,
    create_or_update/2, create_or_update/3, model_init/0, 'after'/5, before/4]).

-define(LEVEL_OVERRIDE(Level), [{level, Level}]).

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
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback save/1 but allows
%% choice of store level.
%% @end
%%--------------------------------------------------------------------
-spec save(Level :: datastore:store_level(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(Level, Document) ->
    model:execute_with_default_context(?MODULE, save, [Document],
        ?LEVEL_OVERRIDE(Level)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback update/2 but allows
%% choice of store level.
%% @end
%%--------------------------------------------------------------------
-spec update(Level :: datastore:store_level(), datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Level, Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff],
        ?LEVEL_OVERRIDE(Level)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback create/1 but allows
%% choice of store level.
%% @end
%%--------------------------------------------------------------------
-spec create(Level :: datastore:store_level(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(Level, Document) ->
    model:execute_with_default_context(?MODULE, create, [Document],
        ?LEVEL_OVERRIDE(Level)).

%%--------------------------------------------------------------------
%% @doc
%% Updates given document by replacing given fields with new values or
%% creates new one if not exists.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(Document :: datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
create_or_update(Document, Diff) ->
    model:execute_with_default_context(?MODULE, create_or_update, [Document, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Updates given document by replacing given fields with new values or
%% creates new one if not exists.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(Level :: datastore:store_level(), Document :: datastore:document(),
    Diff :: datastore:document_diff()) -> {ok, datastore:ext_key()} | datastore:generic_error().
create_or_update(Level, Document, Diff) ->
    model:execute_with_default_context(?MODULE, create_or_update,
        [Document, Diff], ?LEVEL_OVERRIDE(Level)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback get/1 but allows
%% choice of store level.
%% @end
%%--------------------------------------------------------------------
-spec get(Level :: datastore:store_level(), datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Level, Key) ->
    model:execute_with_default_context(?MODULE, get, [Key],
        ?LEVEL_OVERRIDE(Level)).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    model:execute_with_default_context(?MODULE, list, [?GET_ALL, []]).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records at chosen store level.
%% @end
%%--------------------------------------------------------------------
-spec list(Level :: datastore:store_level()) -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list(Level) ->
    model:execute_with_default_context(?MODULE, list, [?GET_ALL, []],
        ?LEVEL_OVERRIDE(Level)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback delete/1 but allows
%% choice of store level.
%% @end
%%--------------------------------------------------------------------
-spec delete(Level :: datastore:store_level(), datastore:ext_key()) ->
    ok | datastore:generic_error().
delete(Level, Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key],
        ?LEVEL_OVERRIDE(Level)).

%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:store_level(), datastore:ext_key(), datastore:delete_predicate()) ->
    ok | datastore:generic_error().
delete(Level, Key, Pred) ->
    model:execute_with_default_context(?MODULE, delete, [Key, Pred],
        ?LEVEL_OVERRIDE(Level)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link model_behaviour} callback exists/1 but allows
%% choice of store level.
%% @end
%%--------------------------------------------------------------------
-spec exists(Level :: datastore:store_level(), datastore:ext_key()) -> datastore:exists_return().
exists(Level, Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key],
        ?LEVEL_OVERRIDE(Level))).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(ccc_bucket, [], ?GLOBAL_ONLY_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok | datastore:generic_error().
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) ->
    ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.
