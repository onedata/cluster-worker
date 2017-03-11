%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model that holds information useful in node management.
%%% @end
%%%-------------------------------------------------------------------
-module(node_management).
-author("Michal Wrzeszcz").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_internal_model.hrl").
-include_lib("ctool/include/logging.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    create_or_update/2, model_init/0, 'after'/5, before/4]).

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
%% Updates given document by replacing given fields with new values or
%% creates new one if not exists.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(Document :: datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create_or_update(Document, Diff) ->
    datastore:create_or_update(?STORE_LEVEL, Document, Diff).

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
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(?STORE_LEVEL, ?MODEL_NAME, ?GET_ALL, []).

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
    ?MODEL_CONFIG(node_management_bucket,
        caches_controller:get_hooks_throttling_config(datastore_config:global_caches()), ?LOCAL_ONLY_LEVEL).

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
    ok | {task, task_manager:task()} | {tasks, [task_manager:task()]} | datastore:generic_error().
before(ModelName, save, ?GLOBAL_ONLY_LEVEL, _) ->
    caches_controller:throttle(ModelName);
before(ModelName, create_or_update, ?GLOBAL_ONLY_LEVEL, _) ->
    caches_controller:throttle(ModelName);
before(ModelName, update, ?GLOBAL_ONLY_LEVEL, _) ->
    caches_controller:throttle(ModelName);
before(ModelName, create, ?GLOBAL_ONLY_LEVEL, _) ->
    caches_controller:throttle(ModelName);
before(ModelName, delete, ?GLOBAL_ONLY_LEVEL, _) ->
    caches_controller:throttle_del(ModelName);
before(ModelName, add_links, ?GLOBAL_ONLY_LEVEL, _) ->
    caches_controller:throttle(ModelName);
before(ModelName, set_links, ?GLOBAL_ONLY_LEVEL, _) ->
    caches_controller:throttle(ModelName);
before(ModelName, create_link, ?GLOBAL_ONLY_LEVEL, _) ->
    caches_controller:throttle(ModelName);
before(ModelName, delete_links, ?GLOBAL_ONLY_LEVEL, _) ->
    caches_controller:throttle_del(ModelName);
before(_ModelName, _Method, _Level, _Context) ->
    ok.
