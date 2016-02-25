%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used by node manager to coordinate
%%% clearing of not used values cached in memory.
%%% TODO - sort cache documents by timestamp.
%%% @end
%%%-------------------------------------------------------------------
-module(caches_controller).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include("elements/task_manager/task_manager.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([clear_local_cache/1, clear_global_cache/1, clear_local_cache/2, clear_global_cache/2]).
-export([clear_cache/2, clear_cache/3, should_clear_cache/1, get_hooks_config/1, wait_for_cache_dump/0]).
-export([delete_old_keys/2, delete_all_keys/1]).
-export([get_cache_uuid/2, decode_uuid/1, cache_to_datastore_level/1, cache_to_task_level/1]).
-export([flush_all/2, flush/3, flush/4, clear/3, clear/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if memory should be cleared.
%% @end
%%--------------------------------------------------------------------
-spec should_clear_cache(MemUsage :: number()) -> boolean().
should_clear_cache(MemUsage) ->
  {ok, TargetMemUse} = application:get_env(?CLUSTER_WORKER_APP_NAME, mem_to_clear_cache),
  MemUsage >= TargetMemUse.

%%--------------------------------------------------------------------
%% @doc
%% Clears local cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_local_cache(Aggressive :: boolean()) -> ok | mem_usage_too_high | cannot_check_mem_usage.
clear_local_cache(Aggressive) ->
  clear_cache(Aggressive, locally_cached).

%%--------------------------------------------------------------------
%% @doc
%% Clears local cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_local_cache(MemUsage :: number(), Aggressive :: boolean()) ->
  ok | mem_usage_too_high | cannot_check_mem_usage.
clear_local_cache(MemUsage, Aggressive) ->
  clear_cache(MemUsage, Aggressive, locally_cached).

%%--------------------------------------------------------------------
%% @doc
%% Clears global cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_global_cache(Aggressive :: boolean()) -> ok | mem_usage_too_high | cannot_check_mem_usage.
clear_global_cache(Aggressive) ->
  clear_cache(Aggressive, globally_cached).

%%--------------------------------------------------------------------
%% @doc
%% Clears global cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_global_cache(MemUsage :: number(), Aggressive :: boolean()) ->
  ok | mem_usage_too_high | cannot_check_mem_usage.
clear_global_cache(MemUsage, Aggressive) ->
  clear_cache(MemUsage, Aggressive, globally_cached).

%%--------------------------------------------------------------------
%% @doc
%% Clears cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_cache(Aggressive :: boolean(), StoreType :: globally_cached | locally_cached) ->
  ok | mem_usage_too_high | cannot_check_mem_usage.
clear_cache(Aggressive, StoreType) ->
  case monitoring:get_memory_stats() of
    [{<<"mem">>, MemUsage}] ->
      clear_cache(MemUsage, Aggressive, StoreType);
    _ ->
      ?warning("Not able to check memory usage"),
      cannot_check_mem_usage
  end.

%%--------------------------------------------------------------------
%% @doc
%% Clears cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_cache(MemUsage :: number(), Aggressive :: boolean(), StoreType :: globally_cached | locally_cached) ->
  ok | mem_usage_too_high | cannot_check_mem_usage.
clear_cache(MemUsage, true, StoreType) ->
  {ok, TargetMemUse} = application:get_env(?CLUSTER_WORKER_APP_NAME, mem_to_clear_cache),
  clear_cache(MemUsage, TargetMemUse, StoreType, [timer:minutes(10), 0]);

clear_cache(MemUsage, _, StoreType) ->
  {ok, TargetMemUse} = application:get_env(?CLUSTER_WORKER_APP_NAME, mem_to_clear_cache),
  clear_cache(MemUsage, TargetMemUse, StoreType, [timer:hours(7*24), timer:hours(24), timer:hours(1)]).

%%--------------------------------------------------------------------
%% @doc
%% Clears cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_cache(MemUsage :: number(), TargetMemUse :: number(),
    StoreType :: globally_cached | locally_cached, TimeWindows :: list()) ->
  ok | mem_usage_too_high | cannot_check_mem_usage.
clear_cache(MemUsage, TargetMemUse, _StoreType, _TimeWindows) when MemUsage < TargetMemUse ->
  ok;

clear_cache(_MemUsage, _TargetMemUse, _StoreType, []) ->
  mem_usage_too_high;

clear_cache(_MemUsage, TargetMemUse, StoreType, [TimeWindow | Windows]) ->
  caches_controller:delete_old_keys(StoreType, TimeWindow),
  timer:sleep(1000), % time for system for mem info update
  case monitoring:get_memory_stats() of
    [{<<"mem">>, NewMemUsage}] ->
      clear_cache(NewMemUsage, TargetMemUse, StoreType, Windows);
    _ ->
      ?warning("Not able to check memory usage"),
      cannot_check_mem_usage
  end.

%%--------------------------------------------------------------------
%% @doc
%% Provides hooks configuration on the basis of models list.
%% @end
%%--------------------------------------------------------------------
-spec get_hooks_config(Models :: list()) -> list().
get_hooks_config(Models) ->
  Methods = [save, get, exists, delete, update, create, fetch_link, add_links, delete_links],
  lists:foldl(fun(Model, Ans) ->
    ModelConfig = lists:map(fun(Method) ->
      {Model, Method}
    end, Methods),
    ModelConfig ++ Ans
  end, [], Models).

%%--------------------------------------------------------------------
%% @doc
%% Generates uuid on the basis of key and model name.
%% @end
%%--------------------------------------------------------------------
-spec get_cache_uuid(Key :: datastore:key() | {datastore:ext_key(), datastore:link_name()},
    ModelName :: model_behaviour:model_type()) -> binary().
get_cache_uuid(Key, ModelName) ->
  base64:encode(term_to_binary({ModelName, Key})).

%%--------------------------------------------------------------------
%% @doc
%% Decodes uuid to key and model name.
%% @end
%%--------------------------------------------------------------------
-spec decode_uuid(binary()) -> {Key :: datastore:key(), ModelName :: model_behaviour:model_type()}.
decode_uuid(Uuid) ->
  binary_to_term(base64:decode(Uuid)).

%%--------------------------------------------------------------------
%% @doc
%% Clears old documents from memory.
%% @end
%%--------------------------------------------------------------------
-spec delete_old_keys(StoreType :: globally_cached | locally_cached, TimeWindow :: integer()) -> ok.
delete_old_keys(globally_cached, TimeWindow) ->
  delete_old_keys(global_only, datastore_config:global_caches(), TimeWindow);

delete_old_keys(locally_cached, TimeWindow) ->
  delete_old_keys(local_only, datastore_config:local_caches(), TimeWindow).

%%--------------------------------------------------------------------
%% @doc
%% Clears all documents from memory.
%% @end
%%--------------------------------------------------------------------
-spec delete_all_keys(StoreType :: globally_cached | locally_cached) -> ok | cleared.
delete_all_keys(globally_cached) ->
  delete_all_keys(global_only, datastore_config:global_caches());

delete_all_keys(locally_cached) ->
  delete_all_keys(local_only, datastore_config:local_caches()).

%%--------------------------------------------------------------------
%% @doc
%% Waits for dumping cache to disk
%% @end
%%--------------------------------------------------------------------
-spec wait_for_cache_dump() ->
  ok | dump_error.
wait_for_cache_dump() ->
  {ok, Delay} = application:get_env(?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms),
  wait_for_cache_dump(round(Delay/1000) + 10).

%%--------------------------------------------------------------------
%% @doc
%% Waits for dumping cache to disk
%% @end
%%--------------------------------------------------------------------
-spec wait_for_cache_dump(N :: integer()) ->
  ok | dump_error.
wait_for_cache_dump(0) ->
  dump_error;
wait_for_cache_dump(N) ->
  case {cache_controller:list_docs_to_be_dumped(?GLOBAL_ONLY_LEVEL),
    cache_controller:list_docs_to_be_dumped(?LOCAL_ONLY_LEVEL)} of
    {{ok, []}, {ok, []}} ->
      ok;
    _ ->
      timer:sleep(timer:seconds(1)),
      wait_for_cache_dump(N-1)
  end.

%%--------------------------------------------------------------------
%% @doc
%% Translates cache name to store level.
%% @end
%%--------------------------------------------------------------------
-spec cache_to_datastore_level(ModelName :: atom()) -> datastore:store_level().
cache_to_datastore_level(ModelName) ->
  case lists:member(ModelName, datastore_config:global_caches()) of
    true -> ?GLOBAL_ONLY_LEVEL;
    _ -> ?LOCAL_ONLY_LEVEL
  end.

%%--------------------------------------------------------------------
%% @doc
%% Translates cache name to task level.
%% @end
%%--------------------------------------------------------------------
-spec cache_to_task_level(ModelName :: atom()) -> task_manager:level().
cache_to_task_level(ModelName) ->
  case lists:member(ModelName, datastore_config:global_caches()) of
    true -> ?CLUSTER_LEVEL;
    _ -> ?NODE_LEVEL
  end.

%%--------------------------------------------------------------------
%% @doc
%% Flushes all documents from memory to disk.
%% @end
%%--------------------------------------------------------------------
-spec flush_all(Level :: datastore:store_level(), ModelName :: atom()) -> ok.
flush_all(Level, ModelName) ->
  {ok, Keys} = cache_controller:list_docs_to_be_dumped(Level),
  lists:foreach(fun(Key) ->
    flush(Level, ModelName, Key)
  end, Keys).

%%--------------------------------------------------------------------
%% @doc
%% Flushes links from memory to disk.
%% @end
%%--------------------------------------------------------------------
-spec flush(Level :: datastore:store_level(), ModelName :: atom(),
    Key :: datastore:ext_key(), datastore:link_name() | all) -> ok | datastore:generic_error().
flush(Level, ModelName, Key, all) ->
  ModelConfig = ModelName:model_init(),
  AccFun = fun(LinkName, _, Acc) ->
    [LinkName | Acc]
  end,
  FullArgs = [ModelConfig, Key, AccFun, []],
  {ok, Links} = erlang:apply(datastore:level_to_driver(Level), foreach_link, FullArgs),
  lists:foldl(fun(Link, Acc) ->
    Ans = flush(Level, ModelName, Key, Link),
    case Ans of
      ok ->
        Acc;
      _ ->
        Ans
    end
  end, ok, Links);

flush(Level, ModelName, Key, Link) ->
  flush(Level, ModelName, {Key, Link}).

%%--------------------------------------------------------------------
%% @doc
%% Flushes document from memory to disk.
%% @end
%%--------------------------------------------------------------------
-spec flush(Level :: datastore:store_level(), ModelName :: atom(),
    Key :: datastore:ext_key() | {datastore:ext_key(), datastore:link_name()}) ->
  ok | datastore:generic_error().
flush(Level, ModelName, Key) ->
  ModelConfig = ModelName:model_init(),
  Uuid = get_cache_uuid(Key, ModelName),
  ToDo = cache_controller:choose_action(save, Level, ModelName, Key, Uuid),

  Ans = case ToDo of
          {ok, NewMethod, NewArgs} ->
            FullArgs = [ModelConfig | NewArgs],
            erlang:apply(get_driver_module(?DISK_ONLY_LEVEL), NewMethod, FullArgs);
          {ok, non} ->
            ok;
          Other ->
            Other
        end,

  case Ans of
    {ok, _} ->
      ok;
    OtherAns -> OtherAns
  end.

%%--------------------------------------------------------------------
%% @doc
%% Clears document from memory.
%% @end
%%--------------------------------------------------------------------
-spec clear(Level :: datastore:store_level(), ModelName :: atom(),
    Key :: datastore:ext_key()) -> ok | datastore:generic_error().
clear(Level, ModelName, Key) ->
  ModelConfig = ModelName:model_init(),
  Uuid = get_cache_uuid(Key, ModelName),

  Pred =fun() ->
    case save_clear_info(Level, Uuid) of
      {ok, _} ->
        true;
      _ ->
        false
    end
  end,
  erlang:apply(get_driver_module(Level), delete, [ModelConfig, Key, Pred]).

%%--------------------------------------------------------------------
%% @doc
%% Clears links from memory.
%% @end
%%--------------------------------------------------------------------
-spec clear(Level :: datastore:store_level(), ModelName :: atom(),
    Key :: datastore:ext_key(), datastore:link_name() | all) -> ok | datastore:generic_error().
clear(Level, ModelName, Key, all) ->
  ModelConfig = ModelName:model_init(),
  AccFun = fun(LinkName, _, Acc) ->
    [LinkName | Acc]
  end,
  FullArgs = [ModelConfig, Key, AccFun, []],
  {ok, Links} = erlang:apply(datastore:level_to_driver(Level), foreach_link, FullArgs),
  lists:foldl(fun(Link, Acc) ->
    Ans = clear(Level, ModelName, Key, Link),
    case Ans of
      ok ->
        Acc;
      _ ->
        Ans
    end
  end, ok, Links);

clear(Level, ModelName, Key, Link) ->
  ModelConfig = ModelName:model_init(),
  Uuid = get_cache_uuid({Key, Link}, ModelName),

  Pred = fun() ->
    case save_clear_info(Level, Uuid) of
      {ok, _} ->
        true;
      _ ->
        false
    end
  end,
  erlang:apply(get_driver_module(Level), delete_links, [ModelConfig, Key, [Link], Pred]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Translates datastore level to module name.
%% @end
%%--------------------------------------------------------------------
-spec get_driver_module(Level :: datastore:store_level()) -> atom().
get_driver_module(Level) ->
  datastore:driver_to_module(datastore:level_to_driver(Level)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves information about clearing doc from memory
%% @end
%%--------------------------------------------------------------------
-spec save_clear_info(Level :: datastore:store_level(), Uuid :: binary()) ->
  {ok, datastore:ext_key()} | datastore:create_error().
save_clear_info(Level, Uuid) ->
  UpdateFun = fun(Record) ->
    {ok, Record#cache_controller{action = cleared}}
  end,
  TS = os:timestamp(),
  V = #cache_controller{timestamp = TS, action = cleared, last_action_time = TS},
  Doc = #document{key = Uuid, value = V},

  cache_controller:create_or_update(Level, Doc, UpdateFun).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Clears old documents from memory.
%% @end
%%--------------------------------------------------------------------
-spec delete_old_keys(Level :: global_only | local_only, Caches :: list(), TimeWindow :: integer()) -> ok.
delete_old_keys(Level, Caches, TimeWindow) ->
  {ok, Uuids} = cache_controller:list(Level, TimeWindow),
  lists:foreach(fun(Uuid) ->
    {ModelName, Key} = decode_uuid(Uuid),
    case safe_delete(Level, ModelName, Key) of
      ok ->
        FullArgs = [cache_controller:model_init(), Uuid, ?PRED_ALWAYS],
        erlang:apply(get_driver_module(Level), delete, FullArgs);
      _ ->
        ok
    end
  end, Uuids),
  case TimeWindow of
    0 ->
      lists:foreach(fun(Cache) ->
        {ok, Docs} = datastore:list(Level, Cache, ?GET_ALL, []),
        lists:foreach(fun(Doc) ->
          % TODO - the same for links
          safe_delete(Level, Cache, Doc#document.key)
        end, Docs)
      end, Caches);
    _ ->
      ok
  end,
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes info from memory when it is dumped to disk.
%% @end
%%--------------------------------------------------------------------
-spec safe_delete(Level :: datastore:store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:key() | {datastore:ext_key(), datastore:link_name()}) ->
  ok | datastore:generic_error().
% TODO - uwzglednic linki
safe_delete(Level, ModelName, {Key, Link}) ->
  try
    ModelConfig = ModelName:model_init(),
    FullArgs = [ModelConfig, Key, Link],
    case erlang:apply(get_driver_module(?DISK_ONLY_LEVEL), fetch_link, FullArgs) of
      {ok, Value} ->
        Pred = fun() ->
          case erlang:apply(get_driver_module(Level), fetch_link, FullArgs) of
            {ok, Value} ->
              true;
            _ ->
              false
          end
        end,
        FullArgs2 = [ModelConfig, Key, [Link], Pred],
        erlang:apply(get_driver_module(Level), delete_links, FullArgs2);
      {error, link_not_found} -> ok;
      {error, Reason} ->
        ?error("Error in cache controller safe_delete. Args: ~p. Error: ~p.",
          [{Level, ModelName, Key}, Reason]),
        {error, Reason}
    end
  catch
    E1:E2 ->
      ?error_stacktrace("Error in cache controller safe_delete. "
      ++ "Args: ~p. Error: ~p:~p.", [{Level, ModelName, {Key, Link}}, E1, E2]),
      {error, safe_delete_failed}
  end;
safe_delete(Level, ModelName, Key) ->
  try
    ModelConfig = ModelName:model_init(),
    FullArgs = [ModelConfig, Key],
    case erlang:apply(get_driver_module(?DISK_ONLY_LEVEL), get, FullArgs) of
      {ok, Doc} ->
        Value = Doc#document.value,
        % TODO moze zmienic na sprawdzanie opisu cache (co jak wartosc bedzie taka sama po kilku zapisach z rzedu
        % wtedy nastepny zrzut potraktuje to jako race z delete i wyczysci dysk
        % moze trzeba korzystac z clear a potem czyscic info z cache
        Pred = fun() ->
          case erlang:apply(get_driver_module(Level), get, FullArgs) of
            {ok, Doc2} ->
              Doc2#document.value =:= Value;
            _ ->
              false
          end
        end,
        FullArgs2 = [ModelConfig, Key, Pred],
        erlang:apply(get_driver_module(Level), delete, FullArgs2);
      {error, {not_found, _}} -> ok;
      {error, Reason} ->
        ?error("Error in cache controller safe_delete. Args: ~p. Error: ~p.",
          [{Level, ModelName, Key}, Reason]),
        {error, Reason}
    end
  catch
    E1:E2 ->
      ?error_stacktrace("Error in cache controller safe_delete. "
      ++ "Args: ~p. Error: ~p:~p.", [{Level, ModelName, Key}, E1, E2]),
      {error, safe_delete_failed}
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Clears all documents from memory.
%% @end
%%--------------------------------------------------------------------
-spec delete_all_keys(Level :: global_only | local_only, Caches :: list()) -> ok | cleared.
delete_all_keys(Level, Caches) ->
  {ok, Uuids} = cache_controller:list(Level, 0),
  UuidsNum = length(Uuids),
  lists:foreach(fun(Uuid) ->
    {ModelName, Key} = decode_uuid(Uuid),
    value_delete(Level, ModelName, Key),
    FullArgs = [cache_controller:model_init(), Uuid, ?PRED_ALWAYS],
    erlang:apply(get_driver_module(Level), delete, FullArgs)
  end, Uuids),

  ClearedNum = lists:foldl(fun(Cache, Sum) ->
    {ok, Docs} = datastore:list(Level, Cache, ?GET_ALL, []),
    DocsNum = length(Docs),
    lists:foreach(fun(Doc) ->
      value_delete(Level, Cache, Doc#document.key)
    end, Docs),
    Sum + DocsNum
  end, 0, Caches),

  case UuidsNum + ClearedNum of
    0 ->
      ok;
    _ ->
      cleared
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes info from memory.
%% @end
%%--------------------------------------------------------------------
-spec value_delete(Level :: datastore:store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:key() | {datastore:ext_key(), datastore:link_name()}) ->
  ok | datastore:generic_error().
value_delete(Level, ModelName, {Key, Link}) ->
  try
    ModelConfig = ModelName:model_init(),
    FullArgs2 = [ModelConfig, Key, [Link]],
    erlang:apply(get_driver_module(Level), delete_links, FullArgs2)
  catch
    E1:E2 ->
      ?error_stacktrace("Error in cache controller value_delete. "
      ++ "Args: ~p. Error: ~p:~p.", [{Level, ModelName, {Key, Link}}, E1, E2]),
      {error, delete_failed}
  end;
value_delete(Level, ModelName, Key) ->
  try
    ModelConfig = ModelName:model_init(),
    FullArgs2 = [ModelConfig, Key, ?PRED_ALWAYS],
    erlang:apply(get_driver_module(Level), delete, FullArgs2)
  catch
    E1:E2 ->
      ?error_stacktrace("Error in cache controller value_delete. "
      ++ "Args: ~p. Error: ~p:~p.", [{Level, ModelName, Key}, E1, E2]),
      {error, delete_failed}
  end.
