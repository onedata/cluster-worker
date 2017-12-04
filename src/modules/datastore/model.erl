%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module used for datastore context operations.
%%% @end
%%%-------------------------------------------------------------------
-module(model).
-author("Michal Wrzeszcz").

-include("exometer_utils.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

-type metchod() :: atom().

%% API
-export([create_datastore_context/3, create_datastore_context/4, is_link_op/1,
  execute/3, execute_with_default_context/3, execute_with_default_context/4,
  make_memory_ctx/2, make_disk_ctx/1, make_disk_ctx/3, validate_model_config/1]).

-define(EXOMETER_NAME(Param), ?exometer_name(datastore,
  list_to_atom(atom_to_list(Param) ++ "_time"))).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates datastore context using model_config..
%% @end
%%--------------------------------------------------------------------
-spec create_datastore_context(Method :: metchod(), Args :: list(),
    model_behaviour:model_type() | model_behaviour:model_config()) ->
  datastore:ctx().
create_datastore_context(Method, Args, TargetMod) ->
  create_datastore_context(Method, Args, TargetMod, []).

%%--------------------------------------------------------------------
%% @doc
%% Creates datastore context using model_config..
%% @end
%%--------------------------------------------------------------------
-spec create_datastore_context(Method :: metchod(), Args :: list(),
    model_behaviour:model_type() | model_behaviour:model_config(),
    DefaultsOverride :: list()) -> datastore:ctx().
create_datastore_context(Method, Args, #model_config{name = Name, store_level = SL,
  link_store_level = LSL, link_replica_scope = LRS, link_duplication = LD,
  disable_remote_link_delete = DRLD, list_enabled = LE, volatile = Volatile} = Config, DefaultsOverride) ->
  % TODO - single level for model
  LinkOp = is_link_op(Method),
  Level = case LinkOp of
    true -> LSL;
    _ -> SL
  end,

  Ctx = datastore_context:create_context(Name, Level,
    LRS, LD, DRLD, run_hooks, false, LinkOp, LE, Volatile),
  #{level := FinalLevel, resolve_conflicts := RC} = Ctx2 = lists:foldl(fun
    ({K, V}, Acc) -> datastore_context:override(K, V, Acc);
    ({_D, _K, _V}, Acc) -> Acc
  end, Ctx, DefaultsOverride),

  Driver = level_to_driver(FinalLevel),
  Key = get_key(Method, Args),
  DriverContext = get_default_driver_context(FinalLevel, Config, Key, RC),
  DriverContext2 = lists:foldl(fun
    ({_K, _V}, Acc) -> Acc;
    ({D, K, V}, Acc) ->
      DCtx = maps:get(D, Acc, #{}),
      datastore_context:override(D, datastore_context:override(K, V, DCtx), Acc)
  end, DriverContext, DefaultsOverride),
  maps:merge(maps:put(driver, Driver, DriverContext2), Ctx2);
create_datastore_context(Method, Args, TargetMod, DefaultsOverride) ->
  create_datastore_context(Method, Args, TargetMod:model_init(), DefaultsOverride).

%%--------------------------------------------------------------------
%% @doc
%% Executes function with context.
%% @end
%%--------------------------------------------------------------------
-spec execute(datastore:ctx(), Fun :: atom(), [term()]) ->
  term().
execute(Context, Fun, Args) ->
  {Time, Ans} = timer:tc(erlang, apply,
    [datastore, Fun, [Context | Args]]),
  ?update_counter(?EXOMETER_NAME(Fun), Time),
  Ans.

%%--------------------------------------------------------------------
%% @doc
%% Executes function with default context.
%% @end
%%--------------------------------------------------------------------
-spec execute_with_default_context(model_behaviour:model_type()
  | model_behaviour:model_config(), Fun :: atom(), [term()]) ->
  term().
execute_with_default_context(Model, Fun, Args) ->
  execute_with_default_context(Model, Fun, Args, []).

%%--------------------------------------------------------------------
%% @doc
%% Executes function with default context.
%% @end
%%--------------------------------------------------------------------
-spec execute_with_default_context(model_behaviour:model_type()
  | model_behaviour:model_config(), Fun :: atom(), [term()],
  DefaultsOverride :: list()) -> term().
execute_with_default_context(#model_config{} = Model, Fun, Args,
    DefaultsOverride) ->
  Context = create_datastore_context(Fun, Args, Model, DefaultsOverride),
  {Time, Ans} = timer:tc(erlang, apply,
    [datastore, Fun, [Context | set_version(Args, Model)]]),
  ?update_counter(?EXOMETER_NAME(Fun), Time),
  Ans;
execute_with_default_context(Model, Fun, Args, DefaultsOverride) ->
  execute_with_default_context(Model:model_init(), Fun, Args, DefaultsOverride).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether method is link operation.
%% @end
%%--------------------------------------------------------------------
-spec is_link_op(Method :: metchod()) -> boolean().
is_link_op(Method) ->
  string:str(atom_to_list(Method), "link") > 0.

%%--------------------------------------------------------------------
%% @doc
%% Validates model's configuration.
%% @end
%%--------------------------------------------------------------------
-spec validate_model_config(model_behaviour:model_config()) -> ok | no_return().
validate_model_config(#model_config{version = CurrentVersion, name = ModelName, store_level = StoreLevel}) ->
  case get_persistence(StoreLevel) of
    {false, _} -> ok;
    _ ->
      case lists:member({record_struct, 1}, ModelName:module_info(exports)) of
        true ->
          try
            %% Check all versions up to CurrentVersion
            [datastore_json:validate_struct(ModelName:record_struct(Version))
              || Version <- lists:seq(1, CurrentVersion)],
            HasUpdater = lists:member({record_upgrade, 2}, ModelName:module_info(exports))
              orelse CurrentVersion == 1,
            case HasUpdater of
              true -> ok;
              false ->
                error({no_record_updater, CurrentVersion, ModelName})
            end,
            ok
          catch
            _:Reason ->
              ?error_stacktrace("Unable to validate record version for model ~p due to ~p", [ModelName, Reason]),
              error({invalid_record_version, CurrentVersion, ModelName})
          end;
        false ->
          error({no_struct_def, ModelName})
      end
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets default context for driver.
%% @end
%%--------------------------------------------------------------------
-spec get_default_driver_context(atom(), model_behaviour:model_config(),
    datastore:ext_key(), ResolveConflict :: boolean()) ->
  datastore_context:driver_ctx().
get_default_driver_context(?DIRECT_DISK_LEVEL, Config, Key, RC) ->
  make_disk_ctx(Config, Key, RC);
get_default_driver_context(Level, #model_config{name = Name} = Config, Key, RC) ->
  {DiskDriver, DiskCtx} = level_to_disk_config(Level, Config, Key, RC),
  {MemoryDriver, MemoryCtx} = level_to_memory_config(Level, Config),
  {Persistence, GetMethod} = get_persistence(Level),
  Locality = get_locality_settings(Level),
  #{prefix => atom_to_binary(Name, utf8),
    memory_driver => MemoryDriver,
    memory_driver_ctx => MemoryCtx,
    disc_driver => DiskDriver,
    disc_driver_ctx => DiskCtx,
    persistence => Persistence,
    get_method => GetMethod,
    locality => Locality,
    generated_uuid => false}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Translates store level into driver's name.
%% @end
%%--------------------------------------------------------------------
-spec level_to_driver(Level :: datastore:store_level()) -> Driver :: atom().
level_to_driver(?DIRECT_DISK_LEVEL) ->
  ?PERSISTENCE_DRIVER;
level_to_driver(_) ->
  ?MEMORY_DRIVER.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Translates store level and config into pair {driver_name, context}
%% that describe disk config.
%% @end
%%--------------------------------------------------------------------
-spec level_to_disk_config(Level :: datastore:store_level(),
    model_behaviour:model_config(), datastore:ext_key(),
    ResolveConflict :: boolean()) ->
  {Driver :: atom(), datastore_context:driver_ctx()}.
level_to_disk_config(?GLOBALLY_CACHED_LEVEL, Config, Key, RC) ->
  {?PERSISTENCE_DRIVER, maps:put(no_rev, true, make_disk_ctx(Config, Key, RC))};
level_to_disk_config(?LOCALLY_CACHED_LEVEL, Config, Key, RC) ->
  {?PERSISTENCE_DRIVER, maps:put(no_rev, true, make_disk_ctx(Config, Key, RC))};
level_to_disk_config(?DISK_ONLY_LEVEL, Config, Key, RC) ->
  {?PERSISTENCE_DRIVER, make_disk_ctx(Config, Key, RC)};
level_to_disk_config(_, _, _, _) ->
  {undefined, #{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Translates store level and config into pair {driver_name, context}
%% that describe disk config.
%% @end
%%--------------------------------------------------------------------
-spec level_to_memory_config(Level :: datastore:store_level(),
    model_behaviour:model_config()) ->
  {Driver :: atom(), datastore_context:driver_ctx()}.
level_to_memory_config(?GLOBALLY_CACHED_LEVEL, Config) ->
  {?GLOBAL_SLAVE_DRIVER, make_memory_ctx(Config, false)};
level_to_memory_config(?GLOBAL_ONLY_LEVEL, Config) ->
  {?GLOBAL_SLAVE_DRIVER, make_memory_ctx(Config, false)};
level_to_memory_config(?DISK_ONLY_LEVEL, _) ->
  {undefined, #{}};
level_to_memory_config(_, Config) ->
  {?LOCAL_SLAVE_DRIVER, make_memory_ctx(Config, true)}.

%%--------------------------------------------------------------------
%% @doc
%% Creates context for disk driver.
%% @end
%%--------------------------------------------------------------------
-spec make_disk_ctx(model_behaviour:model_config()) ->
  datastore_context:driver_ctx().
make_disk_ctx(ModelConfig) ->
  make_disk_ctx(ModelConfig, undefined, false).

%%--------------------------------------------------------------------
%% @doc
%% Creates context for disk driver.
%% @end
%%--------------------------------------------------------------------
-spec make_disk_ctx(model_behaviour:model_config(),
    datastore:document() | datastore:ext_key(), ResolveConflict :: boolean()) ->
  datastore_context:driver_ctx().
make_disk_ctx(ModelConfig, #document{key = Key}, RC) ->
  make_disk_ctx(ModelConfig, Key, RC);
make_disk_ctx(#model_config{name = Name, sync_enabled = true}, undefined, RC) ->
  set_mutator(#{prefix => atom_to_binary(Name, utf8), bucket => <<"onedata">>}, RC);
make_disk_ctx(#model_config{name = Name, sync_enabled = true}, Key, RC) ->
  case binary:match(Key, ?NOSYNC_KEY_OVERRIDE_PREFIX) of
    nomatch ->
      set_mutator(#{prefix => atom_to_binary(Name, utf8), bucket => <<"onedata">>}, RC);
    _ ->
      set_mutator(#{prefix => atom_to_binary(Name, utf8),
        bucket => <<"onedata">>, no_seq => true}, RC)
  end;
make_disk_ctx(#model_config{name = Name}, _Key, RC) ->
  set_mutator(#{prefix => atom_to_binary(Name, utf8),
    bucket => <<"onedata">>, no_seq => true}, RC).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates context for memory driver.
%% @end
%%--------------------------------------------------------------------
-spec make_memory_ctx(model_behaviour:model_config(), Local :: boolean()) ->
  datastore_context:driver_ctx().
make_memory_ctx(ModelConfig, false) ->
  #{table => list_to_atom("global_" ++
  atom_to_list(ModelConfig#model_config.name))};
make_memory_ctx(ModelConfig, _) ->
  #{table => list_to_atom("local_" ++
  atom_to_list(ModelConfig#model_config.name))}.

%%--------------------------------------------------------------------
%% @doc
%% Gets persistence info.
%% @end
%%--------------------------------------------------------------------
-spec get_persistence(datastore:store_level()) ->
  {Persistence :: boolean(), GetMethod :: atom()}.
get_persistence(Level) ->
  case Level of
    ?GLOBALLY_CACHED_LEVEL ->
      {true, fetch};
    ?LOCALLY_CACHED_LEVEL ->
      {true, fetch};
    ?DISK_ONLY_LEVEL ->
      {true, fetch};
    _ ->
      {false, get}
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets locality settings.
%% @end
%%--------------------------------------------------------------------
-spec get_locality_settings(datastore:store_level()) -> atom().
get_locality_settings(Level) ->
  case Level of
    ?LOCAL_ONLY_LEVEL ->
      local;
    ?LOCALLY_CACHED_LEVEL ->
      local;
    _ ->
      global
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets mutator in context.
%% @end
%%--------------------------------------------------------------------
-spec set_mutator(Ctx :: datastore_context:driver_ctx(),
    ResolveConflict :: boolean()) -> datastore_context:driver_ctx().
set_mutator(Ctx, true) ->
  Ctx;
set_mutator(Ctx, _) ->
  case plugins:apply(datastore_config_plugin, get_mutator, []) of
    undefined -> Ctx;
    M -> datastore_context:override(mutator, M, Ctx)
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets document version.
%% @end
%%--------------------------------------------------------------------
-spec set_version(Args :: list(), model_behaviour:model_config()) ->
  NewArgs :: list().
set_version([#document{} = Doc | Args], #model_config{version = V}) ->
  [Doc#document{version = V} | Args];
set_version(Args, _ModelConfig) ->
  Args.

%%--------------------------------------------------------------------
%% @doc
%% Gets key of document that is subject of method.
%% @end
%%--------------------------------------------------------------------
-spec get_key(Function :: metchod(), Args :: [term()]) ->
  term().
get_key(list, _Args) ->
  undefined;
get_key(_, [#document{key = Key} | _]) ->
  Key;
get_key(_, [Key | _]) ->
  Key.