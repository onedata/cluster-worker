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

-include("modules/datastore/datastore_engine.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create_datastore_context/2, is_link_op/1, validate_model_config/1,
  execute/3, execute_with_default_context/3, execute_with_default_context/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates datastore context using model_config..
%% @end
%%--------------------------------------------------------------------
-spec create_datastore_context(Operation :: atom(), model_behaviour:model_type()
| model_behaviour:model_config()) ->
  datastore:opt_ctx().
create_datastore_context(Operation, TargetMod) ->
  create_datastore_context(Operation, TargetMod, []).

%%--------------------------------------------------------------------
%% @doc
%% Creates datastore context using model_config..
%% @end
%%--------------------------------------------------------------------
-spec create_datastore_context(Operation :: atom(), model_behaviour:model_type()
  | model_behaviour:model_config(), DefaultsOverride :: list()) ->
  datastore:opt_ctx().
create_datastore_context(Operation, #model_config{} = Config0, DefaultsOverride) ->
  #model_config{name = Name, store_level = SL, link_store_level = LSL,
    link_replica_scope = LRS, link_duplication = LD,
    disable_remote_link_delete = DRLD} = Config =
    overide_model(Config0, DefaultsOverride),

  % TODO - single level for model
  Level = case is_link_op(Operation) of
    true -> LSL;
    _ -> SL
  end,

  Driver = datastore:level_to_driver(Level),
  DriverContext = get_default_driver_context(Driver, Operation, Config),
  Ctx = datastore_context:create_context(Name, Level, Driver, DriverContext,
    LRS, LD, DRLD, run_hooks),
  Ctx;
%%  lists:foldl(fun({K, V}, Acc) ->
%%    datastore_context:override(K, V, Acc)
%%  end, Ctx, DefaultsOverride);
create_datastore_context(Operation, TargetMod, DefaultsOverride) ->
  create_datastore_context(Operation, TargetMod:model_init(), DefaultsOverride).

%%--------------------------------------------------------------------
%% @doc
%% Executes function with context.
%% @end
%%--------------------------------------------------------------------
-spec execute(datastore:opt_ctx(), Fun :: atom(), [term()]) ->
  term().
execute(Context, Fun, Args) ->
  apply(datastore, Fun, [Context | Args]).

%%--------------------------------------------------------------------
%% @doc
%% Executes function with default context.
%% @end
%%--------------------------------------------------------------------
-spec execute_with_default_context(model_behaviour:model_type()
  | model_behaviour:model_config(), Fun :: atom(), [term()]) ->
  term().
execute_with_default_context(Model, Fun, Args) ->
  Context = create_datastore_context(Fun, Model),
  execute(Context, Fun, Args).

%%--------------------------------------------------------------------
%% @doc
%% Executes function with default context.
%% @end
%%--------------------------------------------------------------------
-spec execute_with_default_context(model_behaviour:model_type()
  | model_behaviour:model_config(), Fun :: atom(), [term()],
  DefaultsOverride :: list()) -> term().
execute_with_default_context(Model, Fun, Args, DefaultsOverride) ->
  Context = create_datastore_context(Fun, Model, DefaultsOverride),
  execute(Context, Fun, Args).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether operation is link operation.
%% @end
%%--------------------------------------------------------------------
-spec is_link_op(Operation :: atom()) -> boolean().
is_link_op(Operation) ->
  string:str(atom_to_list(Operation), "link") > 0.

%%--------------------------------------------------------------------
%% @doc
%% Validates model's configuration.
%% @end
%%--------------------------------------------------------------------
-spec validate_model_config(model_behaviour:model_config()) -> ok | no_return().
validate_model_config(#model_config{version = CurrentVersion, name = ModelName, store_level = StoreLevel}) ->
  case lists:member(?PERSISTENCE_DRIVER, lists:flatten([datastore:level_to_driver(StoreLevel)])) of
    false -> ok;
    true ->
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

get_default_driver_context(?PERSISTENCE_DRIVER, _Op, Config) ->
  Config;
get_default_driver_context(Driver, Op, Config) ->
  Module = datastore:driver_to_module(Driver),
  apply(Module, get_default_context, [Op, Config]).

overide_model(ModelConfig, []) ->
  ModelConfig;
overide_model(ModelConfig, [{level, L} | Override]) ->
  overide_model(ModelConfig#model_config{store_level = L, link_store_level = L}, Override);
overide_model(ModelConfig, [{link_replica_scope, LRS} | Override]) ->
  overide_model(ModelConfig#model_config{link_replica_scope = LRS}, Override);
overide_model(ModelConfig, [{link_duplication, LD} | Override]) ->
  overide_model(ModelConfig#model_config{link_duplication = LD}, Override);
overide_model(ModelConfig, [{disable_remote_link_delete, DRLD} | Override]) ->
  overide_model(ModelConfig#model_config{disable_remote_link_delete = DRLD}, Override).

% couch ctx
%%make_ctx(ModelConfig) ->
%%  make_ctx(ModelConfig, undefined).
%%make_ctx(ModelConfig, #document2{key = Key}) ->
%%  make_ctx(ModelConfig, Key);
%%make_ctx(#model_config{name = Name, sync_enabled = true}, ?NOSYNC_WRAPPED_KEY_OVERRIDE(_)) ->
%%  #{prefix => atom_to_binary(Name, utf8), bucket => <<"default">>};
%%make_ctx(#model_config{name = Name, sync_enabled = true}, Key) when is_binary(Key) ->
%%  case binary:match(Key, ?NOSYNC_KEY_OVERRIDE_PREFIX) of
%%    nomatch ->
%%      #{prefix => atom_to_binary(Name, utf8), bucket => <<"sync">>};
%%    _ ->
%%      #{prefix => atom_to_binary(Name, utf8), bucket => <<"default">>}
%%  end;
%%make_ctx(#model_config{name = Name, sync_enabled = true}, _Key) ->
%%  #{prefix => atom_to_binary(Name, utf8), bucket => <<"sync">>};
%%make_ctx(#model_config{name = Name}, _Key) ->
%%  #{prefix => atom_to_binary(Name, utf8), bucket => <<"default">>}.