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

%% API
-export([create_datastore_context/2, is_link_op/1, validate_model_config/1,
  execute/3, execute_with_default_context/3]).

%%%===================================================================
%%% API
%%%===================================================================

create_datastore_context(Operation, #model_config{store_level = SL,
  link_store_level = LSL, link_replica_scope = LRS, link_duplication = LD,
  disable_remote_link_delete = DRLD} = Config) ->
  % TODO - single level for model
  Level = case is_link_op(Operation) of
    true -> LSL;
    _ -> SL
  end,

  Driver = datastore:level_to_driver(Level),
  DriverContext = get_default_driver_context(Driver, Operation, Config),
  datastore_context:create_context(Level, Driver, DriverContext,
    LRS, LD, DRLD, true);
create_datastore_context(Operation, TargetMod) ->
  create_datastore_context(Operation, TargetMod:model_init()).

execute(Context, Fun, Args) ->
  apply(datastore, Fun, [Context | Args]).

execute_with_default_context(Model, Fun, Args) ->
  Context = create_datastore_context(Fun, Model),
  execute(Context, Fun, Args).

is_link_op(Operation) ->
  string:str(atom_to_list(Operation), "link") > 0.

%%--------------------------------------------------------------------
%% @doc
%% Validates model's configuration.
%% @end
%%--------------------------------------------------------------------
-spec validate_model_config(model_behaviour:model_config()) -> ok | no_return().
validate_model_config(#model_config{version = CurrentVersion, name = ModelName, store_level = StoreLevel}) ->
  case lists:member(?PERSISTENCE_DRIVER, lists:flatten([level_to_driver(StoreLevel)])) of
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

get_default_driver_context(?PERSISTENCE_DRIVER, _Op, Config) ->
  Config;
get_default_driver_context(Driver, Op, Config) ->
  Module = datastore:driver_to_module(Driver),
  apply(Module, get_default_context, [Op, Config]).
  Config.
