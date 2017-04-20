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
-module(datastore_context).
-author("Michal Wrzeszcz").

%% API
-export([get_model_name/1, get_level/1, get_driver/1, get_hooks_config/1,
  get_link_duplication/1, get_link_replica_scope/1, get_disable_remote_link_delete/1,
  get_driver_context/1, create_context/8, override/3]).

-type ctx() :: #{atom() => term()}.
% TODO - change to map
-type driver_ctx() :: term().

-export_type([ctx/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates datastore context.
%% @end
%%--------------------------------------------------------------------
-spec create_context(ModelName :: model_behaviour:model_type(),
    Level :: datastore:store_level(), Driver :: atom(), DriverCtx :: driver_ctx(),
    LRS :: links_utils:link_replica_scope(), LD :: boolean(), DRLD :: boolean(),
    Hooks :: boolean()) -> ctx().
create_context(ModelName, Level, Driver, DriverCtx, LRS, LD, DRLD, Hooks) ->
  C0 = maps:put(model_name, ModelName, #{}),
  C1 = maps:put(level, Level, C0),
  C2 = maps:put(driver, Driver, C1),
  C3 = maps:put(driver_ctx, DriverCtx, C2),
  C4 = maps:put(link_replica_scope, LRS, C3),
  C5 = maps:put(link_duplication, LD, C4),
  C6 = maps:put(disable_remote_link_delete, DRLD, C5),
  maps:put(hooks_config, Hooks, C6).

%%--------------------------------------------------------------------
%% @doc
%% Overrides context parameter. To be used only by model.erl.
%% @end
%%--------------------------------------------------------------------
-spec override(Key :: atom(), Value :: term(), ctx) -> ctx().
override(Key, Value, Ctx) ->
  maps:put(Key, Value, Ctx).

%%--------------------------------------------------------------------
%% @doc
%% Returns model name.
%% @end
%%--------------------------------------------------------------------
-spec get_model_name(ctx()) -> model_behaviour:model_type().
get_model_name(OptCtx) ->
  maps:get(model_name, OptCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns level.
%% @end
%%--------------------------------------------------------------------
-spec get_level(ctx()) -> datastore:store_level().
get_level(OptCtx) ->
  maps:get(level, OptCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns model name.
%% @end
%%--------------------------------------------------------------------
-spec get_driver(ctx()) -> atom().
get_driver(OptCtx) ->
  maps:get(driver, OptCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns hooks config.
%% @end
%%--------------------------------------------------------------------
-spec get_hooks_config(ctx()) -> boolean().
get_hooks_config(OptCtx) ->
  maps:get(hooks_config, OptCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns links duplication settings.
%% @end
%%--------------------------------------------------------------------
-spec get_link_duplication(ctx()) -> boolean().
get_link_duplication(OptCtx) ->
  maps:get(link_duplication, OptCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns links replica scope.
%% @end
%%--------------------------------------------------------------------
-spec get_link_replica_scope(ctx()) -> links_utils:link_replica_scope().
get_link_replica_scope(OptCtx) ->
  maps:get(link_replica_scope, OptCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns remote delete settings.
%% @end
%%--------------------------------------------------------------------
-spec get_disable_remote_link_delete(ctx()) -> boolean().
get_disable_remote_link_delete(OptCtx) ->
  maps:get(disable_remote_link_delete, OptCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns driver context.
%% @end
%%--------------------------------------------------------------------
-spec get_driver_context(ctx()) -> driver_ctx().
get_driver_context(OptCtx) ->
  maps:get(driver_ctx, OptCtx).