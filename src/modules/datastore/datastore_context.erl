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
-export([get_level/1, get_driver/1, get_hooks_config/1, get_link_duplication/1,
  get_link_replica_scope/1, get_disable_remote_link_delete/1, create_context/7,
  get_driver_context/2]).

-type ctx() :: #{}.

-export_type([ctx/0]).

%%%===================================================================
%%% API
%%%===================================================================

create_context(Level, Driver, DriverCtx, LRS, LD, DRLD, Hooks) ->
  C1 = maps:put(level, Level, #{}),
  C2 = maps:put(driver, Driver, C1),
  C3 = maps:put(driver_ctx, DriverCtx, C2),
  C4 = maps:put(link_replica_scope, LRS, C3),
  C5 = maps:put(link_duplication, LD, C4),
  C6 = maps:put(disable_remote_link_delete, DRLD, C5),
  maps:put(validate_model_config, Hooks, C6).

get_level(OptCtx) ->
  maps:get(level, OptCtx).

get_driver(OptCtx) ->
  maps:get(driver, OptCtx).

get_hooks_config(OptCtx) ->
  maps:get(hooks_config, OptCtx).

get_link_duplication(OptCtx) ->
  maps:get(link_duplication, OptCtx).

get_link_replica_scope(OptCtx) ->
  maps:get(link_replica_scope, OptCtx).

get_disable_remote_link_delete(OptCtx) ->
  maps:get(disable_remote_link_delete, OptCtx).

get_driver_context(OptCtx, Driver) ->
  maps:get({driver_ctx, Driver}, OptCtx).