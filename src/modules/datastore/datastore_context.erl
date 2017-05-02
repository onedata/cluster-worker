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
  get_driver_context/1, create_context/10, override/3, get_resolve_conflicts/1,
  get_bucket/1]).

% TODO - define map better when its structure is stable
% (after integration of new drivers)
-type ctx() :: #{atom() => term()}.
% TODO - change to map
-type driver_ctx() :: term().
-type hooks_config() :: run_hooks | no_hooks.
-type resolve_conflicts() :: doc | {links, DocKey :: datastore:ext_key()} | false.
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
    Hooks :: hooks_config(), ResolveConflicts :: resolve_conflicts(),
    Bucket :: couchdb_datastore_driver:couchdb_bucket() | default) -> ctx().
% TODO - migrate some parameters to driver context when drivers use ctx
create_context(ModelName, Level, Driver, DriverCtx, LRS, LD, DRLD, Hooks,
    ResolveConflicts, Bucket) ->
  #{
    model_name => ModelName,
    level => Level,
    driver => Driver,
    driver_ctx => DriverCtx,
    link_replica_scope => LRS,
    link_duplication => LD,
    disable_remote_link_delete => DRLD,
    hooks_config => Hooks,
    resolve_conflicts => ResolveConflicts,
    bucket => Bucket
  }.

%%--------------------------------------------------------------------
%% @doc
%% Overrides context parameter. To be used only by model.erl.
%% @end
%%--------------------------------------------------------------------
-spec override(Key :: atom(), Value :: term(), ctx()) -> ctx().
override(Key, Value, Ctx) ->
  maps:put(Key, Value, Ctx).

%%--------------------------------------------------------------------
%% @doc
%% Returns model name.
%% @end
%%--------------------------------------------------------------------
-spec get_model_name(ctx()) -> model_behaviour:model_type().
get_model_name(#{model_name := ToReturn}) -> ToReturn.

%%--------------------------------------------------------------------
%% @doc
%% Returns level.
%% @end
%%--------------------------------------------------------------------
-spec get_level(ctx()) -> datastore:store_level().
get_level(#{level := ToReturn}) -> ToReturn.

%%--------------------------------------------------------------------
%% @doc
%% Returns model name.
%% @end
%%--------------------------------------------------------------------
-spec get_driver(ctx()) -> atom().
get_driver(#{driver := ToReturn}) -> ToReturn.

%%--------------------------------------------------------------------
%% @doc
%% Returns hooks config.
%% @end
%%--------------------------------------------------------------------
-spec get_hooks_config(ctx()) -> hooks_config().
get_hooks_config(#{hooks_config := ToReturn}) -> ToReturn.

%%--------------------------------------------------------------------
%% @doc
%% Returns links duplication settings.
%% @end
%%--------------------------------------------------------------------
-spec get_link_duplication(ctx()) -> boolean().
get_link_duplication(#{link_duplication := ToReturn}) -> ToReturn.

%%--------------------------------------------------------------------
%% @doc
%% Returns links replica scope.
%% @end
%%--------------------------------------------------------------------
-spec get_link_replica_scope(ctx()) -> links_utils:link_replica_scope().
get_link_replica_scope(#{link_replica_scope := ToReturn}) -> ToReturn.

%%--------------------------------------------------------------------
%% @doc
%% Returns remote delete settings.
%% @end
%%--------------------------------------------------------------------
-spec get_disable_remote_link_delete(ctx()) -> boolean().
get_disable_remote_link_delete(#{disable_remote_link_delete := ToReturn}) ->
  ToReturn.

%%--------------------------------------------------------------------
%% @doc
%% Returns driver context.
%% @end
%%--------------------------------------------------------------------
-spec get_driver_context(ctx()) -> driver_ctx().
get_driver_context(#{driver_ctx := ToReturn}) -> ToReturn.

%%--------------------------------------------------------------------
%% @doc
%% Returns driver context.
%% @end
%%--------------------------------------------------------------------
-spec get_resolve_conflicts(ctx()) -> resolve_conflicts().
get_resolve_conflicts(#{resolve_conflicts := ToReturn}) -> ToReturn.

%%--------------------------------------------------------------------
%% @doc
%% Returns driver context.
%% @end
%%--------------------------------------------------------------------
-spec get_bucket(ctx()) -> couchdb_datastore_driver:couchdb_bucket() | default.
get_bucket(#{bucket := ToReturn}) -> ToReturn.