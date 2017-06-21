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
-export([create_context/10, override/3]).

% TODO - define map better when its structure is stable
% (after integration of new drivers)
-type ctx() :: #{atom() => term()}.
-type hooks_config() :: run_hooks | no_hooks.
-type resolve_conflicts() :: boolean().
-type links_tree() :: {true, DocKey :: datastore:ext_key()} | false.
% Listing config: if listing if on (true), it can return errors when
% saving of information needed to emulate listing fails or this
% information can be save asynchronously.
-type list_opt() :: {true, return_errors | async} | false.
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
    Level :: datastore:store_level(), LRS :: links_utils:link_replica_scope(),
    LD :: boolean(), DRLD :: boolean(),
    Hooks :: hooks_config(), ResolveConflicts :: resolve_conflicts(),
    links_tree(), list_opt(), Volatile :: boolean()) -> ctx().
create_context(ModelName, Level, LRS, LD, DRLD, Hooks,
    ResolveConflicts, LinkOp, LE, Volatile) ->
  #{
    model_name => ModelName,
    level => Level,
    link_replica_scope => LRS,
    link_duplication => LD,
    disable_remote_link_delete => DRLD,
    hooks_config => Hooks,
    resolve_conflicts => ResolveConflicts,
    links_tree => LinkOp,
    list_enabled => LE,
    volatile => Volatile
  }.

%%--------------------------------------------------------------------
%% @doc
%% Overrides context parameter. To be used only by model.erl.
%% @end
%%--------------------------------------------------------------------
-spec override(Key :: atom(), Value :: term(), ctx()) -> ctx().
override(Key, Value, Ctx) ->
  maps:put(Key, Value, Ctx).


