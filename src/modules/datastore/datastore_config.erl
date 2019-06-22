%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides datastore configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_config).
-author("Krzysztof Trzepla").

%% API
-export([init/0, get_models/0, get_throttled_models/0]).

-type model() :: datastore_model:model().

-define(DEFAULT_MODELS, [
    links_forest,
    links_mask,
    links_mask_root,
    links_node,
    lock,
    node_management,
    task_pool,
    gs_session,
    gs_subscription,
    traverse_task,
    traverse_tasks_scheduler
]).

-define(PLUGIN, datastore_config_plugin).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Ensures that datastore_config_plugin is loaded, if existent.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    code:ensure_loaded(?PLUGIN),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of default and custom models.
%% @end
%%--------------------------------------------------------------------
-spec get_models() -> [model()].
get_models() ->
    ?DEFAULT_MODELS ++ apply_plugin(get_models, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of throttled models.
%% @end
%%--------------------------------------------------------------------
-spec get_throttled_models() -> [model()].
get_throttled_models() ->
    apply_plugin(get_throttled_models, [], []).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls plugin function if provided or returns default value.
%% @end
%%--------------------------------------------------------------------
-spec apply_plugin(atom(), list(), term()) -> term().
apply_plugin(Callback, Args, Default) ->
    Arity = length(Args),
    case erlang:function_exported(?PLUGIN, Callback, Arity) of
        true -> erlang:apply(?PLUGIN, Callback, Args);
        false -> Default
    end.
