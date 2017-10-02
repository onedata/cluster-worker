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
-export([get_models/0, get_throttled_models/0]).

-type model() :: datastore_model:model().

-define(DEFAULT_MODELS, [
    cached_identity,
    links_forest,
    links_mask,
    links_mask_root,
    links_node,
    lock,
    node_management,
    synced_cert,
    task_pool,
    gs_session,
    gs_subscription
]).

-define(PLUGIN, datastore_config_plugin).

%%%===================================================================
%%% API
%%%===================================================================

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
apply_plugin(Function, Args, Default) ->
    try
        Exports = ?PLUGIN:module_info(functions),
        Arity = length(Args),
        case lists:keyfind(Function, 1, Exports) of
            {Function, Arity} -> erlang:apply(?PLUGIN, Function, Args);
            _ -> Default
        end
    catch
        _:_ ->
            case code:ensure_loaded(?PLUGIN) of
                {module, ?PLUGIN} ->
                    apply_plugin(Function, Args, Default);
                {error, nofile} ->
                    Default
            end
    end.