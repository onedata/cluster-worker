%%%--------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Utility module for launching plugin callbacks.
%%% @end
%%%--------------------------------------------------------------------
-module(plugins).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([apply/3]).

-type plugin() :: atom().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Executes plugin callback or fallbacks to the default implementation.
%% @end
%%--------------------------------------------------------------------
-spec apply(module(), atom(), list()) -> term().
apply(Plugin, Function, Args) ->
    {Module, DefaultModule} = get_plugin_module(Plugin),
    Exports = Module:module_info(functions),
    Arity = length(Args),
    case lists:keyfind(Function, 1, Exports) of
        {Function, Arity} ->
            erlang:apply(Module, Function, Args);
        _ ->
            ?debug("Using default implementation for plugin call ~tp:~tp/~tp.",
                [Plugin, Function, length(Args)]),
            erlang:apply(DefaultModule, Function, Args)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns pair of custom and default plugin module.
%% @end
%%--------------------------------------------------------------------
-spec get_plugin_module(plugin()) -> {module(), module()}.
get_plugin_module(Plugin) ->
    DefaultModule = list_to_atom(atom_to_list(Plugin) ++ "_default"),
    case application:get_env(?CLUSTER_WORKER_APP_NAME, Plugin) of
        {ok, Module} ->
            {Module, DefaultModule};
        undefined ->
            ?debug("Module not found for a plugin: ~tp. Defaulting to ~tp.",
                [Plugin, DefaultModule]),
            {DefaultModule, DefaultModule}
    end.