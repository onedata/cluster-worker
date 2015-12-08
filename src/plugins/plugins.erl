%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Utility module for launching plugin callbacks.
%%% @end
%%%--------------------------------------------------------------------
-module(plugins).
-author("Michal Zmuda").

-include_lib("ctool/include/logging.hrl").
-include("global_definitions.hrl").

%% API
-export([apply/3]).

%%--------------------------------------------------------------------
%% @doc
%% Executes plugin callback if it is present. Otherwise just return ok.
%% @end
%%--------------------------------------------------------------------
-spec apply(PluginName, Function, Args) -> term() when
  PluginName :: module(),
  Function :: atom(),
  Args :: [term()].

apply(PluginName, Name, Args) ->
  ?error("WTF ~p", [[PluginName, Name, Args, application:get_env(?APP_NAME, PluginName)]]),
  case application:get_env(?APP_NAME, PluginName) of
    undefined ->
      Default = defaults_plugin_name(PluginName),
      ?error("plugin known as '~p' has no module defined - defaulting to ~p", [PluginName, Default]),
      erlang:apply(Default, Name, Args);
    {ok, Module} ->
      erlang:apply(Module, Name, Args)
  end.

defaults_plugin_name(PluginName) ->
  list_to_atom(atom_to_list(PluginName) ++ "_default").
