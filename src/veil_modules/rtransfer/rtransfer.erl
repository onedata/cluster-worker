%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour to provide
%% remote transfer manager functionality (gateways' management).
%% @end
%% ===================================================================

-module(rtransfer).
-behaviour(worker_plugin_behaviour).

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

init(_Args) ->
	[].

handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, get_version) ->
  1;

handle(_ProtocolVersion, _Msg) ->
	ok.

cleanup() ->
	ok.
