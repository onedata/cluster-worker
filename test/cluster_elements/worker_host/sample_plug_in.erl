%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour for testing 
%% purposes.
%% @end
%% ===================================================================

-module(sample_plug_in).
-behaviour(worker_plugin_behaviour).

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

init(_Args) ->
	[].

handle(_ProtocolVersion, {long_request, Time, Id, Pid}=_Msg) ->
    {_, S, M} = now(),
    receive after Time -> Pid ! {Id, S * 1000000 + M} end,
    ok;

handle(_ProtocolVersion, _Msg) ->
	ok.

cleanup() ->
	ok.
