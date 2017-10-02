%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This behaviour defines callbacks that must be implemented by translator
%%% plugin module that is used to translate request results into format
%%% understood by client.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_translator_behaviour).
-author("Lukasz Opiola").


%%--------------------------------------------------------------------
%% @doc
%% Translates CREATE result to the format understood by client. Will be called
%% only for requests that return {ok, {data, Data}}.
%% For other results, translate_get is called.
%% @end
%%--------------------------------------------------------------------
-callback translate_create(gs_protocol:protocol_version(), gs_protocol:gri(),
    Data :: term()) -> gs_protocol:data() | gs_protocol:error().


%%--------------------------------------------------------------------
%% @doc
%% Translates GET result to the format understood by client. Should not include
%% "gri" in the resulting json map, as it is included automatically.
%% @end
%%--------------------------------------------------------------------
-callback translate_get(gs_protocol:protocol_version(), gs_protocol:gri(),
    Data :: term()) -> gs_protocol:data() | gs_protocol:error().
