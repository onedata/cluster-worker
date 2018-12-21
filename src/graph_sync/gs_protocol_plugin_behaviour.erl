%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This behaviour defines callbacks that must be implemented in
%%% gs_protocol_plugin, which is used by gs_protocol to customize message
%%% encoding/decoding.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_protocol_plugin_behaviour).
-author("Lukasz Opiola").


%%--------------------------------------------------------------------
%% @doc
%% Encodes entity type to string, used in serialized GRI form.
%% Should throw ?ERROR_BAD_TYPE upon failure.
%% @end
%%--------------------------------------------------------------------
-callback encode_entity_type(gs_protocol:entity_type()) -> binary().


%%--------------------------------------------------------------------
%% @doc
%% Decodes entity type from string, used in serialized GRI form.
%% Should throw ?ERROR_BAD_TYPE upon failure.
%% @end
%%--------------------------------------------------------------------
-callback decode_entity_type(binary()) -> gs_protocol:entity_type().
