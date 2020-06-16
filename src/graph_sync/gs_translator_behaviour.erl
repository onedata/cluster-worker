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
%% Returns handshake response attributes for given client that has been
%% authorized.
%% @end
%%--------------------------------------------------------------------
-callback handshake_attributes(aai:auth()) ->
    gs_protocol:handshake_attributes().


%%--------------------------------------------------------------------
%% @doc
%% Translates result with data_format = 'value' (currently only applicable to
%% CREATE operation) to the format understood by client.
%% Can return a function taking one argument - the Auth object, that returns
%% result depending on the authorized client.
%% @end
%%--------------------------------------------------------------------
-callback translate_value(gs_protocol:protocol_version(), gri:gri(),
    Value :: term()) -> Result | fun((aai:auth()) -> Result) when
    Result :: gs_protocol:data() | errors:error().


%%--------------------------------------------------------------------
%% @doc
%% Translates result with data_format = 'resource' (can be returned from
%% CREATE or GET operations) to the format understood by client. The requested
%% GRI will be automatically included in the answer.
%% Can return a function taking one argument - the Auth object, that returns
%% result depending on the authorized client.
%% @end
%%--------------------------------------------------------------------
-callback translate_resource(gs_protocol:protocol_version(), gri:gri(),
    ResourceData :: term()) -> Result | fun((aai:auth()) -> Result) when
    Result :: gs_protocol:data() | errors:error().
