%% ===================================================================
%% @author Piotr Ociepka
%% @copyright (C): 2015 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This is a cdmi handler module providing access to cdmi_capabilities
%% which are used to discover operation that can be performed.
%% @end
%% ===================================================================
-module(cdmi_capabilities_handler).

-include("modules/http_worker/http_common.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_errors.hrl").

-include("modules/http_worker/rest/cdmi/cdmi_capabilities.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, malformed_request/2, content_types_provided/2]).
-export([get_cdmi_capability/2]).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), #{}} | {shutdown, req()}.
rest_init(Req, _Opts) ->
  {ok, Req, #{}}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), #{}) -> ok.
terminate(_, _, _) ->
  ok.

%% ====================================================================
%% @doc @equiv pre_handler:allowed_methods/2
%% ====================================================================
-spec allowed_methods(req(), #{}) -> {[binary()], req(), #{}}.
allowed_methods(Req, State) ->
  {[<<"GET">>], Req, State}.

%% ====================================================================
%% @doc @equiv pre_handler:malformed_request/2
%% ====================================================================
-spec malformed_request(req(), #{}) -> {boolean(), req(), #{}} | no_return().
malformed_request(Req, State) ->
  cdmi_arg_parser:malformed_capability_request(Req, State).

%% ====================================================================
%% @doc @equiv pre_handler:content_types_provided/2
%% ====================================================================
-spec content_types_provided(req(), #{}) -> {[{ContentType, Method}], req(), #{}} when
  ContentType :: binary(),
  Method :: atom().
content_types_provided(Req, State) ->
  {[
    {<<"application/cdmi-capability">>, get_cdmi_capability}
  ], Req, State}.

%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for cdmi capability.
%% @end
%% ====================================================================
-spec get_cdmi_capability(req(), #{}) -> {term(), req(), #{}}.
get_cdmi_capability(Req, #{options := Opts} = State) ->
  RawCapabilities = case Opts of
                      [] -> prepare_capability_ans(?default_get_capability_opts);
                      X -> prepare_capability_ans(X)
                    end,
%%   RawCapabilities = prepare_capability_ans(Opts),
  Capabilities = json:encode(RawCapabilities),
  {Capabilities, Req, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% ====================================================================
%% @doc Return proplist contains CDMI answer
%% ====================================================================
-spec prepare_capability_ans([Opt :: binary]) -> [{Capability :: binary(), Value :: term()}].
prepare_capability_ans([]) ->
  [];
prepare_capability_ans([<<"objectType">> | Tail]) ->
  [{<<"objectType">>, <<"application/cdmi-capability">>} | prepare_capability_ans(Tail)];
%% todo uncomment when ID'll be used
prepare_capability_ans([<<"objectID">> | Tail]) ->
  prepare_capability_ans(Tail);
%%   [{<<"objectID">>, ?root_capability_id} | prepare_capability_ans(Tail)];
prepare_capability_ans([<<"objectName">> | Tail]) ->
  [{<<"objectName">>, utils:ensure_unicode_binary(?root_capability_path)} | prepare_capability_ans(Tail)];
prepare_capability_ans([<<"parentURI">> | Tail]) ->
  prepare_capability_ans(Tail);
prepare_capability_ans([<<"parentID">> | Tail]) ->
  prepare_capability_ans(Tail);
prepare_capability_ans([<<"capabilities">> | Tail]) ->
  [{<<"capabilities">>, ?root_capability_list} | prepare_capability_ans(Tail)];
prepare_capability_ans([<<"childrenrange">> | Tail]) ->
  [{<<"childrenrange">>, <<"0-1">>} | prepare_capability_ans(Tail)]; %todo hardcoded childrens, when adding childrenranges or new capabilities, this has to be changed
prepare_capability_ans([<<"children">> | Tail]) ->
  [{<<"children">>, [
    utils:ensure_unicode_binary(rest_utils:get_path_leaf_with_ending_slash(?container_capability_path)),
    utils:ensure_unicode_binary(rest_utils:get_path_leaf_with_ending_slash(?dataobject_capability_path))
  ]} | prepare_capability_ans(Tail)].
