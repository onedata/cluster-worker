%%%-------------------------------------------------------------------
%%% @author RoXeon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jul 2014 23:56
%%%-------------------------------------------------------------------
-module(registry_spaces).
-author("RoXeon").

-include_lib("ctool/include/logging.hrl").
-include("veil_modules/dao/dao_vfs.hrl").

%% API
-export([get_space_info/1]).


%% ====================================================================
%% API functions
%% ====================================================================


get_space_info(SpaceId) ->
    case request(get, "spaces/" ++ SpaceId) of
        {ok, Response} ->
            ?info("Resp: ~p", [Response]),
            #{<<"name">> := SpaceName} = Response,
            {ok, #space_info{uuid = SpaceId, name = binary_to_list(SpaceName)}};
        {error, Reason} ->
            {error, Reason}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

request(Method, URI) ->
    request(Method, URI, <<"">>).
request(Method, URI, Body) ->
    URL = "https://globalregistry.org:8443/" ++ URI,
    case ibrowse:send_req(URL, [{"Content-Type", "application/json"}], Method, Body,
            [{ssl_options, [{verify, verify_none}, {certfile, get_provider_cert_path()}, {keyfile, get_provider_key_path()}]}]) of
        {ok, "200", _, Response} -> {ok, jiffy:decode(Response, [return_maps])};
        {ok, "404", _, _} -> {error, not_found};
        {ok, Status, _, _} -> {error, {invalid_status, Status}};
        {error, Reason} -> {error, Reason}
    end.

get_provider_cert_path() ->
    "./certs/grpcert.pem".

get_provider_key_path() ->
    "./certs/grpkey.pem".