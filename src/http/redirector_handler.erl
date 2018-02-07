%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module handles requests directed to http and returns a 301 redirect to https.
%%% @end
%%%--------------------------------------------------------------------
-module(redirector_handler).
-behaviour(cowboy_handler).

-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").

%% API
-export([init/2]).


%%--------------------------------------------------------------------
%% @doc Cowboy handler callback.
%% Handles a request returning a HTTP Redirect (301 - Moved permanently).
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), term()) -> {ok, cowboy_req:req(), term()}.
init(#{host := FullHostname, path := Path, qs := Qs} = Req, State) ->
    QsString = case str_utils:to_binary(Qs) of
        <<"">> -> <<"">>;
        <<"undefined">> -> <<"">>;
        Bin -> <<"?", Bin/binary>>
    end,
    % Remove the leading 'www.' if present
    Hostname = case FullHostname of
        <<"www.", Rest/binary>> ->
            Rest;
        _ ->
            FullHostname
    end,
    NewReq = cowboy_req:reply(301, #{
        <<"location">> => <<"https://", Hostname/binary, Path/binary, QsString/binary>>,
        <<"content-type">> => <<"text/html">>
    }, Req),
    {ok, NewReq, State}.
