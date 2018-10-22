%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% This module provides an interface to the hex NIF library.
%%% @end
%%%-------------------------------------------------------------------
-module(hex_nif).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

-export([hex/1]).
-on_load(init/0).

init() ->
    LibName = "hex_nif",
    LibPath = filename:join(code:priv_dir(?CLUSTER_WORKER_APP_NAME), LibName),
    
    case erlang:load_nif(LibPath, 0) of
        ok -> ok;
        {error, {reload, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.

hex(_) ->
    erlang:nif_error(hex_nif_not_loaded).

