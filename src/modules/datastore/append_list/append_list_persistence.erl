%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
% fixme This module will be reworked during integration with datastore
%%% @end
%%%-------------------------------------------------------------------
-module(append_list_persistence).
-author("Michal Stanisz").

-include("modules/datastore/append_list.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
% fixme remove after integration with datastore as ets init will be no longer needed 
-export([init/0, destroy_ets/0]).
-export([get_node/1, save_node/2, delete_node/1]). 


%%=====================================================================
%% API
%%=====================================================================

init() ->
    ?MODULE = ets:new(
        ?MODULE,
        [set, public, named_table, {read_concurrency, true}]
    ).

destroy_ets() ->
    true = ets:delete(?MODULE),
    ok.

get_node(Id) ->
%%    io:format("get~n"),
    case ets:lookup(?MODULE, Id) of
        [{Id, Node}] -> Node;
        [] -> ?ERROR_NOT_FOUND
    end.

save_node(Id, Value) ->
%%    io:format("save: ~p~n~p~n~n", [Id, Value]),
    true = ets:insert(?MODULE, {Id, Value}).
    

delete_node(Id) ->
%%    io:format("del~n"),
    ets:delete(?MODULE, Id).

