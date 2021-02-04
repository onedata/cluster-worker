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
-module(sliding_proplist_persistence).
-author("Michal Stanisz").

-include("modules/datastore/sliding_proplist.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
% fixme remove after integration with datastore as ets init will be no longer needed 
-export([init/0, destroy_ets/0]).
-export([get_record/1, save_record/2, delete_record/1]). 


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

get_record(Id) ->
    case ets:lookup(?MODULE, Id) of
        [{Id, Node}] -> {ok, Node};
        [] -> ?ERROR_NOT_FOUND
    end.

save_record(Id, Value) ->
    true = ets:insert(?MODULE, {Id, Value}).
    

delete_record(Id) ->
    ets:delete(?MODULE, Id).

