%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @TODO VFS-7411 This module will be reworked during integration with datastore
%%% @end
%%%-------------------------------------------------------------------
-module(infinite_log_persistence).
-author("Lukasz Opiola").

-include_lib("ctool/include/errors.hrl").

%% API
-export([get_record/1, save_record/2, delete_record/1]).

%%=====================================================================
%% API
%%=====================================================================

-spec get_record(binary()) -> {ok, term()} | {error, term()}.
get_record(Id) ->
    case node_cache:get({record, Id}, undefined) of
        undefined -> {error, not_found};
        Record -> {ok, Record}
    end.


-spec save_record(binary(), term()) -> ok | {error, term()}.
save_record(Id, Value) ->
    %% @TODO VFS-7411 trick dialyzer into thinking that errors can be returned
    %% they actually will after integration with datastore
    case Id of
        <<"123">> -> {error, etmpfail};
        _ -> node_cache:put({record, Id}, Value)
    end.


-spec delete_record(binary()) -> ok | {error, term()}.
delete_record(Id) ->
    %% @TODO VFS-7411 trick dialyzer into thinking that errors can be returned
    %% they actually will after integration with datastore
    case Id of
        <<"123">> -> {error, etmpfail};
        _ -> node_cache:clear({record, Id})
    end.

