%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an interface to Mnesia store.
%%% @end
%%%-------------------------------------------------------------------
-module(mnesia_driver).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models_def.hrl").

%% API
-export([init/2]).
-export([save/2, get/2, delete/2]).

-type table() :: atom().
-type ctx() :: #{table => table()}.
-type key() :: datastore:key().
-type value() :: datastore:document().
-type init_opt() :: {type, set | ordered_set | bag}.

-export_type([table/0, ctx/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates Mnesia table.
%% @end
%%--------------------------------------------------------------------
-spec init(ctx(), [init_opt()]) -> ok | {error, Reason :: term()}.
init(#{table := Table}, Opts) ->
    Opts2 = [
        {type, proplists:get_value(type, Opts, set)},
        {ram_copies, [node()]},
        {record_name, document},
        {attributes, record_info(fields, document)}
    ],
    case mnesia:create_table(Table, Opts2) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, Table}} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves value in Mnesia.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), value()) -> {ok, value()}.
save(#{table := Table}, Doc) ->
    try
        mnesia:dirty_write(Table, Doc),
        {ok, Doc}
    catch
        _:{aborted, Reason} -> {error, {Reason, erlang:get_stacktrace()}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves value from Mnesia.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), key()) -> {ok, value()} | {error, term()}.
get(#{table := Table}, Key) ->
    try mnesia:dirty_read(Table, Key) of
        [Doc] -> {ok, Doc};
        [] -> {error, key_enoent}
    catch
        _:{aborted, Reason} -> {error, {Reason, erlang:get_stacktrace()}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes value from Mnesia.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(), key()) -> ok.
delete(#{table := Table}, Key) ->
    try
        mnesia:dirty_delete(Table, Key),
        ok
    catch
        _:{aborted, Reason} -> {error, {Reason, erlang:get_stacktrace()}}
    end.