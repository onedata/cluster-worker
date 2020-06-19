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

-include("modules/datastore/datastore_models.hrl").

%% API
-export([init/2]).
-export([save/3, get/2, delete/2]).
-export([fold/3]).

-type table() :: atom().
-type ctx() :: #{table => table()}.
-type key() :: datastore:key().
-type doc() :: datastore:doc().
-type init_opt() :: {type, set | ordered_set | bag}.

-export_type([table/0, ctx/0]).

-record(entry, {
    key :: key(),
    value :: doc()
}).

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
        {record_name, entry},
        {attributes, record_info(fields, entry)}
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
-spec save(ctx(), key(), doc()) -> {ok, doc()}.
save(#{table := Table}, Key, Doc = #document{}) ->
    try
        mnesia:dirty_write(Table, #entry{key = Key, value = Doc}),
        {ok, Doc}
    catch
        _:{aborted, Reason} -> {error, {Reason, erlang:get_stacktrace()}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves value from Mnesia.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), key()) -> {ok, doc()} | {error, term()}.
get(#{table := Table}, Key) ->
    try mnesia:dirty_read(Table, Key) of
        [#entry{value = Doc}] -> {ok, Doc};
        [] -> {error, not_found}
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

-spec fold(ctx(), datastore_model:driver_fold_fun(), term()) -> {ok | stop, term()}.
fold(#{table := Table}, Fun, Acc0) ->
    try
        mnesia:async_dirty(fun() ->
            mnesia:foldl(fun
                (#entry{key = Key, value = Doc}, {ok, Acc}) -> Fun(Key, Doc, Acc);
                (_, {stop, Acc}) -> {stop, Acc}
            end, {ok, Acc0}, Table)
        end)
    catch
        _:Reason -> {error, {Reason, erlang:get_stacktrace()}}
    end.