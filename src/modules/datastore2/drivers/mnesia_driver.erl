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
-type value() :: datastore:doc().
-type init_opt() :: {type, set | ordered_set | bag}.
-type one_or_many(Type) :: Type | list(Type).

-export_type([table/0, ctx/0, key/0, value/0]).

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
        {record_name, document2},
        {attributes, record_info(fields, document2)}
    ],
    case mnesia:create_table(Table, Opts2) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, Table}} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves document/documents in Mnesia.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), one_or_many(value())) -> one_or_many({ok, value()}).
save(Ctx, #document2{} = Doc) ->
    hd(save(Ctx, [Doc]));
save(#{table := Table}, Docs) ->
    lists:map(fun(#document2{} = Doc) ->
        mnesia:dirty_write(Table, Doc),
        {ok, Doc}
    end, Docs).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves document/documents associated with key/keys from Mnesia.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), one_or_many(key())) ->
    one_or_many({ok, value()} | {error, term()}).
get(Ctx, <<_/binary>> = Key) ->
    hd(get(Ctx, [Key]));
get(#{table := Table}, Keys) ->
    lists:map(fun(Key) ->
        case mnesia:dirty_read(Table, Key) of
            [Doc] -> {ok, Doc};
            [] -> {error, key_enoent}
        end
    end, Keys).

%%--------------------------------------------------------------------
%% @doc
%% Removes document/documents associated with key/keys from a database.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(), one_or_many(key())) -> one_or_many(ok).
delete(Ctx, <<_/binary>> = Key) ->
    hd(delete(Ctx, [Key]));
delete(#{table := Table}, Keys) ->
    lists:map(fun(Key) ->
        mnesia:dirty_delete(Table, Key),
        ok
    end, Keys).