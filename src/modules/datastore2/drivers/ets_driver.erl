%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an interface to ETS store.
%%% @end
%%%-------------------------------------------------------------------
-module(ets_driver).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models_def.hrl").

%% API
-export([init/2]).
-export([save/2, get/2, delete/2]).

-type table() :: atom().
-type ctx() :: #{table => table()}.
-type key() :: datastore:key().
-type value() :: datastore:doc().
-type init_opt() :: {type, ets:type()} | {read_concurrency, boolean()} |
                    {write_concurrency, boolean()}.
-type one_or_many(Type) :: Type | list(Type).

-export_type([table/0, ctx/0, key/0, value/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates ETS table.
%% @end
%%--------------------------------------------------------------------
-spec init(ctx(), [init_opt()]) -> ok | {error, Reason :: term()}.
init(#{table := Table}, Opts) ->
    Opts2 = [
        proplists:get_value(type, Opts, set),
        public,
        named_table,
        {read_concurrency, proplists:get_value(read_concurrency, Opts, false)},
        {write_concurrency, proplists:get_value(write_concurrency, Opts, false)}
    ],
    try
        ets:new(Table, Opts2),
        ok
    catch
        _:Reason -> {error, {Reason, erlang:get_stacktrace()}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves document/documents in ETS.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), one_or_many(value())) -> one_or_many({ok, value()}).
save(Ctx, #document2{} = Doc) ->
    hd(save(Ctx, [Doc]));
save(#{table := Table}, Docs) ->
    lists:map(fun(#document2{key = Key} = Doc) ->
        ets:insert(Table, {Key, Doc}),
        {ok, Doc}
    end, Docs).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves document/documents associated with key/keys from ETS.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), one_or_many(key())) ->
    one_or_many({ok, value()} | {error, term()}).
get(Ctx, <<_/binary>> = Key) ->
    hd(get(Ctx, [Key]));
get(#{table := Table}, Keys) ->
    lists:map(fun(Key) ->
        case ets:lookup(Table, Key) of
            [{Key, Doc}] -> {ok, Doc};
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
        ets:delete(Table, Key),
        ok
    end, Keys).