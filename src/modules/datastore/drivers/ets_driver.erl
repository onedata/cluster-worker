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

-include("modules/datastore/datastore_models.hrl").

%% API
-export([init/2]).
-export([save/3, get/2, delete/2, delete_all/1]).
-export([fold/3]).

-type table() :: atom().
-type ctx() :: #{table => table()}.
-type key() :: datastore:key().
-type doc() :: datastore:doc().
-type init_opt() :: {type, set | ordered_set | bag | duplicate_bag} |
                    {read_concurrency, boolean()} |
                    {write_concurrency, boolean()}.

-export_type([table/0, ctx/0]).

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
%% Saves value in ETS.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), key(), doc()) -> {ok, doc()}.
save(#{table := Table}, Key, #document{} = Doc) ->
    ets:insert(Table, {Key, Doc}),
    {ok, Doc}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves value from ETS.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), key()) -> {ok, doc()} | {error, term()}.
get(#{table := Table}, Key) ->
    case ets:lookup(Table, Key) of
        [{Key, Doc}] -> {ok, Doc};
        [] -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes value from ETS.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(), key()) -> ok.
delete(#{table := Table}, Key) ->
    ets:delete(Table, Key),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Removes all values from ETS.
%% @end
%%--------------------------------------------------------------------
-spec delete_all(ctx()) -> ok.
delete_all(#{table := Table}) ->
    ets:delete_all_objects(Table),
    ok.

-spec fold(ctx(), datastore_model:driver_fold_fun(), term()) -> {ok | stop, term()}.
fold(#{table := Table}, Fun, Acc0) ->
    ets:foldl(fun
        ({Key, Doc}, {ok, Acc}) -> Fun(Key, Doc, Acc);
        (_, {stop, Acc}) -> {stop, Acc}
    end, {ok, Acc0}, Table).