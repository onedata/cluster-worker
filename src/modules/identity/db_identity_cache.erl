%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module manages identity data in DHT.
%%% @end
%%%-------------------------------------------------------------------
-module(db_identity_cache).
-author("Michal Zmuda").

-behaviour(identity_cache_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("public_key/include/public_key.hrl").

-export([put/2, get/1, invalidate/1]).

%%--------------------------------------------------------------------
%% @doc
%% Cached public key under given ID.
%% @end
%%--------------------------------------------------------------------
-spec put(identity:id(), identity:public_key()) -> ok.
put(ID, Key) ->
    cache(ID, Key).

%%--------------------------------------------------------------------
%% @doc
%% Determines cached public key for given ID.
%% @end
%%--------------------------------------------------------------------
-spec get(identity:id()) ->
    {ok, identity:public_key()} | {error, Reason :: term()}.
get(ID) ->
    Now = now_seconds(),
    {ok, TTL} = application:get_env(?CLUSTER_WORKER_APP_NAME, identity_cache_ttl_seconds),

    case cached_identity:get(ID) of
        {ok, #document{value = #cached_identity{last_update_seconds = Seconds}}}
            when Seconds + TTL < Now ->
            {error, expired};
        {ok, #document{value = #cached_identity{public_key = Key}}} ->
            {ok, Key};
        {error, {not_found, identity_cache}} ->
            {error, not_found};
        {error, Reason} ->
            {error, {db_error, Reason}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Ensures public key for given iD is not cached.
%% @end
%%--------------------------------------------------------------------
-spec invalidate(identity:id()) -> ok | {error, Reason :: term()}.
invalidate(ID) ->
    case cached_identity:delete(ID) of
        {error, Reason} -> {error, {db_error, Reason}};
        ok -> ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec cache(identity:id(), identity:public_key()) -> ok.
cache(ID, PublicKey) ->
    Now = now_seconds(),
    Result = cached_identity:create_or_update(#document{
        key = ID, value = #cached_identity{
            last_update_seconds = Now,
            public_key = PublicKey,
            id = ID
        }}, fun(Current) ->
        {ok, Current#cached_identity{
            last_update_seconds = Now,
            public_key = PublicKey
        }}
    end),

    case Result of
        {ok, _} -> ok;
        {error, Reason} -> ?warning("Unable to cache entry for ~p due to ~p", [ID, Reason])
    end.

-spec now_seconds() -> non_neg_integer().
now_seconds() ->
    erlang:system_time(seconds).