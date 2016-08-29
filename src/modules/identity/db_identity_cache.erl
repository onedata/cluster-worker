%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module caches data about identities in datastore.
%%% {@link cached_identity} provides state implementation for this cache.
%%% @end
%%%-------------------------------------------------------------------
-module(db_identity_cache).
-author("Michal Zmuda").

-behaviour(identity_cache_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

-export([put/2, get/1, invalidate/1]).

%%--------------------------------------------------------------------
%% @doc
%% {@link identity_cache_behaviour} callback put/2.
%% @end
%%--------------------------------------------------------------------
-spec put(identity:id(), identity:encoded_public_key()) -> ok.
put(ID, Key) ->
    cache(ID, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link identity_cache_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(identity:id()) ->
    {ok, identity:encoded_public_key()} | {error, Reason :: term()}.
get(ID) ->
    Now = now_seconds(),
    {ok, TTL} = application:get_env(?CLUSTER_WORKER_APP_NAME, identity_cache_ttl_seconds),

    case cached_identity:get(ID) of
        {ok, #document{value = #cached_identity{last_update_seconds = LastUpdate}}}
            when LastUpdate + TTL < Now ->
            {error, expired};
        {ok, #document{value = #cached_identity{encoded_public_key = Encoded}}} ->
            {ok, Encoded};
        {error, {not_found, identity_cache}} ->
            {error, not_found};
        {error, Reason} ->
            {error, {db_error, Reason}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link identity_cache_behaviour} callback invalidate/1.
%% @end
%%--------------------------------------------------------------------
-spec invalidate(identity:id()) -> ok | {error, Reason :: term()}.
invalidate(ID) ->
    case cached_identity:delete(ID) of
        ok -> ok;
        {error, {not_found, cached_identity}} -> ok;
        {error, Reason} -> {error, {db_error, Reason}}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec cache(identity:id(), identity:public_key()) ->
    ok | {error, Reason :: term()}.
cache(ID, EncodedPublicKey) ->
    Now = now_seconds(),
    Result = cached_identity:create_or_update(#document{
        key = ID, value = #cached_identity{
            last_update_seconds = Now,
            encoded_public_key = EncodedPublicKey,
            id = ID
        }}, fun(Current) ->
        {ok, Current#cached_identity{
            last_update_seconds = Now,
            encoded_public_key = EncodedPublicKey
        }}
    end),

    case Result of
        {ok, _} -> ok;
        {error, Reason} -> {error, {datastore_error, Reason}}
    end.

-spec now_seconds() -> non_neg_integer().
now_seconds() ->
    erlang:system_time(seconds).