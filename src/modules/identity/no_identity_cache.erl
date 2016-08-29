%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Dummy identity data cache.
%%% @end
%%%-------------------------------------------------------------------
-module(no_identity_cache).
-author("Michal Zmuda").

-behaviour(identity_cache_behaviour).
-include_lib("public_key/include/public_key.hrl").
-export([put/2, get/1, invalidate/1]).

%%--------------------------------------------------------------------
%% @doc
%% {@link identity_cache_behaviour} callback put/2.
%% @end
%%--------------------------------------------------------------------
-spec put(identity:id(), identity:encoded_public_key()) -> {error, no_cache}.
put(_, _) -> {error, no_cache}.

%%--------------------------------------------------------------------
%% @doc
%% {@link identity_cache_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(identity:id()) -> {error, no_cache}.
get(_) -> {error, no_cache}.

%%--------------------------------------------------------------------
%% @doc
%% {@link identity_cache_behaviour} callback invalidate/1.
%% @end
%%--------------------------------------------------------------------
-spec invalidate(identity:id()) -> ok | {error, Reason :: term()}.
invalidate(_) -> ok.