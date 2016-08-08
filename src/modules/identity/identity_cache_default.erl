%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% No identity data cache.
%%% @end
%%%-------------------------------------------------------------------
-module(identity_cache_default).
-author("Michal Zmuda").

-behaviour(identity_cache_behaviour).
-include_lib("public_key/include/public_key.hrl").
-export([put/2, get/1, invalidate/1]).

%%--------------------------------------------------------------------
%% @doc
%% Cached public key under given ID.
%% @end
%%--------------------------------------------------------------------
-spec put(identity:id(), identity:public_key()) -> ok.
put(_, _) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Determines cached public key for given ID.
%% @end
%%--------------------------------------------------------------------
-spec get(identity:id()) -> {error, no_cache}.
get(_) -> {error, no_cache}.

%%--------------------------------------------------------------------
%% @doc
%% Ensures public key for given iD is not cached.
%% @end
%%--------------------------------------------------------------------
-spec invalidate(identity:id()) -> ok | {error, Reason :: term()}.
invalidate(_) -> ok.