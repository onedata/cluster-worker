%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module implementing this behaviour provides identity data cache.
%%% @end
%%%-------------------------------------------------------------------
-module(identity_cache_behaviour).
-author("Michal Zmuda").

-include_lib("public_key/include/public_key.hrl").

%%--------------------------------------------------------------------
%% @doc
%% Cached public key under given ID.
%% @end
%%--------------------------------------------------------------------
-callback put(identity:id(), identity:public_key()) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Determines cached public key for given ID.
%% @end
%%--------------------------------------------------------------------
-callback get(identity:id()) ->
    {ok, identity:public_key()} | {error, Reason :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Ensures public key for given iD is not cached.
%% @end
%%--------------------------------------------------------------------
-callback invalidate(identity:id()) -> ok | {error, Reason :: term()}.
