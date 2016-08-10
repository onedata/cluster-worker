%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module implementing this behaviour provides access to public key store.
%%% Identity repository should allow to publish public key data under given ID.
%%% Once published, that public key is obtainable using it's ID. The public key
%%% can be updated by the original publisher by subsequent publish.
%%% That store should guard ownership of the published data - only original
%%% publisher should be able to update public key.
%%% @end
%%%-------------------------------------------------------------------
-module(identity_repository_behaviour).
-author("Michal Zmuda").

-include_lib("public_key/include/public_key.hrl").

%%--------------------------------------------------------------------
%% @doc
%% Publishes public key under given ID.
%% @end
%%--------------------------------------------------------------------
-callback publish(identity:id(), identity:public_key()) ->
    ok | {error, Reason :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Determines public key for given ID.
%% @end
%%--------------------------------------------------------------------
-callback get(identity:id()) ->
    {ok, identity:public_key()} | {error, Reason :: term()}.
