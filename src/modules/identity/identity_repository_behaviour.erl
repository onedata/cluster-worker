%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module implementing this behaviour provides access to identity repository,
%%% which allows onedata components to verify their IDs using certificate data.
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
