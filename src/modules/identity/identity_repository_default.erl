%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Stores identity data in env.
%%% @end
%%%-------------------------------------------------------------------
-module(identity_repository_default).
-author("Michal Zmuda").

-behaviour(identity_repository_behaviour).
-include_lib("public_key/include/public_key.hrl").

-define(REPO_ENV, identity_repository_default_data).

-export([publish/2, get/1]).

%%--------------------------------------------------------------------
%% @doc
%% Publishes public key under given ID.
%% @end
%%--------------------------------------------------------------------
-spec publish(identity:id(), identity:public_key()) ->
    ok | {error, Reason :: term()}.
publish(ID, Key) ->
    Saved = application:get_env(app, ?REPO_ENV, #{}),
    application:set_env(app, ?REPO_ENV, Saved#{ID => Key}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Determines public key for given ID.
%% @end
%%--------------------------------------------------------------------
-spec get(identity:id()) ->
    {ok, identity:public_key()} | {error, Reason :: term()}.
get(ID) ->
    case application:get_env(app, ?REPO_ENV, #{}) of
        #{ID := Value} -> {ok, Value};
        _ -> {error, not_found}
    end.