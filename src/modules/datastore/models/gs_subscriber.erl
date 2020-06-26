%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% DB API for gs_subscriber record.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_subscriber).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([create/1, get_subscriptions/1, modify_subscriptions/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type subscriptions_diff() :: fun((gs_persistence:subscriptions()) -> gs_persistence:subscriptions()).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(gs_protocol:session_id()) -> ok.
create(SessionId) ->
    {ok, _} = datastore_model:create(?CTX, #document{
        key = SessionId,
        value = #gs_subscriber{}
    }),
    ok.


-spec get_subscriptions(gs_protocol:session_id()) -> gs_persistence:subscriptions().
get_subscriptions(SessionId) ->
    case datastore_model:get(?CTX, SessionId) of
        {error, not_found} ->
            ordsets:new();
        {ok, #document{value = #gs_subscriber{subscriptions = Subscriptions}}} ->
            Subscriptions
    end.


-spec modify_subscriptions(gs_protocol:session_id(), subscriptions_diff()) -> ok.
modify_subscriptions(SessionId, SubscriptionsDiff) ->
    Diff = fun(Subscriber = #gs_subscriber{subscriptions = Subs}) ->
        {ok, Subscriber#gs_subscriber{subscriptions = SubscriptionsDiff(Subs)}}
    end,
    case datastore_model:update(?CTX, SessionId, Diff) of
        {ok, _} -> ok;
        {error, not_found} -> ok  % possible when session cleanup is in progress
    end.


-spec delete(gs_protocol:session_id()) -> ok | {error, term()}.
delete(SessionId) ->
    datastore_model:delete(?CTX, SessionId).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

