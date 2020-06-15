%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements a high-level API for GraphSync persistence -
%%% gs_session and gs_subscription records.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_persistence).
-author("Lukasz Opiola").

-include("graph_sync/graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").


% List of subscriptions of given client
-type subscriptions() :: ordsets:ordset(gri:gri()).
% Identifier of a subscriber client
-type subscriber() :: {gs_protocol:session_id(), {aai:auth(), gs_protocol:auth_hint()}}.
% A map of subscribers per {Aspect, Scope} for an entity
-type subscribers() :: #{{gs_protocol:aspect(), gs_protocol:scope()} => ordsets:ordset(subscriber())}.

-export_type([subscriptions/0, subscriber/0, subscribers/0]).


%% API
-export([create_session/1, get_session/1, delete_session/1]).
-export([add_subscriber/4, add_subscription/2]).
-export([get_subscribers/2, get_subscriptions/1]).
-export([remove_subscriber/2, remove_subscription/2]).
-export([remove_all_subscribers/1, remove_all_subscriptions/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a Graph Sync session.
%% @end
%%--------------------------------------------------------------------
-spec create_session(#gs_session{}) -> {ok, gs_protocol:session_id()}.
create_session(GsSession = #gs_session{}) ->
    {ok, #document{key = SessionId}} = gs_session:create(#document{
        value = GsSession
    }),
    {ok, SessionId}.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a Graph Sync session by id. Returned record has an extra 'id'
%% field for more concise code.
%% @end
%%--------------------------------------------------------------------
-spec get_session(gs_protocol:session_id()) -> {ok, #gs_session{}} | {error, term()}.
get_session(SessionId) ->
    case gs_session:get(SessionId) of
        {ok, #document{value = GsSession}} ->
            {ok, GsSession#gs_session{
                id = SessionId
            }};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes a Graph Sync session by id.
%% @end
%%--------------------------------------------------------------------
-spec delete_session(gs_protocol:session_id()) -> ok.
delete_session(SessionId) ->
    gs_session:delete(SessionId).


%%--------------------------------------------------------------------
%% @doc
%% Adds a subscriber for given GRI, i.e. a client that would like to receive
%% updates of given resource. The subscriber is identified by session id and
%% auth + auth_hint that were used to access the resource.
%% If the client with the same session id performs a second subscription,
%% the old one is deleted.
%% @end
%%--------------------------------------------------------------------
-spec add_subscriber(gri:gri(), gs_protocol:session_id(),
    aai:auth(), gs_protocol:auth_hint()) -> ok.
add_subscriber(#gri{type = Type, id = Id, aspect = Aspect, scope = Scope}, SessionId, Auth, AuthHint) ->
    modify_subscribers(Type, Id, fun(AllSubscribers) ->
        Subscribers = maps:get({Aspect, Scope}, AllSubscribers, ordsets:new()),
        NewSubscribers = ordsets:add_element(
            {SessionId, {Auth, AuthHint}},
            proplists:delete(SessionId, Subscribers)
        ),
        AllSubscribers#{
            {Aspect, Scope} => NewSubscribers
        }
    end).


%%--------------------------------------------------------------------
%% @doc
%% Adds a subscription for given client, i.e. the GRI
%% of resource about which he would like to receive updates.
%% @end
%%--------------------------------------------------------------------
-spec add_subscription(gs_protocol:session_id(), gri:gri()) -> ok.
add_subscription(SessionId, GRI) ->
    modify_subscriptions(SessionId, fun(Subscriptions) ->
        ordsets:add_element(GRI, Subscriptions)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves the list of subscribers for given resource.
%% @end
%%--------------------------------------------------------------------
-spec get_subscribers(gs_protocol:entity_type(), gs_protocol:entity_id()) ->
    {ok, subscribers()}.
get_subscribers(Type, Id) ->
    case gs_subscription:get(Type, Id) of
        {error, not_found} ->
            {ok, #{}};
        {ok, #document{value = #gs_subscription{subscribers = Subscribers}}} ->
            {ok, Subscribers}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves the list of subscriptions for given client.
%% @end
%%--------------------------------------------------------------------
-spec get_subscriptions(gs_protocol:session_id()) -> {ok, subscriptions()}.
get_subscriptions(SessionId) ->
    case gs_session:get(SessionId) of
        {error, not_found} ->
            {ok, ordsets:new()};
        {ok, #document{value = #gs_session{subscriptions = Subscriptions}}} ->
            {ok, Subscriptions}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Removes a subscriber (client) from the list of subscribers of given resource.
%% @end
%%--------------------------------------------------------------------
-spec remove_subscriber(gri:gri(), gs_protocol:session_id()) -> ok.
remove_subscriber(#gri{type = Type, id = Id, aspect = Aspect, scope = Scope}, SessionId) ->
    modify_subscribers(Type, Id, fun(AllSubscribers) ->
        SubscribersForAspect = maps:get({Aspect, Scope}, AllSubscribers, ordsets:new()),
        NewSubscribers = ordsets:filter(fun({SessId, _}) ->
            case SessId of
                SessionId -> false;
                _ -> true
            end
        end, SubscribersForAspect),
        case ordsets:size(NewSubscribers) of
            0 ->
                maps:remove({Aspect, Scope}, AllSubscribers);
            _ ->
                AllSubscribers#{{Aspect, Scope} => NewSubscribers}
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% Removes a subscription from the list of resources given client is subscribed for.
%% @end
%%--------------------------------------------------------------------
-spec remove_subscription(gs_protocol:session_id(), gri:gri()) -> ok.
remove_subscription(SessionId, GRI) ->
    modify_subscriptions(SessionId, fun(Subscriptions) ->
        ordsets:del_element(GRI, Subscriptions)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Removes all subscribers of given resource.
%% @end
%%--------------------------------------------------------------------
-spec remove_all_subscribers(gri:gri()) -> ok.
remove_all_subscribers(GRI = #gri{type = Type, id = Id, aspect = Aspect, scope = Scope}) ->
    {ok, AllSubscribers} = get_subscribers(Type, Id),
    SubscribersForAspect = maps:get({Aspect, Scope}, AllSubscribers, ordsets:new()),
    lists:foreach(
        fun({SessionId, {_Auth, _AuthHint}}) ->
            remove_subscription(SessionId, GRI)
        end, SubscribersForAspect),
    modify_subscribers(Type, Id, fun(Subscribers) ->
        maps:remove({Aspect, Scope}, Subscribers)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Removes all subscriptions of given client.
%% @end
%%--------------------------------------------------------------------
-spec remove_all_subscriptions(gs_protocol:session_id()) -> ok.
remove_all_subscriptions(SessionId) ->
    {ok, Subscriptions} = get_subscriptions(SessionId),
    lists:foreach(
        fun(GRI) ->
            remove_subscriber(GRI, SessionId)
        end, Subscriptions),
    modify_subscriptions(SessionId, fun(_Subscriptions) ->
        ordsets:new()
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec modify_subscribers(gs_protocol:entity_type(), gs_protocol:entity_id(),
    gs_subscription:diff()) -> ok.
modify_subscribers(Type, Id, UpdateFun) ->
    Default = #gs_subscription{subscribers = UpdateFun(#{})},
    Diff = fun(GSSub = #gs_subscription{subscribers = Subscribers}) ->
        {ok, GSSub#gs_subscription{subscribers = UpdateFun(Subscribers)}}
    end,
    {ok, _} = gs_subscription:update(Type, Id, Diff, Default),
    ok.


-spec modify_subscriptions(gs_protocol:session_id(), gs_session:diff()) -> ok.
modify_subscriptions(SessionId, UpdateFun) ->
    Diff = fun(Session = #gs_session{subscriptions = Subs}) ->
        {ok, Session#gs_session{subscriptions = UpdateFun(Subs)}}
    end,
    case gs_session:update(SessionId, Diff) of
        {ok, _} -> ok;
        {error, not_found} -> ok  % possible when session cleanup is in progress
    end.
