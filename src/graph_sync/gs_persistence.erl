%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements a the core Graph Sync engine logic - handling of
%%% sessions, subscriptions and publishing changes.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_persistence).
-author("Lukasz Opiola").

-include("graph_sync/graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").

% List of subscriptions of given client
-type subscriptions() :: ordsets:ordset(gri:gri()).
% Identifier of a subscriber client and auth that was used to access a resource
-type subscriber() :: {gs_protocol:session_id(), {aai:auth(), gs_protocol:auth_hint()}}.
% A map of subscribers per {Aspect, Scope} for an entity
-type entity_subscribers() :: #{{gs_protocol:aspect(), gs_protocol:scope()} => ordsets:ordset(subscriber())}.

-export_type([subscriptions/0, subscriber/0, entity_subscribers/0]).

%% API
-export([create_session/4, get_session/1, delete_session/1]).
-export([subscribe/4, unsubscribe/2]).
-export([get_entity_subscribers/2, remove_all_subscribers/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec create_session(aai:auth(), gs_server:conn_ref(), gs_protocol:protocol_version(), gs_server:translator()) ->
    gs_session:data().
create_session(Auth, ConnRef, ProtoVersion, Translator) ->
    SessionData = gs_session:create(Auth, ConnRef, ProtoVersion, Translator),
    gs_subscriber:create(SessionData#gs_session.id),
    SessionData.


-spec get_session(gs_protocol:session_id()) -> {ok, gs_session:data()} | {error, term()}.
get_session(SessionId) ->
    gs_session:get(SessionId).


-spec delete_session(gs_protocol:session_id()) -> ok.
delete_session(SessionId) ->
    Subscriptions = gs_subscriber:get_subscriptions(SessionId),
    gs_subscriber:delete(SessionId),
    gs_session:delete(SessionId),
    lists:foreach(fun(GRI) ->
        remove_subscriber(GRI, SessionId)
    end, Subscriptions).


-spec subscribe(gs_protocol:session_id(), gri:gri(), aai:auth(), gs_protocol:auth_hint()) -> ok.
subscribe(SessionId, GRI, Auth, AuthHint) ->
    add_subscriber(GRI, SessionId, Auth, AuthHint),
    add_subscription(SessionId, GRI).


-spec unsubscribe(gs_protocol:session_id(), gri:gri()) -> ok.
unsubscribe(SessionId, GRI) ->
    remove_subscriber(GRI, SessionId),
    remove_subscription(SessionId, GRI).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves the list of all subscribers for resources related to an entity.
%% @end
%%--------------------------------------------------------------------
-spec get_entity_subscribers(gs_protocol:entity_type(), gs_protocol:entity_id()) -> entity_subscribers().
get_entity_subscribers(EntityType, EntityId) ->
    gs_subscription:get_entity_subscribers(EntityType, EntityId).


%%--------------------------------------------------------------------
%% @doc
%% Removes all subscribers of given resource.
%% @end
%%--------------------------------------------------------------------
-spec remove_all_subscribers(gri:gri()) -> ok.
remove_all_subscribers(GRI = #gri{type = Type, id = Id, aspect = Aspect, scope = Scope}) ->
    EntitySubscribers = get_entity_subscribers(Type, Id),
    SubscribersForAspect = maps:get({Aspect, Scope}, EntitySubscribers, ordsets:new()),
    lists:foreach(fun({SessionId, {_Auth, _AuthHint}}) ->
        remove_subscription(SessionId, GRI)
    end, SubscribersForAspect),
    gs_subscription:modify_entity_subscribers(Type, Id, fun(Subscribers) ->
        maps:remove({Aspect, Scope}, Subscribers)
    end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds a subscriber for given GRI, i.e. a client that would like to receive
%% updates of given resource. The subscriber is identified by session id and
%% its auth + auth_hint that were used to access the resource are stored alongside.
%% If the client with the same session id performs a second subscription,
%% the old one is deleted.
%% @end
%%--------------------------------------------------------------------
-spec add_subscriber(gri:gri(), gs_protocol:session_id(), aai:auth(), gs_protocol:auth_hint()) -> ok.
add_subscriber(#gri{type = Type, id = Id, aspect = Aspect, scope = Scope}, SessionId, Auth, AuthHint) ->
    gs_subscription:modify_entity_subscribers(Type, Id, fun(EntitySubscribers) ->
        Subscribers = maps:get({Aspect, Scope}, EntitySubscribers, ordsets:new()),
        NewSubscribers = ordsets:add_element(
            {SessionId, {Auth, AuthHint}},
            proplists:delete(SessionId, Subscribers)
        ),
        EntitySubscribers#{
            {Aspect, Scope} => NewSubscribers
        }
    end).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes a subscriber (client) from the list of subscribers of given resource.
%% @end
%%--------------------------------------------------------------------
-spec remove_subscriber(gri:gri(), gs_protocol:session_id()) -> ok.
remove_subscriber(#gri{type = Type, id = Id, aspect = Aspect, scope = Scope}, SessionId) ->
    gs_subscription:modify_entity_subscribers(Type, Id, fun(EntitySubscribers) ->
        SubscribersForAspect = maps:get({Aspect, Scope}, EntitySubscribers, ordsets:new()),
        NewSubscribers = ordsets:filter(fun({Current, _}) -> Current /= SessionId end, SubscribersForAspect),
        case ordsets:size(NewSubscribers) of
            0 ->
                maps:remove({Aspect, Scope}, EntitySubscribers);
            _ ->
                EntitySubscribers#{{Aspect, Scope} => NewSubscribers}
        end
    end).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds a subscription for given client denoted by session id, i.e. the GRI
%% of resource about which they would like to receive updates.
%% @end
%%--------------------------------------------------------------------
-spec add_subscription(gs_protocol:session_id(), gri:gri()) -> ok.
add_subscription(SessionId, GRI) ->
    gs_subscriber:modify_subscriptions(SessionId, fun(Subscriptions) ->
        ordsets:add_element(GRI, Subscriptions)
    end).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes a subscription from the list of resources given client is subscribed for.
%% @end
%%--------------------------------------------------------------------
-spec remove_subscription(gs_protocol:session_id(), gri:gri()) -> ok.
remove_subscription(SessionId, GRI) ->
    gs_subscriber:modify_subscriptions(SessionId, fun(Subscriptions) ->
        ordsets:del_element(GRI, Subscriptions)
    end).
