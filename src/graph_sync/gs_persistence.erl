%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains procedures to encode and decode Graph Sync messages
%%% and definitions of types used both on client and server side.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_persistence).
-author("Lukasz Opiola").

-include("graph_sync/graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").


-type subscription() :: #gri{} | binary().
-type subscriber() :: {gs_protocol:session_id(), {gs_protocol:client(), gs_protocol:auth_hint()}}.

-export_type([subscription/0, subscriber/0]).


%% API
-export([create_session/1, get_session/1, delete_session/1]).
-export([add_subscriber/4, add_subscription/2]).
-export([get_subscribers/1, get_subscriptions/1]).
-export([remove_subscriber/2, remove_subscription/2]).
-export([remove_all_subscribers/1, remove_all_subscriptions/1]).
-export([gri_to_hash/1]).

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
%% client + auth_hint that were used to access the resource.
%% @end
%%--------------------------------------------------------------------
-spec add_subscriber(subscription(), gs_protocol:session_id(),
    gs_protocol:client(), gs_protocol:auth_hint()) -> ok.
add_subscriber(#gri{} = GRI, SessionId, Client, AuthHint) ->
    add_subscriber(gri_to_hash(GRI), SessionId, Client, AuthHint);
add_subscriber(HashedGRI, SessionId, Client, AuthHint) ->
    modify_subscribers(HashedGRI, fun(Subscribers) ->
        ordsets:add_element({SessionId, {Client, AuthHint}}, Subscribers)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Adds a subscription for given client, i.e. the GRI
%% of resource about which he would like to receive updates.
%% @end
%%--------------------------------------------------------------------
-spec add_subscription(gs_protocol:session_id(), subscription()) -> ok.
add_subscription(SessionId, #gri{} = GRI) ->
    add_subscription(SessionId, gri_to_hash(GRI));
add_subscription(SessionId, HashedGRI) ->
    modify_subscriptions(SessionId, fun(Subscriptions) ->
        ordsets:add_element(HashedGRI, Subscriptions)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves the list of subscribers for given resource.
%% @end
%%--------------------------------------------------------------------
-spec get_subscribers(subscription()) -> {ok, [subscriber()]}.
get_subscribers(#gri{} = GRI) ->
    get_subscribers(gri_to_hash(GRI));
get_subscribers(HashedGRI) ->
    case gs_subscription:get(HashedGRI) of
        {error, not_found} ->
            {ok, []};
        {ok, #document{value = #gs_subscription{subscribers = Subscribers}}} ->
            {ok, Subscribers}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves the list of subscriptions for given client.
%% @end
%%--------------------------------------------------------------------
-spec get_subscriptions(gs_protocol:session_id()) -> {ok, [subscription()]}.
get_subscriptions(SessionId) ->
    case gs_session:get(SessionId) of
        {error, not_found} ->
            {ok, []};
        {ok, #document{value = #gs_session{subscriptions = Subscriptions}}} ->
            {ok, Subscriptions}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Removes a subscriber (client) from the list of
%% subscribers of given resource.
%% @end
%%--------------------------------------------------------------------
-spec remove_subscriber(subscription(), gs_protocol:session_id()) -> ok.
remove_subscriber(#gri{} = GRI, SessionId) ->
    remove_subscriber(gri_to_hash(GRI), SessionId);
remove_subscriber(HashedGRI, SessionId) ->
    modify_subscribers(HashedGRI, fun(Subscribers) ->
        % Proplists can be safely used here to delete all tuples {SessionId, _}
        % without breaking ordsets list order.
        proplists:delete(SessionId, Subscribers)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Removes a subscription from the list of resources given client is subscribed for.
%% @end
%%--------------------------------------------------------------------
-spec remove_subscription(gs_protocol:session_id(), subscription()) -> ok.
remove_subscription(SessionId, #gri{} = GRI) ->
    remove_subscription(SessionId, gri_to_hash(GRI));
remove_subscription(SessionId, HashedGRI) ->
    modify_subscriptions(SessionId, fun(Subscriptions) ->
        ordsets:del_element(HashedGRI, Subscriptions)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Removes all subscribers of given resource.
%% @end
%%--------------------------------------------------------------------
-spec remove_all_subscribers(subscription()) -> ok.
remove_all_subscribers(#gri{} = GRI) ->
    remove_all_subscribers(gri_to_hash(GRI));
remove_all_subscribers(HashedGRI) ->
    {ok, Subscribers} = get_subscribers(HashedGRI),
    lists:foreach(
        fun({SessionId, {_Client, _AuthHint}}) ->
            remove_subscription(SessionId, HashedGRI)
        end, Subscribers),
    gs_subscription:delete(HashedGRI).


%%--------------------------------------------------------------------
%% @doc
%% Removes all subscriptions of given client.
%% @end
%%--------------------------------------------------------------------
-spec remove_all_subscriptions(gs_protocol:session_id()) -> ok.
remove_all_subscriptions(SessionId) ->
    {ok, Subscriptions} = get_subscriptions(SessionId),
    lists:foreach(
        fun(HashedGRI) ->
            remove_subscriber(HashedGRI, SessionId)
        end, Subscriptions),
    modify_subscriptions(SessionId, fun(_Subscriptions) ->
        ordsets:new()
    end).


%%--------------------------------------------------------------------
%% @doc
%% Generates an md5 checksum for given GRI to be used as resource id in database.
%% @end
%%--------------------------------------------------------------------
-spec gri_to_hash(gs_protocol:gri()) -> binary().
gri_to_hash(GRI) ->
    datastore_utils:gen_key(<<"">>, term_to_binary(GRI)).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec modify_subscribers(HashedGRI :: binary(), gs_subscription:diff()) -> ok.
modify_subscribers(HashedGRI, UpdateFun) ->
    Default = #gs_subscription{subscribers = UpdateFun([])},
    Diff = fun(GSSub = #gs_subscription{subscribers = Subscribers}) ->
        {ok, GSSub#gs_subscription{subscribers = UpdateFun(Subscribers)}}
    end,
    {ok, _} = gs_subscription:update(HashedGRI, Diff, Default),
    ok.


-spec modify_subscriptions(gs_protocol:session_id(), gs_session:diff()) -> ok.
modify_subscriptions(SessionId, UpdateFun) ->
    Default = #gs_session{subscriptions = UpdateFun([])},
    Diff = fun(Session = #gs_session{subscriptions = Subscriptions}) ->
        {ok, Session#gs_session{subscriptions = UpdateFun(Subscriptions)}}
    end,
    {ok, _} = gs_session:update(SessionId, Diff, Default),
    ok.
