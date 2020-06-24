%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% DB API for gs_subscription record.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_subscription).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([get_entity_subscribers/2, modify_entity_subscribers/3]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type entity_subscribers_diff() :: fun((gs_persistence:entity_subscribers()) -> gs_persistence:entity_subscribers()).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_entity_subscribers(gs_protocol:entity_type(), gs_protocol:entity_id()) ->
    gs_persistence:entity_subscribers().
get_entity_subscribers(EntityType, EntityId) ->
    case datastore_model:get(?CTX, id(EntityType, EntityId)) of
        {error, not_found} ->
            #{};
        {ok, #document{value = #gs_subscription{subscribers = Subscribers}}} ->
            Subscribers
    end.


-spec modify_entity_subscribers(gs_protocol:entity_type(), gs_protocol:entity_id(), entity_subscribers_diff()) ->
    ok.
modify_entity_subscribers(EntityType, EntityId, UpdateFun) ->
    Default = #gs_subscription{subscribers = UpdateFun(#{})},
    Diff = fun(Subscription = #gs_subscription{subscribers = Subscribers}) ->
        {ok, Subscription#gs_subscription{subscribers = UpdateFun(Subscribers)}}
    end,
    {ok, _} = datastore_model:update(?CTX, id(EntityType, EntityId), Diff, Default),
    ok.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec id(gs_protocol:entity_type(), gs_protocol:entity_id()) -> binary().
id(EntityType, EntityId) ->
    <<(atom_to_binary(EntityType, utf8))/binary, EntityId/binary>>.
