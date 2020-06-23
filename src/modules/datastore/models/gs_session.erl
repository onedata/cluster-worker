%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% DB API for gs_session record.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_session).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([create/4, get/1, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type data() :: #gs_session{}.

-export_type([data/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined,
    memory_copies => all
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(aai:auth(), gs_server:conn_ref(), gs_protocol:protocol_version(), gs_server:translator()) ->
    data().
create(Auth, ConnRef, ProtoVersion, Translator) ->
    SessionId = datastore_key:new(),
    SessionData = #gs_session{
        id = SessionId,
        auth = Auth,
        conn_ref = ConnRef,
        protocol_version = ProtoVersion,
        translator = Translator
    },
    {ok, _} = datastore_model:create(?CTX, #document{key = SessionId, value = SessionData}),
    SessionData.


-spec get(gs_protocol:session_id()) -> {ok, data()} | {error, term()}.
get(SessionId) ->
    case datastore_model:get(?CTX, SessionId) of
        {ok, #document{value = SessionData}} -> {ok, SessionData};
        {error, _} = Error -> Error
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
