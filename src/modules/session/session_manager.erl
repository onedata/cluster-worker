%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for creating and forwarding requests to
%%% session worker.
%%% @end
%%%-------------------------------------------------------------------
-module(session_manager).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore.hrl").

%% API
-export([reuse_or_create_session/3, remove_session/1]).

-export([create_gui_session/3]).

-define(TIMEOUT, timer:seconds(20)).
-define(SESSION_WORKER, session_manager_worker).

%%%===================================================================
%%% API
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Reuses active session or creates one for user with given identity.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(SessId :: session:id(),
    Iden :: session:identity(), Con :: pid()) ->
    {ok, reused | created} |{error, Reason :: term()}.
reuse_or_create_session(SessId, Iden, Con) ->
    worker_proxy:call(
        ?SESSION_WORKER,
        {reuse_or_create_session, SessId, Iden, Con},
        ?TIMEOUT
    ).

%%--------------------------------------------------------------------
%% @doc
%% Removes session identified by session ID.
%% @end
%%--------------------------------------------------------------------
-spec remove_session(SessId :: session:id()) -> ok | {error, Reason :: term()}.
remove_session(SessId) ->
    worker_proxy:call(
        ?SESSION_WORKER,
        {remove_session, SessId},
        ?TIMEOUT
    ).


% @todo Below functions must be integrated with current session logic.
% For now, they are only stubs.

-spec create_gui_session(Iden :: session:identity(),
    Macaroon :: macaroon:macaroon(), DischMacaroons :: macaroon:macaroon()) ->
    {ok, session:id()} | {error, Reason :: term()}.
create_gui_session(Iden, Macaroon, DischMacaroons) ->
    SessionId = datastore_utils:gen_uuid(),
    SessionRec = #session{
        identity = Iden,
        type = gui,
        macaroon = Macaroon,
        disch_macaroons = DischMacaroons
    },
    SessionDoc = #document{
        key = SessionId,
        value = SessionRec
    },
    case session:save(SessionDoc) of
        {ok, _} -> {ok, SessionId};
        {error, _} = Error -> Error
    end.
