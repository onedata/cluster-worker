%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides DB API for record gs_session that holds information
%%% about a Graph Sync session.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_session).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([create/1, get/1, delete/1, update/3, list/0]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type record() :: #gs_session{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-export_type([diff/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined,
    fold_enabled => true
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates Graph Sync session record.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, doc()} | {error, term()}.
create(Doc) ->
    datastore_model:create(?CTX, Doc).

%%--------------------------------------------------------------------
%% @doc
%% Returns Graph Sync session record.
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes Graph Sync session record.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Updates existing Graph Sync session record or creates default one.
%% @end
%%--------------------------------------------------------------------
-spec update(key(), diff(), record()) -> {ok, doc()} | {error, term()}.
update(Key, Diff, Default) ->
    datastore_model:update(?CTX, Key, Diff, Default).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [doc()]} | {error, term()}.
list() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.

