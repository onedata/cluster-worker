%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides DB API for record gs_subscription that holds
%%% information about clients subscribing for certain resource.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_subscription).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([get/2, update/4, create/1, list/0]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type ctx() :: datastore:ctx().
-type record() :: #gs_subscription{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-export_type([doc/0, diff/0]).

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
%% Returns Graph Sync subscription record.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_protocol:entity_type(), gs_protocol:entity_id()) ->
    {ok, doc()} | {error, term()}.
get(Type, Id) ->
    datastore_model:get(?CTX, id(Type, Id)).

%%--------------------------------------------------------------------
%% @doc
%% Updates existing Graph Sync subscription record or creates default one.
%% @end
%%--------------------------------------------------------------------
-spec update(gs_protocol:entity_type(), gs_protocol:entity_id(), diff(), record()) ->
    {ok, doc()} | {error, term()}.
update(Type, Id, Diff, Default) ->
    datastore_model:update(?CTX, id(Type, Id), Diff, Default).

%%--------------------------------------------------------------------
%% @doc
%% Creates Graph Sync subscription record.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, doc()} | {error, term()}.
create(Doc) ->
    datastore_model:create(?CTX, Doc).

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


-spec id(gs_protocol:entity_type(), gs_protocol:entity_id()) -> binary().
id(Type, Id) ->
    <<(atom_to_binary(Type, utf8))/binary, Id/binary>>.
