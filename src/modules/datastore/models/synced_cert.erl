%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Contains data about certificates to be synchronised among nodes.
%%% @end
%%%-------------------------------------------------------------------
-module(synced_cert).
-author("Michal Zmuda").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([get/1, create/1]).

%% datastore_model callbacks
-export([get_record_struct/1]).

-type key() :: datastore:key().
-type record() :: #synced_cert{}.
-type doc() :: datastore_doc:doc(record()).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns synchronised certificates entry.
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Creates synchronised certificates entry.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, doc()} | {error, term()}.
create(Doc) ->
    datastore_model:create(?CTX, Doc).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of model in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {cert_file_content, binary},
        {key_file_content, binary}
    ]}.
