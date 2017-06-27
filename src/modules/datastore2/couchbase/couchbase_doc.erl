%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides CouchBase document management functions.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_doc).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").

%% API
-export([set_mutator/2, set_prefix/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Stores mutator in a document.
%% @end
%%--------------------------------------------------------------------
-spec set_mutator(couchbase_driver:ctx(), datastore:document()) -> datastore:document().
set_mutator(#{mutator := Mutator}, #document{mutator = Mutators} = Doc) ->
    Length = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_mutator_history_length, 20),
    Doc#document{mutator = lists:sublist([Mutator | Mutators], Length)};
set_mutator(_Ctx, Doc) ->
    Doc.

%%--------------------------------------------------------------------
%% @doc
%% Adds prefix to a key.
%% @end
%%--------------------------------------------------------------------
-spec set_prefix(couchbase_driver:ctx() | datastore_cache:ctx(),
    datastore:key()) -> datastore:key().
set_prefix(#{prefix := <<_/binary>> = Prefix}, Key) ->
    <<Prefix/binary, "-", Key/binary>>;
set_prefix(_Ctx, Key) ->
    Key.