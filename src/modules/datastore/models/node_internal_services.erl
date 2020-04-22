%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model used to store information needed to restart permanent services
%%% in case of node failure (see internal_services_manager.erl)
%%% @end
%%%-------------------------------------------------------------------
-module(node_internal_services).
-author("Michał Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([get/1, update/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type record() :: #node_internal_services{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-define(CTX, #{
    model => ?MODULE
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

-spec update(key(), diff(), record() | doc()) -> {ok, doc()} | {error, term()}.
update(Key, Diff, Default) ->
    datastore_model:update(?CTX, Key, Diff, Default).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.

-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {services, [{record, [
            {module, atom},
            {function, atom},
            {stop_function, atom},
            {args, binary},
            {hashing_key, string}
        ]}]}
    ]}.