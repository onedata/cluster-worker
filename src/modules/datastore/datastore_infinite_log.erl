%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides datastore model API for infinite log.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_infinite_log).
-author("Michal Stanisz").

-type ctx() :: datastore_model:ctx().
-type key() :: datastore_model:key().

-export_type([key/0, ctx/0]).

%% API
-export([create/2, create/3, destroy/2, append/3, list/2, list/3, set_ttl/3]). 

%%%===================================================================
%%% API
%%%===================================================================

-spec create(ctx(), key()) -> ok | {error, term()}.
create(Ctx, Key) ->
    create(Ctx, Key, #{}).

-spec create(ctx(), key(), infinite_log:log_opts()) -> ok | {error, term()}.
create(Ctx, Key, Opts) ->
    datastore_model:datastore_apply(Ctx, Key,
        fun datastore:infinite_log_operation/4, [?FUNCTION_NAME, [Opts]]).


-spec destroy(ctx(), key()) -> ok | {error, term()}.
destroy(Ctx, Key) ->
    datastore_model:datastore_apply(Ctx, Key,
        fun datastore:infinite_log_operation/4, [?FUNCTION_NAME, []]).


-spec append(ctx(), key(), infinite_log:content()) -> ok | {error, term()}.
append(Ctx, Key, Content) ->
    datastore_model:datastore_apply(Ctx, Key,
        fun datastore:infinite_log_operation/4, [?FUNCTION_NAME, [Content]]).


-spec list(ctx(), key()) ->
    {ok, infinite_log_browser:listing_result()} | {error, term()}.
list(Ctx, Key) ->
    list(Ctx, Key, #{}).

-spec list(ctx(), key(), infinite_log_browser:listing_opts()) ->
    {ok, infinite_log_browser:listing_result()} | {error, term()}.
list(Ctx, Key, Opts) ->
    datastore_model:datastore_apply(Ctx, Key,
        fun datastore:infinite_log_operation/4, [?FUNCTION_NAME, [Opts]]).


-spec set_ttl(ctx(), key(), time:seconds()) -> ok | {error, term()}.
set_ttl(Ctx, Key, Ttl) ->
    datastore_model:datastore_apply(Ctx, Key,
        fun datastore:infinite_log_operation/4, [?FUNCTION_NAME, [Ttl]]).

