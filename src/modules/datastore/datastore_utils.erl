%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides datastore utility functions.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_utils).
-author("Krzysztof Trzepla").

%% API
-export([set_expiry/2]).

-type ctx() :: datastore:ctx().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sets expiry field in context.
%% @end
%%--------------------------------------------------------------------
-spec set_expiry(ctx() | couchbase_driver:ctx(), non_neg_integer()) ->
    ctx() | couchbase_driver:ctx().
set_expiry(Ctx, Expiry) when Expiry =< 2592000 ->
    Ctx#{expiry => Expiry};
set_expiry(Ctx, Expiry) ->
    os:timestamp(),
    Ctx#{expiry => erlang:system_time(second) + Expiry}.
