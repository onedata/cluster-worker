%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an interface to a remote store driver.
%%% @end
%%%-------------------------------------------------------------------
-module(remote_driver).
-author("Krzysztof Trzepla").

-type ctx() :: any().
-type key() :: datastore:key().
-type doc() :: datastore:doc().
-type future() :: term().

-export_type([ctx/0, future/0]).

%%====================================================================
%% Callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously retrieves value from remote source.
%% @end
%%--------------------------------------------------------------------
-callback get_async(ctx(), key()) -> future().

%%--------------------------------------------------------------------
%% @doc
%% Waits for completion of an asynchronous operation.
%% @end
%%--------------------------------------------------------------------
-callback wait(future()) -> {ok, doc()} | {error, term()}.