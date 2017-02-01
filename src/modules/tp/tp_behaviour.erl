%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an interface for a transaction process logic.
%%% @end
%%%-------------------------------------------------------------------
-module(tp_behaviour).
-author("Krzysztof Trzepla").

%%--------------------------------------------------------------------
%% @doc
%% Initializes the transaction process state.
%% @end
%%--------------------------------------------------------------------
-callback init(tp:args()) -> {ok, tp:init()} | {error, Reason :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Processes list of pending requests and returns a list of responses.
%% The order of responses must match the order of requests. This callback is
%% called in a separate process, however it is guaranteed that there will be
%% only one process at the time running modify/2 callback. Next handling process
%% will be spawned after completion of a currently running and only if there are
%% pending requests.
%% @end
%%--------------------------------------------------------------------
-callback modify([tp:request()], tp:data()) ->
    {[tp:response()], tp:changes(), tp:data()}.

%%--------------------------------------------------------------------
%% @doc
%% Merges changes from calls to modify/2 or commit/2 callbacks.
%% @end
%%--------------------------------------------------------------------
-callback merge_changes(Previous :: tp:changes(), Next :: tp:changes()) ->
    tp:changes().

%%--------------------------------------------------------------------
%% @doc
%% Commits changes. Should return 'true' if all changes has been successfully
%% committed, otherwise 'false' and uncommitted changes. This callback is
%% called in a separate process, as it is assumed that this operation may be
%% long running. However, it is guaranteed that there will be only one process
%% at the time running commit/2 callback.
%% @end
%%--------------------------------------------------------------------
-callback commit(tp:changes(), tp:data()) -> true | {false, tp:changes()}.

%%--------------------------------------------------------------------
%% @doc
%% Called in case of unsuccessful commit operation. Should return next commit
%% delay.
%% @end
%%--------------------------------------------------------------------
-callback commit_backoff(timeout()) -> timeout().

%%--------------------------------------------------------------------
%% @doc
%% Called on transaction process termination. It is guaranteed that this
%% callback will be called when there are no pending requests and all
%% modifications have been applied and committed.
%% @end
%%--------------------------------------------------------------------
-callback terminate(tp:data()) -> any().
