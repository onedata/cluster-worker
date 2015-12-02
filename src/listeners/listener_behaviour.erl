%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% It is the behaviour of each cluster listener.
%%% @end
%%%-------------------------------------------------------------------
-module(listener_behaviour).
-author("Michal Zmuda").


%%--------------------------------------------------------------------
%% @doc
%% Do your work & start it.
%% @end
%%--------------------------------------------------------------------
-callback start() -> ok | {error, Reason :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% The listener will not be used anymore. Clean up!
%% @end
%%--------------------------------------------------------------------
-callback stop() -> ok | {error, Reason :: term()}.
