%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% It is the behaviour of each listener attached by node manager.
%%% Listeners start just after node manager does  and they are stopped
%%% on node manager termination.
%%% Most often they are http or tcp listeners.
%%% @end
%%%-------------------------------------------------------------------
-module(listener_behaviour).
-author("Michal Zmuda").


%%--------------------------------------------------------------------
%% @doc
%% Returns the port on which listener listens.
%% @end
%%--------------------------------------------------------------------
-callback port() -> integer().


%%--------------------------------------------------------------------
%% @doc
%% Do your work & start it.
%% @end
%%--------------------------------------------------------------------
-callback start() -> ok | {error, Reason :: term()}.


%%--------------------------------------------------------------------
%% @doc
%% Returns the status of a listener.
%% @end
%%--------------------------------------------------------------------
-callback healthcheck() -> ok | {error, server_not_responding}.


%%--------------------------------------------------------------------
%% @doc
%% The listener will not be used anymore. Clean up!
%% @end
%%--------------------------------------------------------------------
-callback stop() -> ok | {error, Reason :: term()}.
