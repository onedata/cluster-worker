%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% It is the behaviour of each module providing definitions of helathcheck
%%% of endpoints.
%%% @end
%%%-------------------------------------------------------------------
-module(endpoint_healthcheck_behaviour).
-author("Michal Zmuda").

%%--------------------------------------------------------------------
%% @doc
%% Healthcheck given endpoint.
%% Used in {@link http_worker_plugin_behaviour} callback healthcheck_endpoints/0.
%% @end
%%--------------------------------------------------------------------
-callback healthcheck(Endpoint :: atom()) -> ok | {error, Reason :: atom()}.