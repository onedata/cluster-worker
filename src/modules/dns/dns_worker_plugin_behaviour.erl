%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is used to inject configuration to stock dns of cluster_worker.
%%% @end
%%%-------------------------------------------------------------------
-module(dns_worker_plugin_behaviour).
-author("Michal Zmuda").

%%--------------------------------------------------------------------
%% @doc
%% Resolve DNS queries with load balancing advices in mind.
%% Examine {@link dns_handler_behaviour} for info about types of
%% queries to be handled.
%% @end
%%--------------------------------------------------------------------
-callback resolve(Method :: atom(), Domain :: string(), LbAdvice :: term()) ->
    dns_handler_behaviour:handler_reply().
