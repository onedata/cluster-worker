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
%% @private
%% @doc
%% Parses the DNS query domain and check if it ends with correct domain.
%% Accepts only domains that fulfill above condition and have a
%% maximum of one part subdomain.
%% Returns NXDOMAIN when the query domain has more parts.
%% Returns REFUSED when query domain is not like intended.
%% @end
%%--------------------------------------------------------------------
-callback parse_domain(Domain :: string()) -> ok | refused | nx_domain.
