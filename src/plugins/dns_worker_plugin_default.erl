%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Plugin for DNS worker. Accepts urls according to op-worker requirements.
%%% @end
%%%-------------------------------------------------------------------
-module(dns_worker_plugin_default).
-author("Michal Zmuda").

- behavior(dns_worker_plugin_behaviour).

-export([parse_domain/1]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% {@link dns_worker_plugin_behaviour} callback parse_domain/0.
%% @end
%%--------------------------------------------------------------------
-spec parse_domain(Domain :: string()) -> ok | refused | nx_domain.

parse_domain(_) ->
  ok.