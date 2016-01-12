%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Contains child specs for cluster-worker (or applications based
%%%      on it).
%%% @end
%%%--------------------------------------------------------------------
-module(cluster_worker_specs).
-author("Michal Zmuda").

-include("global_definitions.hrl").

%% API
-export([main_worker_sup_spec/0, request_dispatcher_spec/0, node_manager_spec/0]).

%%--------------------------------------------------------------------
%% @doc
%% Creates a supervisor child_spec for a main worker supervisor child.
%% To be started by application based on cluster-worker.
%% @end
%%--------------------------------------------------------------------
-spec main_worker_sup_spec() -> supervisor:child_spec().
main_worker_sup_spec() ->
  Id = Module = ?MAIN_WORKER_SUPERVISOR_NAME,
  Restart = permanent,
  Shutdown = infinity,
  Type = supervisor,
  {Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}.

%%--------------------------------------------------------------------
%% @doc
%% Creates a worker child_spec for a request dispatcher child.
%% Started by cluster-worker.
%% @end
%%--------------------------------------------------------------------
-spec request_dispatcher_spec() -> supervisor:child_spec().
request_dispatcher_spec() ->
  Id = Module = request_dispatcher,
  Restart = permanent,
  Shutdown = timer:seconds(5),
  Type = worker,
  {Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}.

%%--------------------------------------------------------------------
%% @doc
%% Creates a worker child_spec for a node manager child.
%% Started by cluster-worker.
%% @end
%%--------------------------------------------------------------------
-spec node_manager_spec() -> supervisor:child_spec().
node_manager_spec() ->
  Id = Module = node_manager,
  Restart = permanent,
  Shutdown = timer:seconds(5),
  Type = worker,
  {Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}.