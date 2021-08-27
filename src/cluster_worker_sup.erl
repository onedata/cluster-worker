%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc It is the main supervisor. It starts (as it child) node manager
%%% which initializes node.
%%% @end
%%%--------------------------------------------------------------------
-module(cluster_worker_sup).
-author("Michal Wrzeszcz").

-behaviour(supervisor).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    ?info("Starting cluster-worker supervisor..."),
    Name = {local, ?CLUSTER_WORKER_APPLICATION_SUPERVISOR_NAME},
    supervisor:start_link(Name, ?MODULE, []).

%%%===================================================================
%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    node_cache:init(),
    global_clock:try_to_restore_previous_synchronization(),
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, [
        cluster_worker_specs:node_manager_spec(),
        cluster_worker_specs:request_dispatcher_spec()
    ]}}.
