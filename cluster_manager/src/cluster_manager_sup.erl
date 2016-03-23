%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% It is the main supervisor. It starts (as it child) cluster manager
%%% @end
%%%-------------------------------------------------------------------
-module(cluster_manager_sup).
-author("Tomasz Lichon").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, {SupFlags :: supervisor:sup_flags(), [ChildSpec :: supervisor:child_spec()]}}.
init([]) ->
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, [
        cluster_manager_spec()
    ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a worker child_spec for a cluster manager child.
%% @end
%%--------------------------------------------------------------------
-spec cluster_manager_spec() -> supervisor:child_spec().
cluster_manager_spec() ->
    #{
        id => cluster_manager_server,
        start => {cluster_manager_server, start_link, []},
        restart => permanent,
        shutdown => timer:seconds(5),
        type => worker,
        modules => [cluster_manager_server]
    }.