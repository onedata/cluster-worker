%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is the supervisor for worker_host usage. It is started as a brother
%%% of worker_host, under main_worker_sup. All permanent processes
%%% started by worker_host should be children of this supervisor.
%%% Every worker has its own supervisor, registered as ${worker_name}_sup,
%%% i.e. dns_worker <-> dns_worker_sup
%%% @end
%%%-------------------------------------------------------------------
-module(worker_host_sup).
-author("Tomasz Lichon").

-behaviour(supervisor).

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%% @end
%%--------------------------------------------------------------------
-spec start_link(Name :: atom(), Args :: term()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(Name, Args) ->
    supervisor:start_link({local, Name}, ?MODULE, Args).

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
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, {SupFlags :: supervisor:sup_flags(), [ChildSpec :: supervisor:child_spec()]}}.
init(Args) ->
    SupervisorSpec = proplists:get_value(supervisor_flags, Args, #{
        strategy => one_for_all, intensity => 1000, period => 3600
    }),
    ChildrenSpec = proplists:get_value(supervisor_children_spec, Args, []),

    {ok, {SupervisorSpec, ChildrenSpec}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
