%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Supervisor for pes_server processes. It can be used as root supervisor
%%% when single supervisor is configured by plug-in
%%% (see pes_plugin_behaviour:get_supervisor_count/0). If many supervisors
%%% are required, pes_server_supervisors are managed internally by pes
%%% framework - see pes_process_manager.erl.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_server_supervisor).
-author("Michal Wrzeszcz").


-behaviour(supervisor).


%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% Supervisor spec
-export([sup_flags/0, child_specs/0]).


-type name() :: atom().
-export_type([name/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link(name()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Name) ->
    supervisor:start_link({local, Name}, ?MODULE, []).


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

-spec init(Args :: term()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(_Args) ->
    {ok, {sup_flags(), child_specs()}}.


%%%===================================================================
%%% Supervisor spec
%%%===================================================================

-spec sup_flags() -> supervisor:sup_flags().
sup_flags() ->
    #{strategy => simple_one_for_one, intensity => 1, period => 5}.


-spec child_specs() -> [supervisor:child_spec()].
child_specs() ->
    [#{
        id => pes_server,
        start => {pes_server, start_link, []},
        restart => transient,
        shutdown => infinity,
        type => worker,
        modules => [pes_server]
    }].