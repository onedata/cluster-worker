%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Supervisor for pes_server processes. It can be used as root supervisor
%%% when single processes' group is required by executor
%%% (see pes_executor_behaviour:get_server_groups_count/0) or as processes'
%%% group's supervisor when more then one group is required (group supervisors
%%% are managed internally by pes framework - see pes_process_manager.erl).
%%% @end
%%%-------------------------------------------------------------------
-module(pes_supervisor).
-author("Michal Wrzeszcz").


-behaviour(supervisor).


%% API
-export([start_link/1, child_spec/1]).

%% Supervisor callbacks
-export([init/1]).

%% Spec exported to allow usage of pes_supervisor as root supervisor
-export([spec/0, children_spec/0]).


-type name() :: atom().
-export_type([name/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link(Name :: name()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(Name) ->
    supervisor:start_link({local, Name}, ?MODULE, []).


-spec child_spec(name()) -> supervisor:child_spec().
child_spec(Name) ->
    #{
        id => Name,
        start => {pes_supervisor, start_link, [Name]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [pes_supervisor]
    }.


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

-spec init(Args :: term()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(_Args) ->
    {ok, {spec(), children_spec()}}.


%%%===================================================================
%%% Spec exported to allow usage of pes_supervisor as root supervisor
%%%===================================================================

-spec spec() -> supervisor:sup_flags().
spec() ->
    #{strategy => simple_one_for_one, intensity => 1, period => 5}.


-spec children_spec() -> [supervisor:child_spec()].
children_spec() ->
    [#{
        id => pes_server,
        start => {pes_server, start_link, []},
        restart => transient,
        shutdown => infinity,
        type => worker,
        modules => [pes_server]
    }].