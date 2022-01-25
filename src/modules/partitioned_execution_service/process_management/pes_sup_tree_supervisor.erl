%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Root supervisor to be used when more than one supervisor is
%%% configured by plug-in
%%% (see pes_plugin_behaviour:get_supervisor_count/0).
%%% @end
%%%-------------------------------------------------------------------
-module(pes_sup_tree_supervisor).
-author("Michal Wrzeszcz").


-behaviour(supervisor).


%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% Supervisor spec
-export([sup_flags/0, child_specs/0, child_spec/1]).

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
%%% NOTE: supervisor is one_for_one so static list of children
%%% returned by child_specs/1 is empty. child_spec/1 is used when
%%% starting children dynamically.
%%%===================================================================

-spec sup_flags() -> supervisor:sup_flags().
sup_flags() ->
    #{strategy => one_for_one, intensity => 1, period => 5}.


-spec child_specs() -> [supervisor:child_spec()].
child_specs() ->
    [].


-spec child_spec(name()) -> supervisor:child_spec().
child_spec(Name) ->
    #{
        id => Name,
        start => {pes_server_supervisor, start_link, [Name]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [pes_server_supervisor]
    }.