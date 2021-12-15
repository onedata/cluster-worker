%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Supervisor used during partition execution service tests
%%% (see pes_test_SUITE) when supervision tree for pes_servers
%%% is used instead of single supervisor.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_root_supervisor).
-author("Michal Wrzeszcz").


-behaviour(supervisor).


%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).


-type name() :: atom().
-export_type([name/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link(Name :: name()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(Name) ->
  supervisor:start_link({local, Name}, ?MODULE, []).


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

-spec init(Args :: term()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(_Args) ->
  {ok, {spec(), children_spec()}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec spec() -> supervisor:sup_flags().
spec() ->
  #{strategy => one_for_one, intensity => 1, period => 5}.


%% @private
-spec children_spec() -> [supervisor:child_spec()].
children_spec() ->
  [].