%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Supervisor for pes_server processes. There can be several
%%% pes_supervisors. In such a case single supervisor is responsible
%%% for a range of keys (see pes_router.erl).
%%% @end
%%%-------------------------------------------------------------------
-module(pes_supervisor).
-author("Michal Wrzeszcz").


-behaviour(supervisor).


%% API
-export([start_link/1, spec/1]).

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


-spec spec(name()) -> supervisor:child_spec().
spec(Name) ->
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
%%% Internal functions
%%%===================================================================

%% @private
-spec spec() -> supervisor:sup_flags().
spec() ->
    #{strategy => simple_one_for_one, intensity => 1, period => 5}.


%% @private
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