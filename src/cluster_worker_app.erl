%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc It is the main module of application. It lunches
%%% supervisor which then initializes appropriate components of node.
%%% @end
%%%--------------------------------------------------------------------
-module(cluster_worker_app).
-author("Michal Zmuda").

-behaviour(application).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts application by supervisor initialization.
%% @end
%%--------------------------------------------------------------------
-spec start(_StartType :: application:start_type(), _StartArgs :: term()) ->
    {ok, Pid :: pid()} | {ok, Pid :: pid(), State :: term()} |
    {error, Reason ::term()}.
start(_StartType, _StartArgs) ->
    test_node_starter:maybe_start_cover(),
    {ok, SchedulersMonitoring} = application:get_env(?CLUSTER_WORKER_APP_NAME, schedulers_monitoring),
    erlang:system_flag(scheduler_wall_time, SchedulersMonitoring),
    cluster_worker_sup:start_link().

%%--------------------------------------------------------------------
%% @doc
%% Stops application.
%% @end
%%--------------------------------------------------------------------
-spec stop(State :: term()) -> ok.
stop(_State) ->
%%    wait_for_cache(), TODO - uncomment after resolving VFS-3040
    test_node_starter:maybe_stop_cover(),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%%TODO - uncomment after resolving VFS-3040
%% @private
%% @doc
%% Waits until cache is dumped to db.
%% @end
%%--------------------------------------------------------------------
%%-spec wait_for_cache() -> ok.
%%wait_for_cache() ->
%%    case caches_controller:wait_for_cache_dump() of
%%        ok ->
%%            ok;
%%        _Error ->
%%            timer:sleep(timer:minutes(1)),
%%            wait_for_cache()
%%    end.