%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements supervisor behaviour and is responsible
%%% for supervising and restarting datastore pool workers.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_pool_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

-include("global_definitions.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the couchbase_gateway supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    supervisor:start_link(?MODULE, []).

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
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    {ok, Size} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        datastore_pool_size),
    {ok, {#{strategy => one_for_one, intensity => 3, period => 1},
        lists:map(fun(N) ->
            datastore_pool_worker_spec(N)
        end, lists:seq(1, Size))
    }}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a worker child_spec for a datastore pool worker.
%% @end
%%--------------------------------------------------------------------
-spec datastore_pool_worker_spec(integer()) -> supervisor:child_spec().
datastore_pool_worker_spec(Id) ->
    #{
        id => Id,
        start => {datastore_pool_worker, start_link, []},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [datastore_pool_worker]
    }.