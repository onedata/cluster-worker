%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements supervisor behaviour and is responsible
%%% for supervising and restarting CouchBase worker pool.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_pool_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

-include("global_definitions.hrl").

%% API
-export([start_link/0]).
-export([register_pool/2, get_pool/1, unregister_pool/1]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase worker pool supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    supervisor:start_link(?MODULE, []).

%%--------------------------------------------------------------------
%% @doc
%% Registers CouchBase worker pool manager.
%% @end
%%--------------------------------------------------------------------
-spec register_pool(datastore_pool:mode(), pid()) -> ok.
register_pool(Mode, Pid) ->
    ets:insert(datastore_pools, {Mode, Pid}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns CouchBase worker pool manager.
%% @end
%%--------------------------------------------------------------------
-spec get_pool(datastore_pool:mode()) -> pid().
get_pool(Mode) ->
    ets:lookup_element(datastore_pools, Mode, 2).

%%--------------------------------------------------------------------
%% @doc
%% Unregisters CouchBase worker pool manager.
%% @end
%%--------------------------------------------------------------------
-spec unregister_pool(datastore_pool:mode()) -> ok.
unregister_pool(Mode) ->
    ets:delete(datastore_pools, Mode),
    ok.

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
    Size = application:get_env(?CLUSTER_WORKER_APP_NAME, datastore_pool_size, 5),
    ets:new(datastore_pools, [public, named_table, {read_concurrency, true}]),
    {ok, {#{strategy => one_for_one, intensity => 3, period => 1},
        lists:map(fun({Mode, Delay}) ->
            datastore_pool_spec(Mode, Size, Delay)
        end, datastore_pool:modes())
    }}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a worker child_spec for a CouchBase worker pool manager.
%% @end
%%--------------------------------------------------------------------
-spec datastore_pool_spec(datastore_pool:mode(), non_neg_integer(),
    non_neg_integer()) -> supervisor:child_spec().
datastore_pool_spec(Mode, Size, Delay) ->
    #{
        id => Mode,
        start => {datastore_pool, start_link, [Mode, Size, Delay]},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [datastore_pool]
    }.
