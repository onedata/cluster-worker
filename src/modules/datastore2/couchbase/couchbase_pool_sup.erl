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
-module(couchbase_pool_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

-include("global_definitions.hrl").

%% API
-export([start_link/0]).
-export([register_pool/3, get_pool/2, unregister_pool/2]).

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
-spec register_pool(couchbase_driver:bucket(), couchbase_pool:mode(), pid()) ->
    ok.
register_pool(Bucket, Mode, Pid) ->
    ets:insert(couchbase_pools, {{Bucket, Mode}, Pid}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns CouchBase worker pool manager.
%% @end
%%--------------------------------------------------------------------
-spec get_pool(couchbase_driver:bucket(), couchbase_pool:mode()) -> pid().
get_pool(Bucket, Mode) ->
    ets:lookup_element(couchbase_pools, {Bucket, Mode}, 2).

%%--------------------------------------------------------------------
%% @doc
%% Unregisters CouchBase worker pool manager.
%% @end
%%--------------------------------------------------------------------
-spec unregister_pool(couchbase_driver:bucket(), couchbase_pool:mode()) -> ok.
unregister_pool(Bucket, Mode) ->
    ets:delete(couchbase_pools, {Bucket, Mode}),
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
    DbHosts = datastore_config2:get_db_hosts(),
    Buckets = couchbase_driver:get_buckets(),
    Size = application:get_env(?CLUSTER_WORKER_APP_NAME, couchbase_pool_size, 1),
    ets:new(couchbase_pools, [public, named_table, {read_concurrency, true}]),
    {ok, {#{strategy => one_for_one, intensity => 3, period => 1},
        lists:foldl(fun(Mode, Specs) ->
            lists:foldl(fun(Bucket, Specs2) ->
                [couchbase_pool_spec(Bucket, Mode, DbHosts, Size) | Specs2]
            end, Specs, Buckets)
        end, [], couchbase_pool:get_modes())
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
-spec couchbase_pool_spec(couchbase_driver:bucket(), couchbase_pool:type(),
    [couchbase_driver:db_host()], non_neg_integer()) -> supervisor:child_spec().
couchbase_pool_spec(Bucket, Mode, DbHosts, Size) ->
    #{
        id => {Bucket, Mode},
        start => {couchbase_pool, start_link, [Bucket, Mode, DbHosts, Size]},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [couchbase_pool]
    }.