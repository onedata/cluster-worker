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
-export([register_worker/4, get_worker/3, unregister_worker/4]).

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
-spec register_worker(couchbase_config:bucket(), couchbase_pool:mode(),
    couchbase_pool_worker:id(), pid()) -> ok.
register_worker(Bucket, Mode, Id, Worker) ->
    ets:insert(couchbase_pool_workers, {{Bucket, Mode, Id}, Worker}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns CouchBase pool worker.
%% @end
%%--------------------------------------------------------------------
get_worker(Bucket, Mode, Id) ->
    ets:lookup_element(couchbase_pool_workers, {Bucket, Mode, Id}, 2).

%%--------------------------------------------------------------------
%% @doc
%% Unregisters CouchBase pool worker.
%% @end
%%--------------------------------------------------------------------
-spec unregister_worker(couchbase_config:bucket(), couchbase_pool:mode(),
    couchbase_pool_worker:id(), pid()) -> ok.
unregister_worker(Bucket, Mode, Id, Worker) ->
    ets:delete_object(couchbase_pool_workers, {{Bucket, Mode, Id}, Worker}),
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
    ets:new(couchbase_pool_workers, [
        public, named_table, {read_concurrency, true}
    ]),
    ets:new(couchbase_pool_stats, [
        public, named_table, {write_concurrency, true}
    ]),

    DbHosts = couchbase_config:get_hosts(),
    {ok, {#{strategy => one_for_one, intensity => 3, period => 1},
        lists:foldl(fun(Bucket, Specs) ->
            lists:foldl(fun(Mode, Specs2) ->
                lists:foldl(fun(Id, Specs3) ->
                    [worker_spec(Bucket, Mode, Id, DbHosts) | Specs3]
                end, Specs2, lists:seq(1, couchbase_pool:get_size(Mode)))
            end, Specs, couchbase_pool:get_modes())
        end, [], couchbase_config:get_buckets())
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
-spec worker_spec(couchbase_config:bucket(), couchbase_pool:type(),
    couchbase_pool_worker:id(), [couchbase_driver:db_host()]) ->
    supervisor:child_spec().
worker_spec(Bucket, Mode, Id, DbHosts) ->
    Args = [Bucket, Mode, Id, DbHosts],
    #{
        id => {Bucket, Mode, Id},
        start => {couchbase_pool_worker, start_link, Args},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [couchbase_pool_worker]
    }.
