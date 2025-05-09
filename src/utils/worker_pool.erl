%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Wrapper for library providing pool of workers.
%%% Currently used library is worker_pool.
%%% https://github.com/inaka/worker_pool
%%% functions from wpool should be called only via this API.
%%% @end
%%%-------------------------------------------------------------------
-module(worker_pool).
-author("Jakub Kudzia").

-include_lib("ctool/include/logging.hrl").

%% API
-export([
    start_sup_pool/2, stop_sup_pool/1,
    call/2, call/3, call/4,
    cast/2, cast/3,
    stats/0, stats/1,
    broadcast/2,
    default_strategy/0
]).


-type name() :: wpool:name().
-type option() :: wpool:option().
-type strategy() :: wpool:strategy().
-type stats() :: wpool:stats().
-type request() :: term().
-type response() :: term().

-export_type([name/0, option/0, strategy/0, request/0, response/0, stats/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts a pool of N wpool_processes supervised by wpool_sup.
%% All options are described on
%% http://inaka.github.io/worker_pool/worker_pool/wpool.html
%% The most important are:
%%      *  {workers, integer() >= 1} - specify no. of workers
%%      *  {worker_type, gen_fsm | gen_server} - specify type of worker
%%      *  {worker, {Module :: atom(), InitArg :: term()}}
%%          - specify worker's module
%%          - by default it's wpool_worker/wpool_worker_fsn
%% @end
%%--------------------------------------------------------------------
-spec start_sup_pool(name(), [option()]) ->
    {ok, pid()} | {error, {already_started, pid()} | term()}.
start_sup_pool(PoolName, Options) ->
    Size = proplists:get_value(workers, Options, 100),  % the default in wpool
    ?info("Starting worker pool: '~ts' (size: ~B)", [PoolName, Size]),
    wpool:start_sup_pool(PoolName, Options).


%%--------------------------------------------------------------------
%% @doc
%% Stops supervised pool.
%% @equiv wpool:stop_sup_pool(PoolName).
%% @end
%%--------------------------------------------------------------------
-spec stop_sup_pool(name()) -> ok.
stop_sup_pool(PoolName) ->
    ?info("Stopping worker pool: '~ts'", [PoolName]),
    wpool:stop_sup_pool(PoolName).


%%--------------------------------------------------------------------
%% @doc
%% Performs call to worker from the pool with default strategy
%% available_worker.
%% @equiv wpool:call(PoolName, Call).
%% @end
%%--------------------------------------------------------------------
-spec call(name(), request()) -> response().
call(PoolName, Call) ->
    wpool:call(PoolName, Call).


%%--------------------------------------------------------------------
%% @doc
%% Performs call to worker from the pool with default timeout = 5s.
%% @equiv wpool:call(PoolName, Call, Strategy).
%% @end
%%--------------------------------------------------------------------
-spec call(name(), request(), strategy()) -> response().
call(PoolName, Call, Strategy) ->
    wpool:call(PoolName, Call, Strategy).


%%--------------------------------------------------------------------
%% @doc
%% Performs call to worker from the pool.
%% @equiv wpool:call(PoolName, Call, Strategy, Timeout).
%% @end
%%--------------------------------------------------------------------
-spec call(name(), request(), strategy(), timeout()) -> response().
call(PoolName, Call, Strategy, Timeout) ->
    wpool:call(PoolName, Call, Strategy, Timeout).


%%--------------------------------------------------------------------
%% @doc
%% Performs cast to worker from the pool with default strategy
%% available_worker.
%% @equiv wpool:cast(PoolName, Cast).
%% @end
%%--------------------------------------------------------------------
-spec cast(name(), request()) -> response().
cast(PoolName, Cast) ->
    wpool:cast(PoolName, Cast).


%%--------------------------------------------------------------------
%% @doc
%% Performs cast to worker from the pool with.
%% @equiv wpool:cast(PoolName, Call, Strategy).
%% @end
%%--------------------------------------------------------------------
-spec cast(name(), request(), strategy()) -> response().
cast(PoolName, Call, Strategy) ->
    wpool:cast(PoolName, Call, Strategy).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a snapshot of the pools' stats
%% @equiv wpool:stat()
%% @end
%%--------------------------------------------------------------------
-spec stats() -> [stats()].
stats() ->
    wpool:stats().


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a snapshot of a given pool stats
%% @equiv wpool:stat()
%% @end
%%--------------------------------------------------------------------
-spec stats(name()) -> stats().
stats(PoolName) ->
    wpool:stats(PoolName).


%%--------------------------------------------------------------------
%% @doc
%% Casts a message to all the workers within the given pool.
%% @equiv wpool:broadcast(PoolName, Cast).
%% @end
%%--------------------------------------------------------------------
-spec broadcast(name(), request()) -> response().
broadcast(PoolName, Cast) ->
    wpool:broadcast(PoolName, Cast).


%%--------------------------------------------------------------------
%% @doc
%% Default strategy
%% @equiv wpool:default_strategy()
%% @end
%%--------------------------------------------------------------------
-spec default_strategy() -> strategy().
default_strategy() ->
    wpool:default_strategy().

