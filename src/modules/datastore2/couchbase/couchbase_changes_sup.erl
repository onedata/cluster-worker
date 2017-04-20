%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements supervisor behaviour and is responsible
%%% for supervising and restarting CouchBase changes processors.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_changes_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

-include("global_definitions.hrl").

%% API
-export([start_link/0]).
-export([start_worker/2, stop_worker/2]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase changes supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase changes processor.
%% @end
%%--------------------------------------------------------------------
-spec start_worker(couchbase_driver:bucket(), datastore:scope()) ->
    {ok, pid()} | {error, Reason :: term()}.
start_worker(Bucket, Scope) ->
    Spec = couchbase_changes_processor_spec(Bucket, Scope),
    supervisor:start_child(?MODULE, Spec).

%%--------------------------------------------------------------------
%% @doc
%% Stops CouchBase changes processor.
%% @end
%%--------------------------------------------------------------------
-spec stop_worker(couchbase_driver:bucket(), datastore:scope()) ->
    ok | {error, Reason :: term()}.
stop_worker(Bucket, Scope) ->
    supervisor:terminate_child(?MODULE, {Bucket, Scope}).

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
    {ok, {#{strategy => one_for_one, intensity => 3, period => 1}, [
        couchbase_changes_stream_sup_spec()
    ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a supervisor child_spec for a CouchBase changes worker.
%% @end
%%--------------------------------------------------------------------
-spec couchbase_changes_processor_spec(couchbase_driver:bucket(),
    datastore:scope()) -> supervisor:child_spec().
couchbase_changes_processor_spec(Bucket, Scope) ->
    #{
        id => {Bucket, Scope},
        start => {couchbase_changes_processor, start_link, [Bucket, Scope]},
        restart => transient,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [couchbase_changes_processor]
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a supervisor child_spec for a CouchBase changes streamer supervisor.
%% @end
%%--------------------------------------------------------------------
-spec couchbase_changes_stream_sup_spec() -> supervisor:child_spec().
couchbase_changes_stream_sup_spec() ->
    #{
        id => couchbase_changes_stream_sup,
        start => {couchbase_changes_stream_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [couchbase_changes_stream_sup]
    }.