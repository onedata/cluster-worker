%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements supervisor behaviour and is responsible
%%% for supervising and restarting CouchBase changes streams.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_changes_stream_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([start_worker/5, stop_worker/1]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase changes stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase changes stream worker.
%% @end
%%--------------------------------------------------------------------
-spec start_worker(couchbase_config:bucket(), datastore_doc:scope(),
    couchbase_changes:callback(), proplists:proplist(), [pid()]) ->
    {ok, pid()} | {error, Reason :: term()}.
start_worker(Bucket, Scope, Callback, Opts, LinkedProcesses) ->
    supervisor:start_child(?MODULE, [Bucket, Scope, Callback, Opts, LinkedProcesses]).

%%--------------------------------------------------------------------
%% @doc
%% Stops CouchBase changes stream worker.
%% @end
%%--------------------------------------------------------------------
-spec stop_worker(pid()) -> ok | {error, Reason :: term()}.
stop_worker(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).

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
    {ok, {#{strategy => simple_one_for_one, intensity => 3, period => 1}, [
        couchbase_changes_stream_spec()
    ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a worker child_spec for a CouchBase changes stream.
%% @end
%%--------------------------------------------------------------------
-spec couchbase_changes_stream_spec() -> supervisor:child_spec().
couchbase_changes_stream_spec() ->
    #{
        id => couchbase_changes_stream,
        start => {couchbase_changes_stream, start_link, []},
        restart => temporary,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [couchbase_changes_stream]
    }.