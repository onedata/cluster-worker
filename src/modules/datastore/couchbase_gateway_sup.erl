%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements supervisor behaviour and is responsible
%%% for supervising and restarting couchbase gateway workers.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_gateway_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

-include("modules/datastore/datastore_common.hrl").

%% API
-export([start_link/0]).
-export([register_gateway/3, get_gateway/1, get_gateway/2,
    unregister_gateway/2]).

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

%%--------------------------------------------------------------------
%% @doc
%% Registers couchbase gateway port.
%% @end
%%--------------------------------------------------------------------
-spec register_gateway(datastore:db_node(), datastore:bucket(), integer()) ->
    ok.
register_gateway(DbNode, Bucket, Port) ->
    ets:insert(?COUCHBASE_GATEWAYS, {{DbNode, Bucket}, Port}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns port of a random couchbase gateway connected to provided bucket.
%% @end
%%--------------------------------------------------------------------
-spec get_gateway(datastore:bucket()) ->
    {ok, Port :: integer()} | {error, not_found}.
get_gateway(Bucket) ->
    case ets:match(?COUCHBASE_GATEWAYS, {{'_', Bucket}, '$1'}) of
        [] -> {error, not_found};
        Ports -> {ok, hd(utils:random_element(Ports))}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns port of a couchbase gateway connected to provided couchbase node
%% and bucket.
%% @end
%%--------------------------------------------------------------------
-spec get_gateway(datastore:db_node(), datastore:bucket()) ->
    {ok, Port :: integer()} | {error, not_found}.
get_gateway(DbNode, Bucket) ->
    case ets:lookup(?COUCHBASE_GATEWAYS, {DbNode, Bucket}) of
        [] -> {error, not_found};
        [{{DbNode, Bucket}, Port}] -> {ok, Port}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Unregisters couchbase gateway.
%% @end
%%--------------------------------------------------------------------
-spec unregister_gateway(datastore:db_node(), datastore:bucket()) -> ok.
unregister_gateway(DbNode, Bucket) ->
    ets:delete(?COUCHBASE_GATEWAYS, {DbNode, Bucket}),
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
    ets:new(?COUCHBASE_GATEWAYS, [
        set,
        public,
        named_table,
        {read_concurrency, true}
    ]),

    DbNodes = datastore_config:db_nodes(),
    Buckets = couchdb_datastore_driver:get_buckets(),
    {ok, {#{strategy => one_for_one, intensity => 3, period => 1},
        lists:foldl(fun(DbNode, Acc) ->
            lists:foldl(fun(Bucket, Acc2) ->
                [couchbase_gateway_spec(DbNode, Bucket) | Acc2]
            end, Acc, Buckets)
        end, [], DbNodes)
    }}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a worker child_spec for a couchbase gateway.
%% @end
%%--------------------------------------------------------------------
-spec couchbase_gateway_spec(datastore:db_node(), datastore:bucket()) ->
    supervisor:child_spec().
couchbase_gateway_spec(DbNode, Bucket) ->
    #{
        id => {DbNode, Bucket},
        start => {couchbase_gateway, start_link, [DbNode, Bucket]},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [couchbase_gateway]
    }.