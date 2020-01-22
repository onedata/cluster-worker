%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides CouchBase configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_config).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").

%% API
-export([get_hosts/0, get_buckets/0, get_flush_queue_size/0]).

-type host() :: binary().
-type bucket() :: binary().

-export_type([host/0, bucket/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns list of CouchBase hosts.
%% @end
%%--------------------------------------------------------------------
-spec get_hosts() -> [host()].
get_hosts() ->
    {ok, Nodes} = plugins:apply(node_manager_plugin, db_nodes, []),
    lists:map(fun(Node) ->
        [Host, _Port] = binary:split(atom_to_binary(Node, utf8), <<":">>),
        Host
    end, Nodes).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of CouchBase buckets.
%% @end
%%--------------------------------------------------------------------
-spec get_buckets() -> [bucket()].
get_buckets() ->
    case application:get_env(?CLUSTER_WORKER_APP_NAME, couchbase_buckets) of
        {ok, Buckets} ->
            Buckets;
        _ ->
            DbHost = lists_utils:random_element(get_hosts()),
            Url = <<DbHost/binary, ":8091/pools/default/buckets">>,
            {ok, 200, _, Body} = http_client:get(Url),
            Ans = lists:map(fun(BucketMap) ->
                maps:get(<<"name">>, BucketMap)
            end, jiffy:decode(Body, [return_maps])),
            catch application:set_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_buckets, Ans),
            Ans
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns size of couchbase disk write queue.
%% @end
%%--------------------------------------------------------------------
-spec get_flush_queue_size() -> non_neg_integer().
get_flush_queue_size() ->
    DbHost = lists_utils:random_element(get_hosts()),
    Buckets = get_buckets(),

    lists:foldl(fun(Bucket, Max) ->
        Url = <<DbHost/binary, ":8091/pools/default/buckets/",
            Bucket/binary, "/stats">>,
        {ok, 200, _, Body} = http_client:get(Url),
        BucketSize = lists:last(maps:get(<<"disk_write_queue">>,
            maps:get(<<"samples">>,
                maps:get(<<"op">>, jiffy:decode(Body, [return_maps]))))),
        max(BucketSize, Max)
    end, 0, Buckets).