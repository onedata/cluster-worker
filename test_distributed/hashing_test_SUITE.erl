%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This test verifies if the consistent hashing ring is initialized
%%% correctly
%%% @end
%%%--------------------------------------------------------------------
-module(hashing_test_SUITE).
-author("Tomasz Lichon").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0]).
-export([test_hashing/1]).

all() -> ?ALL([test_hashing]).

%%%===================================================================
%%% Test functions
%%%===================================================================

test_hashing(Config) ->
    Workers = [Worker1 | _] = ?config(cluster_worker_nodes, Config),

    ?assertEqual(lists:usort(Workers), rpc:call(Worker1, consistent_hashing, get_all_nodes, [])),
    NodeOfUuid1 = rpc:call(Worker1, consistent_hashing, get_node, [<<"uuid1">>]),
    NodeOfUuid2 = rpc:call(Worker1, consistent_hashing, get_node, [<<"uuid2">>]),
    NodeOfObject = rpc:call(Worker1, consistent_hashing, get_node, [{some, <<"object">>}]),

    ?assert(erlang:is_atom(NodeOfUuid1)),
    ?assert(erlang:is_atom(NodeOfUuid2)),
    ?assert(erlang:is_atom(NodeOfObject)),
    ?assert(lists:member(NodeOfUuid1, Workers)),
    ?assert(lists:member(NodeOfUuid2, Workers)),
    ?assert(lists:member(NodeOfObject, Workers)),
    ?assertNotEqual(NodeOfUuid1, NodeOfUuid2).