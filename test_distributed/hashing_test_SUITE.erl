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
    AllNodes = [FirstNode | _] = ?config(cluster_worker_nodes, Config),

    % Randomize some labels
    Labels = lists:map(fun
        (I) when I rem 4 == 0 -> str_utils:rand_hex(16);
        (I) when I rem 4 == 1 -> {some, <<"object">>, str_utils:rand_hex(2)};
        (I) when I rem 4 == 2 -> rand:uniform(10000);
        (I) when I rem 4 == 3 -> ["a", b, {c, d}, e, rand:uniform(10000)]
    end, lists:seq(1, 10000)),

    % Check label mapping to a single node
    NodesChosenForLabels = lists:map(fun(Label) ->
        Node = rpc:call(FirstNode, consistent_hashing, get_node, [Label]),
        % The same label should yield the same node
        ?assertEqual(Node, rpc:call(FirstNode, consistent_hashing, get_node, [Label])),
        ?assert(erlang:is_atom(Node)),
        ?assert(lists:member(Node, AllNodes)),
        Node
    end, Labels),

    % Check label mapping to with get_full_node_info
    lists:foreach(fun(Label) ->
        lists:foreach(fun(NodesCount) ->
            rpc:call(FirstNode, consistent_hashing, set_label_associated_nodes_count, [NodesCount]),
            {Associated, Failed, All} = Nodes = rpc:call(FirstNode, consistent_hashing, get_full_node_info, [Label]),
            % The same label should yield the same nodes
            ?assertEqual(Nodes, rpc:call(FirstNode, consistent_hashing, get_full_node_info, [Label])),
            % TODO - uncomment after VFS-6209
%%            ?assertEqual(NodesCount, length(Associated)),
            ?assertEqual(lists:sort(AllNodes), All),
            lists:foreach(fun(Node) ->
                ?assert(erlang:is_atom(Node)),
                ?assert(lists:member(Node, AllNodes))
            end, Associated ++ Failed)
        end, lists:seq(1, length(AllNodes)))
    end, Labels),

    % At 10000 different labels, at least one should have landed on each node
    lists:foreach(fun(Node) ->
        lists:member(Node, NodesChosenForLabels)
    end, AllNodes).