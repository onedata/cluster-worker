%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This test verifies if the nagios endpoint works as expected.
%%% @end
%%%--------------------------------------------------------------------
-module(nagios_test_SUITE).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("op_test_utils.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([nagios_test/1]).

-perf_test({perf_cases, []}).
all() -> [nagios_test].

% Path to nagios endpoint
-define(HEALTHCHECK_PATH, "https://127.0.0.1:443/nagios").
% How many retries should be performed if nagios endpoint is not responding
-define(HEALTHCHECK_RETRIES, 10).
% How often should the retries be performed
-define(HEALTHCHECK_RETRY_PERIOD, 500).

%%%===================================================================
%%% Test function
%%%===================================================================
nagios_test(Config) ->
    [Worker1, _, _] = WorkerNodes = ?config(op_worker_nodes, Config),

    {ok, "200", _, XMLString} = rpc:call(Worker1, ibrowse, send_req, [?HEALTHCHECK_PATH, [], get]),

    {Xml, _} = xmerl_scan:string(XMLString),

    [MainStatus] = [X#xmlAttribute.value || X <- Xml#xmlElement.attributes, X#xmlAttribute.name == status],
    ?assertEqual(MainStatus, "ok"),

    NodeStatuses = [X || X <- Xml#xmlElement.content, X#xmlElement.name == oneprovider_node],

    WorkersByNodeXML = lists:map(
        fun(#xmlElement{attributes = Attributes, content = Content}) ->
            [NodeName] = [X#xmlAttribute.value || X <- Attributes, X#xmlAttribute.name == name],
            WorkerNames = [X#xmlElement.name || X <- Content, X#xmlElement.name /= ?DISPATCHER_NAME, X#xmlElement.name /= ?NODE_MANAGER_NAME],
            {NodeName, WorkerNames}
        end, NodeStatuses),

    % Check if all nodes are in the report.
    lists:foreach(
        fun(Node) ->
            ?assertNotEqual(undefined, proplists:get_value(atom_to_list(Node), WorkersByNodeXML))
        end, WorkerNodes),

    % Check if all workers are in the report.
    {Workers, _} = gen_server:call({global, ?CCM}, get_workers, 1000),
    lists:foreach(
        fun({WNode, WName}) ->
            WorkersOnNode = proplists:get_value(atom_to_list(WNode), WorkersByNodeXML),
            ?assertEqual(true, lists:member(WName, WorkersOnNode))
        end, Workers),

    % Check if every node's status contains dispatcher and node manager status
    lists:foreach(
        fun(#xmlElement{content = Content}) ->
            ?assertMatch([?NODE_MANAGER_NAME], [X#xmlElement.name || X <- Content, X#xmlElement.name == ?NODE_MANAGER_NAME]),
            ?assertMatch([?DISPATCHER_NAME], [X#xmlElement.name || X <- Content, X#xmlElement.name == ?DISPATCHER_NAME])
        end, NodeStatuses).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_testcase(nagios_test, Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_testcase(nagios_test, Config) ->
    test_node_starter:clean_environment(Config).