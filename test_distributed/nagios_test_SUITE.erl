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

-include("test_utils.hrl").
-include("registered_names.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([nagios_test/1]).

all() -> [nagios_test].

% How many retries sohuld be performed if nagios endpoint is not responding
-define(HEALTHCHECK_RETRIES, 10).
% How often should the retries be performed
-define(HEALTHCHECK_RETRY_PERIOD, 500).

%%%===================================================================
%%% Test function
%%%====================================================================
nagios_test(Config) ->
    [Worker1, _, _] = WorkerNodes = ?config(op_worker_nodes, Config),

    %todo integrate with test_utils
    cluster_state_notifier:cast({subscribe_for_init, self(), length(WorkerNodes)}),
    receive
        init_finished -> ok
    after
        50000 -> throw(timeout)
    end,

    {ok, XMLString} = perform_nagios_healthcheck(Worker1),

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
            ?assert(lists:member(WName, WorkersOnNode))
        end, Workers),

    % Check if every node's status contains dispatcher and node manager status
    lists:foreach(
        fun(#xmlElement{content = Content}) ->
            ?assertMatch([?NODE_MANAGER_NAME], [X#xmlElement.name || X <- Content, X#xmlElement.name == ?NODE_MANAGER_NAME]),
            ?assertMatch([?DISPATCHER_NAME], [X#xmlElement.name || X <- Content, X#xmlElement.name == ?DISPATCHER_NAME])
        end, NodeStatuses).


perform_nagios_healthcheck(Node) ->
    perform_nagios_healthcheck(Node, ?HEALTHCHECK_RETRIES).

perform_nagios_healthcheck(_, 0) ->
    {error, max_retries_reached};

perform_nagios_healthcheck(Node, Retries) ->
    case rpc:call(Node, ibrowse, send_req, ["https://127.0.0.1:443/nagios", [], get]) of
        {ok, "200", _, Response} ->
            {ok, Response};
        {ok, OtherCode, Headers, Response} ->
            {error, {wrong_nagios_response, [
                {code, OtherCode},
                {headers, Headers},
                {body, Response}
            ]}};
        _ ->
            timer:sleep(?HEALTHCHECK_RETRY_PERIOD),
            perform_nagios_healthcheck(Node, Retries - 1)
    end.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_testcase(nagios_test, Config) ->
    ?INIT_CODE_PATH(Config),
    test_node_starter:prepare_test_environment(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_testcase(nagios_test, Config) ->
    ok.
%%     test_node_starter:clean_environment(Config).