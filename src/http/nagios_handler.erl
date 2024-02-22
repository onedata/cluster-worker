%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module handles Nagios monitoring requests.
%%% @end
%%%--------------------------------------------------------------------
-module(nagios_handler).
-behaviour(cowboy_handler).

-author("Lukasz Opiola").


-include("global_definitions.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/global_definitions.hrl").

-export([init/2]).

-define(NAGIOS_HEALTHCHECK_TIMEOUT,
    application:get_env(?CLUSTER_WORKER_APP_NAME, nagios_healthcheck_timeout, timer:seconds(10))).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback.
%% Handles a request returning current cluster status.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), term()) -> {ok, cowboy_req:req(), term()}.
init(#{method := <<"GET">>} = Req, State) ->
    NewReq = case node_manager:get_cluster_status(?NAGIOS_HEALTHCHECK_TIMEOUT) of
        {error, _} ->
            cowboy_req:reply(500, Req);
        {ok, {AppStatus, NodeStatuses}} ->
            Reply = format_reply(AppStatus, NodeStatuses),
            cowboy_req:reply(200,
                #{?HDR_CONTENT_TYPE => <<"application/xml">>}, Reply, Req
            )
    end,
    {ok, NewReq, State};
init(Req, State) ->
    NewReq = cowboy_req:reply(405, #{?HDR_ALLOW => <<"GET">>}, Req),
    {ok, NewReq, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec format_reply(cluster_status:status(), [cluster_status:node_status()]) -> io_lib:chars().
format_reply(AppStatus, NodeStatuses) ->
    MappedClusterState = lists:map(fun({Node, NodeStatus, NodeComponents}) ->
        NodeDetails = lists:map(fun({Component, Status}) ->
            StatusList = case Status of
                {error, Desc} -> "error: " ++ atom_to_list(Desc);
                A when is_atom(A) -> atom_to_list(A);
                _ ->
                    ?debug("Wrong nagios status: {~p, ~p} at node ~p", [Component, Status, Node]),
                    "error: wrong_status"
            end,
            {Component, [{status, StatusList}], []}
        end, NodeComponents),
        NodeName = plugins:apply(node_manager_plugin, app_name, []),
        {NodeName, [{name, atom_to_list(Node)}, {status, atom_to_list(NodeStatus)}], NodeDetails}
    end, NodeStatuses),

    {{YY, MM, DD}, {Hour, Min, Sec}} = time:seconds_to_datetime(global_clock:timestamp_seconds()),
    DateString = str_utils:format("~4..0w/~2..0w/~2..0w ~2..0w:~2..0w:~2..0w", [YY, MM, DD, Hour, Min, Sec]),

    % Create the reply
    HealthData = {healthdata, [{date, DateString},
        {status, atom_to_list(AppStatus)}], MappedClusterState},
    Export = xmerl:export_simple([HealthData], xmerl_xml),
    io_lib:format("~s", [Export]).
