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
-author("Lukasz Opiola").

-behaviour(cowboy_http_handler).

-include("global_definitions.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").

-export([init/3, handle/2, terminate/3]).
-export([get_cluster_status/1]).

-export_type([healthcheck_response/0]).

% ErrorDesc will appear in xml as node status.
-type healthcheck_response() :: ok | out_of_sync | {error, ErrorDesc :: atom()}.

-ifdef(TEST).
-compile(export_all).
-endif.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, no state is required
%% @end
%%--------------------------------------------------------------------
-spec init(term(), term(), term()) -> {ok, cowboy_req:req(), term()}.
init(_Type, Req, _Opts) ->
    {ok, Req, []}.


%%--------------------------------------------------------------------
%% @doc
%% Handles a request producing an XML response.
%% @end
%%--------------------------------------------------------------------
-spec handle(term(), term()) -> {ok, cowboy_req:req(), term()}.
handle(Req, State) ->
    {ok, Timeout} = application:get_env(?CLUSTER_WORKER_APP_NAME, nagios_healthcheck_timeout),
    {ok, CachingTime} = application:get_env(?CLUSTER_WORKER_APP_NAME, nagios_caching_time),
    CachedResponse = case application:get_env(?CLUSTER_WORKER_APP_NAME, nagios_cache) of
                         {ok, {LastCheck, LastValue}} ->
                             case utils:milliseconds_diff(now(), LastCheck) < CachingTime of
                                 true ->
                                     ?debug("Serving nagios response from cache"),
                                     {true, LastValue};
                                 false ->
                                     false
                             end;
                         _ ->
                             false
                     end,
    ClusterStatus = case CachedResponse of
                        {true, Value} ->
                            Value;
                        false ->
                            Status = get_cluster_status(Timeout),
                            % Save cluster state in cache, but only if there was no error
                            case Status of
                                {ok, {?CLUSTER_WORKER_APP_NAME, ok, _}} ->
                                    application:set_env(?CLUSTER_WORKER_APP_NAME, nagios_cache, {now(), Status});
                                _ ->
                                    skip
                            end,
                            Status
                    end,
    NewReq =
        case ClusterStatus of
            error ->
                {ok, Req2} = cowboy_req:reply(500, Req),
                Req2;
            {ok, {?CLUSTER_WORKER_APP_NAME, AppStatus, NodeStatuses}} ->
                MappedClusterState = lists:map(
                    fun({Node, NodeStatus, NodeComponents}) ->
                        NodeDetails = lists:map(
                            fun({Component, Status}) ->
                                StatusList = case Status of
                                                 {error, Desc} ->
                                                     "error: " ++ atom_to_list(Desc);
                                                 _ -> atom_to_list(Status)
                                             end,
                                {Component, [{status, StatusList}], []}
                            end, NodeComponents),
                        {ok, NodeName} = plugins:apply(node_manager_plugin, app_name, []),
                        {NodeName, [{name, atom_to_list(Node)}, {status, atom_to_list(NodeStatus)}], NodeDetails}
                    end, NodeStatuses),

                {{YY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(now()),
                DateString = str_utils:format("~4..0w/~2..0w/~2..0w ~2..0w:~2..0w:~2..0w", [YY, MM, DD, Hour, Min, Sec]),

                % Create the reply
                Healthdata = {healthdata, [{date, DateString}, {status, atom_to_list(AppStatus)}], MappedClusterState},
                Content = lists:flatten([Healthdata]),
                Export = xmerl:export_simple(Content, xmerl_xml),
                Reply = io_lib:format("~s", [lists:flatten(Export)]),

                % Send the reply
                {ok, Req2} = cowboy_req:reply(200, [{<<"content-type">>, <<"application/xml">>}], Reply, Req),
                Req2
        end,
    {ok, NewReq, State}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, no cleanup needed.
%% @end
%%--------------------------------------------------------------------
-spec terminate(term(), term(), term()) -> ok.
terminate(_Reason, _Req, _State) ->
    ok.


%% ====================================================================
%% Internal Functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Contacts all components of the cluster and produces a healthcheck report
%% in following form:
%% {op_worker, AppStatus, [
%%     {Node1, Node1Status, [
%%         {node_manager, NodeManStatus},
%%         {request_dispatcher, DispStatus},
%%         {Worker1, W1Status},
%%         {Worker2, W2Status},
%%         {Worker3, W3Status},
%%         {Listener1, L1Status},
%%         {Listener2, L2Status},
%%         {Listener3, L3Status}
%%     ]},
%%     {Node2, Node2Status, [
%%         ...
%%     ]}
%% ]}
%% Status can be: ok | out_of_sync | error | atom()
%%
%% If cluster manager cannot be contacted, the function returns 'error' atom.
%% @end
%%--------------------------------------------------------------------
-spec get_cluster_status(Timeout :: integer()) -> error | {ok, ClusterStatus} when
    Status :: healthcheck_response(),
    ClusterStatus :: {?CLUSTER_WORKER_APP_NAME, Status, NodeStatuses :: [
    {node(), Status, [
    {ModuleName :: module(), Status}
    ]}
    ]}.
get_cluster_status(Timeout) ->
    case check_cm(Timeout) of
        error ->
            error;
        Nodes ->
            try
                NodeManagerStatuses = check_node_managers(Nodes, Timeout),
                DistpatcherStatuses = check_dispatchers(Nodes, Timeout),
                Workers = [{Node, Name} || Node <- Nodes, Name <- node_manager:modules()],
                WorkerStatuses = check_workers(Nodes, Workers, Timeout),
                Listeners = [{Node, Name} || Node <- Nodes, Name <- node_manager:listeners()],
                ListenerStatuses = check_listeners(Nodes, Listeners, Timeout),
                {ok, _} = calculate_cluster_status(Nodes, NodeManagerStatuses, DistpatcherStatuses, WorkerStatuses, ListenerStatuses)
            catch
                Type:Error ->
                    ?error_stacktrace("Unexpected error during healthcheck: ~p:~p", [Type, Error]),
                    error
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates a proper term expressing cluster health,
%% constructing it from statuses of all components.
%% @end
%%--------------------------------------------------------------------
-spec calculate_cluster_status(Nodes, NodeManagerStatuses, DistpatcherStatuses, WorkerStatuses, ListenerStatuses) ->
    {ok, ClusterStatus} when
    Nodes :: [Node],
    NodeManagerStatuses :: [{Node, Status}],
    DistpatcherStatuses :: [{Node, Status}],
    WorkerStatuses :: [{Node, [{Worker :: atom(), Status}]}],
    ListenerStatuses :: [{Node, [{Listener :: atom(), Status}]}],
    Node :: node(),
    Status :: healthcheck_response(),
    ClusterStatus :: {?CLUSTER_WORKER_APP_NAME, Status, NodeStatuses :: [
    {node(), Status, [
    {ModuleName :: module(), Status}
    ]}
    ]}.
calculate_cluster_status(Nodes, NodeManagerStatuses, DistpatcherStatuses, WorkerStatuses, ListenerStatuses) ->
    NodeStatuses =
        lists:map(
            fun(Node) ->
                % Get all statuses for this node
                % They are all in form:
                % {ModuleName, ok | out_of_sync | {error, atom()}}
                AllStatuses = lists:flatten([
                    {?NODE_MANAGER_NAME, proplists:get_value(Node, NodeManagerStatuses)},
                    {?DISPATCHER_NAME, proplists:get_value(Node, DistpatcherStatuses)},
                    lists:usort(proplists:get_value(Node, WorkerStatuses)),
                    lists:usort(proplists:get_value(Node, ListenerStatuses))
                ]),
                % Calculate status of the whole node - it's the same as the worst status of any child
                % ok < out_of_sync < error
                % i. e. if any node component has an error, node's status will be 'error'.
                NodeStatus = lists:foldl(
                    fun({_, CurrentStatus}, Acc) ->
                        case {Acc, CurrentStatus} of
                            {ok, {error, _}} -> error;
                            {ok, OkOrOOS} -> OkOrOOS;
                            {out_of_sync, {error, _}} -> error;
                            {out_of_sync, _} -> out_of_sync;
                            _ -> error
                        end
                    end, ok, AllStatuses),
                {Node, NodeStatus, AllStatuses}
            end, Nodes),
    % Calculate status of the whole application - it's the same as the worst status of any node
    % ok > out_of_sync > Other (any other atom means an error)
    % If any node has an error, app's status will be 'error'.
    AppStatus = lists:foldl(
        fun({_, CurrentStatus, _}, Acc) ->
            case {Acc, CurrentStatus} of
                {ok, Any} -> Any;
                {out_of_sync, ok} -> out_of_sync;
                {out_of_sync, Any} -> Any;
                _ -> error
            end
        end, ok, NodeStatuses),
    % Sort node statuses by node name
    {ok, {?CLUSTER_WORKER_APP_NAME, AppStatus, lists:usort(NodeStatuses)}}.


%%--------------------------------------------------------------------
%% @doc
%% Contacts cluster manager for healthcheck and gathers information about cluster state.
%% @end
%%--------------------------------------------------------------------
-spec check_cm(Timeout :: integer()) -> Nodes :: [node()] | error.
check_cm(Timeout) ->
    try
        {ok, Nodes} = gen_server:call({global, ?CLUSTER_MANAGER}, healthcheck, Timeout),
        Nodes
    catch
        Type:Error ->
            ?error("Cluster manager error during healthcheck: ~p:~p", [Type, Error]),
            error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Contacts node managers on given nodes for healthcheck. The check is performed in parallel (one proces per node).
%% @end
%%--------------------------------------------------------------------
-spec check_node_managers(Nodes :: [atom()], Timeout :: integer()) -> [healthcheck_response()].
check_node_managers(Nodes, Timeout) ->
    utils:pmap(
        fun(Node) ->
            Result =
                try
                    gen_server:call({?NODE_MANAGER_NAME, Node}, healthcheck, Timeout)
                catch T:M ->
                    ?error("Connection error to ~p at ~p: ~p:~p", [?NODE_MANAGER_NAME, Node, T, M]),
                    {error, timeout}
                end,
            {Node, Result}
        end, Nodes).


%%--------------------------------------------------------------------
%% @doc
%% Contacts request dispatchers on given nodes for healthcheck. The check is performed in parallel (one proces per node).
%% @end
%%--------------------------------------------------------------------
-spec check_dispatchers(Nodes :: [atom()], Timeout :: integer()) -> [healthcheck_response()].
check_dispatchers(Nodes, Timeout) ->
    utils:pmap(
        fun(Node) ->
            Result =
                try
                    gen_server:call({?DISPATCHER_NAME, Node}, healthcheck, Timeout)
                catch T:M ->
                    ?error("Connection error to ~p at ~p: ~p:~p", [?DISPATCHER_NAME, Node, T, M]),
                    {error, timeout}
                end,
            {Node, Result}
        end, Nodes).


%%--------------------------------------------------------------------
%% @doc
%% Contacts workers on given nodes for healthcheck. The check is performed in parallel (one proces per worker).
%% Workers are grouped into one list per each node. Nodes without workers will have empty lists.
%% @end
%%--------------------------------------------------------------------
-spec check_workers(Nodes :: [atom()], Workers :: [{Node :: atom(), WorkerName :: atom()}], Timeout :: integer()) ->
    [{Node :: atom(), [{Worker :: atom(), Status :: healthcheck_response()}]}].
check_workers(Nodes, Workers, Timeout) ->
    WorkerStatuses = utils:pmap(
        fun({WNode, WName}) ->
            Result =
                try
                    worker_proxy:call({WName, WNode}, healthcheck, Timeout)
                catch T:M ->
                    ?error("Connection error to ~p at ~p: ~p:~p", [?DISPATCHER_NAME, WNode, T, M]),
                    {error, timeout}
                end,
            {WNode, WName, Result}
        end, Workers),
    arrange_by_node(Nodes, WorkerStatuses).


%%--------------------------------------------------------------------
%% @doc
%% Health-checks given listeners on given node. The check is performed in parallel (one process per listener).
%% Listeners are grouped into one list per each node. Nodes without workers will have empty lists.
%% @end
%%--------------------------------------------------------------------
-spec check_listeners(Nodes :: [atom()], Listeners :: [{Node :: atom(), ListenerName :: atom()}], Timeout :: integer()) ->
    [{Node :: atom(), [{Listener :: atom(), Status :: healthcheck_response()}]}].
check_listeners(Nodes, Listeners, Timeout) ->
    ListenerStatuses = utils:pmap(
        fun({LNode, LName}) ->
            Result =
                try
                    rpc:call(LNode, LName, healthcheck, [], Timeout)
                catch T:M ->
                    ?error("Connection error during check of listener ~p at ~p: ~p:~p", [LName, LNode, T, M]),
                    {error, timeout}
                end,
            {LNode, LName, Result}
        end, Listeners),
    arrange_by_node(Nodes, ListenerStatuses).


%%--------------------------------------------------------------------
%% @doc
%% Arranges given items (workers, listeners) by node and includes empty nodes.
%% @end
%%--------------------------------------------------------------------
-spec arrange_by_node(Nodes :: [atom()], ItemStatuses) ->
    [{Node :: atom(), [{Listener :: atom(), Status :: healthcheck_response()}]}] when
    ItemStatuses :: [{INode :: node(), IName :: atom(), Result :: healthcheck_response()}].
arrange_by_node(Nodes, ItemStatuses) ->
    ItemsByNode = lists:foldl(
        fun({INode, IName, Status}, Proplist) ->
            NewItemList = [{IName, Status} | proplists:get_value(INode, Proplist, [])],
            [{INode, NewItemList} | proplists:delete(INode, Proplist)]
        end, [], ItemStatuses),

    % If a has no listeners, it won't be on the ListenersByNode list, so lets add it.
    EmptyNodes = lists:foldl(
        fun(Node, Acc) ->
            case proplists:get_value(Node, ItemsByNode) of
                undefined -> [{Node, []} | Acc];
                _ -> Acc
            end
        end, [], Nodes),
    ItemsByNode ++ EmptyNodes.