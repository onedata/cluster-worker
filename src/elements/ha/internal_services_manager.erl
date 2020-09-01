%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used to manage high level permanent services.
%%% The service is started at node chosen using consistent hashing. If node fails,
%%% service is restarted at slave node. The module bases on the fact that
%%% datastore provides HA for documents storing (see ha_datastore.hrl).
%%% @end
%%%-------------------------------------------------------------------
-module(internal_services_manager).
-author("Michał Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_service/6, start_service/3, start_service/4,
    stop_service/3, report_service_stop/3, takeover/1, migrate_to_recovered_master/1,
    do_healthcheck/3, get_processing_node/1]).
%% Export for internal rpc
-export([start_service_locally/4, init_service/4]).

-define(LOCAL_NODE_SELECTOR, <<>>). % Forces choice of local node instead use of consistent_hashing

% Type used by consistent hashing to chose node where service should be running.
-type node_selector() :: binary().
-type node_id() :: binary().
-type service_init_fun() :: apply_start_fun | apply_takeover_fun.
-export_type([node_id/0]).

%%%===================================================================
%%% Message sending API
%%%===================================================================

-spec start_service(module(), internal_service:service_name(), internal_service:service_fun_name(),
    internal_service:service_fun_name(), internal_service:service_fun_args(), node_selector()) ->
    ok | aborted | {error, term()}.
start_service(Module, Name, Fun, StopFun, Args, NodeSelector) ->
    Options = #{
        start_function => Fun,
        stop_function => StopFun,
        start_function_args => Args
    },
    start_service(Module, Name, NodeSelector, Options).

-spec start_service(module(), internal_service:service_name(), internal_service:options()) ->
    ok | aborted | {error, term()}.
start_service(Module, Name, ServiceDescription) ->
    start_service(Module, Name, ?LOCAL_NODE_SELECTOR, ServiceDescription).

-spec start_service(module(), internal_service:service_name(), node_selector(), internal_service:options()) ->
    ok | aborted | {error, term()}.
start_service(Module, Name, NodeSelector, ServiceDescription) ->
    {MasterNode, HandlingNode} = get_master_and_handling_nodes(NodeSelector),
    case rpc:call(HandlingNode, ?MODULE, start_service_locally, [MasterNode, Module, Name, ServiceDescription]) of
        {badrpc, Reason} -> {error, Reason};
        Other -> Other
    end.

-spec stop_service(module(), internal_service:service_name(), node_selector()) -> ok | {error, term()}.
stop_service(Module, Name, NodeSelector) ->
    {MasterNode, _} = get_master_and_handling_nodes(NodeSelector),
    MasterNodeId = get_node_id(MasterNode),
    ServiceName = get_internal_name(Module, Name),
    case get_service_and_processing_node(ServiceName, MasterNodeId) of
        {undefined, _} ->
            ok;
        {Service, ProcessingNode} ->
            ok = internal_service:apply_stop_fun(ProcessingNode, Service),
            remove_service_from_doc(MasterNodeId, ServiceName)
    end.

-spec report_service_stop(module(), internal_service:service_name(), node_selector()) -> ok.
report_service_stop(Module, Name, NodeSelector) ->
    {MasterNode, _} = get_master_and_handling_nodes(NodeSelector),
    MasterNodeId = get_node_id(MasterNode),
    ServiceName = get_internal_name(Module, Name),
    remove_service_from_doc(MasterNodeId, ServiceName).

-spec takeover(node()) -> ok | no_return().
takeover(FailedNode) ->
    NewNode = node(),
    Diff = fun(Record) ->
        {ok, Record#node_internal_services{processing_node = NewNode}}
    end,
    MasterNodeId = get_node_id(FailedNode),
    case node_internal_services:update(MasterNodeId, Diff) of
        {ok, #document{value = #node_internal_services{services = Services}}} ->
            lists:foreach(fun({ServiceName, Service}) ->
                ?info("Starting takeover of service ~s (module ~p) from node ~ts",
                    [ServiceName, internal_service:get_module(Service), FailedNode]),
                init_service(Service, ServiceName, apply_takeover_fun, MasterNodeId),
                ?info("Finished takeover of service ~s (module ~p) from node ~ts",
                    [ServiceName, internal_service:get_module(Service), FailedNode])
            end, maps:to_list(Services));
        {error, not_found} ->
            ok
    end.

-spec migrate_to_recovered_master(node()) -> ok | no_return().
migrate_to_recovered_master(MasterNode) ->
    Diff = fun(Record) ->
        {ok, Record#node_internal_services{processing_node = MasterNode}}
    end,
    MasterNodeId = get_node_id(MasterNode),
    case node_internal_services:update(MasterNodeId, Diff) of
        {ok, #document{value = #node_internal_services{services = Services}}} ->
            lists:foreach(fun({ServiceName, Service}) ->
                ?info("Starting migration of service ~s (module ~p) to node ~ts",
                    [ServiceName, internal_service:get_module(Service), MasterNode]),
                ok = internal_service:apply_migrate_fun(Service),
                rpc:call(MasterNode, ?MODULE, init_service, [Service, ServiceName, apply_start_fun, MasterNodeId]),
                ?info("Finished migration of service ~s (module ~p) to node ~ts",
                    [ServiceName, internal_service:get_module(Service), MasterNode])
            end, maps:to_list(Services));
        {error, not_found} ->
            ok
    end.

-spec do_healthcheck(internal_service:service_name(), node_id(), non_neg_integer()) ->
    {ok, NewInterval :: non_neg_integer()} | ignore.
do_healthcheck(ServiceName, MasterNodeId, LastInterval) ->
    try
        case get_service_and_processing_node(ServiceName, MasterNodeId) of
            {Service, Node} when Service =:= undefined orelse Node =/= node() ->
                ignore;
            {Service, _} ->
                do_healthcheck_insecure(ServiceName, Service, MasterNodeId, LastInterval)
        end
    catch
        _:Reason ->
            % Error can appear during restart and node switching
            ?debug("Service healthcheck error ~p", [Reason]),
            {ok, LastInterval}
    end.

-spec get_processing_node(node_selector()) -> node().
get_processing_node(NodeSelector) ->
    {MasterNode, _} = get_master_and_handling_nodes(NodeSelector),
    MasterNodeId = get_node_id(MasterNode),
    {ok, #document{value = #node_internal_services{processing_node = ProcessingNode}}} =
        node_internal_services:get(MasterNodeId),
    ProcessingNode.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec start_service_locally(node(), module(), internal_service:service_name(),
    internal_service:options()) -> ok | aborted | {error, term()}.
start_service_locally(MasterNode, Module, Name, ServiceDescription) ->
    Service = internal_service:new(Module, ServiceDescription),
    AllowOverride = internal_service:is_override_allowed(ServiceDescription),
    ServiceName = get_internal_name(Module, Name),

    Default = #node_internal_services{services = #{ServiceName => Service}, processing_node = node()},
    Diff = fun(#node_internal_services{services = Services} = Record) ->
        case {maps:is_key(ServiceName, Services), AllowOverride} of
            {true, false} -> {error, already_started};
            _ -> {ok, Record#node_internal_services{services = Services#{ServiceName => Service}}}
        end
    end,

    MasterNodeId = get_node_id(MasterNode),
    case node_internal_services:update(MasterNodeId, Diff, Default) of
        {ok, _} -> init_service(Service, ServiceName, apply_start_fun, MasterNodeId);
        {error, already_started} -> ok
    end.

-spec get_node_id(node()) -> node_id().
get_node_id(Node) ->
    atom_to_binary(Node, utf8).

-spec get_master_and_handling_nodes(node_selector()) -> {MasterNode :: node(), HandlingNode :: node()}.
get_master_and_handling_nodes(NodeSelector) ->
    case NodeSelector of
        ?LOCAL_NODE_SELECTOR ->
            {node(), node()};
        _ ->
            {datastore_key:primary_responsible_node(NodeSelector),
                datastore_key:any_responsible_node(NodeSelector)}
    end.

-spec get_internal_name(module(), internal_service:service_name()) -> internal_service:service_name().
get_internal_name(Module, Name) ->
    <<(atom_to_binary(Module, utf8))/binary, "###", Name/binary>>.

-spec get_service_and_processing_node(internal_service:service_name(), node_id()) ->
    {internal_service:service() | undefined, node()}.
get_service_and_processing_node(ServiceName, MasterNodeId) ->
    {ok, #document{value = #node_internal_services{services = NodeServices, processing_node = ProcessingNode}}} =
        node_internal_services:get(MasterNodeId),

    {maps:get(ServiceName, NodeServices, undefined), ProcessingNode}.

-spec init_service(internal_service:service(), internal_service:service_name(), service_init_fun(), node_id()) ->
    ok | aborted.
init_service(Service, ServiceName, InitFun, MasterNodeId) ->
    try
        case internal_service:InitFun(Service) of
            ok ->
                HealthcheckInterval = internal_service:get_healthcheck_interval(Service),
                ok = node_manager:init_service_healthcheck(ServiceName, MasterNodeId, HealthcheckInterval);
            abort ->
                remove_service_from_doc(MasterNodeId, ServiceName),
                aborted
        end
    catch
        Error:Reason ->
            ?error_stacktrace("Error while initializing service ~p: ~p~p",
                [ServiceName, Error, Reason]),
            remove_service_from_doc(MasterNodeId, ServiceName),
            throw(Reason)
    end.

-spec remove_service_from_doc(node_id(), internal_service:service_name()) -> ok.
remove_service_from_doc(MasterNodeId, ServiceName) ->
    Diff = fun(#node_internal_services{services = Services} = Record) ->
        {ok, Record#node_internal_services{services = maps:remove(ServiceName, Services)}}
    end,
    case node_internal_services:update(MasterNodeId, Diff) of
        {ok, _} -> ok;
        {error, not_found} -> ok
    end.

-spec do_healthcheck_insecure(internal_service:service_name(), internal_service:service(), node_id(),
    non_neg_integer()) -> {ok, NewInterval :: non_neg_integer()} | ignore.
do_healthcheck_insecure(ServiceName, Service, MasterNodeId, LastInterval) ->
    case internal_service:apply_healthcheck_fun(Service, LastInterval) of
        {error, undefined_fun} ->
            ignore;
        {restart, _} ->
            % Check once more in case of race with migration
            case get_service_and_processing_node(ServiceName, MasterNodeId) of
                {undefined, _} ->
                    ignore; % service has been stopped
                {_, OtherNode} when OtherNode =/= node() ->
                    ignore; % service has been migrated to other node
                _ ->
                    case internal_service:apply_start_fun(Service) of
                        ok ->
                            % If the service started, interval can be different
                            % than returned from the first call of healthcheck function
                            case internal_service:apply_healthcheck_fun(Service, LastInterval) of
                                {error, undefined_fun} ->
                                    ignore;
                                {_, NewInterval} ->
                                    {ok, NewInterval}
                            end;
                        abort ->
                            remove_service_from_doc(MasterNodeId, ServiceName),
                            ignore
                    end
            end;
        {ok, NewInterval} ->
            {ok, NewInterval}
    end.