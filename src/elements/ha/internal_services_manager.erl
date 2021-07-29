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
-export([start_service/6, start_service/3, start_service/4]).
-export([reschedule_healthcheck/4]).
-export([stop_service/3, report_service_stop/3]).
-export([takeover/1, migrate_to_recovered_master/1]).
-export([do_healthcheck/3, get_processing_node/1]).
%% Export for internal rpc
-export([start_service_locally/4, init_service/4]).

-define(LOCAL_NODE_SELECTOR, <<>>). % Forces choice of local node instead use of consistent_hashing

% Type used by consistent hashing to chose node where service should be running.
-type node_selector() :: binary().
-type node_id() :: binary().
-type service_init_fun() :: apply_start_fun | apply_takeover_fun.
-export_type([node_id/0]).

% Concatenation of module and service name, used to identify a service across this module
-opaque unique_service_name() :: binary().
-export_type([unique_service_name/0]).

%%%===================================================================
%%% Message sending API
%%%===================================================================

-spec start_service(module(), internal_service:service_name(), internal_service:service_fun_name(),
    internal_service:service_fun_name(), internal_service:service_fun_args(), node_selector()) ->
    ok | aborted | no_return().
start_service(Module, ServiceName, Fun, StopFun, Args, NodeSelector) ->
    Options = #{
        start_function => Fun,
        stop_function => StopFun,
        start_function_args => Args
    },
    start_service(Module, ServiceName, NodeSelector, Options).

-spec start_service(module(), internal_service:service_name(), internal_service:options()) ->
    ok | aborted | {error, term()}.
start_service(Module, ServiceName, ServiceDescription) ->
    start_service(Module, ServiceName, ?LOCAL_NODE_SELECTOR, ServiceDescription).

-spec start_service(module(), internal_service:service_name(), node_selector(), internal_service:options()) ->
    ok | aborted | no_return().
start_service(Module, ServiceName, NodeSelector, ServiceDescription) ->
    {MasterNode, HandlingNode} = get_master_and_handling_nodes(NodeSelector),
    case rpc:call(HandlingNode, ?MODULE, start_service_locally, [MasterNode, Module, ServiceName, ServiceDescription]) of
        ok -> ok;
        aborted -> aborted;
        {badrpc, Reason} -> error(Reason);
        {error, Reason} -> error(Reason)
    end.

-spec reschedule_healthcheck(module(), internal_service:service_name(), node_selector(),
    internal_service:healthcheck_interval()) -> ok | ignored.
reschedule_healthcheck(Module, ServiceName, NodeSelector, NewInterval) ->
    {MasterNode, _} = get_master_and_handling_nodes(NodeSelector),
    MasterNodeId = get_node_id(MasterNode),
    UniqueName = gen_unique_service_name(Module, ServiceName),
    case get_service_and_processing_node(UniqueName, MasterNodeId) of
        stopped ->
            ignored;
        {_, ProcessingNode} ->
            node_manager:reschedule_service_healthcheck(ProcessingNode, UniqueName, MasterNodeId, NewInterval)
    end.

-spec stop_service(module(), internal_service:service_name(), node_selector()) -> ok | {error, term()}.
stop_service(Module, ServiceName, NodeSelector) ->
    {MasterNode, _} = get_master_and_handling_nodes(NodeSelector),
    MasterNodeId = get_node_id(MasterNode),
    UniqueName = gen_unique_service_name(Module, ServiceName),
    case get_service_and_processing_node(UniqueName, MasterNodeId) of
        stopped ->
            ok;
        {Service, ProcessingNode} ->
            ok = internal_service:apply_stop_fun(ProcessingNode, Service),
            remove_service_from_doc(MasterNodeId, UniqueName)
    end.

-spec report_service_stop(module(), internal_service:service_name(), node_selector()) -> ok.
report_service_stop(Module, ServiceName, NodeSelector) ->
    {MasterNode, _} = get_master_and_handling_nodes(NodeSelector),
    MasterNodeId = get_node_id(MasterNode),
    UniqueName = gen_unique_service_name(Module, ServiceName),
    remove_service_from_doc(MasterNodeId, UniqueName).

-spec takeover(node()) -> ok | no_return().
takeover(FailedNode) ->
    NewNode = node(),
    Diff = fun(Record) ->
        {ok, Record#node_internal_services{processing_node = NewNode}}
    end,
    MasterNodeId = get_node_id(FailedNode),
    case node_internal_services:update(MasterNodeId, Diff) of
        {ok, #document{value = #node_internal_services{services = Services}}} ->
            lists:foreach(fun({UniqueName, Service}) ->
                ?info("Starting takeover of service ~s from node ~ts", [UniqueName, FailedNode]),
                init_service(Service, UniqueName, apply_takeover_fun, MasterNodeId),
                ?info("Finished takeover of service ~s from node ~ts", [UniqueName, FailedNode])
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
            lists:foreach(fun({UniqueName, Service}) ->
                ?info("Starting migration of service ~s to node ~ts", [UniqueName, MasterNode]),
                ok = internal_service:apply_migrate_fun(Service),
                rpc:call(MasterNode, ?MODULE, init_service, [Service, UniqueName, apply_start_fun, MasterNodeId]),
                ?info("Finished migration of service ~s to node ~ts", [UniqueName, MasterNode])
            end, maps:to_list(Services));
        {error, not_found} ->
            ok
    end.

-spec do_healthcheck(unique_service_name(), node_id(), internal_service:healthcheck_interval()) ->
    {ok, NewInterval :: internal_service:healthcheck_interval()} | ignore.
do_healthcheck(UniqueName, MasterNodeId, LastInterval) ->
    try
        case get_service_and_processing_node(UniqueName, MasterNodeId) of
            stopped ->
                ignore;
            {_, OtherNode} when OtherNode =/= node() ->
                ignore;
            {Service, _} ->
                do_healthcheck_insecure(UniqueName, Service, MasterNodeId, LastInterval)
        end
    catch
        Class:Reason:Stacktrace ->
            ?warning_stacktrace(
                "Healthcheck function of service ~s crashed (this may happen during cluster reorganization)~n"
                "Error was: ~w:~p",
                [UniqueName, Class, Reason], Stacktrace
            ),
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

-spec gen_unique_service_name(module(), internal_service:service_name()) -> unique_service_name().
gen_unique_service_name(Module, ServiceName) ->
    <<(atom_to_binary(Module, utf8))/binary, "###", ServiceName/binary>>.

-spec start_service_locally(node(), module(), internal_service:service_name(),
    internal_service:options()) -> ok | aborted | {error, term()}.
start_service_locally(MasterNode, Module, ServiceName, ServiceDescription) ->
    Service = internal_service:new(Module, ServiceDescription),
    AllowOverride = internal_service:is_override_allowed(ServiceDescription),
    UniqueName = gen_unique_service_name(Module, ServiceName),

    Default = #node_internal_services{services = #{UniqueName => Service}, processing_node = node()},
    Diff = fun(#node_internal_services{services = Services} = Record) ->
        case {maps:is_key(UniqueName, Services), AllowOverride} of
            {true, false} -> {error, already_started};
            _ -> {ok, Record#node_internal_services{services = Services#{UniqueName => Service}}}
        end
    end,

    MasterNodeId = get_node_id(MasterNode),
    case node_internal_services:update(MasterNodeId, Diff, Default) of
        {ok, _} -> init_service(Service, UniqueName, apply_start_fun, MasterNodeId);
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

-spec get_service_and_processing_node(unique_service_name(), node_id()) ->
    stopped | {internal_service:service(), node()}.
get_service_and_processing_node(UniqueName, MasterNodeId) ->
    {ok, #document{value = #node_internal_services{
        services = NodeServices, processing_node = ProcessingNode
    }}} = node_internal_services:get(MasterNodeId),
    case maps:get(UniqueName, NodeServices, undefined) of
        undefined -> stopped;  % service has been stopped, possibly in the migration process
        Service -> {Service, ProcessingNode}
    end.

-spec init_service(internal_service:service(), unique_service_name(), service_init_fun(), node_id()) ->
    ok | aborted.
init_service(Service, UniqueName, InitFun, MasterNodeId) ->
    try
        case internal_service:InitFun(Service) of
            ok ->
                HealthcheckInterval = internal_service:get_healthcheck_interval(Service),
                node_manager:reschedule_service_healthcheck(node(), UniqueName, MasterNodeId, HealthcheckInterval);
            abort ->
                remove_service_from_doc(MasterNodeId, UniqueName),
                aborted
        end
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Error while initializing service ~s - ~p:~p",
                [UniqueName, Error, Reason], Stacktrace),
            remove_service_from_doc(MasterNodeId, UniqueName),
            error(service_init_failure)
    end.

-spec remove_service_from_doc(node_id(), unique_service_name()) -> ok.
remove_service_from_doc(MasterNodeId, UniqueName) ->
    Diff = fun(#node_internal_services{services = Services} = Record) ->
        {ok, Record#node_internal_services{services = maps:remove(UniqueName, Services)}}
    end,
    case node_internal_services:update(MasterNodeId, Diff) of
        {ok, _} -> ok;
        {error, not_found} -> ok
    end.

-spec do_healthcheck_insecure(unique_service_name(), internal_service:service(), node_id(),
    internal_service:healthcheck_interval()) -> {ok, NewInterval :: internal_service:healthcheck_interval()} | ignore.
do_healthcheck_insecure(UniqueName, Service, MasterNodeId, LastInterval) ->
    case internal_service:apply_healthcheck_fun(Service, LastInterval) of
        {error, undefined_fun} ->
            ignore;
        {restart, _} ->
            % Check once more in case of race with migration
            case get_service_and_processing_node(UniqueName, MasterNodeId) of
                stopped ->
                    ignore;
                {_, OtherNode} when OtherNode =/= node() ->
                    ignore;  % service has been migrated to other node
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
                            remove_service_from_doc(MasterNodeId, UniqueName),
                            ignore
                    end
            end;
        {ok, NewInterval} ->
            {ok, NewInterval}
    end.