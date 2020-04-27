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

%% API
-export([start_service/5, start_service/6, start_service/3, start_service/4,
    stop_service/3, takeover/1, migrate_to_recovered_master/1,
    on_cluster_restart/0, do_healtcheck/4]).
%% Export for internal rpc
-export([start_service_locally/5]).

-define(LOCAL_HASHING_BASE, <<>>).

% Type defining binary used by consistent hashing to chose node where service should be running.
-type hashing_base() :: binary().
-type node_id() :: binary().
-export_type([hashing_base/0, node_id/0]).

%%%===================================================================
%%% Message sending API
%%%===================================================================

-spec start_service(module(), internal_service:service_name(), internal_service:service_fun_name(),
    internal_service:service_fun_name(), internal_service:service_fun_args()) ->
    ok | {error, term()}.
start_service(Module, Name, Fun, StopFun, Args) ->
    start_service(Module, Name, Fun, StopFun, Args, ?LOCAL_HASHING_BASE).

-spec start_service(module(), internal_service:service_name(), internal_service:service_fun_name(),
    internal_service:service_fun_name(), internal_service:service_fun_args(), hashing_base()) ->
    ok | {error, term()}.
start_service(Module, Name, Fun, StopFun, Args, HashingBase) ->
    Options = #{
        start_function => Fun,
        stop_function => StopFun,
        start_function_args => Args
    },
    start_service(Module, Name, HashingBase, Options).

-spec start_service(module(), internal_service:service_name(), internal_service:options()) ->
    ok | {error, term()}.
start_service(Module, Name, ServiceDescription) ->
    start_service(Module, Name, ?LOCAL_HASHING_BASE, ServiceDescription).

-spec start_service(module(), internal_service:service_name(), hashing_base(), internal_service:options()) ->
    ok | {error, term()}.
start_service(Module, Name, HashingBase, ServiceDescription) ->
    {MasterNode, HandlingNode} = get_master_and_handling_nodes(HashingBase),
    case rpc:call(HandlingNode, ?MODULE, start_service_locally, [MasterNode, Module, Name, HashingBase, ServiceDescription]) of
        {badrpc, Reason} -> {error, Reason};
        Other -> Other
    end.

-spec stop_service(module(), internal_service:service_name(), hashing_base()) -> ok | {error, term()}.
stop_service(Module, Name, HashingBase) ->
    {MasterNode, _} = get_master_and_handling_nodes(HashingBase),
    MasterNodeID = get_node_id(MasterNode),
    ServiceName = get_internal_name(Module, Name),
    {Service, ProcessingNode} = get_service_and_processing_node(ServiceName, MasterNodeID),
    case Service of
        undefined ->
            ok;
        _ ->
            ok = internal_service:apply_stop_fun(ProcessingNode, Service),

            Diff = fun(#node_internal_services{services = Services} = Record) ->
                {ok, Record#node_internal_services{services = maps:remove(ServiceName, Services)}}
            end,
            case node_internal_services:update(MasterNodeID, Diff) of
                {ok, _} -> ok;
                {error, not_found} -> ok
            end
    end.

-spec takeover(node()) -> ok | no_return().
takeover(FailedNode) ->
    NewNode = node(),
    Diff = fun(Record) ->
        {ok, Record#node_internal_services{processing_node = NewNode}}
    end,
    MasterNodeID = get_node_id(FailedNode),
    case node_internal_services:update(MasterNodeID, Diff) of
        {ok, #document{value = #node_internal_services{services = Services}}} ->
            lists:foreach(fun({ServiceName, Service}) ->
                TakeoverAns = internal_service:apply_takeover_fun(Service),
                HealthcheckInterval = internal_service:get_healthcheck_interval(Service),
                ok = node_manager:init_service_healthcheck(ServiceName, MasterNodeID, HealthcheckInterval, TakeoverAns)
            end, maps:to_list(Services));
        {error, not_found} ->
            ok
    end.

-spec migrate_to_recovered_master(node()) -> ok | no_return().
migrate_to_recovered_master(MasterNode) ->
    Diff = fun(Record) ->
        {ok, Record#node_internal_services{processing_node = MasterNode}}
    end,
    MasterNodeID = get_node_id(MasterNode),
    case node_internal_services:update(MasterNodeID, Diff) of
        {ok, #document{value = #node_internal_services{services = Services}}} ->
            lists:foreach(fun({ServiceName, Service}) ->
                ok = internal_service:apply_migrate_fun(Service),
                StartAns = internal_service:apply_start_fun(MasterNode, Service),
                HealthcheckInterval = internal_service:get_healthcheck_interval(Service),
                ok = node_manager:init_service_healthcheck(
                    MasterNode, ServiceName, MasterNodeID, HealthcheckInterval, StartAns)
            end, maps:to_list(Services));
        {error, not_found} ->
            ok
    end.

-spec do_healtcheck(internal_service:service_name(), node_id(), non_neg_integer(), term()) ->
    {ok, Interval :: non_neg_integer()} | ignore.
do_healtcheck(ServiceName, MasterNodeID, LastInterval, StartFunAns) ->
    case get_service_and_processing_node(ServiceName, MasterNodeID) of
        {Service, Node} when Service =:= undefined orelse Node =/= node() ->
            ignore;
        {Service, _} ->
            case internal_service:apply_healthcheck_fun(Service, [LastInterval, StartFunAns]) of
                {error, undefined_fun} ->
                    ignore;
                {restart, NewInterval} ->
                    % Check once more in case of race with migration
                    case get_service_and_processing_node(ServiceName, MasterNodeID) of
                        {Service2, Node2} when Service2 =:= undefined orelse Node2 =/= node() ->
                            ignore;
                        _ ->
                            RestartAns = internal_service:apply_start_fun(Service),
                            {ok, NewInterval, RestartAns}
                    end;
                {ok, NewInterval} ->
                    {ok, NewInterval, StartFunAns}
            end
    end.

on_cluster_restart() ->
    ok = node_internal_services:delete(get_node_id(node())).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec start_service_locally(node(), module(), internal_service:service_name(), hashing_base(),
    internal_service:options()) -> ok | {error, term()}.
start_service_locally(MasterNode, Module, Name, HashingBase, ServiceDescription) ->
    Service = internal_service:new(Module, HashingBase, ServiceDescription),
    ServiceName = get_internal_name(Module, Name),
    Default = #node_internal_services{services = #{ServiceName => Service}, processing_node = node()},
    Diff = fun(#node_internal_services{services = Services} = Record) ->
        case maps:is_key(ServiceName, Services) of
            true -> {error, already_started};
            false -> {ok, Record#node_internal_services{services = Services#{ServiceName => Service}}}
        end
    end,

    MasterNodeID = get_node_id(MasterNode),
    case node_internal_services:update(MasterNodeID, Diff, Default) of
        {ok, _} ->
            StartAns = internal_service:apply_start_fun(Service),
            HealthcheckInterval = internal_service:get_healthcheck_interval(Service),
            ok = node_manager:init_service_healthcheck(ServiceName, MasterNodeID, HealthcheckInterval, StartAns);
        {error, already_started} ->
            ok
    end.

-spec get_node_id(node()) -> node_id().
get_node_id(Node) ->
    atom_to_binary(Node, utf8).

-spec get_master_and_handling_nodes(hashing_base()) -> {MasterNode :: node(), HandlingNode :: node()}.
get_master_and_handling_nodes(HashingBase) ->
    case HashingBase of
        ?LOCAL_HASHING_BASE ->
            {node(), node()};
        _ ->
            {Master, _} = datastore_key:primary_responsible_node(HashingBase),
            {Master, datastore_key:responsible_node(HashingBase)}
    end.

-spec get_internal_name(module(), internal_service:service_name()) -> internal_service:service_name().
get_internal_name(Module, Name) ->
    <<(atom_to_binary(Module, utf8))/binary, "###", Name/binary>>.

-spec get_service_and_processing_node(internal_service:service_name(), node_id()) ->
    {internal_service:service() | undefined, node()}.
get_service_and_processing_node(ServiceName, MasterNodeID) ->
    {ok, #document{value = #node_internal_services{services = NodeServices, processing_node = ProcessingNode}}} =
        node_internal_services:get(MasterNodeID),

    {maps:get(ServiceName, NodeServices, undefined), ProcessingNode}.