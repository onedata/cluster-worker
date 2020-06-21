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
-export([start_service/5, start_service/6, start_service/3, start_service/4,
    stop_service/3, takeover/1, migrate_to_recovered_master/1]).
%% Export for internal rpc
-export([start_service_locally/4]).

-define(LOCAL_HASHING_BASE, <<>>).

% Type defining binary used by consistent hashing to chose node where service should be running.
-type hashing_base() :: binary().
-export_type([hashing_base/0]).

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
    Node = get_node(HashingBase),
    case rpc:call(Node, ?MODULE, start_service_locally, [Module, Name, HashingBase, ServiceDescription]) of
        {badrpc, Reason} -> {error, Reason};
        Other -> Other
    end.

-spec stop_service(module(), internal_service:service_name(), hashing_base()) -> ok | {error, term()}.
stop_service(Module, Name, HashingBase) ->
    Node = get_node(HashingBase),
    {ok, #document{value = #node_internal_services{services = NodeServices, processing_node = ProcessingNode}}} =
        node_internal_services:get(get_node_id(Node)),

    ServiceName = get_internal_name(Module, Name),
    case maps:get(ServiceName, NodeServices, undefined) of
        undefined ->
            ok;
        Service ->
            ok = internal_service:apply_stop_fun(ProcessingNode, Service),

            Diff = fun(#node_internal_services{services = Services} = Record) ->
                {ok, Record#node_internal_services{services = maps:remove(ServiceName, Services)}}
            end,
            case node_internal_services:update(get_node_id(Node), Diff) of
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
    case node_internal_services:update(get_node_id(FailedNode), Diff) of
        {ok, #document{value = #node_internal_services{services = Services}}} ->
            lists:foreach(fun(Service) ->
                ok = internal_service:apply_takeover_fun(Service)
            end, maps:values(Services));
        {error, not_found} ->
            ok
    end.

-spec migrate_to_recovered_master(node()) -> ok | no_return().
migrate_to_recovered_master(MasterNode) ->
    Diff = fun(Record) ->
        {ok, Record#node_internal_services{processing_node = MasterNode}}
    end,
    case node_internal_services:update(get_node_id(MasterNode), Diff) of
        {ok, #document{value = #node_internal_services{services = Services}}} ->
            lists:foreach(fun(Service) ->
                ok = internal_service:apply_migrate_fun(Service),
                ok = internal_service:apply_start_fun(MasterNode, Service)
            end, maps:values(Services));
        {error, not_found} ->
            ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec start_service_locally(module(), internal_service:service_name(), hashing_base(), internal_service:options()) ->
    ok | {error, term()}.
start_service_locally(Module, Name, HashingBase, ServiceDescription) ->
    Service = internal_service:new(Module, HashingBase, ServiceDescription),
    ServiceName = get_internal_name(Module, Name),
    Default = #node_internal_services{services = #{ServiceName => Service}, processing_node = node()},
    Diff = fun(#node_internal_services{services = Services} = Record) ->
        case maps:is_key(ServiceName, Services) of
            true -> {error, already_started};
            false -> {ok, Record#node_internal_services{services = Services#{ServiceName => Service}}}
        end
    end,

    case node_internal_services:update(get_node_id(node()), Diff, Default) of
        {ok, _} -> internal_service:apply_start_fun(Service);
        {error, already_started} -> ok
    end.

-spec get_node_id(node()) -> binary().
get_node_id(Node) ->
    atom_to_binary(Node, utf8).

-spec get_node(hashing_base()) -> node().
get_node(HashingBase) ->
    case HashingBase of
        ?LOCAL_HASHING_BASE -> node();
        _ -> datastore_key:responsible_node(HashingBase) % TODO - allow stopping o services when node is failed
    end.

-spec get_internal_name(module(), internal_service:service_name()) -> binary().
get_internal_name(Module, Name) ->
    <<(atom_to_binary(Module, utf8))/binary, "###", Name/binary>>.
