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
-export([start_service/6, start_service/4, stop_service/3, takeover/1, migrate_to_recovered_master/1]).
%% Export for internal rpc
-export([start_service_locally/4]).

-type service_name() :: binary().
-type service_fun_name() :: atom().
-type service_fun_args() :: list().
-type options() :: #{
    start_function := service_fun_name(),
    start_function_args := service_fun_args(),
    takeover_function => service_fun_name(),
    takeover_function_args => service_fun_args(),
    migrate_function => service_fun_name(),
    migrate_function_args => service_fun_args(),
    stop_function => service_fun_name(),
    stop_function_args => service_fun_args()
}.

-export_type([service_name/0, service_fun_name/0]).

%%%===================================================================
%%% Message sending API
%%%===================================================================

-spec start_service(module(), service_name(), service_fun_name(), service_fun_name(), service_fun_args(), binary()) ->
    ok | {error, term()}.
start_service(Module, Name, Fun, StopFun, Args, HashingBase) ->
    Options = #{
        start_function => Fun,
        stop_function => StopFun,
        start_function_args => Args
    },
    start_service(Module, Name, HashingBase, Options).

-spec start_service(module(), service_name(), binary(), options()) -> ok | {error, term()}.
start_service(Module, Name, HashingBase, ServiceDescription) ->
    Node = get_node(HashingBase),
    case rpc:call(Node, ?MODULE, start_service_locally, [Module, Name, HashingBase, ServiceDescription]) of
        {badrpc, Reason} -> {error, Reason};
        Other -> Other
    end.

-spec stop_service(module(), service_name(), binary()) -> ok | {error, term()}.
stop_service(Module, Name, HashingBase) ->
    Node = get_node(HashingBase),
    {ok, #document{value = #node_internal_services{services = NodeServices, processing_node = ProcessingNode}}} =
        node_internal_services:get(get_node_id(Node)),

    ServiceName = get_internal_name(Module, Name),
    case maps:get(ServiceName, NodeServices, undefined) of
        undefined ->
            ok;
        #internal_service{stop_function = Fun, stop_function_args = Args} ->
            ok = rpc:call(ProcessingNode, erlang, apply, [Module, Fun, binary_to_term(Args)]),

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
            lists:foreach(fun(#internal_service{module = Module, takeover_function = Fun, takeover_function_args = Args}) ->
                ok = apply(Module, Fun, binary_to_term(Args))
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
            lists:foreach(fun(#internal_service{module = Module, start_function = Fun, migrate_function = MigrateFun,
                start_function_args = Args, migrate_function_args = MigrateArgs}) ->
                ok = case MigrateFun of
                    undefined -> ok;
                    _ -> apply(Module, MigrateFun, binary_to_term(MigrateArgs))
                end,
                ok = rpc:call(MasterNode, erlang, apply, [Module, Fun, binary_to_term(Args)])
            end, maps:values(Services));
        {error, not_found} ->
            ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec start_service_locally(module(), service_name(), binary(), options()) -> ok | {error, term()}.
start_service_locally(Module, Name, HashingBase, ServiceDescription) ->
    Fun = maps:get(start_function, ServiceDescription),
    Args = maps:get(start_function_args, ServiceDescription),
    FailoverFun = maps:get(takeover_function, ServiceDescription, Fun),
    FailoverFunArgs = maps:get(takeover_function_args, ServiceDescription, Args),

    StopFun = maps:get(stop_function, ServiceDescription, undefined),
    StopFunDefArgs = case StopFun of
        undefined -> [];
        _ -> Args
    end,
    StopFunArgs = maps:get(stop_function_args, ServiceDescription, StopFunDefArgs),
    MigrateFun = maps:get(migrate_function, ServiceDescription, StopFun),
    MigrateFunArgs = maps:get(migrate_function_args, ServiceDescription, StopFunArgs),

    Service = #internal_service{module = Module, start_function = Fun, takeover_function = FailoverFun,
        migrate_function = MigrateFun, stop_function = StopFun, start_function_args = term_to_binary(Args),
        takeover_function_args = term_to_binary(FailoverFunArgs), migrate_function_args = term_to_binary(MigrateFunArgs),
        stop_function_args = term_to_binary(StopFunArgs), hashing_key = HashingBase},
    ServiceName = get_internal_name(Module, Name),
    Default = #node_internal_services{services = #{ServiceName => Service}, processing_node = node()},
    Diff = fun(#node_internal_services{services = Services} = Record) ->
        case maps:is_key(ServiceName, Services) of
            true -> {error, already_started};
            false -> {ok, Record#node_internal_services{services = Services#{ServiceName => Service}}}
        end
    end,

    case node_internal_services:update(get_node_id(node()), Diff, Default) of
        {ok, _} -> apply(Module, Fun, Args);
        {error, already_started} -> ok
    end.

-spec get_node_id(node()) -> binary().
get_node_id(Node) ->
    atom_to_binary(Node, utf8).

-spec get_node(binary()) -> node().
get_node(HashingBase) ->
    case HashingBase of
        <<>> -> node();
        _ -> datastore_key:responsible_node(HashingBase) % TODO - allow stopping o services when node is failed
    end.

-spec get_internal_name(module(), service_name()) -> binary().
get_internal_name(Module, Name) ->
    <<(atom_to_binary(Module, utf8))/binary, "###", Name/binary>>.
