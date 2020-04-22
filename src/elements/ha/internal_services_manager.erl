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
-export([start_service/5, takeover/1, migrate_to_recovered_master/1]).
%% Export for internal rpc
-export([start_service_locally/5]).

-type service_fun_name() :: atom().
-type service_fun_args() :: list().

-export_type([service_fun_name/0]).

%%%===================================================================
%%% Message sending API
%%%===================================================================

-spec start_service(module(), service_fun_name(), service_fun_name(), service_fun_args(), binary()) ->
    ok | {error, term()}.
start_service(Module, Fun, StopFun, Args, HashingBase) ->
    Node = datastore_key:responsible_node(HashingBase),
    case rpc:call(Node, ?MODULE, start_service_locally, [Module, Fun, StopFun, Args, HashingBase]) of
        {badrpc, Reason} -> {error, Reason};
        Other -> Other
    end.

-spec takeover(node()) -> ok | no_return().
takeover(FailedNode) ->
    {ok, #document{value = #node_internal_services{services = Services}}} =
        node_internal_services:get(get_node_id(FailedNode)),
    lists:foreach(fun(#internal_service{module = Module, function = Fun, args = Args}) ->
        ok = apply(Module, Fun, binary_to_term(Args))
    end, lists:reverse(Services)).

-spec migrate_to_recovered_master(node()) -> ok | no_return().
migrate_to_recovered_master(MasterNode) ->
    {ok, #document{value = #node_internal_services{services = Services}}} =
        node_internal_services:get(get_node_id(MasterNode)),
    lists:foreach(fun(#internal_service{module = Module, function = Fun, stop_function = StopFun, args = Args}) ->
        ArgsList = binary_to_term(Args),
        ok = apply(Module, StopFun, ArgsList),
        ok = rpc:call(MasterNode, erlang, apply, [Module, Fun, ArgsList])
    end, lists:reverse(Services)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec start_service_locally(module(), service_fun_name(), service_fun_name(), service_fun_args(), binary()) ->
    ok | {error, term()}.
start_service_locally(Module, Fun, StopFun, Args, HashingBase) ->
    Service = #internal_service{module = Module, function = Fun, stop_function = StopFun,
        args = term_to_binary(Args), hashing_key = HashingBase},
    Default = #node_internal_services{services = [Service]},
    Diff = fun(#node_internal_services{services = Services} = Record) ->
        case lists:member(Service, Services) of
            true -> {error, already_started};
            false -> {ok, Record#node_internal_services{services = [Service | Services]}}
        end
    end,

    case node_internal_services:update(get_node_id(node()), Diff, Default) of
        {ok, _} -> apply(Module, Fun, Args);
        {error, already_started} -> ok
    end.

-spec get_node_id(node()) -> binary().
get_node_id(Node) ->
    atom_to_binary(Node, utf8).