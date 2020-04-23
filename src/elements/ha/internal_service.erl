%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions operating on #internal_service{}
%%% record that represents service that should work permanently
%%% (see internal_services_manager.erl). The record is stored
%%% in datastore as a part of #node_internal_services{} record.
%%% @end
%%%-------------------------------------------------------------------
-module(internal_service).
-author("Michał Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([new/3, apply_start_fun/1, apply_start_fun/2,
    apply_takeover_fun/1, apply_stop_fun/2, apply_migrate_fun/1]).

% Record representing service that should work permanently
-record(internal_service, {
    module :: module(),
    start_function :: service_fun_name(),
    takeover_function :: service_fun_name(),
    migrate_function :: service_fun_name(),
    stop_function :: service_fun_name(),
    start_function_args :: service_fun_args(),
    takeover_function_args :: service_fun_args(),
    migrate_function_args :: service_fun_args(),
    stop_function_args :: service_fun_args(),
    hashing_key :: internal_services_manager:hashing_base()
}).

-type service() :: #internal_service{}.
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

-export_type([service/0, service_name/0, service_fun_name/0, service_fun_args/0, options/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec new(module(), internal_services_manager:hashing_base(), options()) -> service().
new(Module, HashingBase, ServiceDescription) ->
    Fun = maps:get(start_function, ServiceDescription),
    Args = maps:get(start_function_args, ServiceDescription),
    TakeoverFun = maps:get(takeover_function, ServiceDescription, Fun),
    TakeoverFunArgs = maps:get(takeover_function_args, ServiceDescription, Args),

    StopFun = maps:get(stop_function, ServiceDescription, undefined),
    StopFunDefArgs = case StopFun of
        undefined -> [];
        _ -> Args
    end,
    StopFunArgs = maps:get(stop_function_args, ServiceDescription, StopFunDefArgs),
    MigrateFun = maps:get(migrate_function, ServiceDescription, StopFun),
    MigrateFunArgs = maps:get(migrate_function_args, ServiceDescription, StopFunArgs),

    #internal_service{module = Module, start_function = Fun, takeover_function = TakeoverFun,
        migrate_function = MigrateFun, stop_function = StopFun, start_function_args = Args,
        takeover_function_args = TakeoverFunArgs, migrate_function_args = MigrateFunArgs,
        stop_function_args = StopFunArgs, hashing_key = HashingBase}.

-spec apply_start_fun(service()) -> term().
apply_start_fun(#internal_service{module = Module, start_function = Fun, start_function_args = Args}) ->
    apply(Module, Fun, Args).

-spec apply_start_fun(node(), service()) -> term().
apply_start_fun(Node, #internal_service{module = Module, start_function = Fun, start_function_args = Args}) ->
    rpc:call(Node, Module, Fun, Args).

-spec apply_takeover_fun(service()) -> term().
apply_takeover_fun(#internal_service{module = Module, takeover_function = Fun, takeover_function_args = Args}) ->
    apply(Module, Fun, Args).

-spec apply_stop_fun(node(), service()) -> term().
apply_stop_fun(Node, #internal_service{module = Module, stop_function = Fun, stop_function_args = Args}) ->
    case Fun of
        undefined -> ok;
        _ -> rpc:call(Node, Module, Fun, Args)
    end.

-spec apply_migrate_fun(service()) -> term().
apply_migrate_fun(#internal_service{module = Module, migrate_function = Fun, migrate_function_args = Args}) ->
    case Fun of
        undefined -> ok;
        _ -> apply(Module, Fun, Args)
    end.