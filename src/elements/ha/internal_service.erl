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

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([new/2, is_override_allowed/1, get_module/1,
    apply_start_fun/1, apply_takeover_fun/1, apply_stop_fun/2, apply_migrate_fun/1,
    apply_healthcheck_fun/2, get_healthcheck_interval/1]).
%% Export for internal rpc
-export([apply_with_retry/5]).

% Record representing service that should work permanently
-record(internal_service, {
    module :: module(),
    start_function :: service_fun_name(),
    takeover_function :: service_fun_name(),
    migrate_function :: service_fun_name(),
    stop_function :: service_fun_name(),
    healthcheck_fun :: service_fun_name(),
    start_function_args :: service_fun_args(),
    takeover_function_args :: service_fun_args(),
    migrate_function_args :: service_fun_args(),
    stop_function_args :: service_fun_args(),
    healthcheck_interval :: non_neg_integer(),
    async_start :: boolean()
}).

-type service() :: #internal_service{}.
-type service_name() :: binary().
-type service_fun_name() :: atom().
-type service_fun_args() :: list().
-type options() :: #{
    start_function := service_fun_name(),
    start_function_args => service_fun_args(),
    takeover_function => service_fun_name(),
    takeover_function_args => service_fun_args(),
    migrate_function => service_fun_name(),
    migrate_function_args => service_fun_args(),
    stop_function => service_fun_name(),
    stop_function_args => service_fun_args(),
    healthcheck_fun => service_fun_name(),
    healthcheck_interval => non_neg_integer(),
    allow_override => boolean(), % allows overriding of existing service ; if it is false (default),
                                 % an error is returned if service using the name already exists
    async_start => boolean()
}.
-type init_fun_answer() :: ok | abort. % Function that starts service can return `abort` when its internal
                                       % logic verifies that service should not be started because something has
                                       % changed from the moment its start has been initiated. In such a case
                                       % all changes in metadata connected with the service will be reversed.

-export_type([service/0, service_name/0, service_fun_name/0, service_fun_args/0, options/0]).

-define(HEALTHCHECK_DEFAULT_INTERVAL,
    application:get_env(?CLUSTER_WORKER_APP_NAME, service_healthcheck_default_interval, 1000)).
-define(INITIAL_SLEEP,
    application:get_env(?CLUSTER_WORKER_APP_NAME, service_retry_initial_sleep, 100)).
-define(RETRIES_NUM,
    application:get_env(?CLUSTER_WORKER_APP_NAME, service_start_retries_num, 8)).

%%%===================================================================
%%% API
%%%===================================================================

-spec new(module(), options()) -> service().
new(Module, ServiceDescription) ->
    Fun = maps:get(start_function, ServiceDescription),
    Args = maps:get(start_function_args, ServiceDescription, []),
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

    HealthcheckFun = maps:get(healthcheck_fun, ServiceDescription, undefined),
    HealthcheckDefInterval = case HealthcheckFun of
        undefined -> 0;
        _ -> ?HEALTHCHECK_DEFAULT_INTERVAL
    end,
    HealthcheckInterval = maps:get(healthcheck_interval, ServiceDescription, HealthcheckDefInterval),

    AsyncStart = maps:get(async_start, ServiceDescription, false),

    #internal_service{module = Module, start_function = Fun, takeover_function = TakeoverFun,
        migrate_function = MigrateFun, stop_function = StopFun, healthcheck_fun = HealthcheckFun,
        start_function_args = Args, takeover_function_args = TakeoverFunArgs, migrate_function_args = MigrateFunArgs,
        stop_function_args = StopFunArgs, healthcheck_interval = HealthcheckInterval, async_start = AsyncStart}.

-spec is_override_allowed(options()) -> boolean().
is_override_allowed(ServiceDescription) ->
    maps:get(allow_override, ServiceDescription, false).

-spec get_module(service()) -> module().
get_module(#internal_service{module = Module}) ->
    Module.

-spec apply_start_fun(service()) -> init_fun_answer().
apply_start_fun(#internal_service{module = Module, start_function = Fun,
    start_function_args = Args, async_start = Async}) ->
    apply_with_retry(Module, Fun, Args, Async).

-spec apply_takeover_fun(service()) -> init_fun_answer().
apply_takeover_fun(#internal_service{module = Module, takeover_function = Fun,
    takeover_function_args = Args, async_start = Async}) ->
    apply_with_retry(Module, Fun, Args, Async).

-spec apply_stop_fun(node(), service()) -> term().
apply_stop_fun(_Node, #internal_service{stop_function = undefined}) ->
    ok;
apply_stop_fun(Node, #internal_service{module = Module, stop_function = Fun, stop_function_args = Args}) ->
    case rpc:call(Node, Module, Fun, Args) of
        {badrpc, nodedown} -> ok;
        {badrpc, Reason} -> {error, Reason};
        Other -> Other
    end.

-spec apply_migrate_fun(service()) -> term().
apply_migrate_fun(#internal_service{module = Module, migrate_function = Fun, migrate_function_args = Args}) ->
    case Fun of
        undefined -> ok;
        _ -> apply(Module, Fun, Args)
    end.

-spec apply_healthcheck_fun(service(), non_neg_integer()) ->
    {Result :: term(), Interval :: non_neg_integer()} | {error, undefined_fun}.
apply_healthcheck_fun(#internal_service{healthcheck_fun = undefined}, _LastInterval) ->
    {error, undefined_fun};
apply_healthcheck_fun(#internal_service{module = Module, healthcheck_fun = Fun,
    healthcheck_interval = DefaultInterval}, LastInterval) ->
    case apply(Module, Fun, [LastInterval]) of
        {_Result, _OverriddenInterval} = Ans -> Ans;
        Result -> {Result, DefaultInterval}
    end.

-spec get_healthcheck_interval(service()) -> non_neg_integer().
get_healthcheck_interval(#internal_service{healthcheck_interval = Interval}) ->
    Interval.

-spec apply_with_retry(module(), service_fun_name(), service_fun_args(), boolean()) -> init_fun_answer().
apply_with_retry(Module, Fun, Args, Async) ->
    case Async of
        true ->
            spawn(?MODULE, apply_with_retry, [Module, Fun, Args, ?INITIAL_SLEEP, ?RETRIES_NUM]),
            ok;
        false ->
            apply_with_retry(Module, Fun, Args, ?INITIAL_SLEEP, ?RETRIES_NUM)
    end.

-spec apply_with_retry(module(), service_fun_name(), service_fun_args(), non_neg_integer(), non_neg_integer()) ->
    init_fun_answer().
apply_with_retry(Module, Fun, Args, _Sleep, 0) ->
    apply(Module, Fun, Args);
apply_with_retry(Module, Fun, Args, Sleep, RetryCount) ->
    try
        apply(Module, Fun, Args)
    catch
        Error:Reason ->
            ?debug_stacktrace("Error while applying fun ~p:~p with args ~p: ~p~p",
                [Module, Fun, Args, Error, Reason]),
            timer:sleep(Sleep),
            apply_with_retry(Module, Fun, Args, Sleep * 2, RetryCount - 1)
    end.
