%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an interface for a transaction process, that serializes
%%% requests associated with an given key.
%%% @end
%%%-------------------------------------------------------------------
-module(tp).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/tp/tp.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([call/4, call/5, call/6, cast/4, send/4]).
-export([get_processes_limit/0, set_processes_limit/1, get_processes_number/0]).

-type key() :: term().
-type args() :: list().
-type state() :: any().
-type server() :: pid().
-type request() :: term().
-type response() :: term().

-export_type([key/0, args/0, state/0, server/0, request/0, response/0]).

-define(monitor_internal_calls, 1).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv call(Module, Args, Key, Request, timer:seconds(5))
%% @end
%%--------------------------------------------------------------------
-spec call(module(), args(), key(), request()) ->
    response() | {error, Reason :: term()}.
call(Module, Args, Key, Request) ->
    call(Module, Args, Key, Request, timer:seconds(5)).

%%--------------------------------------------------------------------
%% @equiv call(Module, Args, Key, Request, Timeout, 1)
%% @end
%%--------------------------------------------------------------------
-spec call(module(), args(), key(), request(), timeout()) ->
    response() | {error, Reason :: term()}.
call(Module, Args, Key, Request, Timeout) ->
    call(Module, Args, Key, Request, Timeout, 1).

%%--------------------------------------------------------------------
%% @doc
%% Sends synchronous request to transaction process and awaits response.
%% @end
%%--------------------------------------------------------------------
-spec call(module(), args(), key(), request(), timeout(), non_neg_integer()) ->
    {response(), pid()} | {error, Reason :: term()}.
-ifdef(monitor_internal_calls).
call(_Module, _Args, _Key, _Request, _Timeout, 0) ->
    {error, timeout};
call(Module, Args, Key, Request, Timeout, Attempts) ->
    TPMaster = get(tp_master),

    case TPMaster of
        undefined ->
            ok;
        _ ->
            ?critical("Tp internal call, args: ~p",
                [{Module, Args, Key, Request}])
    end,

    case get_or_create_tp_server(Module, Args, Key) of
        {ok, TPMaster} ->
            ?critical("Tp self call, args: ~p",
                [{Module, Args, Key, Request}]),
            {error, self_call};
        {ok, Pid} ->
            try
                {gen_server:call(Pid, Request, Timeout), Pid}
            catch
                _:{noproc, _} ->
                    tp_router:delete(Key, Pid),
                    call(Module, Args, Key, Request, Timeout);
                exit:{normal, _} ->
                    tp_router:delete(Key, Pid),
                    call(Module, Args, Key, Request, Timeout);
                _:{timeout, _} ->
                    call(Module, Args, Key, Request, Timeout, Attempts - 1);
                _:Reason ->
                    {error, {Reason, erlang:get_stacktrace()}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
-endif.
-ifndef(monitor_internal_calls).
call(_Module, _Args, _Key, _Request, _Timeout, 0) ->
    {error, timeout};
call(Module, Args, Key, Request, Timeout, Attempts) ->
    case get_or_create_tp_server(Module, Args, Key) of
        {ok, Pid} ->
            try
                {gen_server:call(Pid, Request, Timeout), Pid}
            catch
                _:{noproc, _} ->
                    tp_router:delete(Key, Pid),
                    call(Module, Args, Key, Request, Timeout);
                exit:{normal, _} ->
                    tp_router:delete(Key, Pid),
                    call(Module, Args, Key, Request, Timeout);
                _:{timeout, _} ->
                    call(Module, Args, Key, Request, Timeout, Attempts - 1);
                _:Reason ->
                    {error, {Reason, erlang:get_stacktrace()}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
-endif.

%%--------------------------------------------------------------------
%% @doc
%% Sends asynchronous request to transaction process.
%% @end
%%--------------------------------------------------------------------
-spec cast(module(), args(), key(), request()) -> ok | {error, Reason :: term()}.
cast(Module, Args, Key, Request) ->
    case get_or_create_tp_server(Module, Args, Key) of
        {ok, Pid} -> gen_server:cast(Pid, Request);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends message to transaction process.
%% @end
%%--------------------------------------------------------------------
-spec send(module(), args(), key(), term()) -> ok | {error, Reason :: term()}.
send(Module, Args, Key, Info) ->
    case get_or_create_tp_server(Module, Args, Key) of
        {ok, Pid} -> Pid ! Info, ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns limit for the number of active tp processes.
%% @end
%%--------------------------------------------------------------------
-spec get_processes_limit() -> Limit :: non_neg_integer().
get_processes_limit() ->
    {ok, Limit} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        ?TP_PROCESSES_LIMIT),
    Limit.

%%--------------------------------------------------------------------
%% @doc
%% Sets limit for the number of active tp processes.
%% NOTE! If the new limit is less than the current number of active tp processes
%% it does not force termination and deletion of any process. It is only
%% guaranteed that new tp processes will not be created as long as it would
%% exceed the limit.
%% @end
%%--------------------------------------------------------------------
-spec set_processes_limit(non_neg_integer()) -> ok.
set_processes_limit(Limit) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, ?TP_PROCESSES_LIMIT, Limit).

%%--------------------------------------------------------------------
%% @doc
%% Returns number of active tp processes.
%% @end
%%--------------------------------------------------------------------
-spec get_processes_number() -> non_neg_integer().
get_processes_number() ->
    tp_router:size().

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns pid of a transaction process server. If a server associated with the
%% provided key is missing it is instantiated.
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_tp_server(module(), args(), key()) ->
    {ok, server()} | {error, Reason :: term()}.
get_or_create_tp_server(Module, Args, Key) ->
    case tp_router:get(Key) of
        {ok, Pid} ->
            {ok, Pid};
        {error, not_found} ->
            create_tp_server(Module, Args, Key)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Instantiates a transaction process server. Ensures that there will be only
%% one server at the time associated with the provided key.
%% @end
%%--------------------------------------------------------------------
-spec create_tp_server(module(), args(), key()) ->
    {ok, server()} | {error, Reason :: term()}.
create_tp_server(Module, Args, Key) ->
    SupName = datastore_multiplier:extend_name(Key, ?TP_ROUTER_SUP),
    case supervisor:start_child(SupName, [Module, Args, Key]) of
        {ok, undefined} -> get_or_create_tp_server(Module, Args, Key);
        {ok, Pid} -> {ok, Pid};
        {error, Reason} -> {error, Reason}
    end.
