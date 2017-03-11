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

%% API
-export([run_async/4, run_sync/4, run_sync/5]).
-export([get_processes_limit/0, set_processes_limit/1, get_processes_number/0]).

-type init() :: #tp_init{}.
-type mod() :: module().
-type args() :: list().
-type key() :: term().
-type data() :: any().
-type server() :: pid().
-type changes() :: any().
-type request() :: any().
-type response() :: any().

-export_type([init/0, mod/0, args/0, key/0, data/0, server/0, changes/0,
    request/0, response/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Delegates request processing to the transaction process server and waits for
%% the response with default 5 seconds timeout.
%% @end
%%--------------------------------------------------------------------
-spec run_sync(module(), args(), key(), request()) ->
    response() | {error, Reason :: term()} | no_return().
run_sync(Module, Args, Key, Request) ->
    run_sync(Module, Args, Key, Request, timer:seconds(5)).

%%--------------------------------------------------------------------
%% @doc
%% Delegates request processing to the transaction process server and waits for
%% the response with custom timeout.
%% @end
%%--------------------------------------------------------------------
-spec run_sync(module(), args(), key(), request(), timeout()) ->
    response() | {error, Reason :: term()} | no_return().
run_sync(Module, Args, Key, Request, Timeout) ->
    case run_async(Module, Args, Key, Request) of
        {ok, Ref} -> receive_response(Ref, Timeout);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Delegates request processing to the transaction process server and returns
%% a reference in which a response will be wrapped ({Ref, Msg}).
%% @end
%%--------------------------------------------------------------------
-spec run_async(module(), args(), key(), request()) ->
    {ok, reference()} | {error, Reason :: term()}.
run_async(Module, Args, Key, Request) ->
    run_async(Module, Args, Key, Request, 3).


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
%% Delegates request processing to the transaction process server. In case of
%% timeout retires 'Attempts' times.
%% @end
%%--------------------------------------------------------------------
-spec run_async(module(), args(), key(), request(),
    Attempts :: non_neg_integer()) ->
    {ok, reference()} | {error, Reason :: term()}.
run_async(_Module, _Args, _Key, _Request, 0) ->
    {error, timeout};
run_async(Module, Args, Key, Request, Attempts) ->
    case get_or_create_tp_server(Module, Args, Key) of
        {ok, Pid} ->
            try
                gen_server:call(Pid, Request)
            catch
                _:{noproc, _} ->
                    tp_router:delete(Key, Pid),
                    run_async(Module, Args, Key, Request);
                exit:{normal, _} ->
                    tp_router:delete(Key, Pid),
                    run_async(Module, Args, Key, Request);
                _:{timeout, _} ->
                    run_async(Module, Args, Key, Request, Attempts - 1);
                _:Reason ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a response form a transaction process server or fails with a timeout.
%% @end
%%--------------------------------------------------------------------
-spec receive_response(reference(), timeout()) ->
    response() | {error, timeout} | no_return().
receive_response(Ref, Timeout) ->
    receive
        {Ref, {throw, Exception}} -> throw(Exception);
        {Ref, Response} -> Response
    after
        Timeout -> {error, timeout}
    end.

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
    case supervisor:start_child(?TP_ROUTER_SUP, [Module, Args, Key]) of
        {ok, undefined} -> get_or_create_tp_server(Module, Args, Key);
        {ok, Pid} -> {ok, Pid};
        {error, Reason} -> {error, Reason}
    end.
