%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is facade of worker_host gen_server api,
%%% It simply translates arguments into appropriate #worker_request,
%%% and sends it to worker.
%%% @end
%%%-------------------------------------------------------------------
-module(worker_proxy).
-author("Tomasz Lichon").
-author("Krzysztof Trzepla").

-include("elements/worker_host/worker_protocol.hrl").
-include("timeouts.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([call/2, call/3, call_direct/2, call_direct/3,
    call_pool/3, call_pool/4, multicall/2, multicall/3,
    cast/2, cast/3, cast/4, cast/5, multicast/2, multicast/3,
    multicast/4, cast_pool/3, cast_pool/4, cast_pool/5,
    cast_and_monitor/3, cast_and_monitor/4]).


-type execute_type() :: direct | spawn | {pool, call | cast, worker_pool:name()}.
-type timeout_spec() :: timeout() | {timeout(), fun(() -> ok)}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv call(WorkerName, Request, ?DEFAULT_REQUEST_TIMEOUT)
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerRef :: request_dispatcher:worker_ref(), Request :: term()) ->
    Result :: term() | {error, term()}.
call(WorkerRef, Request) ->
    call(WorkerRef, Request, ?DEFAULT_REQUEST_TIMEOUT).

%%--------------------------------------------------------------------
%% @doc
%% Synchronously send request to worker with given timeout.
%% New process will be spawned to process this request.
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerRef :: request_dispatcher:worker_ref(), Request :: term(),
    Timeout :: timeout_spec()) -> Result :: term() | {error, term()}.
call(WorkerRef, Request, Timeout) ->
    call(WorkerRef, Request, Timeout, spawn).


%%--------------------------------------------------------------------
%% @doc
%% @equiv call_direct(WorkerRef, Request, ?DEFAULT_REQUEST_TIMEOUT).
%% @end
%%--------------------------------------------------------------------
-spec call_direct(WorkerRef :: request_dispatcher:worker_ref(),
    Request :: term()) -> Result :: term() | {error, term()}.
call_direct(WorkerRef, Request) ->
    call_direct(WorkerRef, Request, ?DEFAULT_REQUEST_TIMEOUT).

%% -------------------------------------------------------------------
%% @doc
%% Same as call/3 but request will be processed in the calling process.
%% @equiv call(WorkerRef, Request, Timeout, direct).
%% @end
%%--------------------------------------------------------------------
-spec call_direct(WorkerRef :: request_dispatcher:worker_ref(),
    Request :: term(), Timeout :: timeout_spec()) ->
    Result :: term() | {error, term()}.
call_direct(WorkerRef, Request, Timeout) ->
    call(WorkerRef, Request, Timeout, direct).

%% -------------------------------------------------------------------
%% @doc
%% @equiv call_pool(WorkerRef, Request, PoolName, ?DEFAULT_REQUEST_TIMEOUT).
%% @end
%%--------------------------------------------------------------------
-spec call_pool(WorkerRef :: request_dispatcher:worker_ref(),
    Request :: term(), worker_pool:name()) -> Result :: term() | {error, term()}.
call_pool(WorkerRef, Request, PoolName) ->
    call_pool(WorkerRef, Request, PoolName, ?DEFAULT_REQUEST_TIMEOUT).


%% -------------------------------------------------------------------
%% @doc
%% Same as call/3 but request will be processed by one of the workers
%% from the pool PoolName.
%% The pool should be started before call to this function.
%% @equiv call(WorkerRef, Request, Timeout, {pool, PoolName}).
%% @end
%%--------------------------------------------------------------------
-spec call_pool(WorkerRef :: request_dispatcher:worker_ref(), Request :: term(),
    worker_pool:name(), Timeout :: timeout_spec()) ->
    Result :: term() | {error, term()}.
call_pool(WorkerRef, Request, PoolName, Timeout) ->
    call(WorkerRef, Request, Timeout, {pool, call, PoolName}).



%%--------------------------------------------------------------------
%% @doc
%% @equiv multicall(WorkerName, Request, ?DEFAULT_REQUEST_TIMEOUT)
%% @end
%%--------------------------------------------------------------------
-spec multicall(WorkerName :: request_dispatcher:worker_name(), Request :: term()) ->
    [{Node :: node(), ok | {ok, term()} | {error, term()}}].
multicall(WorkerName, Request) ->
    multicall(WorkerName, Request, ?DEFAULT_REQUEST_TIMEOUT).

%%--------------------------------------------------------------------
%% @doc
%% Synchronously send request to all workers with given timeout.
%% Returns list of pairs: node and associated answer.
%% @end
%%--------------------------------------------------------------------
-spec multicall(WorkerName :: request_dispatcher:worker_name(),
    Request :: term(), Timeout :: timeout_spec()) ->
    [{Node :: node(), ok | {ok, term()} | {error, term()}}].
multicall(WorkerName, Request, Timeout) ->
    {ok, Nodes} = request_dispatcher:get_worker_nodes(WorkerName),
    lists_utils:pmap(fun(Node) ->
        {Node, call_direct({WorkerName, Node}, Request, Timeout)}
    end, Nodes).


%%--------------------------------------------------------------------
%% @doc
%% @equiv cast(WorkerRef, Request, undefined)
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerRef :: request_dispatcher:worker_ref(),
    Request :: term()) -> ok | {error, term()}.
cast(WorkerRef, Request) ->
    cast(WorkerRef, Request, undefined).

%%--------------------------------------------------------------------
%% @doc
%% @equiv cast(WorkerName, Request, ReplyTo, undefined)
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerRef :: request_dispatcher:worker_ref(),
    Request :: term(), ReplyTo :: process_ref()) -> ok | {error, term()}.
cast(WorkerRef, Request, ReplyTo) ->
    cast(WorkerRef, Request, ReplyTo, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker, answer is expected at ReplyTo
%% process/gen_server. Answer is expected to have MsgId.
%% Answer is a 'worker_answer' record.
%% @equiv cast(WorkerName, Request, ReplyTo, MsgId, spawn).
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerRef :: request_dispatcher:worker_ref(), Request :: term(),
    ReplyTo :: process_ref(), MsgId :: term() | undefined) -> ok | {error, term()}.
cast(WorkerRef, Request, ReplyTo, MsgId) ->
    cast(WorkerRef, Request, ReplyTo, MsgId, spawn).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker, answer with given MsgId is
%% expected at ReplyTo process/gen_server. The answer will be
%% 'worker_answer' record.
%% It depends on ExecOption whether request will be processed by
%% spawned process or process from pool.
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerRef :: request_dispatcher:worker_ref(),
    Request :: term(), ReplyTo :: process_ref(), MsgId :: term() | undefined,
    execute_type()) -> ok | {error, term()}.
cast(WorkerRef, Request, ReplyTo, MsgId, ExecOption) ->
    {Name, Node} = choose_node(WorkerRef),
    Args = prepare_args(Name, Request, MsgId, ReplyTo),
    execute(Args, Node, ExecOption, undefined),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @equiv cast_and_monitor(WorkerRef, Request, {proc, self()}, MsgId).
%% @end
%%--------------------------------------------------------------------
-spec cast_and_monitor(WorkerRef :: request_dispatcher:worker_ref(),
    Request :: term(), MsgId :: term() | undefined) -> pid() | {error, term()}.
cast_and_monitor(WorkerRef, Request, MsgId) ->
    cast_and_monitor(WorkerRef, Request, {proc, self()}, MsgId).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker, answer is expected at ReplyTo
%% process/gen_server. Answer is expected to have MsgId.
%% Answer is a 'worker_answer' record.
%% Returns pid of process that processes request.
%% @end
%%--------------------------------------------------------------------
-spec cast_and_monitor(WorkerRef :: request_dispatcher:worker_ref(), Request :: term(),
    ReplyTo :: process_ref(), MsgId :: term() | undefined) -> pid() | {error, term()}.
cast_and_monitor(WorkerRef, Request, ReplyTo, MsgId) ->
    {Name, Node} = choose_node(WorkerRef),
    Args = prepare_args(Name, Request, MsgId, ReplyTo),
    execute(Args, Node, spawn, undefined).

%%--------------------------------------------------------------------
%% @doc
%% @equiv cast_pool(WorkerName, Request, PoolName, undefined).
%% @end
%%--------------------------------------------------------------------
-spec cast_pool(WorkerRef :: request_dispatcher:worker_ref(), Request :: term(),
    worker_pool:name()) -> ok | {error, term()}.
cast_pool(WorkerRef, Request, PoolName) ->
    cast_pool(WorkerRef, Request, PoolName, undefined).

%%--------------------------------------------------------------------
%% @doc
%% @equiv cast_pool(WorkerName, Request, PoolName, ReplyTo, undefined).
%% @end
%%--------------------------------------------------------------------
-spec cast_pool(WorkerRef :: request_dispatcher:worker_ref(), Request :: term(),
    worker_pool:name(), ReplyTo :: process_ref())
        -> ok | {error, term()}.
cast_pool(WorkerRef, Request, PoolName, ReplyTo) ->
    cast_pool(WorkerRef, Request, PoolName, ReplyTo, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker. Answer will be send to
%% ReplyTo process. If MsgId is defined, the answer is expected to have
%% such id.
%% Answer is a 'worker_answer' record.
%% Request will be processed by worker from pool PoolName.
%% The pool should be started before call to this function.
%% @equiv cast(WorkerName, Request, ReplyTo, MsgId, {pool, PoolName}).
%% @end
%%--------------------------------------------------------------------
-spec cast_pool(WorkerRef :: request_dispatcher:worker_ref(), Request :: term(),
    worker_pool:name(), ReplyTo :: process_ref(), MsgId :: term() | undefined)
        -> ok | {error, term()}.
cast_pool(WorkerRef, Request, PoolName, ReplyTo, MsgId) ->
    cast(WorkerRef, Request, ReplyTo, MsgId, {pool, cast, PoolName}).


%%--------------------------------------------------------------------
%% @doc
%% @equiv multicast(WorkerName, Request, undefined)
%% @end
%%--------------------------------------------------------------------
-spec multicast(WorkerName :: request_dispatcher:worker_name(), Request :: term()) -> ok.
multicast(WorkerName, Request) ->
    multicast(WorkerName, Request, undefined).

%%--------------------------------------------------------------------
%% @doc
%% @equiv multicast(WorkerName, Request, ReplyTo, undefined)
%% @end
%%--------------------------------------------------------------------
-spec multicast(WorkerName :: request_dispatcher:worker_name(), Request :: term(),
    ReplyTo :: process_ref()) -> ok.
multicast(WorkerName, Request, ReplyTo) ->
    multicast(WorkerName, Request, ReplyTo, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to all workers, answers with given MsgId
%% are expected at ReplyTo process/gen_server. The answers will be
%% 'worker_answer' records.
%% @end
%%--------------------------------------------------------------------
-spec multicast(WorkerName :: request_dispatcher:worker_name(), Request :: term(),
    ReplyTo :: process_ref(), MsgId :: term() | undefined) -> ok.
multicast(WorkerName, Request, ReplyTo, MsgId) ->
    {ok, Nodes} = request_dispatcher:get_worker_nodes(WorkerName),
    lists_utils:pforeach(fun(Node) ->
        cast({WorkerName, Node}, Request, ReplyTo, MsgId)
    end, Nodes).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Synchronously send request to worker with given timeout.
%% It depends on ExecOption whether request will be processed by
%% calling process, spawned process or process from pool.
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerRef :: request_dispatcher:worker_ref(), Request :: term(),
    Timeout :: timeout_spec(), execute_type()) ->
    Result :: term() | {error, term()}.
call(WorkerRef, Request, Timeout, ExecOption) ->
    MsgId = make_ref(),
    {Name, Node} = choose_node(WorkerRef),
    Args = prepare_args(Name, Request, MsgId),
    ExecuteAns = execute(Args, Node, ExecOption, Timeout),
    receive_loop(ExecuteAns, MsgId, Timeout, WorkerRef, Request, true).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Waits for worker answer.
%% @end
%%--------------------------------------------------------------------
-spec receive_loop(ExecuteAns :: term(), MsgId :: term(),
    Timeout :: timeout_spec(), WorkerRef :: request_dispatcher:worker_ref(),
    Request :: term(), CheckAndRetry :: boolean()) -> Result :: term() | {error, term()}.
receive_loop(ExecuteAns, MsgId, Timeout, WorkerRef, Request, CheckAndRetry) ->
    {AfterTime, TimeoutFun} = case Timeout of
        {_Time, _Fun} -> Timeout;
        _ -> {Timeout, fun() -> ok end}
    end,
    receive
        #worker_answer{id = MsgId, response = Response} -> Response
    after AfterTime ->
        case {CheckAndRetry, is_pid(ExecuteAns) andalso
            rpc:call(node(ExecuteAns), erlang, is_process_alive, [ExecuteAns])} of
            {true, true} ->
                TimeoutFun(),
                receive_loop(ExecuteAns, MsgId, Timeout, WorkerRef, Request, CheckAndRetry);
            {true, _} ->
                % retry last time to prevent race between answer sending / process terminating
                TimeoutFun(),
                receive_loop(ExecuteAns, MsgId, Timeout, WorkerRef, Request, false);
            _ ->
                LogRequest = application:get_env(?CLUSTER_WORKER_APP_NAME, log_requests_on_error, false),
                {MsgFormat, FormatArgs} = case LogRequest of
                    true ->
                        MF = "Worker: ~tp, request: ~tp exceeded timeout of ~tp ms",
                        FA = [WorkerRef, Request, AfterTime],
                        {MF, FA};
                    _ ->
                        MF = "Worker: ~tp, exceeded timeout of ~tp ms",
                        FA = [WorkerRef, AfterTime],
                        {MF, FA}
                end,

                WorkerName = case WorkerRef of
                    {id, Name, _Key} -> Name;
                    {Name, _Node} -> Name;
                    Name -> Name
                end,
                LogKey = list_to_atom("proxy_timeout_" ++ atom_to_list(WorkerName)),
                node_manager:single_error_log(LogKey, MsgFormat, FormatArgs),
                {error, timeout}
        end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes request by calling worker_host:proc_request.
%% It depends on ExecOption whether request will be processed by
%% the calling process spawned process or process from pool.
%% Timeout argument is ignored for direct and spawn exec options.
%% @end
%%--------------------------------------------------------------------
-spec execute(Args :: list(), node(), ExecOption :: execute_type(),
    Timeout :: timeout_spec() | undefined) -> term().
execute(Args, _Node, direct, _Timeout) ->
    % TODO VFS-4025 check usage - not optimal implementation
    apply(worker_host, proc_request, Args);
execute(Args, Node, spawn, _Timeout) ->
    spawn(Node, worker_host, proc_request, Args);
execute(Args, _Node, {pool, call, PoolName}, Timeout) ->
    % TODO VFS-4025
    worker_pool:call(PoolName, {worker_host, proc_request, Args}, worker_pool:default_strategy(), Timeout);
execute(Args, _Node, {pool, cast, PoolName}, _) ->
    % TODO VFS-4025
    worker_pool:cast(PoolName, {worker_host, proc_request, Args}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Prepares list of args for execute/3 function.
%% @equiv prepare_args(Plugin, Request, MsgId, {proc, self()}).
%% @end
%%--------------------------------------------------------------------
-spec prepare_args(request_dispatcher:worker_ref(), term(), term()) -> list().
prepare_args(Plugin, Request, MsgId) ->
    prepare_args(Plugin, Request, MsgId, {proc, self()}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Prepares list of args for execute/3 function.
%% @end
%%--------------------------------------------------------------------
-spec prepare_args(request_dispatcher:worker_ref(), term(), term(),
    process_ref()) -> list().
prepare_args(Plugin, Request, MsgId, ReplyTo) ->
    [Plugin, #worker_request{
        req = Request,
        id = MsgId,
        reply_to = ReplyTo
    }].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Chooses a node to send a worker request to.
%% @end
%%--------------------------------------------------------------------
-spec choose_node(WorkerRef :: request_dispatcher:worker_ref()) ->
    {WorkerName :: request_dispatcher:worker_name(), WorkerNode :: node()}.
choose_node(WorkerRef) ->
    case WorkerRef of
        {id, WName, Id} ->
            {WName, datastore_key:any_responsible_node(Id)};
        {WName, WNode} ->
            {WName, WNode};
        WName ->
            {WName, node()}
    end.
