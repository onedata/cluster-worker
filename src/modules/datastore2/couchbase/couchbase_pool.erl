%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for management of CouchBase connections (worker pool) and request routing.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_pool).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/4]).
-export([post/3, post/4]).
-export([get_modes/0, get_queue_size/1, get_queue_size/2, get_queue_size/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(queue, {
    size = 0 :: non_neg_integer(),
    data = queue:new() :: queue:queue()
}).

-record(state, {
    db_hosts :: [datastore_config2:db_host()],
    bucket :: couchbase_driver:bucket(),
    mode :: mode(),
    worker_queue :: queue(),
    request_queue :: queue(),
    batch_size :: non_neg_integer()
}).

-type mode() :: read | write.
-type request() :: {save, couchbase_driver:key(), couchbase_driver:value()} |
                   {remove, couchbase_driver:key()} |
                   {mget, [couchbase_driver:key()]} |
                   {update_counter, couchbase_driver:key(),
                       Offset :: non_neg_integer(),
                       Default :: non_neg_integer()} |
                   {save_design_doc, couchbase_driver:design(),
                       couchbase_driver:value()} |
                   {delete_design_doc, couchbase_driver:design()} |
                   {query_view, couchbase_driver:design(),
                       couchbase_driver:view(), [couchbase_driver:view_opt()]}.
-type response() :: ok | {ok, Result :: term()} | {error, Reason :: term()} |
                    list(response()).
-type queue() :: #queue{}.
-type queue_type() :: request | worker.
-type state() :: #state{}.

-export_type([mode/0, queue_type/0, request/0, response/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase worker pool manager.
%% @end
%%--------------------------------------------------------------------
-spec start_link(couchbase_driver:bucket(), mode(),
    [datastore_config2:db_host()], non_neg_integer()) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(Bucket, Mode, DbHosts, Size) ->
    gen_server2:start_link(?MODULE, [Bucket, DbHosts, Mode, Size], []).

%%--------------------------------------------------------------------
%% @equiv post(Bucket, Mode, Request, 30000)
%% @end
%%--------------------------------------------------------------------
-spec post(couchbase_driver:bucket(), mode(), request()) -> response().
post(Bucket, Mode, Request) ->
    Timeout = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_request_timeout, timer:seconds(30)),
    post(Bucket, Mode, Request, Timeout).

%%--------------------------------------------------------------------
%% @doc
%% Schedules request execution on a worker pool and awaits response.
%% @end
%%--------------------------------------------------------------------
-spec post(couchbase_driver:bucket(), mode(), request(), timeout()) -> response().
post(Bucket, Mode, Request, Timeout) ->
    Ref = make_ref(),
    Pool = couchbase_pool_sup:get_pool(Bucket, Mode),
    gen_server2:cast(Pool, {post, Ref, self(), Request}),
    receive
        {Ref, Response} -> Response
    after
        Timeout -> {error, timeout}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of available worker pool modes.
%% @end
%%--------------------------------------------------------------------
-spec get_modes() -> [mode()].
get_modes() ->
    [read, write].

%%--------------------------------------------------------------------
%% @doc
%% Returns size of selected queue type for all buckets and modes.
%% @end
%%--------------------------------------------------------------------
-spec get_queue_size(queue_type()) -> non_neg_integer().
get_queue_size(Type) ->
    lists:foldl(fun(Bucket, Size) ->
        Size + get_queue_size(Type, Bucket)
    end, 0, couchbase_driver:get_buckets()).

%%--------------------------------------------------------------------
%% @doc
%% Returns size of selected queue type for given bucket and all modes.
%% @end
%%--------------------------------------------------------------------
-spec get_queue_size(queue_type(), couchbase_driver:bucket()) ->
    non_neg_integer().
get_queue_size(Type, Bucket) ->
    lists:foldl(fun(Mode, Size) ->
        Size + get_queue_size(Type, Bucket, Mode)
    end, 0, get_modes()).

%%--------------------------------------------------------------------
%% @doc
%% Returns size of selected queue type for given bucket and mode.
%% @end
%%--------------------------------------------------------------------
-spec get_queue_size(queue_type(), couchbase_driver:bucket(), mode()) ->
    non_neg_integer().
get_queue_size(Type, Bucket, Mode) ->
    Pool = couchbase_pool_sup:get_pool(Bucket, Mode),
    gen_server2:call(Pool, {get_queue_size, Type}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes CouchBase worker pool manager.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([Bucket, DbHosts, Mode, Size]) ->
    process_flag(trap_exit, true),
    couchbase_pool_sup:register_pool(Bucket, Mode, self()),
    {ok, #state{
        bucket = Bucket,
        mode = Mode,
        worker_queue = start_workers(Bucket, DbHosts, Size),
        request_queue = #queue{},
        batch_size = application:get_env(?CLUSTER_WORKER_APP_NAME,
            couchbase_pool_batch_size, 50),
        db_hosts = DbHosts
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call({get_queue_size, request}, _From, #state{} = State) ->
    {reply, State#state.request_queue#queue.size, State};
handle_call({get_queue_size, worker}, _From, #state{} = State) ->
    {reply, State#state.worker_queue#queue.size, State};
handle_call(Request, _From, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast({post, Ref, From, Request}, #state{
    request_queue = RQueue,
    worker_queue = #queue{size = 0}
} = State) ->
    {noreply, State#state{
        request_queue = queue({Ref, From, Request}, RQueue)
    }};
handle_cast({post, Ref, From, Request}, #state{
    worker_queue = WQueue
} = State) ->
    {Worker, WQueue2} = dequeue(WQueue),
    gen_server2:cast(Worker, {post, [{Ref, From, Request}]}),
    {noreply, State#state{worker_queue = WQueue2}};
handle_cast({worker, Worker}, #state{
    worker_queue = WQueue,
    request_queue = RQueue,
    batch_size = BatchSize
} = State) ->
    case RQueue#queue.size > 0 of
        true ->
            {Requests, RQueue2} = dequeue(BatchSize, RQueue),
            gen_server2:cast(Worker, {post, Requests}),
            {noreply, State#state{request_queue = RQueue2}};
        false ->
            State2 = State#state{worker_queue = queue(Worker, WQueue)},
            {noreply, State2}
    end;
handle_cast(Request, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info({'EXIT', _Worker, normal}, #state{} = State) ->
    {noreply, State};
handle_info({'EXIT', Worker, Reason}, #state{} = State) ->
    ?error("Couchbase pool worker ~p exited with reason: ~p", [Worker, Reason]),
    #state{db_hosts = DbHosts, bucket = Bucket, worker_queue = WQueue} = State,
    Worker2 = start_worker(Bucket, DbHosts),
    gen_server2:cast(self(), {worker, Worker2}),
    Workers = queue:filter(fun(W) -> W =/= Worker end, WQueue#queue.data),
    WQueue2 = WQueue#queue{
        size = queue:len(Workers),
        data = Workers
    },
    {noreply, State#state{worker_queue = WQueue2}};
handle_info(Info, #state{} = State) ->
    ?log_bad_request(Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: state()) -> term().
terminate(Reason, #state{bucket = Bucket, mode = Mode} = State) ->
    couchbase_pool_sup:unregister_pool(Bucket, Mode),
    ?log_terminate(Reason, State).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts CouchBase pool worker.
%% @end
%%--------------------------------------------------------------------
-spec start_worker(couchbase_driver:bucket(), couchbase_driver:db_host()) ->
    pid().
start_worker(Bucket, DbHosts) ->
    {ok, Worker} = couchbase_pool_worker:start_link(self(), Bucket, DbHosts),
    Worker.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes worker pool by starting given amount of workers.
%% @end
%%--------------------------------------------------------------------
-spec start_workers(couchbase_driver:bucket(), [couchbase_driver:db_host()],
    non_neg_integer()) -> queue().
start_workers(Bucket, DbHosts, Size) ->
    lists:foldl(fun(_, WorkerQueue) ->
        Worker = start_worker(Bucket, DbHosts),
        queue(Worker, WorkerQueue)
    end, #queue{}, lists:seq(1, Size)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes an item from the front of the queue.
%% Fails with an error if the queue is empty.
%% @end
%%--------------------------------------------------------------------
-spec dequeue(queue()) -> {Item :: term(), queue()}.
dequeue(#queue{size = Size, data = Data} = Queue) ->
    {{value, Item}, Data2} = queue:out(Data),
    {Item, Queue#queue{size = Size - 1, data = Data2}}.

%%--------------------------------------------------------------------
%% @private
%% @equiv dequeue(Count, Queue, [])
%% @end
%%--------------------------------------------------------------------
-spec dequeue(non_neg_integer(), queue()) -> {Items :: list(), queue()}.
dequeue(Count, #queue{} = Queue) ->
    dequeue(Count, Queue, []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes provided number of items from the front of the queue.
%% If queue size if smaller that the requested number returns all elements.
%% @end
%%--------------------------------------------------------------------
-spec dequeue(non_neg_integer(), queue(), list()) -> {Items :: list(), queue()}.
dequeue(0, #queue{} = Queue, Items) ->
    {lists:reverse(Items), Queue};
dequeue(_Count, #queue{size = 0} = Queue, Items) ->
    {lists:reverse(Items), Queue};
dequeue(Count, #queue{} = Queue, Items) ->
    {Item, Queue2} = dequeue(Queue),
    dequeue(Count - 1, Queue2, [Item | Items]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds an item to the queue.
%% @end
%%--------------------------------------------------------------------
-spec queue(Item :: term(), queue()) -> queue().
queue(Value, #queue{size = Size, data = Data} = Queue) ->
    Queue#queue{size = Size + 1, data = queue:in(Value, Data)}.