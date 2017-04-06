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
-module(datastore_pool).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/4]).
-export([post_async/3, post_sync/3, post_sync/4]).
-export([wait/1, wait/2]).
-export([modes/0, request_queue_size/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(queue, {
    size = 0 :: non_neg_integer(),
    data = queue:new() :: queue:queue()
}).

-record(state, {
    bucket :: binary(),
    mode :: mode(),
    worker_queue :: queue(),
    request_queue :: queue(),
    batch_size :: non_neg_integer(),
    batch_delay :: non_neg_integer(),
    flush_ref :: undefined | reference()
}).

-type mode() :: read | write.
-type request() :: term().
-type response() :: term().
-type queue() :: #queue{}.
-type state() :: #state{}.

-export_type([mode/0, request/0, response/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase worker pool manager.
%% @end
%%--------------------------------------------------------------------
-spec start_link(binary(), mode(), non_neg_integer(), non_neg_integer()) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(Bucket, Mode, Size, Delay) ->
    gen_server2:start_link(?MODULE, [Bucket, Mode, Size, Delay], []).

%%--------------------------------------------------------------------
%% @doc
%% Schedules request execution on a worker pool and returns a reference
%% to a future response.
%% @end
%%--------------------------------------------------------------------
-spec post_async(binary(), mode(), request()) -> reference().
post_async(Bucket, Mode, Request) ->
    Ref = make_ref(),
    Pool = datastore_pool_sup:get_pool(Bucket, Mode),
    gen_server2:cast(Pool, {post, Ref, self(), Request}),
    Ref.

%%--------------------------------------------------------------------
%% @equiv post_sync(Bucket, Mode, Request, 300000)
%% @end
%%--------------------------------------------------------------------
-spec post_sync(binary(), mode(), request()) -> response().
post_sync(Bucket, Mode, Request) ->
    post_sync(Bucket, Mode, Request, timer:minutes(5)).

%%--------------------------------------------------------------------
%% @doc
%% Schedules request execution on a worker pool and awaits response.
%% @end
%%--------------------------------------------------------------------
-spec post_sync(binary(), mode(), request(), timeout()) -> response().
post_sync(Bucket, Mode, Request, Timeout) ->
    Ref = post_async(Bucket, Mode, Request),
    wait(Ref, Timeout).

%%--------------------------------------------------------------------
%% @equiv wait(Ref, 300000)
%% @end
%%--------------------------------------------------------------------
-spec wait(reference()) -> response().
wait(Ref) ->
    wait(Ref, timer:minutes(5)).

%%--------------------------------------------------------------------
%% @doc
%% Returns response associated with provided reference or fails with timeout.
%% @end
%%--------------------------------------------------------------------
-spec wait(reference(), timeout()) -> response().
wait(Ref, Timeout) ->
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
-spec modes() -> [{mode(), non_neg_integer()}].
modes() ->
    [
        {read, 0},
        {write, application:get_env(?CLUSTER_WORKER_APP_NAME,
            datastore_pool_batch_delay, 5000)}
    ].

%%--------------------------------------------------------------------
%% @doc
%% Returns size of waiting processes queue.
%% @end
%%--------------------------------------------------------------------
-spec request_queue_size() -> non_neg_integer().
request_queue_size() ->
    Buckets = couchdb_datastore_driver:get_buckets(),
    lists:foldl(fun(Bucket, Size) ->
        lists:foldl(fun({Mode, _}, Size2) ->
            Pool = datastore_pool_sup:get_pool(Bucket, Mode),
            Size2 + gen_server2:call(Pool, request_queue_size)
        end, Size, modes())
    end, 0, Buckets).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the transaction process.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([Bucket, Mode, Size, Delay]) ->
    process_flag(trap_exit, true),
    datastore_pool_sup:register_pool(Bucket, Mode, self()),
    {ok, schedule_flush(#state{
        bucket = Bucket,
        mode = Mode,
        worker_queue = start_workers(Bucket, Size),
        request_queue = #queue{},
        batch_size = application:get_env(?CLUSTER_WORKER_APP_NAME,
            datastore_pool_batch_size, 25),
        batch_delay = Delay
    })}.

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
handle_call(request_queue_size, _From, #state{} = State) ->
    {reply, State#state.request_queue#queue.size, State};
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
    request_queue = RQueue,
    worker_queue = WQueue,
    batch_size = BatchSize,
    batch_delay = BatchDelay
} = State) ->
    Size = case BatchDelay of
        0 -> 1;
        _ -> BatchSize
    end,
    RQueue2 = queue({Ref, From, Request}, RQueue),
    case RQueue2#queue.size >= Size of
        true ->
            {Worker, WQueue2} = dequeue(WQueue),
            {Requests, RQueue3} = dequeue(BatchSize, RQueue2),
            gen_server2:cast(Worker, {post, Requests}),
            {noreply, State#state{
                request_queue = RQueue3,
                worker_queue = WQueue2
            }};
        false ->
            {noreply, schedule_flush(State#state{request_queue = RQueue2})}
    end;
handle_cast({worker, Worker}, #state{
    worker_queue = WQueue,
    request_queue = RQueue,
    batch_size = BatchSize,
    batch_delay = BatchDelay
} = State) ->
    Size = case BatchDelay of
        0 -> 1;
        _ -> BatchSize
    end,
    case RQueue#queue.size >= Size of
        true ->
            {Requests, RQueue2} = dequeue(BatchSize, RQueue),
            gen_server2:cast(Worker, {post, Requests}),
            {noreply, State#state{
                request_queue = RQueue2
            }};
        false ->
            State2 = State#state{worker_queue = queue(Worker, WQueue)},
            {noreply, schedule_flush(State2)}
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
handle_info(flush, #state{worker_queue = #queue{size = 0}} = State) ->
    {noreply, State#state{flush_ref = undefined}};
handle_info(flush, #state{request_queue = #queue{size = 0}} = State) ->
    {noreply, State#state{flush_ref = undefined}};
handle_info(flush, #state{
    request_queue = RQueue,
    worker_queue = WQueue,
    batch_size = BatchSize
} = State) ->
    {Worker, WQueue2} = dequeue(WQueue),
    {Requests, RQueue2} = dequeue(BatchSize, RQueue),
    erlang:send_after(State#state.batch_delay, self(), flush),
    gen_server2:cast(Worker, {post, Requests}),
    {noreply, State#state{
        request_queue = RQueue2,
        worker_queue = WQueue2,
        flush_ref = undefined
    }};
handle_info({'EXIT', _Worker, normal}, #state{} = State) ->
    {noreply, State};
handle_info({'EXIT', Worker, Reason}, #state{bucket = Bucket} = State) ->
    ?error("Couchbase pool worker ~p exited with reason: ~p", [Worker, Reason]),
    Worker2 = start_worker(Bucket),
    gen_server2:cast(self(), {worker, Worker2}),
    {noreply, State};
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
    datastore_pool_sup:unregister_pool(Bucket, Mode),
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
-spec start_worker(binary()) -> pid().
start_worker(Bucket) ->
    {ok, Worker} = datastore_pool_worker:start_link(Bucket, self()),
    Worker.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes worker pool by starting given amount of workers.
%% @end
%%--------------------------------------------------------------------
-spec start_workers(binary(), non_neg_integer()) -> queue().
start_workers(Bucket, Size) ->
    lists:foldl(fun(_, WorkerQueue) ->
        Worker = start_worker(Bucket),
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules pool flush.
%% @end
%%--------------------------------------------------------------------
-spec schedule_flush(state()) -> state().
schedule_flush(#state{batch_delay = 0} = State) ->
    State;
schedule_flush(#state{request_queue = #queue{size = 0}} = State) ->
    State;
schedule_flush(#state{batch_delay = Delay, flush_ref = undefined} = State) ->
    State#state{flush_ref = erlang:send_after(Delay, self(), flush)};
schedule_flush(#state{} = State) ->
    State.