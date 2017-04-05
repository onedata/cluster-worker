%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for worker pool management and request routing.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_pool).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("timeouts.hrl").
-include("modules/datastore/datastore_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0]).
-export([save_doc/3, save_doc_asynch/3, delete_doc/3, delete_doc_asynch/3,
    receive_response/1, queue_size/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(queue, {
    worker :: undefined | pid(),
    size = 0 :: integer(),
    flush_ref :: undefined | reference()
}).

-record(state, {
    requests = #{} :: #{request_key() => [request()]},
    queues = #{} :: #{request_key() => queue()},
    idle_workers = [] :: [pid()]
}).

-type state() :: #state{}.
-type queue() :: #queue{}.
-type request_key() :: {datastore:bucket(), action()}.
-type request() :: {pid(), reference(), term()}.
-type action() :: save_docs | delete_doc.

-export_type([action/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the transaction process.
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server2:start_link({local, ?DATASTORE_POOL_MANAGER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Forwards document save request to a worker pool.
%% @end
%%--------------------------------------------------------------------
-spec save_doc(module(), model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save_doc(Driver, ModelConfig, Doc) ->
    Ref = save_doc_asynch(Driver, ModelConfig, Doc),
    receive_response(Ref).

%%--------------------------------------------------------------------
%% @doc
%% Forwards document deletion request to a worker pool.
%% @end
%%--------------------------------------------------------------------
-spec delete_doc(module(), model_behaviour:model_config(), datastore:document()) ->
    ok | datastore:generic_error().
delete_doc(Driver, ModelConfig, Doc) ->
    Ref = make_ref(),
    Request = {delete_doc, Driver, ModelConfig, Doc},
    gen_server:cast(?DATASTORE_POOL_MANAGER, {self(), Ref, Request}),
    receive_response(Ref).

%%--------------------------------------------------------------------
%% @doc
%% Forwards document deletion request to a worker pool.
%% @end
%%--------------------------------------------------------------------
-spec delete_doc_asynch(module(), model_behaviour:model_config(), datastore:document()) ->
    reference().
delete_doc_asynch(Driver, ModelConfig, Doc) ->
    Ref = make_ref(),
    Request = {delete_doc, Driver, ModelConfig, Doc},
    gen_server:cast(?DATASTORE_POOL_MANAGER, {self(), Ref, Request}),
    Ref.

%%--------------------------------------------------------------------
%% @doc
%% Saves document not using transactions and not waiting for answer.
%% Returns ref to receive answer asynch.
%% @end
%%--------------------------------------------------------------------
-spec save_doc_asynch(module(), model_behaviour:model_config(), datastore:document()) ->
    reference().
save_doc_asynch(Driver, ModelConfig, Doc) ->
    Ref = make_ref(),
    Request = {save_docs, Driver, ModelConfig, Doc},
    gen_server:cast(?DATASTORE_POOL_MANAGER, {self(), Ref, Request}),
    Ref.

%%--------------------------------------------------------------------
%% @doc
%% Waits and returns answer of asynch save operation.
%% @end
%%--------------------------------------------------------------------
-spec receive_response(reference()) ->
    ok | {ok, datastore:ext_key()} | datastore:generic_error().
receive_response(Ref) ->
    receive
        {Ref, Response} -> Response
    after
        ?DOCUMENT_AGGREGATE_SAVE_TIMEOUT -> {error, timeout}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns number of requests pending to be processed by a worker.
%% @end
%%--------------------------------------------------------------------
-spec queue_size() -> non_neg_integer().
queue_size() ->
    gen_server:call(?DATASTORE_POOL_MANAGER, queue_size).

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
init([]) ->
    {ok, #state{}}.

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
handle_call(queue_size, _From, #state{requests = RequestsMap} = State) ->
    QueueSize = maps:fold(fun(_, Requests, Size) ->
        Size + length(Requests)
    end, 0, RequestsMap),
    {reply, QueueSize, State};
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
handle_cast({register, Worker}, #state{
    requests = RequestsMap,
    idle_workers = Workers
} = State) ->
    State2 = State#state{idle_workers = [Worker | Workers]},
    case maps:keys(RequestsMap) of
        [] ->
            {noreply, State2};
        [Key | _] ->
            Requests = maps:get(Key, RequestsMap, []),
            {noreply, queue_requests(Requests, Key, #queue{}, State2)}
    end;
handle_cast({unregister, Worker}, #state{idle_workers = Workers} = State) ->
    {noreply, State#state{idle_workers = lists:delete(Worker, Workers)}};
handle_cast(Request, #state{queues = QueuesMap} = State) ->
    Key = get_request_key(Request),
    Queue = maps:get(Key, QueuesMap, #queue{}),
    {noreply, queue_request(Request, Key, Queue, State)}.

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
handle_info({flush, {Ref, {Bucket, _} = Key}}, #state{queues = QueuesMap} = State) ->
    case maps:find(Key, QueuesMap) of
        {ok, #queue{worker = Worker, flush_ref = Ref}} ->
            gen_server:cast(Worker, {flush, Bucket}),
            {noreply, State#state{queues = maps:remove(Key, QueuesMap)}};
        _ ->
            {noreply, State}
    end;
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
terminate(Reason, #state{} = State) ->
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
%% Returns request key consisting of a bucket name associated with the request
%% and action name.
%% @end
%%--------------------------------------------------------------------
-spec get_request_key(request()) -> request_key().
get_request_key({_Pid, _Ref, {Action, Driver, ModelConfig, Doc}}) ->
    Bucket = Driver:select_bucket(ModelConfig, Doc),
    {Bucket, Action}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards request to a dedicated worker or it all workers are busy stores
%% it in a queue.
%% @end
%%--------------------------------------------------------------------
-spec queue_request(request(), request_key(), queue(), state()) -> state().
queue_request(Request, Key, #queue{worker = undefined}, #state{
    requests = RequestsMap,
    idle_workers = []
} = State) ->
    Requests = maps:get(Key, RequestsMap, []),
    State#state{requests = maps:put(Key, [Request | Requests], RequestsMap)};
queue_request(Request, Key, #queue{worker = undefined} = Queue, #state{
    idle_workers = [Worker | Workers]
} = State) ->
    Queue2 = Queue#queue{worker = Worker},
    queue_request(Request, Key, Queue2, State#state{idle_workers = Workers});
queue_request(Request, Key, #queue{flush_ref = undefined} = Queue, State) ->
    {ok, Delay} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        datastore_pool_queue_flush_delay),
    Ref = make_ref(),
    erlang:send_after(Delay, self(), {flush, {Ref, Key}}),
    queue_request(Request, Key, Queue#queue{flush_ref = Ref}, State);
queue_request(Request, Key, #queue{} = Queue, #state{
    queues = QueuesMap
} = State) ->
    {ok, SizeLimit} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        datastore_pool_queue_size),
    #queue{worker = Worker, size = Size} = Queue,
    Queue2 = Queue#queue{size = Size + 1},
    gen_server:cast(Worker, Request),
    case Queue2#queue.size >= SizeLimit of
        true ->
            {Bucket, _} = Key,
            gen_server:cast(Worker, {flush, Bucket}),
            State#state{queues = maps:remove(Key, QueuesMap)};
        false ->
            State#state{queues = maps:put(Key, Queue2, QueuesMap)}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes requests with a default limit.
%% @end
%%--------------------------------------------------------------------
-spec queue_requests([request()], request_key(), queue(), state()) -> state().
queue_requests(Requests, Key, Queue, State) ->
    {ok, SizeLimit} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        datastore_pool_queue_size),
    queue_requests(Requests, Key, Queue, SizeLimit, State).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes requests by calling {@link queue_request/4} function.
%% Number of processed requests can not exceed SizeLimit.
%% @end
%%--------------------------------------------------------------------
-spec queue_requests([request()], request_key(), queue(), integer(), state()) ->
    state().
queue_requests([], Key, _Queue, _SizeLimit, #state{
    requests = RequestsMap
} = State) ->
    State#state{requests = maps:remove(Key, RequestsMap)};
queue_requests(Requests, Key, _Queue, 0, #state{
    requests = RequestsMap
} = State) ->
    State#state{requests = maps:put(Key, Requests, RequestsMap)};
queue_requests([Request | Requests], Key, Queue, SizeLimit, State) ->
    State2 = queue_request(Request, Key, Queue, State),
    Queue2 = maps:get(Key, State2#state.queues, #queue{}),
    queue_requests(Requests, Key, Queue2, SizeLimit - 1, State2).
