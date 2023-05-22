%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and represents single connection
%%% to the CouchBase database. It is responsible for requests aggregation and
%%% batch processing.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_pool_worker).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

% TODO VFS-3871 - check why it has so many binaries with high reference count

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% for timer:tc
-export([wait/2]).

-type id() :: non_neg_integer().
-type request() :: {reference(), pid(), couchbase_pool:request()}.
-type batch_requests() :: #{save := [couchbase_crud:save_request()],
                            get := [couchbase_crud:get_request()],
                            delete := [couchbase_crud:delete_request()],
                            delta := non_neg_integer()}.
-type batch_responses() :: #{save := [couchbase_crud:save_response()],
                             get := [couchbase_crud:get_response()],
                             delete := [couchbase_crud:delete_response()]}.

-export_type([id/0]).

-record(state, {
    bucket :: couchbase_config:bucket(),
    mode :: couchbase_pool:mode(),
    id :: id(),
    requests_queue :: queue:queue(request()),
    connection :: cberl:connection()
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase pool worker.
%% @end
%%--------------------------------------------------------------------
-spec start_link(couchbase_config:bucket(), couchbase_pool:mode(), id(),
    couchbase_driver:db_host(), cberl_nif:client()) ->
    {ok, pid()} | {error, term()}.
start_link(Bucket, Mode, Id, DbHosts, Client) ->
    gen_server:start_link(?MODULE, [Bucket, Mode, Id, DbHosts, Client], []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes CouchBase pool worker.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([Bucket, Mode, Id, DbHosts, Client]) ->
    process_flag(trap_exit, true),
    reset_reconnect_retry_timer(),

    Host = lists:foldl(fun(DbHost, Acc) ->
        <<Acc/binary, ";", DbHost/binary>>
    end, hd(DbHosts), tl(DbHosts)),
    Opts = get_connect_opts(),
    Timeout = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_config_total_timeout, timer:seconds(30)),
    {ok, Connection} = case Client of
        undefined -> cberl:connect(Host, <<>>, <<>>, Bucket, Opts, Timeout);
        _ -> cberl:connect(Host, <<>>, <<>>, Bucket, Opts, Timeout, Client)
    end,

    couchbase_pool:reset_request_queue_size(Bucket, Mode, Id),
    couchbase_pool_sup:register_worker(Bucket, Mode, Id, self()),

    {ok, #state{
        bucket = Bucket,
        mode = Mode,
        id = Id,
        requests_queue = queue:new(),
        connection = Connection
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
handle_call(ping, _From, #state{} = State) ->
    {reply, pong, State};
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
handle_info({post, Request}, #state{requests_queue = Queue} = State) ->
    State2 = State#state{requests_queue = queue:in(Request, Queue)},
    {noreply, process_requests(State2)};
handle_info({'EXIT', _, Reason}, State = #state{}) ->
    {stop, Reason, State};
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
terminate(Reason, #state{bucket = Bucket, mode = Mode, id = Id} = State) ->
    case Reason of
        normal -> ok;
        shutdown -> ok;
        {shutdown, _} -> ok;
        _ -> reset_reconnect_retry_timer()
    end,
    catch couchbase_pool_sup:unregister_worker(Bucket, Mode, Id, self()),
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
%% Returns CouchBase server configuration.
%% @end
%%--------------------------------------------------------------------
-spec get_connect_opts() -> [cberl:connect_opt()].
get_connect_opts() ->
    lists:map(fun({OptName, EnvName, OptDefault}) ->
        OptValue = application:get_env(
            ?CLUSTER_WORKER_APP_NAME, EnvName, OptDefault
        ),
        {OptName, 1000 * OptValue}
    end, [
        {operation_timeout, couchbase_operation_timeout, timer:seconds(60)},
        {config_total_timeout, couchbase_config_total_timeout, timer:seconds(30)},
        {view_timeout, couchbase_view_timeout, timer:seconds(120)},
        {durability_interval, couchbase_durability_interval, 500},
        {durability_timeout, couchbase_durability_timeout, timer:seconds(300)},
        {http_timeout, couchbase_http_timeout, timer:seconds(60)}
    ]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Receives and processes pending requests in batch.
%% @end
%%--------------------------------------------------------------------
-spec process_requests(state()) -> state().
process_requests(State) ->
    Size = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, 1000),
    State2 = receive_pending_requests(State),
    #state{requests_queue = Queue} = State2,
    {Requests, Queue2} = dequeue(Size, Queue, []),
    State3 = State2#state{requests_queue = Queue2},
    handle_requests(Requests, State3),

    case application:get_env(?CLUSTER_WORKER_APP_NAME, couchbase_pool_gc, on) of
        on ->
            erlang:garbage_collect();
        _ ->
            ok
    end,

    case queue:is_empty(Queue2) of
        true -> State3;
        false -> process_requests(State3)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes pending requests from message inbox.
%% @end
%%--------------------------------------------------------------------
-spec receive_pending_requests(state()) -> state().
receive_pending_requests(#state{requests_queue = Queue} = State) ->
    receive
        {post, Request} ->
            receive_pending_requests(State#state{
                requests_queue = queue:in(Request, Queue)
            })
    after
        0 -> State
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles requests in batch.
%% @end
%%--------------------------------------------------------------------
-spec handle_requests([request()], state()) -> ok.
handle_requests(Requests, #state{} = State) ->
    #state{
        bucket = Bucket,
        mode = Mode,
        id = Id,
        connection = Connection
    } = State,

    RequestsBatch = #{delta := Delta} = batch_requests(Requests),
    couchbase_pool:update_request_queue_size(Bucket, Mode, Id, Delta),

    ResponsesBatch = handle_requests_batch(Connection, RequestsBatch),
    lists:foreach(fun({Ref, From, Request}) ->
        Response = try
            handle_request(Connection, Request, ResponsesBatch)
        catch
            _:Reason:Stacktrace -> {error, {Reason, Stacktrace}}
        end,
        From ! {Ref, Response}
    end, Requests),

    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates batch requests for save, get and delete operations.
%% @end
%%--------------------------------------------------------------------
-spec batch_requests([request()]) -> batch_requests().
batch_requests(Requests) ->
    RequestsBatch2 = lists:foldl(fun({_Ref, _From, Request}, #{delta := Delta} = RequestsBatch) ->
        batch_request(Request, RequestsBatch#{delta => Delta + couchbase_pool:get_request_size(Request)})
    end, #{
        save => [],
        get => gb_sets:new(),
        delete => [],
        delta => 0
    }, Requests),
    RequestsBatch2#{get => gb_sets:to_list(maps:get(get, RequestsBatch2))}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds request to a batch if possible.
%% @end
%%--------------------------------------------------------------------
-spec batch_request(couchbase_pool:request(), map()) ->
    batch_requests().
batch_request({save, Ctx, Key, Value}, RequestsBatch) ->
    #{save := SaveRequests} = RequestsBatch,
    SaveRequests2 = lists:keystore(Key, 2, SaveRequests, {Ctx, Key, Value}),
    RequestsBatch#{save => SaveRequests2};
batch_request({get, Keys}, RequestsBatch) when is_list(Keys) ->
    lists:foldl(fun(Key, RequestsBatchAcc) -> batch_request({get, Key}, RequestsBatchAcc) end, RequestsBatch, Keys);
batch_request({get, Key}, #{get := GetRequests} = RequestsBatch) ->
    RequestsBatch#{get => gb_sets:add(Key, GetRequests)};
batch_request({delete, Ctx, Key}, #{delete := RemoveRequests} = RequestsBatch) ->
    RemoveRequests2 = lists:keystore(Key, 2, RemoveRequests, {Ctx, Key}),
    RequestsBatch#{delete => RemoveRequests2};
batch_request(_Request, RequestsBatch) ->
    RequestsBatch.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles batch requests and returns batch responses.
%% @end
%%--------------------------------------------------------------------
-spec handle_requests_batch(cberl:connection(), batch_requests()) ->
    batch_responses().
handle_requests_batch(Connection, RequestsBatch) ->
    SaveRequests = maps:get(save, RequestsBatch),
    GetRequests = maps:get(get, RequestsBatch),
    RemoveRequests = maps:get(delete, RequestsBatch),

    SaveResponses = handle_save_requests_batch(Connection, SaveRequests),

    GetStopwatch = stopwatch:start(),
    GetResponses = couchbase_crud:get(Connection, GetRequests),
    GetTime = stopwatch:read_micros(GetStopwatch),
    couchbase_batch:check_timeout(GetResponses, get, GetTime),

    DeleteStopwatch = stopwatch:start(),
    DeleteResponses = couchbase_crud:delete(Connection, RemoveRequests),
    DeleteTime = stopwatch:read_micros(DeleteStopwatch),
    couchbase_batch:check_timeout(DeleteResponses, delete, DeleteTime),

    #{
        save => SaveResponses,
        get => GetResponses,
        delete => DeleteResponses
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles save requests batch.
%% @end
%%--------------------------------------------------------------------
-spec handle_save_requests_batch(cberl:connection(),
    [couchbase_crud:save_request()]) -> [couchbase_crud:save_response()].
handle_save_requests_batch(_Connection, []) ->
    [];
handle_save_requests_batch(Connection, Requests) ->
    {SaveRequests, SaveResponses} = couchbase_crud:init_save_requests(
        Connection, Requests
    ),

    {Time1, {SaveRequests2, SaveResponses2}} =
        timer:tc(couchbase_crud, store_change_docs, [
            Connection, SaveRequests
        ]),
    AnalyseAns1 = couchbase_batch:check_timeout(SaveResponses2,
        store_change_docs, Time1),

    WaitChangeDocsDurable = fun() ->
        couchbase_crud:wait_change_docs_durable(
            Connection, SaveRequests2
        )
    end,
    {Time2, {AnalyseAns2, {SaveRequests3, SaveResponses3}}} =
        timer:tc(?MODULE, wait, [
            WaitChangeDocsDurable, wait_change_docs_durable
        ]),

    {Time3, {SaveRequests4, SaveResponses4}} =
        timer:tc(couchbase_crud, store_docs, [
            Connection, SaveRequests3
        ]),
    AnalyseAns3 = couchbase_batch:check_timeout(SaveResponses4, store_docs,
        Time3),

    WaitDocsDurable = fun() ->
        couchbase_crud:wait_docs_durable(
            Connection, SaveRequests4
        )
    end,
    {Time4, {AnalyseAns4, {SaveRequests5, SaveResponses5}}} =
        timer:tc(?MODULE, wait, [
            WaitDocsDurable, wait_docs_durable
        ]),

    Times = [Time1, Time2, Time3, Time4],
    Timeouts = [AnalyseAns1, AnalyseAns2, AnalyseAns3, AnalyseAns4],
    couchbase_batch:verify_batch_size_increase(SaveRequests5, Times, Timeouts),

    SaveResponses6 = couchbase_crud:terminate_save_requests(SaveRequests5),
    lists:merge([SaveResponses, SaveResponses2, SaveResponses3, SaveResponses4,
        SaveResponses5, SaveResponses6]).

%%--------------------------------------------------------------------
%% @doc
%% @equiv wait(WaitFun, 5, FunName).
%% @end
%%--------------------------------------------------------------------
-spec wait(WaitFun :: fun(() ->
    {couchbase_crud:save_requests_map(), [couchbase_crud:save_response()]}),
    atom()) -> {ok | timeout,
        {couchbase_crud:save_requests_map(), [couchbase_crud:save_response()]}}.
wait(WaitFun, FunName) ->
    wait(WaitFun, 2, FunName).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Waits for batch durability.
%% @end
%%--------------------------------------------------------------------
-spec wait(WaitFun :: fun(() ->
    {couchbase_crud:save_requests_map(), [couchbase_crud:save_response()]}),
    Num :: non_neg_integer(), atom()) -> {ok | timeout,
    {couchbase_crud:save_requests_map(), [couchbase_crud:save_response()]}}.
wait(WaitFun, Num, FunName) ->
    Stopwatch = stopwatch:start(),
    {_, SaveResponses} = Ans = WaitFun(),
    Time = stopwatch:read_micros(Stopwatch),
    case couchbase_batch:check_timeout(SaveResponses, FunName, Time) of
        timeout when Num > 1 ->
            wait(WaitFun, Num - 1, FunName);
        ok when Num =:= 2 ->
            {ok, Ans};
        _ ->
            {timeout, Ans}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles request. If request has already been handled in a batch, returns
%% available response.
%% @end
%%--------------------------------------------------------------------
-spec handle_request(cberl:connection(), couchbase_pool:request(),
    batch_responses()) -> couchbase_pool:response().
handle_request(_Connection, {save, _Ctx, Key, _Value}, ResponsesBatch) ->
    SaveResponses = maps:get(save, ResponsesBatch),
    get_response(Key, SaveResponses);
handle_request(_Connection, {get, Keys}, ResponsesBatch) when is_list(Keys) ->
    GetResponses = maps:get(get, ResponsesBatch),
    lists:map(fun(Key) -> get_response(Key, GetResponses) end, Keys);
handle_request(_Connection, {get, Key}, ResponsesBatch) ->
    GetResponses = maps:get(get, ResponsesBatch),
    get_response(Key, GetResponses);
handle_request(_Connection, {delete, _, Key}, ResponsesBatch) ->
    RemoveResponses = maps:get(delete, ResponsesBatch),
    get_response(Key, RemoveResponses);
handle_request(Connection, {get_counter, Key, Default}, _) ->
    Stopwatch = stopwatch:start(),
    Ans = couchbase_crud:get_counter(Connection, Key, Default),
    Time = stopwatch:read_micros(Stopwatch),
    couchbase_batch:check_timeout([Ans], get_counter, Time),
    Ans;
handle_request(Connection, {update_counter, Key, Delta, Default}, _) ->
    Stopwatch = stopwatch:start(),
    Ans = couchbase_crud:update_counter(Connection, Key, Delta, Default),
    Time = stopwatch:read_micros(Stopwatch),
    couchbase_batch:check_timeout([Ans], update_counter, Time),
    Ans;
handle_request(Connection, {save_design_doc, DesignName, EJson}, _) ->
    couchbase_view:save_design_doc(Connection, DesignName, EJson);
handle_request(Connection, {get_design_doc, DesignName}, _) ->
    couchbase_view:get_design_doc(Connection, DesignName);
handle_request(Connection, {delete_design_doc, DesignName}, _) ->
    couchbase_view:delete_design_doc(Connection, DesignName);
handle_request(Connection, {query_view, DesignName, ViewName, Opts}, _) ->
    couchbase_view:query(Connection, DesignName, ViewName, Opts).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Selects response from batch.
%% @end
%%--------------------------------------------------------------------
-spec get_response(couchbase_driver:key(), [{couchbase_driver:key(),
    couchbase_pool:response()}]) -> couchbase_pool:response().
get_response(Key, Responses) ->
    {Key, Response} = lists:keyfind(Key, 1, Responses),
    Response.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes and returns up to Count requests from a queue.
%% @end
%%--------------------------------------------------------------------
-spec dequeue(non_neg_integer(), queue:queue(request()), [request()]) ->
    {[request()], queue:queue(request())}.
dequeue(0, Queue, Requests) ->
    {lists:reverse(Requests), Queue};
dequeue(Count, Queue, Requests) ->
    case queue:out(Queue) of
        {{value, Request}, Queue2} ->
            dequeue(Count - 1, Queue2, [Request | Requests]);
        {empty, Queue2} ->
            {lists:reverse(Requests), Queue2}
    end.


%% @private
-spec reset_reconnect_retry_timer() -> ok.
reset_reconnect_retry_timer() ->
    TimeoutMillis = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_changes_restart_timeout, timer:minutes(1)),
    node_cache:put(db_reconnect_retry_timer, countdown_timer:start_millis(TimeoutMillis)).