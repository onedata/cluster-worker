%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for dumping datastore cache periodically.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_disc_writer).
-author("Krzysztof Trzepla").

-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    master_pid :: pid(),
    cache_writer_pid :: pid()
}).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type cached_keys() :: datastore_doc_batch:cached_keys().
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts and links datastore disc writer process to the caller.
%% @end
%%--------------------------------------------------------------------
-spec start_link(pid(), pid()) -> {ok, pid()} | {error, term()}.
start_link(MasterPid, CacheWriterPid) ->
    gen_server:start_link(?MODULE, [MasterPid, CacheWriterPid], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes datastore disc writer process.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([MasterPid, CacheWriterPid]) ->
    {ok, #state{master_pid = MasterPid, cache_writer_pid = CacheWriterPid}}.

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
handle_call({flush, Ref, CachedKeys}, From, State = #state{
    master_pid = MasterPid, cache_writer_pid = CacheWriterPid
}) ->
    Futures = flush_async(CachedKeys),
    gen_server:reply(From, ok),
    NotFlushedWithReason = wait_flushed(Futures),
    case NotFlushedWithReason of
        [] ->
            gen_server:cast(MasterPid, {mark_disc_writer_idle, Ref});
        _ ->
            ok
    end,
    {NotFlushed, _} = lists:unzip(NotFlushedWithReason),
    gen_server:cast(CacheWriterPid, {flushed, maps:from_list(NotFlushed)}),
    {noreply, State};
handle_call({terminate, CachedKeys}, _From, State = #state{}) ->
    Delay = application:get_env(cluster_worker, datastore_writer_flush_delay,
        timer:seconds(5)),
    force_flush(CachedKeys, Delay),
    {stop, normal, ok, State};
handle_call(Request, _From, State = #state{}) ->
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
%% Asynchronously flushes datastore documents from cache to disc.
%% @end
%%--------------------------------------------------------------------
-spec flush_async(cached_keys()) ->
    [{{key(), ctx()}, datastore_cache:future()}].
flush_async(CachedKeys) ->
    maps:fold(fun(Key, Ctx, RequestFutures) ->
        [{{Key, Ctx}, datastore_cache:flush_async(Ctx, Key)} | RequestFutures]
    end, [], CachedKeys).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Waits for completion of asynchronous flush of datastore documents from
%% cache to disc.
%% @end
%%--------------------------------------------------------------------
-spec wait_flushed([{{key(), ctx()}, datastore_cache:future()}]) ->
    [{{key(), ctx()}, {error, term()}}].
wait_flushed(RequestFutures) ->
    {Requests, Futures} = lists:unzip(RequestFutures),
    Responses = datastore_cache:wait(Futures),
    lists:filtermap(fun
        ({_, {ok, disc, _}}) -> false;
        ({_, {error, not_found}}) -> false;
        ({_, {error, memory_driver_undefined}}) -> false;
        ({_, {error, disc_driver_undefined}}) -> false;
        ({{Key, Ctx}, Error = {error, _}}) -> {true, {{Key, Ctx}, Error}}
    end, lists:zip(Requests, Responses)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Synchronously flushes datastore documents from cache to disc.
%% @end
%%--------------------------------------------------------------------
-spec flush(cached_keys()) -> [{{key(), ctx()}, {error, term()}}].
flush(CachedKeys) ->
    wait_flushed(flush_async(CachedKeys)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Synchronously flushes datastore documents from cache to disc.
%% Repeats flush for documents that haven't been successfully flushed.
%% @end
%%--------------------------------------------------------------------
-spec force_flush(cached_keys(), non_neg_integer()) -> ok.
force_flush(CachedKeys, Delay) ->
    case flush(CachedKeys) of
        [] ->
            ok;
        NotFlushedWithReason ->
            CachedKeys2 = lists:foldl(fun({{Key, Ctx}, {error, Reason}}, Map) ->
                LogKey = case Reason of
                    etimedout -> flush_etimedout;
                    timeout -> flush_timeout;
                    etmpfail -> flush_etmpfail;
                    _ -> flush_other_error
                end,
                node_manager:single_error_log(LogKey, "Failed to flush document
                    ~p using context ~p due to: ~p. Retrying after ~p ms...",
                    [Key, Ctx, Reason, Delay]),
                maps:put(Key, Ctx, Map)
            end, #{}, NotFlushedWithReason),
            timer:sleep(Delay),
            force_flush(CachedKeys2, min(2 * Delay, timer:minutes(5)))
    end.