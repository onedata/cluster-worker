%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for moving forward safe sequence number.
%%% Safe sequence number defines upper bound for changes that are guaranteed
%%% to be present in the changes view and may be streamed to the clients.
%%% There should be only one couchbase_changes_worker process per bucket
%%% and scope, as it mutates safe sequence number associated with this pair.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_changes_worker).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_changes.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    bucket :: couchbase_config:bucket(),
    scope :: datastore_doc:scope(),
    seq :: couchbase_changes:since(),
    seq_safe :: couchbase_changes:until(),
    batch_size :: non_neg_integer(),
    interval :: non_neg_integer(),
    gc :: pid(),

    % Optional callback that allows changes handling without usage of changes stream
    % (when only one changes stream with all documents is needed)
    callback :: couchbase_changes:callback() | undefined
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% API function used to start couchbase_changes_worker. When Callback and PropagationSince
%% are undefined, couchbase_changes_worker updates only seq_safe number and
%% couchbase_changes_stream is responsible for Callback execution.
%% @end
%%--------------------------------------------------------------------
-spec start_link(couchbase_config:bucket(), datastore_doc:scope(),
    couchbase_changes:callback() | undefined, couchbase_changes:since() | undefined) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(Bucket, Scope, Callback, PropagationSince) ->
    gen_server2:start_link(?MODULE, [Bucket, Scope, Callback, PropagationSince], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes CouchBase changes worker.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([Bucket, Scope, Callback, PropagationSince]) ->
    {ok, GCPid} = couchbase_changes_worker_gc:start_link(Bucket, Scope),
    Ctx = #{bucket => Bucket},
    SeqSafeKey = couchbase_changes:get_seq_safe_key(Scope),
    {ok, _, SeqSafe} = couchbase_driver:get_counter(Ctx, SeqSafeKey),
    SeqKey = couchbase_changes:get_seq_key(Scope),
    {ok, _, Seq} = couchbase_driver:get_counter(Ctx, SeqKey, 0),
    Interval = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_changes_update_interval, 1000),

    case PropagationSince of
        undefined -> erlang:send_after(Interval, self(), update);
        Num when Num >= SeqSafe -> erlang:send_after(Interval, self(), update);
        _ -> erlang:send_after(Interval, self(), {propagate_changes, PropagationSince})
    end,

    Seq3 = case SeqSafe > Seq of
        true ->
            Seq2 = max(Seq, SeqSafe),
            {ok, _, _} = couchbase_driver:update_counter(Ctx, SeqKey, Seq2 - Seq, Seq2),

            ?warning("Wrong seq and seq_safe for scope ~p: seq_safe = ~p, "
                "seq = ~p, new_seq = ~p", [Scope, SeqSafe, Seq, Seq2]),
            Seq2;
        _ ->
            Seq
    end,

    {ok, #state{
        bucket = Bucket,
        scope = Scope,
        seq = Seq3,
        seq_safe = SeqSafe,
        batch_size = application:get_env(?CLUSTER_WORKER_APP_NAME,
            couchbase_changes_batch_size, 200),
        interval = Interval,
        gc = GCPid,
        callback = Callback
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
handle_info(update, #state{
    seq_safe = Seq,
    seq = Seq,
    bucket = Bucket,
    scope = Scope
} = State) ->
    Ctx = #{bucket => Bucket},
    SeqKey = couchbase_changes:get_seq_key(Scope),
    Seq3 = case couchbase_driver:get_counter(Ctx, SeqKey) of
        {ok, _, Seq2} -> Seq2;
        {error, _Reason} -> Seq
    end,
    {noreply, fetch_changes(State#state{seq = Seq3})};
handle_info({propagate_changes, Since}, #state{} = State) ->
    propagate_changes(Since, State),
    {noreply, State};
handle_info(update, #state{} = State) ->
    {noreply, fetch_changes(State)};
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
terminate(Reason, #state{callback = undefined} = State) ->
    ?log_terminate(Reason, State);
terminate(Reason, #state{callback = Callback, seq_safe = SeqSafe} = State) ->
    case Reason of
        normal ->
            ok;
        _ ->
            try
                Callback({error, SeqSafe, Reason})
            catch
                CatchError:CatchReason ->
                    ?error_stacktrace("~p terminate callback failed due to ~p:~p~n"
                        "Termination on seq ~p", [?MODULE, CatchError, CatchReason, SeqSafe])
            end
    end,
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
%% Retrieves and processes changes from the CouchBase database.
%% Sets safe sequence number to the last acknowledge sequence number.
%% @end
%%--------------------------------------------------------------------
-spec fetch_changes(state()) -> state().
fetch_changes(#state{seq_safe = Seq, seq = Seq, interval = Interval} = State) ->
    erlang:send_after(Interval, self(), update),
    State;
fetch_changes(#state{
    bucket = Bucket,
    scope = Scope,
    seq_safe = SeqSafe,
    seq = Seq,
    batch_size = BatchSize,
    interval = Interval,
    gc = GCPid
} = State) ->
    SeqSafe2 = SeqSafe + 1,

    Ctx = #{bucket => Bucket},
    Design = couchbase_changes:design(),
    View = couchbase_changes:view(),
    QueryAns = couchbase_driver:query_view(Ctx, Design, View, [
        {startkey, [Scope, SeqSafe2]},
        {endkey, [Scope, Seq]},
        {limit, BatchSize},
        {inclusive_end, true}
    ]),

    case QueryAns of
        {ok, #{<<"rows">> := Changes}} ->
            UpperSeqNum = couchbase_changes_utils:get_upper_seq_num(Changes, BatchSize, Seq),
            State2 = #state{
                seq_safe = SeqSafe3
            } = process_changes(SeqSafe2, UpperSeqNum + 1, Changes, State, []),

            ets:insert(?CHANGES_COUNTERS, {Scope, SeqSafe3}),
            gen_server:cast(GCPid, {batch_ready, SeqSafe3}),
            stream_docs(Changes, Bucket, SeqSafe3, State),

            case SeqSafe3 of
                UpperSeqNum -> erlang:send_after(0, self(), update);
                _ -> erlang:send_after(Interval, self(), update)
            end,
            State2;
        Error ->
            ?warning("Cannot fetch changes, error: ~p, scope: ~p, start: ~p, stop: ~p", [
                Error, Scope, SeqSafe2, Seq
            ]),

            erlang:send_after(Interval, self(), update),
            State
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes consecutive sequence numbers in range [SeqSafe, Seq).
%% For each sequence number checks whether it appears in changes.
%% If sequence number is not found in changes checks whether it can be ignored.
%% If sequence number is not found in changes and can not be ignored processing
%% is stopped.
%% @end
%%--------------------------------------------------------------------
-spec process_changes(couchbase_changes:seq(), couchbase_changes:seq(),
    [couchbase_changes:change()], state(), [pid()]) -> state().
process_changes(Seq, Seq, [], State, _WorkersChecked) ->
    State;
process_changes(SeqSafe, Seq, [], State, WorkersChecked) ->
    case ignore_change(SeqSafe, State, WorkersChecked, true) of
        {true, WorkersChecked2} ->
            process_changes(SeqSafe + 1, Seq, [], State#state{
                seq_safe = SeqSafe
            }, WorkersChecked2);
        _ ->
            State
    end;
process_changes(SeqSafe, Seq, [Change | _] = Changes, State, WorkersChecked) ->
    case maps:get(<<"key">>, Change) of
        [_, SeqSafe] ->
            process_changes(SeqSafe + 1, Seq, tl(Changes), State#state{
                seq_safe = SeqSafe
            }, WorkersChecked);
        [_, _] ->
            case ignore_change(SeqSafe, State, WorkersChecked, true) of
                {true, WorkersChecked2} ->
                    process_changes(SeqSafe + 1, Seq, Changes, State#state{
                        seq_safe = SeqSafe
                    }, WorkersChecked2);
                _ ->
                    State
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check whether provided sequence number can be ignored.
%% @end
%%--------------------------------------------------------------------
-spec ignore_change(couchbase_changes:seq(), state(), [pid()], boolean()) ->
    {boolean(), [pid()]}.
ignore_change(Seq, State = #state{bucket = Bucket, scope = Scope},
    WorkersChecked, Retry) ->
    Ctx = #{bucket => Bucket},
    ChangeKey = couchbase_changes:get_change_key(Scope, Seq),
    case couchbase_driver:get(Ctx, ChangeKey) of
        {ok, _, {Props}} ->
            {<<"key">>, Key} = lists:keyfind(<<"key">>, 1, Props),
            case ignore_change(Seq, Key, State) of
                undefined ->
                    {<<"pid">>, Term} = lists:keyfind(<<"pid">>, 1, Props),
                    Pid = binary_to_term(base64:decode(Term)),
                    WorkersChecked2 = wait_for_worker(Pid, WorkersChecked),
                    case ignore_change(Seq, Key, State) of
                        undefined -> {true, WorkersChecked2};
                        Ignore -> {Ignore, WorkersChecked2}
                    end;
                Ignore ->
                    {Ignore, WorkersChecked}
            end;
        {error, not_found} ->
            case Retry of
                true ->
                    WorkersChecked2 = lists:foldl(fun(Pid, Acc2) ->
                        wait_for_worker(Pid, Acc2)
                    end, [], couchbase_pool:get_workers(write)),
                    ignore_change(Seq, State, WorkersChecked2, false);
                _ ->
                    case check_reconect_retry() of
                        true ->
                            timer:sleep(1000),
                            ignore_change(Seq, State, WorkersChecked, false);
                        _ ->
                            {true, WorkersChecked}
                    end
            end;
        Error ->
            ?error("Error during ignore change procedure ~p", [Error]),
            {false, WorkersChecked}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check whether provided sequence number can be ignored, that is smaller than
%% a sequence number of a document fetched from a database.
%% @end
%%--------------------------------------------------------------------
-spec ignore_change(couchbase_changes:seq(), couchbase_driver:key(), state()) ->
    boolean() | undefined.
ignore_change(Seq, Key, #state{bucket = Bucket}) ->
    Ctx = #{bucket => Bucket},
    case couchbase_driver:get(Ctx, Key) of
        {ok, _, #document{seq = Seq}} -> false;
        {ok, _, #document{seq = Seq2}} when Seq2 > Seq -> true;
        {ok, _, #document{seq = Seq2}} when Seq2 < Seq -> undefined;
        {error, not_found} -> undefined
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Waits until CouchBase pool worker complete current action.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_worker(pid(), [pid()]) -> [pid()].
wait_for_worker(Pid, WorkersChecked) ->
    case lists:member(Pid, WorkersChecked) of
        true ->
            WorkersChecked;
        _ ->
            try
                pong = gen_server:call(Pid, ping, couchbase_pool:get_timeout()),
                [Pid | WorkersChecked]
            catch
                _:{noproc, _} -> [Pid | WorkersChecked];
                exit:{normal, _} -> [Pid | WorkersChecked];
                _:{timeout, _} -> wait_for_worker(Pid, WorkersChecked)
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if ignore check should be retry because of reconnects to db.
%% @end
%%--------------------------------------------------------------------
-spec check_reconect_retry() -> boolean().
check_reconect_retry() ->
    Timeout = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_changes_restart_timeout, timer:minutes(1)),
    TimeoutUs = Timeout * 1000,
    StartTime = node_cache:get(db_connection_timestamp, {0, 0, 0}),

    Now = os:timestamp(), % @TODO VFS-6841 switch to the clock module
    timer:now_diff(Now, StartTime) < TimeoutUs.

%% @private
-spec stream_docs([couchbase_changes:change()], couchbase_config:bucket(),
    couchbase_changes:seq(), state()) -> ok.
stream_docs(_Changes, _Bucket, _SeqSafe, #state{callback = undefined}) ->
    ok;
stream_docs(Changes, Bucket, SeqSafe, #state{callback = Callback}) ->
    case couchbase_changes_utils:get_docs(Changes, Bucket, <<>>, SeqSafe) of
        [] -> ok;
        Docs ->
            Callback({ok, Docs}),
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function used during worker init to propagate changes up to SeqSafe (if requested)
%% @end
%%--------------------------------------------------------------------
-spec propagate_changes(couchbase_changes:since(), state()) -> ok.
propagate_changes(Since, #state{seq_safe = SeqSafe, interval = Interval,
    bucket = Bucket, scope = Scope} = State) ->
    BatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_changes_stream_batch_size, 1000),

    QueryAns = couchbase_driver:query_view(#{bucket => Bucket},
        couchbase_changes:design(), couchbase_changes:view(), [
            {startkey, [Scope, Since]},
            {endkey, [Scope, SeqSafe]},
            {limit, BatchSize},
            {inclusive_end, true},
            {stale, ?CHANGES_STALE_OPTION} % it is recommended to use stale=false option as
                                           % couchbase_changes_stream does not analyse missing documents
                                           % (couchbase_changes_worker does), without it document can be
                                           % lost when view is being rebuilt by couch after an error;
                                           % use stale=true only when you are fully aware of view status
        ]
    ),

    NewSince = case QueryAns of
        {ok, #{<<"rows">> := Changes}} ->
            UpperSeqNum = couchbase_changes_utils:get_upper_seq_num(Changes, BatchSize, SeqSafe),
            stream_docs(Changes, Bucket, UpperSeqNum, State),
            UpperSeqNum + 1;
        Error ->
            ?error("Cannot get changes, error: ~p", [Error]),
            Since
    end,

    SeqSafeToStartUpdate = SeqSafe + 1,
    case NewSince of
        SeqSafeToStartUpdate -> erlang:send_after(Interval, self(), update);
        Since -> erlang:send_after(Interval, self(), {propagate_changes, NewSince});
        _ -> erlang:send_after(0, self(), {propagate_changes, NewSince})
    end,
    ok.