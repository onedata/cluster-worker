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

-include("global_definitions.hrl").
-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    bucket :: couchbase_config:bucket(),
    scope :: datastore:scope(),
    seq :: couchbase_changes:since(),
    seq_safe :: couchbase_changes:until(),
    batch_size :: non_neg_integer(),
    interval :: non_neg_integer(),
    gc :: pid()
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase changes worker.
%% @end
%%--------------------------------------------------------------------
-spec start_link(couchbase_config:bucket(), datastore:scope()) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(Bucket, Scope) ->
    gen_server2:start_link(?MODULE, [Bucket, Scope], []).

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
init([Bucket, Scope]) ->
    {ok, GCPid} = couchbase_changes_worker_gc:start_link(Bucket, Scope),
    Ctx = #{bucket => Bucket},
    SeqSafeKey = couchbase_changes:get_seq_safe_key(Scope),
    {ok, _, SeqSafe} = couchbase_driver:get_counter(Ctx, SeqSafeKey),
    SeqKey = couchbase_changes:get_seq_key(Scope),
    {ok, _, Seq} = couchbase_driver:get_counter(Ctx, SeqKey, 0),
    erlang:send_after(0, self(), update),
    {ok, #state{
        bucket = Bucket,
        scope = Scope,
        seq = Seq,
        seq_safe = SeqSafe,
        batch_size = application:get_env(?CLUSTER_WORKER_APP_NAME,
            couchbase_changes_batch_size, 100),
        interval = application:get_env(?CLUSTER_WORKER_APP_NAME,
            couchbase_changes_update_interval, 1000),
        gc = GCPid
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
    Seq2 = min(SeqSafe2 + BatchSize - 1, Seq),

    Ctx = #{bucket => Bucket},
    Design = couchbase_changes:design(),
    View = couchbase_changes:view(),
    QueryAns = couchbase_driver:query_view(Ctx, Design, View, [
        {startkey, jiffy:encode([Scope, SeqSafe2])},
        {endkey, jiffy:encode([Scope, Seq2])},
        {inclusive_end, true}
    ]),

    Changes = case QueryAns of
        {ok, {Changes0}} ->
            Changes0;
        Error ->
            ?error("Cannot fetch changes, error: ~p", [Error]),
            []
    end,

    State2 = #state{
        seq_safe = SeqSafe3
    } = process_changes(SeqSafe2, Seq2 + 1, Changes, State),

    ets:insert(?CHANGES_COUNTERS, {Scope, SeqSafe3}),
    gen_server:cast(GCPid, {batch_ready, SeqSafe3}),

    case SeqSafe3 of
        Seq2 -> erlang:send_after(0, self(), update);
        _ -> erlang:send_after(Interval, self(), update)
    end,
    ?info("changes_worker: ~p", [{Changes, State2}]),

    State2.

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
    [couchbase_changes:change()], state()) -> state().
process_changes(Seq, Seq, [], State) ->
    State;
process_changes(SeqSafe, Seq, [], State) ->
    % TODO - remove this case?
    Timeout = 2 * couchbase_pool:get_timeout(),
    case ignore_change(SeqSafe, State, Timeout, 500) of
        true ->
            process_changes(SeqSafe + 1, Seq, [], State#state{
                seq_safe = SeqSafe
            });
        false ->
            State
    end;
process_changes(SeqSafe, Seq, [Change | _] = Changes, State) ->
    case lists:keyfind(<<"key">>, 1, Change) of
        {<<"key">>, [_, SeqSafe]} ->
            process_changes(SeqSafe + 1, Seq, tl(Changes), State#state{
                seq_safe = SeqSafe
            });
        {<<"key">>, [_, _]} ->
            Timeout = 2 * couchbase_pool:get_timeout(),
            case ignore_change(SeqSafe, State, Timeout, 500) of
                true ->
                    process_changes(SeqSafe + 1, Seq, Changes, State#state{
                        seq_safe = SeqSafe
                    });
                false ->
                    State
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check whether provided sequence number can be ignored.
%% Retries when status is undefined.
%% @end
%%--------------------------------------------------------------------
-spec ignore_change(couchbase_changes:seq(), state(), timeout(), timeout()) ->
    boolean().
ignore_change(Seq, State, Timeout, Delay) when Timeout =< Delay ->
    case ignore_change(Seq, State) of
        undefined -> true;
        Ignore -> Ignore
    end;
ignore_change(Seq, State, Timeout, Delay) ->
    case ignore_change(Seq, State) of
        undefined ->
            timer:sleep(Delay),
            ignore_change(Seq, State, Timeout - Delay, Delay);
        Ignore ->
            Ignore
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check whether provided sequence number can be ignored.
%% @end
%%--------------------------------------------------------------------
-spec ignore_change(couchbase_changes:seq(), state()) -> boolean() | undefined.
ignore_change(Seq, State = #state{bucket = Bucket, scope = Scope}) ->
    Ctx = #{bucket => Bucket},
    ChangeKey = couchbase_changes:get_change_key(Scope, Seq),
    case couchbase_driver:get(Ctx, ChangeKey) of
        {ok, _, {Props}} ->
            {<<"key">>, Key} = lists:keyfind(<<"key">>, 1, Props),
            {<<"pid">>, Term} = lists:keyfind(<<"pid">>, 1, Props),
            Pid = binary_to_term(base64:decode(Term)),
            case ignore_change(Seq, Key, State) of
                undefined ->
                    wait_for_worker(Pid),
                    case ignore_change(Seq, Key, State) of
                        undefined -> true;
                        Ignore -> Ignore
                    end;
                Ignore ->
                    Ignore
            end;
        {error, not_found} ->
            undefined;
        Error ->
            ?error("Error during ignore change procedure ~p", [Error]),
            false
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
-spec wait_for_worker(pid()) -> ok.
wait_for_worker(Pid) ->
    try
        pong = gen_server:call(Pid, ping, couchbase_pool:get_timeout()),
        ok
    catch
        _:{noproc, _} -> ok;
        exit:{normal, _} -> ok;
        _:{timeout, _} -> wait_for_worker(Pid)
    end.