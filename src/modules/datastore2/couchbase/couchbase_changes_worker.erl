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
-include("modules/datastore/datastore_models_def.hrl").
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
    seq_safe_cas :: cberl:cas(),
    batch_size :: non_neg_integer(),
    interval :: non_neg_integer()
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
    Ctx = #{bucket => Bucket},
    SeqSafeKey = couchbase_changes:get_seq_safe_key(Scope),
    {ok, Cas, SeqSafe} = couchbase_driver:get_counter(Ctx, SeqSafeKey, 0),
    SeqKey = couchbase_changes:get_seq_key(Scope),
    {ok, _, Seq} = couchbase_driver:get_counter(Ctx, SeqKey, 0),
    erlang:send_after(0, self(), update),
    {ok, #state{
        bucket = Bucket,
        scope = Scope,
        seq = Seq,
        seq_safe = SeqSafe,
        seq_safe_cas = Cas,
        batch_size = application:get_env(?CLUSTER_WORKER_APP_NAME,
            couchbase_changes_batch_size, 25),
        interval = application:get_env(?CLUSTER_WORKER_APP_NAME,
            couchbase_changes_update_interval, 5000)
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
    {noreply, fetch_changes(Seq, Seq3, State)};
handle_info(update, #state{seq_safe = SeqSafe, seq = Seq} = State) ->
    {noreply, fetch_changes(SeqSafe, Seq, State)};
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
-spec fetch_changes(couchbase_changes:seq(), couchbase_changes:seq(),
    state()) -> state().
fetch_changes(Seq, Seq, #state{interval = Interval} = State) ->
    erlang:send_after(Interval, self(), update),
    State;
fetch_changes(SeqSafe, Seq, #state{
    bucket = Bucket,
    scope = Scope,
    seq_safe_cas = Cas,
    batch_size = BatchSize,
    interval = Interval
} = State) ->
    SeqSafe2 = SeqSafe + 1,
    Seq2 = min(SeqSafe2 + BatchSize - 1, Seq),

    Ctx = #{bucket => Bucket},
    Design = couchbase_changes:design(),
    View = couchbase_changes:view(),
    {ok, {Changes}} = couchbase_driver:query_view(Ctx, Design, View, [
        {startkey, jiffy:encode([Scope, SeqSafe2])},
        {endkey, jiffy:encode([Scope, Seq2])},
        {inclusive_end, true}
    ]),

    SeqSafe3 = process_changes(SeqSafe2, Seq2 + 1, Changes, State),
    SeqSafeKey = couchbase_changes:get_seq_safe_key(Scope),
    {ok, Cas2, SeqSafe3} = couchbase_driver:save(
        Ctx#{cas => Cas}, {SeqSafeKey, SeqSafe3}
    ),

    ChangeKeys = lists:map(fun(S) ->
        couchbase_changes:get_change_key(Scope, S)
    end, lists:seq(SeqSafe2, SeqSafe3)),
    couchbase_driver:delete(Ctx, ChangeKeys),

    case SeqSafe3 of
        Seq2 -> erlang:send_after(0, self(), update);
        _ -> erlang:send_after(Interval, self(), update)
    end,
    State#state{seq_safe = SeqSafe3, seq_safe_cas = Cas2, seq = Seq}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes consecutive sequence numbers in range [SeqSafe, Seq).
%% For each sequence number checks whether it appears in changes.
%% If sequence number is not found in changes checks whether it can be ignored.
%% If sequence number is not found in changes and can not be ignored processing
%% is stopped and previous sequence number is returned.
%% @end
%%--------------------------------------------------------------------
-spec process_changes(couchbase_changes:seq(), couchbase_changes:seq(),
    [couchbase_changes:change()], state()) -> couchbase_changes:seq().
process_changes(Seq, Seq, [], _State) ->
    Seq - 1;
process_changes(SeqSafe, Seq, [], State) ->
    case ignore_change(SeqSafe, State, 10) of
        true -> process_changes(SeqSafe + 1, Seq, [], State);
        false -> SeqSafe - 1
    end;
process_changes(SeqSafe, Seq, [Change | _] = Changes, State) ->
    case lists:keyfind(<<"key">>, 1, Change) of
        {<<"key">>, [_, SeqSafe]} ->
            process_changes(SeqSafe + 1, Seq, tl(Changes), State);
        {<<"key">>, [_, _]} ->
            case ignore_change(SeqSafe, State, 10) of
                true -> process_changes(SeqSafe + 1, Seq, Changes, State);
                false -> SeqSafe - 1
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check whether provided sequence number can be ignored.
%% Retries when status is undefined.
%% @end
%%--------------------------------------------------------------------
-spec ignore_change(couchbase_changes:seq(), state(), non_neg_integer()) ->
    boolean().
ignore_change(Seq, State, AttemptsLeft) ->
    case {ignore_change(Seq, State), AttemptsLeft} of
        {undefined, 1} ->
            true;
        {undefined, _} ->
            timer:sleep(timer:seconds(1)),
            ignore_change(Seq, State, AttemptsLeft - 1);
        {Ignore, _} ->
            Ignore
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check whether provided sequence number can be ignored.
%% @end
%%--------------------------------------------------------------------
-spec ignore_change(couchbase_changes:seq(), state()) -> boolean() | undefined.
ignore_change(Seq, #state{bucket = Bucket, scope = Scope}) ->
    Ctx = #{bucket => Bucket},
    ChangeKey = couchbase_changes:get_change_key(Scope, Seq),
    case couchbase_driver:get(Ctx, ChangeKey) of
        {ok, _, Key} ->
            case couchbase_driver:get(Ctx, Key) of
                {ok, _, #document2{seq = Seq}} -> false;
                {ok, _, #document2{}} -> true;
                {error, key_enoent} -> undefined
            end;
        {error, key_enoent} ->
            undefined
    end.
