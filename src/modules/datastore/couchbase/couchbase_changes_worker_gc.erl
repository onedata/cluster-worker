%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for removing internal documents that describe
%%% changes when they become obsolete. It also moves forward seq_safe counter
%%% as removing documents.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_changes_worker_gc).
-author("Michał Wrzeszcz").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    bucket :: couchbase_config:bucket(),
    scope :: datastore_doc:scope(),
    seq_safe_cas :: cberl:cas() | undefined,
    batch_begin = 0 :: couchbase_changes:until(),
    batch_end = 0 :: cberl:cas(),
    processing = false ::boolean()
}).

-type state() :: #state{}.

-define(ERROR_SLEEP_TIME, 2 * application:get_env(?CLUSTER_WORKER_APP_NAME,
    couchbase_operation_timeout, 60000)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase changes worker garbage collector.
%% @end
%%--------------------------------------------------------------------
-spec start_link(couchbase_config:bucket(), datastore_doc:scope()) ->
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
    {ok, #state{
        seq_safe_cas = Cas,
        batch_begin = SeqSafe + 1,
        bucket = Bucket,
        scope = Scope
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
handle_cast({batch_ready, End}, #state{} = State) ->
    {noreply, delete_internal_docs(State#state{batch_end = End})};
handle_cast({processing_finished, End, Cas2}, #state{} = State) ->
    {noreply, delete_internal_docs(State#state{
        batch_begin = End + 1,
        seq_safe_cas = Cas2,
        processing = false
    })};
handle_cast(processing_finished, #state{} = State) ->
    State2 = State#state{processing = false, seq_safe_cas = undefined},
    {noreply, delete_internal_docs(State2)};
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
%% Deletes not needed docs and move safe_key.
%% @end
%%--------------------------------------------------------------------
-spec delete_internal_docs(state()) -> state().
delete_internal_docs(State = #state{batch_begin = Begin, batch_end = End})
    when Begin > End ->
    State;
delete_internal_docs(State = #state{processing = true}) ->
    State;
delete_internal_docs(State = #state{
    batch_begin = Begin, batch_end = End,
    bucket = Bucket, scope = Scope, seq_safe_cas = Cas
}) ->
    Pid = self(),
    spawn(fun() ->
        try
            MaxBatch = application:get_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_changes_gc_batch_size, 500),
            End2 = min(End, Begin + MaxBatch),

            Ctx = #{bucket => Bucket, pool_mode => changes},
            SeqSafeKey = couchbase_changes:get_seq_safe_key(Scope),
            SaveCas = case Cas of
                undefined ->
                    {ok, CurrentCas, _} = couchbase_driver:get_counter(Ctx, SeqSafeKey),
                    CurrentCas;
                _ ->
                    Cas
            end,

            {ok, Cas2, End2} = couchbase_driver:save(
                Ctx#{cas => SaveCas}, SeqSafeKey, End2
            ),

            ChangeKeys = lists:map(fun(S) ->
                couchbase_changes:get_change_key(Scope, S)
            end, lists:seq(Begin, End2)),
            couchbase_driver:delete(Ctx, ChangeKeys),
            gen_server:cast(Pid, {processing_finished, End2, Cas2})
        catch
            _:Reason:Stacktrace ->
                ?error_stacktrace("Clearing changes intenal documents failed"
                " due to: ~p (pid: ~p, scope: ~p, bucket: ~p)",
                    [Reason, Pid, Scope, Bucket], Stacktrace),
                timer:sleep(?ERROR_SLEEP_TIME),
                gen_server:cast(Pid, processing_finished)
        end
    end),
    State#state{processing = true}.