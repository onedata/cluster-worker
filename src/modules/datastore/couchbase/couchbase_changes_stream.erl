%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for streaming CouchBase documents changes in
%%% range [Since, Until). It should be used by clients. There may be many
%%% couchbase_changes_stream processes, as they work in a readonly mode.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_changes_stream).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    bucket :: couchbase_config:bucket(),
    scope :: datastore:scope(),
    callback :: couchbase_changes:callback(),
    since :: couchbase_changes:since(),
    until :: couchbase_changes:until(),
    except_mutator :: datastore:mutator(),
    batch_size :: non_neg_integer(),
    interval :: non_neg_integer()
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase changes stream.
%% @end
%%--------------------------------------------------------------------
-spec start_link(couchbase_config:bucket(), datastore:scope(),
    couchbase_changes:callback(), proplists:proplist()) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(Bucket, Scope, Callback, Opts) ->
    gen_server:start_link(?MODULE, [Bucket, Scope, Callback, Opts], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes CouchBase changes stream.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([Bucket, Scope, Callback, Opts]) ->
    process_flag(trap_exit, true),
    erlang:send_after(0, self(), update),
    {ok, #state{
        bucket = Bucket,
        scope = Scope,
        callback = Callback,
        since = proplists:get_value(since, Opts, 1),
        until = proplists:get_value(until, Opts, infinity),
        except_mutator = proplists:get_value(except_mutator, Opts),
        batch_size = application:get_env(?CLUSTER_WORKER_APP_NAME,
            couchbase_changes_stream_batch_size, 200),
        interval = application:get_env(?CLUSTER_WORKER_APP_NAME,
            couchbase_changes_stream_update_interval, 1000)
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
handle_info(update, #state{since = Since, until = Until} = State) ->
    {Changes, State2} = get_changes(Since, Until, State),
    Docs = get_docs(Changes, State2),
    stream_docs(Docs, State2),
    case State2#state.since >= Until of
        true -> {stop, normal, State2};
        false -> {noreply, State2}
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
terminate(Reason, #state{since = Since, callback = Callback} = State) ->
    case Reason of
        normal -> Callback({ok, end_of_stream});
        _ -> Callback({error, Since, Reason})
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
%% Returns list of CouchBase changes and updated state.
%% @end
%%--------------------------------------------------------------------
-spec get_changes(couchbase_changes:since(), couchbase_changes:until(), state()) ->
    {[couchbase_changes:change()], state()}.
get_changes(Since, infinity, #state{batch_size = BatchSize} = State) ->
    get_changes(Since, Since + BatchSize, State);
get_changes(Since, Until, #state{} = State) ->
    #state{
        bucket = Bucket,
        scope = Scope,
        batch_size = BatchSize
    } = State,
    Ctx = #{bucket => Bucket},
    SeqSafe = get_seq_safe(Scope, Ctx),
    Until2 = min(Since + BatchSize, min(Until, SeqSafe + 1)),

    case Since >= Until2 of
        true ->
            {[], State};
        false ->
            QueryAns = couchbase_driver:query_view(Ctx,
                couchbase_changes:design(), couchbase_changes:view(), [
                    {startkey, jiffy:encode([Scope, Since])},
                    {endkey, jiffy:encode([Scope, Until2])},
                    {inclusive_end, false}
                ]
            ),

            case QueryAns of
                {ok, {Changes}} ->
                    {Changes, State#state{since = Until2}};
                Error ->
                    ?error("Cannot get changes, error: ~p", [Error]),
                    {[], State}
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns list of documents associated with the changes. Skips documents that
%% has changed in the database since the changes generation (they will be
%% included in the future changes).
%% @end
%%--------------------------------------------------------------------
-spec get_docs([couchbase_changes:change()], state()) -> [datastore:document()].
get_docs(Changes, #state{bucket = Bucket, except_mutator = Mutator}) ->
    KeyRevisionsAnsSequences = lists:filtermap(fun(Change) ->
        {<<"id">>, Key} = lists:keyfind(<<"id">>, 1, Change),
        {<<"value">>, {Value}} = lists:keyfind(<<"value">>, 1, Change),
        {<<"key">>,[_, Seq]} = lists:keyfind(<<"key">>, 1, Change),
        {<<"_rev">>, Rev} = lists:keyfind(<<"_rev">>, 1, Value),
        case lists:keyfind(<<"_mutator">>, 1, Value) of
            {<<"_mutator">>, Mutator} -> false;
            _ -> {true, {Key, {Rev, Seq}}}
        end
    end, Changes),
    Ctx = #{bucket => Bucket},
    {Keys, RevisionsAnsSequences} = lists:unzip(KeyRevisionsAnsSequences),
    lists:filtermap(fun
        ({{ok, _, #document{revs = [Rev | _], seq = Seq} = Doc}, {Rev, Seq}}) ->
            {true, Doc};
        ({{ok, _, #document{}}, _Rev}) ->
            false
    end, lists:zip(couchbase_driver:get(Ctx, Keys), RevisionsAnsSequences)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Streams documents and schedules next update.
%% @end
%%--------------------------------------------------------------------
-spec stream_docs([datastore:document()], state()) -> reference().
stream_docs([], #state{interval = Interval}) ->
    erlang:send_after(Interval, self(), update);
stream_docs(Docs, #state{callback = Callback}) ->
    Callback({ok, Docs}),
    erlang:send_after(0, self(), update).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets seq_safe from memory or from db (if it is not found in memory).
%% @end
%%--------------------------------------------------------------------
-spec get_seq_safe(datastore:scope(), datastore_context:ctx()) -> non_neg_integer().
get_seq_safe(Scope, Ctx) ->
    case ets:lookup(?CHANGES_COUNTERS, Scope) of
        [{_, SeqSafe}] ->
            SeqSafe;
        _ ->
            Key = couchbase_changes:get_seq_safe_key(Scope),
            {ok, _, SeqSafe} = couchbase_driver:get_counter(Ctx, Key),
            SeqSafe
    end.