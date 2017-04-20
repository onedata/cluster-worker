%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for streaming CouchBase documents changes in
%%% range [Since, Until] (inclusive except for infinity). It should be used by
%%% clients. There may be many couchbase_changes_streamer processes, as they
%%% work in a readonly mode.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_changes_streamer).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    bucket :: couchbase_driver:bucket(),
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
-spec start_link(couchbase_driver:bucket(), datastore:scope(),
    couchbase_changes:callback(), proplists:proplist()) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(Bucket, Scope, Callback, Opts) ->
    gen_server2:start_link(?MODULE, [Bucket, Scope, Callback, Opts], []).

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
    erlang:send_after(0, self(), update),
    {ok, #state{
        bucket = Bucket,
        scope = Scope,
        callback = Callback,
        since = proplists:get_value(since, Opts, 1),
        until = proplists:get_value(until, Opts, infinity),
        except_mutator = proplists:get_value(except_mutator, Opts),
        batch_size = application:get_env(?CLUSTER_WORKER_APP_NAME,
            couchbase_changes_stream_batch_size, 25),
        interval = application:get_env(?CLUSTER_WORKER_APP_NAME,
            couchbase_changes_stream_update_interval, 5000)
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
    {Since2, Changes} = get_changes(Since, Until, State),
    Docs = get_docs(Changes, State),
    stream_docs(Docs, State),
    case Since2 > Until of
        true -> {stop, normal, State};
        false -> {noreply, State#state{since = Since2}}
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
%% Returns updated since value and a list of CouchBase changes.
%% @end
%%--------------------------------------------------------------------
-spec get_changes(couchbase_changes:since(), couchbase_changes:until(), state()) ->
    {couchbase_changes:since(), [couchbase_changes:change()]}.
get_changes(Since, infinity, #state{batch_size = BatchSize} = State) ->
    get_changes(Since, Since + BatchSize, State);
get_changes(Since, Until, #state{} = State) ->
    #state{
        bucket = Bucket,
        scope = Scope,
        batch_size = BatchSize
    } = State,
    Ctx = #{bucket => Bucket},
    Key = couchbase_changes:get_seq_safe_key(Scope),
    {ok, Until2} = couchbase_driver:get_counter(Ctx, Key, 0),
    Until3 = min(Since + BatchSize, min(Until, Until2)),

    case Since > Until3 of
        true ->
            {Since, []};
        false ->
            {ok, {Changes}} = couchbase_driver:query_view(Ctx,
                couchbase_changes:design(), couchbase_changes:view(), [
                    {startkey, jiffy:encode([Scope, Since])},
                    {endkey, jiffy:encode([Scope, Until3])},
                    {inclusive_end, true}
                ]
            ),
            {Until3 + 1, Changes}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns list of documents associated with the changes. Skips documents that
%% has changed in the database since the changes generation (they will be
%% included in the future changes).
%% @end
%%--------------------------------------------------------------------
-spec get_docs([couchbase_changes:change()], state()) -> [datastore:doc()].
get_docs(Changes, #state{bucket = Bucket, except_mutator = Mutator}) ->
    KeyRevs = lists:filtermap(fun(Change) ->
        {<<"id">>, Key} = lists:keyfind(<<"id">>, 1, Change),
        {<<"value">>, {Value}} = lists:keyfind(<<"value">>, 1, Change),
        {<<"_rev">>, Rev} = lists:keyfind(<<"_rev">>, 1, Value),
        case lists:keyfind(<<"_mutator">>, 1, Value) of
            {<<"_mutator">>, Mutator} -> false;
            _ -> {true, {Key, Rev}}
        end
    end, Changes),
    Ctx = #{bucket => Bucket},
    {Keys, Revs} = lists:unzip(KeyRevs),
    lists:filtermap(fun
        ({{ok, #document2{rev = [Rev1 | _]} = Doc}, Rev2}) when Rev1 =:= Rev2 ->
            {true, Doc};
        ({{ok, #document2{}}, _Rev}) ->
            false
    end, lists:zip(couchbase_driver:mget(Ctx, Keys), Revs)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Streams documents and schedules next update.
%% @end
%%--------------------------------------------------------------------
-spec stream_docs([datastore:doc()], state()) -> reference().
stream_docs([], #state{interval = Interval}) ->
    erlang:send_after(Interval, self(), update);
stream_docs(Docs, #state{callback = Callback}) ->
    lists:foreach(fun(Doc) ->
        Callback(Doc)
    end, Docs),
    erlang:send_after(0, self(), update).