%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides transactional functionality for a datastore document
%%% by creating a dedicated process associated with a document. It handles
%%% modification requests and ensures that all changes will be committed.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_doc).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_doc.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([run_sync/3, run_sync/4, run_async/3, receive_response/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type init() :: #datastore_doc_init{}.
-type data() :: any().
-type changes() :: any().
-type rev() :: any().
-type key() :: tp:key().
-type args() :: tp:args().
-type request() :: tp:request().
-type response() :: tp:response().

-export_type([init/0, data/0, changes/0, rev/0]).

-record(state, {
    data :: data(),
    rev :: rev(),
    changes = undefined :: undefined | changes(),
    requests = [] :: [{pid(), reference(), request()}],
    % a pid of the modify handler process
    modify_handler_pid :: undefined | pid(),
    % a pid of the commit handler process
    commit_handler_pid :: undefined | pid(),
    % a reference to a timer expected to trigger terminate
    terminate_timer_ref :: undefined | reference(),
    % a reference to a message expected to trigger commit
    commit_msg_ref :: undefined | reference(),
    % a reference to a message expected to trigger terminate
    terminate_msg_ref :: undefined | reference(),
    commit_delay :: timeout(),
    idle_timeout :: timeout()
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv run_sync(Args, Key, Request, timer:seconds(5))
%% @end
%%--------------------------------------------------------------------
-spec run_sync(args(), key(), request()) ->
    response() | {error, Reason :: term()} | no_return().
run_sync(Args, Key, Request) ->
    run_sync(Args, Key, Request, timer:minutes(10)).

%%--------------------------------------------------------------------
%% @doc
%% Delegates request processing to a datastore document process and waits for
%% the response.
%% @end
%%--------------------------------------------------------------------
-spec run_sync(args(), key(), request(), timeout()) ->
    response() | {error, Reason :: term()} | no_return().
run_sync(Args, Key, Request, Timeout) ->
    case run_async(Args, Key, Request) of
        {ok, Ref} -> receive_response(Ref, Timeout);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Delegates request processing to a datastore document process and returns
%% a reference which allows to receive response using {@link receive_response/2}
%% function.
%% @end
%%--------------------------------------------------------------------
-spec run_async(args(), key(), request()) ->
    {ok, reference()} | {error, Reason :: term()}.
run_async(Args, Key, Request) ->
    tp:call(?MODULE, Args, Key, Request, timer:seconds(5), 3).

%%--------------------------------------------------------------------
%% @doc
%% Returns a response form a datastore document process or fails with a timeout.
%% @end
%%--------------------------------------------------------------------
-spec receive_response(reference(), timeout()) ->
    response() | {error, timeout} | no_return().
receive_response(Ref, Timeout) ->
    receive
        {Ref, {throw, Exception}} -> throw(Exception);
        {Ref, Response} -> Response
    after
        Timeout -> {error, timeout}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes process associated with a datastore document.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init(Args) ->
    case exec(init, [Args]) of
        {ok, #datastore_doc_init{} = Init} ->
            {ok, schedule_terminate(#state{
                data = Init#datastore_doc_init.data,
                rev = Init#datastore_doc_init.rev,
                commit_delay = Init#datastore_doc_init.max_commit_delay,
                idle_timeout = Init#datastore_doc_init.idle_timeout
            })};
        {error, Reason} ->
            {stop, Reason}
    end.

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
handle_call(Request, {Pid, _Tag}, #state{requests = Requests} = State) ->
    Ref = make_ref(),
    State2 = State#state{
        requests = [{Pid, Ref, Request} | Requests]
    },
    {reply, {ok, Ref}, modify_async(State2)}.

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
handle_info({'EXIT', Pid, Exit}, #state{
    modify_handler_pid = Pid
} = State) ->
    {noreply, handle_modified(Exit, State)};
handle_info({'EXIT', Pid, Exit}, #state{
    commit_handler_pid = Pid
} = State) ->
    {noreply, handle_committed(Exit, State)};
handle_info({Ref, {commit, Delay}}, #state{commit_msg_ref = Ref} = State) ->
    {noreply, commit_async(Delay, State#state{commit_msg_ref = undefined})};
handle_info({Ref, terminate}, #state{
    requests = [],
    changes = undefined,
    modify_handler_pid = undefined,
    commit_handler_pid = undefined,
    terminate_msg_ref = Ref
} = State) ->
    {stop, normal, State};
handle_info({_Ref, terminate}, #state{} = State) ->
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
terminate(Reason, #state{rev = Rev} = State) when
    Reason == normal;
    Reason == shutdown ->
    State2 = modify_sync(State),
    #state{data = Data} = commit_sync(State2),
    Delay = application:get_env(?CLUSTER_WORKER_APP_NAME,
        datastore_doc_commit_retry_delay, timer:seconds(1)),
    exec_retry(terminate, [Data, Rev], Delay);
terminate({shutdown, _}, State) ->
    terminate(shutdown, State);
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
%% Synchronously modifies datastore document data.
%% @end
%%--------------------------------------------------------------------
-spec modify_sync(state()) -> state().
modify_sync(#state{requests = [], modify_handler_pid = undefined} = State) ->
    State;
modify_sync(#state{} = State) ->
    modify_sync(wait_modify(State)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asynchronously modifies datastore document data by spawning a handling
%% process. The process is spawned only if there are pending requests and
%% there is no other handler already spawned.
%% @end
%%--------------------------------------------------------------------
-spec modify_async(state()) -> state().
modify_async(#state{requests = []} = State) ->
    State;
modify_async(#state{
    data = Data,
    rev = Rev,
    requests = Requests,
    modify_handler_pid = undefined
} = State) ->
    Pid = spawn_link(fun() ->
        {Pids, Refs, DocRequests} = lists:unzip3(lists:reverse(Requests)),
        case exec_noexcept(modify, [DocRequests, Data, Rev]) of
            {ok, {DocResponses, Changes, Data2}} ->
                notify(Pids, Refs, DocResponses),
                return({modified, Changes, Data2});
            {error, Reason, Stacktrace} ->
                notify(Pids, Refs, {error, Reason, Stacktrace}),
                return({Reason, Stacktrace})
        end
    end),
    State#state{
        requests = [],
        modify_handler_pid = Pid
    };
modify_async(#state{} = State) ->
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Waits for the modify handler completion.
%% @end
%%--------------------------------------------------------------------
-spec wait_modify(state()) -> state().
wait_modify(#state{modify_handler_pid = undefined} = State) ->
    State;
wait_modify(#state{modify_handler_pid = Pid} = State) ->
    receive
        {'EXIT', Pid, Exit} -> handle_modified(Exit, State)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles the outcome of the modify handler.
%% @end
%%--------------------------------------------------------------------
-spec handle_modified(Exit :: any(), state()) -> state().
handle_modified({modified, false, Data}, #state{} = State) ->
    schedule_terminate(handle_modified(
        {modified, {true, undefined}, Data}, State
    ));
handle_modified({modified, {true, NextChanges}, Data}, #state{
    changes = Changes,
    commit_delay = Delay
} = State) ->
    schedule_commit(Delay, modify_async(State#state{
        data = Data,
        changes = merge_changes(Changes, NextChanges),
        modify_handler_pid = undefined
    }));
handle_modified(Exit, #state{commit_delay = Delay} = State) ->
    ?error("Modify handler of a datastore document terminated abnormally: ~p",
        [Exit]),
    schedule_terminate(schedule_commit(Delay, modify_async(State#state{
        modify_handler_pid = undefined
    }))).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Synchronously commits datastore document changes.
%% @end
%%--------------------------------------------------------------------
-spec commit_sync(state()) -> state().
commit_sync(#state{
    changes = undefined,
    commit_handler_pid = undefined
} = State) ->
    State;
commit_sync(#state{commit_delay = Delay} = State) ->
    commit_sync(commit_async(Delay, wait_commit(State))).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asynchronously commits datastore document changes by spawning a handling
%% process. The process is spawned only if there are uncommitted changes and
%% there is no other handler already spawned.
%% @end
%%--------------------------------------------------------------------
-spec commit_async(timeout(), state()) -> state().
commit_async(_Delay, #state{changes = undefined} = State) ->
    State;
commit_async(Delay, #state{
    data = Data,
    changes = Changes,
    commit_handler_pid = undefined
} = State) ->
    Pid = spawn_link(fun() ->
        Response = exec(commit, [Changes, Data]),
        return({committed, Response, Delay})
    end),
    State#state{
        changes = undefined,
        commit_handler_pid = Pid
    };
commit_async(_Delay, #state{} = State) ->
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Waits for the commit handler completion or a trigger commit message.
%% @end
%%--------------------------------------------------------------------
-spec wait_commit(state()) -> state().
wait_commit(#state{commit_handler_pid = undefined} = State) ->
    State;
wait_commit(#state{commit_handler_pid = Pid} = State) ->
    receive
        {'EXIT', Pid, Exit} -> handle_committed(Exit, State)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles the outcome of the commit handler.
%% @end
%%--------------------------------------------------------------------
-spec handle_committed(Exit :: any(), state()) -> state().
handle_committed({committed, {true, Rev}, _}, #state{
    commit_delay = Delay
} = State) ->
    schedule_terminate(schedule_commit(Delay, State#state{
        rev = Rev,
        commit_handler_pid = undefined
    }));
handle_committed({committed, {{false, Changes}, Rev}, Delay}, #state{
    changes = Changes2
} = State) ->
    Delay2 = exec(commit_backoff, [Delay]),
    schedule_commit(Delay2, State#state{
        rev = Rev,
        changes = merge_changes(Changes, Changes2),
        commit_handler_pid = undefined
    });
handle_committed(Exit, #state{commit_delay = Delay} = State) ->
    ?error("Commit handler of a datastore document terminated abnormally: ~p",
        [Exit]),
    schedule_terminate(schedule_commit(Delay, State#state{
        commit_handler_pid = undefined
    })).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Merges datastore document changes.
%% @end
%%--------------------------------------------------------------------
-spec merge_changes(changes(), changes()) -> changes().
merge_changes(Changes, undefined) ->
    Changes;
merge_changes(undefined, NextChanges) ->
    NextChanges;
merge_changes(Changes, NextChanges) ->
    exec(merge_changes, [Changes, NextChanges]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules commit operation if there are uncommitted changes and a commit
%% trigger message is no already awaited.
%% @end
%%--------------------------------------------------------------------
-spec schedule_commit(timeout(), state()) -> state().
schedule_commit(_Delay, #state{changes = undefined} = State) ->
    State;
schedule_commit(Delay, #state{commit_msg_ref = undefined} = State) ->
    Ref = make_ref(),
    erlang:send_after(Delay, self(), {Ref, {commit, Delay}}),
    State#state{commit_msg_ref = Ref};
schedule_commit(_Delay, #state{} = State) ->
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules terminate operation.
%% @end
%%--------------------------------------------------------------------
-spec schedule_terminate(state()) -> state().
schedule_terminate(#state{
    idle_timeout = IdleTimeout,
    terminate_timer_ref = undefined
} = State) ->
    MsgRef = make_ref(),
    TimerRef = erlang:send_after(IdleTimeout, self(), {MsgRef, terminate}),
    State#state{
        terminate_timer_ref = TimerRef,
        terminate_msg_ref = MsgRef
    };
schedule_terminate(#state{terminate_timer_ref = Ref} = State) ->
    erlang:cancel_timer(Ref),
    schedule_terminate(State#state{terminate_timer_ref = undefined}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the result of applying Function in Module to Args.
%% @end
%%--------------------------------------------------------------------
-spec exec(atom(), list()) -> Result :: term().
exec(Function, Args) ->
    erlang:apply(memory_store_driver, Function, Args).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls {@link exec/3} and catches exceptions.
%% @end
%%--------------------------------------------------------------------
-spec exec_noexcept(atom(), list()) ->
    {ok, Result :: term()} | {error, Reason :: term(), Stacktrace :: term()}.
exec_noexcept(Function, Args) ->
    try
        {ok, exec(Function, Args)}
    catch
        _:Reason -> {error, Reason, erlang:get_stacktrace()}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls {@link exec/3} as long as it fails with an exception.
%% Waits 'RetryDelay' milliseconds between consecutive attempts.
%% @end
%%--------------------------------------------------------------------
-spec exec_retry(atom(), list(), timeout()) -> term().
exec_retry(Function, Args, RetryDelay) ->
    try
        exec(Function, Args)
    catch
        _:Reason ->
            ?error_stacktrace("Datastore document process encountered "
            "unexpected error: ~p. Retrying after ~p...", [Reason, RetryDelay]),
            timer:sleep(RetryDelay),
            exec_retry(Function, Args, RetryDelay)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends responses to callers.
%% @end
%%--------------------------------------------------------------------
-spec notify([pid()], [reference()], term() | [response()]) -> ok.
notify(Pids, Refs, DocResponses) when is_list(DocResponses) ->
    Responses = lists:zip3(Pids, Refs, DocResponses),
    lists:foreach(fun({Pid, Ref, DocResponse}) ->
        Pid ! {Ref, DocResponse}
    end, Responses);
notify(Pids, Refs, Response) ->
    Responses = lists:duplicate(length(Pids), Response),
    notify(Pids, Refs, Responses).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops the execution of the calling process with exit response.
%% @end
%%--------------------------------------------------------------------
-spec return(Response :: term()) -> ok.
return(Response) ->
    exit(self(), Response),
    receive _ -> ok end.