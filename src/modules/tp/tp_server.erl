%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for serialization and aggregation of requests associated with a transaction
%%% process key. Requests handling logic is provided by a module implementing
%%% behaviour defined in a behaviour module.
%%% @end
%%%-------------------------------------------------------------------
-module(tp_server).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("modules/tp/tp.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    module :: tp:mod(),
    key :: tp:key(),
    data :: tp:data(),
    changes = undefined :: undefined | tp:changes(),
    changes_in_commit = undefined :: undefined | tp:changes(),
    requests = [] :: [{pid(), reference(), tp:request()}],
    requests_in_modify = [] :: [{pid(), reference(), tp:request()}],
    % a pid of the modify handler process
    modify_handler_pid :: undefined | pid(),
    % a pid of the commit handler process
    commit_handler_pid :: undefined | pid(),
    % a reference to the message expected to trigger commit
    commit_msg_ref :: undefined | reference(),
    % a reference to the message expected to trigger terminate
    terminate_msg_ref :: undefined | reference(),
    commit_delay :: timeout(),
    idle_timeout :: timeout()
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the transaction process.
%% @end
%%--------------------------------------------------------------------
-spec start_link(tp:mod(), tp:args(), tp:key()) ->
    {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(Module, Args, Key) ->
    gen_server:start_link(?MODULE, [Module, Args, Key], []).

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
init([Module, Args, Key]) ->
    case tp_router:create(Key, self()) of
        ok ->
            process_flag(trap_exit, true),
            case exec(Module, init, [Args]) of
                {ok, #tp_init{} = Init} ->
                    {ok, schedule_terminate(#state{
                        module = Module,
                        key = Key,
                        data = Init#tp_init.data,
                        commit_delay = Init#tp_init.max_commit_delay,
                        idle_timeout = Init#tp_init.idle_timeout
                    })};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, already_exists} ->
            ignore;
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
    requests_in_modify = [],
    changes = undefined,
    changes_in_commit = undefined,
    terminate_msg_ref = Ref
} = State) ->
    {stop, normal, State};
handle_info({Ref, terminate}, #state{terminate_msg_ref = Ref} = State) ->
    {noreply, schedule_terminate(State)};
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
terminate(Reason, #state{module = Module, key = Key} = State) when
    Reason == normal;
    Reason == shutdown ->
    State2 = modify_sync(State),
    #state{data = Data} = commit_sync(State2),
    exec(Module, terminate, [Data]),
    tp_router:delete(Key, self());
terminate({shutdown, _}, State) ->
    terminate(shutdown, State);
terminate(Reason, #state{key = Key} = State) ->
    ?log_terminate(Reason, State),
    tp_router:delete(Key, self()).

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
%% Synchronously modifies transaction process data.
%% @end
%%--------------------------------------------------------------------
-spec modify_sync(state()) -> state().
modify_sync(#state{requests = [], requests_in_modify = []} = State) ->
    State;
modify_sync(#state{
    module = Module,
    data = Data,
    changes = Changes,
    requests = Requests,
    requests_in_modify = []
} = State) ->
    {Pids, Refs, TpRequests} = lists:unzip3(lists:reverse(Requests)),
    case exec_noexcept(Module, modify, [TpRequests, Data]) of
        {ok, {TpResponses, NextChanges, Data2}} ->
            notify(Pids, Refs, TpResponses),
            State#state{
                data = Data2,
                changes = merge_changes(Module, Changes, NextChanges),
                modify_handler_pid = undefined,
                requests = []
            };
        {error, Reason, Stacktrace} ->
            notify(Pids, Refs, {error, Reason, Stacktrace}),
            State#state{
                modify_handler_pid = undefined,
                requests = []
            }
    end;
modify_sync(#state{
    requests = Requests,
    requests_in_modify = RequestsInModify
} = State) ->
    modify_sync(State#state{
        requests = Requests ++ RequestsInModify,
        requests_in_modify = []
    }).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asynchronously modifies transaction process data by spawning a handling
%% process. The process is spawned only if there are pending requests and
%% there is no other handler already spawned.
%% @end
%%--------------------------------------------------------------------
-spec modify_async(state()) -> state().
modify_async(#state{requests = []} = State) ->
    State;
modify_async(#state{
    module = Module,
    data = Data,
    requests = Requests,
    modify_handler_pid = undefined
} = State) ->
    Pid = spawn_link(fun() ->
        {Pids, Refs, TpRequests} = lists:unzip3(lists:reverse(Requests)),
        case exec_noexcept(Module, modify, [TpRequests, Data]) of
            {ok, {TpResponses, Changes, Data2}} ->
                notify(Pids, Refs, TpResponses),
                return({modified, Changes, Data2});
            {error, Reason, Stacktrace} ->
                notify(Pids, Refs, {error, Reason, Stacktrace}),
                return({Reason, Stacktrace})
        end
    end),
    State#state{
        requests = [],
        requests_in_modify = Requests,
        modify_handler_pid = Pid
    };
modify_async(#state{} = State) ->
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles the outcome of the modify handler.
%% @end
%%--------------------------------------------------------------------
-spec handle_modified(Exit :: any(), state()) -> state().
handle_modified({modified, false, Data}, #state{} = State) ->
    handle_modified(
        {modified, {true, undefined}, Data}, State
    );
handle_modified({modified, {true, NextChanges}, Data}, #state{
    module = Module,
    changes = Changes,
    commit_delay = Delay
} = State) ->
    schedule_commit(Delay, modify_async(State#state{
        data = Data,
        requests_in_modify = [],
        changes = merge_changes(Module, Changes, NextChanges),
        modify_handler_pid = undefined
    }));
handle_modified(Exit, #state{
    requests = Requests,
    requests_in_modify = RequestsInModify,
    commit_delay = Delay
} = State) ->
    ?error("Modify handler of a transaction process terminated abnormally: ~p",
        [Exit]),
    schedule_commit(Delay, modify_async(State#state{
        requests = Requests ++ RequestsInModify,
        requests_in_modify = [],
        modify_handler_pid = undefined
    })).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Synchronously commits transaction process changes.
%% @end
%%--------------------------------------------------------------------
-spec commit_sync(state()) -> state().
commit_sync(#state{
    changes = undefined,
    changes_in_commit = undefined
} = State) ->
    State;
commit_sync(#state{
    module = Module,
    data = Data,
    changes = Changes,
    changes_in_commit = undefined,
    commit_delay = Delay
} = State) ->
    case exec_noexcept(Module, commit, [Changes, Data]) of
        {ok, true} ->
            State#state{changes = undefined};
        {ok, {false, Changes2}} ->
            Delay2 = exec(Module, commit_backoff, [Delay]),
            timer:sleep(Delay2),
            commit_sync(State#state{
                changes = Changes2,
                commit_delay = Delay2
            });
        {error, Reason, Stacktrace} ->
            ?error("Synchronous commit handler of a transaction process
            terminated abnormally: ~p~nStacktrace: ~p", [Reason, Stacktrace]),
            Delay2 = exec(Module, commit_backoff, [Delay]),
            timer:sleep(Delay2),
            commit_sync(State#state{commit_delay = Delay2})
    end;
commit_sync(#state{
    module = Module,
    changes = Changes,
    changes_in_commit = ChangesInCommit
} = State) ->
    commit_sync(State#state{
        changes = merge_changes(Module, ChangesInCommit, Changes),
        changes_in_commit = undefined
    }).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asynchronously commits transaction process changes by spawning a handling
%% process. The process is spawned only if there are uncommitted changes and
%% there is no other handler already spawned.
%% @end
%%--------------------------------------------------------------------
-spec commit_async(timeout(), state()) -> state().
commit_async(_Delay, #state{changes = undefined} = State) ->
    State;
commit_async(Delay, #state{
    module = Module,
    data = Data,
    changes = Changes,
    commit_handler_pid = undefined
} = State) ->
    Pid = spawn_link(fun() ->
        Response = exec(Module, commit, [Changes, Data]),
        return({committed, Response, Delay})
    end),
    State#state{
        changes = undefined,
        changes_in_commit = Changes,
        commit_handler_pid = Pid
    };
commit_async(_Delay, #state{} = State) ->
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles the outcome of the commit handler.
%% @end
%%--------------------------------------------------------------------
-spec handle_committed(Exit :: any(), state()) -> state().
handle_committed({committed, true, _}, #state{commit_delay = Delay} = State) ->
    schedule_commit(Delay, State#state{
        changes_in_commit = undefined,
        commit_handler_pid = undefined
    });
handle_committed({committed, {false, Changes}, Delay}, #state{
    module = Module,
    changes = Changes2
} = State) ->
    Delay2 = exec(Module, commit_backoff, [Delay]),
    schedule_commit(Delay2, State#state{
        changes = merge_changes(Module, Changes, Changes2),
        changes_in_commit = undefined,
        commit_handler_pid = undefined
    });
handle_committed(Exit, #state{
    module = Module,
    changes = Changes,
    changes_in_commit = ChangesInCommit,
    commit_delay = Delay
} = State) ->
    ?error("Commit handler of a transaction process terminated abnormally: ~p",
        [Exit]),
    schedule_commit(Delay, State#state{
        changes = merge_changes(Module, ChangesInCommit, Changes),
        changes_in_commit = undefined,
        commit_handler_pid = undefined
    }).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Merges transaction process changes.
%% @end
%%--------------------------------------------------------------------
-spec merge_changes(tp:mod(), tp:changes(), tp:changes()) -> tp:changes().
merge_changes(_Module, Changes, undefined) ->
    Changes;
merge_changes(_Module, undefined, NextChanges) ->
    NextChanges;
merge_changes(Module, Changes, NextChanges) ->
    exec(Module, merge_changes, [Changes, NextChanges]).

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
    State#state{commit_msg_ref = schedule_msg(Delay, {commit, Delay})};
schedule_commit(_Delay, #state{} = State) ->
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules terminate operation.
%% @end
%%--------------------------------------------------------------------
-spec schedule_terminate(state()) -> state().
schedule_terminate(#state{idle_timeout = IdleTimeout} = State) ->
    State#state{terminate_msg_ref = schedule_msg(IdleTimeout, terminate)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Wraps message with a reference tag and sends after delay to the calling
%% process. Returns reference tag.
%% @end
%%--------------------------------------------------------------------
-spec schedule_msg(timeout(), any()) -> undefined | reference().
schedule_msg(infinity, _Msg) ->
    undefined;
schedule_msg(Delay, Msg) ->
    Ref = make_ref(),
    erlang:send_after(Delay, self(), {Ref, Msg}),
    Ref.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the result of applying Function in Module to Args.
%% @end
%%--------------------------------------------------------------------
-spec exec(tp:mod(), atom(), list()) -> Result :: term().
exec(Module, Function, Args) ->
    erlang:apply(Module, Function, Args).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls {@link exec/3} and catches exceptions.
%% @end
%%--------------------------------------------------------------------
-spec exec_noexcept(tp:mod(), atom(), list()) ->
    {ok, Result :: term()} | {error, Reason :: term(), Stacktrace :: term()}.
exec_noexcept(Module, Function, Args) ->
    try
        {ok, exec(Module, Function, Args)}
    catch
        _:Reason -> {error, Reason, erlang:get_stacktrace()}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends responses to the tp_server callers.
%% @end
%%--------------------------------------------------------------------
-spec notify([pid()], [reference()], term() | [tp:response()]) -> ok.
notify(Pids, Refs, TpResponses) when is_list(TpResponses) ->
    Responses = lists:zip3(Pids, Refs, TpResponses),
    lists:foreach(fun({Pid, Ref, TpResponse}) ->
        Pid ! {Ref, TpResponse}
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