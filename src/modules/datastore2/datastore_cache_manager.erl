%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for management of datastore cache. It keeps track
%%% of active and inactive entries and does not allow to overfill limit for
%%% number of entries stored in cache. An entry is a datastore document. When
%%% marked as inactive it will be automatically removed from cache if new
%%% entries are to be stored and it would exceed the limit. Active entries
%%% cannot be removed from cache and must be explicitly deactivated by calling
%%% {@link mark_inactive/3} function.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_cache_manager).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0]).
-export([reset/0, resize/1, mark_active/1, mark_inactive/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    size :: non_neg_integer(),
    max_size :: non_neg_integer()
}).

-type state() :: #state{}.

-define(ACTIVE, datastore_cache_active).
-define(INACTIVE, datastore_cache_inactive).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts datastore cache manager.
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | {error, Reason :: term()}.
start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Resets cache manager to initial state.
%% IMPORTANT! This function does not free active/inactive entries.
%% @end
%%--------------------------------------------------------------------
-spec reset() -> ok.
reset() ->
    gen_server2:call(?MODULE, reset).

%%--------------------------------------------------------------------
%% @doc
%% Changes the maximal number of entries that may be stored in cache.
%% If current cache size if greater than the new maximal size, inactive entries
%% are removed from cache.
%% @end
%%--------------------------------------------------------------------
-spec resize(non_neg_integer()) -> ok.
resize(NewSize) ->
    gen_server2:call(?MODULE, {resize, NewSize}).

%%--------------------------------------------------------------------
%% @doc
%% Tries to mark entry associated with key as active. Activation does not take
%% place if it would cause cache limit overflow and removal of inactive entries
%% does not change this situation. Returns 'true' if entry has been activated,
%% otherwise returns 'false'.
%% @end
%%--------------------------------------------------------------------
-spec mark_active(datastore:key()) -> boolean().
mark_active(Key) ->
    case ets:lookup(?ACTIVE, Key) of
        [{Key}] -> true;
        [] -> gen_server2:call(?MODULE, {mark_active, Key})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Marks previously active entry as inactive. If current cache size is greater
%% than the maximal allowed size, this entry will be instantaneously removed
%% from cache.
%% @end
%%--------------------------------------------------------------------
-spec mark_inactive(datastore:key(), datastore:memory_driver(),
    datastore:memory_driver_ctx()) -> boolean().
mark_inactive(Key, MemoryDriver, MemoryDriverCtx) ->
    case ets:lookup(?ACTIVE, Key) of
        [{Key}] ->
            Request = {mark_inactive, Key, MemoryDriver, MemoryDriverCtx},
            gen_server2:call(?MODULE, Request);
        [] ->
            false
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes datastore cache manager.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([]) ->
    ets:new(?ACTIVE, [set, public, named_table, {read_concurrency, true}]),
    ets:new(?INACTIVE, [set, named_table]),
    {ok, #state{
        size = 0,
        max_size = application:get_env(?CLUSTER_WORKER_APP_NAME,
            datastore_cache_size, 10000)
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
handle_call(reset, _From, #state{} = State) ->
    ets:delete_all_objects(?ACTIVE),
    ets:delete_all_objects(?INACTIVE),
    {reply, ok, State#state{size = 0}};
handle_call({resize, Size}, _From, #state{} = State) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, datastore_cache_size, Size),
    {reply, ok, deallocate(State#state{max_size = Size})};
handle_call({mark_active, Key}, _From, #state{} = State) ->
    case ets:lookup(?INACTIVE, Key) of
        [{Key, _}] ->
            ets:insert(?ACTIVE, {Key}),
            ets:delete(?INACTIVE, Key),
            {reply, true, State};
        [] ->
            {Activated, State2} = activate(Key, State),
            {reply, Activated, State2}
    end;
handle_call({mark_inactive, Key, Driver, Ctx}, _From, #state{} = State) ->
    ets:insert(?INACTIVE, {Key, {Driver, Ctx}}),
    ets:delete(?ACTIVE, Key),
    {reply, true, deallocate(State)};
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
%% Tries to activate entry associated with key. Activation does not take place
%% if it would cause cache limit overflow and removal of inactive entries
%% does not change this situation. Returns tuple where first element denotes
%% whether entry has been activated, and second element is new state.
%% @end
%%--------------------------------------------------------------------
-spec activate(datastore:key(), state()) -> {boolean(), state()}.
activate(Key, #state{} = State) ->
    #state{size = Size, max_size = MaxSize} = State2 = deallocate(State),
    case Size + 1 > MaxSize of
        true ->
            case ets:first(?INACTIVE) of
                '$end_of_table' ->
                    {false, State2};
                Key2 ->
                    [{Key2, {Driver, Ctx}}] = ets:lookup(?INACTIVE, Key2),
                    Driver:delete(Ctx, Key2),
                    ets:delete(?INACTIVE, Key2),
                    ets:insert(?ACTIVE, {Key}),
                    {true, State2}
            end;
        false ->
            ets:insert(?ACTIVE, {Key}),
            {true, State2#state{size = Size + 1}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes inactive entries from cache as long as current cache size is greater
%% than maximal allowed size or all inactive entries are removed.
%% @end
%%--------------------------------------------------------------------
-spec deallocate(state()) -> state().
deallocate(#state{size = Size, max_size = MSize} = State) when Size > MSize ->
    case ets:first(?INACTIVE) of
        '$end_of_table' ->
            State;
        Key ->
            [{Key, {Driver, Ctx}}] = ets:lookup(?INACTIVE, Key),
            Driver:delete(Ctx, Key),
            ets:delete(?INACTIVE, Key),
            deallocate(State#state{size = Size - 1})
    end;
deallocate(#state{} = State) ->
    State.
    