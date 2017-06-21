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
-include("modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/1]).
-export([reset/1, resize/2]).
-export([mark_active/3, mark_inactive/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(entry, {
    cache_key :: datastore:key() | '_',
    mutator_pid :: pid() | '_',
    volatile :: boolean() | '_',
    driver :: datastore:memory_driver() | '_',
    driver_ctx :: datastore:memory_driver_ctx() | '_',
    driver_key :: datastore:key() | '_'
}).

-record(state, {
    pool :: pool(),
    size :: non_neg_integer(),
    max_size :: non_neg_integer()
}).

-type pool() :: memory | disc.
-type entry() :: #entry{}.
-type state() :: #state{}.

-export_type([pool/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts datastore cache manager.
%% @end
%%--------------------------------------------------------------------
-spec start_link(pool()) -> {ok, pid()} | {error, Reason :: term()}.
start_link(Pool) ->
    gen_server2:start_link({local, name(Pool)}, ?MODULE, [Pool], []).

%%--------------------------------------------------------------------
%% @doc
%% Resets cache manager to initial state.
%% IMPORTANT! This function does not free active/inactive entries.
%% @end
%%--------------------------------------------------------------------
-spec reset(pool()) -> ok.
reset(Pool) ->
    gen_server2:call(name(Pool), reset).

%%--------------------------------------------------------------------
%% @doc
%% Changes the maximal number of entries that may be stored in cache.
%% If current cache size if greater than the new maximal size, inactive entries
%% are removed from cache.
%% @end
%%--------------------------------------------------------------------
-spec resize(pool(), non_neg_integer()) -> ok.
resize(Pool, NewSize) ->
    gen_server2:call(name(Pool), {resize, NewSize}).

%%--------------------------------------------------------------------
%% @doc
%% Tries to mark entry associated with key as active. Activation does not take
%% place if it would cause cache limit overflow and removal of inactive entries
%% does not change this situation. Returns 'true' if entry has been activated,
%% otherwise returns 'false'.
%% @end
%%--------------------------------------------------------------------
-spec mark_active(pool(), datastore_cache:ctx(), datastore:key()) -> boolean().
mark_active(Pool, Ctx = #{
    mutator_pid := Pid,
    volatile := Volatile,
    memory_driver := Driver,
    memory_driver_ctx := DriverCtx
}, Key) ->
    CacheKey = couchbase_doc:set_prefix(Ctx, Key),
    NewEntry = #entry{
        cache_key = CacheKey,
        mutator_pid = Pid,
        volatile = Volatile,
        driver = Driver,
        driver_ctx = DriverCtx,
        driver_key = Key
    },
    case ets:lookup(active(Pool), CacheKey) of
        [#entry{mutator_pid = Pid}] ->
            true;
        [#entry{}] ->
            gen_server2:call(name(Pool), {remark_active, NewEntry});
        _ ->
            gen_server2:call(name(Pool), {mark_active, NewEntry})
    end;
mark_active(Pool, Ctx, Key) ->
    mark_active(Pool, Ctx#{volatile => false}, Key).

%%--------------------------------------------------------------------
%% @doc
%% Marks previously active entry as inactive. If current cache size is greater
%% than the maximal allowed size, this entry will be instantaneously removed
%% from cache.
%% @end
%%--------------------------------------------------------------------
-spec mark_inactive(pool(), pid() | datastore:key()) -> boolean().
mark_inactive(memory, Selector) ->
    Filter = fun
        (#entry{volatile = true}) ->
            true;
        (#entry{driver = Driver, driver_ctx = Ctx, driver_key = Key}) ->
            case Driver:get(Ctx, Key) of
                {ok, #document{deleted = Deleted}} -> Deleted;
                {error, key_enoent} -> true
            end
    end,
    inactivate(memory, Selector, Filter);
mark_inactive(disc, Selector) ->
    inactivate(disc, Selector, fun(_) -> true end).

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
init([Pool]) ->
    ets:new(active(Pool), [
        set, public, named_table, {keypos, 2}, {read_concurrency, true}
    ]),
    ets:new(inactive(Pool), [set, named_table, {keypos, 2}]),
    SizeByPool = application:get_env(?CLUSTER_WORKER_APP_NAME,
        datastore_cache_size, []),
    {ok, #state{
        pool = Pool,
        size = 0,
        max_size = proplists:get_value(Pool, SizeByPool, 500000)
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
handle_call(reset, _From, State = #state{pool = Pool}) ->
    ets:delete_all_objects(active(Pool)),
    ets:delete_all_objects(inactive(Pool)),
    {reply, ok, State#state{size = 0}};
handle_call({resize, Size}, _From, #state{} = State) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, datastore_cache_size, Size),
    {reply, ok, deallocate(State#state{max_size = Size})};
handle_call({remark_active, Entry = #entry{}}, _, State = #state{
    pool = Pool
}) ->
    ets:insert(active(Pool), Entry),
    {reply, true, State};
handle_call({mark_active, Entry = #entry{cache_key = Key}}, _, State = #state{
    pool = Pool
}) ->
    case ets:lookup(inactive(Pool), Key) of
        [#entry{}] ->
            ets:insert(active(Pool), Entry),
            ets:delete(inactive(Pool), Key),
            {reply, true, State};
        [] ->
            {Activated, State2} = activate(Entry, State),
            {reply, Activated, State2}
    end;
handle_call({mark_inactive, Entries}, _From, State = #state{pool = Pool}) ->
    lists:foreach(fun(Entry = #entry{cache_key = Key}) ->
        ets:insert(inactive(Pool), Entry),
        ets:delete(active(Pool), Key)
    end, Entries),
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
%% Returns name of datastore cache manager server for a pool.
%% @end
%%--------------------------------------------------------------------
-spec name(pool()) -> atom().
name(memory) -> datastore_cache_manager_memory_pool;
name(disc) -> datastore_cache_manager_disc_pool.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns name of active keys table for a pool.
%% @end
%%--------------------------------------------------------------------
-spec active(pool()) -> atom().
active(memory) -> datastore_cache_active_memory_pool;
active(disc) -> datastore_cache_active_disc_pool.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns name of inactive keys table for a pool.
%% @end
%%--------------------------------------------------------------------
-spec inactive(pool()) -> atom().
inactive(memory) -> datastore_cache_inactive_memory_pool;
inactive(disc) -> datastore_cache_inactive_disc_pool.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to activate entry associated with key. Activation does not take place
%% if it would cause cache limit overflow and removal of inactive entries
%% does not change this situation. Returns tuple where first element denotes
%% whether entry has been activated, and second element is new state.
%% @end
%%--------------------------------------------------------------------
-spec activate(entry(), state()) -> {boolean(), state()}.
activate(Entry = #entry{}, State = #state{pool = Pool}) ->
    #state{size = Size, max_size = MaxSize} = State2 = deallocate(State),
    case Size + 1 > MaxSize of
        true ->
            case ets:first(inactive(Pool)) of
                '$end_of_table' ->
                    {false, State2};
                CacheKey ->
                    [#entry{
                        driver = Driver,
                        driver_ctx = Ctx,
                        driver_key = Key
                    }] = ets:lookup(inactive(Pool), CacheKey),
                    Driver:delete(Ctx, Key),
                    ets:delete(inactive(Pool), CacheKey),
                    ets:insert(active(Pool), Entry),
                    {true, State2}
            end;
        false ->
            ets:insert(active(Pool), Entry),
            {true, State2#state{size = Size + 1}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Marks previously active entries as inactive.
%% @end
%%--------------------------------------------------------------------
-spec inactivate(pool(), pid() | datastore:key(),
    fun((datastore:key()) -> boolean())) -> boolean().
inactivate(Pool, Pid, Filter) when is_pid(Pid) ->
    Entries = ets:select(active(Pool), [
        {#entry{mutator_pid = Pid, _='_'}, [], ['$_']}
    ]),
    Entries2 = lists:filter(Filter, Entries),
    case Entries2 of
        [] -> false;
        _ -> gen_server2:call(name(Pool), {mark_inactive, Entries2})
    end;
inactivate(Pool, Key, Filter) when is_binary(Key) ->
    Entries = ets:lookup(active(Pool), Key),
    Entries2 = lists:filter(Filter, Entries),
    case Entries2 of
        [] -> false;
        _ -> gen_server2:call(name(Pool), {mark_inactive, Entries2})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes inactive entries from cache as long as current cache size is greater
%% than maximal allowed size or all inactive entries are removed.
%% @end
%%--------------------------------------------------------------------
-spec deallocate(state()) -> state().
deallocate(State = #state{
    pool = Pool,
    size = Size,
    max_size = MSize
} = State) when Size > MSize ->
    case ets:first(inactive(Pool)) of
        '$end_of_table' ->
            State;
        CacheKey ->
            [#entry{
                driver = Driver,
                driver_ctx = Ctx,
                driver_key = Key
            }] = ets:lookup(inactive(Pool), CacheKey),
            Driver:delete(Ctx, Key),
            ets:delete(inactive(Pool), CacheKey),
            deallocate(State#state{size = Size - 1})
    end;
deallocate(#state{} = State) ->
    State.