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

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/0, reset/2, get_size/1]).
-export([mark_active/3, mark_inactive/2]).

-record(entry, {
    key :: key() | '_',
    mutator_pid :: pid() | '_',
    volatile :: boolean() | '_',
    driver :: datastore:memory_driver() | '_',
    driver_ctx :: datastore:memory_driver_ctx() | '_'
}).

-record(stats, {
    key :: atom(),
    value :: non_neg_integer()
}).

-type ctx() :: datastore_cache:ctx().
-type key() :: datastore:key().
-type pool() :: memory | disc.
-type entry() :: #entry{}.

-export_type([pool/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes datastore cache manager.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    lists:foreach(fun(Pool) ->
        ets:new(active(Pool), [set, public, named_table, {keypos, 2}]),
        ets:new(inactive(Pool), [set, public, named_table, {keypos, 2}]),
        ets:new(clear(Pool), [set, public, named_table]),
        SizeByPool = application:get_env(?CLUSTER_WORKER_APP_NAME,
            datastore_cache_size, []),
        MaxSize = proplists:get_value(Pool, SizeByPool, 500000),
        ets:insert(active(Pool), #stats{key = size, value = 0}),
        ets:insert(active(Pool), #stats{key = max_size, value = MaxSize})
    end, [memory, disc]).

%%--------------------------------------------------------------------
%% @doc
%% Resets cache manager and changes the maximal number of entries that
%% may be stored in cache.
%% IMPORTANT! This function does not free active/inactive entries.
%% @end
%%--------------------------------------------------------------------
-spec reset(pool(), non_neg_integer()) -> ok.
reset(Pool, NewSize) ->
    ets:delete_all_objects(active(Pool)),
    ets:delete_all_objects(inactive(Pool)),
    ets:insert(active(Pool), #stats{key = size, value = 0}),
    ets:insert(active(Pool), #stats{key = max_size, value = NewSize}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns cache size.
%% @end
%%--------------------------------------------------------------------
-spec get_size(pool()) -> non_neg_integer().
get_size(Pool) ->
    ets:lookup_element(active(Pool), size, 3).

%%--------------------------------------------------------------------
%% @doc
%% Tries to mark entry associated with key as active. Activation does not take
%% place if it would cause cache limit overflow and removal of inactive entries
%% does not change this situation. Returns 'true' if entry has been activated,
%% otherwise returns 'false'.
%% @end
%%--------------------------------------------------------------------
-spec mark_active(pool(), ctx(), key()) -> boolean().
mark_active(Pool, #{
    mutator_pid := MutatorPid,
    volatile := Volatile,
    memory_driver := Driver,
    memory_driver_ctx := DriverCtx
}, Key) ->
    Entry = #entry{
        key = Key,
        mutator_pid = MutatorPid,
        volatile = Volatile,
        driver = Driver,
        driver_ctx = DriverCtx
    },
    case ets:lookup(active(Pool), Key) of
        [#entry{mutator_pid = MutatorPid}] -> true;
        [#entry{}] -> remark_active(Pool, Entry);
        _ -> mark_active(Pool, Entry)
    end;
mark_active(Pool, Ctx = #{mutator_pid := _}, Key) ->
    mark_active(Pool, Ctx#{volatile => false}, Key).

%%--------------------------------------------------------------------
%% @doc
%% Marks previously active entry/entries as inactive.
%% @end
%%--------------------------------------------------------------------
-spec mark_inactive(pool(), pid() | datastore:key()) -> boolean().
mark_inactive(memory, Selector) ->
    Filter = fun
        (#entry{volatile = true}) ->
            true;
        (#entry{key = Key, driver = Driver, driver_ctx = Ctx}) ->
            case Driver:get(Ctx, Key) of
                {ok, #document{deleted = Deleted}} -> Deleted;
                {error, not_found} -> true
            end
    end,
    mark_inactive(memory, Selector, Filter);
mark_inactive(disc, Selector) ->
    mark_inactive(disc, Selector, fun(_) -> true end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
%% Returns name of table (for a pool) that keeps keys that are beeing cleared.
%% @end
%%--------------------------------------------------------------------
-spec clear(pool()) -> atom().
clear(memory) -> datastore_cache_clear_memory_pool;
clear(disc) -> datastore_cache_clear_disc_pool.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to mark entry as active.
%% @end
%%--------------------------------------------------------------------
-spec mark_active(pool(), entry()) -> boolean().
mark_active(Pool, Entry = #entry{key = Key}) ->
    case ets:lookup(inactive(Pool), Key) of
        [_] ->
            case ets:insert_new(clear(Pool), {Key, ok}) of
                true ->
                    case ets:lookup(inactive(Pool), Key) of
                        [_] ->
                            ets:delete(inactive(Pool), Key),
                            ets:delete(clear(Pool), Key),
                            ets:insert(active(Pool), Entry),
                            true;
                        _ ->
                            ets:delete(clear(Pool), Key),
                            activate(Pool, Entry)
                    end;
                _ ->
                    activate(Pool, Entry)
            end;
        _ ->
            activate(Pool, Entry)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Remarks already active entry as active with new mutator.
%% @end
%%--------------------------------------------------------------------
-spec remark_active(pool(), entry()) -> true.
remark_active(Pool, Entry) ->
    ets:insert(active(Pool), Entry).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to activate entry associated with key. Activation does not take place
%% if it would cause cache limit overflow and removal of inactive entries
%% does not change this situation.
%% @end
%%--------------------------------------------------------------------
-spec activate(pool(), entry()) -> boolean().
activate(Pool, Entry = #entry{}) ->
    [#stats{value = MaxSize}] = ets:lookup(active(Pool), max_size),
    Size = ets:update_counter(active(Pool), size, {3, 1}),
    case Size > MaxSize of
        true ->
            Activated = relocate(Pool, Entry),
            ets:update_counter(active(Pool), size, {3, -1}),
            Activated;
        false ->
            ets:insert(active(Pool), Entry),
            true
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes inactive entry from cache and adds new one if possible.
%% @end
%%--------------------------------------------------------------------
-spec relocate(pool(), entry()) -> boolean().
relocate(Pool, Entry) ->
    case ets:first(inactive(Pool)) of
        '$end_of_table' ->
            false;
        Key ->
            case ets:insert_new(clear(Pool), {Key, ok}) of
                true ->
                    case ets:lookup(inactive(Pool), Key) of
                        [#entry{
                            driver = Driver,
                            driver_ctx = Ctx
                        }] ->
                            ets:delete(inactive(Pool), Key),
                            ets:delete(clear(Pool), Key),
                            Driver:delete(Ctx, Key),
                            ets:insert(active(Pool), Entry),
                            true;
                        [] ->
                            relocate(Pool, Entry)
                    end;
                _ ->
                    relocate(Pool, Entry)
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Selects active entries and inactivates them.
%% @end
%%--------------------------------------------------------------------
-spec mark_inactive(pool(), pid() | datastore:key(),
    fun((datastore:key()) -> boolean())) -> boolean().
mark_inactive(Pool, Pid, Filter) when is_pid(Pid) ->
    Entries = ets:select(active(Pool), [
        {#entry{mutator_pid = Pid, _ = '_'}, [], ['$_']}
    ]),
    Entries2 = lists:filter(Filter, Entries),
    inactivate(Pool, Entries2);
mark_inactive(Pool, Key, Filter) when is_binary(Key) ->
    Entries = ets:lookup(active(Pool), Key),
    Entries2 = lists:filter(Filter, Entries),
    inactivate(Pool, Entries2).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Marks previously active entries as inactive.
%% @end
%%--------------------------------------------------------------------
-spec inactivate(pool(), [entry()]) -> boolean().
inactivate(_Pool, []) ->
    false;
inactivate(Pool, Entries) ->
    lists:foreach(fun(Entry = #entry{key = Key}) ->
        ets:insert(inactive(Pool), Entry),
        ets:delete(active(Pool), Key)
    end, Entries),
    true.