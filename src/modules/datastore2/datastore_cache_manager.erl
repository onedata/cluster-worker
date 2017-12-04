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
-include("modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/0, reset/2, get_size/1]).
-export([mark_active/3, mark_inactive/2]).

-record(entry, {
    cache_key :: datastore:key() | '_',
    mutator_pid :: pid() | '_',
    volatile :: boolean() | '_',
    driver :: datastore:memory_driver() | '_',
    driver_ctx :: datastore:memory_driver_ctx() | '_',
    driver_key :: datastore:key() | '_'
}).

-record(stats, {
    key :: atom(),
    value :: non_neg_integer()
}).

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
    Pools = datastore_multiplier:get_names(memory)
        ++ datastore_multiplier:get_names(disc),

    lists:foreach(fun(Pool) ->
        ets:new(active(Pool), [set, public, named_table, {keypos, 2}]),
        ets:new(inactive(Pool), [set, public, named_table, {keypos, 2}]),
        ets:new(clear(Pool), [set, public, named_table]),
        SizeByPool = application:get_env(?CLUSTER_WORKER_APP_NAME,
            datastore_cache_size, []),
        MaxSize = proplists:get_value(Pool, SizeByPool, 500000),
        ets:insert(active(Pool), #stats{key = size, value = 0}),
        ets:insert(active(Pool), #stats{key = max_size, value = MaxSize})
    end, Pools).

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
-spec mark_active(pool(), datastore_cache:ctx(), datastore:key()) -> boolean().
mark_active(Pool, Ctx = #{
    mutator_pid := Pid,
    volatile := Volatile,
    memory_driver := Driver,
    memory_driver_ctx := DriverCtx
}, Key) ->
    CacheKey = couchbase_doc:set_prefix(Ctx, Key),
    Entry = #entry{
        cache_key = CacheKey,
        mutator_pid = Pid,
        volatile = Volatile,
        driver = Driver,
        driver_ctx = DriverCtx,
        driver_key = Key
    },
    case ets:lookup(active(Pool), CacheKey) of
        [#entry{mutator_pid = Pid}] -> true;
        [#entry{}] -> remark_active(Pool, Entry);
        _ -> mark_active(Pool, Entry)
    end;
mark_active(Pool, Ctx, Key) ->
    mark_active(Pool, Ctx#{volatile => false}, Key).

%%--------------------------------------------------------------------
%% @doc
%% Marks previously active entry/entries as inactive.
%% @end
%%--------------------------------------------------------------------
-spec mark_inactive(pool(), pid() | datastore:key() | [datastore:key()]) -> boolean().
mark_inactive(Pool, Selector) ->
    case lists:sublist(atom_to_list(Pool), 4) of
        "disc" ->
            mark_inactive(Pool, Selector, fun(_) -> true end);
        _ ->
            Filter = fun
                (#entry{volatile = true}) ->
                    true;
                (#entry{driver = Driver, driver_ctx = Ctx, driver_key = Key}) ->
                    case Driver:get(Ctx, Key) of
                        {ok, #document{deleted = Deleted}} -> Deleted;
                        {error, key_enoent} -> true
                    end
            end,
            mark_inactive(Pool, Selector, Filter)
    end.

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
active(Pool) ->
    list_to_atom("datastore_cache_active_pool_" ++ atom_to_list(Pool)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns name of inactive keys table for a pool.
%% @end
%%--------------------------------------------------------------------
-spec inactive(pool()) -> atom().
inactive(Pool) ->
    list_to_atom("datastore_cache_inactive_pool_" ++ atom_to_list(Pool)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns name of table (for a pool) that keeps keys that are beeing cleared.
%% @end
%%--------------------------------------------------------------------
-spec clear(pool()) -> atom().
clear(Pool) ->
    list_to_atom("datastore_cache_clear_pool_" ++ atom_to_list(Pool)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to mark entry as active.
%% @end
%%--------------------------------------------------------------------
-spec mark_active(pool(), entry()) -> boolean().
mark_active(Pool, Entry = #entry{cache_key = CacheKey}) ->
    case ets:lookup(inactive(Pool), CacheKey) of
        [_] ->
            case ets:insert_new(clear(Pool), {CacheKey, ok}) of
                true ->
                    case ets:lookup(inactive(Pool), CacheKey) of
                        [_] ->
                            ets:delete(inactive(Pool), CacheKey),
                            ets:delete(clear(Pool), CacheKey),
                            ets:insert(active(Pool), Entry),
                            true;
                        _ ->
                            ets:delete(clear(Pool), CacheKey),
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
        CacheKey ->
            case ets:insert_new(clear(Pool), {CacheKey, ok}) of
                true ->
                    case ets:lookup(inactive(Pool), CacheKey) of
                        [#entry{
                            driver = Driver,
                            driver_ctx = Ctx,
                            driver_key = Key
                        }] ->
                            ets:delete(inactive(Pool), CacheKey),
                            ets:delete(clear(Pool), CacheKey),
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
-spec mark_inactive(pool(), pid() | datastore:key() | [datastore:key()],
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
    inactivate(Pool, Entries2);
mark_inactive(Pool, Keys, Filter) when is_list(Keys) ->
    Entries = lists:foldl(fun(Key, Acc) ->
        ets:lookup(active(Pool), Key) ++ Acc
    end, [], Keys),
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
    lists:foreach(fun(Entry = #entry{cache_key = Key}) ->
        ets:insert(inactive(Pool), Entry),
        ets:delete(active(Pool), Key)
    end, Entries),
    true.