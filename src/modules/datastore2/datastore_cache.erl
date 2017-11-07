%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an API for datastore cache. Datastore cache consists
%%% of two components: memory and disc store. Operations on memory take
%%% precedence over disc, as long as memory limit is not reached. For each
%%% successful operation along with result a durability level is returned.
%%% It denotes which layer, memory or disc, has been used. An exception
%%% constitute {@link get/2} and {@link flush/2} functions, which are meant
%%% to operate in memory and on disc accordingly and therefore do not return
%%% durability level.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_cache).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models_def.hrl").

%% API
-export([get/2, fetch/2, save/2, update/2, update/3, flush/2, flush/1]).
-export([inactivate/1, inactivate/3]).

-type ctx() :: #{prefix => binary(),
                 mutator_pid => pid(),
                 volatile => boolean(),
                 memory_driver => datastore:memory_driver(),
                 memory_driver_ctx => datastore:memory_driver_ctx(),
                 disc_driver => datastore:disc_driver(),
                 disc_driver_ctx => datastore:disc_driver_ctx(),
                % TODO - remove when driver and datastore ctx are separated
                 atom() => term()}.
-type key() :: datastore:key().
-type value() :: datastore:document().
-type diff() :: fun((value()) -> {ok, value()} | {error, term()}).
-type durability() :: memory | disc.

-export_type([ctx/0, diff/0, durability/0]).

-record(future, {
    durability :: undefined | durability(),
    driver :: undefined | datastore:driver(),
    value :: {ok, value()} | {error, term()} | couchbase_pool:future()
}).

-type future() :: #future{}.

-define(FUTURE(Value), ?FUTURE(undefined, undefined, Value)).
-define(FUTURE(Durability, Driver, Value), #future{
    durability = Durability,
    driver = Driver,
    value = Value
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves values from memory only.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), key()) -> {ok, value()} | {error, term()};
    (ctx(), [datastore:key()]) -> [{ok, value()} | {error, term()}].
get(Ctx, <<_/binary>> = Key) ->
    hd(get(Ctx, [Key]));
get(Ctx, Keys) when is_list(Keys) ->
    lists:map(fun
        ({ok, memory, Value}) -> {ok, Value};
        ({error, Reason}) -> {error, Reason}
    end, wait([get_async(Ctx, Key, false) || Key <- Keys])).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves values from memory or if missing from disc. For values read from
%% disc, an attempt is made to store them in memory. If cache update fails with
%% out of memory error, the returned durability level equals to 'disc',
%% otherwise it is set to 'memory'.
%% @end
%%--------------------------------------------------------------------
-spec fetch(ctx(), key()) -> {ok, durability(), value()} | {error, term()};
    (ctx(), [key()]) -> [{ok, durability(), value()} | {error, term()}].
fetch(Ctx, <<_/binary>> = Key) ->
    hd(fetch(Ctx, [Key]));
fetch(#{memory_driver := MemoryDriver} = Ctx, Keys) when is_list(Keys) ->
    Futures = lists:map(fun
        ({_Key, {ok, memory, Value}}) ->
            ?FUTURE(memory, MemoryDriver, {ok, Value});
        ({_Key, {ok, disc, Value}}) ->
            save_async(Ctx, Value, false);
        ({Key, {error, key_enoent}}) ->
            Value = #document{key = Key, value = undefined, deleted = true},
            save_async(Ctx, Value, false),
            ?FUTURE({error, key_enoent});
        ({_Key, {error, Reason}}) ->
            ?FUTURE({error, Reason})
    end, lists:zip(Keys, wait([get_async(Ctx, Key, true) || Key <- Keys]))),

    lists:map(fun
        ({ok, memory, Value}) -> {ok, memory, Value};
        ({error, {enomem, Value}}) -> {ok, disc, Value};
        ({error, Reason}) -> {error, Reason}
    end, wait(Futures)).

%%--------------------------------------------------------------------
%% @doc
%% Stores values in memory or if cache is full on disc.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), value() | [value()]) ->
    {ok, durability(), value()} | {error, term()}
    | [{ok, durability(), value()} | {error, term()}].
save(Ctx, #document{} = Value) ->
    hd(save(Ctx, [Value]));
save(Ctx, Values) when is_list(Values) ->
    lists:map(fun
        ({error, {enomem, _Value}}) -> {error, enomem};
        (Other) -> Other
    end, wait([save_async(Ctx, Value, true) || Value <- Values])).

%%--------------------------------------------------------------------
%% @equiv hd(update(Ctx, [{Key, Diff}]))
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff()) ->
    {ok, durability(), value()} | {error, term()}.
update(Ctx, Key, Diff) ->
    hd(update(Ctx, [{Key, Diff}])).

%%--------------------------------------------------------------------
%% @doc
%% Updates values by retrieving them from memory or disc, applying transition
%% function, and storing them back to memory or disc if cache limit has been
%% reached.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), [{key(), diff()}]) ->
    [{ok, durability(), value()} | {error, term()}].
update(Ctx, Updates) when is_list(Updates) ->
    {Keys, Diffs} = lists:unzip(Updates),

    Futures = lists:map(fun
        ({Diff, {ok, _, Value}}) ->
            case Diff(Value) of
                {ok, NewValue} -> save_async(Ctx, NewValue, true);
                {error, Reason} -> ?FUTURE({error, Reason})
            end;
        ({_Diff, {error, Reason}}) ->
            ?FUTURE({error, Reason})
    end, lists:zip(Diffs, wait([get_async(Ctx, Key, true) || Key <- Keys]))),

    lists:map(fun
        ({error, {enomem, _Value}}) -> {error, enomem};
        (Other) -> Other
    end, wait(Futures)).

%%--------------------------------------------------------------------
%% @doc
%% Stores values from memory on disc.
%% @end
%%--------------------------------------------------------------------
-spec flush([{key(), ctx()}]) -> [{ok, value()} | {error, term()}].
flush(List) ->
    Futures = [flush_async(Ctx, Key) || {Key, Ctx} <- List],
    lists:map(fun
        ({ok, disc, Value}) -> {ok, Value};
        ({error, Reason}) -> {error, Reason}
    end, wait(Futures)).

%%--------------------------------------------------------------------
%% @doc
%% Stores values from memory on disc.
%% @end
%%--------------------------------------------------------------------
-spec flush(ctx(), key()) -> {ok, value()} | {error, term()};
    (ctx(), [key()]) -> [{ok, value()} | {error, term()}].
flush(Ctx, <<_/binary>> = Key) ->
    hd(flush(Ctx, [Key]));
flush(Ctx, Keys) when is_list(Keys) ->
    lists:map(fun
        ({ok, disc, Value}) -> {ok, Value};
        ({error, Reason}) -> {error, Reason}
    end, wait([flush_async(Ctx, Key) || Key <- Keys])).

%%--------------------------------------------------------------------
%% @doc
%% Marks all values stored in memory as inactive, i.e. all inactivated entries
%% may be removed from cache when its capacity limit is reached.
%% @end
%%--------------------------------------------------------------------
-spec inactivate(ctx()) -> boolean().
inactivate(#{memory_driver := undefined}) ->
    false;
inactivate(#{disc_driver := undefined, mutator_pid := Pid}) ->
    lists:foldl(fun(Pool, Acc) ->
        Acc or datastore_cache_manager:mark_inactive(Pool, Pid)
    end, false, datastore_multiplier:get_names(memory));
inactivate(#{mutator_pid := Pid}) ->
    lists:foldl(fun(Pool, Acc) ->
        Acc or datastore_cache_manager:mark_inactive(Pool, Pid)
    end, false, datastore_multiplier:get_names(disc)).

%%--------------------------------------------------------------------
%% @doc
%% Marks all values stored in memory as inactive, i.e. all inactivated entries
%% may be removed from cache when its capacity limit is reached.
%% @end
%%--------------------------------------------------------------------
-spec inactivate(ctx(), datastore:key(), [datastore:key()]) -> boolean().
inactivate(#{memory_driver := undefined}, _, _) ->
    false;
inactivate(#{disc_driver := undefined}, Key, Keys) ->
    Pool = datastore_multiplier:extend_name(Key, memory),
    datastore_cache_manager:mark_inactive(Pool, Keys);
inactivate(_, Key, Keys) ->
    Pool = datastore_multiplier:extend_name(Key, disc),
    datastore_cache_manager:mark_inactive(Pool, Keys).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asynchronously retrieves value from memory or, if missing and disc fallback
%% is enabled, from disc.
%% @end
%%--------------------------------------------------------------------
-spec get_async(ctx(), key(), boolean()) -> future().
get_async(#{memory_driver := undefined} = Ctx, Key, true) ->
    #{
        disc_driver := DiscDriver,
        disc_driver_ctx := DiscCtx
    } = Ctx,
    ?FUTURE(disc, DiscDriver, DiscDriver:get_async(DiscCtx, Key));
get_async(#{memory_driver := undefined}, _Key, false) ->
    ?FUTURE(memory, undefined, {error, key_enoent});
get_async(#{disc_driver := undefined} = Ctx, Key, _) ->
    #{
        memory_driver := MemoryDriver,
        memory_driver_ctx := MemoryCtx
    } = Ctx,

    case MemoryDriver:get(MemoryCtx, Key) of
        {ok, Value} ->
            ?FUTURE(memory, MemoryDriver, {ok, Value});
        {error, Reason} ->
            ?FUTURE(memory, MemoryDriver, {error, Reason})
    end;
get_async(Ctx, Key, DiscFallback) ->
    #{
        memory_driver := MemoryDriver,
        memory_driver_ctx := MemoryCtx,
        disc_driver := DiscDriver,
        disc_driver_ctx := DiscCtx
    } = Ctx,

    case {MemoryDriver:get(MemoryCtx, Key), DiscFallback} of
        {{ok, Value}, _} ->
            ?FUTURE(memory, MemoryDriver, {ok, Value});
        {{error, key_enoent}, true} ->
            ?FUTURE(disc, DiscDriver, DiscDriver:get_async(DiscCtx, Key));
        {{error, Reason}, _} ->
            ?FUTURE(memory, MemoryDriver, {error, Reason})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asynchronously stores values in memory or, if cache is full and disc fallback
%% is enabled, on disc.
%% @end
%%--------------------------------------------------------------------
-spec save_async(ctx(), value(), boolean()) -> future().
save_async(#{memory_driver := undefined} = Ctx, #document{} = Value, true) ->
    #{
        disc_driver := DiscDriver,
        disc_driver_ctx := DiscCtx
    } = Ctx,
    ?FUTURE(disc, DiscDriver, DiscDriver:save_async(DiscCtx, Value));
save_async(#{memory_driver := undefined}, #document{} = Value, false) ->
    ?FUTURE(memory, undefined, {error, {enomem, Value}});
save_async(#{disc_driver := undefined} = Ctx, #document{key = Key} = Value, _) ->
    #{
        memory_driver := MemoryDriver,
        memory_driver_ctx := MemoryCtx
    } = Ctx,

    Pool = datastore_multiplier:extend_name(Key, memory),
    case datastore_cache_manager:mark_active(Pool, Ctx, Key) of
        true ->
            Result = MemoryDriver:save(MemoryCtx, Value),
            inactivate_volatile(Pool, Ctx, Key),
            ?FUTURE(memory, MemoryDriver, Result);
        false ->
            ?FUTURE(memory, MemoryDriver, {error, {enomem, Value}})
    end;
save_async(Ctx, #document{key = Key} = Value, DiscFallback) ->
    #{
        memory_driver := MemoryDriver,
        memory_driver_ctx := MemoryCtx,
        disc_driver := DiscDriver,
        disc_driver_ctx := DiscCtx
    } = Ctx,

    Pool = datastore_multiplier:extend_name(Key, disc),
    case {datastore_cache_manager:mark_active(Pool, Ctx, Key), DiscFallback} of
        {true, _} ->
            Result = MemoryDriver:save(MemoryCtx, Value),
            inactivate_volatile(Pool, Ctx, Key),
            ?FUTURE(memory, MemoryDriver, Result);
        {false, true} ->
            ?FUTURE(disc, DiscDriver, DiscDriver:save_async(DiscCtx, Value));
        {false, false} ->
            ?FUTURE(memory, MemoryDriver, {error, {enomem, Value}})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asynchronously saves value from memory on disc.
%% @end
%%--------------------------------------------------------------------
-spec flush_async(ctx(), key()) -> future().
flush_async(Ctx, Key) ->
    #{
        memory_driver := MemoryDriver,
        memory_driver_ctx := MemoryCtx,
        disc_driver := DiscDriver,
        disc_driver_ctx := DiscCtx
    } = Ctx,

    case MemoryDriver:get(MemoryCtx, Key) of
        {ok, Value} ->
            ?FUTURE(disc, DiscDriver, DiscDriver:save_async(DiscCtx, Value));
        {error, Reason} ->
            ?FUTURE(memory, MemoryDriver, {error, Reason})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Waits for completion of asynchronous operations.
%% @end
%%--------------------------------------------------------------------
-spec wait(future()) -> {ok, durability(), value()} | {error, term()};
    ([future()]) -> [{ok, durability(), value()} | {error, term()}].
wait(#future{durability = memory, value = {ok, Value}}) ->
    {ok, memory, Value};
wait(#future{value = {error, Reason}}) ->
    {error, Reason};
wait(#future{durability = disc, driver = Driver, value = Ref}) ->
    case Driver:wait(Ref) of
        {ok, _Cas, Value} -> {ok, disc, Value};
        {error, Reason} -> {error, Reason}
    end;
wait(Futures) when is_list(Futures) ->
    [wait(Future) || Future <- Futures].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Inactivates volatile entry.
%% @end
%%--------------------------------------------------------------------
-spec inactivate_volatile(datastore_cache_manager:pool(), ctx(), key()) ->
    boolean().
inactivate_volatile(Pool, Ctx = #{volatile := true}, Key) ->
    CacheKey = couchbase_doc:set_prefix(Ctx, Key),
    datastore_cache_manager:mark_inactive(Pool, CacheKey);
inactivate_volatile(_Pool, _Ctx, _Key) ->
    false.