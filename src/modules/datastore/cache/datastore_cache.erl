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

-include("modules/datastore/datastore_models.hrl").

%% API
-export([get/2, fetch/2, save/1, save/3]).
-export([flush/1, flush/2]).
-export([flush_async/2, wait/1]).
-export([inactivate/1, inactivate/2]).

-record(future, {
    durability :: undefined | durability(),
    driver :: undefined | datastore:driver(),
    value :: {ok, doc()} | {error, term()} | couchbase_pool:future()
}).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type value() :: datastore_doc:value().
-type doc() :: datastore_doc:doc(value()).
-type durability() :: memory | disc.
-type future() :: #future{}.

-export_type([durability/0, future/0]).

-define(FUTURE(Doc), ?FUTURE(undefined, undefined, Doc)).
-define(FUTURE(Durability, Driver, Doc), #future{
    durability = Durability,
    driver = Driver,
    value = Doc
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves values from memory only.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), key()) -> {ok, doc()} | {error, term()};
    (ctx(), [datastore:key()]) -> [{ok, doc()} | {error, term()}].
get(Ctx, <<_/binary>> = Key) ->
    hd(get(Ctx, [Key]));
get(Ctx, Keys) when is_list(Keys) ->
    lists:map(fun
        ({ok, memory, Doc}) -> {ok, Doc};
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
-spec fetch(ctx(), key()) -> {ok, durability(), doc()} | {error, term()};
    (ctx(), [key()]) -> [{ok, durability(), doc()} | {error, term()}].
fetch(Ctx, <<_/binary>> = Key) ->
    hd(fetch(Ctx, [Key]));
fetch(#{memory_driver := MemoryDriver} = Ctx, Keys) when is_list(Keys) ->
    Futures = lists:map(fun
        ({_Key, {ok, memory, #document{value = undefined, deleted = true}}}) ->
            ?FUTURE(memory, MemoryDriver, {error, not_found});
        ({_Key, {ok, memory, Doc}}) ->
            ?FUTURE(memory, MemoryDriver, {ok, Doc});
        ({Key, {ok, disc, Doc}}) ->
            save_async(Ctx, Key, Doc, false);
        ({Key, {error, not_found}}) ->
            Doc = #document{key = Key, value = undefined, deleted = true},
            save_async(Ctx, Key, Doc, false),
            ?FUTURE({error, not_found});
        ({_Key, {error, Reason}}) ->
            ?FUTURE({error, Reason})
    end, lists:zip(Keys, wait([get_async(Ctx, Key, true) || Key <- Keys]))),

    lists:map(fun
        ({ok, memory, Doc}) -> {ok, memory, Doc};
        ({error, {enomem, Doc}}) -> {ok, disc, Doc};
        ({error, Reason}) -> {error, Reason}
    end, wait(Futures)).

%%--------------------------------------------------------------------
%% @doc
%% Stores values in memory or if cache is full on disc.
%% @end
%%--------------------------------------------------------------------
-spec save([{ctx(), key(), doc()}]) ->
    [{ok, durability(), doc()} | {error, term()}].
save(Items) when is_list(Items) ->
    lists:map(fun
        ({error, {enomem, _Doc}}) -> {error, enomem};
        (Other) -> Other
    end, wait([save_async(Ctx, Key, Doc, true) || {Ctx, Key, Doc} <- Items])).

%%--------------------------------------------------------------------
%% @doc
%% Stores values in memory or if cache is full on disc.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), key(), doc()) ->
    {ok, durability(), doc()} | {error, term()}.
save(Ctx, Key, Doc) ->
    hd(save([{Ctx, Key, Doc}])).

%%--------------------------------------------------------------------
%% @doc
%% Stores values from memory on disc.
%% @end
%%--------------------------------------------------------------------
-spec flush([{ctx(), key()}]) -> [{ok, doc()} | {error, term()}].
flush(Items) when is_list(Items) ->
    lists:map(fun
        ({ok, disc, Doc}) -> {ok, Doc};
        ({error, Reason}) -> {error, Reason}
    end, wait([flush_async(Ctx, Key) || {Ctx, Key} <- Items])).

%%--------------------------------------------------------------------
%% @doc
%% Stores value from memory on disc.
%% @end
%%--------------------------------------------------------------------
-spec flush(ctx(), key()) -> {ok, doc()} | {error, term()}.
flush(Ctx, <<_/binary>> = Key) ->
    hd(flush([{Ctx, Key}])).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously saves value from memory on disc.
%% @end
%%--------------------------------------------------------------------
-spec flush_async(ctx(), key()) -> future().
flush_async(#{memory_driver := undefined}, _Key) ->
    ?FUTURE(memory, undefined, {error, not_found});
flush_async(#{disc_driver := undefined}, _Key) ->
    ?FUTURE(disc, undefined, {error, not_found});
flush_async(Ctx, Key) ->
    #{
        memory_driver := MemoryDriver,
        memory_driver_ctx := MemoryCtx,
        disc_driver := DiscDriver,
        disc_driver_ctx := DiscCtx
    } = Ctx,

    case MemoryDriver:get(MemoryCtx, Key) of
        {ok, Doc} ->
            ?FUTURE(disc, DiscDriver, DiscDriver:save_async(DiscCtx, Key, Doc));
        {error, Reason} ->
            ?FUTURE(memory, MemoryDriver, {error, Reason})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Waits for completion of asynchronous operations.
%% @end
%%--------------------------------------------------------------------
-spec wait(future()) -> {ok, durability(), doc()} | {error, term()};
    ([future()]) -> [{ok, durability(), doc()} | {error, term()}].
wait(#future{durability = memory, value = {ok, Doc}}) ->
    {ok, memory, Doc};
wait(#future{value = {error, Reason}}) ->
    {error, Reason};
wait(#future{durability = disc, driver = Driver, value = Ref}) ->
    case Driver:wait(Ref) of
        {ok, _Cas, Doc} -> {ok, disc, Doc};
        {error, Reason} -> {error, Reason}
    end;
wait(Futures) when is_list(Futures) ->
    [wait(Future) || Future <- Futures].

%%--------------------------------------------------------------------
%% @doc
%% Marks values stored in memory by mutator associated with a pid as inactive,
%% i.e. all inactivated entries may be removed from cache when its capacity
%% limit is reached.
%% @end
%%--------------------------------------------------------------------
-spec inactivate(pid()) -> boolean().
inactivate(MutatorPid) when is_pid(MutatorPid) ->
    datastore_cache_manager:mark_inactive(memory, MutatorPid) or
    datastore_cache_manager:mark_inactive(disc, MutatorPid).

%%--------------------------------------------------------------------
%% @doc
%% Marks value stored in memory as inactive, i.e. all inactivated entries
%% may be removed from cache when its capacity limit is reached.
%% @end
%%--------------------------------------------------------------------
-spec inactivate(ctx(), key()) -> boolean().
inactivate(#{memory_driver := undefined}, _Key) ->
    false;
inactivate(#{disc_driver := undefined}, Key) ->
    datastore_cache_manager:mark_inactive(memory, Key);
inactivate(_Ctx, Key) ->
    datastore_cache_manager:mark_inactive(disc, Key).

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
    ?FUTURE(memory, undefined, {error, not_found});
get_async(#{disc_driver := undefined} = Ctx, Key, _) ->
    #{
        memory_driver := MemoryDriver,
        memory_driver_ctx := MemoryCtx
    } = Ctx,

    case MemoryDriver:get(MemoryCtx, Key) of
        {ok, Doc} ->
            ?FUTURE(memory, MemoryDriver, {ok, Doc});
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
        {{ok, Doc}, _} ->
            ?FUTURE(memory, MemoryDriver, {ok, Doc});
        {{error, not_found}, true} ->
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
-spec save_async(ctx(), key(), doc(), boolean()) -> future().
save_async(#{memory_driver := undefined} = Ctx, Key, Doc, true) ->
    #{
        disc_driver := DiscDriver,
        disc_driver_ctx := DiscCtx
    } = Ctx,
    ?FUTURE(disc, DiscDriver, DiscDriver:save_async(DiscCtx, Key, Doc));
save_async(#{memory_driver := undefined}, _Key, Doc, false) ->
    ?FUTURE(memory, undefined, {error, {enomem, Doc}});
save_async(Ctx = #{disc_driver := undefined}, Key, Doc, _) ->
    #{
        memory_driver := MemoryDriver,
        memory_driver_ctx := MemoryCtx
    } = Ctx,

    case datastore_cache_manager:mark_active(memory, Ctx, Key) of
        true ->
            ?FUTURE(memory, MemoryDriver, MemoryDriver:save(MemoryCtx, Key, Doc));
        false ->
            ?FUTURE(memory, MemoryDriver, {error, {enomem, Doc}})
    end;
save_async(Ctx, Key, Doc, DiscFallback) ->
    #{
        memory_driver := MemoryDriver,
        memory_driver_ctx := MemoryCtx,
        disc_driver := DiscDriver,
        disc_driver_ctx := DiscCtx
    } = Ctx,

    case {datastore_cache_manager:mark_active(disc, Ctx, Key), DiscFallback} of
        {true, _} ->
            ?FUTURE(memory, MemoryDriver, MemoryDriver:save(MemoryCtx, Key, Doc));
        {false, true} ->
            ?FUTURE(disc, DiscDriver, DiscDriver:save_async(DiscCtx, Key, Doc));
        {false, false} ->
            ?FUTURE(memory, MemoryDriver, {error, {enomem, Doc}})
    end.