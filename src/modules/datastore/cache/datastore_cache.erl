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

-include("exometer_utils.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get/2, get/3, fetch/2, get_remote/2, save/1, save/3]).
-export([flush/1, flush/2]).
-export([flush_async/2, wait/1]).
-export([inactivate/1, inactivate/2]).
-export([init_counters/0, init_report/0]).

%% For RPC
-export([save_memory_copy/4]).

-record(future, {
    durability :: undefined | durability(),
    driver :: undefined | datastore:driver(),
    value :: {ok, doc()} | {error, term()} | couchbase_pool:future()
}).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type value() :: datastore_doc:value().
-type doc() :: datastore_doc:doc(value()).
-type durability() :: memory | disc | remote.
-type future() :: #future{}.
-type cache_save_request() :: {ctx(), key(), doc()}.

-export_type([durability/0, future/0, ctx/0, cache_save_request/0]).

-define(FUTURE(Value), ?FUTURE(undefined, Value)).
-define(FUTURE(Durability, Value), ?FUTURE(Durability, undefined, Value)).
-define(FUTURE(Durability, Driver, Value), #future{
    durability = Durability,
    driver = Driver,
    value = Value
}).

-define(EXOMETER_COUNTERS,
        [get, fetch, save, flush]). 


-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE, Param)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes all counters.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
    Counters = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), counter}
    end, ?EXOMETER_COUNTERS),
    ?init_counters(Counters).

%%--------------------------------------------------------------------
%% @doc
%% Subscribe for reports for all parameters.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    Reports = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), [value]}
    end, ?EXOMETER_COUNTERS),
    ?init_reports(Reports).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves values from memory only.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), key()) -> {ok, doc()} | {error, term()};
    (ctx(), [datastore:key()]) -> [{ok, doc()} | {error, term()}].
get(Ctx, Keys) ->
    get(Ctx, Keys, false).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves values.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), key(), boolean()) -> {ok, doc()} | {error, term()};
    (ctx(), [datastore:key()], boolean()) -> [{ok, doc()} | {error, term()}].
get(Ctx, <<_/binary>> = Key, DiscFallback) ->
    hd(get(Ctx, [Key], DiscFallback));
get(Ctx, Keys, DiscFallback) when is_list(Keys) ->
    ?update_datastore_counter(?EXOMETER_NAME(cache_get), length(Keys)),
    lists:map(fun
        ({ok, _, Doc}) -> {ok, Doc};
        ({error, Reason}) -> {error, Reason}
    end, wait([get_async(Ctx, Key, DiscFallback) || Key <- Keys])).

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
fetch(Ctx, Keys) when is_list(Keys) ->
    ?update_datastore_counter(?EXOMETER_NAME(cache_fetch), length(Keys)),
    Results = fetch_local_or_remote(Ctx, Keys),
    Results2 = cache_disc_or_remote_results(Ctx, Keys, Results),

    lists:map(fun
        ({ok, _Durability, #document{value = undefined, deleted = true}}) ->
            {error, not_found};
        ({ok, Durability, Doc}) ->
            {ok, Durability, Doc};
        ({error, {enomem, Doc}}) ->
            {ok, disc, Doc};
        ({error, Reason}) ->
            {error, Reason}
    end, Results2).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves values from remote store.
%% @end
%%--------------------------------------------------------------------
-spec get_remote(ctx(), key()) -> {ok, doc()} | {error, term()}.
get_remote(Ctx, Key) ->
    Future = get_remote_async(Ctx, Key),
    case wait(Future) of
        ({ok, _, Doc}) -> {ok, Doc};
        ({error, Reason}) -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Stores values in memory or if cache is full on disc.
%% @end
%%--------------------------------------------------------------------
-spec save([cache_save_request()]) -> [{ok, durability(), doc()} | {error, term()}].
save(Items) when is_list(Items) ->
    ?update_datastore_counter(?EXOMETER_NAME(cache_save), length(Items)),
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
    ?update_datastore_counter(?EXOMETER_NAME(cache_flush), length(Items)),
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
    ?FUTURE(memory, undefined, {error, memory_driver_undefined});
flush_async(#{disc_driver := undefined}, _Key) ->
    ?FUTURE(disc, undefined, {error, disc_driver_undefined});
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
wait(#future{durability = Durability, value = {ok, Doc = #document{}}}) ->
    {ok, Durability, Doc};
wait(#future{value = {error, Reason}}) ->
    {error, Reason};
wait(#future{durability = Durability, driver = Driver, value = Value}) ->
    case Driver:wait(Value) of
        {ok, Doc} -> {ok, Durability, Doc};
        {ok, _Cas, Doc} -> {ok, Durability, Doc};
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
-spec inactivate(map()) -> boolean().
inactivate(KeysMap) ->
    lists:foreach(fun
        ({K, #{disc_driver := DD}}) when DD =/= undefined ->
            Pool = datastore_multiplier:extend_name(K, disc),
            datastore_cache_manager:mark_inactive(Pool, K);
        ({K, _Ctx}) ->
            Pool = datastore_multiplier:extend_name(K, memory),
            datastore_cache_manager:mark_inactive(Pool, K)
    end, maps:to_list(KeysMap)),
    true.

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
    Pool = datastore_multiplier:extend_name(Key, memory),
    datastore_cache_manager:mark_inactive(Pool, Key);
inactivate(_Ctx, Key) ->
    Pool = datastore_multiplier:extend_name(Key, disc),
    datastore_cache_manager:mark_inactive(Pool, Key).

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
        {ok, Doc} -> ?FUTURE(memory, MemoryDriver, {ok, Doc});
        {error, Reason} -> ?FUTURE(memory, MemoryDriver, {error, Reason})
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
%% Asynchronously retrieves value from remote store.
%% @end
%%--------------------------------------------------------------------
-spec get_remote_async(ctx(), key()) -> future().
get_remote_async(#{remote_driver := undefined}, _Key) ->
    ?FUTURE({error, not_found});
get_remote_async(#{
    remote_driver := RemoteDriver,
    remote_driver_ctx := RemoteCtx
}, Key) ->
    ?FUTURE(remote, RemoteDriver, RemoteDriver:get_async(RemoteCtx, Key));
get_remote_async(_Ctx, _Key) ->
    ?FUTURE({error, not_found}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fetches documents from local store and fallbacks to remote store for missing
%% ones.
%% @end
%%--------------------------------------------------------------------
-spec fetch_local_or_remote(ctx(), [key()]) ->
    [{ok, durability(), doc()} | {error, term()}].
fetch_local_or_remote(Ctx, Keys) ->
    Futures = lists:map(fun
        ({_Key, {ok, Durability, Doc}}) ->
            ?FUTURE(Durability, {ok, Doc});
        ({Key, {error, not_found}}) ->
            get_remote_async(Ctx, Key);
        ({_Key, {error, Reason}}) ->
            ?FUTURE({error, Reason})
    end, lists:zip(Keys, wait([get_async(Ctx, Key, true) || Key <- Keys]))),

    wait(Futures).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to save documents fetched from disc or remote store in memory cache.
%% @end
%%--------------------------------------------------------------------
-spec cache_disc_or_remote_results(ctx(), [key()],
    [{ok, durability(), doc()} | {error, term()}]) ->
    [{ok, durability(), doc()} | {error, term()}].
cache_disc_or_remote_results(#{disc_driver := DD} = Ctx, Keys, Results) when DD =/= undefined ->
    Futures = lists:map(fun
        ({Key, {ok, memory, Doc}}) ->
            save_memory_copies(Ctx, Key, Doc, disc),
            ?FUTURE(memory, {ok, Doc});
        ({Key, {ok, disc, Doc}}) ->
            save_async(Ctx, Key, Doc, false, true);
        ({Key, {ok, remote, Doc}}) ->
            save_async(Ctx, Key, Doc, true, true);
        ({Key, {error, not_found}}) ->
            Doc = #document{key = Key, value = undefined, deleted = true},
            save_async(Ctx, Key, Doc, false, true),
            ?FUTURE({error, not_found});
        ({_Key, {error, Reason}}) ->
            ?FUTURE({error, Reason})
    end, lists:zip(Keys, Results)),

    wait(Futures);
cache_disc_or_remote_results(#{memory_driver := MD} = Ctx, Keys, Results) when MD =/= undefined ->
    lists:map(fun
        ({Key, {ok, memory, Doc} = Result}) ->
            save_memory_copies(Ctx, Key, Doc, memory),
            Result;
        ({Key, {ok, remote, Doc}}) ->
            wait(save_async(Ctx, Key, Doc, false, true));
        ({Key, {error, not_found} = Result}) ->
            Doc = #document{key = Key, value = undefined, deleted = true},
            save_memory_copies(Ctx, Key, Doc, memory),
            Result;
        ({_Key, Result}) ->
            Result
    end, lists:zip(Keys, Results));
cache_disc_or_remote_results(_Ctx, _Keys, Results) ->
    Results.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asynchronously stores values in memory or, if cache is full and disc fallback
%% is enabled, on disc.
%% @end
%%--------------------------------------------------------------------
-spec save_async(ctx(), key(), doc(), boolean()) -> future().
save_async(Ctx, Key, Doc, DiscFallback) ->
    save_async(Ctx, Key, Doc, DiscFallback, false).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asynchronously stores values in memory or, if cache is full and disc fallback
%% is enabled, on disc.
%% @end
%%--------------------------------------------------------------------
-spec save_async(ctx(), key(), doc(), boolean(), boolean()) -> future().
save_async(#{memory_driver := undefined} = Ctx, Key, Doc, true, _) ->
    #{
        disc_driver := DiscDriver,
        disc_driver_ctx := DiscCtx
    } = Ctx,
    ?FUTURE(disc, DiscDriver, DiscDriver:save_async(DiscCtx, Key, Doc));
save_async(#{memory_driver := undefined}, _Key, Doc, false, _) ->
    ?FUTURE(memory, undefined, {error, {enomem, Doc}});
save_async(Ctx = #{disc_driver := undefined}, Key, Doc, _, _) ->
    #{
        memory_driver := MemoryDriver,
        memory_driver_ctx := MemoryCtx
    } = Ctx,

    Pool = datastore_multiplier:extend_name(Key, memory),
    case datastore_cache_manager:mark_active(Pool, Ctx, Key) of
        true ->
            save_memory_copies(Ctx, Key, Doc, memory),
            ?FUTURE(memory, MemoryDriver, MemoryDriver:save(MemoryCtx, Key, Doc));
        false ->
            ?FUTURE(memory, MemoryDriver, {error, {enomem, Doc}})
    end;
save_async(Ctx, Key, Doc, DiscFallback, Inactivate) ->
    #{
        memory_driver := MemoryDriver,
        memory_driver_ctx := MemoryCtx,
        disc_driver := DiscDriver,
        disc_driver_ctx := DiscCtx
    } = Ctx,

    Pool = datastore_multiplier:extend_name(Key, disc),
    case {datastore_cache_manager:mark_active(Pool, Ctx, Key), DiscFallback} of
        {true, _} ->
            Ans = ?FUTURE(memory, MemoryDriver, MemoryDriver:save(MemoryCtx, Key, Doc)),
            case Inactivate of
              true ->
                datastore_cache_manager:mark_inactive(Pool, Key);
              _ ->
                ok
            end,
            save_memory_copies(Ctx, Key, Doc, disc),
            Ans;
        {false, true} ->
            ?FUTURE(disc, DiscDriver, DiscDriver:save_async(DiscCtx, Key, Doc));
        {false, false} ->
            ?FUTURE(memory, MemoryDriver, {error, {enomem, Doc}})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves copy to memory of nodes listed in ctx.
%% @end
%%--------------------------------------------------------------------
-spec save_memory_copies(ctx(), key(), doc(),datastore_cache_manager:pool_type()) -> ok.
save_memory_copies(#{routing := local}, _Key, _Doc, _PoolType) ->
    ok;
save_memory_copies(#{memory_copies_nodes := Nodes} = Ctx, Key, Doc, PoolType) ->
    Pool = datastore_multiplier:extend_name(Key, PoolType),
    {Ans, BadNodes} = FullAns = rpc:multicall(Nodes, ?MODULE, save_memory_copy, [Ctx, Key, Doc, Pool]),
    Errors = lists:filter(fun
        ({ok, _}) -> false;
        (_) -> true
    end, Ans),
    case {Errors, BadNodes} of
        {[], []} ->
            ok;
        _ ->
            ?error("Error saving memory copies for key: ~p~ndoc: ~p~nnodes ~p~nrpc ans: ~p", [Key, Doc, Nodes, FullAns])
    end;
save_memory_copies(_Ctx, _Key, _Doc, _PoolType) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves copy to memory of node.
%% @end
%%--------------------------------------------------------------------
-spec save_memory_copy(ctx(), key(), doc(), datastore_cache_manager:pool()) -> {ok, doc()} | {error, term()}.
save_memory_copy(#{
    memory_driver := MemoryDriver,
    memory_driver_ctx := MemoryCtx
} = Ctx, Key, Doc, Pool) ->
    case datastore_cache_manager:mark_active(Pool, Ctx, Key) of
        true ->
            Ans = MemoryDriver:save(MemoryCtx, Key, Doc),
            datastore_cache_manager:mark_inactive(Pool, Key, fun(_) -> true end),
            Ans;
        false ->
            {error, {enomem, Doc}}
    end.