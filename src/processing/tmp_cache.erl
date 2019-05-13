%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides ets cache for temporary data.
%%% @end
%%%-------------------------------------------------------------------
-module(tmp_cache).
-author("Michal Wrzeszcz").

%% Basic cache API
-export([get_or_calculate/4, get_or_calculate/5, get/2,
    calculate_and_cache/4, calculate_and_cache/5, calculate_and_cache/6,
    invalidate/1, get_timestamp/0]).
%% Cache management API
-export([init_group_manager/0, init_cache/2, init_group/2,
    terminate_cache/2, check_cache_size/1]).

-type cache() :: atom().
-type group() :: atom().
-type key() :: term().
-type value() :: term().
-type additional_info() :: cached | term().
-type callback() :: fun((term()) -> {ok, value(), additional_info()} | {error, term()}).
-type callback_args() :: term().
-type timestamp() :: non_neg_integer().
-type cache_options() :: #{size := non_neg_integer(), check_frequency := non_neg_integer(),
    group => group()}.
-type group_options() :: #{size := non_neg_integer(), check_frequency := non_neg_integer()}.
-type terminate_options() :: #{group => group()}.
-type check_options() :: #{size := non_neg_integer(), name := cache() | group(),
    check_frequency := non_neg_integer(), group => boolean()}.

-define(TIMER_MESSAGE(Options), {tmp_cache_timer, Options}).
-define(CACHE_MANAGER, tmp_cache_manager).
-define(INVALIDATION_TIMESTAMP_KEY(Cache), {invalidation_timestamp, Cache}).
-define(GROUP_MEMBERS_KEY(Group), {group_members, Group}).
-define(CREATE_CRITICAL_SECTION(Group), {tmp_cache_group_create, Group}).
-define(INVALIDATE_CRITICAL_SECTION(Cache), {tmp_cache_invalidation, Cache}).
-define(INSERT_CRITICAL_SECTION(Cache, Key), {tmp_cache_insert, Cache, Key}).

%%%===================================================================
%%% Basic cache API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Gets value from cache. If it is not found - uses callback to calculate it.
%% Calculated value is cached.
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(cache(), key(), callback(), callback_args()) ->
    {ok, value(), additional_info()} | {error, term()}.
get_or_calculate(Cache, Key, CalculateCallback, Args) ->
    get_or_calculate(Cache, Key, CalculateCallback, Args, false).

get_or_calculate(Cache, Key, CalculateCallback, Args, false) ->
    case get(Cache, Key) of
        {ok, Value} ->
            {ok, Value, cached};
        {error, not_found} ->
            calculate_and_cache(Cache, Key, CalculateCallback, Args)
    end;
get_or_calculate(Cache, Key, CalculateCallback, Args, true) ->
    case get(Cache, Key) of
        {ok, Value} ->
            {ok, Value, cached};
        {error, not_found} ->
            critical_section:run(?INSERT_CRITICAL_SECTION(Cache, Key), fun() ->
                get_or_calculate(Cache, Key, CalculateCallback, Args, false)
            end)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets value from cache.
%% @end
%%--------------------------------------------------------------------
-spec get(cache(), key()) -> {ok, value()} | {error, not_found}.
get(Cache, Key) ->
    case ets:lookup(Cache, Key) of
        [{Key, Value, Timestamp, Timestamp}] ->
            {ok, Value};
        [{Key, Value, Timestamp, _}] ->
            case check_invalidation(Cache, Timestamp) of
                invalidated -> {error, not_found};
                _ -> {ok, Value}
            end;
        [] ->
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv calculate_and_cache(Cache, Key, CalculateCallback, Args, get_timestamp()).
%% @end
%%--------------------------------------------------------------------
-spec calculate_and_cache(cache(), key(), callback(), callback_args()) ->
    {ok, value(), additional_info()} | {error, term()}.
calculate_and_cache(Cache, Key, CalculateCallback, Args) ->
    calculate_and_cache(Cache, Key, CalculateCallback, Args, get_timestamp()).

%%--------------------------------------------------------------------
%% @doc
%% Calculates value and caches it if invalidation has not been done in parallel.
%% @end
%%--------------------------------------------------------------------
-spec calculate_and_cache(cache(), key(), callback(), callback_args(), timestamp()) ->
    {ok, value(), additional_info()} | {error, term()}.
calculate_and_cache(Cache, Key, CalculateCallback, Args, Timestamp) ->
    calculate_and_cache(Cache, Key, CalculateCallback, Args, Timestamp, false).

calculate_and_cache(Cache, Key, CalculateCallback, Args, Timestamp, false) ->
    case CalculateCallback(Args) of
        {ok, Value, _CalculationInfo} = Ans ->
            ets:insert(Cache, {Key, Value, Timestamp, 0}),

            case check_invalidation(Cache, Timestamp) of
                invalidated -> ok; % value in cache will be invalid without counter update
                _ -> catch ets:update_counter(Cache, Key, {4, Timestamp, Timestamp, Timestamp})
            end,

            Ans;
        Other ->
            Other
    end;
calculate_and_cache(Cache, Key, CalculateCallback, Args, Timestamp, true) ->
    critical_section:run(?INSERT_CRITICAL_SECTION(Cache, Key), fun() ->
        calculate_and_cache(Cache, Key, CalculateCallback, Args, Timestamp, false)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Deletes all data in cache.
%% @end
%%--------------------------------------------------------------------
-spec invalidate(cache()) -> ok.
invalidate(Cache) ->
    critical_section:run(?INVALIDATE_CRITICAL_SECTION(Cache), fun() ->
        ets:insert(?CACHE_MANAGER, {?INVALIDATION_TIMESTAMP_KEY(Cache), get_timestamp()}),
        ets:delete_all_objects(Cache),
        ok
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns timestamp in format used by module.
%% @end
%%--------------------------------------------------------------------
-spec get_timestamp() -> timestamp().
get_timestamp() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

%%%===================================================================
%%% Cache management API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes tmp cache group manager ets.
%% @end
%%--------------------------------------------------------------------
-spec init_group_manager() -> ok | {error, term()}.
init_group_manager() ->
    try
        ets:new(?CACHE_MANAGER, [set, public, named_table]),
        ok
    catch
        _:Reason -> {error, {Reason, erlang:get_stacktrace()}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Initializes particular cache and binds it with process.
%% @end
%%--------------------------------------------------------------------
-spec init_cache(cache(), cache_options()) -> ok | {error, term()}.
init_cache(Cache, #{group := Group}) ->
    try
        ets:new(Cache, [set, public, named_table]),
        critical_section:run(?CREATE_CRITICAL_SECTION(Group), fun() ->
            [{?GROUP_MEMBERS_KEY(Group), List}] = ets:lookup(?CACHE_MANAGER,
                ?GROUP_MEMBERS_KEY(Group)),
            ets:insert(?CACHE_MANAGER, {?GROUP_MEMBERS_KEY(Group), [Cache | List]})
        end),
        ok
    catch
        _:Reason -> {error, {Reason, erlang:get_stacktrace()}}
    end;
init_cache(Cache, Options) ->
    try
        ets:new(Cache, [set, public, named_table]),
        send_check_message(Options#{name => Cache}),
        ok
    catch
        _:Reason -> {error, {Reason, erlang:get_stacktrace()}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Initializes particular group and binds it with process.
%% @end
%%--------------------------------------------------------------------
-spec init_group(group(), group_options()) -> ok | {error, term()}.
init_group(Group, Options) ->
    try
        ets:insert(?CACHE_MANAGER, {?GROUP_MEMBERS_KEY(Group), []}),
        send_check_message(Options#{group => true, name => Group}),
        ok
    catch
        _:Reason -> {error, {Reason, erlang:get_stacktrace()}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Terminates particular cache (all data is cleared).
%% @end
%%--------------------------------------------------------------------
-spec terminate_cache(cache(), terminate_options()) -> ok.
terminate_cache(Cache, #{group := Group}) ->
    [{?GROUP_MEMBERS_KEY(Group), List}] =
        ets:lookup(?CACHE_MANAGER, ?GROUP_MEMBERS_KEY(Group)),
    ets:insert(?CACHE_MANAGER, {?GROUP_MEMBERS_KEY(Group), [List] -- [Cache]}),
    ets:delete(Cache),
    ok;
terminate_cache(Cache, _Options) ->
    ets:delete(Cache),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Verified if cache should be cleared.
%% @end
%%--------------------------------------------------------------------
-spec check_cache_size(check_options()) -> ok.
check_cache_size(#{size := Size, group := true, name := Group} = Options) ->
    [{?GROUP_MEMBERS_KEY(Group), List}] =
        ets:lookup(?CACHE_MANAGER, ?GROUP_MEMBERS_KEY(Group)),
    SizeSum = lists:foldl(fun(Cache, Sum) ->
        Sum + ets:info(Cache, size)
    end, 0, List),

    case SizeSum > Size of
        % TODO - jesli delete all objects byloby wolne to mozna skasowac caly ets i stworzyc od nowa (i dac catch na operacje)
        true ->
            lists:foreach(fun(Cache) ->
                ets:delete_all_objects(Cache)
            end, List);
        _ -> ok
    end,
    send_check_message(Options),
    ok;
check_cache_size(#{size := Size, name := Cache} = Options) ->
    try
        case ets:info(Cache, size) > Size of
            % TODO - jesli delete all objects byloby wolne to mozna skasowac caly ets i stworzyc od nowa (i dac catch na operacje)
            true -> ets:delete_all_objects(Cache);
            _ -> ok
        end,
        send_check_message(Options)
    catch
        _:badarg -> ok % cache has been deleted
    end,
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec send_check_message(check_options()) -> reference().
send_check_message(#{check_frequency := Frequency} = Options) ->
    case Options of
        #{worker := true} ->
            erlang:send_after(Frequency, self(), {sync_timer, ?TIMER_MESSAGE(Options)});
        _ ->
            erlang:send_after(Frequency, self(), ?TIMER_MESSAGE(Options))
    end.

-spec check_invalidation(cache(), timestamp()) -> ok | invalidated.
check_invalidation(Cache, Timestamp) ->
    case ets:lookup(?CACHE_MANAGER, ?INVALIDATION_TIMESTAMP_KEY(Cache)) of
        [] -> ok;
        [{_, InvalidationTimestamp}] when Timestamp > InvalidationTimestamp -> ok;
        _ -> invalidated
    end.