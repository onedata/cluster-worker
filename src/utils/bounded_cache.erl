%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides ets cache that is cleaned automatically when defined size is exceeded (size is checked
%%% periodically).
%%% The caches can form groups which size is calculated together. It is useful as caches can be invalidated separately
%%% while number of slots is fixed regardless the number of caches in group.
%%% The cache stores information using record cache_item where timestamp and timestamp_check fields are used
%%% to verify if there were no races between insert and invalidation. Although ets cleaning is atomic, calculation of
%%% value can take some time. Thus, it is necessary to store timestamp connected with key (invalidation may occur
%%% during calculation of value). TimestampCheck is used to verify race between invalidation and value calculation to
%%% prevent comparing invalidation and calculation timestamps during "get" operations.
%%% @end
%%%-------------------------------------------------------------------
-module(bounded_cache).
-author("Michal Wrzeszcz").

-dialyzer({nowarn_function, [init_cache/2, terminate_cache/1]}). % silence warning about race on ets

%% Basic cache API
-export([get_or_calculate/4, get_or_calculate/5, get/2,
    calculate_and_cache/4, calculate_and_cache/5,
    invalidate/1, get_timestamp/0]).
%% Cache management API
-export([init_group_manager/0, init_cache/2, init_group/2, terminate_cache/1, check_cache_size/1]).

-type cache() :: atom().
-type group() :: binary().
-type key() :: term().
-type value() :: term().
-type additional_info() :: cached | term().
-type callback() :: fun((callback_args()) -> {ok, value(), additional_info()} | {error, term()}).
-type callback_args() :: term().
-type timestamp() :: non_neg_integer().
-type cache_options() :: #{
    size := non_neg_integer(),
    check_frequency := non_neg_integer(),
    group => group()
}.
-type group_options() :: #{
    size := non_neg_integer(),
    check_frequency := non_neg_integer()
}.
-type check_options() :: #{
    size := non_neg_integer(),
    name := cache() | group(),
    check_frequency := non_neg_integer(),
    group => boolean()
}.
-type in_critical_section() :: boolean().

-define(TIMER_MESSAGE(Options), {bounded_cache_timer, Options}).
-define(CACHE_MANAGER, bounded_cache_manager).
-define(CACHE_GROUP_KEY(Cache), {cache_group, Cache}).
-define(INVALIDATION_TIMESTAMP_KEY(Cache), {invalidation_timestamp, Cache}).
-define(GROUP_MEMBERS_KEY(Group), {group_members, Group}).
-define(CREATE_CRITICAL_SECTION(Group), {bounded_cache_group_create, Group}).
-define(INVALIDATE_CRITICAL_SECTION(Cache), {bounded_cache_invalidation, Cache}).
-define(INSERT_CRITICAL_SECTION(Cache, Key), {bounded_cache_insert, Cache, Key}).

-record(cache_item, {
    key :: key(),
    value :: value(),
    timestamp :: timestamp(),
    timestamp_check :: timestamp()
}).

%%%===================================================================
%%% Basic cache API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_or_calculate(Cache, Key, CalculateCallback, Args, false).
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(cache(), key(), callback(), callback_args()) ->
    {ok, value(), additional_info()} | {error, term()}.
get_or_calculate(Cache, Key, CalculateCallback, Args) ->
    get_or_calculate(Cache, Key, CalculateCallback, Args, false).

%%--------------------------------------------------------------------
%% @doc
%% Gets value from cache. If it is not found - uses callback to calculate it.
%% Calculated value is cached.
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(cache(), key(), callback(), callback_args(),
    in_critical_section()) -> {ok, value(), additional_info()} | {error, term()}.
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
%% Gets value from cache. Returns {error, not_found} if value is not found or is invalid.
%% @end
%%--------------------------------------------------------------------
-spec get(cache(), key()) -> {ok, value()} | {error, not_found}.
get(Cache, Key) ->
    case ets:lookup(Cache, Key) of
        [#cache_item{value = Value, timestamp = Timestamp, timestamp_check = Timestamp}] ->
            % No need to check invalidation (timestamp and timestamp_check fields are equal so it was checked on insert).
            {ok, Value};
        [#cache_item{value = Value, timestamp = Timestamp}] ->
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
    case CalculateCallback(Args) of
        {ok, Value, _CalculationInfo} = Ans ->
            % Insert value with timestamp
            case ets:insert_new(Cache, #cache_item{
                key = Key,
                value = Value,
                timestamp = Timestamp,
                timestamp_check = 0
            }) of
                true ->
                    case check_invalidation(Cache, Timestamp) of
                        invalidated ->
                            ok; % value in cache will be invalid without counter update
                        _ ->
                            % Increment timestamp_check field as there was no invalidation.
                            % Use incrementation threshold in case of parallel insert race.
                            % As update_counter fails if there is no particular key, races
                            % with invalidation are prevented.
                            catch ets:update_counter(Cache, Key,
                                {#cache_item.timestamp_check, Timestamp, Timestamp, Timestamp})
                    end;
                _ ->
                    ok
            end,
            Ans;
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes all data in cache. Saves invalidation timestamp for possible races.
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
%% Initializes tmp cache group manager ets. Thus, the process calling this function should be permanent.
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
%% Initializes particular cache and binds it with calling process.
%% @end
%%--------------------------------------------------------------------
-spec init_cache(cache(), cache_options()) -> ok | {error, term()}.
init_cache(Cache, #{group := Group}) ->
    try
        ets:insert(?CACHE_MANAGER, {?CACHE_GROUP_KEY(Cache), Group}),
        ets:new(Cache, [set, public, named_table, {keypos, #cache_item.key}]),
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
        ets:new(Cache, [set, public, named_table, {keypos, #cache_item.key}]),
        send_check_message(Options#{name => Cache}),
        ok
    catch
        _:Reason -> {error, {Reason, erlang:get_stacktrace()}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Initializes particular group and binds it with calling process.
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
-spec terminate_cache(cache()) -> ok.
terminate_cache(Cache) ->
    case ets:lookup(?CACHE_MANAGER, ?CACHE_GROUP_KEY(Cache)) of
        [{?CACHE_GROUP_KEY(Cache), Group}] ->
            ets:delete(?CACHE_MANAGER, ?CACHE_GROUP_KEY(Cache)),
            [{?GROUP_MEMBERS_KEY(Group), Caches}] =
                ets:lookup(?CACHE_MANAGER, ?GROUP_MEMBERS_KEY(Group)),
            ets:insert(?CACHE_MANAGER, {?GROUP_MEMBERS_KEY(Group), Caches -- [Cache]});
        _ ->
            ok
    end,
    ets:delete(Cache),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Verifies if cache should be cleared and clears it if needed.
%% @end
%%--------------------------------------------------------------------
-spec check_cache_size(check_options()) -> ok.
check_cache_size(#{size := Size, group := true, name := Group} = Options) ->
    [{?GROUP_MEMBERS_KEY(Group), Caches}] =
        ets:lookup(?CACHE_MANAGER, ?GROUP_MEMBERS_KEY(Group)),
    SizeSum = lists:foldl(fun(Cache, Sum) ->
        try
            Sum + ets:info(Cache, size)
        catch
            _:badarg -> Sum % cache has been deleted
        end
    end, 0, Caches),

    case SizeSum > Size of
        true ->
            lists:foreach(fun(Cache) ->
                try
                    ets:delete_all_objects(Cache)
                catch
                    _:badarg -> ok % cache has been deleted
                end
            end, Caches);
        _ -> ok
    end,
    send_check_message(Options),
    ok;
check_cache_size(#{size := Size, name := Cache} = Options) ->
    try
        case ets:info(Cache, size) > Size of
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