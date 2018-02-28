%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functionality for couchbase batch size management.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_batch).
-author("Michał Wrzeszcz").

-include("global_definitions.hrl").
-include("exometer_utils.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([check_timeout/1, verify_batch_size_increase/3, init_counters/0, init_report/0]).
% for eunit
-export([decrease_batch_size/1]).

-define(OP_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME,
    couchbase_operation_timeout, 60000)).
-define(DUR_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME,
    couchbase_durability_timeout, 60000)).

-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE, Param)).
-define(EXOMETER_DEFAULT_TIME_SPAN, 10000).

-define(MIN_BATCH_SIZE_DEFAULT, 10).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes exometer counters used by this module.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
    TimeSpan = application:get_env(?CLUSTER_WORKER_APP_NAME,
        exometer_batch_time_span, ?EXOMETER_DEFAULT_TIME_SPAN),
    TimeSpan2 = application:get_env(?CLUSTER_WORKER_APP_NAME,
        exometer_timeouts_time_span, ?EXOMETER_DEFAULT_TIME_SPAN),

    Counters = [
        {?EXOMETER_NAME(times), histogram, TimeSpan},
        {?EXOMETER_NAME(sizes), histogram, TimeSpan},
        {?EXOMETER_NAME(timeouts), spiral, TimeSpan2}
    ],
    exometer_utils:init_counters(Counters),

    Counters2 = [
        {?EXOMETER_NAME(timeouts_history), counter},
        {?EXOMETER_NAME(sizes_config), histogram, TimeSpan}
    ],
    ?init_counters(Counters2).

%%--------------------------------------------------------------------
%% @doc
%% Sets exometer report connected with counters used by this module.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    HistogramReport = [min, max, median, mean, n],

    Reports = [
        {?EXOMETER_NAME(times), HistogramReport},
        {?EXOMETER_NAME(sizes), HistogramReport},
        {?EXOMETER_NAME(sizes_config), HistogramReport},
        {?EXOMETER_NAME(timeouts), [count]},
        {?EXOMETER_NAME(timeouts_history), [value]}
    ],
    ?init_reports(Reports).

%%--------------------------------------------------------------------
%% @doc
%% Checks if timeout appears in the response and changes batch size if needed.
%% @end
%%--------------------------------------------------------------------
-spec check_timeout([couchbase_crud:delete_response()]
    | [couchbase_crud:get_response()] | [couchbase_crud:save_response()]) ->
    ok | timeout.
check_timeout(Responses) ->
    Check = lists:foldl(fun
        ({_Key, {error, etimedout}}, _) ->
            timeout;
        ({_Key, {error, timeout}}, _) ->
            timeout;
        (_, TmpAns) ->
            TmpAns
    end, ok, Responses),

    case Check of
        timeout ->
            decrease_batch_size(length(Responses)),
            timeout;
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if batch size can be increased and increases it if needed.
%% @end
%%--------------------------------------------------------------------
-spec verify_batch_size_increase(couchbase_crud:save_requests_map(),
    list(), list()) -> ok.
verify_batch_size_increase(Requests, Times, Timeouts) ->
    try
        Check = lists:foldl(fun
            (_, false) ->
                false;
            (_, timeout) ->
                timeout;
            ({_, timeout}, _Acc) ->
                timeout;
            ({T, _}, _Acc) ->
                T =< (min(?OP_TIMEOUT, ?DUR_TIMEOUT) / 4)
        end, true, lists:zip(Times, Timeouts)),

        case Check of
            timeout ->
                ok = exometer_utils:update_counter(?EXOMETER_NAME(timeouts)),
                ok = ?update_counter(?EXOMETER_NAME(timeouts_history));
            _ ->
                ok = exometer_utils:update_counter(?EXOMETER_NAME(times),
                    round(lists:max(Times)/1000))
        end,

        ok = exometer_utils:update_counter(?EXOMETER_NAME(sizes), maps:size(Requests)),

        {ok, TimesDatapoints} =
            exometer_utils:get_value(?EXOMETER_NAME(times), [max, mean]),
        Max = proplists:get_value(max, TimesDatapoints),
        Mean = proplists:get_value(mean, TimesDatapoints),

        {ok, [{count, TimeoutsCount}]} = exometer_utils:get_value(?EXOMETER_NAME(timeouts), [count]),
        {ok, [{mean, Size}]} = exometer_utils:get_value(?EXOMETER_NAME(sizes), [mean]),
        Limit = min(?OP_TIMEOUT, ?DUR_TIMEOUT) / 4,

        case Mean > 0 of
            true ->
                NewValue = round(Limit * Size / Mean),
                MaxBatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
                    couchbase_pool_max_batch_size, 2000),
                MinBatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
                    couchbase_pool_min_batch_size, ?MIN_BATCH_SIZE_DEFAULT),
                BatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
                    couchbase_pool_batch_size, ?MIN_BATCH_SIZE_DEFAULT),
                NewValueFinal = max(MinBatchSize, min(NewValue, MaxBatchSize)),

                case {NewValueFinal > BatchSize, (Max < Limit) and (TimeoutsCount == 0)} of
                    {true, true} ->
                        set_batch_size(NewValueFinal);
                    {false, _} ->
                        set_batch_size(NewValueFinal);
                    _ ->
                        ok
                end;
            _ ->
                ok
        end
    catch
        E1:E2 ->
            ?error_stacktrace("Error during reconfiguration of couchbase "
            "batch size: ~p:~p", [E1, E2])
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Decreases batch size as a result of timeout.
%% @end
%%--------------------------------------------------------------------
-spec decrease_batch_size(non_neg_integer()) -> ok.
decrease_batch_size(BatchSize) ->
    try
        MinBatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
            couchbase_pool_min_batch_size, ?MIN_BATCH_SIZE_DEFAULT),
        set_batch_size(MinBatchSize),
        exometer_utils:reset(?EXOMETER_NAME(times)),
        exometer_utils:reset(?EXOMETER_NAME(sizes)),
        ?info("Timeout for batch with ~p elements, reset counters,"
        " decrease batch size to: ~p, stacktrance ~p", [BatchSize, MinBatchSize,
            erlang:process_info(self(), current_stacktrace)])
    catch
        E1:E2 ->
            ?error_stacktrace("Error during decrease of couchbase"
            "batch size: ~p:~p", [E1, E2])
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets batch size and updates exometer.
%% @end
%%--------------------------------------------------------------------
-spec set_batch_size(non_neg_integer()) -> ok.
set_batch_size(Size) ->
    try
        application:set_env(?CLUSTER_WORKER_APP_NAME,
            couchbase_pool_batch_size, Size)
    catch
        _:_ ->
            ok % can fail when application is stopping
    end,
    ok = ?update_counter(?EXOMETER_NAME(sizes_config), Size).
