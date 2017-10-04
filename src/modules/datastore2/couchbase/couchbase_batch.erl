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
-include_lib("ctool/include/logging.hrl").

%% API
-export([check_timeout/1, verify_batch_size_increase/3,
    init_counters/0, init_report/0]).

-define(OP_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME,
    couchbase_operation_timeout, 60000)).
-define(DUR_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME,
    couchbase_durability_timeout, 60000)).

-define(EXOMETER_NAME(Param), [batch_stats, Param]).
-define(MIN_BATCH_SIZE_DEFAULT, 50).

%%%===================================================================
%%% API
%%%===================================================================

% TODO - dodac liczenie statystyk dla ilosci procesow (nie tylko tp) oraz
% sredniej wielkosci batcha
init_counters() ->
    TimeSpan = application:get_env(?CLUSTER_WORKER_APP_NAME,
        exometer_batch_time_span, 600000),
    TimeSpan2 = application:get_env(?CLUSTER_WORKER_APP_NAME,
        exometer_timeouts_time_span, 600000),
    init_counter(times, histogram, TimeSpan),
    init_counter(sizes, histogram, TimeSpan),
    init_counter(sizes_config, histogram, TimeSpan),
    init_counter(timeouts, spiral, TimeSpan2).

init_report() ->
    HistogramReport = [min, max, median, mean, n],
    init_report(times, HistogramReport),
    init_report(sizes, HistogramReport),
    init_report(sizes_config, HistogramReport),
    init_report(timeouts, [count]).

init_counter(Param, Type, TimeSpan) ->
    exometer:new(?EXOMETER_NAME(Param), Type, [{time_span, TimeSpan}]).

init_report(Param, Report) ->
    % TODO - manipulacja poziomem logowania
    exometer_report:subscribe(exometer_report_lager, ?EXOMETER_NAME(Param), Report,
        application:get_env(?CLUSTER_WORKER_APP_NAME, exometer_logging_interval, 1000)).

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
% TODO - zle wyspecyfikowany typ
-spec verify_batch_size_increase([couchbase_crud:save_response()], list(), list()) ->
    ok | timeout.
verify_batch_size_increase(Requests, Times, Timeouts) ->
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
            exometer:update(?EXOMETER_NAME(timeouts), 1);
        _ ->
            ok = exometer:update(?EXOMETER_NAME(times),
                round(lists:max(Times)/1000))
    end,

    exometer:update(?EXOMETER_NAME(sizes), maps:size(Requests)),

    {ok, TimesDatapoints} =
        exometer:get_value(?EXOMETER_NAME(times), [max, mean, n]),
    Max = proplists:get_value(max, TimesDatapoints),
    Mean = proplists:get_value(mean, TimesDatapoints),
    Number = proplists:get_value(n, TimesDatapoints),

    {ok, [{count, TimeoutsCount}]} = exometer:get_value(?EXOMETER_NAME(timeouts), [count]),
    {ok, [{mean, Size}]} = exometer:get_value(?EXOMETER_NAME(sizes), [mean]),
    Limit = min(?OP_TIMEOUT, ?DUR_TIMEOUT) / 4,

    MinNum = application:get_env(?CLUSTER_WORKER_APP_NAME,
        min_stats_number_to_reconfigure, 2),
    case Number >= MinNum of
        true ->
            NewValue = round(Limit * Size / Mean),
            MaxBatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_max_batch_size, 2000),
            MinBatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_min_batch_size, ?MIN_BATCH_SIZE_DEFAULT),
            BatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_batch_size, ?MIN_BATCH_SIZE_DEFAULT),
            NewValueFinal = max(MinBatchSize, min(NewValue, MaxBatchSize)),

            ?info("hhhhh ~p", [{NewValue, NewValueFinal, Limit, Size, Mean}]),

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
    MinBatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_min_batch_size, ?MIN_BATCH_SIZE_DEFAULT),
    set_batch_size(MinBatchSize),
    exometer:reset(?EXOMETER_NAME(times)),
    exometer:reset(?EXOMETER_NAME(sizes)),
    ?info("Timeout for batch with ~p elements, reset counters,"
        " decrease batch size to: ~p", [BatchSize, MinBatchSize]).

set_batch_size(Size) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, Size),
    exometer:update(?EXOMETER_NAME(sizes_config), Size),
    ok.