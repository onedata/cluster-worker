%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for time_series_collection module calculating strategy of
%%% documents splitting when any metric has too many windows to be stored in
%%% single datastore document (see #splitting_strategy{} record definition).
%%% @end
%%%-------------------------------------------------------------------
-module(ts_doc_splitting_strategies).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_time_series.hrl").
-include("modules/datastore/ts_metric_config.hrl").
-include("global_definitions.hrl").

%% API
-export([calculate/1]).

-type flat_config_map() :: #{time_series_collection:full_metric_id() => ts_metric:config()}.
-type windows_count_map() :: #{time_series_collection:full_metric_id() => non_neg_integer()}.
-type splitting_strategies_map() :: #{time_series_collection:full_metric_id() => ts_metric:splitting_strategy()}.

-export_type([splitting_strategies_map/0]).

% Warning: do not use this env in app.config (setting it to very high value can result in creation of
% datastore documents that are too big for couchbase). Use of env limited to tests.
-define(MAX_VALUES_IN_DOC, application:get_env(?CLUSTER_WORKER_APP_NAME, time_series_max_doc_size, 50000)).

%%=====================================================================
%% API
%%=====================================================================

-spec calculate(time_series_collection:collection_config()) -> splitting_strategies_map().
calculate(ConfigMap) ->
    FlattenedMap = maps:fold(fun(TimeSeriesId, MetricsConfigs, Acc) ->
        maps:fold(fun
            (_, #metric_config{retention = Retention}, _) when Retention =< 0 ->
                throw({error, empty_metric});
            (_, #metric_config{resolution = 0, retention = Retention}, _) when Retention > 1 ->
                throw({error, wrong_retention});
            (_, #metric_config{resolution = Resolution}, _) when Resolution < 0 ->
                throw({error, wrong_resolution});
            (MetricId, Config, InternalAcc) ->
                InternalAcc#{{TimeSeriesId, MetricId} => Config}
        end, Acc, MetricsConfigs)
    end, #{}, ConfigMap),

    MaxValuesInDoc = ?MAX_VALUES_IN_DOC,
    MaxWindowsInHeadMap = calculate_windows_in_head_doc_count(FlattenedMap),
    maps:map(fun(Key, MaxWindowsInHead) ->
        #metric_config{retention = Retention} = maps:get(Key, FlattenedMap),
        {MaxWindowsInTailDoc, MaxDocsCount} = case MaxWindowsInHead of
            Retention ->
                % All windows can be stored in head
                {0, 1};
            _ ->
                % It is guaranteed that each tail document uses at least half of its capacity after split
                % so windows count should be multiplied by 2 when calculating number of tail documents
                % (head can be cleared to optimize upload so tail documents have to store required windows count).
                case {Retention =< MaxValuesInDoc, Retention =< MaxValuesInDoc div 2} of
                    {true, true} ->
                        {2 * Retention, 2};
                    {true, false} ->
                        {Retention, 3};
                    _ ->
                        {MaxValuesInDoc, 1 + ceil(2 * Retention / MaxValuesInDoc)}
                end
        end,
        #splitting_strategy{
            max_docs_count = MaxDocsCount,
            max_windows_in_head_doc = MaxWindowsInHead,
            max_windows_in_tail_doc = MaxWindowsInTailDoc
        }
    end, MaxWindowsInHeadMap).


%%=====================================================================
%% Internal functions
%%=====================================================================

-spec calculate_windows_in_head_doc_count(flat_config_map()) -> windows_count_map().
calculate_windows_in_head_doc_count(FlattenedMap) ->
    MaxValuesInHead = ?MAX_VALUES_IN_DOC,
    case maps:size(FlattenedMap) > MaxValuesInHead of
        true ->
            throw({error, too_many_metrics});
        false ->
            NotFullyStoredInHead = maps:map(fun(_, _) -> 0 end, FlattenedMap),
            calculate_windows_in_head_doc_count(#{}, NotFullyStoredInHead, MaxValuesInHead, FlattenedMap)

    end.


-spec calculate_windows_in_head_doc_count(windows_count_map(), windows_count_map(), non_neg_integer(),
    flat_config_map()) -> windows_count_map().
calculate_windows_in_head_doc_count(FullyStoredInHead, NotFullyStoredInHead, 0, _FlattenedMap) ->
    maps:merge(FullyStoredInHead, NotFullyStoredInHead);
calculate_windows_in_head_doc_count(FullyStoredInHead, NotFullyStoredInHead, RemainingWindowsInHead, FlattenedMap) ->
    LimitUpdate = max(1, RemainingWindowsInHead div maps:size(NotFullyStoredInHead)),
    {UpdatedFullyStoredInHead, UpdatedNotFullyStoredInHead, UpdatedRemainingWindowsInHead} =
        update_windows_in_head_doc_count(maps:keys(NotFullyStoredInHead), FullyStoredInHead, NotFullyStoredInHead,
            FlattenedMap, LimitUpdate, RemainingWindowsInHead),
    case maps:size(UpdatedNotFullyStoredInHead) of
        0 ->
            UpdatedFullyStoredInHead;
        _ ->
            calculate_windows_in_head_doc_count(UpdatedFullyStoredInHead, UpdatedNotFullyStoredInHead,
                UpdatedRemainingWindowsInHead, FlattenedMap)
    end.


-spec update_windows_in_head_doc_count([time_series_collection:full_metric_id()], windows_count_map(),
    windows_count_map(), flat_config_map(), non_neg_integer(), non_neg_integer()) ->
    {windows_count_map(), windows_count_map(), non_neg_integer()}.
update_windows_in_head_doc_count(_MetricsKeys, FullyStoredInHead, NotFullyStoredInHead, _FlattenedMap, _LimitUpdate, 0) ->
    {FullyStoredInHead, NotFullyStoredInHead, 0};
update_windows_in_head_doc_count([], FullyStoredInHead, NotFullyStoredInHead, _FlattenedMap,
    _LimitUpdate, RemainingWindowsInHead) ->
    {FullyStoredInHead, NotFullyStoredInHead, RemainingWindowsInHead};
update_windows_in_head_doc_count([Key | MetricsKeys], FullyStoredInHead, NotFullyStoredInHead, FlattenedMap,
    LimitUpdate, RemainingWindowsInHead) ->
    Update = min(LimitUpdate, RemainingWindowsInHead),
    #metric_config{retention = Retention} = maps:get(Key, FlattenedMap),
    CurrentLimit = maps:get(Key, NotFullyStoredInHead),
    NewLimit = CurrentLimit + Update,
    {UpdatedFullyStoredInHead, UpdatedNotFullyStoredInHead, FinalUpdate} = case NewLimit >= Retention of
        true ->
            {FullyStoredInHead#{Key => Retention},
                maps:remove(Key, NotFullyStoredInHead), Retention - CurrentLimit};
        false ->
            {FullyStoredInHead, NotFullyStoredInHead#{Key => NewLimit}, Update}
    end,
    update_windows_in_head_doc_count(MetricsKeys, UpdatedFullyStoredInHead, UpdatedNotFullyStoredInHead, FlattenedMap,
        LimitUpdate, RemainingWindowsInHead - FinalUpdate).