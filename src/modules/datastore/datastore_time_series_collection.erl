%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides datastore model API for time series collections
%%% (mapped to internal datastore API provided by time_series_collection module).
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_time_series_collection).
-author("Michal Wrzeszcz").

%% API
-export([create/3, delete/2,
    add_metrics/4, delete_metrics/3,
    list_time_series_ids/2, list_metrics_by_time_series/2,
    update/4, check_and_update/4, update/5, check_and_update/5, update_many/3,
    list_windows/3, list_windows/4]).

-type ctx() :: datastore_model:ctx().

%%%===================================================================
%%% API
%%%===================================================================

-spec create(ctx(), time_series_collection:collection_id(), time_series_collection:collection_config()) ->
    ok | {error, term()}.
create(Ctx, Id, ConfigMap) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4,
        [?FUNCTION_NAME, [ConfigMap]]).


-spec delete(ctx(), time_series_collection:collection_id()) -> ok | {error, term()}.
delete(Ctx, Id) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4, [?FUNCTION_NAME, []]).


-spec add_metrics(ctx(), time_series_collection:collection_id(), time_series_collection:collection_config(),
    time_series_collection:add_metrics_option()) -> ok | {error, term()}.
add_metrics(Ctx, Id, ConfigMapExtension, Options) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4,
        [?FUNCTION_NAME, [ConfigMapExtension, Options]]).


-spec delete_metrics(ctx(), time_series_collection:collection_id(), time_series_collection:request_range()) ->
    ok | {error, term()}.
delete_metrics(Ctx, Id, MetricsToDelete) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4,
        [?FUNCTION_NAME, [MetricsToDelete]]).


-spec list_time_series_ids(ctx(), time_series_collection:collection_id()) ->
    {ok, [time_series_collection:time_series_id()]} | {error, term()}.
list_time_series_ids(Ctx, Id) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4, [?FUNCTION_NAME, []]).


-spec list_metrics_by_time_series(ctx(), time_series_collection:collection_id()) ->
    {ok, time_series_collection:metrics_by_time_series()} | {error, term()}.
list_metrics_by_time_series(Ctx, Id) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4, [?FUNCTION_NAME, []]).


%%--------------------------------------------------------------------
%% @doc
%% Puts metrics value for particular timestamp for all metrics from all time series or chosen subset
%% of metrics - see time_series_collection:update/5. If updated metric or time series is missing,
%% the function ignores it and updates only existing ones.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), time_series_collection:collection_id(), ts_windows:timestamp(),
    ts_windows:value() | time_series_collection:update_range()) -> ok | {error, term()}.
update(Ctx, Id, NewTimestamp, ValueOrUpdateRange) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4,
        [?FUNCTION_NAME, [NewTimestamp, ValueOrUpdateRange]]).


%%--------------------------------------------------------------------
%% @doc
%% Puts metrics value for particular timestamp for all metrics from all time series or chosen subset
%% of metrics - see time_series_collection:check_and_update/5. If updated metric or time series is missing,
%% the function returns error.
%% @end
%%--------------------------------------------------------------------
-spec check_and_update(ctx(), time_series_collection:collection_id(), ts_windows:timestamp(),
    ts_windows:value() | time_series_collection:update_range()) -> ok | {error, term()}.
check_and_update(Ctx, Id, NewTimestamp, ValueOrUpdateRange) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4,
        [?FUNCTION_NAME, [NewTimestamp, ValueOrUpdateRange]]).


%%--------------------------------------------------------------------
%% @doc
%% Puts metrics value for particular timestamp. Updated value is the same for all metrics provided in 4th argument - see
%% time_series_collection:update/6. If updated metric or time series is missing, the function ignores it and updates
%% only existing ones.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), time_series_collection:collection_id(), ts_windows:timestamp(),
    time_series_collection:request_range(), ts_windows:value()) -> ok | {error, term()}.
update(Ctx, Id, NewTimestamp, MetricsToUpdate, NewValue) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4,
        [?FUNCTION_NAME, [NewTimestamp, MetricsToUpdate, NewValue]]).


%%--------------------------------------------------------------------
%% @doc
%% Puts metrics value for particular timestamp. Updated value is the same for all metrics provided in 4th argument - see
%% time_series_collection:check_and_update/6. If updated metric or time series is missing, the function returns error.
%% @end
%%--------------------------------------------------------------------
-spec check_and_update(ctx(), time_series_collection:collection_id(), ts_windows:timestamp(),
    time_series_collection:request_range(), ts_windows:value()) -> ok | {error, term()}.
check_and_update(Ctx, Id, NewTimestamp, MetricsToUpdate, NewValue) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4,
        [?FUNCTION_NAME, [NewTimestamp, MetricsToUpdate, NewValue]]).


%%--------------------------------------------------------------------
%% @doc
%% Puts multiple measurements to all metrics from all time series.
%% Usage of this function allows reduction of datastore overhead.
%% @end
%%--------------------------------------------------------------------
-spec update_many(ctx(), time_series_collection:collection_id(), [{ts_windows:timestamp(), ts_windows:value()}]) ->
    ok | {error, term()}.
update_many(Ctx, Id, Measurements) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4,
        [?FUNCTION_NAME, [Measurements]]).


%%--------------------------------------------------------------------
%% @doc
%% Returns windows for requested ranges. Windows for all metrics from all time series are included in
%% answer - see time_series_collection:list_windows/4.
%% @end
%%--------------------------------------------------------------------
-spec list_windows(ctx(), time_series_collection:collection_id(), ts_windows:list_options()) ->
    {ok, time_series_collection:windows_map()} | {error, term()}.
list_windows(Ctx, Id, Options) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4,
        [?FUNCTION_NAME, [Options]]).


%%--------------------------------------------------------------------
%% @doc
%% Returns windows for requested ranges for chosen metrics - see time_series_collection:list_windows/5.
%% @end
%%--------------------------------------------------------------------
-spec list_windows(ctx(), time_series_collection:collection_id(), time_series_collection:request_range(),
    ts_windows:list_options()) ->
    {ok, ts_windows:descending_windows_list() | time_series_collection:windows_map()} | {error, term()}.
list_windows(Ctx, Id, RequestedMetrics, Options) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4,
        [?FUNCTION_NAME, [RequestedMetrics, Options]]).