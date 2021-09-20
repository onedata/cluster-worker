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
-export([create/3, update/4, update/5, update_many/3, list_windows/3, list_windows/4, delete/2]).

-type ctx() :: datastore_model:ctx().

%%%===================================================================
%%% API
%%%===================================================================

-spec create(ctx(), time_series_collection:collection_id(), time_series_collection:collection_config()) -> ok | {error, term()}.
create(Ctx, Id, ConfigMap) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4, [?FUNCTION_NAME, [ConfigMap]]).


%%--------------------------------------------------------------------
%% @doc
%% Puts metrics value for particular timestamp for all metrics from all time series or chosen subset
%% of metrics - see time_series_collection:update/5.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), time_series_collection:collection_id(), ts_windows:timestamp(),
    ts_windows:value() | time_series_collection:update_range()) -> ok | {error, term()}.
update(Ctx, Id, NewTimestamp, ValueOrUpdateRange) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4,
        [?FUNCTION_NAME, [NewTimestamp, ValueOrUpdateRange]]).


%%--------------------------------------------------------------------
%% @doc
%% Updates single metric - see time_series_collection:update/6.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), time_series_collection:collection_id(), ts_windows:timestamp(), time_series_collection:request_range(),
    ts_windows:value()) -> ok | {error, term()}.
update(Ctx, Id, NewTimestamp, MetricsToUpdate, NewValue) ->
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
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4, [?FUNCTION_NAME, [Measurements]]).


%%--------------------------------------------------------------------
%% @doc
%% Returns windows for requested ranges. Windows for all metrics from all time series are included in
%% answer - see time_series_collection:list_windows/4.
%% @end
%%--------------------------------------------------------------------
-spec list_windows(ctx(), time_series_collection:collection_id(), ts_windows:list_options()) ->
    time_series_collection:windows_map() | {error, term()}.
list_windows(Ctx, Id, Options) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4, [?FUNCTION_NAME, [Options]]).


%%--------------------------------------------------------------------
%% @doc
%% Returns windows for requested ranges for chosen metrics - see time_series_collection:list_windows/5.
%% @end
%%--------------------------------------------------------------------
-spec list_windows(ctx(), time_series_collection:collection_id(), time_series_collection:request_range(),
    ts_windows:list_options()) -> [ts_windows:window()] | time_series_collection:windows_map() | {error, term()}.
list_windows(Ctx, Id, RequestedMetrics, Options) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4,
        [?FUNCTION_NAME, [RequestedMetrics, Options]]).


-spec delete(ctx(), time_series_collection:collection_id()) -> ok | {error, term()}.
delete(Ctx, Id) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:time_series_collection_operation/4, [?FUNCTION_NAME, []]).