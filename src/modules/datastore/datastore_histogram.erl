%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides datastore model API for histograms
%%% (mapped to internal datastore API provided by histogram_time_series).
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_histogram).
-author("Michal Wrzeszcz").

%% API
-export([create/3, update/4, update/5, update_many/3, get/3, get/4, delete/2]).

-type ctx() :: datastore_model:ctx().

%%%===================================================================
%%% API
%%%===================================================================

-spec create(ctx(), histogram_time_series:id(), histogram_time_series:time_series_config()) -> ok | {error, term()}.
create(Ctx, Id, ConfigMap) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:histogram_operation/4, [?FUNCTION_NAME, [ConfigMap]]).


%%--------------------------------------------------------------------
%% @doc
%% Puts metrics value for particular timestamp for all metrics from all time series or chosen subset
%% of metrics - see histogram_time_series:update/5.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), histogram_time_series:id(), histogram_windows:timestamp(),
    histogram_windows:value() | histogram_time_series:update_range()) -> ok | {error, term()}.
update(Ctx, Id, NewTimestamp, ValueOrUpdateRange) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:histogram_operation/4,
        [?FUNCTION_NAME, [NewTimestamp, ValueOrUpdateRange]]).


%%--------------------------------------------------------------------
%% @doc
%% Updates single metric - see histogram_time_series:update/6.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), histogram_time_series:id(), histogram_windows:timestamp(), histogram_time_series:request_range(),
    histogram_windows:value()) -> ok | {error, term()}.
update(Ctx, Id, NewTimestamp, MetricsToUpdate, NewValue) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:histogram_operation/4,
        [?FUNCTION_NAME, [NewTimestamp, MetricsToUpdate, NewValue]]).


%%--------------------------------------------------------------------
%% @doc
%% Puts multiple measurements to all metrics from all time series.
%% Usage of this function allows reduction of datastore overhead.
%% @end
%%--------------------------------------------------------------------
-spec update_many(ctx(), histogram_time_series:id(), [{histogram_windows:timestamp(), histogram_windows:value()}]) ->
    ok | {error, term()}.
update_many(Ctx, Id, Measurements) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:histogram_operation/4, [?FUNCTION_NAME, [Measurements]]).


%%--------------------------------------------------------------------
%% @doc
%% Returns windows for requested ranges. Windows for all metrics from all time series are included in
%% answer - see histogram_time_series:get/4.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), histogram_time_series:id(), histogram_windows:get_options()) -> ok | {error, term()}.
get(Ctx, Id, Options) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:histogram_operation/4, [?FUNCTION_NAME, [Options]]).


%%--------------------------------------------------------------------
%% @doc
%% Returns windows for requested ranges for chosen metrics - see histogram_time_series:get/5.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), histogram_time_series:id(), histogram_time_series:request_range(),
    histogram_windows:get_options()) -> ok | {error, term()}.
get(Ctx, Id, RequestedMetrics, Options) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:histogram_operation/4,
        [?FUNCTION_NAME, [RequestedMetrics, Options]]).


-spec delete(ctx(), histogram_time_series:id()) -> ok | {error, term()}.
delete(Ctx, Id) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:histogram_operation/4, [?FUNCTION_NAME, []]).