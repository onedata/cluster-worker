%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides datastore model API for histograms
%%% (mapped to internal datastore API provided by histogram_api).
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_histogram).
-author("Michal Wrzeszcz").

-type ctx() :: datastore_model:ctx().
-type key() :: datastore_model:key().

-export_type([key/0, ctx/0]).

%% API
-export([create/3, update/4, update/5, update_many/3, get/3, get/4, delete/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(ctx(), histogram_api:id(), histogram_api:time_series_config()) -> ok | {error, term()}.
create(Ctx, Id, ConfigMap) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:histogram_operation/4, [?FUNCTION_NAME, [ConfigMap]]).


-spec update(ctx(), histogram_api:id(), histogram_windows:timestamp(),
    histogram_windows:value() | histogram_api:update_range()) -> ok | {error, term()}.
update(Ctx, Id, NewTimestamp, ValueOrUpdateRange) ->
    datastore_model:datastore_apply(Ctx, Id,
        fun datastore:histogram_operation/4, [?FUNCTION_NAME, [NewTimestamp, ValueOrUpdateRange]]).


-spec update(ctx(), histogram_api:id(), histogram_windows:timestamp(), histogram_api:request_range(),
    histogram_windows:value()) -> ok | {error, term()}.
update(Ctx, Id, NewTimestamp, MetricsToUpdate, NewValue) ->
    datastore_model:datastore_apply(Ctx, Id,
        fun datastore:histogram_operation/4, [?FUNCTION_NAME, [NewTimestamp, MetricsToUpdate, NewValue]]).


-spec update_many(ctx(), histogram_api:id(), [{histogram_windows:timestamp(), histogram_windows:value()}]) ->
    ok | {error, term()}.
update_many(Ctx, Id, Measurements) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:histogram_operation/4, [?FUNCTION_NAME, [Measurements]]).


-spec get(ctx(), histogram_api:id(), histogram_windows:get_options()) -> ok | {error, term()}.
get(Ctx, Id, Options) ->
    datastore_model:datastore_apply(Ctx, Id,
        fun datastore:histogram_operation/4, [?FUNCTION_NAME, [Options]]).


-spec get(ctx(), histogram_api:id(), histogram_api:request_range(),
    histogram_windows:get_options()) -> ok | {error, term()}.
get(Ctx, Id, RequestedMetrics, Options) ->
    datastore_model:datastore_apply(Ctx, Id,
        fun datastore:histogram_operation/4, [?FUNCTION_NAME, [RequestedMetrics, Options]]).


-spec delete(ctx(), histogram_api:id()) -> ok | {error, term()}.
delete(Ctx, Id) ->
    datastore_model:datastore_apply(Ctx, Id, fun datastore:histogram_operation/4, [?FUNCTION_NAME, []]).