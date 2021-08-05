%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for management of histogram documents
%%% (getting from and saving to datastore internal structures).
%%%
%%% The module bases on #ctx{} record that stores all data needed to
%%% hide interaction with internal datastore modules/structures.
%%% It has to be finalized with finalize/1 function after last usage to return
%%% structure used by datastore to save all changes.
%%%
%%% The module uses 2 helper records: histogram_hub that stores heads (beginnings)
%%% of each metric and histogram_metric_data that stores windows of single
%%% metrics if there are to many of them to store then all in head
%%% (see histogram_api for head/tail description). There is always exactly one
%%% histogram_hub document (storing heads of all metrics). histogram_metric_data
%%% documents are created on demand and multiple histogram_metric_data documents
%%% can be created for each metric. Key of histogram_hub document is equal to
%%% id of histogram while histogram_metric_data documents have randomly  generated ids.
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_persistence).
-author("Michal Wrzeszcz").

-include("modules/datastore/histogram_internal.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([init_for_new_histogram/4, init_for_existing_histogram/3, finalize/1, set_active_time_series/2, set_active_metric/2,
    get_head_key/1, is_head/2, get/2,
    create/2, update/3, delete/2]).

-record(ctx, {
    datastore_ctx :: datastore_ctx(),
    batch :: batch() | undefined, % Undefined when histogram is used outside tp process
                                  % (call via datastore_reader:histogram_get/3)
    head :: doc(), % TODO head czy hub
    is_head_updated = false :: boolean(), % Field used to determine if head should be saved by finalize/1 function
    % Fields used to determine metric to update
    active_time_series :: histogram_api:time_series_id() | undefined,
    active_metric :: histogram_metric:id() | undefined
}).

-type ctx() :: #ctx{}.
-export_type([ctx/0]).

-type key() :: datastore:key().
-type doc() :: datastore:doc().
-type datastore_ctx() :: datastore:ctx().
-type batch() :: datastore_doc:batch().

%%%===================================================================
%%% API
%%%===================================================================

% TODO - przeniesc tutaj zarzadzanie mapa w hubie

-spec init_for_new_histogram(datastore_ctx(), histogram_api:id(), histogram_api:time_series_map(), batch()) -> ctx().
init_for_new_histogram(DatastoreCtx, Id, TimeSeriesMap, Batch) ->
    HistogramHub = #document{key = Id, value = histogram_hub:set_time_series_map(TimeSeriesMap)},
    {{ok, SavedHistogramHub}, UpdatedBatch} = datastore_doc:save(DatastoreCtx, Id, HistogramHub, Batch),
    #ctx{
        datastore_ctx = DatastoreCtx,
        batch = UpdatedBatch,
        head = SavedHistogramHub
    }.


-spec init_for_existing_histogram(datastore_ctx(), histogram_api:id(), batch() | undefined) ->
    {histogram_api:time_series_map(), ctx()}.
init_for_existing_histogram(DatastoreCtx, Id, Batch) ->
    {{ok, #document{value = HistogramRecord} = HistogramDoc}, UpdatedBatch} =
        datastore_doc:fetch(DatastoreCtx, Id, Batch),
    {
        histogram_hub:get_time_series_map(HistogramRecord),
        #ctx{
            datastore_ctx = DatastoreCtx,
            batch = UpdatedBatch,
            head = HistogramDoc
        }
    }.


-spec finalize(ctx()) ->  batch() | undefined.
finalize(#ctx{is_head_updated = false, batch = Batch}) ->
    Batch;

finalize(#ctx{
    is_head_updated = true,
    head = #document{key = HeadKey} = HeadDoc,
    datastore_ctx = DatastoreCtx,
    batch = Batch
}) ->
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx, HeadKey, HeadDoc, Batch),
    UpdatedBatch.


-spec set_active_time_series(histogram_api:time_series_id(), ctx()) -> ctx().
set_active_time_series(TimeSeriesId, Ctx) ->
    Ctx#ctx{active_time_series = TimeSeriesId}.


-spec set_active_metric(histogram_metric:id(), ctx()) -> ctx().
set_active_metric(MetricsId, Ctx) ->
    Ctx#ctx{active_metric = MetricsId}.


-spec get_head_key(ctx()) -> key().
get_head_key(#ctx{head = #document{key = HeadKey}}) ->
    HeadKey.


-spec is_head(key(), ctx()) -> boolean().
is_head(Key, #ctx{head = #document{key = HeadKey}}) ->
    Key =:= HeadKey.


-spec get(key(), ctx()) -> {histogram_metric:data(), ctx()}.
get(Key, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    {{ok, #document{value = HistogramRecord}}, UpdatedBatch} = datastore_doc:fetch(DatastoreCtx, Key, Batch),
    {histogram_metric_data:get_data(HistogramRecord), Ctx#ctx{batch = UpdatedBatch}}.


-spec create(histogram_metric:data(), ctx()) -> {key(), ctx()}.
create(DataToCreate, #ctx{head = #document{key = HeadKey}, datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    NewDocKey = datastore_key:new_adjacent_to(HeadKey),
    Doc = #document{key = NewDocKey, value = histogram_metric_data:set_data(DataToCreate)},
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx#{generated_key => true}, NewDocKey, Doc, Batch),
    {NewDocKey, Ctx#ctx{batch = UpdatedBatch}}.


-spec update(key(), histogram_metric:data(), ctx()) -> ctx().
update(HeadKey, Data, #ctx{
    head = #document{key = HeadKey, value = HistogramRecord} = HeadDoc,
    active_time_series = TimeSeriesId,
    active_metric = MetricsId
} = Ctx) ->
    TimeSeriesMap = histogram_hub:get_time_series_map(HistogramRecord),
    TimeSeries = maps:get(TimeSeriesId, TimeSeriesMap),
    Metrics = maps:get(MetricsId, TimeSeries),
    UpdatedTimeSeries = TimeSeriesMap#{TimeSeriesId => TimeSeries#{MetricsId => Metrics#metric{data = Data}}},
    UpdatedDoc = HeadDoc#document{value = histogram_hub:set_time_series_map(UpdatedTimeSeries)},
    Ctx#ctx{head = UpdatedDoc, is_head_updated = true};

update(DataDocKey, Data, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    Doc = #document{key = DataDocKey, value = histogram_metric_data:set_data(Data)},
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx, DataDocKey, Doc, Batch),
    Ctx#ctx{batch = UpdatedBatch}.


-spec delete(key(), ctx()) -> ctx().
delete(Key, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    {ok, UpdatedBatch} = datastore_doc:delete(DatastoreCtx, Key, Batch),
    Ctx#ctx{batch = UpdatedBatch}.
