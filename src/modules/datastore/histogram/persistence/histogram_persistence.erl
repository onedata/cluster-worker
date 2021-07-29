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
%%% The module base on #ctx{} record that stores all data needed to
%%% hide interaction with internal datastore modules/structures.
%%% #ctx{} has created with new/4 function when histogram does not exist
%%% or initialized with init/3 function for existing histograms.
%%% It has to finalized with finalize/1 function after last usage to return
%%% structure used by datastore to save all changes.
%%%
%%% The module uses 2 helper records: histogram_hub that stores beginnings
%%% of each metrics and histogram_tail_node that stores windows of single
%%% metrics if there are to many of them to store then all in histogram_hub.
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_persistence).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([new/4, init/3, finalize/1, set_active_time_series/2, set_active_metrics/2,
    get_head_key/1, is_head/2, get/2,
    create/2, update/3, delete/2]).

-record(ctx, {
    datastore_ctx :: datastore_ctx(),
    batch :: batch() | undefined, % Undefined when histogram is used outside tp process
                                  % (call via datastore_reader:histogram_get/3)
    head :: doc(),
    head_updated = false :: boolean(), % Field used to determine if head should be saved by finalize/1 function
    % Fields used to determine metrics to update
    active_time_series :: histogram_api:time_series_id() | undefined,
    active_metrics :: histogram_api:metrics_id() | undefined
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

-spec new(datastore_ctx(), histogram_api:id(), histogram_api:time_series_map(), batch()) -> ctx().
new(DatastoreCtx, Id, TimeSeries, Batch) ->
    HistogramDoc = #document{key = Id, value = histogram_hub:set_time_series(TimeSeries)},
    {{ok, SavedHistogramDoc}, UpdatedBatch} = datastore_doc:save(DatastoreCtx, Id, HistogramDoc, Batch),
    #ctx{
        datastore_ctx = DatastoreCtx,
        batch = UpdatedBatch,
        head = SavedHistogramDoc
    }.


-spec init(datastore_ctx(), histogram_api:id(), batch() | undefined) ->
    {histogram_api:time_series_map(), ctx()}.
init(DatastoreCtx, Id, Batch) ->
    {{ok, #document{value = HistogramRecord} = HistogramDoc}, UpdatedBatch} =
        datastore_doc:fetch(DatastoreCtx, Id, Batch),
    {
        histogram_hub:get_time_series(HistogramRecord),
        #ctx{
            datastore_ctx = DatastoreCtx,
            batch = UpdatedBatch,
            head = HistogramDoc
        }
    }.


-spec finalize(ctx()) ->  batch() | undefined.
finalize(#ctx{head_updated = false, batch = Batch}) ->
    Batch;

finalize(#ctx{
    head_updated = true,
    head = #document{key = HeadKey} = HeadDoc,
    datastore_ctx = DatastoreCtx,
    batch = Batch
}) ->
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx, HeadKey, HeadDoc, Batch),
    UpdatedBatch.


-spec set_active_time_series(histogram_api:time_series_id(), ctx()) -> ctx().
set_active_time_series(TimeSeriesId, Ctx) ->
    Ctx#ctx{active_time_series = TimeSeriesId}.


-spec set_active_metrics(histogram_api:metrics_id(), ctx()) -> ctx().
set_active_metrics(MetricsId, Ctx) ->
    Ctx#ctx{active_metrics = MetricsId}.


-spec get_head_key(ctx()) -> key().
get_head_key(#ctx{head = #document{key = HeadKey}}) ->
    HeadKey.


-spec is_head(key(), ctx()) -> boolean().
is_head(Key, #ctx{head = #document{key = HeadKey}}) ->
    Key =:= HeadKey.


-spec get(key(), ctx()) -> {histogram_api:data(), ctx()}.
get(Key, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    {{ok, #document{value = HistogramRecord}}, UpdatedBatch} = datastore_doc:fetch(DatastoreCtx, Key, Batch),
    {histogram_tail_node:get_data(HistogramRecord), Ctx#ctx{batch = UpdatedBatch}}.


-spec create(histogram_api:data(), ctx()) -> {key(), ctx()}.
create(DataToCreate, #ctx{head = #document{key = HeadKey}, datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    NewDocKey = datastore_key:new_adjacent_to(HeadKey),
    Doc = #document{key = NewDocKey, value = histogram_tail_node:set_data(DataToCreate)},
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx#{generated_key => true}, NewDocKey, Doc, Batch),
    {NewDocKey, Ctx#ctx{batch = UpdatedBatch}}.


-spec update(key(), histogram_api:data(), ctx()) -> ctx().
update(HeadKey, Data, #ctx{
    head = #document{key = HeadKey, value = HistogramRecord} = HeadDoc,
    active_time_series = TimeSeriesId,
    active_metrics = MetricsId
} = Ctx) ->
    TimeSeries = histogram_hub:get_time_series(HistogramRecord),
    UpdatedTimeSeries = histogram_api:update_time_series_map_data(TimeSeries, TimeSeriesId, MetricsId, Data),
    UpdatedDoc = HeadDoc#document{value = histogram_hub:set_time_series(UpdatedTimeSeries)},
    Ctx#ctx{head = UpdatedDoc, head_updated = true};

update(DataDocKey, Data, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    Doc = #document{key = DataDocKey, value = histogram_tail_node:set_data(Data)},
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx, DataDocKey, Doc, Batch),
    Ctx#ctx{batch = UpdatedBatch}.


-spec delete(key(), ctx()) -> ctx().
delete(Key, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    {ok, UpdatedBatch} = datastore_doc:delete(DatastoreCtx, Key, Batch),
    Ctx#ctx{batch = UpdatedBatch}.
