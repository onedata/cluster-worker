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
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_persistence).
-author("Michal Wrzeszcz").

-include("modules/datastore/histogram.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([new/4, init/3, finalize/1, set_active_time_series/2, set_active_metrics/2,
    get_head_key/1, is_head/2, get/2,
    create/2, update/3, delete/2]).
%% datastore_model callbacks
-export([get_ctx/0]).

-record(histogram, {
    time_series :: histogram_api:time_series_map()
}).

-record(ctx, {
    datastore_ctx :: datastore_ctx(),
    batch :: batch() | undefined,
    head :: doc(),
    head_updated = false :: boolean(),
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
    HistogramDoc = #document{key = Id, value = #histogram{time_series = TimeSeries}},
    {{ok, SavedHistogramDoc}, UpdatedBatch} = datastore_doc:save(DatastoreCtx, Id, HistogramDoc, Batch),
    #ctx{
        datastore_ctx = DatastoreCtx,
        batch = UpdatedBatch,
        head = SavedHistogramDoc
    }.


-spec init(datastore_ctx(), histogram_api:id(), batch() | undefined) ->
    {histogram_api:time_series_map(), ctx()}.
init(DatastoreCtx, Id, Batch) ->
    {{ok, #document{value = #histogram{time_series = TimeSeriesMap}} = HistogramDoc}, UpdatedBatch} =
        datastore_doc:fetch(DatastoreCtx, Id, Batch),
    {
        TimeSeriesMap,
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
    {{ok, #document{value = Data}}, UpdatedBatch} = datastore_doc:fetch(DatastoreCtx, Key, Batch),
    {Data, Ctx#ctx{batch = UpdatedBatch}}.


-spec create(histogram_api:data(), ctx()) -> {key(), ctx()}.
create(DataToCreate, #ctx{head = #document{key = HeadKey}, datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    NewDocKey = datastore_key:new_adjacent_to(HeadKey),
    Doc = #document{key = NewDocKey, value = DataToCreate},
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx#{generated_key => true}, NewDocKey, Doc, Batch),
    {NewDocKey, Ctx#ctx{batch = UpdatedBatch}}.


-spec update(key(), histogram_api:data(), ctx()) -> ctx().
update(HeadKey, Data, #ctx{
    head = #document{key = HeadKey, value = #histogram{time_series = TimeSeriesMap} = HistogramRecord} = HeadDoc,
    active_time_series = TimeSeriesId,
    active_metrics = MetricsId
} = Ctx) ->
    TimeSeries = maps:get(TimeSeriesId, TimeSeriesMap),
    Metrics = maps:get(MetricsId, TimeSeries),
    UpdatedTimeSeriesMap = TimeSeriesMap#{TimeSeriesId => TimeSeries#{MetricsId => Metrics#metrics{data = Data}}},
    UpdatedDoc = HeadDoc#document{value = HistogramRecord#histogram{time_series = UpdatedTimeSeriesMap}},
    Ctx#ctx{head = UpdatedDoc, head_updated = true};

update(DataDocKey, Data, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    Doc = #document{key = DataDocKey, value = Data},
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx, DataDocKey, Doc, Batch),
    Ctx#ctx{batch = UpdatedBatch}.


-spec delete(key(), ctx()) -> ctx().
delete(Key, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    {ok, UpdatedBatch} = datastore_doc:delete(DatastoreCtx, Key, Batch),
    Ctx#ctx{batch = UpdatedBatch}.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore_ctx().
get_ctx() ->
    #{
        model => ?MODULE,
        memory_driver => undefined,
        disc_driver => undefined
    }.

% TODO - dodac struukture, enkodowanie i dekodowanie