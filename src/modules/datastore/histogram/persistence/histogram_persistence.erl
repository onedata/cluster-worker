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
%%% The module uses 2 helper records: histogram_hub that stores heads
%%% of each metric and histogram_metric_data that stores windows of single
%%% metrics if there are to many of them to store then all in head
%%% (see histogram_time_series for head/tail description). Head of each metric contains
%%% all windows or part of windows set (depending on windows count) as well as config 
%%% and splitting strategy (see #metric{} record definition) while histogram_metric_data
%%% stores only parts of windows sets. There is always exactly one 
%%% histogram_hub document (storing heads of all metrics). histogram_metric_data
%%% documents are created on demand and multiple histogram_metric_data documents
%%% can be created for each metric. E.g.:
%%%
%%%                              histogram_hub
%%% +----------------------------------------------------------------------+
%%% |                                                                      |
%%% |    metric{                metric{               metric{              |
%%% |      data{                  data{                 data{              |
%%% |        prev_record            prev_record           prev_record      |     Heads inside hub records
%%% |      }    |                   =undefined          }    |             |
%%% |    }      |                 }                   }      |             |
%%% |           |               }                            |             |
%%% |           |                                            |             |
%%% +-----------+--------------------------------------------+-------------+
%%%             |                                            |
%%%             |                                            |
%%%             v                                            v
%%% histogram_metric_data                           histogram_metric_data
%%% +---------------------+                         +---------------------+
%%% |                     |                         |                     |
%%% |    data{            |                         |    data{            |
%%% |      prev_record    |                         |      prev_record    |     Rest of data inside
%%% |    }      |         |                         |      =undefined     |     metric_data records
%%% |           |         |                         |    }                |
%%% +-----------+---------+                         +---------------------+
%%%             |
%%%             |
%%%             v
%%% histogram_metric_data
%%% +---------------------+
%%% |                     |
%%% |    data{            |
%%% |      prev_record    |
%%% |      =undefined     |
%%% |    }                |
%%% +---------------------+
%%%
%%% Key of histogram_hub document is equal to id of histogram while
%%% histogram_metric_data documents have randomly  generated ids.
%%% @end
%%%-------------------------------------------------------------------
-module(histogram_persistence).
-author("Michal Wrzeszcz").

-include("modules/datastore/histogram_internal.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([init_for_new_histogram/4, init_for_existing_histogram/3, finalize/1,
    set_active_time_series/2, set_active_metric/2,
    get_histogram_id/1, is_hub_key/2,
    get/2, create/2, update/3, delete/2, delete_hub/1]).

-record(ctx, {
    datastore_ctx :: datastore_ctx(),
    batch :: batch() | undefined, % Undefined when histogram is used outside tp process
                                  % (call via datastore_reader:histogram_get/3)
    hub :: doc(),
    is_hub_updated = false :: boolean(), % Field used to determine if hub should be saved by finalize/1 function
    % Fields used to determine metric to update
    active_time_series :: histogram_time_series:time_series_id() | undefined,
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

-spec init_for_new_histogram(datastore_ctx(), histogram_time_series:id(), histogram_time_series:time_series_pack(), batch()) -> ctx().
init_for_new_histogram(DatastoreCtx, Id, TimeSeriesPack, Batch) ->
    HistogramHub = #document{key = Id, value = histogram_hub:set_time_series_pack(TimeSeriesPack)},
    {{ok, SavedHistogramHub}, UpdatedBatch} = datastore_doc:save(DatastoreCtx, Id, HistogramHub, Batch),
    #ctx{
        datastore_ctx = DatastoreCtx,
        batch = UpdatedBatch,
        hub = SavedHistogramHub
    }.


-spec init_for_existing_histogram(datastore_ctx(), histogram_time_series:id(), batch() | undefined) ->
    {histogram_time_series:time_series_pack(), ctx()}.
init_for_existing_histogram(DatastoreCtx, Id, Batch) ->
    {{ok, #document{value = HistogramHubRecord} = HistogramHub}, UpdatedBatch} =
        datastore_doc:fetch(DatastoreCtx, Id, Batch),
    {
        histogram_hub:get_time_series_pack(HistogramHubRecord),
        #ctx{
            datastore_ctx = DatastoreCtx,
            batch = UpdatedBatch,
            hub = HistogramHub
        }
    }.


-spec finalize(ctx()) ->  batch() | undefined.
finalize(#ctx{is_hub_updated = false, batch = Batch}) ->
    Batch;

finalize(#ctx{
    is_hub_updated = true,
    hub = #document{key = HubKey} = HubDoc,
    datastore_ctx = DatastoreCtx,
    batch = Batch
}) ->
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx, HubKey, HubDoc, Batch),
    UpdatedBatch.


-spec set_active_time_series(histogram_time_series:time_series_id(), ctx()) -> ctx().
set_active_time_series(TimeSeriesId, Ctx) ->
    Ctx#ctx{active_time_series = TimeSeriesId}.


-spec set_active_metric(histogram_metric:id(), ctx()) -> ctx().
set_active_metric(MetricsId, Ctx) ->
    Ctx#ctx{active_metric = MetricsId}.


-spec get_histogram_id(ctx()) -> key().
get_histogram_id(#ctx{hub = #document{key = HubKey}}) ->
    HubKey. % Hub key is always equal to histogram id


-spec is_hub_key(key(), ctx()) -> boolean().
is_hub_key(Key, #ctx{hub = #document{key = HubKey}}) ->
    Key =:= HubKey.


-spec get(key(), ctx()) -> {histogram_metric:data(), ctx()}.
get(Key, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    {{ok, #document{value = HistogramRecord}}, UpdatedBatch} = datastore_doc:fetch(DatastoreCtx, Key, Batch),
    {histogram_metric_data:get_data(HistogramRecord), Ctx#ctx{batch = UpdatedBatch}}.


-spec create(histogram_metric:data(), ctx()) -> {key(), ctx()}.
create(DataToCreate, #ctx{hub = #document{key = HubKey}, datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    NewDocKey = datastore_key:new_adjacent_to(HubKey),
    Doc = #document{key = NewDocKey, value = histogram_metric_data:set_data(DataToCreate)},
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx#{generated_key => true}, NewDocKey, Doc, Batch),
    {NewDocKey, Ctx#ctx{batch = UpdatedBatch}}.


-spec update(key(), histogram_metric:data(), ctx()) -> ctx().
update(HubKey, Data, #ctx{
    hub = #document{key = HubKey, value = HistogramRecord} = HubDoc,
    active_time_series = TimeSeriesId,
    active_metric = MetricsId
} = Ctx) ->
    TimeSeriesPack = histogram_hub:get_time_series_pack(HistogramRecord),
    TimeSeries = maps:get(TimeSeriesId, TimeSeriesPack),
    Metrics = maps:get(MetricsId, TimeSeries),
    UpdatedTimeSeries = TimeSeriesPack#{TimeSeriesId => TimeSeries#{MetricsId => Metrics#metric{data = Data}}},
    UpdatedDoc = HubDoc#document{value = histogram_hub:set_time_series_pack(UpdatedTimeSeries)},
    Ctx#ctx{hub = UpdatedDoc, is_hub_updated = true};

update(DataDocKey, Data, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    Doc = #document{key = DataDocKey, value = histogram_metric_data:set_data(Data)},
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx, DataDocKey, Doc, Batch),
    Ctx#ctx{batch = UpdatedBatch}.


-spec delete(key(), ctx()) -> ctx().
delete(Key, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    {ok, UpdatedBatch} = datastore_doc:delete(DatastoreCtx, Key, Batch),
    Ctx#ctx{batch = UpdatedBatch}.


-spec delete_hub(ctx()) -> ctx().
delete_hub(#ctx{hub = #document{key = HubKey}, datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    {ok, UpdatedBatch} = datastore_doc:delete(DatastoreCtx, HubKey, Batch),
    Ctx#ctx{batch = UpdatedBatch}.
