%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for management of time series documents
%%% (getting from and saving to datastore internal structures).
%%%
%%% The module bases on #ctx{} record that stores all data needed to
%%% hide interaction with internal datastore modules/structures.
%%% It has to be finalized with finalize/1 function after last usage to return
%%% structure used by datastore to save all changes.
%%%
%%% The module uses 2 helper records: ts_hub that stores heads
%%% of each metric and ts_metric_data_node that stores windows of single
%%% metrics if there are too many windows to be stored in head
%%% (see ts_metric for head/tail description). Head of each metric contains
%%% all windows or part of windows set (depending on windows count) as well as config 
%%% and splitting strategy (see #metric{} record definition) while ts_metric_data_node
%%% stores only parts of windows sets. There is always exactly one 
%%% ts_hub document (storing heads of all metrics). ts_metric_data_node
%%% documents are created on demand and multiple ts_metric_data_node documents
%%% can be created for each metric. E.g.:
%%%
%%%                                  ts_hub
%%% +----------------------------------------------------------------------+
%%% |                                                                      |
%%% |    metric{                metric{               metric{              |
%%% |      data{                  data{                 data{              |
%%% |        prev_record            prev_record           prev_record      |     Heads inside hub records
%%% |      }    |                   = undefined         }    |             |
%%% |    }      |                 }                   }      |             |
%%% |           |               }                            |             |
%%% |           |                                            |             |
%%% +-----------+--------------------------------------------+-------------+
%%%             |                                            |
%%%             |                                            |
%%%             v                                            v
%%%   ts_metric_data_node                             ts_metric_data_node
%%% +---------------------+                         +---------------------+
%%% |                     |                         |                     |
%%% |    data{            |                         |    data{            |
%%% |      prev_record    |                         |      prev_record    |     Rest of data inside
%%% |    }      |         |                         |      = undefined    |     metric_data records
%%% |           |         |                         |    }                |
%%% +-----------+---------+                         +---------------------+
%%%             |
%%%             |
%%%             v
%%%   ts_metric_data_node
%%% +---------------------+
%%% |                     |
%%% |    data{            |
%%% |      prev_record    |
%%% |      = undefined    |
%%% |    }                |
%%% +---------------------+
%%%
%%% Key of ts_hub document is equal to id of time series collection while
%%% ts_metric_data_node documents have randomly generated ids.
%%% @end
%%%-------------------------------------------------------------------
-module(ts_persistence).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_time_series.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([init_for_new_collection/4, init_for_existing_collection/3, finalize/1,
    set_currently_processed_time_series/2, set_currently_processed_metric/2,
    get_time_series_collection_id/1, is_hub_key/2,
    get/2, create/2, update/3, delete/2, delete_hub/1]).

-record(ctx, {
    datastore_ctx :: datastore_ctx(),
    batch :: batch() | undefined, % Undefined when time series collection is used outside tp process
                                  % (call via datastore_reader:time_series_collection_list/3)
    hub :: doc(),
    is_hub_updated = false :: boolean(), % Field used to determine if hub should be saved by finalize/1 function
    % Fields representing metric currently being updated (single ctx can be used to update several metrics)
    currently_processed_time_series :: time_series_collection:time_series_id() | undefined,
    currently_processed_metric :: ts_metric:id() | undefined
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

-spec init_for_new_collection(datastore_ctx(), time_series_collection:collection_id(), ts_hub:time_series_collection_heads(),
    batch()) -> ctx().
init_for_new_collection(DatastoreCtx, Id, TimeSeriesHeads, Batch) ->
    TSHub = #document{key = Id, value = ts_hub:set_time_series_collection_heads(TimeSeriesHeads)},
    #ctx{
        datastore_ctx = DatastoreCtx,
        batch = Batch,
        hub = TSHub,
        is_hub_updated = true
    }.


-spec init_for_existing_collection(datastore_ctx(), time_series_collection:collection_id(), batch() | undefined) ->
    {ts_hub:time_series_collection_heads(), ctx()}.
init_for_existing_collection(DatastoreCtx, Id, Batch) ->
    {{ok, #document{value = TSHubRecord} = TSHub}, UpdatedBatch} =
        datastore_doc:fetch(DatastoreCtx, Id, Batch),
    {
        ts_hub:get_time_series_collection_heads(TSHubRecord),
        #ctx{
            datastore_ctx = DatastoreCtx,
            batch = UpdatedBatch,
            hub = TSHub
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


-spec set_currently_processed_time_series(time_series_collection:time_series_id(), ctx()) -> ctx().
set_currently_processed_time_series(TimeSeriesId, Ctx) ->
    Ctx#ctx{currently_processed_time_series = TimeSeriesId}.


-spec set_currently_processed_metric(ts_metric:id(), ctx()) -> ctx().
set_currently_processed_metric(MetricsId, Ctx) ->
    Ctx#ctx{currently_processed_metric = MetricsId}.


-spec get_time_series_collection_id(ctx()) -> key().
get_time_series_collection_id(#ctx{hub = #document{key = HubKey}}) ->
    HubKey. % Hub key is always equal to time series collection id


-spec is_hub_key(key(), ctx()) -> boolean().
is_hub_key(Key, #ctx{hub = #document{key = HubKey}}) ->
    Key =:= HubKey.


-spec get(key(), ctx()) -> {ts_metric:data_node(), ctx()}.
get(Key, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    {{ok, #document{value = Record}}, UpdatedBatch} = datastore_doc:fetch(DatastoreCtx, Key, Batch),
    {ts_metric_data_node:get(Record), Ctx#ctx{batch = UpdatedBatch}}.


-spec create(ts_metric:data_node(), ctx()) -> {key(), ctx()}.
create(DataToCreate, #ctx{hub = #document{key = HubKey}, datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    NewDocKey = datastore_key:new_adjacent_to(HubKey),
    Doc = #document{key = NewDocKey, value = ts_metric_data_node:set(DataToCreate)},
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx#{generated_key => true}, NewDocKey, Doc, Batch),
    {NewDocKey, Ctx#ctx{batch = UpdatedBatch}}.


-spec update(key(), ts_metric:data_node(), ctx()) -> ctx().
update(HubKey, Data, #ctx{
    hub = #document{key = HubKey, value = Record} = HubDoc,
    currently_processed_time_series = TimeSeriesId,
    currently_processed_metric = MetricsId
} = Ctx) ->
    TimeSeriesHeads = ts_hub:get_time_series_collection_heads(Record),
    TimeSeries = maps:get(TimeSeriesId, TimeSeriesHeads),
    Metrics = maps:get(MetricsId, TimeSeries),
    UpdatedTimeSeriesHeads = TimeSeriesHeads#{TimeSeriesId => TimeSeries#{MetricsId => Metrics#metric{head_data = Data}}},
    UpdatedDoc = HubDoc#document{value = ts_hub:set_time_series_collection_heads(UpdatedTimeSeriesHeads)},
    Ctx#ctx{hub = UpdatedDoc, is_hub_updated = true};

update(DataDocKey, Data, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    Doc = #document{key = DataDocKey, value = ts_metric_data_node:set(Data)},
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
