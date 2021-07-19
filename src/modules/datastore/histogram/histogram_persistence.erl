%%%-------------------------------------------------------------------
%%% @author michal
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Jul 2021 12:23 AM
%%%-------------------------------------------------------------------
-module(histogram_persistence).
-author("michal").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([new/4, init/3, finalize/1,
    get_head_key/1, is_head/2, get/2,
    create/2, update/3]).
%% datastore_model callbacks
-export([get_ctx/0]).

-record(ctx, {
    datastore_ctx,
    batch,
    head,
    head_updated = false,
    active_time_series,
    active_metrics
}).

-type ctx() :: #ctx{}.

%%%===================================================================
%%% API
%%%===================================================================

new(DatastoreCtx, Id, Histogram, Batch) ->
    HistogramDoc = #document{key = Id, value = Histogram},
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx, Id, HistogramDoc, Batch),
    #ctx{
        datastore_ctx = DatastoreCtx,
        batch = UpdatedBatch
    }.


init(Id, DatastoreCtx, Batch) ->
    {{ok, HistogramDoc}, UpdatedBatch} = datastore_doc:fetch(DatastoreCtx, Id, Batch),
    #ctx{
        datastore_ctx = DatastoreCtx,
        batch = UpdatedBatch,
        head = HistogramDoc
    }.


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


get_head_key(#ctx{head = #document{key = HeadKey}}) ->
    HeadKey.


is_head(Key, #ctx{head = #document{key = HeadKey}}) ->
    Key =:= HeadKey.


get(HeadKey, #ctx{
    head = #document{key = HeadKey, value = HistogramRecord},
    active_time_series = TimeSeriesId,
    active_metrics = MetricsId
} = Ctx) ->
    Data = maps:get(MetricsId, maps:get(TimeSeriesId, HistogramRecord)),
    {Data, Ctx};

get(Key, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    {{ok, #document{value = Data}}, UpdatedBatch} = datastore_doc:fetch(DatastoreCtx, Key, Batch),
    {Data, Ctx#ctx{batch = UpdatedBatch}}.


create(DataToCreate, #ctx{head = #document{key = HeadKey}, datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    NewDocKey = datastore_key:new_adjacent_to(HeadKey),
    Doc = #document{key = NewDocKey, value = DataToCreate},
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx#{generated_key => true}, NewDocKey, Doc, Batch),
    {NewDocKey, Ctx#ctx{batch = UpdatedBatch}}.


update(HeadKey, Data, #ctx{
    head = #document{key = HeadKey, value = HistogramRecord} = HeadDoc,
    active_time_series = TimeSeriesId,
    active_metrics = MetricsId
} = Ctx) ->
    TimeSeries = maps:get(TimeSeriesId, HistogramRecord),
    UpdatedRecord = HistogramRecord#{TimeSeriesId => TimeSeries#{MetricsId => Data}},
    UpdatedDoc = HeadDoc#document{value = UpdatedRecord},
    Ctx#ctx{head = UpdatedDoc};

update(DataDocKey, Data, #ctx{datastore_ctx = DatastoreCtx, batch = Batch} = Ctx) ->
    Doc = #document{key = DataDocKey, value = Data},
    {{ok, _}, UpdatedBatch} = datastore_doc:save(DatastoreCtx, DataDocKey, Doc, Batch),
    Ctx#ctx{batch = UpdatedBatch}.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> ctx().
get_ctx() ->
    #{
        model => ?MODULE,
        memory_driver => undefined,
        disc_driver => undefined
    }.

% TODO - dodac struukture, enkodowanie i dekodowanie