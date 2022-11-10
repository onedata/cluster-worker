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

-include("time_series/browsing.hrl").

%% API
-export([create/3, incorporate_config/3, delete/2, clone/2]).
-export([get_layout/2]).
-export([consume_measurements/3]).
-export([get_slice/4]).
-export([get_windows_timestamps/3]).
-export([browse/3]).

-type ctx() :: datastore_model:ctx().

-define(apply(Ctx, Id, Args), ?apply(?FUNCTION_NAME, Ctx, Id, Args)).
-define(apply(FunctionName, Ctx, Id, Args), datastore_model:datastore_apply(
    Ctx, Id, fun datastore:time_series_collection_operation/4, [FunctionName, Args]
)).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc @see time_series_collection:create/4
-spec create(ctx(), time_series_collection:id(), time_series_collection:config()) ->
    ok | {error, term()}.
create(Ctx, Id, ConfigMap) ->
    ?apply(Ctx, Id, [ConfigMap]).


%% @doc @see time_series_collection:incorporate_config/4
-spec incorporate_config(ctx(), time_series_collection:id(), time_series_collection:config()) ->
    ok | {error, term()}.
incorporate_config(Ctx, Id, ConfigToIncorporate) ->
    ?apply(Ctx, Id, [ConfigToIncorporate]).


%% @doc @see time_series_collection:delete/3
-spec delete(ctx(), time_series_collection:id()) ->
    ok | {error, term()}.
delete(Ctx, Id) ->
    ?apply(Ctx, Id, []).


-spec clone(ctx(), time_series_collection:id()) ->
    {ok, time_series_collection:id()} | {error, term()}.
clone(Ctx, Id) ->
    case ?apply(generate_dump, Ctx, Id, []) of
        {ok, Dump} ->
            % Generate a new key for the clone that is adjacent to retain the same routing.
            % It must be generated on this layer as TP processes operate on a different type of keys.
            NewCollectionId = datastore_key:new_adjacent_to(Id),
            case ?apply(create_from_dump, Ctx, NewCollectionId, [Dump]) of
                ok -> {ok, NewCollectionId};
                {error, _} = SaveError -> SaveError
            end;
        {error, _} = GetError ->
            GetError
    end.


%% @doc @see time_series_collection:get_layout/3
-spec get_layout(ctx(), time_series_collection:id()) ->
    {ok, time_series_collection:layout()} | {error, term()}.
get_layout(Ctx, Id) ->
    ?apply(Ctx, Id, []).


%% @doc @see time_series_collection:consume_measurements/4
-spec consume_measurements(ctx(), time_series_collection:id(), time_series_collection:consume_spec()) ->
    ok | {error, term()}.
consume_measurements(Ctx, Id, ConsumeSpec) ->
    ?apply(Ctx, Id, [ConsumeSpec]).


%% @doc @see time_series_collection:get_slice/5
-spec get_slice(ctx(), time_series_collection:id(), time_series_collection:layout(), ts_windows:list_options()) ->
    {ok, time_series_collection:slice()} | {error, term()}.
get_slice(Ctx, Id, SliceLayout, Options) ->
    ?apply(Ctx, Id, [SliceLayout, Options]).


%% @doc @see time_series_collection:get_windows_timestamps/4
-spec get_windows_timestamps(ctx(), time_series_collection:id(), time_series_collection:windows_spec()) ->
    {ok, time_series_collection:timestamps_spec()} | {error, term()}.
get_windows_timestamps(Ctx, Id, WindowsSpec) ->
    ?apply(Ctx, Id, [WindowsSpec]).


-spec browse(ctx(), time_series_collection:id(), ts_browse_request:record()) -> 
    {ok, ts_browse_result:record()} | {error, term()}.
browse(Ctx, Id, #time_series_layout_get_request{}) ->
    case get_layout(Ctx, Id) of
        {ok, Layout} -> {ok, #time_series_layout_get_result{layout = Layout}};
        {error, _} = Error -> Error
    end;
browse(Ctx, Id, #time_series_slice_get_request{} = SliceReq) ->
    #time_series_slice_get_request{
        layout = SliceLayout, 
        start_timestamp = StartTimestamp, 
        window_limit = WindowLimit
    } = SliceReq,
    Opts = maps_utils:remove_undefined(#{
        start_timestamp => StartTimestamp, window_limit => WindowLimit
    }),
    case get_slice(Ctx, Id, SliceLayout, Opts) of
        {ok, Slice} -> {ok, #time_series_slice_get_result{slice = Slice}};
        {error, _} = Error -> Error
    end.
