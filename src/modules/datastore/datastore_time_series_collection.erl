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

-include("middleware/ts_browser.hrl").

%% API
-export([create/3, incorporate_config/3, delete/2]).
-export([get_layout/2]).
-export([consume_measurements/3]).
-export([get_slice/4]).
-export([browse/3]).

-type ctx() :: datastore_model:ctx().

-define(apply(Ctx, Id, Args), datastore_model:datastore_apply(
    Ctx, Id, fun datastore:time_series_collection_operation/4, [?FUNCTION_NAME, Args]
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


-spec browse(ctx(), time_series_collection:id(), ts_browse_request:req()) -> 
    {ok, ts_browse_result:res()} | {error, term()}.
browse(Ctx, Id, #time_series_get_layout_req{}) ->
    case get_layout(Ctx, Id) of
        {ok, Layout} -> {ok, #time_series_layout_result{layout = Layout}};
        Error -> Error
    end;
browse(Ctx, Id, #time_series_get_slice_req{} = SliceReq) ->
    #time_series_get_slice_req{
        layout = SliceLayout, 
        start_timestamp = StartTimestamp, 
        window_limit = WindowLimit
    } = SliceReq,
    Opts = maps_utils:remove_undefined(#{
        start_timestamp => StartTimestamp, window_limit => WindowLimit
    }),
    case get_slice(Ctx, Id, SliceLayout, Opts) of
        {ok, Slice} -> {ok, #time_series_slice_result{slice = Slice}};
        Error -> Error
    end.
