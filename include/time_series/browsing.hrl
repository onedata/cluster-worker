%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains records definitions used across time series browse modules.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(TS_BROWSER_HRL).
-define(TS_BROWSER_HRL, 1).

-record(time_series_layout_get_request, {}).

-record(time_series_slice_get_request, {
    layout :: time_series_collection:layout(),
    start_timestamp :: undefined | ts_browse_request:timestamp(),
    window_limit :: undefined | ts_browse_request:window_limit(),
    extended_info :: undefined | boolean()
}).

-record(time_series_layout_get_result, {
    layout :: time_series_collection:layout()
}).

-record(time_series_slice_get_result, {
    slice :: time_series_collection:slice()
}).

-endif.
