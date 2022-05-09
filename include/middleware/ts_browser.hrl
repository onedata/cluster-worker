%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains records definitions used across time series middleware modules.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(TS_BROWSER_HRL).
-define(TS_BROWSER_HRL, 1).

-record(time_series_get_layout_req, {}).

-record(time_series_get_slice_req, {
    layout :: time_series_collection:layout(),
    start_timestamp :: undefined | ts_browse_request:timestamp(),
    window_limit :: ts_browse_request:window_limit()
}).

-record(time_series_layout_result, {
    layout :: time_series_collection:layout()
}).

-record(time_series_slice_result, {
    slice :: time_series_collection:slice()
}).

-endif.
