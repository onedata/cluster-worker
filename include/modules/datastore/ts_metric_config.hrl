%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains metric config record definition used to configure
%%% single time series metric (see #metric{} in datastore_time_series.hrl)
%%% @end
%%%-------------------------------------------------------------------

-ifndef(METRIC_CONFIG_HRL).
-define(METRIC_CONFIG_HRL, 1).

-record(metric_config, {
    legend = <<>> :: ts_metric:legend(),
    resolution :: non_neg_integer(),
    retention :: pos_integer(),
    aggregator :: ts_windows:aggregator()
}).

-endif.
