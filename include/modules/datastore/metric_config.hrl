%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains metric config record definition used to configure
%%% single histogram metric (see #metric{} in datastore_histogram.hrl)
%%% @end
%%%-------------------------------------------------------------------

-ifndef(METRIC_CONFIG_HRL).
-define(METRIC_CONFIG_HRL, 1).

-record(metric_config, {
    legend = <<>> :: histogram_metric:legend(),
    resolution :: non_neg_integer(),
    retention :: non_neg_integer(),
    aggregator :: histogram_windows:aggregator()
}).

-endif.
