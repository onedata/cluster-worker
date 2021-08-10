%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains metric config record definition.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(METRIC_CONFIG_HRL).
-define(METRIC_CONFIG_HRL, 1).

-record(metric_config, {
    legend = <<>> :: histogram_metric:legend(),
    window_timespan :: non_neg_integer(), % TODO - zmienic na resolution
    max_windows_count :: non_neg_integer(), % TODO - zmienic na retention
    aggregator :: histogram_windows:aggregator()
}).

-endif.
