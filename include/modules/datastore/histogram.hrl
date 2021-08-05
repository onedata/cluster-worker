%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains histogram config record definition.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(HISTOGRAM_HRL).
-define(HISTOGRAM_HRL, 1).

-record(metric_config, {
    legend = <<>> :: histogram_metric:legend(),
    window_timespan :: non_neg_integer(),
    max_windows_count :: non_neg_integer(),
    aggregator :: histogram_windows:aggregator()
}).

-endif.
