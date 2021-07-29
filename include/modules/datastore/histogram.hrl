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

-record(histogram_config, {
    legend = <<>> :: histogram_api:legend(),
    window_size :: non_neg_integer(),
    max_windows_count :: non_neg_integer(),
    apply_function :: histogram_windows:apply_function()
}).

-endif.
