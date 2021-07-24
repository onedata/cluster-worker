%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains histogram connected records definitions.
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

-record(metrics, {
    config :: histogram_api:config(),
    % NOTE: Doc splitting strategy may result in keeping more windows than required by config
    % (in order to optimize documents management)
    doc_splitting_strategy :: histogram_api:doc_splitting_strategy(),
    data :: histogram_api:data()
}).

-endif.
