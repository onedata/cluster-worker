%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains records definitions used by histogram_api module.
%%% Records are defined in hrl to be used during unit tests and should
%%% not be used by any other module.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(HISTOGRAM_API_HRL).
-define(HISTOGRAM_API_HRL, 1).

-record(data, {
    windows = histogram_windows:init() :: histogram_windows:windows(),
    prev_record :: histogram_api:key() | undefined,
    % Timestamp of newest point in previous record
    prev_record_timestamp :: histogram_windows:timestamp() | undefined
}).

-record(doc_splitting_strategy, {
    max_docs_count :: non_neg_integer(),
    max_windows_in_head_doc :: non_neg_integer(),
    max_windows_in_tail_doc :: non_neg_integer()
}).

-record(metrics, {
    config :: histogram_api:config(),
    % NOTE: Doc splitting strategy may result in keeping more windows than required by config
    % (in order to optimize documents management)
    doc_splitting_strategy :: histogram_api:doc_splitting_strategy(),
    data = #data{} :: histogram_api:data()
}).

-endif.
