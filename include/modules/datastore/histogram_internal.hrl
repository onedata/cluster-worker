%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains records definitions used by datastore histogram modules.
%%% Records are defined in hrl should not be used outside datastore.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(HISTOGRAM_INTERNAL_HRL).
-define(HISTOGRAM_INTERNAL_HRL, 1).


% Record storing part of metric's windows. If windows count exceeds capacity of
% of single document (see histogram_persistence), windows of each metric are
% stored in linked list of #data{} records where newest windows are stored in
% first record.
-record(data, {
    windows = histogram_windows:init() :: histogram_windows:windows(),
    prev_record :: histogram_metric:key() | undefined,
    % Timestamp of newest measurement in previous record
    prev_record_timestamp :: histogram_windows:timestamp() | undefined
}).


% Record describing number of #data{} records used to store windows of particular metric.
% #data{} records form linked list and this record also describes capacity of list's head and
% capacity of other #data{} records (records being part of list's tail).
-record(splitting_strategy, {
    max_docs_count :: non_neg_integer(),
    max_windows_in_head_doc :: non_neg_integer(),
    max_windows_in_tail_doc :: non_neg_integer()
}).


% Record describing single metric. It stores config, splitting_strategy (see above) and head of
% list of #data{} records.
-record(metric, {
    config :: histogram_metric:config(),
    % NOTE: Doc splitting strategy may result in keeping more windows than required by config
    % (in order to optimize documents management)
    splitting_strategy :: histogram_metric:splitting_strategy(),
    data = #data{} :: histogram_metric:data()
}).

-endif.
