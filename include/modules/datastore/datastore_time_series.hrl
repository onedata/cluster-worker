%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains records definitions used by datastore time series modules.
%%% Records defined in this hrl should not be used outside datastore.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_TIME_SERIES_HRL).
-define(DATASTORE_TIME_SERIES_HRL, 1).


% Record storing part of metric's windows. If windows count exceeds capacity of
% of single record (see ts_persistence), windows of each metric are
% stored in linked list of #data{} records where newest windows are stored in
% first record (head).
-record(data, {
    windows = ts_windows:init() :: ts_windows:windows(),
    prev_record :: ts_metric_data:key() | undefined,
    % Timestamp of newest measurement in previous record
    prev_record_timestamp :: ts_windows:timestamp() | undefined
}).


% Record describing number of #data{} records used to store windows of particular metric.
% #data{} records form linked list so this record also describes capacity of list's head and
% capacity of other #data{} records in list (records being part of list's tail).
-record(splitting_strategy, {
    max_docs_count :: non_neg_integer(),
    max_windows_in_head_doc :: non_neg_integer(),
    max_windows_in_tail_doc :: non_neg_integer()
}).


% Record describing single metric that is part of time series (see time_series.erl).
% It stores config, splitting_strategy (see above) and head of list of #data{} records.
-record(metric, {
    config :: ts_metric:config(),
    % NOTE: splitting strategy may result in keeping more windows than required by config
    % (in order to optimize documents management)
    splitting_strategy :: ts_metric:splitting_strategy(),
    head_data = #data{} :: ts_metric:data()
}).

-endif.
