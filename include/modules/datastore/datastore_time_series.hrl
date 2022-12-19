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

-include_lib("ctool/include/errors.hrl").

% Record storing part of metric's windows. If windows count exceeds capacity of
% of single record (see ts_persistence), windows of each metric are
% stored in linked list of #data_node{} records where newest windows are stored in
% first record (head).
-record(data_node, {
    windows = ts_windows:init() :: ts_windows:window_collection(),
    older_node_key :: ts_metric_data_node:key() | undefined,
    % Timestamp of newest measurement in older data node
    older_node_timestamp :: ts_window:timestamp_seconds() | undefined
}).


% Record describing number of #data_node{} records used to store windows of particular metric.
% #data_node{} records form linked list so this record also describes capacity of list's head and
% capacity of other #data_node{} records in list (records being part of list's tail).
-record(splitting_strategy, {
    max_docs_count :: non_neg_integer(),
    max_windows_in_head_doc :: non_neg_integer(),
    max_windows_in_tail_doc :: non_neg_integer()
}).


% Record describing single metric that is part of time series (see time_series_collection.erl).
% It stores config, splitting_strategy (see above) and head of list of #data_node{} records.
-record(metric, {
    config :: metric_config:record(),
    % NOTE: splitting strategy may result in keeping more windows than required by config
    % (in order to optimize documents management)
    splitting_strategy :: ts_metric:splitting_strategy(),
    head_data = #data_node{} :: ts_metric:data_node()
}).


% Record storing whole metric created on the basis of all data nodes.
-record(metric_dump, {
    head_record :: ts_metric:record(),
    data_nodes = [] :: [ts_metric:data_node()]
}).


% Collections of windows are stored inside #data_node{}
-record(window, {
    aggregated_measurements :: ts_window:aggregated_measurements(),
    % First and last timestamp among measurements that have been
    % used to calculate value of aggregated_measurements field
    first_measurement_timestamp :: ts_window:timestamp_seconds(),
    last_measurement_timestamp :: ts_window:timestamp_seconds()
}).


% Information about window used to generate slices
-record(window_info, {
    timestamp :: ts_window:id(),  % id of window (timestamp representing beginning of the window)
    value :: ts_window:value(), % value generated from #window.aggregated_measurements
                                % using ts_window:calculate_window_value function

    % extended_info fields - first and last timestamp among measurements that have been used to calculate value
    % of #window.aggregated_measurements field. If extended_info is not requested, fields are undefined.
    first_measurement_timestamp :: ts_window:timestamp_seconds() | undefined,
    last_measurement_timestamp :: ts_window:timestamp_seconds() | undefined
}).

-endif.
