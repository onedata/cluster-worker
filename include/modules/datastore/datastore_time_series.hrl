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
    windows = ts_windows:init() :: ts_windows:windows_collection(),
    older_node_key :: ts_metric_data_node:key() | undefined,
    % Timestamp of newest measurement in older data node
    older_node_timestamp :: ts_windows:timestamp_seconds() | undefined
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


% Record storing value of window
-record(window_value, {
    aggregated_measurements :: ts_windows:aggregated_value(),
    lowest_timestamp :: ts_windows:timestamp_seconds() | undefined,
    highest_timestamp :: ts_windows:timestamp_seconds() | undefined
}).

-endif.
