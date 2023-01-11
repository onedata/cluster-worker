%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore infinite log records definitions.
%%% For detailed description see {@link infinite_log} module.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(INFINITE_LOG_HRL).
-define(INFINITE_LOG_HRL, 1).

-define(DEFAULT_MAX_ENTRIES_PER_NODE, 1000).

-define(MAX_LISTING_BATCH, 1000).
% possible directions of listing
-define(BACKWARD, backward_from_newest).
-define(FORWARD, forward_from_oldest).

-record(infinite_log_node, {
    entries = []  :: [infinite_log:entry()],
    oldest_timestamp = 0 :: infinite_log:timestamp_millis(),
    newest_timestamp = 0 :: infinite_log:timestamp_millis()
}).

% Each infinite log instance has one #sentinel{} record with the id equal to the
% log id, which holds information required to access entries in the log.
% In addition, it contains the newest node, to which all appends are done until
% it becomes full - to optimize the performance.
-record(infinite_log_sentinel, {
    log_id :: infinite_log:log_id(),
    max_entries_per_node = ?DEFAULT_MAX_ENTRIES_PER_NODE :: pos_integer(),

    % current entry count is equal to (total - oldest_entry_index)
    total_entry_count = 0 :: non_neg_integer(),
    % modified when the log is pruned
    oldest_entry_index = 0 :: non_neg_integer(),

    oldest_timestamp = 0 :: infinite_log:timestamp_millis(),
    newest_timestamp = 0 :: infinite_log:timestamp_millis(),
    oldest_node_timestamp = 0 :: infinite_log:timestamp_millis(),
    buffer = #infinite_log_node{} :: infinite_log_node:record(),

    size_pruning_threshold :: undefined | non_neg_integer(),
    age_pruning_threshold :: undefined | time:seconds(),
    expiry_threshold :: undefined | time:seconds()
}).

-endif.
