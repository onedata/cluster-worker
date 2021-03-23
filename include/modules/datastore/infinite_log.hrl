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

-record(node, {
    entries = []  :: [infinite_log:entry()],
    oldest_timestamp = 0 :: infinite_log:timestamp(),
    newest_timestamp = 0 :: infinite_log:timestamp()
}).

% Each infinite log instance has one #sentinel{} record with the id equal to the
% log id, which holds information required to access entries in the log.
% In addition, it contains the newest node, to which all appends are done until
% it becomes full - to optimize the performance.
-record(sentinel, {
    structure_id :: infinite_log:id(),
    max_entries_per_node :: pos_integer(),
    entry_count = 0 :: non_neg_integer(),
    oldest_timestamp = 0 :: infinite_log:timestamp(),
    newest_timestamp = 0 :: infinite_log:timestamp(),
    buffer = #node{} :: infinite_log:log_node()
}).

-endif.