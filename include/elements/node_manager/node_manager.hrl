%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions used by node manager & its plugins
%%% @end
%%%-------------------------------------------------------------------

-ifndef(NODE_MANAGER_HRL).
-define(NODE_MANAGER_HRL, 1).

%% This record is used by node_manager (it contains its state).
%% It hold the status of connection to cluster manager.
-record(state, {
  node_ip = {127, 0, 0, 1} :: {A :: byte(), B :: byte(), C :: byte(), D :: byte()},
  cm_con_status = not_connected :: not_connected | connected | registered,
  initialized = {false, 0} :: true | {false, TriesNum :: non_neg_integer()},
  monitoring_state = undefined :: monitoring:node_monitoring_state(),
  scheduler_info = undefined :: undefined | list(),
  % TODO - better task manager
  task_control = false,
  last_cache_cleaning = {0,0,0},
  cache_cleaning_pid = undefined :: undefined | pid(),
  last_state_analysis = {0,0,0},
  throttling = true
}).

-endif.