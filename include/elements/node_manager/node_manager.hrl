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
%% It holds the status of connection to cluster manager.
-record(state, {
    cm_con_status = not_connected :: not_connected | connected | registered,
    initialized = {false, 0} :: true | {false, TriesNum :: non_neg_integer()},
    monitoring_state = undefined :: monitoring:node_monitoring_state(),
    scheduler_info = undefined :: undefined | list(),
    task_control = false,
    last_state_analysis = {stopwatch:start(), undefined} :: node_manager:last_state_analysis(),
    throttling = true,
    % Holds a unique reference for each service that is regenerated only upon
    % rescheduling of periodic healthchecks and otherwise stays the same through
    % all consecutive runs. Used to detect that a scheduled healthcheck has been
    % overriden in the meantime and should be ignored.
    service_healthcheck_generations = #{} :: #{internal_service:service_name() => reference()}
}).

-endif.