%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Internal models definitions. Shall not be included directly
%%% in any erl file.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_MODELS_HRL).
-define(DATASTORE_MODELS_HRL, 1).

%% Wrapper for all models' records
%% Also contains info from couchdb changes stream
%% todo: consider introducing separate record for couchdb stream updates
-record(document, {
    key :: datastore:ext_key(),
    %% holds revision
    %% or revision history (in changes stream)
    rev :: term(),
    %% if record has been deleted  (in changes stream)
    deleted = false :: boolean(),
    value :: datastore:value(),
    links :: term()
}).

%% Model that controls utilization of cache
-record(cache_controller, {
    timestamp = {0, 0, 0} :: erlang:timestamp(),
    action = non :: atom(),
    last_user = non :: string() | non,
    last_action_time = {0, 0, 0} :: erlang:timestamp()
}).

% Max size of cleared_list in cache_consistency_controller
-define(CLEAR_MONITOR_MAX_SIZE, 32).
%% Model that controls consistency of cache
-record(cache_consistency_controller, {
    cleared_list = [] :: [datastore:key() | datastore:link_name()],
    status = ok :: ok | not_monitored | {restoring, pid()}
}).

%% Description of task to be done
-record(task_pool, {
    task :: task_manager:task(),
    owner :: pid(),
    node :: node()
}).

-endif.
