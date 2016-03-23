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

%% sample model with example fields
-record(globally_cached_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% sample model with example fields
-record(locally_cached_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% sample model with example fields
-record(global_only_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% sample model with example fields
-record(global_only_no_transactions_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% sample model with example fields
-record(local_only_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% sample model with example fields
-record(disk_only_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% sample model with example fields
-record(globally_cached_sync_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% sample model with example fields
-record(locally_cached_sync_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% Model that controls utilization of cache
-record(cache_controller, {
    timestamp = {0, 0, 0} :: erlang:timestamp(),
    action = non :: atom(),
    last_user = non :: string() | non,
    last_action_time = {0, 0, 0} :: erlang:timestamp()
}).

%% Description of task to be done
-record(task_pool, {
    task :: task_manager:task(),
    owner :: pid(),
    node :: node()
}).

%% TODO: inject in tests, VFS-1630
%% Test models
-record(test_record_1, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

-record(test_record_2, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

-endif.
