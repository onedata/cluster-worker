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
-record(document, {
    key :: datastore:ext_key(),
    rev :: term(),
    value :: datastore:value(),
    links :: term()
}).

%% sample model with example fields
-record(some_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% Model that controls utilization of cache
-record(cache_controller, {
    timestamp = {0, 0, 0} :: erlang:timestamp(),
    action = non :: atom(),
    last_user = non :: string() | non,
    last_action_time = {0, 0, 0} :: erlang:timestamp(),
    deleted_links = [] :: list()
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
