%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Internal test models definitions.
%%% @end
%%%-------------------------------------------------------------------
-author("Rafal Slota").

-ifndef(DATASTORE_TEST_MODELS_HRL).
-define(DATASTORE_TEST_MODELS_HRL, 1).

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

%% sample model with example fields
-record(link_scopes_test_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% sample model with example fields
-record(link_scopes_test_record2, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

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
