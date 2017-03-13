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
    generated_uuid = false :: boolean(),
    value :: datastore:value(),
    links :: term(),
    version :: non_neg_integer() | undefined
}).

% Max size of cleared_list in cache_consistency_controller
% TODO - new cache_consistency_controller for new datastore
-define(CLEAR_MONITOR_MAX_SIZE, 0).
%% Model that controls consistency of cache
-record(cache_consistency_controller, {
    cleared_list = [] :: [datastore:key() | datastore:link_name()],
    status = ok :: ok | not_monitored | {restoring, pid()},
    clearing_counter = 0 :: non_neg_integer(),
    restore_counter  = 0 :: non_neg_integer()
}).

%% Description of task to be done
-record(task_pool, {
    task :: task_manager:task(),
    task_type :: atom(),
    owner :: undefined | pid() | string(),    % MemOpt - float for ets and mnesia
    node :: node()
}).

%% Queue of processes waiting for lock
-record(lock, {
    queue = [] :: [lock:queue_element()]
}).

%% Contents of cert files synced between nodes
-record(synced_cert, {
    cert_file_content :: undefined | binary(),
    key_file_content :: undefined | binary()
}).

% Cached info about identities
-record(cached_identity, {
    id :: undefined | identity:id(),
    encoded_public_key :: undefined | identity:encoded_public_key(),
    last_update_seconds :: undefined | integer()
}).

% Record for various data used during node management
-record(node_management, {
    value :: term()
}).

%% Record for auxiliary_cache_controller model
-record(auxiliary_cache_controller,{
    dummy :: undefined % never used - only for tests (this record is only saved during tests, it is used only for hooks)
}).

%% Record for entry in auxiliary_cache_controller structure
%% key consists of:
%%  * Key which can by any term, it must be value of a field by which
%%    given auxiliary table is ordered
%%  * datastore key of given entity
-record(auxiliary_cache_entry,{
    key :: {Key :: term(), datastore:ext_key()},
    value % currently this field is not used
}).

-endif.
