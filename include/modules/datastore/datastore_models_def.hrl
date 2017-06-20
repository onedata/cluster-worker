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
    % TODO - models do not use records as keys at model level (change type to key)
    key :: datastore:ext_key(),
    value :: datastore:value(),
    scope = <<>> :: datastore:scope(),
    mutator = [] :: [datastore:mutator()],
    rev = [] :: datastore:rev(),
    seq = 0 :: datastore:seq(),
    deleted = false :: boolean(),
    version = 1 :: datastore_version2:version()
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
