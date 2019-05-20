%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header contains datastore models definitions.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_MODELS_HRL).
-define(DATASTORE_MODELS_HRL, 1).

-include("datastore_links.hrl").

%% ID of a tree containing links to all instances of a given foldable model.
%% It is used in datastore_model:fold/3 and datastore_model:fold_keys/3 functions.
-define(MODEL_ALL_TREE_ID, <<"all">>).

-record(document, {
    key :: datastore_doc:key(),
    value :: datastore_doc:value(),
    scope = <<>> :: datastore_doc:scope(),
    mutators = [] :: [datastore_doc:mutator()],
    revs = [] :: [datastore_doc:rev()],
    seq = null :: datastore_doc:seq(),
    deleted = false :: boolean(),
    version = 1 :: datastore_doc:version()
}).

-record(task_pool, {
    task :: task_manager:task(),
    task_type :: atom(),
    owner :: undefined | pid() | string(),
    node :: node()
}).

-record(lock, {
    queue = [] :: [lock:queue_element()]
}).

-record(synced_cert, {
    cert_file_content :: undefined | binary(),
    key_file_content :: undefined | binary()
}).

-record(cached_identity, {
    id :: undefined | identity:id(),
    encoded_public_key :: undefined | identity:encoded_public_key(),
    last_update_seconds :: undefined | integer()
}).

-record(node_management, {
    value :: term()
}).

% Holds information about Graph Sync session
-record(gs_session, {
    id :: undefined | gs_protocol:session_id(),
    client :: gs_protocol:client(),
    protocol_version = 0 :: gs_protocol:protocol_version(),
    conn_ref :: undefined | gs_server:conn_ref(),
    translator :: gs_server:translator(),
    subscriptions = [] :: ordsets:ordset(gs_persistence:subscription())
}).

% Holds a list of subscribers for certain resource.
-record(gs_subscription, {
    subscribers = [] :: ordsets:ordset(gs_persistence:subscriber())
}).

-record(traverse_task, {
    task_module :: traverse:task_module(),
    status = waiting :: traverse:status(),
    enqueued = true :: boolean(),
    canceled = false :: boolean(),
    executor :: traverse:executor(),
    group :: traverse:group(),
    description = #{} :: maps:map()
}).

-record(traverse_tasks_load_balance, {
    task_module :: traverse:task_module(),
    ongoing_tasks = 0 :: non_neg_integer(),
    ongoing_tasks_limit = 0 :: non_neg_integer(),
    groups = [] :: [traverse:group()],
    groups_to_cancel = [] :: [traverse:group()]
}).

-endif.
