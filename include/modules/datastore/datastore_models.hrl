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
    timestamp = null :: datastore_doc:timestamp(),
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

-record(node_management, {
    value :: term()
}).

% Holds information about a Graph Sync session - each GS connection has its
% own gs_session
-record(gs_session, {
    id :: gs_protocol:session_id(),
    auth :: aai:auth(),
    conn_ref :: gs_server:conn_ref(),
    protocol_version :: gs_protocol:protocol_version(),
    translator :: gs_server:translator()
}).

% Holds a list of subscriptions of a subscriber - related 1:1 with gs_session
-record(gs_subscriber, {
    subscriptions = [] :: gs_persistence:subscriptions()
}).

% Holds a list of subscribers (session ids) for resources of a certain entity.
-record(gs_subscription, {
    subscribers = #{} :: gs_persistence:entity_subscribers()
}).

% Models for traversing via different structures (see traverse.erl)
-record(traverse_task, {
    callback_module :: traverse:callback_module(),
    creator :: traverse:environment_id(),
    executor :: traverse:environment_id(),
    group :: traverse:group(),

    % Used for tasks sorting (see traverse_task_list.erl)
    schedule_time = 0 :: traverse:timestamp(),
    start_time = 0 :: traverse:timestamp(),
    finish_time = 0 :: traverse:timestamp(),

    main_job_id = <<>> :: traverse:job_id(), % First job used to init task (see traverse.erl)
    enqueued = true :: boolean(),
    canceled = false :: boolean(),
    node :: undefined | node(),
    status = scheduled :: traverse:status(),
    description = #{} :: traverse:description(),
    additional_data = #{} :: traverse:additional_data(),

    single_master_job_mode :: boolean()
}).

-record(traverse_tasks_scheduler, {
    pool :: traverse:pool(),
    ongoing_tasks = 0 :: non_neg_integer(),
    ongoing_tasks_per_node = #{} :: traverse_tasks_scheduler:ongoing_tasks_map(),
    ongoing_tasks_limit = 0 :: traverse_tasks_scheduler:ongoing_tasks_limit(),
    ongoing_tasks_per_node_limit = 0 :: traverse_tasks_scheduler:ongoing_tasks_limit(),
    groups = [] :: [traverse:group()],
    nodes = [] :: [node()]
}).

-record(cluster_generation, {
    generation :: node_manager:cluster_generation()
}).

-record(view_traverse_job, {
    task_id :: traverse:id(),
    pool :: traverse:pool(),
    view_name :: couchbase_driver:view(),
    view_processing_module :: view_traverse:view_processing_module(),
    token :: view_traverse:token(),
    query_opts :: view_traverse:query_opts(),
    async_next_batch_job :: boolean(),
    info :: view_traverse:info()
}).

% Model used for performance testing (mocked models cannot be used as they affect performance)
-record(performance_test_record, {
    value :: binary()
}).

% Record representing services running on particular node
-record(node_internal_services, {
    services :: #{internal_service:service_name() => internal_service:service()},
    processing_node :: node() % services can be migrated in case of failure
}).

-endif.
