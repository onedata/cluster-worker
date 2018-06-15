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
    subscriptions = [] :: gs_persistence:subscriptions()
}).

% Holds a list of subscribers for certain resource.
-record(gs_subscription, {
    subscribers = #{} :: gs_persistence:subscribers()
}).

-endif.
