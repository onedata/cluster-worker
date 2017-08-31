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

-record(links_forest, {
    model :: datastore_model:model(),
    key :: datastore:key(),
    trees = #{} :: links_forest:trees()
}).

-record(links_node, {
    model :: datastore_model:model(),
    key :: datastore:key(),
    node :: undefined | links_node:links_node()
}).

-record(links_mask, {
    model :: datastore_model:model(),
    key :: datastore:key(),
    tree_id :: links_tree:id(),
    links = [] :: [{datastore_links:link_name(), datastore_links:link_rev()}],
    next = <<>> :: datastore:key()
}).

-record(links_mask_root, {
    heads :: #{links_tree:id() => datastore:key()},
    tails :: #{links_tree:id() => datastore:key()}
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

-endif.
