%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header defines messages exchanged between datastore_writer and datastore_cache_writer.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_PROTOCOL_HRL).
-define(DATASTORE_PROTOCOL_HRL, 1).

% Request sent to datastore_writer by processes that use datastore
-record(datastore_request, {
    module :: module() | undefined,
    function :: atom(),
    ctx :: datastore:ctx(),
    args :: list()
}).

% Internal representation of request by datastore_writer (when request is waiting in queue to be processed)
-record(datastore_internal_request, {
    pid :: pid(), % Pid of calling process
    ref :: reference(), % Reference used by calling process to identify answer
    request :: #datastore_request{}
}).

% Batch of requests to be processed by datastore_cache_writer
% Can be sent between tp processes in case of node failures (see ha_datastore.hrl)
-record(datastore_internal_requests_batch, {
    ref :: reference(),
    requests :: [#datastore_internal_request{}],
    mode :: ha_datastore:slave_mode()
}).

% Request sent to datastore_cache_writer to initiate flushing of keys not resulting from standard message handling
% Used to flush keys from other nodes if these nodes fail (see ha_datastore.hrl)
-record(datastore_flush_request, {
    keys :: datastore_doc_batch:cached_keys()
}).

% Remote documents processing modes
-define(HANDLE_LOCALLY, handle_locally). % Remote node is down - handle locally
-define(DELEGATE, delegate). % Delegate to remote node
-define(IGNORE, ignore). % No action is needed

% Record describing classification of requests that indicates where requests should be processed
-record(qualified_datastore_requests, {
    local_requests :: datastore_writer:requests_internal(),
    remote_requests :: datastore_writer:requests_internal(),
    remote_node :: undefined | node(),
    remote_requests_processing_mode :: datastore_cache_writer:remote_requests_processing_mode()
}).

-endif.
