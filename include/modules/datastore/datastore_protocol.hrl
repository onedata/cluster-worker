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
    function :: atom(),
    ctx :: datastore:ctx(),
    args :: list()
}).

% Internal representation of request by datastore_writer (when request is waiting in queue to be processed)
-record(datastre_internal_request, {
    pid :: pid(),
    ref :: reference(),
    request :: #datastore_request{}
}).

% Batch of requests to be processed by datastore_cache_writer
% Can be sent between tp processes in case of node failures (see ha_datastore.hrl)
-record(datastre_internal_requests_batch, {
    ref :: reference(),
    requests :: [#datastre_internal_request{}],
    mode :: ha_datastore_utils:slave_mode()
}).

% Request sent to datastore_cache_writer to initiate flushing of keys not resulting from standard message handling
% Used to flush keys from other nodes if these nodes fail (see ha_datastore.hrl)
-record(datastore_flush_request, {
    keys :: datastore_doc_batch:cached_keys()
}).

-endif.
