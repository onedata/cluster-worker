%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an interface for a CouchBase worker pool.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_pool).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").

%% API
-export([post_async/3, post/3, wait/1]).
-export([get_modes/0, get_size/1]).
-export([get_request_queue_size/1, get_request_queue_size/2,
    reset_request_queue_size/3, update_request_queue_size/4]).

-type mode() :: read | write.
-type request() :: {save, couchbase_driver:ctx(), couchbase_driver:item()} |
                   {get, datastore:key()} |
                   {delete, datastore:key()} |
                   {get_counter, datastore:key(), cberl:arithmetic_default()} |
                   {update_counter, datastore:key(), cberl:arithmetic_delta(),
                       cberl:arithmetic_default()} |
                   {save_design_doc, couchbase_driver:design(),
                       datastore_json2:ejson()} |
                   {delete_design_doc, couchbase_driver:design()} |
                   {query_view, couchbase_driver:design(),
                       couchbase_driver:view(), [couchbase_driver:view_opt()]}.
-type response() :: ok | {ok, term()} | {error, term()}.
-type future() :: reference().

-export_type([mode/0, request/0, response/0, future/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Schedules request execution on a worker pool.
%% @end
%%--------------------------------------------------------------------
-spec post_async(couchbase_config:bucket(), mode(), request()) -> future().
post_async(Bucket, Mode, Request) ->
    Ref = make_ref(),
    Id = get_next_worker_id(Bucket, Mode),
    Worker = couchbase_pool_sup:get_worker(Bucket, Mode, Id),
    update_request_queue_size(Bucket, Mode, Id, 1),
    Worker ! {post, {Ref, self(), Request}},
    Ref.

%%--------------------------------------------------------------------
%% @doc
%% Schedules request execution on a worker pool and awaits response.
%% @end
%%--------------------------------------------------------------------
-spec post(couchbase_config:bucket(), mode(), request()) -> response().
post(Bucket, Mode, Request) ->
    wait(post_async(Bucket, Mode, Request)).

%%--------------------------------------------------------------------
%% @doc
%% Waits for response associated with a reference.
%% @end
%%--------------------------------------------------------------------
-spec wait(future()) -> response().
wait(Future) ->
    Timeout = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_request_timeout, 60000),
    receive
        {Future, Response} -> Response
    after
        Timeout -> {error, timeout}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of available worker pool modes.
%% @end
%%--------------------------------------------------------------------
-spec get_modes() -> [mode()].
get_modes() ->
    [read, write].

%%--------------------------------------------------------------------
%% @doc
%% Returns size of worker pool for given mode.
%% @end
%%--------------------------------------------------------------------
-spec get_size(mode()) -> non_neg_integer().
get_size(Mode) ->
    PoolSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_size, []),
    proplists:get_value(Mode, PoolSize, 1).

%%--------------------------------------------------------------------
%% @doc
%% Returns requests queue size for given bucket and all modes.
%% @end
%%--------------------------------------------------------------------
-spec get_request_queue_size(couchbase_config:bucket()) -> non_neg_integer().
get_request_queue_size(Bucket) ->
    lists:foldl(fun(Mode, Size) ->
        Size + get_request_queue_size(Bucket, Mode)
    end, 0, get_modes()).

%%--------------------------------------------------------------------
%% @doc
%% Returns requests queue size for given bucket and mode.
%% @end
%%--------------------------------------------------------------------
-spec get_request_queue_size(couchbase_config:bucket(), mode()) ->
    non_neg_integer().
get_request_queue_size(Bucket, Mode) ->
    lists:foldl(fun(Id, Size) ->
        Key = {request_queue_size, Bucket, Mode, Id},
        Size + ets:lookup_element(couchbase_pool_stats, Key, 2)
    end, 0, lists:seq(1, get_size(Mode))).

%%--------------------------------------------------------------------
%% @doc
%% Set worker's requests queue size to zero.
%% @end
%%--------------------------------------------------------------------
-spec reset_request_queue_size(couchbase_config:bucket(), mode(),
    couchbase_pool_worker:id()) -> ok.
reset_request_queue_size(Bucket, Mode, Id) ->
    Key = {request_queue_size, Bucket, Mode, Id},
    ets:insert(couchbase_pool_stats, {Key, 0}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Updates worker's requests queue size by delta. Prevents queue size from
%% getting below zero.
%% @end
%%--------------------------------------------------------------------
-spec update_request_queue_size(couchbase_config:bucket(), mode(),
    couchbase_pool_worker:id(), integer()) -> ok.
update_request_queue_size(Bucket, Mode, Id, Delta) when Delta < 0 ->
    Key = {request_queue_size, Bucket, Mode, Id},
    ets:update_counter(couchbase_pool_stats, Key, {2, Delta, 0, 0}, {Key, 0}),
    ok;
update_request_queue_size(Bucket, Mode, Id, Delta) ->
    Key = {request_queue_size, Bucket, Mode, Id},
    ets:update_counter(couchbase_pool_stats, Key, {2, Delta}, {Key, 0}),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns next worker ID.
%% @end
%%--------------------------------------------------------------------
-spec get_next_worker_id(couchbase_config:bucket(), mode()) ->
    couchbase_pool_worker:id().
get_next_worker_id(Bucket, Mode) ->
    Key = {next_worker_id, Bucket, Mode},
    Size = get_size(Mode),
    ets:update_counter(couchbase_pool_stats, Key, {2, 1, Size, 1}, {Key, 0}).