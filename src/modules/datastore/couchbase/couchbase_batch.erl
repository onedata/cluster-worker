%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functionality for couchbase batch size management.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_batch).
-author("Michał Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([check_timeout/1, verify_batch_size_increase/3]).
%% For eunit
-export([decrease_batch_size/0]).

-define(OP_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME,
    couchbase_operation_timeout, 60000)).
-define(DUR_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME,
    couchbase_durability_timeout, 60000)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if timeout appears in the response and changes batch size if needed.
%% @end
%%--------------------------------------------------------------------
-spec check_timeout([couchbase_crud:delete_response()]
    | [couchbase_crud:get_response()] | [couchbase_crud:save_response()]) ->
    ok | timeout.
check_timeout(Responses) ->
    Check = lists:foldl(fun
        ({_Key, {error, etimedout}}, _) ->
            timeout;
        ({_Key, {error, timeout}}, _) ->
            timeout;
        (_, TmpAns) ->
            TmpAns
    end, ok, Responses),

    case Check of
        timeout ->
            decrease_batch_size(),
            timeout;
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if batch size can be increased and increases it if needed.
%% @end
%%--------------------------------------------------------------------
-spec verify_batch_size_increase([couchbase_crud:save_response()], list(), list()) ->
    ok | timeout.
verify_batch_size_increase(Requests, Times, Timeouts) ->
    BatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, 2000),
    MaxBatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_max_batch_size, 2000),
    case (BatchSize < MaxBatchSize)
        andalso (maps:size(Requests) =:= BatchSize) of
        true ->
            verify_batches_times(Times, Timeouts);
        _ ->
            ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Decreases batch size as a result of timeout.
%% @end
%%--------------------------------------------------------------------
-spec decrease_batch_size() -> ok.
decrease_batch_size() ->
    ?info("Couchbase crud timeout - batch size checking"),
    case can_modify_batch_size() of
        true ->
            BatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_batch_size, 2000),
            MinBatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_min_batch_size, 250),
            NewSize = max(round(BatchSize/2), MinBatchSize),
            application:set_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_batch_size, NewSize),
            ?info("Decrease batch size to: ~p", [NewSize]),
            save_modify_batch_size_time();
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if batch size can be modified.
%% @end
%%--------------------------------------------------------------------
-spec can_modify_batch_size() -> boolean().
can_modify_batch_size() ->
    LastMod = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, 0),
    MinDiff = (?OP_TIMEOUT + ?DUR_TIMEOUT) / 1000,
    (os:system_time(seconds) - LastMod) > MinDiff.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves batch modification time.
%% @end
%%--------------------------------------------------------------------
-spec save_modify_batch_size_time() -> ok.
save_modify_batch_size_time() ->
    T = os:system_time(seconds),
    application:set_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size_check_time, T).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Verifies if batch processing times allow increase of batch size and
%% increases it if possible.
%% @end
%%--------------------------------------------------------------------
-spec verify_batches_times(list(), list()) -> ok.
verify_batches_times(CheckList, Timeouts) ->
    Ans = lists:foldl(fun
        (_, false) ->
            false;
        ({_, timeout}, _Acc) ->
            false;
        ({T, _}, _Acc) ->
            T =< (min(?OP_TIMEOUT, ?DUR_TIMEOUT) / 4)
    end, true, lists:zip(CheckList, Timeouts)),

    case {Ans, can_modify_batch_size()} of
        {true, true} ->
            BatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_batch_size, 2000),
            MaxBatchSize = application:get_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_max_batch_size, 2000),
            NewSize = min(round(BatchSize*2), MaxBatchSize),
            application:set_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_batch_size, NewSize),
            ?info("Increase batch size to: ~p", [NewSize]),
            save_modify_batch_size_time();
        {true, _} ->
            ?info("Couchbase crud max batch size write checking"),
            ok;
        _ ->
            ok
    end.