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
-include("modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([analyse_answer/1, analyse_times/3]).
%% For eunit
-export([timeout/0]).

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
-spec analyse_answer([couchbase_crud:delete_response()]
    | [couchbase_crud:get_response()] | couchbase_crud:save_requests_map()) ->
    ok | timeout.
analyse_answer([]) ->
    ok;
analyse_answer(Responses) when is_list(Responses) ->
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
            timeout(),
            timeout;
        _ ->
            ok
    end;
analyse_answer(Responses) ->
    Check = maps:fold(fun
        (_Key, {_Ctx, {error, etimedout}}, _) ->
            timeout;
        (_Key, {_Ctx, {error, timeout}}, _) ->
            timeout;
        (_, _, TmpAns) ->
            TmpAns
    end, ok, Responses),

    case Check of
        timeout ->
            timeout(),
            timeout;
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if batch size can be increased and increases it if needed.
%% @end
%%--------------------------------------------------------------------
-spec analyse_times(couchbase_crud:save_requests_map(), list(), list()) ->
    ok | timeout.
analyse_times(Responses, Times, Timeouts) ->
    BS = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, 2000),
    MaxBS = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_max_batch_size, 2000),
    case (BS < MaxBS) andalso (maps:size(Responses) =:= BS) of
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
-spec timeout() -> ok.
timeout() ->
    ?info("Couchbase crud timeout - batch size checking ~p",
        [erlang:process_info(self(), current_stacktrace)]),
    case can_modify_batch_size() of
        true ->
            BS = application:get_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_batch_size, 2000),
            MinBS = application:get_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_min_batch_size, 250),
            NewSize = max(round(BS/2), MinBS),
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
            BS = application:get_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_batch_size, 2000),
            MaxBS = application:get_env(?CLUSTER_WORKER_APP_NAME,
                couchbase_pool_max_batch_size, 2000),
            NewSize = min(round(BS*2), MaxBS),
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