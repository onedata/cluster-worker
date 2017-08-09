%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides CRUD interface to a CouchBase database.
%%% It should be used only by CouchBase pool workers.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_crud).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([save/2, get/2, delete/2]).
-export([get_counter/3, update_counter/4]).

% For tests
-export([init_batch_size_check/1, timeout/0, execute_and_check_batch_size/3]).

-type save_request() :: {couchbase_driver:ctx(), couchbase_driver:key(),
                         couchbase_driver:value()}.
-type get_request() :: couchbase_driver:key().
-type delete_request() :: {couchbase_driver:ctx(), couchbase_driver:key()}.

-export_type([save_request/0, get_request/0, delete_request/0]).

-type response(R) :: {couchbase_driver:key(), R | {error, term()}}.
-type save_response() :: response({ok, cberl:cas(), couchbase_driver:value()}).
-type get_response() :: response({ok, cberl:cas(), couchbase_driver:value()}).
-type delete_response() :: response(ok).

-export_type([save_response/0, get_response/0, delete_response/0]).

-type save_requests_map() :: #{couchbase_driver:key() => {
    couchbase_driver:ctx(),
    {ok, cberl:cas(), couchbase_driver:value()} | {error, term()}
}}.
-type change_key_map() :: #{couchbase_driver:key() => couchbase_driver:key()}.

-define(OP_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME,
    couchbase_operation_timeout, 60000)).
-define(DUR_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME,
    couchbase_durability_timeout, 60000)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves key-value pairs in a database.
%% @end
%%--------------------------------------------------------------------
-spec save(cberl:connection(), [save_request()]) -> [save_response()].
save(_Connection, []) ->
    [];
save(Connection, Requests) ->
    init_batch_size_check(Requests),
    CountByScope = count_by_scope(Requests),
    SeqsByScope = allocate_seq(Connection, CountByScope),
    SaveRequests = prepare_save_requests(SeqsByScope, Requests),

    {ChangeStoreRequests, ChangeKeys} = prepare_change_store(SaveRequests),
    ChangeStoreResponses = store(Connection, ChangeStoreRequests),
    ChangeStoreResponses2 = replace_change_keys(ChangeKeys, ChangeStoreResponses),
    SaveRequests2 = update_save_responses(ChangeStoreResponses2, SaveRequests),

    {ChangeDurableRequests, ChangeKeys2} = prepare_change_durable(SaveRequests2),
    ChangeDurableResponses = wait_durable(Connection, ChangeDurableRequests),
    ChangeDurableResponses2 = replace_change_keys(ChangeKeys2, ChangeDurableResponses),
    SaveRequests3 = update_save_responses(ChangeDurableResponses2, SaveRequests2),

    {StoreRequests, SaveRequests4} = prepare_store(SaveRequests3),
    StoreResponses = store(Connection, StoreRequests),
    SaveRequests5 = update_save_responses(StoreResponses, SaveRequests4),

    DurableRequests = prepare_durable(SaveRequests5),
    DurableResponses = wait_durable(Connection, DurableRequests),
    SaveRequests6 = update_save_responses(DurableResponses, SaveRequests5),

    maps:fold(fun(Key, {_Ctx, Response}, SaveResponses) ->
        [{Key, Response} | SaveResponses]
    end, [], SaveRequests6).

%%--------------------------------------------------------------------
%% @doc
%% Returns values associated with keys from a database.
%% @end
%%--------------------------------------------------------------------
-spec get(cberl:connection(), [get_request()]) -> [get_response()].
get(_Connection, []) ->
    [];
get(Connection, Requests) ->
    GetRequests = [{Key, 0, false} || Key <- Requests],
    case cberl:bulk_get(Connection, GetRequests, ?OP_TIMEOUT) of
        {ok, Responses} ->
            lists:map(fun
                ({Key, {ok, Cas, {_} = EJson}}) ->
                    try
                        {Key, {ok, Cas, datastore_json2:decode(EJson)}}
                    catch
                        _:Reason ->
                            {Key, {error, {Reason, erlang:get_stacktrace()}}}
                    end;
                ({Key, {ok, Cas, Value}}) ->
                    {Key, {ok, Cas, Value}};
                ({Key, {error, etimedout}}) ->
                    timeout(),
                    {Key, {error, etimedout}};
                ({Key, {error, Reason}}) ->
                    {Key, {error, Reason}}
            end, Responses);
        {error, etimedout} = E ->
            timeout(),
            [{Key, E} || Key <- Requests];
        {error, timeout} = E ->
            timeout(),
            [{Key, E} || Key <- Requests];
        {error, Reason} ->
            [{Key, {error, Reason}} || Key <- Requests]
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes values associated with keys in a database.
%% @end
%%--------------------------------------------------------------------
-spec delete(cberl:connection(), [delete_request()]) -> [delete_response()].
delete(_Connection, []) ->
    [];
delete(Connection, Requests) ->
    RemoveRequests = [{Key, maps:get(cas, Ctx, 0)} || {Ctx, Key} <- Requests],
    case cberl:bulk_remove(Connection, RemoveRequests, ?OP_TIMEOUT) of
        {ok, Responses} ->
            lists:foreach(fun
                ({Key, {error, etimedout}}) ->
                    timeout();
                (_) ->
                    ok
            end, Responses),
            Responses;
        {error, etimedout} = E ->
            timeout(),
            [{Key, E} || {_, Key} <- Requests];
        {error, timeout} = E ->
            timeout(),
            [{Key, E} || {_, Key} <- Requests];
        {error, Reason} ->
            [{Key, {error, Reason}} || {_, Key} <- Requests]
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns counter value from a database.
%% If counter does not exist and default value is provided, it is created.
%% @end
%%--------------------------------------------------------------------
-spec get_counter(cberl:connection(), couchbase_driver:key(),
    cberl:arithmetic_default()) ->
    {ok, cberl:cas(), non_neg_integer()} | {error, term()}.
get_counter(Connection, Key, Default) ->
    update_counter(Connection, Key, 0, Default).

%%--------------------------------------------------------------------
%% @doc
%% Updates counter value by delta in a database.
%% If counter does not exist and default value is provided, it is created.
%% @end
%%--------------------------------------------------------------------
-spec update_counter(cberl:connection(), couchbase_driver:key(),
    cberl:arithmetic_delta(), cberl:arithmetic_default()) ->
    {ok, cberl:cas(), non_neg_integer()} | {error, term()}.
update_counter(Connection, Key, Delta, Default) ->
    cberl:arithmetic(Connection, Key, Delta, Default, 0, ?OP_TIMEOUT).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns values count by scope.
%% @end
%%--------------------------------------------------------------------
-spec count_by_scope([save_request()]) -> #{datastore:scope() => pos_integer()}.
count_by_scope(Requests) ->
    lists:foldl(fun
        ({#{no_seq := true}, _Key, #document{}}, Scopes) ->
            Scopes;
        ({_Ctx, _Key, #document{scope = Scope}}, Scopes) ->
            Delta = maps:get(Scope, Scopes, 0),
            maps:put(Scope, Delta + 1, Scopes);
        ({_Ctx, _Key, _Value}, Scopes) ->
            Scopes
    end, #{}, Requests).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Allocates sequence numbers by scopes.
%% @end
%%--------------------------------------------------------------------
-spec allocate_seq(cberl:connection(), #{datastore:scope() => pos_integer()}) ->
    #{datastore:scope() => [pos_integer()]}.
allocate_seq(Connection, CountByScope) ->
    maps:fold(fun(Scope, Count, SeqByScope) ->
        Key = couchbase_changes:get_seq_key(Scope),
        case update_counter(Connection, Key, Count, Count) of
            {ok, _, Seq} ->
                Seqs = lists:seq(Seq - Count + 1, Seq),
                maps:put(Scope, {ok, Seqs}, SeqByScope);
            {error, Reason} ->
                maps:put(Scope, {error, Reason}, SeqByScope)
        end
    end, #{}, CountByScope).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Builds save requests map and fills sequence numbers in documents.
%% @end
%%--------------------------------------------------------------------
-spec prepare_save_requests(#{datastore:scope() => [pos_integer()]},
    [save_request()]) -> save_requests_map().
prepare_save_requests(SeqsByScope, Requests) ->
    {_, Requests3} = lists:foldl(fun
        (
            {Ctx = #{no_seq := true}, Key, Doc = #document{}},
            {SeqsByScope2, Requests2}
        ) ->
            {SeqsByScope2, maps:put(Key, {Ctx, {ok, 0, Doc}}, Requests2)};
        (
            {Ctx, Key, Doc = #document{scope = Scope}},
            {SeqsByScope2, Requests2}
        ) ->
            case maps:get(Scope, SeqsByScope2) of
                {ok, [Seq | Seqs]} ->
                    Doc2 = Doc#document{seq = Seq},
                    {
                        maps:put(Scope, {ok, Seqs}, SeqsByScope2),
                        maps:put(Key, {Ctx, {ok, 0, Doc2}}, Requests2)
                    };
                {error, Reason} ->
                    {
                        SeqsByScope2,
                        maps:put(Key, {Ctx, {error, Reason}}, Requests2)
                    }
            end;
        ({Ctx, Key, Value}, {SeqsByScope2, Requests2}) ->
            {SeqsByScope2, maps:put(Key, {Ctx, {ok, 0, Value}}, Requests2)}
    end, {SeqsByScope, #{}}, Requests),
    Requests3.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Prepares bulk store request for change documents.
%% @end
%%--------------------------------------------------------------------
-spec prepare_change_store(save_requests_map()) ->
    {[cberl:store_request()], change_key_map()}.
prepare_change_store(Requests) ->
    maps:fold(fun
        (_Key, {_, {error, _}}, {ChangeStoreRequests, ChangeKeys}) ->
            {ChangeStoreRequests, ChangeKeys};
        (_Key, {#{no_seq := true}, _}, {ChangeStoreRequests, ChangeKeys}) ->
            {ChangeStoreRequests, ChangeKeys};
        (Key, {_, {ok, _, Doc = #document{}}}, {ChangeStoreRequests, ChangeKeys}) ->
            #document{scope = Scope, seq = Seq} = Doc,
            ChangeKey = couchbase_changes:get_change_key(Scope, Seq),
            Json = jiffy:encode({[
                {<<"key">>, Key},
                {<<"pid">>, base64:encode(term_to_binary(self()))}
            ]}),
            {
                [{set, ChangeKey, Json, raw, 0, 0} | ChangeStoreRequests],
                maps:put(ChangeKey, Key, ChangeKeys)
            };
        (_Key, {_, _}, {ChangeStoreRequests, ChangeKeys}) ->
            {ChangeStoreRequests, ChangeKeys}
    end, {[], #{}}, Requests).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Prepares bulk durability check request for change documents.
%% @end
%%--------------------------------------------------------------------
-spec prepare_change_durable(save_requests_map()) ->
    {[cberl:durability_request()], change_key_map()}.
prepare_change_durable(Requests) ->
    maps:fold(fun
        (_Key, {_, {error, _}}, {ChangeDurableRequests, ChangeKeys}) ->
            {ChangeDurableRequests, ChangeKeys};
        (_Key, {#{no_seq := true}, _}, {ChangeDurableRequests, ChangeKeys}) ->
            {ChangeDurableRequests, ChangeKeys};
        (_Key, {#{no_durability := true}, _}, {ChangeDurableRequests, ChangeKeys}) ->
            {ChangeDurableRequests, ChangeKeys};
        (Key, {_, {ok, Cas, Doc = #document{}}}, {ChangeDurableRequests, ChangeKeys}) ->
            #document{scope = Scope, seq = Seq} = Doc,
            ChangeKey = couchbase_changes:get_change_key(Scope, Seq),
            {
                [{ChangeKey, Cas} | ChangeDurableRequests],
                maps:put(ChangeKey, Key, ChangeKeys)
            };
        (_Key, {_, _}, {ChangeDurableRequests, ChangeKeys}) ->
            {ChangeDurableRequests, ChangeKeys}
    end, {[], #{}}, Requests).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Prepares bulk store request and future save responses.
%% @end
%%--------------------------------------------------------------------
-spec prepare_store(save_requests_map()) ->
    {[cberl:store_request()], save_requests_map()}.
prepare_store(Requests) ->
    maps:fold(fun
        (Key, {_, {error, _Reason}} = Request, {StoreRequests, Requests2}) ->
            {
                StoreRequests,
                maps:put(Key, Request, Requests2)
            };
        (Key, {Ctx, {ok, _, Doc = #document{}}}, {StoreRequests, Requests2}) ->
            Doc2 = couchbase_doc:set_mutator(Ctx, Doc),
            {Doc3, EJson} = couchbase_doc:set_next_rev(Ctx, Doc2),
            Cas = maps:get(cas, Ctx, 0),
            {
                [{set, Key, EJson, json, Cas, 0} | StoreRequests],
                maps:put(Key, {Ctx, {ok, Cas, Doc3}}, Requests2)
            };
        (Key, {Ctx, {ok, _, Value}}, {StoreRequests, Responses}) ->
            Cas = maps:get(cas, Ctx, 0),
            {
                [{set, Key, Value, json, Cas, 0} | StoreRequests],
                maps:put(Key, {Ctx, {ok, Cas, Value}}, Responses)
            }
    end, {[], #{}}, Requests).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes bulk store request.
%% @end
%%--------------------------------------------------------------------
-spec store(cberl:connection(), [cberl:store_request()]) ->
    [cberl:store_response()].
store(_Connection, []) ->
    [];
store(Connection, Requests) ->
    Ans = execute_and_check_batch_size(cberl, bulk_store,
        [Connection, Requests, ?OP_TIMEOUT]),
    case Ans of
        {ok, Responses} ->
            lists:foreach(fun
                ({Key, {error, etimedout}}) ->
                    timeout();
                (_) ->
                    ok
            end, Responses),
            Responses;
        {error, etimedout} = E ->
            timeout(),
            [{Key, E} || {_, Key, _, _, _, _} <- Requests];
        {error, timeout} = E ->
            timeout(),
            [{Key, E} || {_, Key, _, _, _, _} <- Requests];
        {error, Reason} ->
            [{Key, {error, Reason}} || {_, Key, _, _, _, _} <- Requests]
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Prepares bulk durability check request.
%% @end
%%--------------------------------------------------------------------
-spec prepare_durable(save_requests_map()) -> [cberl:durability_request()].
prepare_durable(Requests) ->
    maps:fold(fun
        (_Key, {_, {error, _}}, DurableRequests) ->
            DurableRequests;
        (_Key, {#{no_durability := true}, _}, DurableRequests) ->
            DurableRequests;
        (Key, {_, {ok, Cas, _}}, DurableRequests) ->
            [{Key, Cas} | DurableRequests]
    end, [], Requests).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes bulk durability check request.
%% @end
%%--------------------------------------------------------------------
-spec wait_durable(cberl:connection(), [cberl:durability_request()]) ->
    [cberl:durability_response()].
wait_durable(Connection, Requests) ->
    wait_durable(Connection, Requests, 5).
wait_durable(_Connection, [], _) ->
    [];
wait_durable(Connection, Requests, Num) ->
    put(timeout, false),
    Ans = execute_and_check_batch_size(cberl, bulk_durability,
        [Connection, Requests, {1, -1}, ?DUR_TIMEOUT]),
    A = case Ans of
        {ok, Responses} ->
            lists:foreach(fun
                ({Key, {error, etimedout}}) ->
                    timeout();
                (_) ->
                    ok
            end, Responses),
            Responses;
        {error, etimedout} = E ->
            timeout(),
            [{Key, E} || {Key, _} <- Requests];
        {error, timeout} = E ->
            timeout(),
            [{Key, E} || {Key, _} <- Requests];
        {error, Reason} ->
            [{Key, {error, Reason}} || {Key, _} <- Requests]
    end,

    case get(timeout) of
        true when Num > 1 ->
            wait_durable(Connection, Requests, Num - 1);
        _ ->
            A
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replaces change key with a document key in change store or durability
%% requests.
%% @end
%%--------------------------------------------------------------------
-spec replace_change_keys(change_key_map(),
    [cberl:store_response() | cberl:durability_response()]) ->
    [cberl:store_response() | cberl:durability_response()].
replace_change_keys(ChangeKeys, ChangeResponses) ->
    lists:map(fun({ChangeKey, Response}) ->
        {maps:get(ChangeKey, ChangeKeys), Response}
    end, ChangeResponses).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates future save responses according to store and durability check
%% requests outcomes.
%% @end
%%--------------------------------------------------------------------
-spec update_save_responses([cberl:store_response() | cberl:durability_response()],
    save_requests_map()) -> save_requests_map().
update_save_responses(Responses, SaveRequests) ->
    lists:foldl(fun
        ({Key, {ok, Cas}}, SaveRequests2) ->
            {Ctx, {ok, _Cas, Value}} = maps:get(Key, SaveRequests2),
            maps:put(Key, {Ctx, {ok, Cas, Value}}, SaveRequests2);
        ({Key, {error, Reason}}, SaveRequests2) ->
            {Ctx, _Response} = maps:get(Key, SaveRequests2),
            maps:put(Key, {Ctx, {error, Reason}}, SaveRequests2)
    end, SaveRequests, Responses).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes batch size checking if batch size is appropriate.
%% @end
%%--------------------------------------------------------------------
-spec init_batch_size_check(list()) -> ok.
init_batch_size_check(Requests) ->
    BS = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_batch_size, 2000),
    MaxBS = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_pool_max_batch_size, 2000),
    case BS < MaxBS of
        true ->
            case length(Requests) of
                BS ->
                    put(batch_size_check, []);
                _ ->
                    put(batch_size_check, false)
            end;
        _ ->
            put(batch_size_check, false)
    end,
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Decreases batch size as a result of timeout.
%% @end
%%--------------------------------------------------------------------
-spec timeout() -> ok.
timeout() ->
    ?info("Couchbase crud timeout - batch size checking"),
    put(timeout, true),
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
%% Executes function, analysis its execution and increases batch size if needed.
%% @end
%%--------------------------------------------------------------------
-spec execute_and_check_batch_size(Module, Function, Args) -> term() when
    Module :: module(),
    Function :: atom(),
    Args :: [term()].
execute_and_check_batch_size(Module, Fun, Args) ->
    case get(batch_size_check) of
        false ->
            erlang:apply(Module, Fun, Args);
        CheckList ->
            {Time, Ans} = timer:tc(Module, Fun, Args),
            CheckList2 = [(Time / 500) | CheckList],
            put(batch_size_check, CheckList2),
            case length(CheckList2) of
                4 ->
                    verify_batches_times(CheckList2);
                _ ->
                    ok
            end,
            Ans
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
-spec verify_batches_times(list()) -> ok.
verify_batches_times(CheckList) ->
    Ans = lists:foldl(fun
        (_T, false) ->
            false;
        (T, _Acc) ->
            T =< (min(?OP_TIMEOUT, ?DUR_TIMEOUT) / 4)
    end, true, CheckList),

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