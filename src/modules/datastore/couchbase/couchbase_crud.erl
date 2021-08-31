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
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_save_requests/2, terminate_save_requests/1]).
-export([store_change_docs/2, wait_change_docs_durable/2]).
-export([store_docs/2, wait_docs_durable/2]).
-export([get/2, delete/2]).
-export([get_counter/3, update_counter/4]).

% for tests
-export([prepare_store/1, prepare_change_store/1]).

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
    couchbase_driver:ctx(), {ok, cberl:cas(), couchbase_driver:value()}
}}.
-type change_key_map() :: #{couchbase_driver:key() => couchbase_driver:key()}.

-export_type([save_requests_map/0]).

-define(OP_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME,
    couchbase_operation_timeout, 60000)).
-define(DUR_TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME,
    couchbase_durability_timeout, 300000)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Prepares saves requests by allocating and assigning sequence numbers.
%% In general call to this function should be followed by consecutive calls to
%% {@link store_change_docs/2}, {@link wait_change_docs_durable/2},
%% {@link store_docs/2}, {@link wait_docs_durable/2}
%% and {@link terminate_save_requests/1} functions.
%% @end
%%--------------------------------------------------------------------
-spec init_save_requests(cberl:connection(), [save_request()]) ->
    {save_requests_map(), [save_response()]}.
init_save_requests(Connection, Requests) ->
    CountByScope = count_by_scope(Requests),
    SeqsByScope = allocate_seq(Connection, CountByScope),
    assign_seq(SeqsByScope, Requests).

%%--------------------------------------------------------------------
%% @doc
%% Completes save requests and returns responses.
%% @end
%%--------------------------------------------------------------------
-spec terminate_save_requests(save_requests_map()) -> [save_response()].
terminate_save_requests(SaveRequests) ->
    maps:fold(fun(Key, {_Ctx, Response}, SaveResponses) ->
        [{Key, Response} | SaveResponses]
    end, [], SaveRequests).

%%--------------------------------------------------------------------
%% @doc
%% Stores documents that associates sequence number with a key that has been
%% changed. Returns updated save requests and erroneous responses.
%% @end
%%--------------------------------------------------------------------
-spec store_change_docs(cberl:connection(), save_requests_map()) ->
    {save_requests_map(), [save_response()]}.
store_change_docs(Connection, SaveRequests) ->
    {ChangeStoreRequests, ChangeKeys} = ?MODULE:prepare_change_store(SaveRequests),
    ChangeStoreResponses = store(Connection, ChangeStoreRequests),
    ChangeStoreResponses2 = replace_change_keys(ChangeKeys, ChangeStoreResponses),
    update_save_requests(ChangeStoreResponses2, SaveRequests).

%%--------------------------------------------------------------------
%% @doc
%% Waits until change documents are persisted on disc. Returns updated save
%% requests and erroneous responses.
%% @end
%%--------------------------------------------------------------------
-spec wait_change_docs_durable(cberl:connection(), save_requests_map()) ->
    {save_requests_map(), [save_response()]}.
wait_change_docs_durable(Connection, SaveRequests) ->
    {ChangeDurableRequests, ChangeKeys2} = prepare_change_durable(SaveRequests),
    ChangeDurableResponses = wait_durable(Connection, ChangeDurableRequests),
    ChangeDurableResponses2 = replace_change_keys(ChangeKeys2, ChangeDurableResponses),
    update_save_requests(ChangeDurableResponses2, SaveRequests).

%%--------------------------------------------------------------------
%% @doc
%% Stores documents or key-value pairs. Returns updated save requests
%% and erroneous responses.
%% @end
%%--------------------------------------------------------------------
-spec store_docs(cberl:connection(), save_requests_map()) ->
    {save_requests_map(), [save_response()]}.
store_docs(Connection, SaveRequests) ->
    {StoreRequests, SaveRequests2, SaveResponses} = ?MODULE:prepare_store(SaveRequests),
    StoreResponses = store(Connection, StoreRequests),
    {SaveRequests3, SaveResponses2} = update_save_requests(
        StoreResponses, SaveRequests2
    ),
    {SaveRequests3, SaveResponses ++ SaveResponses2}.

%%--------------------------------------------------------------------
%% @doc
%% Waits until documents are persisted on disc. Returns updated save requests
%% and erroneous responses.
%% @end
%%--------------------------------------------------------------------
-spec wait_docs_durable(cberl:connection(), save_requests_map()) ->
    {save_requests_map(), [save_response()]}.
wait_docs_durable(Connection, SaveRequests) ->
    DurableRequests = prepare_durable(SaveRequests),
    DurableResponses = wait_durable(Connection, DurableRequests),
    update_save_requests(DurableResponses, SaveRequests).

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
                ({Key, {ok, Cas, Value}}) ->
                    try
                        {Key, {ok, Cas, datastore_json:decode(Value)}}
                    catch
                        _:Reason:Stacktrace ->
                            ?error_stacktrace("Cannot decode couchbase value for key ~p~nValue: ~p~nReason: ~p", [
                                Key, Value, Reason
                            ], Stacktrace),
                            {Key, {error, {Reason, Stacktrace}}}
                    end;
                ({Key, {error, Reason}}) ->
                    {Key, {error, Reason}}
            end, Responses);
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
            Responses;
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
-spec count_by_scope([save_request()]) -> #{datastore_doc:scope() => pos_integer()}.
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
-spec allocate_seq(cberl:connection(), #{datastore_doc:scope() => pos_integer()}) ->
    #{datastore_doc:scope() => [pos_integer()]}.
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
-spec assign_seq(#{datastore_doc:scope() => [pos_integer()]},
    [save_request()]) -> {save_requests_map(), [save_response()]}.
assign_seq(SeqsByScope, Requests) ->
    Timestamp = datastore_config:get_timestamp(),
    {_, SaveRequests2, SaveResponses2} = lists:foldl(fun
        (
            {Ctx = #{no_seq := true}, Key, Doc = #document{}},
            {SeqsByScope2, SaveRequests, SaveResponses}
        ) ->
            {
                SeqsByScope2,
                maps:put(Key, {Ctx, {ok, 0, Doc}}, SaveRequests),
                SaveResponses
            };
        (
            {Ctx, Key, Doc = #document{scope = Scope}},
            {SeqsByScope2, SaveRequests, SaveResponses}
        ) ->
            case maps:get(Scope, SeqsByScope2) of
                {ok, [Seq | Seqs]} ->
                    Doc2 = Doc#document{seq = Seq, timestamp = Timestamp},
                    {
                        maps:put(Scope, {ok, Seqs}, SeqsByScope2),
                        maps:put(Key, {Ctx, {ok, 0, Doc2}}, SaveRequests),
                        SaveResponses
                    };
                {error, Reason} ->
                    {
                        SeqsByScope2,
                        SaveRequests,
                        [{Key, {error, Reason}} | SaveResponses]
                    }
            end;
        ({Ctx, Key, Value}, {SeqsByScope2, SaveRequests, SaveResponses}) ->
            {
                SeqsByScope2,
                maps:put(Key, {Ctx, {ok, 0, Value}}, SaveRequests),
                SaveResponses
            }
    end, {SeqsByScope, #{}, []}, Requests),
    {SaveRequests2, SaveResponses2}.

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
        (_Key, {#{no_seq := true}, _}, {ChangeStoreRequests, ChangeKeys}) ->
            {ChangeStoreRequests, ChangeKeys};
        (Key, {Ctx, {ok, _, Doc = #document{}}}, {ChangeStoreRequests, ChangeKeys}) ->
            #document{scope = Scope, seq = Seq} = Doc,
            ChangeKey = couchbase_changes:get_change_key(Scope, Seq),
            EJson = #{
                <<"_record">> => <<"seq">>,
                <<"key">> => Key,
                <<"pid">> => base64:encode(term_to_binary(self()))
            },
            Expiry = maps:get(expiry, Ctx, 0),
            {
                [{set, ChangeKey, EJson, json, 0, Expiry} | ChangeStoreRequests],
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
    {[cberl:store_request()], save_requests_map(), [save_response()]}.
prepare_store(Requests) ->
    maps:fold(fun
        (Key, {Ctx, {ok, _, Value}}, {StoreRequests, Requests2, Responses}) ->
            try
                EJson = datastore_json:encode(Value),
                Cas = maps:get(cas, Ctx, 0),
                Expiry = maps:get(expiry, Ctx, 0),    
                {
                    [{set, Key, EJson, json, Cas, Expiry} | StoreRequests],
                    maps:put(Key, {Ctx, {ok, Cas, Value}}, Requests2),
                    Responses
                }
            catch
                Type:Reason:Stacktrace ->
                    ?warning_stacktrace("Cannot encode document due to ~p:~p~nDoc: ~p", [
                        Type, Reason
                    ], Stacktrace),
                    Reason2 = {Reason, Stacktrace},
                    {
                        StoreRequests,
                        Requests2,
                        [{Key, {error, Reason2}} | Responses]
                    }
            end
    end, {[], #{}, []}, Requests).

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
    case cberl:bulk_store(Connection, Requests, ?OP_TIMEOUT) of
        {ok, Responses} ->
            Responses;
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
wait_durable(_Connection, []) ->
    [];
wait_durable(Connection, Requests) ->
    case application:get_env(?CLUSTER_WORKER_APP_NAME, wait_durable, false) of
        true ->
            case cberl:bulk_durability(Connection, Requests, {1, -1}, ?DUR_TIMEOUT) of
                {ok, Responses} ->
                    Responses;
                {error, Reason} ->
                    [{Key, {error, Reason}} || {Key, _} <- Requests]
            end;
        _ ->
            lists:map(fun({Key, Cas}) -> {Key, {ok, Cas}} end, Requests)
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
-spec update_save_requests([cberl:store_response() | cberl:durability_response()],
    save_requests_map()) -> {save_requests_map(), [save_response()]}.
update_save_requests(Responses, SaveRequests) ->
    lists:foldl(fun
        ({Key, {ok, Cas}}, {SaveRequests2, SaveResponses}) ->
            {Ctx, {ok, _Cas, Value}} = maps:get(Key, SaveRequests2),
            {
                maps:put(Key, {Ctx, {ok, Cas, Value}}, SaveRequests2),
                SaveResponses
            };
        ({Key, {error, Reason}}, {SaveRequests2, SaveResponses}) ->
            {
                maps:remove(Key, SaveRequests2),
                [{Key, {error, Reason}} | SaveResponses]
            }
    end, {SaveRequests, []}, Responses).