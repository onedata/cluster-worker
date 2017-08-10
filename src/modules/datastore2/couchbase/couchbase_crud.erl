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

%% API
-export([get/2, delete/2]).
-export([init_save/2, do_requests/4, do_change_requests/4, finish_save/1]).
-export([get_counter/3, update_counter/4]).
% For apply
-export([store/2, wait_durable/2]).
-export([prepare_change_store/1, prepare_store/1,
    prepare_durable/1, prepare_change_durable/1]).

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
%% Initializes save operation allocating seqs and preparing requests.
%% @end
%%--------------------------------------------------------------------
-spec init_save(cberl:connection(), [save_request()]) ->
    save_requests_map().
init_save(Connection, Requests) ->
    CountByScope = count_by_scope(Requests),
    SeqsByScope = allocate_seq(Connection, CountByScope),
    prepare_save_requests(SeqsByScope, Requests).

%%--------------------------------------------------------------------
%% @doc
%% Does requests to couch.
%% @end
%%--------------------------------------------------------------------
-spec do_requests(cberl:connection(), save_requests_map(),
    RequestFun :: atom(), PrepareFun :: atom()) ->
    {Time :: non_neg_integer(), save_requests_map()}.
do_requests(Connection, Requests, RequestFun, PrepareFun) ->
    {Requests2, ForUpdate} = case  apply(?MODULE, PrepareFun, [Requests]) of
        {R, FU} -> {R, FU};
        R -> {R, Requests}
    end,
    {Time, Responses} =
        apply(?MODULE, RequestFun, [Connection, Requests2]),
    {Time, update_save_responses(Responses, ForUpdate)}.

%%--------------------------------------------------------------------
%% @doc
%% Does requests about seq numbers to couch.
%% @end
%%--------------------------------------------------------------------
-spec do_change_requests(cberl:connection(), save_requests_map(),
    RequestFun :: atom(), PrepareFun :: atom()) ->
    {Time :: non_neg_integer(), save_requests_map()}.
do_change_requests(Connection, Requests, RequestFun, PrepareFun) ->
    {ChangeRequests, ChangeKeys} =
        apply(?MODULE, PrepareFun, [Requests]),
    {Time, ChangeResponses} =
        apply(?MODULE, RequestFun, [Connection, ChangeRequests]),
    ChangeResponses2 = replace_change_keys(ChangeKeys, ChangeResponses),
    {Time, update_save_responses(ChangeResponses2, Requests)}.

%%--------------------------------------------------------------------
%% @doc
%% Finishes save operation translating response to its final form.
%% @end
%%--------------------------------------------------------------------
-spec finish_save(save_requests_map()) ->
    [save_response()].
finish_save(SaveRequests) ->
    maps:fold(fun(Key, {_Ctx, Response}, SaveResponses) ->
        [{Key, Response} | SaveResponses]
    end, [], SaveRequests).

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
%%% Exported for apply
%%%===================================================================

%%--------------------------------------------------------------------
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
%% @doc
%% Executes bulk store request.
%% @end
%%--------------------------------------------------------------------
-spec store(cberl:connection(), [cberl:store_request()]) ->
    {Time :: non_neg_integer(), [cberl:store_response()]}.
store(_Connection, []) ->
    {0, []};
store(Connection, Requests) ->
    {Time, Ans} = timer:tc(cberl, bulk_store,
        [Connection, Requests, ?OP_TIMEOUT]),
    FinalAns = case Ans of
        {ok, Responses} ->
            Responses;
        {error, Reason} ->
            [{Key, {error, Reason}} || {_, Key, _, _, _, _} <- Requests]
    end,
    {round(Time / 1000), FinalAns}.

%%--------------------------------------------------------------------
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
%% @doc
%% Executes bulk durability check request.
%% @end
%%--------------------------------------------------------------------
-spec wait_durable(cberl:connection(), [cberl:durability_request()]) ->
    {Time :: non_neg_integer(), [cberl:durability_response()]}.
wait_durable(_Connection, []) ->
    {0, []};
wait_durable(Connection, Requests) ->
    {Time, Ans} = timer:tc(cberl, bulk_durability,
        [Connection, Requests, {1, -1}, ?DUR_TIMEOUT]),
    FinalAns = case Ans of
        {ok, Responses} ->
            Responses;
        {error, Reason} ->
            [{Key, {error, Reason}} || {Key, _} <- Requests]
    end,
    {round(Time / 1000), FinalAns}.

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
