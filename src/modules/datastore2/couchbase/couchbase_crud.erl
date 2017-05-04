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
-export([save/2, get/2, delete/2]).
-export([get_counter/3, update_counter/4]).

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
    {StoreRequests, SaveResponses} = prepare_store(Connection, Requests),
    StoreResponses = store(Connection, StoreRequests),
    SaveResponses2 = update_save_responses(StoreResponses, SaveResponses),
    DurableRequests = prepare_durable(Requests, SaveResponses2),
    DurableResponses = wait_durable(Connection, DurableRequests),
    SaveResponses3 = update_save_responses(DurableResponses, SaveResponses2),
    maps:to_list(SaveResponses3).

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
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Prepares store requests and future save responses.
%% @end
%%--------------------------------------------------------------------
-spec prepare_store(cberl:connection(), [save_request()]) ->
    {[cberl:store_request()], maps:map([save_response()])}.
prepare_store(Connection, Requests) ->
    lists:foldl(fun
        ({Ctx, Key, #document2{} = Doc}, {StoreRequests, Responses}) ->
            try
                Doc2 = couchbase_doc:set_mutator(Ctx, Doc),
                Doc3 = couchbase_doc:set_next_seq(Connection, Ctx, Doc2),
                {Doc4, EJson} = couchbase_doc:set_next_rev(Ctx, Doc3),
                Cas = maps:get(cas, Ctx, 0),
                {
                    [{set, Key, EJson, json, Cas, 0} | StoreRequests],
                    maps:put(Key, {ok, 0, Doc4}, Responses)
                }
            catch
                _:Reason ->
                    Error = {error, {Reason, erlang:get_stacktrace()}},
                    {StoreRequests, maps:put(Key, Error, Responses)}
            end;
        ({Ctx, Key, Value}, {StoreRequests, Responses}) ->
            Cas = maps:get(cas, Ctx, 0),
            {
                [{set, Key, Value, json, Cas, 0} | StoreRequests],
                maps:put(Key, {ok, 0, Value}, Responses)
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
    case cberl:bulk_store(Connection, Requests, ?OP_TIMEOUT) of
        {ok, Responses} ->
            Responses;
        {error, Reason} ->
            [{Key, {error, Reason}} || {_, Key, _, _, _, _} <- Requests]
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Prepares durability check requests.
%% @end
%%--------------------------------------------------------------------
-spec prepare_durable([save_request()], maps:map([save_response()])) ->
    [cberl:durability_request()].
prepare_durable(Requests, SaveResponses) ->
    lists:filtermap(fun
        ({#{no_durability := true}, _, _}) ->
            false;
        ({_, Key, _}) ->
            case maps:get(Key, SaveResponses) of
                {ok, Cas, _} -> {true, {Key, Cas}};
                _ -> false
            end
    end, Requests).

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
    case cberl:bulk_durability(Connection, Requests, {1, -1}, ?DUR_TIMEOUT) of
        {ok, Responses} ->
            Responses;
        {error, Reason} ->
            [{Key, {error, Reason}} || {Key, _} <- Requests]
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates future save responses according to store and durability check
%% requests outcomes.
%% @end
%%--------------------------------------------------------------------
-spec update_save_responses([cberl:store_response() | cberl:durability_response()],
    maps:map([save_response()])) -> maps:map([save_response()]).
update_save_responses(Responses, SaveResponses) ->
    lists:foldl(fun
        ({Key, {ok, Cas}}, SaveResponses2) ->
            {ok, _, Value} = maps:get(Key, SaveResponses2),
            maps:put(Key, {ok, Cas, Value}, SaveResponses2);
        ({Key, {error, Reason}}, SaveResponses2) ->
            maps:put(Key, {error, Reason}, SaveResponses2)
    end, SaveResponses, Responses).