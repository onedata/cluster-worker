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
-export([save/2, get/2, remove/2]).
-export([get_counter/3, update_counter/4]).

-type save_request() :: {couchbase_driver:ctx(), couchbase_driver:key(),
                         couchbase_driver:value()}.
-type get_request() :: couchbase_driver:key().
-type remove_request() :: couchbase_driver:key().
-export_type([save_request/0, get_request/0, remove_request/0]).

-type response(R) :: {couchbase_driver:key(), R | {error, term()}}.
-type save_response() :: response(ok | {ok, datastore:doc()}).
-type get_response() :: response({ok, couchbase_driver:value()}).
-type remove_response() :: response(ok).
-export_type([save_response/0, get_response/0, remove_response/0]).

-define(TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME,
    couchbase_request_timeout, 60000)).

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
    {SRequests, DRequests, Responses} =
        prepare_save_requests(Connection, Requests),
    {DRequests2, Responses2} =
        store(Connection, SRequests, DRequests, Responses),
    check_durable(Connection, DRequests2, Responses2).

%%--------------------------------------------------------------------
%% @doc
%% Returns values associated with keys from a database.
%% @end
%%--------------------------------------------------------------------
-spec get(cberl:connection(), [get_request()]) -> [get_response()].
get(_Connection, []) ->
    [];
get(Connection, Keys) ->
    Requests = lists:map(fun(Key) -> {Key, 0, false} end, Keys),
    case cberl:bulk_get(Connection, Requests, ?TIMEOUT) of
        {ok, Responses} ->
            lists:map(fun
                ({Key, {ok, _, {_} = EJson}}) ->
                    try
                        {Key, {ok, datastore_json2:decode(EJson)}}
                    catch
                        _:Reason ->
                            {Key, {error, {Reason, erlang:get_stacktrace()}}}
                    end;
                ({Key, {ok, _, Value}}) ->
                    {Key, {ok, Value}};
                ({Key, {error, Reason}}) ->
                    {Key, {error, Reason}}
            end, Responses);
        {error, Reason} ->
            lists:map(fun(Key) -> {Key, {error, Reason}} end, Keys)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes values associated with keys in a database.
%% @end
%%--------------------------------------------------------------------
-spec remove(cberl:connection(), [remove_request()]) -> [remove_response()].
remove(_Connection, []) ->
    [];
remove(Connection, Keys) ->
    Requests = lists:map(fun(Key) -> {Key, 0} end, Keys),
    case cberl:bulk_remove(Connection, Requests, ?TIMEOUT) of
        {ok, Responses} ->
            Responses;
        {error, Reason} ->
            lists:map(fun(Key) -> {Key, {error, Reason}} end, Keys)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns counter value from a database.
%% If counter does not exist and default value is provided, it is created.
%% @end
%%--------------------------------------------------------------------
-spec get_counter(cberl:connection(), couchbase_driver:key(),
    cberl:arithmetic_default()) -> {ok, non_neg_integer()} | {error, term()}.
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
    {ok, non_neg_integer()} | {error, term()}.
update_counter(Connection, Key, Delta, Default) ->
    case cberl:arithmetic(Connection, Key, Delta, Default, 0, ?TIMEOUT) of
        {ok, _, Value} -> {ok, Value};
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates store/durability requests and future save responses.
%% @end
%%--------------------------------------------------------------------
-spec prepare_save_requests(cberl:connection(), [save_request()]) ->
    {[cberl:store_request()], [cberl:durability_request()], [save_response()]}.
prepare_save_requests(Connection, Requests) ->
    lists:foldl(fun
        ({Ctx, Key, #document2{} = Doc}, {SRequests, DRequests, Responses}) ->
            try
                Doc2 = couchbase_doc:set_mutator(Ctx, Doc),
                Doc3 = couchbase_doc:set_next_seq(Connection, Ctx, Doc2),
                {Doc4, EJson} = couchbase_doc:set_next_rev(Ctx, Doc3),
                {
                    [{set, Key, EJson, json, 0, 0} | SRequests],
                    add_durability_request(Ctx, Key, DRequests),
                    [{Key, {ok, Doc4}} | Responses]
                }
            catch
                _:Reason ->
                    Error = {error, {Reason, erlang:get_stacktrace()}},
                    {SRequests, DRequests, [{Key, Error} | Responses]}
            end;
        ({Ctx, Key, Value}, {SRequests, DRequests, Responses}) ->
            {
                [{set, Key, Value, json, 0, 0} | SRequests],
                add_durability_request(Ctx, Key, DRequests),
                [{Key, ok} | Responses]
            }
    end, {[], [], []}, Requests).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds durability request for a key.
%% @end
%%--------------------------------------------------------------------
-spec add_durability_request(couchbase_driver:ctx(), couchbase_driver:key(),
    [cberl:durability_request()]) -> [cberl:durability_request()].
add_durability_request(#{no_durability := true}, _Key, Requests) ->
    Requests;
add_durability_request(_Ctx, Key, Requests) ->
    [{Key, 0} | Requests].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles store requests and updates durability requests and save responses
%% according to encountered errors.
%% @end
%%--------------------------------------------------------------------
-spec store(cberl:connection(), [cberl:store_request()],
    [cberl:durability_request()], [save_response()]) ->
    {[cberl:durability_request()], [save_response()]}.
store(_Connection, [], DRequests, Responses) ->
    {DRequests, Responses};
store(Connection, SRequests, DRequests, Responses) ->
    case cberl:bulk_store(Connection, SRequests, ?TIMEOUT) of
        {ok, StoreResponses} ->
            lists:foldl(fun
                ({_Key, {ok, _}}, {DRequests2, Responses2}) ->
                    {DRequests2, Responses2};
                ({Key, {error, _} = Error}, {DRequests2, Responses2}) ->
                    {
                        lists:keydelete(Key, 1, DRequests2),
                        lists:keyreplace(Key, 1, Responses2, {Key, Error})
                    }
            end, {DRequests, Responses}, StoreResponses);
        {error, Reason} ->
            Responses3 = lists:foldl(fun({_, Key, _, _, _, _}, Responses2) ->
                lists:keyreplace(Key, 1, Responses2, {Key, {error, Reason}})
            end, Responses, SRequests),
            {[], Responses3}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles durability requests and updates future save responses according to
%% encountered errors.
%% @end
%%--------------------------------------------------------------------
-spec check_durable(cberl:connection(), [cberl:durability_request()],
    [save_response()]) -> [save_response()].
check_durable(_Connection, [], Responses) ->
    Responses;
check_durable(Connection, Requests, Responses) ->
    case cberl:bulk_durability(Connection, Requests, {1, -1}, ?TIMEOUT) of
        {ok, DurabilityResponses} ->
            lists:foldl(fun
                ({_Key, {ok, _}}, Responses2) ->
                    Responses2;
                ({Key, {error, _} = Error}, Responses2) ->
                    lists:keyreplace(Key, 1, Responses2, {Key, Error})
            end, Responses, DurabilityResponses);
        {error, Reason} ->
            lists:foldl(fun({Key, _}, Responses2) ->
                lists:keyreplace(Key, 1, Responses2, {Key, {error, Reason}})
            end, Responses, Requests)
    end.