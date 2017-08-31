%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides API for datastore cache batch operations.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_doc_batch).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([init/0, apply/1, terminate/1]).
-export([init_request/2, terminate_request/2]).
-export([save/4, fetch/3]).

-record(batch, {
    cache :: ets:tid(),
    requests :: ets:tid(),
    request_ref :: undefined | request_ref()
}).

-record(entry, {
    ctx :: ctx() | ets:match_pattern(),
    key :: key() | ets:match_pattern(),
    doc :: doc() | ets:match_pattern(),
    status :: fetched | pending | cached | saved | {error, term()}
}).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type doc() :: datastore:doc().
-type request_ref() :: reference().
-type cached_keys() :: #{key() => ctx()}.
-opaque batch() :: #batch{}.

-export_type([batch/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes datastore documents batch.
%% @end
%%--------------------------------------------------------------------
-spec init() -> batch().
init() ->
    #batch{
        cache = ets:new(datastore_doc_batch_cache, [{keypos, 3}]),
        requests = ets:new(datastore_doc_batch_request, []),
        request_ref = undefined
    }.

%%--------------------------------------------------------------------
%% @doc
%% Applies documents batch on a datastore cache.
%% @end
%%--------------------------------------------------------------------
-spec apply(batch()) -> batch().
apply(Batch = #batch{cache = Cache}) ->
    Requests = ets:select(Cache, [{
        #entry{ctx = '$1', key = '$2', doc = '$3', status = pending},
        [],
        [{{'$1', '$2', '$3'}}]
    }]),
    {_, Keys, _} = lists:unzip3(Requests),
    Responses = datastore_cache:save(Requests),
    Statuses = lists:map(fun
        ({ok, memory, _}) -> cached;
        ({ok, disc, _}) -> saved;
        ({error, Reason}) -> {error, Reason}
    end, Responses),
    lists:foreach(fun({Key, Status}) ->
        ets:update_element(Cache, Key, {5, Status})
    end, lists:zip(Keys, Statuses)),
    Batch.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of pairs of keys and contexts for documents that have been
%% stored in datastore cache.
%% @end
%%--------------------------------------------------------------------
-spec terminate(batch()) -> cached_keys().
terminate(#batch{cache = Cache, requests = Requests}) ->
    CachedKeys = ets:select(Cache, [{
        #entry{ctx = '$1', key = '$2', status = cached, _ = '_'},
        [],
        [{{'$2', '$1'}}]
    }]),
    ets:delete(Cache),
    ets:delete(Requests),
    maps:from_list(CachedKeys).

%%--------------------------------------------------------------------
%% @doc
%% Initializes request context. All operations on datastore batch will be
%% associated with this request until next call to this function, which will
%% initialize next request context.
%% @end
%%--------------------------------------------------------------------
-spec init_request(request_ref(), batch()) -> batch().
init_request(Ref, Batch = #batch{requests = Requests}) ->
    ets:insert(Requests, {Ref, []}),
    Batch#batch{request_ref = Ref}.

%%--------------------------------------------------------------------
%% @doc
%% Checks whether all operations associated with a request have been successful.
%% @end
%%--------------------------------------------------------------------
-spec terminate_request(request_ref(), batch()) -> ok | {error, term()}.
terminate_request(Ref, #batch{cache = Cache, requests = Requests}) ->
    Keys2 = case ets:lookup(Requests, Ref) of
        [{Ref, Keys}] -> Keys;
        [] -> []
    end,
    Responses = lists:map(fun(Key) ->
        [#entry{status = Status}] = ets:lookup(Cache, Key),
        Status
    end, Keys2),
    Responses2 = lists:filter(fun
        ({error, _}) -> true;
        (_) -> false
    end, Responses),
    case Responses2 of
        [] -> ok;
        [Response] -> {error, Response};
        _ -> {error, Responses2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves datastore document in a batch.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), key(), doc(), batch()) -> {{ok, doc()}, batch()}.
save(Ctx, Key, Doc, Batch = #batch{
    cache = Cache, requests = Requests, request_ref = Ref
}) ->
    case ets:lookup(Requests, Ref) of
        [{Ref, Keys}] -> ets:insert(Requests, {Ref, [Key | Keys]});
        [] -> ok
    end,
    ets:insert(Cache, #entry{ctx = Ctx, key = Key, doc = Doc, status = pending}),
    {{ok, Doc}, Batch}.

%%--------------------------------------------------------------------
%% @doc
%% Gets datastore document from a cache. Documents fetch from a disc
%% are stored in a batch.
%% @end
%%--------------------------------------------------------------------
-spec fetch(ctx(), key(), batch()) -> {{ok, doc()} | {error, term()}, batch()}.
fetch(Ctx, Key, Batch = #batch{cache = Cache}) ->
    case ets:lookup(Cache, Key) of
        [#entry{doc = Doc}] -> {{ok, Doc}, Batch};
        [] ->
            case datastore_cache:fetch(Ctx, Key) of
                {ok, memory, Doc} ->
                    {{ok, Doc}, Batch};
                {ok, disc, Doc} ->
                    ets:insert(Cache, #entry{
                        ctx = Ctx, key = Key, doc = Doc, status = fetched
                    }),
                    {{ok, Doc}, Batch};
                {error, Reason} ->
                    {{error, Reason}, Batch}
            end
    end.