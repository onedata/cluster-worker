%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides API for datastore documents batch.
%%%
%%% Datastore documents batch is a temporary cache object that handles requests
%%% batch and keeps all modified documents as well as documents fetched from
%%% disc that can not be stored in {@link datastore_cache} due to lack of space.
%%% After handling all requests a batch should be applied on a datastore cache
%%% by calling {@link apply/1} function. Batch that has been applied on a cache
%%% should be destroyed by calling {@link terminate/1} function. This function
%%% returns details for documents that has been stored in cache and should be
%%% later on flushed.
%%%
%%% NOTE! A single request may modify multiple documents therefore each request
%%% should be proceeded with a call to {@link init_request/2} function.
%%% To collect request outcome a {@link terminate_request} function should be
%%% called with a request reference. Requests can be terminated only after
%%% successful application of a batch on a datastore cache
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
    cache = #{} :: #{key() => entry()},
    requests = #{} :: #{request_ref() => [key()]},
    request_ref = undefined :: undefined | request_ref()
}).

-record(entry, {
    ctx :: ctx(),
    doc :: doc(),
    status :: fetched | pending | cached | saved | {error, term()}
}).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type doc() :: datastore:doc().
-type entry() :: #entry{}.
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
    #batch{}.

%%--------------------------------------------------------------------
%% @doc
%% Applies documents batch on a datastore cache.
%% @end
%%--------------------------------------------------------------------
-spec apply(batch()) -> batch().
apply(Batch = #batch{cache = Cache}) ->
    Requests = maps:fold(fun
        (Key, #entry{ctx = Ctx, doc = Doc, status = pending}, Acc) ->
            [{Ctx, Key, Doc} | Acc];
        (_Key, _Entry, Acc) ->
            Acc
    end, [], Cache),
    {_, Keys, _} = lists:unzip3(Requests),
    Responses = datastore_cache:save(Requests),
    Statuses = lists:map(fun
        ({ok, memory, _}) -> cached;
        ({ok, disc, _}) -> saved;
        ({error, Reason}) -> {error, Reason}
    end, Responses),
    lists:foldl(fun({Key, Status}, Batch2 = #batch{cache = Cache2}) ->
        Entry = maps:get(Key, Cache2),
        Batch2#batch{
            cache = maps:put(Key, Entry#entry{status = Status}, Cache2)
        }
    end, Batch, lists:zip(Keys, Statuses)).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of pairs of keys and contexts for documents that have been
%% stored in datastore cache.
%% @end
%%--------------------------------------------------------------------
-spec terminate(batch()) -> cached_keys().
terminate(#batch{cache = Cache}) ->
    maps:fold(fun
        (Key, #entry{ctx = Ctx, status = cached}, CachedKeys) ->
            maps:put(Key, Ctx, CachedKeys);
        (_Key, _Entry, CachedKeys) ->
            CachedKeys
    end, #{}, Cache).

%%--------------------------------------------------------------------
%% @doc
%% Initializes request context. All operations on datastore batch will be
%% associated with this request until next call to this function, which will
%% initialize next request context.
%% @end
%%--------------------------------------------------------------------
-spec init_request(request_ref(), batch()) -> batch().
init_request(Ref, Batch = #batch{requests = Requests}) ->
    Batch#batch{
        requests = maps:put(Ref, [], Requests),
        request_ref = Ref
    }.

%%--------------------------------------------------------------------
%% @doc
%% Checks whether all operations associated with a request have been successful.
%% @end
%%--------------------------------------------------------------------
-spec terminate_request(request_ref(), batch()) -> ok | {error, term()}.
terminate_request(Ref, #batch{cache = Cache, requests = Requests}) ->
    Keys = maps:get(Ref, Requests, []),
    Responses = lists:map(fun(Key) ->
        #entry{status = Status} = maps:get(Key, Cache),
        Status
    end, Keys),
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
    Keys = maps:get(Ref, Requests, []),
    Entry = #entry{ctx = Ctx, doc = Doc, status = pending},
    {{ok, Doc}, Batch#batch{
        requests = maps:put(Ref, [Key | Keys], Requests),
        cache = maps:put(Key, Entry, Cache)
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Gets datastore document from a cache. Documents fetched from a disc
%% are stored in a batch.
%% @end
%%--------------------------------------------------------------------
-spec fetch(ctx(), key(), batch()) -> {{ok, doc()} | {error, term()}, batch()}.
fetch(Ctx, Key, Batch = #batch{cache = Cache}) ->
    case maps:find(Key, Cache) of
        {ok, #entry{doc = Doc}} -> {{ok, Doc}, Batch};
        error ->
            case datastore_cache:fetch(Ctx, Key) of
                {ok, memory, Doc} ->
                    {{ok, Doc}, Batch};
                {ok, disc, Doc} ->
                    Entry = #entry{ctx = Ctx, doc = Doc, status = fetched},
                    {{ok, Doc}, Batch#batch{
                        cache = maps:put(Key, Entry, Cache)
                    }};
                {error, Reason} ->
                    {{error, Reason}, Batch}
            end
    end.