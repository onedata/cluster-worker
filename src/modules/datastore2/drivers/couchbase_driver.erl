%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an interface to a CouchBase store.
%%% All exposed operations are executed using a dedicated worker pool.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_driver).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models_def.hrl").

%% API
-export([save_async/2, get_async/2, delete_async/2, wait/1]).
-export([save/2, get/2, delete/2]).
-export([get_counter/2, get_counter/3, update_counter/4]).
-export([save_design_doc/3, get_design_doc/2, delete_design_doc/2]).
-export([query_view/4]).

-type ctx() :: #{bucket => couchbase_config:bucket(),
                 mutator => datastore:mutator(),
                 cas => cberl:cas(),
                 no_rev => boolean(),
                 no_seq => boolean(),
                 no_durability => boolean()}.
-type key() :: datastore:key().
-type value() :: datastore:doc() | cberl:value().
-type item() :: datastore:doc() | {cberl:key(), cberl:value()}.
-type rev() :: [couchbase_doc:hash()].
-type design() :: binary().
-type view() :: binary().
-type view_opt() :: {descending, boolean()} |
                    {endkey, binary()} |
                    {endkey_docid, binary()} |
                    {full_set, boolean()} |
                    {group, boolean()} |
                    {group_level, integer()} |
                    {inclusive_end, boolean()} |
                    {key, binary()} |
                    {keys, [binary()]} |
                    {limit, integer()} |
                    {on_error, continue | stop} |
                    {reduce, boolean()} |
                    {skip, integer()} |
                    {stale, false | ok | update_after} |
                    {startkey, binary()} |
                    {startkey_docid, binary()}.

-export_type([ctx/0, key/0, value/0, item/0, rev/0, design/0, view/0,
    view_opt/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously saves value in CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec save_async(ctx(), item()) -> couchbase_pool:future().
save_async(#{bucket := Bucket} = Ctx, #document2{} = Doc) ->
    couchbase_pool:post_async(Bucket, write, {save, Ctx, Doc});
save_async(#{bucket := Bucket} = Ctx, {Key, Value}) ->
    couchbase_pool:post_async(Bucket, write, {save, Ctx, {Key, Value}}).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously retrieves value from CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec get_async(ctx(), key()) -> couchbase_pool:future().
get_async(#{bucket := Bucket}, Key) ->
    couchbase_pool:post_async(Bucket, read, {get, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously removes value from CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec delete_async(ctx(), key()) -> couchbase_pool:future().
delete_async(#{bucket := Bucket} = Ctx, Key) ->
    couchbase_pool:post_async(Bucket, write, {delete, Ctx, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Waits for completion of an asynchronous operation.
%% Fails with a timeout error if awaited operation takes too long.
%% @end
%%--------------------------------------------------------------------
-spec wait(couchbase_pool:future()) -> couchbase_pool:response();
    ([couchbase_pool:future()]) -> [couchbase_pool:response()].
wait(Futures) when is_list(Futures) ->
    [wait(Future) || Future <- Futures];
wait(Future) ->
    couchbase_pool:wait(Future).

%%--------------------------------------------------------------------
%% @doc
%% Saves values in CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), value() | [value()]) -> {ok, cberl:cas(), value()} |
    {error, term()} | [{ok, cberl:cas(), value()} | {error, term()}].
save(Ctx, Values) when is_list(Values) ->
    wait([save_async(Ctx, Value) || Value <- Values]);
save(Ctx, Value) ->
    hd(save(Ctx, [Value])).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves values from CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), key()) -> {ok, cberl:cas(), value()} | {error, term()};
    (ctx(), [key()]) -> [{ok, cberl:cas(), value()} | {error, term()}].
get(Ctx, Keys) when is_list(Keys) ->
    wait([get_async(Ctx, Key) || Key <- Keys]);
get(Ctx, Key) ->
    hd(get(Ctx, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% Removes value from CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(), key()) -> ok | {error, term()};
    (ctx(), [key()]) -> [ok | {error, term()}].
delete(Ctx, Keys) when is_list(Keys) ->
    wait([delete_async(Ctx, Key) || Key <- Keys]);
delete(Ctx, Key) ->
    hd(delete(Ctx, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% Returns counter value from a database.
%% @end
%%--------------------------------------------------------------------
-spec get_counter(ctx(), key()) ->
    {ok, cberl:cas(), non_neg_integer()} | {error, term()}.
get_counter(Ctx, Key) ->
    case get(Ctx, Key) of
        {ok, Cas, Value} when is_integer(Value) ->
            {ok, Cas, Value};
        {ok, Cas, Value} when is_binary(Value) ->
            {ok, Cas, binary_to_integer(Value)};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns counter value from a database.
%% If counter does not exist and default value is provided, it is created.
%% @end
%%--------------------------------------------------------------------
-spec get_counter(ctx(), key(), cberl:arithmetic_default()) ->
    {ok, cberl:cas(), non_neg_integer()} | {error, term()}.
get_counter(#{bucket := Bucket}, Key, Default) ->
    couchbase_pool:post(Bucket, read, {get_counter, Key, Default}).

%%--------------------------------------------------------------------
%% @doc
%% Updates counter value by delta in a database.
%% If counter does not exist and default value is provided, it is created.
%% @end
%%--------------------------------------------------------------------
-spec update_counter(ctx(), key(), cberl:arithmetic_delta(),
    cberl:arithmetic_default()) ->
    {ok, cberl:cas(), non_neg_integer()} | {error, term()}.
update_counter(#{bucket := Bucket}, Key, Delta, Default) ->
    couchbase_pool:post(Bucket, write, {update_counter, Key, Delta, Default}).

%%--------------------------------------------------------------------
%% @doc
%% Saves design document in a database.
%% @end
%%--------------------------------------------------------------------
-spec save_design_doc(ctx(), design(), datastore_json2:ejson()) ->
    ok | {error, term()}.
save_design_doc(#{bucket := Bucket}, DesignName, EJson) ->
    couchbase_pool:post(Bucket, write, {save_design_doc, DesignName, EJson}).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves design document from a database.
%% @end
%%--------------------------------------------------------------------
-spec get_design_doc(ctx(), design()) ->
    {ok, datastore_json2:ejson()} | {error, term()}.
get_design_doc(#{bucket := Bucket}, DesignName) ->
    couchbase_pool:post(Bucket, read, {get_design_doc, DesignName}).

%%--------------------------------------------------------------------
%% @doc
%% Removes design document from a database.
%% @end
%%--------------------------------------------------------------------
-spec delete_design_doc(ctx(), design()) -> ok | {error, term()}.
delete_design_doc(#{bucket := Bucket}, DesignName) ->
    couchbase_pool:post(Bucket, write, {delete_design_doc, DesignName}).

%%--------------------------------------------------------------------
%% @doc
%% Returns view response from a database.
%% @end
%%--------------------------------------------------------------------
-spec query_view(ctx(), design(), view(), [view_opt()]) ->
    {ok, datastore_json2:ejson()} | {error, term()}.
query_view(#{bucket := Bucket}, DesignName, ViewName, Opts) ->
    couchbase_pool:post(Bucket, read, {query_view, DesignName, ViewName, Opts}).