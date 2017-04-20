%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an interface to a CouchBase database.
%%% All exposed operations are executed using a dedicated worker pool.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_driver).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models_def.hrl").

%% API
-export([save_async/2, get_async/2, delete_async/2, purge_async/2, wait/1]).
-export([save/2, get/2, delete/2, purge/2]).
-export([get_counter/3, update_counter/4]).
-export([save_design_doc/3, delete_design_doc/2, query_view/4]).
-export([get_buckets/0]).

-type ctx() :: #{atom() => term()}.
-type key() :: cberl:key().
-type value() :: datastore:doc() | datastore_json2:ejson().
-type rev() :: [couchbase_doc:hash()].
-type bucket() :: binary().
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
-opaque future() :: {bulk, reference()} | {single, future()}.
-type one_or_many(Type) :: Type | list(Type).

-export_type([ctx/0, key/0, value/0, rev/0, bucket/0, design/0, view/0,
    view_opt/0, future/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously saves raw key-value pair/pairs or document/documents
%% in a database.
%% @end
%%--------------------------------------------------------------------
-spec save_async(ctx(), one_or_many(value())) -> future().
save_async(Ctx, #document2{} = Doc) ->
    {single, save_async(Ctx, [Doc])};
save_async(Ctx, {Key, Value}) ->
    {single, save_async(Ctx, [{Key, Value}])};
save_async(#{bucket := Bucket} = Ctx, Values) when is_list(Values) ->
    Requests = lists:map(fun(Value) -> {Ctx, Value} end, Values),
    {bulk, couchbase_pool:post_async(Bucket, write, {save, Requests})}.

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously retrieves value/values associated with key/keys
%% from a database.
%% @end
%%--------------------------------------------------------------------
-spec get_async(ctx(), one_or_many(key())) -> future().
get_async(Ctx, <<_/binary>> = Key) ->
    {single, get_async(Ctx, [Key])};
get_async(#{bucket := Bucket}, Keys) when is_list(Keys) ->
    {bulk, couchbase_pool:post_async(Bucket, read, {get, Keys})}.

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously marks document/documents as deleted in a database.
%% @end
%%--------------------------------------------------------------------
-spec delete_async(ctx(), one_or_many(datastore:doc())) -> future().
delete_async(Ctx, #document2{} = Doc) ->
    {single, delete_async(Ctx, [Doc])};
delete_async(Ctx, Docs) ->
    Docs2 = lists:map(fun(#document2{} = Doc) ->
        Doc#document2{deleted = true}
    end, Docs),
    save_async(Ctx, Docs2).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously removes value/values associated with key/keys in a database.
%% @end
%%--------------------------------------------------------------------
-spec purge_async(ctx(), one_or_many(key())) -> future().
purge_async(Ctx, <<_/binary>> = Key) ->
    {single, purge_async(Ctx, [Key])};
purge_async(#{bucket := Bucket}, Keys) when is_list(Keys) ->
    {bulk, couchbase_pool:post_async(Bucket, write, {remove, Keys})}.

%%--------------------------------------------------------------------
%% @doc
%% Waits for completion of an asynchronous operation. Fails with a timeout error
%% if awaited operation takes too long.
%% @end
%%--------------------------------------------------------------------
-spec wait(future()) -> couchbase_pool:response().
wait({single, Ref}) ->
    case wait(Ref) of
        {error, Reason} -> {error, Reason};
        Response when is_list(Response) -> hd(Response);
        Response -> Response
    end;
wait({bulk, Ref}) ->
    couchbase_pool:wait(Ref).

%%--------------------------------------------------------------------
%% @doc
%% Saves key-value pair/pairs or document/documents in a database.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), one_or_many(value())) ->
    one_or_many(ok | {ok, datastore:doc()} | {error, term()}).
save(Ctx, Values) ->
    wait(save_async(Ctx, Values)).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves value/values associated with key/keys from a database.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), one_or_many(key())) ->
    one_or_many({ok, value()} | {error, term()}).
get(Ctx, Keys) ->
    wait(get_async(Ctx, Keys)).

%%--------------------------------------------------------------------
%% @doc
%% Marks document/documents as deleted in a database.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(), one_or_many(datastore:doc())) ->
    one_or_many({ok, datastore:doc()} | {error, term()}).
delete(Ctx, Docs) ->
    wait(delete_async(Ctx, Docs)).

%%--------------------------------------------------------------------
%% @doc
%% Removes value/values associated with key/keys in a database.
%% @end
%%--------------------------------------------------------------------
-spec purge(ctx(), one_or_many(key())) -> one_or_many(ok | {error, term()}).
purge(Ctx, Keys) ->
    wait(purge_async(Ctx, Keys)).

%%--------------------------------------------------------------------
%% @doc
%% Returns counter value from a database.
%% If counter does not exist and default value is provided, it is created.
%% @end
%%--------------------------------------------------------------------
-spec get_counter(ctx(), key(), cberl:arithmetic_default()) ->
    {ok, non_neg_integer()} | {error, term()}.
get_counter(#{bucket := Bucket}, Key, Default) ->
    couchbase_pool:post(Bucket, read, {get_counter, Key, Default}).

%%--------------------------------------------------------------------
%% @doc
%% Updates counter value by delta in a database.
%% If counter does not exist and default value is provided, it is created.
%% @end
%%--------------------------------------------------------------------
-spec update_counter(ctx(), key(), cberl:arithmetic_delta(),
    cberl:arithmetic_default()) -> {ok, non_neg_integer()} | {error, term()}.
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

%%--------------------------------------------------------------------
%% @doc
%% Returns list of database buckets.
%% @end
%%--------------------------------------------------------------------
-spec get_buckets() -> [bucket()].
get_buckets() ->
    DbHost = utils:random_element(datastore_config2:get_db_hosts()),
    Url = <<DbHost/binary, ":8091/pools/default/buckets">>,
    {ok, 200, _, Body} = http_client:get(Url),
    lists:map(fun(BucketMap) ->
        maps:get(<<"name">>, BucketMap)
    end, jiffy:decode(Body, [return_maps])).