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
-export([save_view_doc/3, save_spatial_view_doc/3, query_view/4]).
-export([call/3]).

-type ctx() :: #{prefix => binary(),
                 pool_mode => couchbase_pool:mode(),
                 bucket => couchbase_config:bucket(),
                 mutator => datastore:mutator(),
                 cas => cberl:cas(),
                 no_seq => boolean(),
                 no_durability => boolean()}.
-type key() :: datastore:key().
-type value() :: datastore:document() | cberl:value().
-type item() :: datastore:document() | {cberl:key(), cberl:value()}.
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
                    {spatial, boolean()} |
                    {skip, integer()} |
                    {stale, false | ok | update_after} |
                    {startkey, binary()} |
                    {startkey_docid, binary()} |
                    {bbox, binary()} |
                    {start_range, binary()} |
                    {end_range, binary()}.

-export_type([ctx/0, key/0, value/0, item/0, rev/0, design/0, view/0,
    view_opt/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Call method on this driver.
%% @end
%%--------------------------------------------------------------------
-spec call(Function :: atom(), ctx(), Args :: [term()]) ->
    term().
call(delete = Method, OptCtx, [Key, _Pred]) ->
    call(Method, OptCtx, [Key]);
call(Method, OptCtx, Args) ->
    apply(?MODULE, Method, [OptCtx | Args]).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously saves value in CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec save_async(ctx(), item()) -> couchbase_pool:future().
save_async(#{bucket := Bucket} = Ctx, #document{} = Doc) ->
    Mode = maps:get(pool_mode, Ctx, write),
    couchbase_pool:post_async(Bucket, Mode, {save, Ctx, Doc});
save_async(#{bucket := Bucket} = Ctx, {Key, Value}) ->
    Mode = maps:get(pool_mode, Ctx, write),
    couchbase_pool:post_async(Bucket, Mode, {save, Ctx, {Key, Value}}).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously retrieves value from CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec get_async(ctx(), key()) -> couchbase_pool:future().
get_async(#{bucket := Bucket} = Ctx, Key) ->
    Mode = maps:get(pool_mode, Ctx, read),
    Key2 = couchbase_doc:set_prefix(Ctx, Key),
    couchbase_pool:post_async(Bucket, Mode, {get, Key2}).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously removes value from CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec delete_async(ctx(), key()) -> couchbase_pool:future().
delete_async(#{bucket := Bucket} = Ctx, Key) ->
    Mode = maps:get(pool_mode, Ctx, write),
    Key2 = couchbase_doc:set_prefix(Ctx, Key),
    couchbase_pool:post_async(Bucket, Mode, {delete, Ctx, Key2}).

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
-spec save(ctx(), item() | [item()]) -> {ok, cberl:cas(), value()} |
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
    case get(maps:remove(prefix, Ctx), Key) of
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
get_counter(#{bucket := Bucket} = Ctx, Key, Default) ->
    Mode = maps:get(pool_mode, Ctx, read),
    couchbase_pool:post(Bucket, Mode, {get_counter, Key, Default}).

%%--------------------------------------------------------------------
%% @doc
%% Updates counter value by delta in a database.
%% If counter does not exist and default value is provided, it is created.
%% @end
%%--------------------------------------------------------------------
-spec update_counter(ctx(), key(), cberl:arithmetic_delta(),
    cberl:arithmetic_default()) ->
    {ok, cberl:cas(), non_neg_integer()} | {error, term()}.
update_counter(#{bucket := Bucket} = Ctx, Key, Delta, Default) ->
    Mode = maps:get(pool_mode, Ctx, write),
    couchbase_pool:post(Bucket, Mode, {update_counter, Key, Delta, Default}).

%%--------------------------------------------------------------------
%% @doc
%% Saves design document in a database.
%% @end
%%--------------------------------------------------------------------
-spec save_design_doc(ctx(), design(), datastore_json2:ejson()) ->
    ok | {error, term()}.
save_design_doc(#{bucket := Bucket} = Ctx, DesignName, EJson) ->
    Mode = maps:get(pool_mode, Ctx, write),
    couchbase_pool:post(Bucket, Mode, {save_design_doc, DesignName, EJson}).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves design document from a database.
%% @end
%%--------------------------------------------------------------------
-spec get_design_doc(ctx(), design()) ->
    {ok, datastore_json2:ejson()} | {error, term()}.
get_design_doc(#{bucket := Bucket} = Ctx, DesignName) ->
    Mode = maps:get(pool_mode, Ctx, read),
    couchbase_pool:post(Bucket, Mode, {get_design_doc, DesignName}).

%%--------------------------------------------------------------------
%% @doc
%% Removes design document from a database.
%% @end
%%--------------------------------------------------------------------
-spec delete_design_doc(ctx(), design()) -> ok | {error, term()}.
delete_design_doc(#{bucket := Bucket} = Ctx, DesignName) ->
    Mode = maps:get(pool_mode, Ctx, write),
    couchbase_pool:post(Bucket, Mode, {delete_design_doc, DesignName}).

%%--------------------------------------------------------------------
%% @doc
%% Creates a design document with a single view. Name of design document is
%% equal to the view name.
%% @end
%%--------------------------------------------------------------------
-spec save_view_doc(ctx(), view(), binary()) -> ok | {error, term()}.
save_view_doc(Ctx, ViewName, Function) ->
    EJson = {[{<<"views">>, {[{
        ViewName, {[{
            <<"map">>, Function
        }]}
    }]}}]},
    save_design_doc(Ctx, ViewName, EJson).

%%--------------------------------------------------------------------
%% @doc
%% Creates a design document with a single spatial view. Name of design document
%% is equal to the view name.
%% @end
%%--------------------------------------------------------------------
-spec save_spatial_view_doc(ctx(), view(), binary()) -> ok | {error, term()}.
save_spatial_view_doc(Ctx, ViewName, Function) ->
    EJson = {[{<<"spatial">>, {[{
        ViewName, Function
    }]}}]},
    save_design_doc(Ctx, ViewName, EJson).

%%--------------------------------------------------------------------
%% @doc
%% Returns view response from a database.
%% @end
%%--------------------------------------------------------------------
-spec query_view(ctx(), design(), view(), [view_opt()]) ->
    {ok, datastore_json2:ejson()} | {error, term()}.
query_view(#{bucket := Bucket} = Ctx, DesignName, ViewName, Opts) ->
    Mode = maps:get(pool_mode, Ctx, read),
    couchbase_pool:post(Bucket, Mode, {query_view, DesignName, ViewName, Opts}).