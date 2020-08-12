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

%% API
-export([save_async/3, get_async/2, delete_async/2, wait/1]).
-export([save/1, save/3, get/2, delete/2]).
-export([get_counter/2, get_counter/3, update_counter/4]).
-export([save_design_doc/3, get_design_doc/2, delete_design_doc/2]).
-export([save_view_doc/3, save_view_doc/4, save_view_doc/5,
    save_spatial_view_doc/3, save_spatial_view_doc/4, query_view/4]).
-export([set_expiry/2]).

-type ctx() :: #{bucket := couchbase_config:bucket(),
                 pool_mode => couchbase_pool:mode(),
                 cas => cberl:cas(),
                 no_seq => boolean(),
                 no_durability => boolean(),
                 answer_to => pid(),
                 expiry => cberl:expiry()}.
-type key() :: datastore:key().
-type value() :: datastore:doc() | cberl:value().
-type item() :: {ctx(), key(), value()}.
-type design() :: binary().
-type view() :: binary().
-type view_creation_opt() :: {update_min_changes, integer()} |
                             {replica_update_min_changes, integer()}.
-type view_creation_opts() :: [view_creation_opt()].
-type view_opt() :: {descending, boolean()} |
                    {endkey, binary() | [datastore_doc:scope() | couchbase_changes:until()]} |
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
                    {startkey, binary() | [datastore_doc:scope() | couchbase_changes:since()]} |
                    {startkey_docid, binary()} |
                    {bbox, binary()} |
                    {start_range, binary()} |
                    {end_range, binary()}.

-type db_host() :: list().

-export_type([ctx/0, key/0, value/0, item/0, design/0, view/0, view_opt/0, db_host/0]).

% below parameters are described in
% https://docs.couchbase.com/server/4.1/developer-guide/views-operation.html#automated-index-updates
-define(UPDATE_MIN_CHANGES, 5000).
-define(REPLICA_UPDATE_MIN_CHANGES, 5000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously saves value in CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec save_async(ctx(), key(), value()) -> couchbase_pool:future().
save_async(#{bucket := Bucket, answer_to := Pid} = Ctx, Key, Value) ->
    Mode = maps:get(pool_mode, Ctx, write),
    couchbase_pool:post_async(Bucket, Mode, {save, Ctx, Key, Value}, Pid);
save_async(#{bucket := Bucket} = Ctx, Key, Value) ->
  Mode = maps:get(pool_mode, Ctx, write),
  couchbase_pool:post_async(Bucket, Mode, {save, Ctx, Key, Value}).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously retrieves value from CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec get_async(ctx(), key()) -> couchbase_pool:future().
get_async(#{bucket := Bucket} = Ctx, Key) ->
    Mode = maps:get(pool_mode, Ctx, read),
    couchbase_pool:post_async(Bucket, Mode, {get, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously removes value from CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec delete_async(ctx(), key()) -> couchbase_pool:future().
delete_async(#{bucket := Bucket} = Ctx, Key) ->
    Mode = maps:get(pool_mode, Ctx, write),
    couchbase_pool:post_async(Bucket, Mode, {delete, Ctx, Key}).

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
    case couchbase_pool:wait(Future) of
        {error, key_enoent} -> {error, not_found};
        {error, key_eexists} -> {error, already_exists};
        Other -> Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves values in CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec save([item()]) -> [{ok, cberl:cas(), value()} | {error, term()}].
save(Items) when is_list(Items) ->
    wait([save_async(Ctx, Key, Value) || {Ctx, Key, Value} <- Items]).

%%--------------------------------------------------------------------
%% @doc
%% Saves value in CouchBase.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), key(), value()) ->
    {ok, cberl:cas(), value()} | {error, term()}.
save(Ctx, Key, Value) ->
    hd(save([{Ctx, Key, Value}])).

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
-spec save_design_doc(ctx(), design(), datastore_json:ejson()) ->
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
    {ok, datastore_json:ejson()} | {error, term()}.
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
%% @equiv save_view_doc(Ctx, ViewName, Function, []).
%% @end
%%--------------------------------------------------------------------
-spec save_view_doc(ctx(), view(), binary()) -> ok | {error, term()}.
save_view_doc(Ctx, ViewName, Function) ->
    save_view_doc(Ctx, ViewName, Function, []).

%%--------------------------------------------------------------------
%% @doc
%% @equiv save_view_doc(Ctx, ViewName, Function, undefined, Opts).
%% @end
%%--------------------------------------------------------------------
-spec save_view_doc(ctx(), view(), binary(), view_creation_opts()) -> ok | {error, term()}.
save_view_doc(Ctx, ViewName, Function, Opts) ->
    save_view_doc(Ctx, ViewName, Function, undefined, Opts).

%%--------------------------------------------------------------------
%% @doc
%% Creates a design document with a single view. Name of design document is
%% equal to the view name.
%% @end
%%--------------------------------------------------------------------
-spec save_view_doc(ctx(), view(), binary(), binary() | undefined,
    view_creation_opts()) -> ok | {error, term()}.
save_view_doc(Ctx, ViewName, MapFunction, ReduceFunction, Opts) ->
    EJson = {[
        {<<"views">>, {[
            view_definition(ViewName, MapFunction, ReduceFunction)
        ]}},
        {<<"options">>, {[
            {<<"updateMinChanges">>,
                proplists:get_value(update_min_changes, Opts, ?UPDATE_MIN_CHANGES)},
            {<<"replicaUpdateMinChanges">>,
                proplists:get_value(replica_update_min_changes, Opts,
                    ?REPLICA_UPDATE_MIN_CHANGES)}
        ]}}
    ]},
    save_design_doc(Ctx, ViewName, EJson).

%%--------------------------------------------------------------------
%% @doc
%% @equiv save_spatial_view_doc(Ctx, ViewName, Function, []).
%% @end
%%--------------------------------------------------------------------
-spec save_spatial_view_doc(ctx(), view(), binary()) -> ok | {error, term()}.
save_spatial_view_doc(Ctx, ViewName, Function) ->
    save_spatial_view_doc(Ctx, ViewName, Function, []).

%%--------------------------------------------------------------------
%% @doc
%% Creates a design document with a single spatial view. Name of design document
%% is equal to the view name.
%% @end
%%--------------------------------------------------------------------
-spec save_spatial_view_doc(ctx(), view(), binary(), view_creation_opts()) -> ok | {error, term()}.
save_spatial_view_doc(Ctx, ViewName, Function, Opts) ->
    EJson = {[
        {<<"spatial">>, {[
            {ViewName, Function}
        ]}},
        {<<"options">>, {[
            {<<"updateMinChanges">>,
                proplists:get_value(update_min_changes, Opts, ?UPDATE_MIN_CHANGES)},
            {<<"replicaUpdateMinChanges">>,
                proplists:get_value(replica_update_min_changes, Opts,
                    ?REPLICA_UPDATE_MIN_CHANGES)}
        ]}}
    ]},
    save_design_doc(Ctx, ViewName, EJson).

%%--------------------------------------------------------------------
%% @doc
%% Returns view response from a database.
%% @end
%%--------------------------------------------------------------------
-spec query_view(ctx(), design(), view(), [view_opt()]) ->
    {ok, json_utils:json_map()} | {error, term()}.
query_view(#{bucket := Bucket} = Ctx, DesignName, ViewName, Opts) ->
    Mode = maps:get(pool_mode, Ctx, read),
    couchbase_pool:post(Bucket, Mode, {query_view, DesignName, ViewName, Opts}).

%%--------------------------------------------------------------------
%% @doc
%% Sets expiry field in context.
%% Couchbase treats numbers smaller or equal to 2592000 as document's time to live
%% and greater values as timestamp of deletion
%% Translate expiry time to couchbase format
%% @end
%%--------------------------------------------------------------------
-spec set_expiry(ctx() | datastore:ctx(), non_neg_integer()) -> ctx() | datastore:ctx().
set_expiry(Ctx, Expiry) when Expiry =< 2592000 ->
    Ctx#{expiry => Expiry};
set_expiry(Ctx, Expiry) ->
    Ctx#{expiry => erlang:system_time(second) + Expiry}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns view definition in EJSON format.
%% @end
%%-------------------------------------------------------------------
-spec view_definition(view(), binary(), binary() | undefined) -> {view(), datastore_json:ejson()}.
view_definition(ViewName, MapFunction, undefined) ->
    {ViewName, {[
        {<<"map">>, MapFunction}
    ]}};
view_definition(ViewName, MapFunction, ReduceFunction) ->
    {ViewName, {[
        {<<"map">>, MapFunction},
        {<<"reduce">>, ReduceFunction}
    ]}}.
