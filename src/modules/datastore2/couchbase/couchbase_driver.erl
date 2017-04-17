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
-include_lib("ctool/include/logging.hrl").

%% API
-export([save/2, save/3, get/2, mget/2, delete/2, purge/2]).
-export([get_counter/3, update_counter/4]).
-export([save_design_doc/3, delete_design_doc/2]).
-export([query_view/4]).
-export([get_buckets/0]).

-type ctx() :: #{atom() => term()}.
-type ejson() :: datastore_json2:ejson().
-type value() :: integer() | binary() | ejson().
-type hash() :: datastore_utils2:hex().
-type rev() :: [hash()].
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

-export_type([ctx/0, rev/0, bucket/0, design/0, view/0, view_opt/0]).

-define(REVISION_HISTORY_LEN, 20).
-define(MUTATOR_HISTORY_LEN, 20).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves document in the database.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), datastore:doc()) -> 
    {ok, datastore:doc()} | {error, Reason :: term()}.
save(Ctx, #document2{key = Key} = Doc) ->
    try
        Doc2 = set_mutator(Ctx, Doc),
        Doc3 = set_next_seq(Ctx, Key, Doc2),
        {Doc4, EJson} = set_next_rev(Ctx, Doc3),
        case save(Ctx, Key, EJson) of
            ok -> {ok, Doc4};
            {error, Reason} -> {error, Reason}
        end
    catch
        _:{badmatch, Reason2} -> {error, Reason2};
        _:Reason2 -> {error, {Reason2, erlang:get_stacktrace()}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves key-value pair in the database.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), datastore:key(), value()) -> ok | {error, Reason :: term()}.
save(#{bucket := Bucket}, Key, Value) ->
    couchbase_pool:post(Bucket, write, {save, Key, Value}).

%%--------------------------------------------------------------------
%% @doc
%% Returns a document from the database.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), datastore:key()) ->
    {ok, value() | datastore:doc()} | {error, Reason :: term()}.
get(Ctx, Key) ->
    hd(mget(Ctx, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% Returns batch of documents from the database.
%% @end
%%--------------------------------------------------------------------
-spec mget(ctx(), [datastore:key()]) ->
    [{ok, value() | datastore:doc()} | {error, Reason :: term()}].
mget(#{bucket := Bucket}, Keys) ->
    lists:map(fun
        ({ok, {_} = EJson}) -> {ok, datastore_json2:decode(EJson)};
        ({ok, Value}) -> {ok, Value};
        ({error, Reason}) -> {error, Reason}
    end, couchbase_pool:post(Bucket, read, {mget, Keys})).

%%--------------------------------------------------------------------
%% @doc
%% Marks document as deleted in the database.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(), datastore:doc()) ->
    {ok, datastore:doc()} | {error, Reason :: term()}.
delete(Ctx, #document2{} = Doc) ->
    save(Ctx, Doc#document2{deleted = true}).

%%--------------------------------------------------------------------
%% @doc
%% Removes document from the database.
%% @end
%%--------------------------------------------------------------------
-spec purge(ctx(), datastore:key()) -> ok | {error, Reason :: term()}.
purge(#{bucket := Bucket}, Key) ->
    couchbase_pool:post(Bucket, write, {remove, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Returns counter value from the database. If counter does not exist
%% creates it with the default value and returns it.
%% @end
%%--------------------------------------------------------------------
-spec get_counter(ctx(), datastore:key(), integer()) ->
    {ok, integer()} | {error, Reason :: term()}.
get_counter(Ctx, Key, Default) ->
    update_counter(Ctx, Key, 0, Default).

%%--------------------------------------------------------------------
%% @doc
%% Updates counter value in the database by Offset. If counter does not exist
%% creates it with the default value.
%% @end
%%--------------------------------------------------------------------
-spec update_counter(ctx(), datastore:key(), integer(), integer()) ->
    {ok, integer()} | {error, Reason :: term()}.
update_counter(#{bucket := Bucket}, Key, Offset, Default) ->
    couchbase_pool:post(Bucket, write, {update_counter, Key, Offset, Default}).

%%--------------------------------------------------------------------
%% @doc
%% Saves design document in the database.
%% @end
%%--------------------------------------------------------------------
-spec save_design_doc(ctx(), design(), ejson()) ->
    ok | {error, Reason :: term()}.
save_design_doc(#{bucket := Bucket}, DesignName, EJson) ->
    couchbase_pool:post(Bucket, write, {save_design_doc, DesignName, EJson}).

%%--------------------------------------------------------------------
%% @doc
%% Removes design document from the database.
%% @end
%%--------------------------------------------------------------------
-spec delete_design_doc(ctx(), design()) -> ok | {error, Reason :: term()}.
delete_design_doc(#{bucket := Bucket}, DesignName) ->
    couchbase_pool:post(Bucket, write, {delete_design_doc, DesignName}).

%%--------------------------------------------------------------------
%% @doc
%% Returns view response from the database.
%% @end
%%--------------------------------------------------------------------
-spec query_view(ctx(), design(), view(), [view_opt()]) ->
    {ok, ejson()} | {error, Reason :: term()}.
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates and stores next sequence number in a document.
%% @end
%%--------------------------------------------------------------------
-spec set_next_seq(ctx(), datastore:key(), datastore:doc()) -> datastore:doc().
set_next_seq(#{no_seq := true}, _Key, Doc) ->
    Doc;
set_next_seq(Ctx, Key, #document2{scope = Scope} = Doc) ->
    {ok, Seq} = update_counter(Ctx, couchbase_changes:get_seq_key(Scope), 1, 1),
    ok = save(Ctx, couchbase_changes:get_change_key(Scope, Seq), Key),
    Doc#document2{seq = Seq}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates and stores next revision hash in a document.
%% @end
%%--------------------------------------------------------------------
-spec set_next_rev(ctx(), datastore:doc()) -> {datastore:doc(), ejson()}.
set_next_rev(#{no_rev := true}, Doc) ->
    {Doc, datastore_json2:encode(Doc)};
set_next_rev(_Ctx, #document2{rev = Revs} = Doc) ->
    {Props} = EJson = datastore_json2:encode(Doc),
    Rev = create_rev(EJson),
    Revs2 = lists:sublist([Rev | Revs], ?REVISION_HISTORY_LEN),

    Doc2 = Doc#document2{rev = Revs2},
    Props2 = lists:keystore(<<"_rev">>, 1, Props, {<<"_rev">>, Revs2}),
    {Doc2, {Props2}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stores mutator in a document.
%% @end
%%--------------------------------------------------------------------
-spec set_mutator(ctx(), datastore:doc()) -> datastore:doc().
set_mutator(#{mutator := Mutator}, #document2{mutator = Mutators} = Doc) ->
    Doc#document2{
        mutator = lists:sublist([Mutator | Mutators], ?MUTATOR_HISTORY_LEN)
    };
set_mutator(_Ctx, Doc) ->
    Doc.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates revision hash for a document.
%% @end
%%--------------------------------------------------------------------
-spec create_rev(ejson()) -> hash().
create_rev({Props} = EJson) ->
    {Gen, Hash} = parse_last_rev(EJson),
    Props2 = lists:filter(fun
        ({<<"_deleted">>, _}) -> true;
        ({<<"_record">>, _}) -> true;
        ({<<"_version">>, _}) -> true;
        ({<<"_", _/binary>>, _}) -> false;
        ({_, _}) -> true
    end, Props),

    Ctx = crypto:hash_init(md5),
    Ctx2 = crypto:hash_update(Ctx, integer_to_binary(size(Hash))),
    Ctx3 = crypto:hash_update(Ctx2, Hash),
    Ctx4 = crypto:hash_update(Ctx3, jiffy:encode({Props2})),
    Digest = crypto:hash_final(Ctx4),

    Gen2 = Gen + 1,
    Hash2 = datastore_utils2:hex(Digest),
    <<(integer_to_binary(Gen2))/binary, "-", Hash2/binary>>.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns generation and hash of the last document revision.
%% @end
%%--------------------------------------------------------------------
-spec parse_last_rev(ejson()) -> {non_neg_integer(), hash()}.
parse_last_rev({Props}) ->
    case lists:keyfind(<<"_rev">>, 1, Props) of
        {<<"_rev">>, []} ->
            {0, <<>>};
        {<<"_rev">>, [Rev | _]} ->
            [Gen, Hash] = binary:split(Rev, <<"-">>),
            {binary_to_integer(Gen), Hash}
    end.