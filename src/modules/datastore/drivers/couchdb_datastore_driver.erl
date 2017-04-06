%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc CouchDB database driver (REST based) that supports changes stream
%%%      and connecting to couchbase via couchbase's sync_gateway (that emulates CouchDB API).
%%%      Values of document saved with this driver cannot be bigger then 512kB.
%%% @end
%%%-------------------------------------------------------------------
% TODO - change all transaction to critical sections when critical section is repaired
-module(couchdb_datastore_driver).
-author("Rafal Slota").
-behaviour(store_driver_behaviour).

-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

%% Encoded object prefix
-define(OBJ_PREFIX, "OBJ::").

%% Encoded atom prefix
-define(ATOM_PREFIX, "ATOM::").

%% Encoded integer prefix
-define(INT_PREFIX, "INT::").

-define(LINKS_KEY_SUFFIX, "$$").

%% Maximum size of document's value.
-define(MAX_VALUE_SIZE, 10 * 1024 * 1024).

%% Base port for gateway endpoints
-define(GATEWAY_BASE_PORT_MIN, 12000).
-define(GATEWAY_BASE_PORT_MAX, 12999).

%% Supported buckets
-define(DEFAULT_BUCKET, <<"default">>).
-define(SYNC_ENABLED_BUCKET, <<"sync">>).

-define(COUCHBASE_ADMIN_PORT, 8091).
-define(COUCHBASE_API_PORT, 8092).

-type couchdb_bucket() :: binary().
-type db_run_opt() :: direct.
-export_type([couchdb_bucket/0]).

%% store_driver_behaviour callbacks
-export([init_bucket/3, healthcheck/1, init_driver/1]).
-export([save/2, create/2, update/3, create_or_update/3, exists/2, get/2, list/4, delete/3, is_model_empty/1]).
-export([add_links/3, set_links/3, create_link/3, create_or_update_link/4, delete_links/3, fetch_link/3, foreach_link/4]).

-export([get/3, force_save/2, force_save/3, db_run/4, db_run/5]).
-export([save_docs/2, save_docs/3, save_docs_direct/2, save_docs_direct/3,
    save_doc_asynch/2, save_doc_asynch_response/1, get_docs/2, get_docs_direct/2,
    delete_doc_direct/2, save_revision_direct/3]).

-export([changes_start_link/3, changes_start_link/4, get_with_revs/2]).
-export([init/1, handle_call/3, handle_info/2, handle_change/2, handle_cast/2, terminate/2]).
-export([save_link_doc/2, get_link_doc/2, get_link_doc/3, delete_link_doc/2, exists_link_doc/3]).
-export([to_binary/1]).
-export([add_view/4, query_view/3, delete_view/2, stream_view/3]).
-export([default_bucket/0, sync_enabled_bucket/0, select_bucket/1, select_bucket/2, get_buckets/0]).
-export([rev_to_number/1]).

% for tests
-export([normalize_seq/1]).

%%%===================================================================
%%% buckets
%%%===================================================================

-spec default_bucket() -> couchdb_bucket().
default_bucket() ->
    ?DEFAULT_BUCKET.

-spec sync_enabled_bucket() -> couchdb_bucket().
sync_enabled_bucket() ->
    ?SYNC_ENABLED_BUCKET.

%%%===================================================================
%%% store_driver_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_driver/1.
%% @end
%%--------------------------------------------------------------------
-spec init_driver(worker_host:plugin_state()) -> {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init_driver(State) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/3.
%% @end
%%--------------------------------------------------------------------
-spec init_bucket(Bucket :: datastore:bucket(), Models :: [model_behaviour:model_config()],
    NodeToSync :: node()) -> ok.
init_bucket(_Bucket, _Models, _NodeToSync) ->
    DesignId = <<"_design/versions">>,

    Doc = jiffy:decode(jiffy:encode(#{
        <<"_id">> => DesignId,
        <<"views">> => #{
            <<"versions">> => #{
                <<"map">> => <<"function(doc) { "
                "emit([doc['", ?RECORD_TYPE_MARKER, "'], "
                "doc['", ?RECORD_VERSION_MARKER, "']], 1); }">>}
        }
    })),
    lists:foreach(fun(Bucket) ->
        db_run(Bucket, couchbeam, save_doc, [Doc], 5)
    end, get_buckets()),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns available data buckets in database.
%% @end
%%--------------------------------------------------------------------
-spec get_buckets() -> [binary()].
get_buckets() ->
    case datastore_worker:state_get(buckets) of
        undefined ->
            [{Hostname, _} | _] = datastore_config:db_nodes(),
            Port = integer_to_binary(?COUCHBASE_ADMIN_PORT),
            Url = <<Hostname/binary, ":", Port/binary, "/pools/default/buckets">>,
            {ok, 200, _, Response} = http_client:get(Url),
            Buckets = lists:map(fun(BucketDetails) ->
                maps:get(<<"name">>, BucketDetails)
            end, json_utils:decode_map(Response)),
            datastore_worker:state_put(buckets, Buckets),
            Buckets;
        Buckets ->
            Buckets
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/2.
%% @end
%%--------------------------------------------------------------------
-spec save(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
% TODO - filter only DISK_ONLY
save(#model_config{store_level = L} = ModelConfig,
    #document{rev = undefined, key = Key, value = Value} = Doc)
    when L =:= ?DISK_ONLY_LEVEL; L =:= ?LOCALLY_CACHED_LEVEL ->
    critical_section:run(synchronization_doc_key(ModelConfig, Key),
        fun() ->
            case get(ModelConfig, Key) of
                {error, {not_found, _}} ->
                    create(ModelConfig, Doc);
                {error, Reason} ->
                    {error, Reason};
                {ok, #document{rev = _Rev, value = Value}} ->
                    {ok, Key};
                {ok, #document{rev = Rev}} ->
                    save_doc(ModelConfig, Doc#document{rev = Rev})
            end
        end);
save(ModelConfig, #document{rev = undefined, key = Key, value = Value} = Doc) ->
    case get(ModelConfig, Key) of
        {error, {not_found, _}} ->
            create(ModelConfig, Doc);
        {error, Reason} ->
            {error, Reason};
        {ok, #document{rev = _Rev, value = Value}} ->
            {ok, Key};
        {ok, #document{rev = Rev}} ->
            save_doc(ModelConfig, Doc#document{rev = Rev})
    end;
save(ModelConfig, Doc) ->
    save_doc(ModelConfig, Doc).

%%--------------------------------------------------------------------
%% @doc
%% Saves document that describes links, not using transactions (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec save_link_doc(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save_link_doc(ModelConfig, Doc) ->
    save_doc(ModelConfig, Doc).

%%--------------------------------------------------------------------
%% @doc
%% Saves document not using transactions (to be used inside transaction).
%% @end
%%--------------------------------------------------------------------
-spec save_doc(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
% TODO - also for locally cached, check link_store_level
save_doc(#model_config{store_level = ?GLOBALLY_CACHED_LEVEL} = ModelConfig, Doc) ->
    Bucket = select_bucket(ModelConfig, Doc),
    datastore_pool:post_sync(Bucket, write, {save_doc, [ModelConfig, Doc]});
save_doc(ModelConfig, Doc) ->
    [RawDoc] = make_raw_doc(ModelConfig, [Doc]),
    {ok, [RawRes]} = db_run(select_bucket(ModelConfig, Doc), couchbeam, save_docs,
        [[RawDoc], ?DEFAULT_DB_REQUEST_TIMEOUT_OPT], 3),
    parse_response(save, RawRes).

%%--------------------------------------------------------------------
%% @doc
%% Saves document not using transactions and not waiting for answer.
%% Returns ref to receive answer asynch.
%% @end
%%--------------------------------------------------------------------
-spec save_doc_asynch(model_behaviour:model_config(), datastore:document()) ->
    reference().
save_doc_asynch(ModelConfig, Doc) ->
    Bucket = select_bucket(ModelConfig, Doc),
    datastore_pool:post_async(Bucket, write, {save_doc, [ModelConfig, Doc]}).

%%--------------------------------------------------------------------
%% @doc
%% Waits and returns answer of asynch save operation.
%% @end
%%--------------------------------------------------------------------
-spec save_doc_asynch_response(reference()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save_doc_asynch_response(Ref) ->
    datastore_pool:wait(Ref).

%%--------------------------------------------------------------------
%% @doc
%% Saves documents in batch.
%% @end
%%--------------------------------------------------------------------
-spec save_docs(model_behaviour:model_config() | couchdb_bucket(), [{model_behaviour:model_config(), datastore:ext_key()}]) ->
    [{ok, datastore:ext_key()} | datastore:generic_error()].
save_docs(DBBucketOrModelConfig, Documents) ->
    save_docs(DBBucketOrModelConfig, Documents, []).
save_docs(DBBucketOrModelConfig, Documents, Opts) ->
    Bucket = select_bucket(DBBucketOrModelConfig),
    datastore_pool:post_sync(Bucket, write,
        {save_docs_direct, [DBBucketOrModelConfig, Documents, Opts]}).

%%--------------------------------------------------------------------
%% @doc
%% Saves documents in batch.
%% @end
%%--------------------------------------------------------------------
-spec save_docs_direct(model_behaviour:model_config() | couchdb_bucket(), [{model_behaviour:model_config(), datastore:ext_key()}]) ->
    [{ok, datastore:ext_key()} | datastore:generic_error()].
save_docs_direct(DBBucketOrModelConfig, Documents) ->
    save_docs_direct(DBBucketOrModelConfig, Documents, []).
save_docs_direct(#model_config{} = ModelConfig, Documents, Opts) ->
    save_docs_direct(select_bucket(ModelConfig), Documents, Opts);
save_docs_direct(_DBBucket, [], _Opts) ->
    [];
save_docs_direct(DBBucket, Documents, Opts) ->
    Docs = [make_raw_doc(MC, Doc) || {MC, Doc} <- Documents],
    case db_run(DBBucket, couchbeam, save_docs, [Docs, Opts], 3) of
        {ok, Res} ->
            parse_response(save, Res);
        {error, Reason} ->
            [{error, Reason} || _ <- lists:seq(1, length(Documents))]
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets documents in batch.
%% @end
%%--------------------------------------------------------------------
-spec get_docs(model_behaviour:model_config() | couchdb_bucket(), [{model_behaviour:model_config(), datastore:ext_key()}]) ->
    [{ok, datastore:ext_key()} | datastore:generic_error()].
get_docs(#model_config{} = ModelConfig, KeysWithModelConfig) ->
    Bucket = select_bucket(ModelConfig),
    datastore_pool:post_sync(Bucket, read, {get_docs_direct, [ModelConfig, KeysWithModelConfig]}).

%%--------------------------------------------------------------------
%% @doc
%% Gets documents in batch.
%% @end
%%--------------------------------------------------------------------
-spec get_docs_direct(model_behaviour:model_config() | couchdb_bucket(), [{model_behaviour:model_config(), datastore:ext_key()}]) ->
    [{ok, datastore:ext_key()} | datastore:generic_error()].
get_docs_direct(#model_config{} = ModelConfig, KeysWithModelConfig) ->
    get_docs_direct(select_bucket(ModelConfig), KeysWithModelConfig);
get_docs_direct(DBBucket, KeysWithModelConfig) ->
    DriverKeys = [to_driver_key(Bucket, Key) || {#model_config{bucket = Bucket}, Key} <- KeysWithModelConfig],
    case db_run(DBBucket, couchbeam_view, all, [[
        {keys, DriverKeys}, include_docs
    ]], 3) of
        {ok, Res} ->
            lists:map(fun
                ({{_, Key}, {ok, {Proplist}}}) ->
                    {_, Rev} = lists:keyfind(<<"_rev">>, 1, Proplist),
                    Proplist1 = [KV || {<<"_", _/binary>>, _} = KV <- Proplist],
                    Proplist2 = Proplist -- Proplist1,
                    {_WasUpdated, Version, Value} = datastore_json:decode_record_vcs({Proplist2}),
                    {ok, #document{key = Key, value = Value, rev = Rev, version = Version}};
                ({{_, _Key}, Other}) ->
                    Other
            end, lists:zip(KeysWithModelConfig, parse_response(get, Res)));
        {error, Reason} ->
            [{error, Reason} || _ <- lists:seq(1, length(KeysWithModelConfig))]
    end.

%%--------------------------------------------------------------------
%% @doc
%% Translates given document to eJSON format.
%% @end
%%--------------------------------------------------------------------
-spec make_raw_doc(model_behaviour:model_config(), [datastore:document()]) ->
    [datastore_json:ejson()].
make_raw_doc(_MC, []) ->
    [];
make_raw_doc(MC, [Doc | T]) ->
    [make_raw_doc(MC, Doc) | make_raw_doc(MC, T)];
make_raw_doc(#model_config{bucket = Bucket} = ModelConfig, #document{deleted = Del, key = Key, rev = Rev, value = Value} = Doc) ->
    ok = assert_value_size(Value, ModelConfig, Key),

    {Props} = datastore_json:encode_record(Doc),
    RawRevInfo = case Rev of
        undefined ->
            [];
        {Start, Ids} = Revs ->
            [{<<"_revisions">>, {[{<<"ids">>, Ids}, {<<"start">>, Start}]}}, {<<"_rev">>, rev_info_to_rev(Revs)}];
        _ ->
            [{<<"_rev">>, Rev}]
    end,
    {RawRevInfo ++ [{<<"_deleted">>, atom_to_binary(Del, utf8)}, {<<"_id">>, to_driver_key(Bucket, Key)} | Props]}.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback update/3.
%% @end
%%--------------------------------------------------------------------
-spec update(model_behaviour:model_config(), datastore:ext_key(),
    Diff :: datastore:document_diff()) -> {ok, datastore:ext_key()} | datastore:update_error().
update(ModelConfig, Key, Diff) when is_function(Diff) ->
    critical_section:run(synchronization_doc_key(ModelConfig, Key),
        fun() ->
            case get(ModelConfig, Key) of
                {error, Reason} ->
                    {error, Reason};
                {ok, #document{value = Value} = Doc} ->
                    case Diff(Value) of
                        {ok, NewValue} ->
                            save(ModelConfig, Doc#document{value = NewValue});
                        {error, Reason2} ->
                            {error, Reason2}
                    end
            end
        end);
update(ModelConfig, Key, Diff) when is_map(Diff) ->
    critical_section:run(synchronization_doc_key(ModelConfig, Key),
        fun() ->
            case get(ModelConfig, Key) of
                {error, Reason} ->
                    {error, Reason};
                {ok, #document{value = Value} = Doc} ->
                    NewValue = maps:merge(datastore_utils:shallow_to_map(Value), Diff),
                    save(ModelConfig, Doc#document{value = datastore_utils:shallow_to_record(NewValue)})
            end
        end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create/2.
%% @end
%%--------------------------------------------------------------------
-spec create(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(#model_config{} = ModelConfig, ToSave = #document{}) ->
    save_doc(ModelConfig, ToSave#document{rev = undefined}).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_or_update/2.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(model_behaviour:model_config(), datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create_or_update(#model_config{name = ModelName} = ModelConfig, #document{key = Key} = NewDoc, Diff)
    when is_function(Diff) ->
    critical_section:run(synchronization_doc_key(ModelConfig, Key),
        fun() ->
            case get(ModelConfig, Key) of
                {error, {not_found, ModelName}} ->
                    create(ModelConfig, NewDoc);
                {error, Reason} ->
                    {error, Reason};
                {ok, #document{value = Value} = Doc} ->
                    case Diff(Value) of
                        {ok, NewValue} ->
                            save(ModelConfig, Doc#document{value = NewValue});
                        {error, Reason2} ->
                            {error, Reason2}
                    end
            end
        end);
create_or_update(#model_config{name = ModelName} = ModelConfig, #document{key = Key} = NewDoc, Diff)
    when is_map(Diff) ->
    critical_section:run(synchronization_doc_key(ModelConfig, Key),
        fun() ->
            case get(ModelConfig, Key) of
                {error, {not_found, ModelName}} ->
                    create(ModelConfig, NewDoc);
                {error, Reason} ->
                    {error, Reason};
                {ok, #document{value = Value} = Doc} ->
                    NewValue = maps:merge(datastore_utils:shallow_to_map(Value), Diff),
                    save(ModelConfig, Doc#document{value = datastore_utils:shallow_to_record(NewValue)})
            end
        end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get(#model_config{} = ModelConfig, Key) ->
    get(ModelConfig, select_bucket(ModelConfig, Key), Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(model_behaviour:model_config(), binary(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get(#model_config{bucket = Bucket, name = ModelName} = ModelConfig, BucketOverride, Key) ->
    case db_run(BucketOverride, couchbeam, open_doc, [to_driver_key(Bucket, Key)], 3) of
        {ok, {Proplist} = _Doc} ->
            case verify_ans(Proplist) of
                true ->
                    {_, Rev} = lists:keyfind(<<"_rev">>, 1, Proplist),
                    Proplist1 = [KV || {<<"_", _/binary>>, _} = KV <- Proplist],
                    Proplist2 = Proplist -- Proplist1,
                    {WasUpdated, Version, Value} = datastore_json:decode_record_vcs({Proplist2}),
                    RetDoc = #document{key = Key, value = Value, rev = Rev, version = Version},
                    spawn(
                        fun() ->
                            case WasUpdated of
                                true ->
                                    save(ModelConfig, RetDoc);
                                _ -> ok
                            end
                        end),
                    {ok, RetDoc};
                _ ->
                    {error, db_internal_error}
            end;
        {error, {not_found, _}} ->
            {error, {not_found, ModelName}};
        {error, not_found} ->
            {error, {not_found, ModelName}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets document that describes links (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec get_link_doc(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get_link_doc(ModelConfig, Key) ->
    get(ModelConfig, Key).

%%--------------------------------------------------------------------
%% @doc
%% Gets document that describes links (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec get_link_doc(model_behaviour:model_config(), binary(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get_link_doc(ModelConfig, BucketOverride, Key) ->
    get(ModelConfig, BucketOverride, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks if document that describes links from scope exists.
%% @end
%%--------------------------------------------------------------------
-spec exists_link_doc(model_behaviour:model_config(), datastore:ext_key(), links_utils:scope()) ->
    {ok, boolean()} | datastore:generic_error().
exists_link_doc(ModelConfig, Key, Scope) ->
    DocKey = links_utils:links_doc_key(Key, Scope),
    case get_link_doc(ModelConfig, DocKey) of
        {ok, _} -> {ok, true};
        {error, {not_found, _}} -> {ok, false};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback list/4.
%% @end
%%--------------------------------------------------------------------
-spec list(model_behaviour:model_config(),
    Fun :: datastore:list_fun(), AccIn :: term(), _Opts :: store_driver_behaviour:list_options()) ->
    {ok, Handle :: term()} | datastore:generic_error() | no_return().
list(#model_config{name = ModelName} = ModelConfig, Fun, AccIn, _Mode) ->
    BinModelName = atom_to_binary(ModelName, utf8),
    case db_run(select_bucket(ModelConfig, undefined), couchbeam_view, fetch, [all_docs, [include_docs, {start_key, BinModelName}, {end_key, BinModelName}]], 3) of
        {ok, Rows} ->
            case verify_ans(Rows) of
                true ->
                    Ret =
                        try
                            lists:foldl(
                                fun({Row}, Acc) ->
                                    try
                                        {Value} = proplists:get_value(<<"doc">>, Row, {[]}),
                                        {_, KeyBin} = lists:keyfind(<<"id">>, 1, Row),
                                        Value1 = [KV || {<<"_", _/binary>>, _} = KV <- Value],
                                        Value2 = Value -- Value1,
                                        {_, Key} = from_driver_key(KeyBin),
                                        {_WasUpdated, Version, DocValue} = datastore_json:decode_record_vcs({Value2}),
                                        Doc = #document{key = Key, value = DocValue, version = Version},
                                        case element(1, Doc#document.value) of
                                            ModelName ->
                                                case Fun(Doc, Acc) of
                                                    {next, NewAcc} ->
                                                        NewAcc;
                                                    {abort, LastAcc} ->
                                                        throw({abort, LastAcc})
                                                end;
                                            _ ->
                                                Acc %% Trash entry, skipping
                                        end
                                    catch
                                        {abort, RetAcc} ->
                                            throw({abort, RetAcc}); %% Provided function requested end of stream, exiting loop
                                        _:_ ->
                                            Acc %% Invalid entry, skipping
                                    end
                                end, AccIn, Rows)
                        catch
                            {abort, RetAcc} ->
                                RetAcc %% Catch exit loop
                        end,
                    {ok, Ret};
                _ ->
                    {error, db_internal_error}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback is_model_empty/1.
%% @end
%%--------------------------------------------------------------------
-spec is_model_empty(model_behaviour:model_config()) -> {ok, boolean()} | datastore:generic_error().
is_model_empty(#model_config{name = ModelName} = ModelConfig) ->
    BinModelName = atom_to_binary(ModelName, utf8),
    case db_run(select_bucket(ModelConfig, undefined), couchbeam_view, first, [all_docs, [include_docs, {start_key, BinModelName}, {end_key, BinModelName}]], 3) of
        {ok, nil} ->
            {ok, true};
        {ok, Row} ->
            case verify_ans(Row) of
                true ->
                    {ok, false};
                _ ->
                    {error, db_internal_error}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete/2.
%% @end
%%--------------------------------------------------------------------
-spec delete(model_behaviour:model_config(), datastore:ext_key(), datastore:delete_predicate()) ->
    ok | datastore:generic_error().
% TODO - filter only DISK_ONLY
delete(#model_config{store_level = L} = ModelConfig, Key, Pred)
    when L =:= ?DISK_ONLY_LEVEL; L =:= ?LOCALLY_CACHED_LEVEL ->
    critical_section:run(synchronization_doc_key(ModelConfig, Key),
        fun() ->
            case Pred() of
                true ->
                    case get(ModelConfig, Key) of
                        {error, {not_found, _}} ->
                            ok;
                        {error, not_found} ->
                            ok;
                        {error, Reason} ->
                            {error, Reason};
                        {ok, Doc} ->
                            delete_doc(ModelConfig, Doc)
                    end;
                false ->
                    ok
            end
        end);
delete(ModelConfig, Key, Pred) ->
    case Pred() of
        true ->
            case get(ModelConfig, Key) of
                {error, {not_found, _}} ->
                    ok;
                {error, not_found} ->
                    ok;
                {error, Reason} ->
                    {error, Reason};
                {ok, Doc} ->
                    delete_doc(ModelConfig, Doc)
            end;
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes document that describes links, not using transactions (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec delete_link_doc(model_behaviour:model_config(), datastore:document()) ->
    ok | datastore:generic_error().
delete_link_doc(ModelConfig, Doc) ->
    delete_doc(ModelConfig, Doc).

%%--------------------------------------------------------------------
%% @doc
%% Deletes document without transaction using worker pool.
%% @end
%%--------------------------------------------------------------------
-spec delete_doc(model_behaviour:model_config(), datastore:document()) ->
    ok | datastore:generic_error().
delete_doc(ModelConfig = #model_config{}, #document{} = Doc) ->
    Bucket = select_bucket(ModelConfig, Doc),
    datastore_pool:post_sync(Bucket, write, {delete_doc_direct, [ModelConfig, Doc]}).

%%--------------------------------------------------------------------
%% @doc
%% Deletes document without transaction.
%% @end
%%--------------------------------------------------------------------
-spec delete_doc_direct(model_behaviour:model_config(), datastore:document()) ->
    ok | datastore:generic_error().
delete_doc_direct(ModelConfig = #model_config{bucket = Bucket}, #document{key = Key, rev = Rev} = ToDel) ->
    {Props} = datastore_json:encode_record(ToDel),
    Doc = {[{<<"_id">>, to_driver_key(Bucket, Key)}, {<<"_rev">>, Rev} | Props]},
    case db_run(select_bucket(ModelConfig, ToDel), couchbeam, delete_doc, [Doc, ?DEFAULT_DB_REQUEST_TIMEOUT_OPT], 3) of
        ok ->
            ok;
        {ok, DelAns} ->
            case verify_ans(DelAns) of
                true ->
                    ok;
                _ ->
                    {error, db_internal_error}
            end;
        {error, key_enoent} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, boolean()} | datastore:generic_error().
exists(#model_config{bucket = _Bucket} = ModelConfig, Key) ->
    case get(ModelConfig, Key) of
        {error, {not_found, _}} ->
            {ok, false};
        {error, Reason} ->
            {error, Reason};
        {ok, _} ->
            {ok, true}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback add_links/3.
%% @end
%%--------------------------------------------------------------------
-spec add_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()]) ->
    ok | datastore:generic_error().
add_links(ModelConfig, Key, Links) when is_list(Links) ->
    critical_section:run(synchronization_link_key(ModelConfig, Key),
        fun() ->
            links_utils:save_links_maps(?MODULE, ModelConfig, Key, Links, add)
        end
    ).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback set_links/3.
%% @end
%%--------------------------------------------------------------------
-spec set_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()]) ->
    ok | datastore:generic_error().
set_links(ModelConfig, Key, Links) when is_list(Links) ->
    critical_section:run(synchronization_link_key(ModelConfig, Key),
        fun() ->
            links_utils:save_links_maps(?MODULE, ModelConfig, Key, Links, set)
        end
    ).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_link/3.
%% @end
%%--------------------------------------------------------------------
-spec create_link(model_behaviour:model_config(), datastore:ext_key(), datastore:normalized_link_spec()) ->
    ok | datastore:create_error().
create_link(ModelConfig, Key, Link) ->
    critical_section:run(synchronization_link_key(ModelConfig, Key),
        fun() ->
            links_utils:create_link_in_map(?MODULE, ModelConfig, Key, Link)
        end
    ).

%%--------------------------------------------------------------------
%% @doc
%% Creates or updates link if does not exist
%% @end
%%--------------------------------------------------------------------
-spec create_or_update_link(model_behaviour:model_config(), datastore:ext_key(), datastore:normalized_link_spec(),
    fun((OldValue :: datastore:normalized_link_target()) -> {ok, NewValue :: datastore:normalized_link_target()}
    | {error, Reason :: term()})) -> ok | datastore:generic_error().
create_or_update_link(ModelConfig, Key, {LinkName, _} = Link, UpdateFun) ->
    critical_section:run(synchronization_link_key(ModelConfig, Key),
        fun() ->
            case links_utils:fetch_link(?MODULE, ModelConfig, LinkName, Key) of
                {error, link_not_found} ->
                    links_utils:save_links_maps(?MODULE, ModelConfig, Key, [Link], set);
                {error, Reason} ->
                    {error, Reason};
                {ok, LinkTarget} ->
                    case UpdateFun(LinkTarget) of
                        {ok, NewLinkValue} ->
                            links_utils:save_links_maps(?MODULE, ModelConfig, Key, [{LinkName, NewLinkValue}], set);
                        Other ->
                            Other
                    end
            end
        end
    ).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete_links/3.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:link_name()] | all) ->
    ok | datastore:generic_error().
delete_links(ModelConfig, Key, all) ->
    critical_section:run(synchronization_link_key(ModelConfig, Key),
        fun() ->
            links_utils:delete_links(?MODULE, ModelConfig, Key)
        end
    );
delete_links(ModelConfig, Key, Links) ->
    critical_section:run(synchronization_link_key(ModelConfig, Key),
        fun() ->
            links_utils:delete_links_from_maps(?MODULE, ModelConfig, Key, Links)
        end
    ).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback fetch_links/3.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(model_behaviour:model_config(), datastore:ext_key(), datastore:link_name()) ->
    {ok, datastore:link_target()} | datastore:link_error().
fetch_link(#model_config{bucket = _Bucket} = ModelConfig, Key, LinkName) ->
    links_utils:fetch_link(?MODULE, ModelConfig, LinkName, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback foreach_link/4.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(model_behaviour:model_config(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error().
foreach_link(#model_config{bucket = _Bucket} = ModelConfig, Key, Fun, AccIn) ->
    links_utils:foreach_link(?MODULE, ModelConfig, Key, Fun, AccIn).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback healthcheck/1.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck(WorkerState :: term()) -> ok | {error, Reason :: term()}.
healthcheck(_State) ->
    try
        Reasons = lists:foldl(fun(Bucket, AccIn) ->
            case get_server(Bucket, []) of
                {ok, Server} ->
                    case couchbeam:server_info(Server) of
                        {ok, _} -> AccIn;
                        Error -> [Error | AccIn]
                    end;
                {error, Reason} ->
                    [Reason | AccIn]
            end
        end, [], get_buckets()),
        case Reasons of
            [] -> ok;
            _ -> {error, Reasons}
        end
    catch
        _:R -> {error, R}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Normalizes given sequence number to non negative integer.
%% @end
%%--------------------------------------------------------------------
-spec normalize_seq(Seq :: non_neg_integer() | binary()) -> non_neg_integer().
normalize_seq(Seq) when is_integer(Seq) ->
    Seq;
normalize_seq(SeqBin) when is_binary(SeqBin) ->
    try binary_to_integer(SeqBin, 10) of
        Seq -> Seq
    catch
        _:_ ->
            try
                ?warning("Changes loss: ~p", [SeqBin]),
                [_SeqStable, SeqCurrent] = binary:split(SeqBin, <<"::">>),
                normalize_seq(SeqCurrent)
            catch
                _:_ ->
                    throw({invalid_seq_format, SeqBin})
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Encodes given given term as binary which maybe human readable if possible.
%% @end
%%--------------------------------------------------------------------
-spec to_binary(term()) -> binary().
to_binary(Term) when is_binary(Term) ->
    Term;
to_binary(Term) when is_atom(Term) ->
    <<?ATOM_PREFIX, (atom_to_binary(Term, utf8))/binary>>;
to_binary(Term) when is_integer(Term) ->
    <<?INT_PREFIX, (integer_to_binary(Term))/binary>>;
to_binary(Term) ->
    Base = base64:encode(term_to_binary(Term)),
    <<?OBJ_PREFIX, Base/binary>>.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates key used by driver itself for storing document for given Bucket/Key combination.
%% @end
%%--------------------------------------------------------------------
-spec to_driver_key(Bucket :: datastore:bucket(), Key :: datastore:key()) -> BinKey :: binary().
to_driver_key(Bucket, Key) ->
    base64:encode(term_to_binary({Bucket, Key})).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Reverses to_driver_key/1
%% @end
%%--------------------------------------------------------------------
-spec from_driver_key(RawKey :: binary()) -> {Bucket :: datastore:bucket(), Key :: datastore:key()}.
from_driver_key(RawKey) ->
    binary_to_term(base64:decode(RawKey)).

%%--------------------------------------------------------------------
%% @equiv get_db(Bucket, []).
%%--------------------------------------------------------------------
-spec get_db(binary()) -> {ok, term()} | {error, term()}.
get_db(Bucket) ->
    get_db(Bucket, []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns DB handle used by couchbeam library to connect to couchdb-based DB.
%% @end
%%--------------------------------------------------------------------
-spec get_db(binary(), [db_run_opt()]) -> {ok, term()} | {error, term()}.
get_db(Bucket, Opts) ->
    case get_server(Bucket, Opts) of
        {ok, Server} ->
            couchbeam:open_db(Server, Bucket, [{recv_timeout, timer:minutes(5)}]);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns server handle used by couchbeam library to connect to couchdb-based DB.
%% @end
%%--------------------------------------------------------------------
-spec get_server(binary(), [db_run_opt()]) -> {ok, term()} | {error, term()}.
get_server(Bucket, Opts) ->
    case lists:member(direct, Opts) of
        true ->
            {DbHost, _DbPort} = utils:random_element(datastore_worker:state_get(db_nodes)),
            {ok, couchbeam:server_connection(DbHost, ?COUCHBASE_API_PORT)};
        false ->
            case couchbase_gateway_sup:get_gateway(Bucket) of
                {ok, Port} ->
                    {ok, couchbeam:server_connection("localhost", Port)};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec db_run(atom(), atom(), [term()], non_neg_integer()) -> term().
db_run(Mod, Fun, Args, Retry) ->
    db_run(default_bucket(), Mod, Fun, Args, Retry).

-spec db_run(couchdb_bucket(), atom(), atom(), [term()], non_neg_integer()) -> term().
db_run(Bucket, Mod, Fun, Args, Retry) ->
    db_run_internal(Bucket, Mod, Fun, Args, Retry, []).

-spec db_run_direct(couchdb_bucket(), atom(), atom(), [term()], non_neg_integer()) -> term().
db_run_direct(Bucket, Mod, Fun, Args, Retry) ->
    db_run_internal(Bucket, Mod, Fun, Args, Retry, [direct]).

-spec db_run_internal(couchdb_bucket(), atom(), atom(), [term()], non_neg_integer(),
    [db_run_opt()]) -> term().
db_run_internal(Bucket, Mod, Fun, Args, Retry, Opts) ->
    {ok, DB} = get_db(Bucket, Opts),
    ?debug("Running CouchBase operation ~p:~p(~p)", [Mod, Fun, Args]),
    case apply(Mod, Fun, [DB | Args]) of
        {error, econnrefused} when Retry > 0 ->
            timer:sleep(timer:seconds(1)),
            db_run(Bucket, Mod, Fun, Args, Retry - 1);
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Inserts given document to database while preserving revision number.
%% Used only for document replication.
%% @end
%%--------------------------------------------------------------------
-spec force_save(model_behaviour:model_config(), datastore:document()) ->
    {{ok, datastore:ext_key()} | datastore:generic_error(),
        ChangedDoc :: datastore:document() | not_changed}.
force_save(#model_config{} = ModelConfig, ToSave) ->
    force_save(ModelConfig, select_bucket(ModelConfig, ToSave), ToSave).


%%--------------------------------------------------------------------
%% @doc
%% Inserts given document to database while preserving revision number.
%% Used only for document replication.
%% @end
%%--------------------------------------------------------------------
-spec force_save(model_behaviour:model_config(), binary(), datastore:document()) ->
    {{ok, datastore:ext_key()} | datastore:generic_error(),
        ChangedDoc :: datastore:document() | not_changed}.
force_save(ModelConfig, BucketOverride,
    #document{key = Key, rev = {RNum, [Id | IdsTail]}} = ToSave) ->
    case get_last(ModelConfig, Key) of
        {error, {not_found, _}} ->
            FinalDoc = ToSave#document{rev = {RNum, [Id]}},
            case save_revision(ModelConfig, BucketOverride, FinalDoc) of
                {ok, _} ->
                    {{ok, Key}, FinalDoc};
                Other ->
                    {Other, not_changed}
            end;
        {error, not_found} ->
            FinalDoc = ToSave#document{rev = {RNum, [Id]}},
            case save_revision(ModelConfig, BucketOverride, FinalDoc) of
                {ok, _} ->
                    {{ok, Key}, FinalDoc};
                Other ->
                    {Other, not_changed}
            end;
        {error, Reason} ->
            {{error, Reason}, not_changed};
        {ok, #document{key = Key, rev = Rev, deleted = OldDel} = Old} ->
            {OldRNum, OldId} = rev_to_info(Rev),
            case RNum of
                OldRNum ->
                    case Id > OldId of
                        true ->
                            case save_revision(ModelConfig, BucketOverride, ToSave) of
                                {ok, _} ->
                                    % TODO - what happens if first save is ok and second fails
                                    % Delete in new task type that starts if first try fails
                                    case OldDel of
                                        true -> ok;
                                        _ -> delete_doc(ModelConfig, Old)
                                    end,
                                    {{ok, Key}, ToSave};
                                Other ->
                                    {Other, not_changed}
                            end;
                        false ->
                            {{ok, Key}, not_changed}
                    end;
                Higher when Higher > OldRNum ->
                    NewIDs = check_revisions_list(OldId, IdsTail, OldRNum, Higher),
                    FinalDoc = ToSave#document{rev = {RNum, [Id | NewIDs]}},
                    case save_revision(ModelConfig, BucketOverride, FinalDoc) of
                        {ok, _} ->
                            {{ok, Key}, FinalDoc};
                        Other ->
                            {Other, not_changed}
                    end;
                _ ->
                    {{ok, Key}, not_changed}
            end
    end.

%% @todo: VFS-2825 Use this functions to optimize dbsync performance
%%force_save(#model_config{} = ModelConfig, BucketOverride, Doc) ->
%%    [Res] = save_docs(BucketOverride, [{ModelConfig, Doc}], [{<<"new_edits">>, false}] ++ ?DEFAULT_DB_REQUEST_TIMEOUT_OPT),
%%    Res.
%%
%%
%%bulk_force_save(BucketOverride, Docs) ->
%%    bulk_force_save(BucketOverride, Docs, []).
%%bulk_force_save(BucketOverride, Docs, Opts) ->
%%    save_docs(BucketOverride, Docs, Opts ++ [{<<"new_edits">>, false}] ++ ?DEFAULT_DB_REQUEST_TIMEOUT_OPT).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%                                    CHANGES                                         %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(state, {
    callback,
    until,
    last_seq = 0,
    bucket :: binary()
}).

-type gen_changes_state() :: #state{}.

%% API

%%--------------------------------------------------------------------
%% @doc
%% Fetches latest revision of document with given key. As in changes stream,
%% revision field is populated with structure describing all the revisions
%% of the document.
%% @end
%%--------------------------------------------------------------------
-spec get_with_revs(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get_with_revs(#model_config{bucket = Bucket, name = ModelName} = ModelConfig, Key) ->
    Args = [to_driver_key(Bucket, Key), [{<<"revs">>, <<"true">>}]],
    case db_run(select_bucket(ModelConfig, Key), couchbeam, open_doc, Args, 3) of
        {ok, {Proplist} = _Doc} ->
            case verify_ans(Proplist) of
                true ->
                    Proplist1 = [KV || {<<"_", _/binary>>, _} = KV <- Proplist],
                    Proplist2 = Proplist -- Proplist1,

                    {_, {RevsRaw}} = lists:keyfind(<<"_revisions">>, 1, Proplist),
                    {_, Revs} = lists:keyfind(<<"ids">>, 1, RevsRaw),
                    {_, Start} = lists:keyfind(<<"start">>, 1, RevsRaw),
                    {_WasUpdated, Version, Value} = datastore_json:decode_record_vcs({Proplist2}),
                    {ok, #document{
                        key = Key,
                        value = Value,
                        version = Version,
                        rev = {Start, Revs}}
                    };
                _ ->
                    {error, db_internal_error}
            end;
        {error, {not_found, _}} ->
            {error, {not_found, ModelName}};
        {error, not_found} ->
            {error, {not_found, ModelName}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Starts changes stream with given callback function that is called on every change received from DB.
%% @end
%%--------------------------------------------------------------------
-spec changes_start_link(
    Callback :: fun((Seq :: non_neg_integer(), datastore:document() | stream_ended, model_behaviour:model_type() | undefined) -> ok),
    Since :: non_neg_integer(), Until :: non_neg_integer() | infinity) -> {ok, pid()}.
changes_start_link(Callback, Since, Until) ->
    changes_start_link(Callback, Since, Until, <<"default">>).
changes_start_link(Callback, Since, Until, Bucket) ->
    {ok, Db} = get_db(Bucket),
    Opts = [{<<"include_docs">>, <<"true">>}, {since, Since}, {<<"revs_info">>, <<"true">>}],
    gen_changes:start_link(?MODULE, Db, Opts, [Callback, Until, Bucket]).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% init/1 callback for gen_changes server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: [term()]) -> {ok, gen_changes_state()}.
init([Callback, Until, Bucket]) ->
    ?debug("Starting changes stream until ~p", [Until]),
    {ok, HS} = application:get_env(?CLUSTER_WORKER_APP_NAME, changes_max_heap_size_words),
    erlang:process_flag(max_heap_size, HS),
    {ok, #state{callback = Callback, until = Until, bucket = Bucket}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% handle_change/2 callback for gen_changes server.
%% @end
%%--------------------------------------------------------------------
-spec handle_change(term(), gen_changes_state()) -> {noreply, gen_changes_state()} | {stop, normal, gen_changes_state()}.
handle_change({done, _LastSeq}, State) ->
    {noreply, State};


handle_change(Change, #state{callback = Callback, until = Until, last_seq = LastSeq, bucket = Bucket} = State) when Until > LastSeq; Until =:= infinity ->
    NewChanges =
        try
            RawDoc = doc(Change),
            Seq = normalize_seq(seq(Change)),
            Deleted = deleted(Change),

            RawDocOnceAgain = jiffy:decode(jsx:encode(RawDoc)),
            Document = process_raw_doc(Bucket, RawDocOnceAgain),

            Callback(Seq, Document#document{deleted = Deleted}, model(Document)),
            State#state{last_seq = max(Seq, LastSeq)}
        catch
            _:{badmatch, false} ->
                State;
            _:{badmatch, {error, not_found}} = Reason -> %% This revision is an ancestor to deleted one
                ?debug_stacktrace("Unable to process CouchDB change ~p due to ~p", [Change, Reason]),
                State;
            _:Reason ->
                ?error_stacktrace("Unable to process CouchDB change ~p due to ~p", [Change, Reason]),
                State
        end,
    {noreply, NewChanges};
handle_change(_Change, #state{callback = Callback, until = Until, last_seq = LastSeq} = State) ->
    ?info("Changes stream has ended: until ~p, LastSeq ~p", [Until, LastSeq]),
    Callback(LastSeq, stream_ended, undefined),
    {stop, normal, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% handle_call/3 callback for gen_changes server.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(term(), pid(), gen_changes_state()) -> {reply, term(), gen_changes_state()}.
handle_call(_Req, _From, State) ->
    {reply, _Req, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% handle_cast/2 callback for gen_changes server.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(term(), gen_changes_state()) -> {noreply, gen_changes_state()}.
handle_cast(_Msg, State) -> {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% handle_info/2 callback for gen_changes server.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(term(), gen_changes_state()) -> {noreply, gen_changes_state()}.
handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% terminate/2 callback for gen_changes server.
%% @end
%%--------------------------------------------------------------------
-spec terminate(term(), gen_changes_state()) -> ok.
terminate(Reason, _State) ->
    ?warning("~p terminating with reason ~p~n", [?MODULE, Reason]),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Inserts given revision of document to database. Used only for document replication.
%% @end
%%--------------------------------------------------------------------
-spec save_revision(model_behaviour:model_config(), binary(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save_revision(#model_config{} = ModelConfig, BucketOverride, #document{} = ToSave) ->
    datastore_pool:post_sync(BucketOverride, write,
        {save_revision_direct, [ModelConfig, BucketOverride, ToSave]}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Inserts given revision of document to database. Used only for document replication.
%% @end
%%--------------------------------------------------------------------
-spec save_revision_direct(model_behaviour:model_config(), binary(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save_revision_direct(#model_config{bucket = Bucket} = ModelConfig, BucketOverride,
    #document{deleted = Del, key = Key, rev = {Start, Ids} = Revs, value = Value} = ToSave) ->
    ok = assert_value_size(Value, ModelConfig, Key),
    {Props} = datastore_json:encode_record(ToSave),
    Doc = {[{<<"_revisions">>, {[{<<"ids">>, Ids}, {<<"start">>, Start}]}}, {<<"_rev">>, rev_info_to_rev(Revs)},
        {<<"_id">>, to_driver_key(Bucket, Key)}, {<<"_deleted">>, Del} | Props]},
    case db_run(BucketOverride, couchbeam, save_doc, [Doc, [{<<"new_edits">>, <<"false">>}] ++ ?DEFAULT_DB_REQUEST_TIMEOUT_OPT], 3) of
        {ok, {SaveAns}} ->
            case verify_ans(SaveAns) of
                true ->
                    {ok, Key};
                _ ->
                    {error, db_internal_error}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts raw document given by CouchDB to datastore's #document.
%% @end
%%--------------------------------------------------------------------
-spec process_raw_doc(binary(), term()) -> datastore:document().
process_raw_doc(Bucket, {RawDoc}) ->
    {_, Rev} = lists:keyfind(<<"_rev">>, 1, RawDoc),
    {_, RawKey} = lists:keyfind(<<"_id">>, 1, RawDoc),
    {_, Key} = from_driver_key(RawKey),
    RawDoc1 = [KV || {<<"_", _/binary>>, _} = KV <- RawDoc],
    RawDoc2 = RawDoc -- RawDoc1,
    {ok, {RawRichDoc}} = db_run(Bucket, couchbeam, open_doc, [RawKey, [{<<"revs">>, <<"true">>}, {<<"rev">>, Rev}]], 3),
    true = verify_ans(RawRichDoc),
    {_, {RevsRaw}} = lists:keyfind(<<"_revisions">>, 1, RawRichDoc),
    {_, Revs} = lists:keyfind(<<"ids">>, 1, RevsRaw),
    {_, Start} = lists:keyfind(<<"start">>, 1, RevsRaw),
    {_WasUpdated, Version, Value} = datastore_json:decode_record_vcs({RawDoc2}),
    #document{key = Key, rev = {Start, Revs}, value = Value, version = Version}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Extracts CouchDB document term from changes term received from couchbeam library.
%% @end
%%--------------------------------------------------------------------
-spec doc({change, term()}) -> term().
doc({change, {Props}}) ->
    {_, Doc} = lists:keyfind(<<"doc">>, 1, Props),
    Doc.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Extracts CouchDB 'seq info' term from changes term received from couchbeam library.
%% @end
%%--------------------------------------------------------------------
-spec seq(term()) -> non_neg_integer().
seq({change, {Props}}) ->
    {_, LastSeq} = lists:keyfind(<<"seq">>, 1, Props),
    LastSeq.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Extracts info about document deletion
%% @end
%%--------------------------------------------------------------------
-spec deleted(term()) -> boolean().
deleted({change, {Props}}) ->
    case lists:keyfind(<<"deleted">>, 1, Props) of
        {_, true} -> true;
        _ -> false
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Extracts model type from given #document.
%% @end
%%--------------------------------------------------------------------
-spec model(datastore:document()) -> model_behaviour:model_type().
model(#document{value = #links{model = ModelName}}) ->
    ModelName;
model(#document{value = Value}) ->
    element(1, Value).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts given 'rev info' tuple into text (binary) representation.
%% @end
%%--------------------------------------------------------------------
-spec rev_info_to_rev({Num :: non_neg_integer() | binary(), [Hash :: binary()]}) ->
    binary().
rev_info_to_rev({Num, [_Hash | _] = Revs}) when is_integer(Num) ->
    rev_info_to_rev({integer_to_binary(Num), Revs});
rev_info_to_rev({NumBin, [Hash | _]}) when is_binary(NumBin) ->
    <<NumBin/binary, "-", Hash/binary>>.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts given binary into tuple {revision_num, hash}.
%% @end
%%--------------------------------------------------------------------
-spec rev_to_info(binary()) ->
    {Num :: non_neg_integer() | binary(), Hash :: binary()}.
rev_to_info(Rev) ->
    [Num, ID] = binary:split(Rev, <<"-">>),
    {binary_to_integer(Num), ID}.


%%--------------------------------------------------------------------
%% @doc
%% Converts given binary into revision number.
%% @end
%%--------------------------------------------------------------------
-spec rev_to_number(binary()) -> non_neg_integer().
rev_to_number(Rev) ->
    [Num, _ID] = binary:split(Rev, <<"-">>),
    binary_to_integer(Num).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Ensure that given term does not exceed maximum document's value size.
%% @end
%%--------------------------------------------------------------------
-spec assert_value_size(Value :: term(), model_behaviour:model_config(), datastore:ext_key()) -> ok | no_return().
assert_value_size(Value, ModelConfig, Key) ->
    case byte_size(term_to_binary(Value)) > ?MAX_VALUE_SIZE of
        true ->
            ?error_stacktrace("term_too_big: key ~p, model ~p, value ~p",
                [Key, ModelConfig, Value]),
            error(term_too_big);
        false -> ok
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets last version of document event if it was deleted.
%% @end
%%--------------------------------------------------------------------
-spec get_last(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get_last(#model_config{} = ModelConfig, Key) ->
    get_last(ModelConfig, select_bucket(ModelConfig, Key), Key).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets last version of document event if it was deleted.
%% @end
%%--------------------------------------------------------------------
-spec get_last(model_behaviour:model_config(), binary(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get_last(#model_config{bucket = Bucket, name = ModelName} = ModelConfig, BucketOverride, Key) ->
    case get(ModelConfig, BucketOverride, Key) of
        {error, {not_found, ModelName}} ->
            case db_run(BucketOverride, couchbeam, open_doc, [to_driver_key(Bucket, Key), [{<<"open_revs">>, all}]], 3) of
                {ok, {multipart, M}} ->
                    case collect_mp(couchbeam:stream_doc(M), []) of
                        List when is_list(List) ->
                            lists:foldl(fun({doc, {Proplist}}, Ans) ->
                                DocAnc = case verify_ans(Proplist) of
                                    true ->
                                        {_, Rev} = lists:keyfind(<<"_rev">>, 1, Proplist),
                                        Proplist1 = [KV || {<<"_", _/binary>>, _} = KV <- Proplist],
                                        Proplist2 = Proplist -- Proplist1,
                                        {_WasUpdated, Version, Value} = datastore_json:decode_record_vcs({Proplist2}),
                                        Deleted = case lists:keyfind(<<"deleted">>, 1, Proplist) of
                                            {_, true} -> true;
                                            _ -> false
                                        end,
                                        RetDoc = #document{key = Key, value = Value, rev = Rev, version = Version, deleted = Deleted},
                                        {ok, RetDoc};
                                    _ ->
                                        {error, db_internal_error}
                                end,
                                case {Ans, DocAnc} of
                                    {{error, empty_answer}, _} ->
                                        DocAnc;
                                    {{error, db_internal_error}, _} ->
                                        {error, db_internal_error};
                                    {_, {error, db_internal_error}} ->
                                        {error, db_internal_error};
                                    {{ok, #document{rev = R1} = D1}, {ok, #document{rev = R2} = D2}} ->
                                        {R1Num, R1Id} = rev_to_info(R1),
                                        {R2Num, R2Id} = rev_to_info(R2),
                                        case R1Num of
                                            R2Num ->
                                                case R1Id > R2Id of
                                                    true ->
                                                        {ok, D1};
                                                    false ->
                                                        {ok, D2}
                                                end;
                                            Higher when Higher > R2Num ->
                                                {ok, D1};
                                            _ ->
                                                {ok, D2}
                                        end
                                end
                            end, {error, empty_answer}, List)
                    end;
                {error, {not_found, _}} ->
                    {error, {not_found, ModelName}};
                {error, not_found} ->
                    {error, {not_found, ModelName}};
                {error, Reason} ->
                    {error, Reason}
            end;
        GetAns ->
            GetAns
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Collects multipart answer.
%% @end
%%--------------------------------------------------------------------
-spec collect_mp(tuple(), list()) -> list().
collect_mp({doc, Doc, Next}, Acc) ->
    collect_mp(couchbeam:stream_doc(Next), [{doc, Doc} | Acc]);
collect_mp({att, Name, Next}, Acc) ->
    collect_mp(couchbeam:stream_doc(Next), [{Name, <<>>} | Acc]);
collect_mp({att_body, Name, Chunk, Next}, Acc) ->
    Buffer = proplists:get_value(Name, Acc),
    NBuffer = <<Buffer/binary, Chunk/binary>>,
    Acc1 = lists:keystore(Name, 1, Acc, {Name, NBuffer}),
    collect_mp(couchbeam:stream_doc(Next), Acc1);
collect_mp({att_eof, _Name, Next}, Acc) ->
    collect_mp(couchbeam:stream_doc(Next), Acc);
collect_mp(eof, Acc) ->
    Acc.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%                                 CUSTOM VIEWS                                       %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%--------------------------------------------------------------------
%% @doc
%% Add design doc with custom view under given Id. The ViewFunction will be
%% invoked for each instance of given record stored in db, and should return null or key,
%% that will be queried with get_view function later.
%% @end
%%--------------------------------------------------------------------
-spec add_view(ModelName :: model_behaviour:model_type(), binary(), binary(), boolean()) -> ok.
add_view(ModelName, Id, ViewFunction, Spatial) ->
    DesignId = <<"_design/", Id/binary>>,
    Doc = case Spatial of
        true ->
            jiffy:decode(jiffy:encode(#{
                <<"_id">> => DesignId,
                <<"spatial">> => #{Id => ViewFunction}
            }));
        false ->
            jiffy:decode(jiffy:encode(#{
                <<"_id">> => DesignId,
                <<"views">> => #{Id => #{<<"map">> => ViewFunction}}
            }))
    end,

    {ok, SaveAns} = db_run_direct(select_bucket(ModelName:model_init()), couchbeam, save_doc, [Doc], 5),
    true = verify_ans(SaveAns),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Streams documents from given view in form of messages:
%% {Ref, {stream_data, DocKey :: datastore:ext_key()}} - received DocKey of document in view
%% {Ref, {stream_ended, Reason :: done | {error, Reason :: any()}} - stream has ended
%% {Ref, {stream_data, {unknown, Unknown :: any()}}} - unknown data from stream,
%% @end
%%--------------------------------------------------------------------
-spec stream_view(ModelName :: model_behaviour:model_type(), Id :: binary(), Options :: [term()]) ->
    Ref :: term().
stream_view(ModelName, Id, Options) ->
    Host = self(),
    spawn_link(fun() ->
        case db_run_direct(select_bucket(ModelName:model_init()), couchbeam_view, stream, [{Id, Id}, Options], 3) of
            {ok, Ref} ->
                Loop = fun LoopFun() ->
                    receive
                        {Ref, done} ->
                            Host ! {self(), {stream_ended, done}};
                        {Ref, {row, {Proplist}}} ->
                            try
                                case lists:keyfind(<<"id">>, 1, Proplist) of
                                    {<<"id">>, <<"_sync:", _/binary>>} ->
                                        ok;
                                    {<<"id">>, DbDocId} ->
                                        {_, DocKey} = from_driver_key(DbDocId),
                                        Host ! {self(), {stream_data, DocKey}}
                                end
                            catch
                                _:Reason ->
                                    ?warning_stacktrace("Stream View: Unable to process document ~p due to ~p", [{Proplist}, Reason])
                            end,
                            LoopFun();
                        {Ref, Unknown} ->
                            Host ! {self(), {stream_data, {unknown, Unknown}}}
                    end end,
                Loop();
            {error, Reason} ->
                Host ! {self(), {stream_ended, {error, Reason}}}
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% Get list of document ids for view with given id, matching given key.
%%
%% Available options:
%% Options :: view_options() [{key, binary()} | {start_docid, binary()}
%%    | {end_docid, binary()} | {start_key, binary()}
%%    | {end_key, binary()} | {limit, integer()}
%%    | {stale, stale()}
%%    | descending
%%    | {skip, integer()}
%%    | group | {group_level, integer()}
%%    | {inclusive_end, boolean()} | {reduce, boolean()} | reduce | include_docs | conflicts
%%    | {keys, list(binary())}
%%    | `{stream_to, Pid}': the pid where the changes will be sent,
%%      by default the current pid. Used for continuous and longpoll
%%      connections
%%
%%      {key, Key}: key value
%%      {start_docid, DocId}: document id to start with (to allow pagination
%%          for duplicate start keys
%%      {end_docid, DocId}: last document id to include in the result (to
%%          allow pagination for duplicate endkeys)
%%      {start_key, Key}: start result from key value
%%      {end_key, Key}: end result from key value
%%      {limit, Limit}: Limit the number of documents in the result
%%      {stale, Stale}: If stale=ok is set, CouchDB will not refresh the view
%%      even if it is stale, the benefit is a an improved query latency. If
%%      stale=update_after is set, CouchDB will update the view after the stale
%%      result is returned.
%%      descending: reverse the result
%%      {skip, N}: skip n number of documents
%%      group: the reduce function reduces to a single result
%%      row.
%%      {group_level, Level}: the reduce function reduces to a set
%%      of distinct keys.
%%      {reduce, boolean()}: whether to use the reduce function of the view. It defaults to
%%      true, if a reduce function is defined and to false otherwise.
%%      include_docs: automatically fetch and include the document
%%      which emitted each view entry
%%      {inclusive_end, boolean()}: Controls whether the endkey is included in
%%      the result. It defaults to true.
%%      conflicts: include conflicts
%%      {keys, [Keys]}: to pass multiple keys to the view query
%%      {spatial, boolean()}: to query view of spatial type
%%
%% @end
%%--------------------------------------------------------------------
-spec query_view(ModelName :: model_behaviour:model_type(), binary(), list()) -> {ok, [binary()]}.
query_view(ModelName, Id, Options) ->
    case db_run_direct(select_bucket(ModelName:model_init()), couchbeam_view, fetch, [{Id, Id}, Options], 3) of
        {ok, List} ->
            case verify_ans(List) of
                true ->
                    Ids = lists:filtermap(fun
                        ({[{<<"id">>, <<"_sync:", _/binary>>} | _]}) ->
                            false;
                        ({[{<<"id">>, DbDocId} | _]}) ->
                            {_, DocUuid} = from_driver_key(DbDocId),
                            {true, DocUuid}
                    end, List),
                    {ok, Ids};
                _ ->
                    {error, db_internal_error}
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Delete view and design doc with given Id
%% @end
%%--------------------------------------------------------------------
-spec delete_view(ModelName :: model_behaviour:model_type(), binary()) -> ok.
delete_view(ModelName, Id) ->
    db_run_direct(select_bucket(ModelName:model_init()), couchbeam, delete_design, [Id], 5).

%%--------------------------------------------------------------------
%% @doc
%% Returns critical section's resource ID for document operations using this driver.
%% @end
%%--------------------------------------------------------------------
-spec synchronization_doc_key(model_behaviour:model_config(), datastore:ext_key()) -> binary().
synchronization_doc_key(#model_config{bucket = Bucket}, Key) ->
    to_binary({?MODULE, Bucket, doc, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Returns critical section's resource ID for link operations using this driver.
%% @end
%%--------------------------------------------------------------------
-spec synchronization_link_key(model_behaviour:model_config(), datastore:ext_key()) -> binary().
synchronization_link_key(#model_config{bucket = Bucket}, Key) ->
    to_binary({?MODULE, Bucket, link, Key}).


%%--------------------------------------------------------------------
%% @doc
%% @equiv select_bucket(ModelConfig, undefined)
%% @end
%%--------------------------------------------------------------------
-spec select_bucket(model_behaviour:model_config()) -> couchdb_bucket().
select_bucket(ModelConfig) ->
    select_bucket(ModelConfig, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Returns couchdb's bucket name for given Model and/or document's Key
%% @end
%%--------------------------------------------------------------------
-spec select_bucket(model_behaviour:model_config(), datastore:ext_key() | undefined) ->
    couchdb_bucket().
select_bucket(ModelConfig, #document{key = Key}) ->
    select_bucket(ModelConfig, Key);
select_bucket(#model_config{sync_enabled = true}, ?NOSYNC_WRAPPED_KEY_OVERRIDE(_)) ->
    default_bucket();
select_bucket(#model_config{sync_enabled = true}, Key) when is_binary(Key) ->
    case binary:match(Key, ?NOSYNC_KEY_OVERRIDE_PREFIX) of
        nomatch ->
            sync_enabled_bucket();
        _ ->
            default_bucket()
    end;
select_bucket(#model_config{sync_enabled = true}, _Key) ->
    sync_enabled_bucket();
select_bucket(#model_config{}, _Key) ->
    default_bucket().


%%--------------------------------------------------------------------
%% @doc
%% Check if ans is ok
%% @end
%%--------------------------------------------------------------------
-spec verify_ans(term()) -> boolean().
verify_ans(Ans) when is_list(Ans) ->
    lists:foldl(fun(E, Acc) ->
        case Acc of
            false ->
                false;

            _ ->
                verify_ans(E)
        end
    end, true, Ans);
verify_ans({<<"error">>, _} = Ans) ->
    ?error("Couch db error: ~p", [Ans]),
    false;
verify_ans({Ans}) ->
    verify_ans(Ans);
verify_ans(_Ans) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% Parse JSON response form DB and return corresponding result normalize to datastore standards.
%% @end
%%--------------------------------------------------------------------
-spec parse_response(get | save, [datastore_json:ejson()] | datastore_json:ejson()) ->
    Response | [Response] when
    Response :: {ok, datastore:ext_key() | datastore:document()} | datastore:generic_error().
parse_response(_, []) ->
    [];
parse_response(OpType, [E | T]) ->
    [parse_response(OpType, E) | parse_response(OpType, T)];
parse_response(OpType, {_} = JSONTerm) ->
    JSONBin = json_utils:encode(JSONTerm),
    JSONMap = json_utils:decode_map(JSONBin),
    case maps:get(<<"error">>, JSONMap, undefined) of
        undefined ->
            MaybeDoc =
                case maps:get(<<"doc">>, JSONMap, undefined) of
                    undefined ->
                        JSONMap;
                    Doc -> Doc
                end,
            case OpType of
                save ->
                    {_, Key} = from_driver_key(maps:get(<<"id">>, JSONMap, maps:get(<<"_id">>, JSONMap, undefined))),
                    {ok, Key};
                get ->
                    {ok, jiffy:decode(jiffy:encode(MaybeDoc))}
            end;
        <<"conflict">> ->
            {error, already_exists};
        Error ->
            {error, binary_to_atom(Error, utf8)}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks if revision list provided to force_save can be written.
%% @end
%%--------------------------------------------------------------------
-spec check_revisions_list(term(), list(), integer(), integer()) -> list().
check_revisions_list(OldID, [OldID | _] = NewIDs, OldNum, NewNum) when NewNum =:= OldNum + 1 ->
    NewIDs;
check_revisions_list(_, _, _, _) ->
    [].
