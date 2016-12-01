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

-type couchdb_bucket() :: binary().
-export_type([couchdb_bucket/0]).

%% store_driver_behaviour callbacks
-export([init_bucket/3, healthcheck/1, init_driver/1]).
-export([save/2, create/2, update/3, create_or_update/3, exists/2, get/2, list/4, delete/3, is_model_empty/1]).
-export([add_links/3, set_links/3, create_link/3, create_or_update_link/4, delete_links/3, fetch_link/3, foreach_link/4]).
-export([synchronization_doc_key/2, synchronization_link_key/2]).

-export([start_gateway/5, get/3, force_save/2, force_save/3, db_run/4, db_run/5, normalize_seq/1]).
-export([save_docs/2, save_docs/3, get_docs/2]).

-export([changes_start_link/3, changes_start_link/4, get_with_revs/2]).
-export([init/1, handle_call/3, handle_info/2, handle_change/2, handle_cast/2, terminate/2]).
-export([save_link_doc/2, get_link_doc/2, delete_link_doc/2, exists_link_doc/3]).
-export([to_binary/1]).
-export([add_view/3, query_view/3, delete_view/2, stream_view/3]).
-export([default_bucket/0, sync_enabled_bucket/0]).
-export([rev_to_number/1]).

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
init_driver(#{db_nodes := DBNodes} = State) ->
    Port = 8091,

    BucketInfo = lists:foldl(fun
        ({Hostname, _}, no_data) ->
            URL = <<Hostname/binary, ":", (integer_to_binary(Port))/binary, "/pools/default/buckets">>,
            case http_client:get(URL) of
                {ok, 200, _, JSON} ->
                    json_utils:decode_map(JSON);
                Res ->
                    ?warning("Unable to fetch bucket info from ~p. REST reponse: ~p", [Hostname, Res]),
                    no_data
            end;
        (_, Data) ->
            Data
    end, no_data, DBNodes),

    Buckets = case BucketInfo of
        no_data ->
            ?warning("Unable to fetch bucket info. Using only default bucket."),
            [?DEFAULT_BUCKET];
        JSONData ->
            lists:map(
                fun(BucketMap) ->
                    maps:get(<<"name">>, BucketMap)
                end, JSONData)
    end,

    ?info("CouchDB driver initializing with buckets: ~p", [Buckets]),

    AllGateways = lists:foldl(
        fun({BucketNo, Bucket}, Gateways) ->
            Gateways ++ lists:map(fun({N, {Hostname, _Port}}) ->
                GWState = proc_lib:start_link(?MODULE, start_gateway, [self(), N, Hostname, Port, Bucket],
                    ?DATASTORE_GATEWAY_SPAWN_TIMEOUT),
                {N, GWState}
            end, lists:zip(lists:seq((BucketNo - 1) * length(DBNodes) + 1, BucketNo * length(DBNodes)), DBNodes))
        end, [], lists:zip(lists:seq(1, length(Buckets)), Buckets)),


    {ok, State#{db_gateways => maps:from_list(AllGateways), available_buckets => Buckets}}.


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
            <<"versions">> =>
                #{<<"map">> =>
                    <<"function(doc) { emit([doc['", ?RECORD_TYPE_MARKER, "'], doc['", ?RECORD_VERSION_MARKER ,"']], 1); }">>}
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
-spec get_buckets() -> [binary()] | undefined.
get_buckets() ->
    datastore_worker:state_get(available_buckets).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/2.
%% @end
%%--------------------------------------------------------------------
-spec save(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(ModelConfig, #document{rev = undefined, key = Key, value = Value} = Doc) ->
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
save_doc(#model_config{aggregate_db_writes = Aggregate} = ModelConfig, ToSave = #document{}) ->
    {ok, {Pid, _}} = get_server(select_bucket(ModelConfig, ToSave)),
    Ref = make_ref(),

    case Aggregate of
        true ->
            Pid ! {save_doc, {{self(), Ref}, {ModelConfig, ToSave}}},
            
            receive
                {Ref, Response} ->
                    Response
            after
                ?DOCUMENT_AGGREGATE_SAVE_TIMEOUT ->
                    {error, gateway_loop_timeout}
            end;
        false ->
            [RawDoc] = make_raw_doc(ModelConfig, [ToSave]),
            {ok, [RawRes]} = db_run(select_bucket(ModelConfig, ToSave), couchbeam, save_docs,
                [[RawDoc], ?DEFAULT_DB_REQUEST_TIMEOUT_OPT], 3),
            parse_response(save, RawRes)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Saves documents in batch.
%% @end
%%--------------------------------------------------------------------
-spec save_docs(model_behaviour:model_config() | couchdb_bucket(), [{model_behaviour:model_config(), datastore:ext_key()}]) ->
    [{ok, datastore:ext_key()} | datastore:generic_error()].
save_docs(DBBucketOrModelConfig, Documents) ->
    save_docs(DBBucketOrModelConfig, Documents, []).
save_docs(#model_config{} = ModelConfig, Documents, Opts) ->
    save_docs(select_bucket(ModelConfig), Documents, Opts);
save_docs(DBBucket, Documents, Opts) ->
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
    get_docs(select_bucket(ModelConfig), KeysWithModelConfig);
get_docs(DBBucket, KeysWithModelConfig) ->
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
    Fun :: datastore:list_fun(), AccIn :: term(), _Opts :: store_driver_behaviour:list_options()) -> no_return().
list(#model_config{bucket = Bucket, name = ModelName} = ModelConfig, Fun, AccIn, _Mode) ->
    BinModelName = atom_to_binary(ModelName, utf8),
    _BinBucket = atom_to_binary(Bucket, utf8),
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
delete(ModelConfig, Key, Pred) ->
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
        end).

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
%% Deletes document not using transactions.
%% @end
%%--------------------------------------------------------------------
-spec delete_doc(model_behaviour:model_config(), datastore:document()) ->
    ok | datastore:generic_error().
delete_doc(ModelConfig = #model_config{bucket = Bucket}, #document{key = Key, value = Value, rev = Rev} = ToDel) ->
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
        Reasons = lists:foldl(
            fun(Bucket, AccIn) ->
                case get_server(Bucket) of
                    {ok, _} -> AccIn;
                    {error, Reason} ->
                        [Reason | AccIn]
                end
            end, [], get_buckets()),
        case Reasons of
            [] -> ok;
            _ ->
                {error, Reasons}
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

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Encodes given term to base64 binary.
%% @end
%%--------------------------------------------------------------------
-spec term_to_base64(term()) -> binary().
term_to_base64(Term) ->
    Base = base64:encode(term_to_binary(Term)),
    <<?OBJ_PREFIX, Base/binary>>.


%%--------------------------------------------------------------------
%% @private
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
    term_to_base64(Term).


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
%% @private
%% @doc
%% Returns DB handle used by couchbeam library to connect to couchdb-based DB.
%% @end
%%--------------------------------------------------------------------
-spec get_db(binary()) -> {ok, {pid, term()}} | {error, term()}.
get_db(Bucket) ->
    case get_server(Bucket) of
        {error, Reason} ->
            {error, Reason};
        {ok, {ServerLoop, Server}} ->
            try
                {ok, DB} = couchbeam:open_db(Server, Bucket, [{recv_timeout, timer:minutes(5)}]),
                {ok, {ServerLoop, DB}}
            catch
                _:Reason ->
                    Reason %% Just to silence dialyzer since couchbeam methods supposedly have no return.
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns server handle used by couchbeam library to connect to couchdb-based DB.
%% @end
%%--------------------------------------------------------------------
-spec get_server(binary()) -> {ok, {pid(), term()}} | {error, term()}.
get_server(Bucket) ->
    get_server(datastore_worker:state_get(db_gateways), Bucket).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns server handle used by couchbeam library to connect to couchdb-based DB.
%% @end
%%--------------------------------------------------------------------
-spec get_server(State :: worker_host:plugin_state(), binary()) -> {ok, {pid, term()}} | {error, term()}.
get_server(DBGateways, Bucket) ->
    Gateways = maps:values(DBGateways),
    ActiveGateways = [GW || #{status := running, bucket := LBucket} = GW <- Gateways, LBucket =:= Bucket],

    case ActiveGateways of
        [] ->
            ?warning("Unable to select CouchBase Gateway for bucket ~p: no active gateway among: ~p", [Bucket, Gateways]),
            {error, no_active_gateway};
        _ ->
            try
                #{gw_admin_port := Port, server := ServerLoop} = lists:nth(crypto:rand_uniform(1, length(ActiveGateways) + 1), ActiveGateways),
                Server = couchbeam:server_connection("localhost", Port),
                {ok, {ServerLoop, Server}}
            catch
                _:Reason ->
                    Reason %% Just to silence dialyzer since couchbeam methods supposedly have no return.
            end
    end.

-spec db_run(atom(), atom(), [term()], non_neg_integer()) -> term().
db_run(Mod, Fun, Args, Retry) ->
    db_run(default_bucket(), Mod, Fun, Args, Retry).

-spec db_run(couchdb_bucket(), atom(), atom(), [term()], non_neg_integer()) -> term().
db_run(Bucket, Mod, Fun, Args, Retry) ->
    {ok, {ServerPid, DB}} = get_db(Bucket),
    ?debug("Running CouchBase operation ~p:~p(~p)", [Mod, Fun, Args]),
    case apply(Mod, Fun, [DB | Args]) of
        {error, econnrefused} when Retry > 0 ->
            ?info("Unable to connect to ~p", [DB]),
            ServerPid ! restart,
            timer:sleep(crypto:rand_uniform(20, 50)),
            db_run(Bucket, Mod, Fun, Args, Retry - 1);
        Other ->
            Other
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%                          couchbase-sync-gateway management                         %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%--------------------------------------------------------------------
%% @doc
%% Inserts given document to database while preserving revision number. Used only for document replication.
%% @end
%%--------------------------------------------------------------------
-spec force_save(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
force_save(#model_config{} = ModelConfig, ToSave) ->
    force_save(ModelConfig, select_bucket(ModelConfig, ToSave), ToSave).


%%--------------------------------------------------------------------
%% @doc
%% Inserts given document to database while preserving revision number. Used only for document replication.
%% @end
%%--------------------------------------------------------------------
-spec force_save(model_behaviour:model_config(), binary(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
force_save(ModelConfig, BucketOverride,
    #document{key = Key, rev = {RNum, [Id | IdsTail]}, value = V} = ToSave) ->
    SynchKey = case V of
                   #links{doc_key = DK} ->
                       synchronization_link_key(ModelConfig, DK);
                   _ ->
                       synchronization_doc_key(ModelConfig, Key)
               end,
    critical_section:run(SynchKey,
        fun() ->
            case get_last(ModelConfig, Key) of
                {error, {not_found, _}} ->
                    save_revision(ModelConfig, BucketOverride, ToSave#document{rev = {RNum, [Id]}});
                {error, not_found} ->
                    save_revision(ModelConfig, BucketOverride, ToSave#document{rev = {RNum, [Id]}});
                {error, Reason} ->
                    {error, Reason};
                {ok, #document{key = Key, rev = Rev} = Old} ->
                    {OldRNum, OldId} = rev_to_info(Rev),
                    case RNum of
                        OldRNum ->
                            case Id > OldId of
                                true ->
                                    case save_revision(ModelConfig, BucketOverride, ToSave) of
                                        {ok, _} ->
                                            % TODO - what happens if first save is ok and second fails
                                            % Delete in new task type that starts if first try fails
                                            delete_doc(ModelConfig, Old),
                                            {ok, Key};
                                        Other ->
                                            Other
                                    end;
                                false ->
                                    {ok, Key}
                            end;
                        Higher when Higher > OldRNum ->
                            NewIDs = check_revisions_list(OldId, IdsTail, OldRNum, Higher),
                            save_revision(ModelConfig, BucketOverride, ToSave#document{rev = {RNum, [Id | NewIDs]}});
                        _ ->
                            {ok, Key}
                    end
            end
        end).


%%--------------------------------------------------------------------
%% @doc
%% Apply given function with specified timeout.
%% @end
%%--------------------------------------------------------------------
-spec timeoutize_apply(fun(() -> Res :: term()), non_neg_integer()) -> Res :: term() | {error, timeoutize_apply_timeout}.
timeoutize_apply(Fun, Timeout) ->
    Ref = make_ref(),
    Srv = self(),
    Pid = spawn(fun() ->
        Srv ! {timeoutize_apply, Ref, catch Fun()}
    end),
    receive
        {timeoutize_apply, Ref, Res} -> Res
    after Timeout ->
        exit(Pid, normal),
        {error, timeoutize_apply_timeout}
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

%%--------------------------------------------------------------------
%% @doc
%% Entry point for Erlang Port (couchbase-sync-gateway) loop spawned with proc_lib.
%% Spawned couchbase-sync-gateway connects to given couchbase node and gives CouchDB-like
%% endpoint on localhost : ?GATEWAY_BASE_PORT + N .
%% @end
%%--------------------------------------------------------------------
-spec start_gateway(Parent :: pid(), N :: non_neg_integer(), Hostname :: binary(), Port :: non_neg_integer(), couchdb_bucket()) -> no_return().
start_gateway(Parent, N, Hostname, Port, Bucket) ->
    process_flag(trap_exit, true),
    GWPort = crypto:rand_uniform(?GATEWAY_BASE_PORT_MIN, ?GATEWAY_BASE_PORT_MAX),
    GWAdminPort = GWPort + 1000,
    ?info("Statring couchbase gateway #~p: localhost:~p => ~p:~p", [N, GWPort, Hostname, Port]),

    InitState = #{
        server => self(), port_fd => undefined, status => pre_init, id => {node(), N},
        gw_port => GWPort, gw_admin_port => GWAdminPort, db_hostname => Hostname, db_port => Port,
        start_time => erlang:system_time(milli_seconds), parent => Parent, last_ping_time => -1, bucket => Bucket,
        doc_batch_map => #{}, doc_batch_ts => erlang:system_time(milli_seconds)
    },
    Gateways = datastore_worker:state_get(db_gateways),
    case Gateways of
        _ when is_map(Gateways) ->
            NewGateways = maps:update(N, InitState, Gateways),
            datastore_worker:state_put(db_gateways, NewGateways);
        _ -> ok
    end,

    FlushFun = fun Flush() ->
        receive
            _ -> Flush()
        after 0 -> ok
        end end,

    FlushFun(),


    BinPath = code:priv_dir(cluster_worker) ++ "/sync_gateway",
    PortFD = erlang:open_port({spawn_executable, BinPath}, [binary, stderr_to_stdout, {line, 4 * 1024}, {args, [
        "-bucket", binary_to_list(Bucket),
        "-dbname", binary_to_list(Bucket),
        "-url", "http://" ++ binary_to_list(Hostname) ++ ":" ++ integer_to_list(Port),
        "-adminInterface", "127.0.0.1:" ++ integer_to_list(GWAdminPort),
        "-interface", ":" ++ integer_to_list(GWPort)
    ]}]),
    erlang:link(PortFD),

    State = InitState#{port_fd => PortFD, status => init},
    proc_lib:init_ack(Parent, State),

    BusyWaitInterval = 20,

    WaitForStateFun = fun WaitForState(Timeout) ->
        case datastore_worker:state_get(db_gateways) of
            undefined when Timeout > BusyWaitInterval ->
                timer:sleep(BusyWaitInterval),
                WaitForState(Timeout - BusyWaitInterval);
            undefined ->
                exit(state_not_initialized);
            Map when is_map(Map) ->
                ok
        end
    end,

    WaitForConnectionFun = fun WaitForConnection(Timeout) ->

        CheckDBFun = fun() ->
            couchbeam:server_info(catch couchbeam:server_connection("localhost", maps:get(gw_port, State)))
        end,
        try timeoutize_apply(CheckDBFun, timer:seconds(30)) of
            {error, econnrefused} when Timeout > BusyWaitInterval ->
                timer:sleep(BusyWaitInterval),
                case erlang:port_info(PortFD, os_pid) of
                    {os_pid, _} ->
                        WaitForConnection(Timeout - BusyWaitInterval);
                    _ ->
                        ok %% Other errors will be handled in gateway_loop/1
                end;
            _ ->
                ok %% Other errors will be handled in gateway_loop/1
        catch
            _:_ -> ok %% Other errors will be handled in gateway_loop/1
        end
    end,

    WaitForStateFun(?WAIT_FOR_STATE_TIMEOUT),
    WaitForConnectionFun(?WAIT_FOR_CONNECTION_TIMEOUT),

    gateway_loop(State#{status => running}).

%%--------------------------------------------------------------------
%% @doc
%% Loop for managing Erlang Port (couchbase-sync-gateway).
%% @end
%%--------------------------------------------------------------------
-spec gateway_loop(State :: #{atom() => term()}) -> no_return().
gateway_loop(#{port_fd := PortFD, id := {_, N} = ID, db_hostname := Hostname, db_port := Port, gw_port := GWPort,
    start_time := ST, parent := Parent, last_ping_time := LPT, bucket := Bucket, doc_batch_ts := DocBatchTS} = State) ->

    %% Update state
    Gateways = datastore_worker:state_get(db_gateways),
    NewGateways = maps:update(N, State, Gateways),
    case NewGateways of
        Gateways -> ok;
        _ ->
            datastore_worker:state_put(db_gateways, NewGateways)
    end,


    UpdatedState0 = case erlang:system_time(milli_seconds) - LPT > timer:seconds(1) of
        true ->
            try
                true = port_command(PortFD, <<"ping">>, [nosuspend]),
                CheckDBFun = fun() ->
                    couchbeam:server_info(catch couchbeam:server_connection("localhost", maps:get(gw_port, State)))
                end,
                {ok, _} = timeoutize_apply(CheckDBFun, timer:seconds(30)),
                State#{last_ping_time => erlang:system_time(milli_seconds)}
            catch
                _:{badmap, undefined} ->
                    State; %% State of the worker may not be initialised yet, so there is not way to check if connection is active
                _:Reason0 ->
                    self() ! {port_comm_error, Reason0},
                    State
            end;
        false ->
            State
    end,

    CT = erlang:system_time(milli_seconds),
    MinRestartTime = ST + ?DATASTORE_GATEWAY_SPAWN_TIMEOUT,

    FlushBatchDocs = fun(LState) ->
        BatchDocs = maps:get(doc_batch_map, LState),
        {Keys, Docs} = lists:unzip(maps:to_list(BatchDocs)),
        spawn(fun() ->
            Responses =
                try
                    save_docs(Bucket, Docs)
                catch
                    _:Reason2 ->
                        [{error, Reason2} || _ <- lists:seq(1, length(Docs))]
                end,

            utils:pmap(fun({{Pid, Ref}, Res}) ->
                Pid ! {Ref, Res}
            end, lists:zip(Keys, Responses))
        end),
        LState#{doc_batch_ts => erlang:system_time(milli_seconds), doc_batch_map => #{}}
    end,

    UpdatedState = case CT - DocBatchTS > timer:seconds(1) of
        true ->
            FlushBatchDocs(UpdatedState0);
        false ->
            UpdatedState0
    end,

    NewState =
        receive
            {PortFD, {data, {_, Data}}} ->
                case binary:matches(Data, <<"HTTP:">>) of
                    [] -> ?debug("[CouchBase Gateway ~p] ~s", [ID, Data]);
                    _ -> ok
                end,
                UpdatedState;
            {PortFD, closed} ->
                UpdatedState#{status => closed};
            {'EXIT', PortFD, Reason} ->
                ?error("CouchBase gateway's port ~p exited with reason: ~p", [UpdatedState, Reason]),
                UpdatedState#{status => failed};
            {'EXIT', Parent, Reason} ->
                ?info("Parent: ~p down due to: ~p", [Parent, Reason]),
                stop_gateway(UpdatedState),
                UpdatedState#{status => closed};
            {port_comm_error, Reason} ->
                ?error("[CouchBase Gateway ~p] Unable to communicate with port due to: ~p", [ID, Reason]),
                UpdatedState#{status => failed};
            restart when CT > MinRestartTime ->
                ?info("[CouchBase Gateway ~p] Restart request...", [ID]),
                UpdatedState#{status => restarting};
            restart ->
                UpdatedState;
            stop ->
                stop_gateway(UpdatedState),
                UpdatedState#{status => closed};
            {save_doc, {{_From, _Ref} = K, ModelWithDoc}} ->
                UpdatedState1 = case maps:size(maps:get(doc_batch_map, UpdatedState)) > 100 of
                    true ->
                         FlushBatchDocs(UpdatedState);
                    false ->
                        UpdatedState
                end,
                UpdatedState1#{doc_batch_map => maps:put(K, ModelWithDoc, maps:get(doc_batch_map, UpdatedState1))};
            flush_docs ->
                FlushBatchDocs(UpdatedState);
            Other ->
                ?warning("[CouchBase Gateway ~p] ~p", [ID, Other]),
                UpdatedState
        after timer:seconds(1) ->
            UpdatedState
        end,
    case NewState of
        #{status := running} ->
            gateway_loop(NewState);
        #{status := closed} ->
            ok;
        #{status := restarting} ->
            ?info("[CouchBase Gateway ~p] Restarting...", [ID]),
            stop_gateway(NewState),
            start_gateway(self(), N, Hostname, Port, Bucket);
        #{status := failed} ->
            ?info("[CouchBase Gateway ~p] Restarting due to failure...", [ID]),
            stop_gateway(NewState),
            start_gateway(self(), N, Hostname, Port, Bucket)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Closes port and forces exit of couchbase-sync-gateway executable.
%% @end
%%--------------------------------------------------------------------
-spec stop_gateway(State :: #{atom() => term()}) -> ok | {error, timeout}.
stop_gateway(#{port_fd := PortFD, id := {_, N}, db_hostname := Hostname, db_port := Port, gw_port := GWPort}) ->
    ?info("Stopping couchbase gateway #~p: localhost:~p => ~p:~p", [N, GWPort, Hostname, Port]),
    timeoutize_apply(fun() ->
        ForcePortCloseCmd = case erlang:port_info(PortFD, os_pid) of
            {os_pid, OsPid} -> lists:flatten(io_lib:format("kill -9 ~p", [OsPid]));
            _ -> undefined
        end,
        catch port_close(PortFD),
        case ForcePortCloseCmd of
            undefined -> ok;
            _ ->
                os:cmd(ForcePortCloseCmd)
        end,
        ok
    end, timer:seconds(10)).


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
    {ok, {_, Db}} = get_db(Bucket),
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
            Seq = seq(Change),
            Deleted = deleted(Change),

            RawDocOnceAgain = jiffy:decode(jsx:encode(RawDoc)),
            Document = process_raw_doc(Bucket, RawDocOnceAgain),

            Callback(Seq, Document#document{deleted = Deleted}, model(Document)),
            State#state{last_seq = max(normalize_seq(Seq), LastSeq)}
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
save_revision(#model_config{bucket = Bucket} = ModelConfig, BucketOverride,
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
                {ok,{multipart,M}} ->
                    case collect_mp(couchbeam:stream_doc(M), []) of
                        [{doc,{Proplist}}] ->
                            case verify_ans(Proplist) of
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
                            end;
                        MultipartError ->
                            ?error("Multipart get error: ~p", [MultipartError]),
                            {error, db_internal_error}
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
    NBuffer = << Buffer/binary, Chunk/binary >>,
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
-spec add_view(ModelName :: model_behaviour:model_type(), binary(), binary()) -> ok.
add_view(ModelName, Id, ViewFunction) ->
    DesignId = <<"_design/", Id/binary>>,

    Doc = jiffy:decode(jiffy:encode(#{
        <<"_id">> => DesignId,
        <<"views">> => maps:from_list(
            [{Id, #{<<"map">> => ViewFunction}}]
        )
    })),
    {ok, SaveAns} = db_run(select_bucket(ModelName:model_init()), couchbeam, save_doc, [Doc], 5),
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
        case db_run(select_bucket(ModelName:model_init()), couchbeam_view, stream, [{Id, Id}, Options], 3) of
            {ok, Ref} ->
                Loop = fun LoopFun() ->
                    receive
                        {Ref, done} ->
                            Host ! {self(), {stream_ended, done}};
                        {Ref, {row, {Proplist}}} ->
                            try
                                {_, DocId} = lists:keyfind(<<"id">>, 1, Proplist),
                                {_, DocKey} = from_driver_key(DocId),
                                Host ! {self(), {stream_data, DocKey}}
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
%%
%% @end
%%--------------------------------------------------------------------
-spec query_view(ModelName :: model_behaviour:model_type(), binary(), list()) -> {ok, [binary()]}.
query_view(ModelName, Id, Options) ->
    case db_run(select_bucket(ModelName:model_init()), couchbeam_view, fetch, [{Id, Id}, Options], 3) of
        {ok, List} ->
            case verify_ans(List) of
                true ->
                    Ids = lists:map(fun({[{<<"id">>, DbDocId} | _]}) ->
                        {_, DocUuid} = from_driver_key(DbDocId),
                        DocUuid
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
%% @TODO TODO
%%--------------------------------------------------------------------
-spec delete_view(ModelName :: model_behaviour:model_type(), binary()) -> ok.
delete_view(_ModelName, _Id) ->
%%    DesignId = <<"_design/", Id/binary>>,
    % TODO - verify ans
%%    {ok, _} = couchdb_datastore_driver:db_run(select_bucket(ModelName:model_init()), couchbeam, delete_doc, [{[<<"_id">>, DesignId]}], 5),
    ok.


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
    ?error("Cauch db error: ~p", [Ans]),
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
