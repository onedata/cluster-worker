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

%% Encoded record name field
-define(RECORD_MARKER, "RECORD::").

-define(LINKS_KEY_SUFFIX, "$$").

%% Maximum size of document's value.
-define(MAX_VALUE_SIZE, 512 * 1024).

%% Base port for gateway endpoints
-define(GATEWAY_BASE_PORT_MIN, 12000).
-define(GATEWAY_BASE_PORT_MAX, 12999).

%% store_driver_behaviour callbacks
-export([init_bucket/3, healthcheck/1, init_driver/1]).
-export([save/2, create/2, update/3, create_or_update/3, exists/2, get/2, list/3, delete/3]).
-export([add_links/3, create_link/3, delete_links/3, fetch_link/3, foreach_link/4]).

-export([start_gateway/4, force_save/2, db_run/4, normalize_seq/1]).

-export([changes_start_link/3, get_with_revs/2]).
-export([init/1, handle_call/3, handle_info/2, handle_change/2, handle_cast/2, terminate/2]).
-export([save_link_doc/2, get_link_doc/2, delete_link_doc/2]).
-export([to_binary/1]).

%%%===================================================================
%%% store_driver_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_driver/1.
%% @end
%%--------------------------------------------------------------------
-spec init_driver(worker_host:plugin_state()) -> {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init_driver(#{db_nodes := DBNodes0} = State) ->
    DBNodes = [lists:nth(crypto:rand_uniform(1, length(DBNodes0) + 1), DBNodes0)],
    Gateways = lists:map(
        fun({N, {Hostname, _Port}}) ->
            GWState = proc_lib:start_link(?MODULE, start_gateway, [self(), N, Hostname, 8091], ?DATASTORE_GATEWAY_SPAWN_TIMEOUT),
            {N, GWState}
        end, lists:zip(lists:seq(1, length(DBNodes)), DBNodes)),
    {ok, State#{db_gateways => maps:from_list(Gateways)}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/3.
%% @end
%%--------------------------------------------------------------------
-spec init_bucket(Bucket :: datastore:bucket(), Models :: [model_behaviour:model_config()],
    NodeToSync :: node()) -> ok.
init_bucket(Bucket, Models, _NodeToSync) ->
    BinBucket = atom_to_binary(Bucket, utf8),
    DesignId = <<"_design/", BinBucket/binary>>,

    Doc = to_json_term(#{
        <<"_id">> => DesignId,
        <<"views">> => maps:from_list(lists:map(
            fun(#model_config{name = ModelName}) ->
                BinModelName = atom_to_binary(ModelName, utf8),
                {BinModelName, #{<<"map">> => <<"function(doc) { if(doc['", ?RECORD_MARKER, "'] == \"", BinModelName/binary, "\") emit(doc['", ?RECORD_MARKER, "'], doc); }">>}}
            end, Models))
    }),
    {ok, _} = db_run(couchbeam, save_doc, [Doc], 5),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/2.
%% @end
%%--------------------------------------------------------------------
-spec save(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(#model_config{name = ModelName} = ModelConfig, #document{rev = undefined, key = Key, value = Value} = Doc) ->
    critical_section:run([ModelName, Key],
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
save_doc(ModelConfig, #document{rev = undefined} = Doc) ->
    create(ModelConfig, Doc);
save_doc(#model_config{bucket = Bucket} = ModelConfig, #document{key = Key, rev = Rev, value = Value}) ->
    ok = assert_value_size(Value, ModelConfig, Key),

    {Props} = to_json_term(Value),
    Doc = {[{<<"_rev">>, Rev}, {<<"_id">>, to_driver_key(Bucket, Key)} | Props]},
    case db_run(couchbeam, save_doc, [Doc, ?DEFAULT_DB_REQUEST_TIMEOUT_OPT], 3) of
        {ok, {_}} ->
            {ok, Key};
        {error, conflict} ->
            {error, already_exists};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback update/3.
%% @end
%%--------------------------------------------------------------------
-spec update(model_behaviour:model_config(), datastore:ext_key(),
    Diff :: datastore:document_diff()) -> {ok, datastore:ext_key()} | datastore:update_error().
update(#model_config{bucket = _Bucket, name = ModelName} = ModelConfig, Key, Diff) when is_function(Diff) ->
    critical_section:run([ModelName, Key],
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
update(#model_config{bucket = _Bucket, name = ModelName} = ModelConfig, Key, Diff) when is_map(Diff) ->
    critical_section:run([ModelName, Key],
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
create(#model_config{bucket = Bucket} = ModelConfig, #document{key = Key, value = Value}) ->
    ok = assert_value_size(Value, ModelConfig, Key),

    {Props} = to_json_term(Value),
    Doc = {[{<<"_id">>, to_driver_key(Bucket, Key)} | Props]},
    case db_run(couchbeam, save_doc, [Doc, ?DEFAULT_DB_REQUEST_TIMEOUT_OPT], 3) of
        {ok, {_}} ->
            {ok, Key};
        {error, conflict} ->
            {error, already_exists};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_or_update/2.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(model_behaviour:model_config(), datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create_or_update(#model_config{name = ModelName} = ModelConfig, #document{key = Key} = NewDoc, Diff)
    when is_function(Diff) ->
    critical_section:run([ModelName, Key],
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
    critical_section:run([ModelName, Key],
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
get(#model_config{bucket = Bucket, name = ModelName} = _ModelConfig, Key) ->
    case db_run(couchbeam, open_doc, [to_driver_key(Bucket, Key)], 3) of
        {ok, {Proplist} = _Doc} ->
            {_, Rev} = lists:keyfind(<<"_rev">>, 1, Proplist),
            Proplist1 = [KV || {<<"_", _/binary>>, _} = KV <- Proplist],
            Proplist2 = Proplist -- Proplist1,
            {ok, #document{key = Key, value = from_json_term({Proplist2}), rev = Rev}};
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
%% {@link store_driver_behaviour} callback list/3.
%% @end
%%--------------------------------------------------------------------
-spec list(model_behaviour:model_config(),
    Fun :: datastore:list_fun(), AccIn :: term()) -> no_return().
list(#model_config{bucket = Bucket, name = ModelName} = _ModelConfig, Fun, AccIn) ->
    BinModelName = atom_to_binary(ModelName, utf8),
    _BinBucket = atom_to_binary(Bucket, utf8),
    case db_run(couchbeam_view, fetch, [all_docs, [include_docs, {start_key, BinModelName}, {end_key, BinModelName}]], 3) of
        {ok, Rows} ->
            Ret =
                lists:foldl(
                    fun({Row}, Acc) ->
                        try
                            {Value} = proplists:get_value(<<"doc">>, Row, {[]}),
                            {_, KeyBin} = lists:keyfind(<<"id">>, 1, Row),
                            Value1 = [KV || {<<"_", _/binary>>, _} = KV <- Value],
                            Value2 = Value -- Value1,
                            {_, Key} = from_driver_key(KeyBin),
                            Doc = #document{key = Key, value = from_json_term({Value2})},
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
                                RetAcc; %% Provided function requested end of stream, exiting loop
                            _:_ ->
                                Acc %% Invalid entry, skipping
                        end
                    end, AccIn, Rows),
            {ok, Ret};
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
delete(#model_config{bucket = Bucket, name = ModelName} = ModelConfig, Key, Pred) ->
    critical_section:run([ModelName, Key],
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
                            delete_doc(Bucket, Doc)
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
delete_link_doc(#model_config{bucket = Bucket} = _ModelConfig, Doc) ->
    delete_doc(Bucket, Doc).

%%--------------------------------------------------------------------
%% @doc
%% Deletes document not using transactions.
%% @end
%%--------------------------------------------------------------------
-spec delete_doc(datastore:bucket(), datastore:ext_key()) ->
    ok | datastore:generic_error().
delete_doc(Bucket, #document{key = Key, value = Value, rev = Rev}) ->
    {Props} = to_json_term(Value),
    Doc = {[{<<"_id">>, to_driver_key(Bucket, Key)}, {<<"_rev">>, Rev} | Props]},
    case db_run(couchbeam, delete_doc, [Doc, ?DEFAULT_DB_REQUEST_TIMEOUT_OPT], 3) of
        ok ->
            ok;
        {ok, _} ->
            ok;
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
add_links(#model_config{name = ModelName, bucket = Bucket} = ModelConfig, Key, Links) when is_list(Links) ->
    critical_section:run([ModelName, Bucket, Key],
        fun() ->
            links_utils:save_links_maps(?MODULE, ModelConfig, Key, Links)
        end
    ).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_link/3.
%% @end
%%--------------------------------------------------------------------
-spec create_link(model_behaviour:model_config(), datastore:ext_key(), datastore:normalized_link_spec()) ->
    ok | datastore:create_error().
create_link(#model_config{name = ModelName, bucket = Bucket} = ModelConfig, Key, Link) ->
    critical_section:run([ModelName, Bucket, Key],
        fun() ->
            links_utils:create_link_in_map(?MODULE, ModelConfig, Key, Link)
        end
    ).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete_links/3.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:link_name()] | all) ->
    ok | datastore:generic_error().
delete_links(#model_config{name = ModelName, bucket = Bucket} = ModelConfig, Key, all) ->
    critical_section:run([ModelName, Bucket, Key],
        fun() ->
            links_utils:delete_links(?MODULE, ModelConfig, Key)
        end
    );
delete_links(#model_config{name = ModelName, bucket = Bucket} = ModelConfig, Key, Links) ->
    critical_section:run([ModelName, Bucket, Key],
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
        case get_server() of
            {ok, _} -> ok;
            {error, Reason} ->
                {error, Reason}
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
%% Decodes given base64 binary to erlang term (reverses term_to_base64/1).
%% @end
%%--------------------------------------------------------------------
-spec base64_to_term(binary()) -> term().
base64_to_term(<<?OBJ_PREFIX, Base/binary>>) ->
    binary_to_term(base64:decode(Base)).

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
to_binary(Term) ->
    term_to_base64(Term).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Translates given database "register" object to erlang term (reverses to_binary/1).
%% @end
%%--------------------------------------------------------------------
-spec from_binary(binary()) -> term().
from_binary(<<?OBJ_PREFIX, _/binary>> = Bin) ->
    base64_to_term(Bin);
from_binary(<<?ATOM_PREFIX, Atom/binary>>) ->
    binary_to_atom(Atom, utf8);
from_binary(Bin) ->
    Bin.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Translates given internal model's record format into couchbeam document.
%% @end
%%--------------------------------------------------------------------
-spec to_json_term(term()) -> term().
to_json_term(Term) when is_integer(Term) ->
    Term;
to_json_term(Term) when is_binary(Term) ->
    Term;
to_json_term(Term) when is_boolean(Term) ->
    Term;
to_json_term(Term) when is_float(Term) ->
    Term;
to_json_term(Term) when is_list(Term) ->
    [to_json_term(Elem) || Elem <- Term];
to_json_term(Term) when is_atom(Term) ->
    to_binary(Term);
to_json_term(Term) when is_tuple(Term) ->
    ModelName = element(1, Term),
    try ModelName:model_init() of
        #model_config{fields = Fields} ->
            [_ | Values1] = tuple_to_list(Term),
            Map = maps:from_list(lists:zip(Fields, Values1)),
            to_json_term(Map#{<<?RECORD_MARKER>> => atom_to_binary(ModelName, utf8)})
    catch
        _:_ -> %% encode as tuple
            Values = tuple_to_list(Term),
            Keys = lists:seq(1, length(Values)),
            KeyValue = lists:zip(Keys, Values),
            Map = maps:from_list(KeyValue),
            to_json_term(Map#{<<?RECORD_MARKER>> => atom_to_binary(undefined, utf8)})
    end
;
to_json_term(Term) when is_map(Term) ->
    Proplist0 = maps:to_list(Term),
    Proplist1 = [{to_binary(Key), to_json_term(Value)} || {Key, Value} <- Proplist0],
    {Proplist1};
to_json_term(Term) ->
    to_binary(Term).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Translates given couchbeam document into internal model's record format.
%% @end
%%--------------------------------------------------------------------
-spec from_json_term(term()) -> term().
from_json_term(Term) when is_integer(Term) ->
    Term;
from_json_term(Term) when is_boolean(Term) ->
    Term;
from_json_term(Term) when is_float(Term) ->
    Term;
from_json_term(Term) when is_list(Term) ->
    [from_json_term(Elem) || Elem <- Term];
from_json_term({Term}) when is_list(Term) ->
    case lists:keyfind(<<?RECORD_MARKER>>, 1, Term) of
        false ->
            Proplist2 = [{from_binary(Key), from_json_term(Value)} || {Key, Value} <- Term],
            maps:from_list(Proplist2);
        {_, <<"undefined">>} ->
            Proplist0 = [{from_binary(Key), from_json_term(Value)} || {Key, Value} <- Term, Key =/= <<?RECORD_MARKER>>],
            Proplist1 = lists:sort(Proplist0),
            {_, Values} = lists:unzip(Proplist1),
            list_to_tuple(Values);
        {_, RecordType} ->
            Proplist0 = [{from_binary(Key), from_json_term(Value)} || {Key, Value} <- Term, Key =/= <<?RECORD_MARKER>>],
            ModelName = binary_to_atom(RecordType, utf8),
            #model_config{fields = Fields} = ModelName:model_init(),
            Values = [proplists:get_value(Key, Proplist0, undefined) || Key <- Fields],
            list_to_tuple([binary_to_atom(RecordType, utf8) | Values])
    end;
from_json_term(Term) when is_binary(Term) ->
    from_binary(Term).

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
-spec get_db() -> {ok, {pid, term()}} | {error, term()}.
get_db() ->
    case get_server() of
        {error, Reason} ->
            {error, Reason};
        {ok, {ServerLoop, Server}} ->
            try
                {ok, DB} = couchbeam:open_db(Server, <<"default">>),
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
-spec get_server() -> {ok, {pid, term()}} | {error, term()}.
get_server() ->
    get_server(datastore_worker:state_get(db_gateways)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns server handle used by couchbeam library to connect to couchdb-based DB.
%% @end
%%--------------------------------------------------------------------
-spec get_server(State :: worker_host:plugin_state()) -> {ok, {pid, term()}} | {error, term()}.
get_server(DBGateways) ->
    Gateways = maps:values(DBGateways),
    ActiveGateways = [GW || #{status := running} = GW <- Gateways],

    case ActiveGateways of
        [] ->
            ?error("Unable to select CouchBase Gateway: no active gateway among: ~p", [Gateways]),
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
    {ok, {ServerPid, DB}} = get_db(),
    case apply(Mod, Fun, [DB | Args]) of
        {error, econnrefused} when Retry > 0 ->
            ?info("Unable to connect to ~p", [DB]),
            ServerPid ! restart,
            timer:sleep(crypto:rand_uniform(20, 50)),
            db_run(Mod, Fun, Args, Retry - 1);
        Other -> Other
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
force_save(#model_config{bucket = Bucket} = ModelConfig, #document{key = Key, rev = {Start, Ids} = Revs, value = Value}) ->
    ok = assert_value_size(Value, ModelConfig, Key),

    {Props} = to_json_term(Value),
    Doc = {[{<<"_revisions">>, {[{<<"ids">>, Ids}, {<<"start">>, Start}]}}, {<<"_rev">>, rev_info_to_rev(Revs)}, {<<"_id">>, to_driver_key(Bucket, Key)} | Props]},
    case db_run(couchbeam, save_doc, [Doc, [{<<"new_edits">>, <<"false">>}] ++ ?DEFAULT_DB_REQUEST_TIMEOUT_OPT], 3) of
        {ok, {_}} ->
            {ok, Key};
        {error, conflict} ->
            {error, already_exists};
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Entry point for Erlang Port (couchbase-sync-gateway) loop spawned with proc_lib.
%% Spawned couchbase-sync-gateway connects to given couchbase node and gives CouchDB-like
%% endpoint on localhost : ?GATEWAY_BASE_PORT + N .
%% @end
%%--------------------------------------------------------------------
-spec start_gateway(Parent :: pid(), N :: non_neg_integer(), Hostname :: binary(), Port :: non_neg_integer()) -> no_return().
start_gateway(Parent, N, Hostname, Port) ->
    process_flag(trap_exit, true),
    GWPort = crypto:rand_uniform(?GATEWAY_BASE_PORT_MIN, ?GATEWAY_BASE_PORT_MAX),
    GWAdminPort = GWPort + 1000,
    ?info("Statring couchbase gateway #~p: localhost:~p => ~p:~p", [N, GWPort, Hostname, Port]),

    BinPath = code:priv_dir(cluster_worker) ++ "/sync_gateway",
    PortFD = erlang:open_port({spawn_executable, BinPath}, [binary, stderr_to_stdout, {line, 4 * 1024}, {args, [
        "-bucket", "default",
        "-url", "http://" ++ binary_to_list(Hostname) ++ ":" ++ integer_to_list(Port),
        "-adminInterface", "127.0.0.1:" ++ integer_to_list(GWAdminPort),
        "-interface", ":" ++ integer_to_list(GWPort)
    ]}]),
    erlang:link(PortFD),

    State = #{
        server => self(), port_fd => PortFD, status => init, id => {node(), N},
        gw_port => GWPort, gw_admin_port => GWAdminPort, db_hostname => Hostname, db_port => Port,
        start_time => erlang:system_time(milli_seconds), parent => Parent, last_ping_time => -1
    },
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
        try couchbeam:server_info(catch couchbeam:server_connection("localhost", maps:get(gw_port, State))) of
            {error, econnrefused} when Timeout > BusyWaitInterval ->
                timer:sleep(BusyWaitInterval),
                WaitForConnection(Timeout - BusyWaitInterval);
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
    start_time := ST, parent := Parent, last_ping_time := LPT} = State) ->

    %% Update state
    Gateways = datastore_worker:state_get(db_gateways),
    NewGateways = maps:update(N, State, Gateways),
    case NewGateways of
        Gateways -> ok;
        _ ->
            datastore_worker:state_put(db_gateways, NewGateways)
    end,


    UpdatedState = case erlang:system_time(milli_seconds) - LPT > timer:seconds(1) of
        true ->
            try
                true = port_command(PortFD, <<"ping">>, [nosuspend]),
                {ok, _} = couchbeam:server_info(catch couchbeam:server_connection("localhost", GWPort)),
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

    NewState =
        receive
            {PortFD, {data, {_, Data}}} ->
                case binary:matches(Data, <<"HTTP:">>) of
                    [] -> ?info("[CouchBase Gateway ~p] ~s", [ID, Data]);
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
                stop_gateway(PortFD),
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
                stop_gateway(PortFD),
                UpdatedState#{status => closed};
            Other ->
                ?warning("[CouchBase Gateway ~p] ~p", [ID, Other]),
                UpdatedState
        after timer:seconds(2) ->
            UpdatedState
        end,
    case NewState of
        #{status := running} ->
            gateway_loop(NewState);
        #{status := closed} ->
            ok;
        #{status := restarting} ->
            stop_gateway(PortFD),
            start_gateway(self(), N, Hostname, Port);
        #{status := failed} ->
            stop_gateway(PortFD),
            start_gateway(self(), N, Hostname, Port)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Closes port and forces exit of couchbase-sync-gateway executable.
%% @end
%%--------------------------------------------------------------------
-spec stop_gateway(PortFD :: port()) -> ok.
stop_gateway(PortFD) ->
    ForcePortCloseCmd = case erlang:port_info(PortFD, os_pid) of
        {os_pid, OsPid} -> lists:flatten(io_lib:format("kill -9 ~p", [OsPid]));
        _ -> ""
    end,
        catch port_close(PortFD),
    os:cmd(ForcePortCloseCmd),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%                                    CHANGES                                         %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(state, {
    callback,
    until,
    last_seq = 0
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
get_with_revs(#model_config{bucket = Bucket, name = ModelName} = _ModelConfig, Key) ->
    Args = [to_driver_key(Bucket, Key), [{<<"revs">>, <<"true">>}]],
    case db_run(couchbeam, open_doc, Args, 3) of
        {ok, {Proplist} = _Doc} ->
            Proplist1 = [KV || {<<"_", _/binary>>, _} = KV <- Proplist],
            Proplist2 = Proplist -- Proplist1,

            {_, {RevsRaw}} = lists:keyfind(<<"_revisions">>, 1, Proplist),
            {_, Revs} = lists:keyfind(<<"ids">>, 1, RevsRaw),
            {_, Start} = lists:keyfind(<<"start">>, 1, RevsRaw),

            {ok, #document{
                key = Key,
                value = from_json_term({Proplist2}),
                rev = {Start, Revs}}
            };
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
    {ok, {_, Db}} = get_db(),
    Opts = [{<<"include_docs">>, <<"true">>}, {since, Since}, {<<"revs_info">>, <<"true">>}],
    gen_changes:start_link(?MODULE, Db, Opts, [Callback, Until]).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% init/1 callback for gen_changes server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: [term()]) -> {ok, gen_changes_state()}.
init([Callback, Until]) ->
    ?debug("Starting changes stream until ~p", [Until]),
    {ok, #state{callback = Callback, until = Until}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% handle_change/2 callback for gen_changes server.
%% @end
%%--------------------------------------------------------------------
-spec handle_change(term(), gen_changes_state()) -> {noreply, gen_changes_state()} | {stop, normal, gen_changes_state()}.
handle_change({done, _LastSeq}, State) ->
    {noreply, State};


handle_change(Change, #state{callback = Callback, until = Until, last_seq = LastSeq} = State) when Until > LastSeq; Until =:= infinity ->
    NewChanges =
        try
            RawDoc = doc(Change),
            Seq = seq(Change),
            Deleted = deleted(Change),

            RawDocOnceAgain = jiffy:decode(jsx:encode(RawDoc)),
            Document = process_raw_doc(RawDocOnceAgain),

            Callback(Seq, Document#document{deleted = Deleted}, model(Document)),
            State#state{last_seq = max(normalize_seq(Seq), LastSeq)}
        catch
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
%% Converts raw document given by CouchDB to datastore's #document.
%% @end
%%--------------------------------------------------------------------
-spec process_raw_doc(term()) -> datastore:document().
process_raw_doc({RawDoc}) ->
    {_, Rev} = lists:keyfind(<<"_rev">>, 1, RawDoc),
    {_, RawKey} = lists:keyfind(<<"_id">>, 1, RawDoc),
    {_, Key} = from_driver_key(RawKey),
    RawDoc1 = [KV || {<<"_", _/binary>>, _} = KV <- RawDoc],
    RawDoc2 = RawDoc -- RawDoc1,
    {ok, {RawRichDoc}} = db_run(couchbeam, open_doc, [RawKey, [{<<"revs">>, <<"true">>}, {<<"rev">>, Rev}]], 3),
    {_, {RevsRaw}} = lists:keyfind(<<"_revisions">>, 1, RawRichDoc),
    {_, Revs} = lists:keyfind(<<"ids">>, 1, RevsRaw),
    {_, Start} = lists:keyfind(<<"start">>, 1, RevsRaw),

    #document{key = Key, rev = {Start, Revs}, value = from_json_term({RawDoc2})}.


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
