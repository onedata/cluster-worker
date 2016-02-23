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

%% Encoded object prefix
-define(OBJ_PREFIX, "OBJ::").

%% Encoded atom prefix
-define(ATOM_PREFIX, "ATOM::").

-define(LINKS_KEY_SUFFIX, "$$").

%% Maximum size of document's value.
-define(MAX_VALUE_SIZE, 512 * 1024).

%% Base port for gateway endpoints
-define(GATEWAY_BASE_PORT_MIN, 12000).
-define(GATEWAY_BASE_PORT_MAX, 12999).

%% store_driver_behaviour callbacks
-export([init_bucket/3, healthcheck/1, init_driver/1]).
-export([save/2, create/2, update/3, create_or_update/3, exists/2, get/2, list/3, delete/3]).
-export([add_links/3, delete_links/3, fetch_link/3, foreach_link/4]).
-export([links_doc_key/1, links_key_to_doc_key/1]).

-export([start_gateway/4, force_save/2, db_run/4]).

-export([changes_start_link/3]).
-export([init/1, handle_call/3, handle_info/2, handle_change/2, handle_cast/2, terminate/2]).

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
            GWState = proc_lib:start_link(?MODULE, start_gateway, [self(), N, Hostname, 8091], timer:seconds(5)),
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
init_bucket(_Bucket, _Models, _NodeToSync) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/2.
%% @end
%%--------------------------------------------------------------------
-spec save(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(#model_config{name = ModelName} = ModelConfig, #document{rev = undefined, key = Key} = Doc) ->
    datastore:run_synchronized(ModelName, to_binary({?MODULE, Key}),
        fun() ->
            case get(ModelConfig, Key) of
                {error, {not_found, _}} ->
                    create(ModelConfig, Doc);
                {error, Reason} ->
                    {error, Reason};
                {ok, #document{rev = undefined}} ->
                    create(ModelConfig, Doc);
                {ok, #document{rev = Rev}} ->
                    save(ModelConfig, Doc#document{rev = Rev})
            end
        end);
save(#model_config{bucket = Bucket} = _ModelConfig, #document{key = Key, rev = Rev, value = Value}) ->
    ok = assert_value_size(Value),

    {Props} = to_json_term(Value),
    Doc = {[{<<"_rev">>, Rev}, {<<"_id">>, to_driver_key(Bucket, Key)} | Props]},
    case db_run(couchbeam, save_doc, [Doc], 3) of
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
update(#model_config{bucket = _Bucket} = _ModelConfig, _Key, Diff) when is_function(Diff) ->
    erlang:error(not_implemented);
update(#model_config{bucket = _Bucket, name = ModelName} = ModelConfig, Key, Diff) when is_map(Diff) ->
    datastore:run_synchronized(ModelName, to_binary({?MODULE, Key}),
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
create(#model_config{bucket = Bucket} = _ModelConfig, #document{key = Key, value = Value}) ->
    ok = assert_value_size(Value),

    {Props} = to_json_term(Value),
    Doc = {[{<<"_id">>, to_driver_key(Bucket, Key)} | Props]},
    case db_run(couchbeam, save_doc, [Doc], 3) of
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
    %%     {ok, datastore:ext_key()} | datastore:create_error().
    no_return().
create_or_update(#model_config{} = _ModelConfig, #document{key = _Key, value = _Value}, _Diff) ->
    erlang:error(not_implemented).

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
%% {@link store_driver_behaviour} callback list/3.
%% @end
%%--------------------------------------------------------------------
-spec list(model_behaviour:model_config(),
    Fun :: datastore:list_fun(), AccIn :: term()) -> no_return().
list(#model_config{} = _ModelConfig, _Fun, _AccIn) ->
    % Add support for multivelel list in datastore (simmilar to foreach_link) during implementation
    error(not_supported).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete/2.
%% @end
%%--------------------------------------------------------------------
-spec delete(model_behaviour:model_config(), datastore:ext_key(), datastore:delete_predicate()) ->
    ok | datastore:generic_error().
delete(#model_config{bucket = Bucket, name = ModelName} = ModelConfig, Key, Pred) ->
    datastore:run_synchronized(ModelName, to_binary({?MODULE, Key}),
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
                        {ok, #document{value = Value, rev = Rev}} ->
                            {Props} = to_json_term(Value),
                            Doc = {[{<<"_id">>, to_driver_key(Bucket, Key)}, {<<"_rev">>, Rev} | Props]},
                            case db_run(couchbeam, delete_doc, [Doc], 3) of
                                ok ->
                                    ok;
                                {ok, _} ->
                                    ok;
                                {error, key_enoent} ->
                                    ok;
                                {error, Reason} ->
                                    {error, Reason}
                            end
                    end;
                false ->
                    ok
            end
        end).

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
    datastore:run_synchronized(ModelName, to_binary({?MODULE, Bucket, Key}),
        fun() ->
            case get(ModelConfig, links_doc_key(Key)) of
                {ok, #document{value = #links{link_map = LinkMap}}} ->
                    add_links4(ModelConfig, Key, Links, LinkMap);
                {error, {not_found, _}} ->
                    add_links4(ModelConfig, Key, Links, #{});
                {error, Reason} ->
                    {error, Reason}
            end
        end
    ).

-spec add_links4(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()], InternalCtx :: term()) ->
    ok | datastore:generic_error().
add_links4(#model_config{bucket = _Bucket, name = ModelName} = ModelConfig, Key, [], Ctx) ->
    case save(ModelConfig, #document{key = links_doc_key(Key), value = #links{key = Key, model = ModelName, link_map = Ctx}}) of
        {ok, _} -> ok;
        {error, Reason} ->
            {error, Reason}
    end;
add_links4(#model_config{bucket = _Bucket} = ModelConfig, Key, [{LinkName, LinkTarget} | R], Ctx) ->
    add_links4(ModelConfig, Key, R, maps:put(LinkName, LinkTarget, Ctx)).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete_links/3.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:link_name()] | all) ->
    ok | datastore:generic_error().
delete_links(#model_config{bucket = _Bucket} = ModelConfig, Key, all) ->
    delete(ModelConfig, links_doc_key(Key), ?PRED_ALWAYS);
delete_links(#model_config{name = ModelName, bucket = Bucket} = ModelConfig, Key, Links) ->
    datastore:run_synchronized(ModelName, to_binary({?MODULE, Bucket, Key}),
        fun() ->
            case get(ModelConfig, links_doc_key(Key)) of
                {ok, #document{value = #links{link_map = LinkMap}}} ->
                    delete_links4(ModelConfig, Key, Links, LinkMap);
                {error, {not_found, _}} ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
        end
    ).

-spec delete_links4(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()] | all, InternalCtx :: term()) ->
    ok | datastore:generic_error().
delete_links4(#model_config{bucket = _Bucket, name = ModelName} = ModelConfig, Key, [], Ctx) ->
    case save(ModelConfig, #document{key = links_doc_key(Key), value = #links{key = Key, model = ModelName, link_map = Ctx}}) of
        {ok, _} -> ok;
        {error, Reason} ->
            {error, Reason}
    end;
delete_links4(#model_config{} = ModelConfig, Key, [Link | R], Ctx) ->
    delete_links4(ModelConfig, Key, R, maps:remove(Link, Ctx)).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback fetch_links/3.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(model_behaviour:model_config(), datastore:ext_key(), datastore:link_name()) ->
    {ok, datastore:link_target()} | datastore:link_error().
fetch_link(#model_config{bucket = _Bucket} = ModelConfig, Key, LinkName) ->
    case get(ModelConfig, links_doc_key(Key)) of
        {ok, #document{value = #links{link_map = LinkMap}}} ->
            case maps:get(LinkName, LinkMap, undefined) of
                undefined ->
                    {error, link_not_found};
                LinkTarget ->
                    {ok, LinkTarget}
            end;
        {error, {not_found, _}} ->
            {error, link_not_found};
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback foreach_link/4.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(model_behaviour:model_config(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error().
foreach_link(#model_config{bucket = _Bucket} = ModelConfig, Key, Fun, AccIn) ->
    case get(ModelConfig, links_doc_key(Key)) of
        {ok, #document{value = #links{link_map = LinkMap}}} ->
            {ok, maps:fold(Fun, AccIn, LinkMap)};
        {error, {not_found, _}} ->
            {ok, AccIn};
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback healthcheck/1.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck(WorkerState :: term()) -> ok | {error, Reason :: term()}.
healthcheck(_) ->
    try
        case get_db() of
            {ok, _} -> ok;
            {error, Reason} ->
                {error, Reason}
        end
    catch
        _:R -> {error, R}
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
    Elems = tuple_to_list(Term),
    Proplist0 = [{<<"RECORD::">>, <<"unknown">>} | lists:zip(lists:seq(1, length(Elems)), Elems)],
    Proplist1 = [{to_binary(Key), to_json_term(Value)} || {Key, Value} <- Proplist0],
    {Proplist1};
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
    case lists:keyfind(<<"RECORD::">>, 1, Term) of
        false ->
            Proplist2 = [{from_binary(Key), from_json_term(Value)} || {Key, Value} <- Term],
            maps:from_list(Proplist2);
        {_, _RecordType} ->
            Proplist0 = [{from_binary(Key), from_json_term(Value)} || {Key, Value} <- Term, Key =/= <<"RECORD::">>],
            Proplist1 = lists:sort(Proplist0),
            {_, Values} = lists:unzip(Proplist1),
            list_to_tuple(Values)
    end;
from_json_term(Term) when is_binary(Term) ->
    from_binary(Term).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns key for document holding links for given document.
%% @end
%%--------------------------------------------------------------------
-spec links_doc_key(Key :: datastore:key()) -> BinKey :: binary().
links_doc_key(Key) ->
    base64:encode(term_to_binary({links, Key})).

%%--------------------------------------------------------------------
%% @doc
%% Returns key of document that owns links saved as document with given key.
%% Reverses links_doc_key/1.
%% @end
%%--------------------------------------------------------------------
-spec links_key_to_doc_key(Key :: datastore:key()) -> BinKey :: binary().
links_key_to_doc_key(Key) ->
    {links, DocKey} = binary_to_term(base64:decode(Key)),
    DocKey.

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
    Gateways = maps:values(datastore_worker:state_get(db_gateways)),
    ActiveGateways = [GW || #{status := running} = GW <- Gateways],

    case ActiveGateways of
        [] ->
            ?error("Unable to select CouchBase Gateway: no active gateway among: ~p", [Gateways]),
            {error, no_active_gateway};
        _ ->
            try
                #{gw_port := Port, server := ServerLoop} = lists:nth(crypto:rand_uniform(1, length(ActiveGateways) + 1), ActiveGateways),
                Server = couchbeam:server_connection("localhost", Port),
                {ok, DB} = couchbeam:open_db(Server, <<"default">>),
                {ok, {ServerLoop, DB}}
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
force_save(#model_config{bucket = Bucket} = _ModelConfig, #document{key = Key, rev = {Start, Ids} = Revs, value = Value}) ->
    ok = assert_value_size(Value),

    {Props} = to_json_term(Value),
    Doc = {[{<<"_revisions">>, {[{<<"ids">>, Ids}, {<<"start">>, Start}]}}, {<<"_rev">>, rev_info_to_rev(Revs)}, {<<"_id">>, to_driver_key(Bucket, Key)} | Props]},
    case db_run(couchbeam, save_doc, [Doc, [{<<"new_edits">>, <<"false">>}]], 3) of
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
        server => self(), port_fd => PortFD, status => running, id => {node(), N},
        gw_port => GWPort, gw_admin_port => GWAdminPort, db_hostname => Hostname, db_port => Port,
        start_time => erlang:system_time(milli_seconds), parent => Parent
    },
    monitor(process, Parent),
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
        try db_run(couchbeam, db_info, [], 0) of
            {error, econnrefused} when Timeout > BusyWaitInterval ->
                timer:sleep(BusyWaitInterval),
                WaitForConnection(Timeout - BusyWaitInterval);
            _ ->
                ok %% Other errors will be handled in gateway_loop/1
        catch
            _:_ -> ok %% Other errors will be handled in gateway_loop/1
        end
    end,

    WaitForStateFun(timer:seconds(2)),
    WaitForConnectionFun(timer:seconds(2)),

    gateway_loop(State).


%%--------------------------------------------------------------------
%% @doc
%% Loop for managing Erlang Port (couchbase-sync-gateway).
%% @end
%%--------------------------------------------------------------------
-spec gateway_loop(State :: #{atom() => term()}) -> no_return().
gateway_loop(#{port_fd := PortFD, id := {_, N} = ID, db_hostname := Hostname, db_port := Port,
    start_time := ST, parent := Parent} = State) ->
    try port_command(PortFD, <<"ping">>) of
        true ->
            try db_run(couchbeam, db_info, [], 0) of
                {ok, _} -> ok;
                {error, Reason00} ->
                    self() ! {port_comm_error, Reason00}
            catch
                _:{badmap, undefined} ->
                    ok; %% State of the worker may not be initialised yet, so there is not way to check if connection is active
                _:Reason01 ->
                    self() ! {port_comm_error, Reason01}
            end
    catch
        _:Reason0 ->
            self() ! {port_comm_error, Reason0}
    end,

    CT = erlang:system_time(milli_seconds),
    MinRestartTime = ST + timer:seconds(5),

    NewState =
        receive
            {PortFD, {data, {_, Data}}} ->
                case binary:matches(Data, <<"HTTP:">>) of
                    [] ->
                        ?info("[CouchBase Gateway ~p] ~s", [ID, Data]);
                    _ -> ok
                end,
                State;
            {PortFD, closed} ->
                State#{status => closed};

            {'EXIT', PortFD, Reason} ->
                ?error("CouchBase gateway's port ~p exited with reason: ~p", [State, Reason]),
                State#{status => failed};
            {port_comm_error, Reason} ->
                ?error("[CouchBase Gateway ~p] Unable to communicate with port due to: ~p", [ID, Reason]),
                State#{status => failed};
            restart when CT > MinRestartTime ->
                ?info("[CouchBase Gateway ~p] Restart request...", [ID]),
                State#{status => restarting};
            restart ->
                State;
            {'DOWN', _, process, Parent, Reason} ->
                    catch port_close(PortFD),
                State#{status => closed};
            stop ->
                    catch port_close(PortFD),
                State#{status => closed};
            Other ->
                ?warning("[CouchBase Gateway ~p] ~p", [ID, Other]),
                State
        after timer:seconds(1) ->
            State
        end,
    case NewState of
        #{status := running} ->
            gateway_loop(NewState);
        #{status := closed} ->
            ok;
        #{status := restarting} ->
                catch port_close(PortFD),
            start_gateway(self(), N, Hostname, Port);
        #{status := failed} ->
                catch port_close(PortFD),
            start_gateway(self(), N, Hostname, Port)
    end.


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
            RawDocOnceAgian = jiffy:decode(jsx:encode(RawDoc)),
            Document = process_raw_doc(RawDocOnceAgian),
                catch Callback(Seq, Document, model(Document)),
            State#state{last_seq = Seq}
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
-spec assert_value_size(Value :: term()) -> ok | no_return().
assert_value_size(Value) ->
    case byte_size(term_to_binary(Value)) > ?MAX_VALUE_SIZE of
        true -> error(term_to_big);
        false -> ok
    end.
