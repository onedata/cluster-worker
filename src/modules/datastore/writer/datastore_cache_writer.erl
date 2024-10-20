%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling requests batch to a datastore
%%% document and applying them on a datastore cache.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_cache_writer).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_protocol.hrl").
-include("modules/datastore/datastore_links.hrl").
-include("modules/datastore/ha_datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/4, call/2, save_master_pid/1, get_master_pid/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    process_key :: datastore:key(), % Key used by tp_router to unambiguously identify tp process
                                    % NOTE: many datastore keys are mapped to single tp_router key
    master_pid :: pid(),
    disc_writer_pid :: pid(),
    cached_keys_to_flush = #{} :: cached_keys(),
    keys_in_flush = #{} :: keys_in_flush(),
    keys_to_inactivate = #{} :: cached_keys(),
    keys_to_expire = #{} :: #{key() => countdown_timer:instance()},
    flush_countdown_timers = #{} :: #{key() => countdown_timer:instance()},
    requests_ref = undefined :: undefined | reference(),
    flush_timer :: undefined | reference(),
    inactivate_timer :: undefined | reference(),
    link_tokens = #{} :: cached_token_map(),

    ha_master_data :: ha_datastore_master:ha_master_data(),
    ha_failover_requests_data :: ha_datastore_slave:ha_failover_requests_data()
}).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type tree_id() :: datastore_links:tree_id().
-type tree() :: datastore_links:tree().
-type mask() :: datastore_links_mask:mask().
-type batch() :: datastore_doc_batch:batch().
-type cached_keys() :: datastore_doc_batch:cached_keys().
-type keys_in_flush() :: #{key() => {reference() | slave_flush, ctx() | undefined}}. % Undefined ctx value for keys
                                                                                     % in flush on HA slave node
-type request() :: term().
-type state() :: #state{}.
-type cached_token_map() ::
    #{reference() => {datastore_links_iter:token(), countdown_timer:instance()}}.
-type is_failover_request() :: boolean(). % see ha_datastore.hrl for failover requests description
-type remote_requests_processing_mode() :: ?HANDLE_LOCALLY | ?DELEGATE | ?IGNORE. % remote documents processing modes
                                                                         % (see datastore_protocol.hrl)

-export_type([keys_in_flush/0, remote_requests_processing_mode/0]).

-define(REV_LENGTH,
    application:get_env(cluster_worker, datastore_links_rev_length, 16)).

-define(DATASTORE_WRITER_FLUSH_DELAY,
    application:get_env(?CLUSTER_WORKER_APP_NAME, datastore_writer_flush_delay, timer:seconds(1))).
-define(FLUSH_INTERVAL, 100). % Time in us between consecutive flushes
-define(FLUSH_COOLDOWN, application:get_env(?CLUSTER_WORKER_APP_NAME, flush_key_cooldown_sec, 3)).
-define(TERMINATE_CHECK_INTERVAL, 1000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts and links datastore cache writer process to the caller.
%% @end
%%--------------------------------------------------------------------
-spec start_link(pid(), key(), [node()], [key()]) -> {ok, pid()} | {error, term()}.
start_link(MasterPid, Key, BackupNodes, KeysInSlaveFlush) ->
    gen_server:start_link(?MODULE, {MasterPid, Key, BackupNodes, KeysInSlaveFlush}, []).

-spec call(pid(), term()) -> term().
call(Pid, Msg) ->
    gen_server:call(Pid, Msg, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Saves pid of master process in memory.
%% @end
%%--------------------------------------------------------------------
-spec save_master_pid(pid()) -> ok.
save_master_pid(MasterPid) ->
    put(tp_master, MasterPid),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Gets pid of master process from memory.
%% @end
%%--------------------------------------------------------------------
-spec get_master_pid() -> pid() | undefined.
get_master_pid() ->
    get(tp_master).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes datastore cache writer process.
%% @end
%%--------------------------------------------------------------------
-spec init({MasterPid :: pid(), Key :: key(), BackupNodes :: [node()], KeysInSlaveFlush :: [key()]}) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init({MasterPid, Key, BackupNodes, KeysInSlaveFlush}) ->
    {ok, DiscWriterPid} = datastore_disc_writer:start_link(MasterPid, self()),
    save_master_pid(MasterPid),

    KiF = lists:map(fun(KeyInFlush) -> {KeyInFlush, {slave_flush, undefined}} end, KeysInSlaveFlush),
    {ok, #state{process_key = Key, master_pid = MasterPid, disc_writer_pid = DiscWriterPid,
        keys_in_flush = maps:from_list(KiF),
        ha_master_data = ha_datastore_master:init_data(BackupNodes),
        ha_failover_requests_data = ha_datastore_slave:init_failover_requests_data()}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call(#datastore_internal_requests_batch{ref = Ref, requests = Requests, mode = Mode} = RequestsBatch, From,
    State = #state{process_key = ProcessKey, master_pid = Pid}) ->
    #qualified_datastore_requests{local_requests = LocalRequests, remote_requests = RemoteRequests,
        remote_node = RemoteNode, remote_requests_processing_mode = RemoteRequestsProcessingMode} =
        ha_datastore_slave:qualify_and_reverse_requests(Requests, Mode),
    gen_server:reply(From, {RemoteRequestsProcessingMode, RemoteNode}),
    State2 = handle_requests(LocalRequests, false, State#state{requests_ref = Ref}),
    State3 = case RemoteRequestsProcessingMode of
        ?DELEGATE ->
            % TODO - VFS-6171 - reply to caller
            RemoteRequestsReversed = lists:reverse(RemoteRequests), % requests are stored and sent in reversed list
            case rpc:call(RemoteNode, datastore_writer, generic_call, [ProcessKey,
                RequestsBatch#datastore_internal_requests_batch{requests = RemoteRequestsReversed}]) of
                {ok, ProxyPid} ->
                    send_proxy_info(RemoteRequestsReversed, {request_delegated, ProxyPid}),
                    State2;
                {badrpc, nodedown} ->
                    % TODO VFS-6169 - wrong return status in such case
                    % TODO VFS-6295 - log to dedicated logfile
                    ?debug("Proxy call to failed node ~tp for requests ~tp", [RemoteNode, RemoteRequestsReversed]),
                    handle_requests(RemoteRequests, true, State2);
                {badrpc, Reason} ->
                    ?error("Proxy call to node ~tp badrpc ~tp for requests ~tp",
                        [RemoteNode, Reason, RemoteRequestsReversed]),
                    send_proxy_info(RemoteRequestsReversed, {error, Reason}),
                    State2;
                Error ->
                    ?error("Proxy call to node ~tp error ~tp for requests ~tp",
                        [RemoteNode, Error, RemoteRequestsReversed]),
                    send_proxy_info(RemoteRequestsReversed, Error),
                    State2
            end;
        _ ->
            handle_requests(RemoteRequests, true, State2)
    end,
    gen_server:cast(Pid, {mark_cache_writer_idle, Ref}),
    {noreply, schedule_flush(State3)};
handle_call(#datastore_flush_request{keys = Keys}, From, State = #state{
    cached_keys_to_flush = CachedKeys,
    master_pid = Pid
}) ->
    gen_server:reply(From, ok),
    NewKeys = maps:merge(CachedKeys, Keys),
    tp_router:update_process_size(Pid, maps:size(NewKeys)),
    State2 = State#state{cached_keys_to_flush = NewKeys},
    {noreply, schedule_flush(State2)};
% Standard termination request
% If datastore_cache_writer cannot terminate instantly, it replies 'working` and will not terminate.
% Datastore_writer will try again later.
% The reason of such behaviour is that new requests can appear anytime so
% datastore_writer should not be blocked waiting for datastore_cache_writer termination.
handle_call(terminate, _From, #state{
    keys_to_inactivate = ToInactivate,
    disc_writer_pid = Pid} = State) ->
    case allow_terminate(State) of
        true ->
            catch gen_server:call(Pid, terminate, infinity), % catch exception - disc writer could be already terminated
            datastore_cache:inactivate(ToInactivate),
            {stop, normal, ok, State};
        false ->
            {reply, working, State}
    end;
% Termination request during node shutdown
% If datastore_cache_writer cannot terminate instantly, it starts procedure that leads to termination.
% Datastore_writer will be answered when datastore_cache_writer is ready to terminate.
% The reason of such behaviour is that new requests will not appear (cluster is terminating) and
% datastore_writer should be blocked to prevent node termination before all documents are flushed.
handle_call({terminate, Requests}, From, State) ->
    State2 = handle_requests(Requests, false, State), % TODO - VFS-6169 Handle failover and proxy requests
    case handle_call(terminate, From, State2) of
        {reply, working, State} ->
            schedule_terminate_check(From),
            {noreply, State2};
        Other ->
            Other
    end;
% Call used during the test (do not use - test-only method)
handle_call(force_terminate, _From, #state{
    keys_to_inactivate = ToInactivate,
    disc_writer_pid = Pid} = State) ->
    catch gen_server:call(Pid, terminate, infinity), % catch exception - disc writer could be already terminated
    datastore_cache:inactivate(ToInactivate),
    {stop, normal, ok, State};
handle_call(?SLAVE_MSG(Msg), _From, #state{ha_master_data = Data} = State) ->
    Data2 = ha_datastore_master:handle_slave_lifecycle_message(Msg, Data),
    {reply, ok, State#state{ha_master_data = Data2}};
handle_call(?INTERNAL_MSG(Msg), _From, #state{ha_master_data = Data} = State) ->
    Data2 = ha_datastore_master:handle_internal_call(Msg, Data),
    {reply, ok, State#state{ha_master_data = Data2}};
handle_call(#cluster_reorganization_started{caller_pid = CallerPid, message_ref = Ref}, From, State = #state{
    cached_keys_to_flush = CachedKeys,
    keys_in_flush = KeysInFlush,
    master_pid = MasterPid,
    ha_failover_requests_data = FailoverData
}) ->
    gen_server:reply(From, ok),
    UsedKeysMap = maps:fold(fun
        (_Key, {slave_flush, undefined}, Acc) -> Acc;
        (Key, {_Ref, Ctx}, Acc) -> maps:put(Key, Ctx, Acc)
    end, CachedKeys, KeysInFlush),
    FailoverData2 = ha_datastore_slave:prepare_and_send_reorganization_failover_requests(
        UsedKeysMap, MasterPid, FailoverData),
    CallerPid ! {Ref, ok},
    gen_server:cast(MasterPid, {mark_cache_writer_idle, Ref}),
    {noreply, State#state{ha_failover_requests_data = FailoverData2}};
handle_call(Request, _From, State = #state{}) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast({flushed, Ref, NotFlushed}, State = #state{
    cached_keys_to_flush = CachedKeys,
    requests_ref = CurrentRef,
    master_pid = Pid,
    keys_in_flush = KIF,
    keys_to_expire = ToExpire,
    flush_countdown_timers = FT
}) ->
    NewKeys = maps:merge(NotFlushed, CachedKeys),

    FlushTimer = countdown_timer:start_seconds(?FLUSH_COOLDOWN),
    {KIF2, FT2, Flushed} = case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_fast_flush, on) of
        on ->
            Filtered = maps:filter(fun(_K, {V, _}) ->
                V =/= Ref
            end, KIF),

            Flushed0 = maps:without(maps:keys(Filtered), KIF),
            NewFT = maps:fold(fun(K, _V, Acc) ->
                maps:put(K, FlushTimer, Acc)
            end, FT, Flushed0),

            {Filtered, NewFT, Flushed0};
        _ ->
            {#{}, #{}, KIF}
    end,

    NewRef = case {Ref, map_size(NotFlushed)} of
        {CurrentRef, 0} -> undefined;
        _ -> CurrentRef
    end,

    ToExpire2 = maps:fold(fun
        (K, {_, #{expiry := Expiry, disc_driver := undefined}}, Acc) when Expiry > 0 ->
            maps:put(K, countdown_timer:start_seconds(Expiry), Acc);
        (_, _, Acc) ->
            Acc
    end, ToExpire, Flushed),

    tp_router:update_process_size(Pid, maps:size(NewKeys)),
    {noreply, check_inactivate(schedule_flush(State#state{
        cached_keys_to_flush = NewKeys,
        flush_timer = undefined,
        requests_ref = NewRef,
        keys_in_flush = KIF2,
        keys_to_expire = ToExpire2,
        flush_countdown_timers = FT2
    }, ?FLUSH_INTERVAL))};
handle_cast(?INTERNAL_MSG(Msg), #state{keys_in_flush = KiF} = State) ->
    NewKiF = ha_datastore_master:handle_internal_cast(Msg, KiF),
    {noreply, schedule_flush(State#state{keys_in_flush = NewKiF}, ?FLUSH_INTERVAL)};
handle_cast(Request, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info(flush, State = #state{
    master_pid = MasterPid,
    requests_ref = Ref,
    cached_keys_to_flush = CachedKeys,
    keys_in_flush = KiF
}) when map_size(CachedKeys) =:= 0, map_size(KiF) =:= 0 ->
    % Some requests' handling may not change any key
    % However, master has to be informed
    gen_server:cast(MasterPid, {mark_disc_writer_idle, Ref}),
    {noreply, State#state{requests_ref = undefined, flush_timer = undefined}};
handle_info(flush, State = #state{
    disc_writer_pid = Pid,
    requests_ref = Ref,
    keys_in_flush = KiF,
    cached_keys_to_flush = CachedKeys,
    keys_to_inactivate = ToInactivate,
    flush_countdown_timers = FT
}) ->
    {NewCachedKeys, NewKiF, NewFT, State2} =
        case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_fast_flush, on) of
            on ->
                KiFKeys = maps:keys(KiF),
                ToFlush0 = maps:without(KiFKeys, CachedKeys),

                ToFlush = maps:filter(fun(K, _V) ->
                    case maps:get(K, FT, undefined) of
                        undefined -> true;
                        TimerInstance -> countdown_timer:is_expired(TimerInstance)
                    end
                end, ToFlush0),

                case {maps:size(ToFlush), maps:size(ToFlush0)} of
                    {0, 0} ->
                        {CachedKeys, KiF, FT, State#state{flush_timer = undefined}};
                    {0, _} ->
                        {CachedKeys, KiF, FT, schedule_flush(State#state{flush_timer = undefined})};
                    _ ->
                        Waiting = maps:without(maps:keys(ToFlush), CachedKeys),

                        KiF2 = maps:fold(fun(K, Ctx, Acc) ->
                            maps:put(K, {Ref, Ctx}, Acc)
                        end, KiF, ToFlush),

                        % TODO VFS-4221 - remove datastore_disc_writer
                        ToFlush2 = maps:map(fun
                            (_K, #{
                                disc_driver_ctx := DiscCtx
                            } = Ctx) ->
                                maps:put(disc_driver_ctx,
                                    maps:put(answer_to, Pid, DiscCtx), Ctx);
                            (_K, Ctx) ->
                                Ctx
                        end, ToFlush),

                        case maps:size(Waiting) of
                            0 ->
                                tp_router:delete_process_size(Pid);
                            Size ->
                                tp_router:update_process_size(Pid, Size)
                        end,

                        Futures = datastore_disc_writer:flush_async(ToFlush2),
                        gen_server:cast(Pid, {wait_flush, Ref, Futures}),

                        Timer = countdown_timer:start_seconds(?FLUSH_COOLDOWN),
                        FT2 = maps:fold(fun(K, _V, Acc) ->
                            maps:put(K, Timer, Acc)
                        end, FT, ToFlush2),

                        {Waiting, KiF2, FT2, State}
                end;
            _ ->
                tp_router:delete_process_size(Pid),
                gen_server:call(Pid, {flush, Ref, CachedKeys}, infinity),
                KiF2 = maps:fold(fun(K, Ctx, Acc) ->
                    maps:put(K, {Ref, Ctx}, Acc)
                end, KiF, CachedKeys),

                {#{}, KiF2, #{}, State}
        end,

    case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_gc, on) of
        on ->
            erlang:garbage_collect();
        _ ->
            ok
    end,

    State3 = State2#state{
        cached_keys_to_flush = NewCachedKeys,
        keys_in_flush = NewKiF,
        keys_to_inactivate = maps:merge(ToInactivate, CachedKeys),
        flush_countdown_timers = NewFT
    },

    {noreply, State3};
handle_info(inactivate, #state{} = State) ->
    {noreply, check_inactivate(State#state{inactivate_timer = undefined})};
handle_info({check_terminate, ReplyTo}, #state{
    keys_to_inactivate = ToInactivate,
    disc_writer_pid = Pid} = State) ->
    case allow_terminate(State) of
        true ->
            catch gen_server:call(Pid, terminate, infinity), % catch exception - disc writer could be already terminated
            datastore_cache:inactivate(ToInactivate),
            gen_server:reply(ReplyTo, ok),
            {stop, normal, State};
        false ->
            schedule_terminate_check(ReplyTo),
            {noreply, State}
    end;
handle_info(Info, #state{} = State) ->
    ?log_bad_request(Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: state()) -> term().
terminate(Reason, #state{} = State) ->
    ?log_terminate(Reason, State).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles requests.
%% @end
%%--------------------------------------------------------------------
-spec handle_requests(datastore_writer:requests_internal(), is_failover_request(), state()) -> state().
handle_requests([], _IsFailoverRequest, State) ->
    State;
handle_requests(Requests, IsFailoverRequest, State = #state{
    cached_keys_to_flush = CachedKeys,
    master_pid = Pid,
    link_tokens = LT
}) ->
    Batch = datastore_doc_batch:init(),
    {Responses, Batch2, LT2} = batch_requests(Requests, [], Batch, LT),
    CacheRequests = datastore_doc_batch:create_cache_requests(Batch2),
    {Batch3, SuccessfulCacheRequests} = datastore_doc_batch:apply_cache_requests(Batch2, CacheRequests),
    Batch4 = send_responses(Responses, Batch3),
    CachedKeys2 = datastore_doc_batch:terminate(Batch4),

    NewKeys = maps:merge(CachedKeys, CachedKeys2),
    tp_router:update_process_size(Pid, maps:size(NewKeys)),
    State2 = handle_ha_requests(CachedKeys2, SuccessfulCacheRequests, IsFailoverRequest, State),

    State2#state{cached_keys_to_flush = NewKeys, link_tokens = LT2}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Batches requests.
%% @end
%%--------------------------------------------------------------------
-spec batch_requests(datastore_writer:requests_internal(), list(), batch(), cached_token_map()) ->
    {list(), batch(), cached_token_map()}.
batch_requests([], Responses, Batch, LinkTokens) ->
    {lists:reverse(Responses), Batch, LinkTokens};
batch_requests([#datastore_internal_request{pid = Pid, ref = Ref, request = Request} | Requests],
    Responses, Batch, LinkTokens) ->
    case batch_request(Request, Batch, LinkTokens) of
        {Response, Batch2, LinkTokens2} ->
            batch_requests(Requests, [{Pid, Ref, Response} | Responses],
                Batch2, LinkTokens2);
        {Response, Batch2} ->
            % Request does not use tokens
            batch_requests(Requests, [{Pid, Ref, Response} | Responses],
                Batch2, LinkTokens)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Batches request.
%% @end
%%--------------------------------------------------------------------
-spec batch_request(term(), batch(), cached_token_map()) ->
    {term(), batch()} | {term(), batch(), cached_token_map()}.
batch_request(#datastore_request{module = Module, function = Function, ctx = Ctx, args = Args}, Batch, _LinkTokens)
    when Module =:= infinite_log orelse Module =:= time_series_collection ->
    batch_apply(Batch, fun(Batch2) ->
        erlang:apply(Module, Function, [set_mutator_pid(Ctx) | Args] ++ [Batch2])
    end);
%% @TODO VFS-7614 Add module value to datastore_request record for each call
batch_request(#datastore_request{function = create, ctx = Ctx, args = [Key, Doc]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:create(set_mutator_pid(Ctx), Key, Doc, Batch2)
    end);
batch_request(#datastore_request{function = save, ctx = Ctx, args = [Key, Doc]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:save(set_mutator_pid(Ctx), Key, Doc, Batch2)
    end);
batch_request(#datastore_request{function = update, ctx = Ctx, args = [Key, Diff]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:update(set_mutator_pid(Ctx), Key, Diff, Batch2)
    end);
batch_request(#datastore_request{function = update, ctx = Ctx, args = [Key, Diff, Doc]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:update(set_mutator_pid(Ctx), Key, Diff, Doc, Batch2)
    end);
batch_request(#datastore_request{function = create_backup, ctx = Ctx, args = [Key]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        CtxWithMutator = set_mutator_pid(Ctx),
        case datastore_doc:fetch(CtxWithMutator, Key, Batch2) of
            {{ok, Doc}, Batch3} -> datastore_doc:save(CtxWithMutator, Key, Doc, Batch3);
            Other -> Other
        end
    end);
batch_request(#datastore_request{function = fetch, ctx = Ctx, args = [Key]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:fetch(set_mutator_pid(Ctx), Key, Batch2)
    end);
batch_request(#datastore_request{function = delete, ctx = Ctx, args = [Key, Pred]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:delete(set_mutator_pid(Ctx), Key, Pred, Batch2)
    end);
batch_request(#datastore_request{function = add_links, ctx = Ctx, args = [Key, TreeId, Links]}, Batch, _LinkTokens) ->
    Items = case maps:get(sync_enabled, Ctx, false) of
        true ->
            lists:map(fun({LinkName, LinkTarget}) ->
                % TODO - VFS-4904 takes half of time
                LinkRev = str_utils:rand_hex(?REV_LENGTH),
                {LinkName, {LinkTarget, LinkRev}}
            end, Links);
        _ ->
            lists:map(fun({LinkName, LinkTarget}) ->
                {LinkName, {LinkTarget, undefined}}
            end, Links)
    end,
    links_tree_apply(Ctx, Key, TreeId, Batch, fun(Tree) ->
        add_links(Items, Tree, TreeId, [], [])
    end);
batch_request(#datastore_request{function = check_and_add_links, ctx = Ctx, args = [Key, TreeId, CheckTrees, Links]},
    Batch, _LinkTokens) ->
    TreesToFetch = case CheckTrees of
        all ->
            datastore_links:get_links_trees(set_mutator_pid(Ctx), Key, Batch);
        [TreeId] -> % tree where links are added are not fetched
            {{error, not_found}, Batch};
        [] ->
            {{error, not_found}, Batch};
        _ ->
            {{ok, CheckTrees}, Batch}
    end,
    case TreesToFetch of
        {{ok, TreeIds}, Batch2} ->
            % do not fetch tree where links are added - links will be checked during add_links execution
            {FetchResult, Batch3} = batch_request(#datastore_request{function = fetch_links, ctx = Ctx,
                args = [Key, TreeIds -- [TreeId], lists:map(fun({Name, _}) -> Name end, Links)]}, Batch2, _LinkTokens),

            ToAdd = lists:filtermap(fun
                ({{_, {error, not_found}}, Link}) -> {true, Link};
                (_) -> false
            end, lists:zip(lists:reverse(FetchResult), Links)),
            {AddResult, Batch4} = batch_request(#datastore_request{function = add_links, ctx = Ctx,
                args = [Key, TreeId, ToAdd]}, Batch3, _LinkTokens),

            {prepare_check_and_add_ans(FetchResult, AddResult), Batch4};
        {{error, not_found}, Batch2} ->
            batch_request(#datastore_request{function = add_links, ctx = Ctx, args = [Key, TreeId, Links]},
                Batch2, _LinkTokens);
        Other ->
            {generate_error_ans(length(Links), Other), Batch}
    end;
batch_request(#datastore_request{function = fetch_links, ctx = Ctx, args = [Key, TreeIds, LinkNames]},
    Batch, _LinkTokens) ->
    lists:foldl(fun(LinkName, {Responses, Batch2}) ->
        {Response, Batch4} = batch_apply(Batch2, fun(Batch3) ->
            Ctx2 = set_mutator_pid(Ctx),
            case datastore_links_iter:init(Ctx2, Key, TreeIds, Batch3) of
                {ok, ForestIt} ->
                    {Result, ForestIt2} = datastore_links:get(LinkName, ForestIt),
                    {Result, datastore_links_iter:terminate(ForestIt2)};
                {{error, Reason}, ForestIt} ->
                    {{error, Reason}, datastore_links_iter:terminate(ForestIt)}
            end
        end),
        {[Response | Responses], Batch4}
    end, {[], Batch}, LinkNames);
batch_request(#datastore_request{function = delete_links, ctx = Ctx, args = [Key, TreeId, Links]}, Batch, _LinkTokens) ->
    Items = lists:map(fun({LinkName, LinkRev}) ->
        Pred = fun
            ({_, Rev}) when LinkRev =/= undefined -> Rev =:= LinkRev;
            (_) -> true
        end,
        {LinkName, Pred}
    end, Links),
    links_tree_apply(Ctx, Key, TreeId, Batch, fun(Tree) ->
        delete_links(Items, Tree, [], [])
    end);
batch_request(#datastore_request{function = mark_links_deleted, ctx = Ctx, args = [Key, TreeId, Links]},
    Batch, _LinkTokens) ->
    lists:foldl(fun({LinkName, LinkRev}, {Responses, Batch2}) ->
        {Response, Batch4} = batch_apply(Batch2, fun(Batch3) ->
            links_mask_apply(Ctx, Key, TreeId, Batch3, fun(Mask) ->
                datastore_links:mark_deleted(LinkName, LinkRev, Mask)
            end)
        end),
        {[Response | Responses], Batch4}
    end, {[], Batch}, Links);
batch_request(#datastore_request{function = fold_links, ctx = Ctx, args = [Key, TreeIds, Fun, Acc, Opts]},
    Batch, LinkTokens) ->
    Ref = make_ref(),
    Batch2 = datastore_doc_batch:init_request(Ref, Batch),

    Ctx2 = set_mutator_pid(Ctx),
    CacheToken = application:get_env(?CLUSTER_WORKER_APP_NAME,
        cache_fold_token, true),

    Opts2 = case CacheToken of
        true ->
            case maps:get(token, Opts, undefined) of
                undefined ->
                    Opts;
                OriginalToken ->
                    CachedToken = get_link_token(LinkTokens, OriginalToken),
                    maps:put(token, CachedToken, Opts)
            end;
        _ ->
            Opts
    end,

    {FoldResult, ForestIt} = datastore_links_iter:fold(Ctx2, Key, TreeIds,
        Fun, Acc, Opts2, Batch2),

    Batch3 = datastore_links_iter:terminate(ForestIt),
    case CacheToken of
        true ->
            case {FoldResult, maps:get(token, Opts, undefined)} of
                % If fold is executed with token, final results is extended with new token
                % that has to be processed before returning it to caller
                {{ListingResult, #link_token{} = Token}, OldToken} when OldToken =/= undefined ->
                    {NewToken, LinkTokens2} =
                        set_link_token(LinkTokens, Token, OldToken),
                    {{Ref, {ListingResult, NewToken}}, Batch3, LinkTokens2};
                _ ->
                    {{Ref, FoldResult}, Batch3, LinkTokens}
            end;
        _ ->
            {{Ref, FoldResult}, Batch3, LinkTokens}
    end;
batch_request(#datastore_request{function = fetch_links_trees, ctx = Ctx, args = [Key]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_links:get_links_trees(set_mutator_pid(Ctx), Key, Batch2)
    end);
batch_request(#datastore_request{function = ensure_forest_in_changes, ctx = Ctx0, args = [Key, TreeId]}, Batch, _LinkTokens) ->
    Ctx = set_mutator_pid(Ctx0#{ignore_in_changes => false}),
    Ref = make_ref(),
    Batch2 = datastore_doc_batch:init_request(Ref, Batch),
    case datastore_links:force_forest_update(Ctx, Key, Batch2) of
        {ok, Batch3} ->
            links_tree_apply(Ctx, Key, TreeId, Batch3, fun(Tree) ->
                {Response, Tree2} = datastore_links:force_all_nodes_update(Tree),
                {{Ref, Response}, Tree2}
            end);
        {{error, not_found}, Batch3} ->
            {{Ref, ok}, Batch3};
        {{error, Reason}, Batch3} ->
            {{Ref, {error, Reason}}, Batch3}
    end;
batch_request(#datastore_request{function = Function, ctx = Ctx, args = Args}, Batch, _LinkTokens) ->
    apply(datastore_doc_batch, Function, [Ctx | Args] ++ [Batch]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes links from tree.
%% Warning: links have to be sorted.
%% @end
%%--------------------------------------------------------------------
-spec delete_links([{datastore_links:link_name(), datastore_links:remove_pred()}],
    tree(), [{reference(), term()}], [datastore_links:link_name()]) ->
    {[{reference(), term()}], tree()}.
delete_links([], Tree, Responses, _RemovedKeys) ->
    {Responses, Tree};
delete_links([_ | LinksTail] = Links, Tree, Responses, []) ->
    {Response, Tree3} = batch_link_apply(Tree, fun(Tree2) ->
        datastore_links:delete(Links, Tree2)
    end),
    case Response of
        {Ref, {ok, [_ | RemovedKeys]}} ->
            delete_links(LinksTail, Tree3, [{Ref, ok} | Responses], RemovedKeys);
        _ ->
            delete_links(LinksTail, Tree3, [Response | Responses], [])
    end;
delete_links([_ | LinksTail], Tree, Responses, [_ | RemovedKeys]) ->
    {Response, Tree3} = batch_link_apply(Tree, fun(Tree2) ->
        {ok, Tree2}
    end),
    delete_links(LinksTail, Tree3, [Response | Responses], RemovedKeys).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds links to tree.
%% Warning: links have to be sorted.
%% @end
%%--------------------------------------------------------------------
-spec add_links([{datastore_links:link_name(), {datastore_links:link_target(),
    datastore_links:link_rev()}}], tree(), tree_id(), [{reference(), term()}],
    [datastore_links:link_name()]) -> {[{reference(), term()}], tree()}.
add_links([], Tree, _TreeId, Responses, _AddedKeys) ->
    {Responses, Tree};
add_links([{LinkName, {LinkTarget, LinkRev}} | LinksTail] = Links, Tree, TreeId,
    Responses, []) ->
    {Response, Tree3} = batch_link_apply(Tree, fun(Tree2) ->
        datastore_links:add(Links, Tree2)
    end),
    case Response of
        {Ref, {ok, [_ | AddedKeys]}} ->
            FinalResponse = {ok, #link{
                tree_id = TreeId,
                name = LinkName,
                target = LinkTarget,
                rev = LinkRev
            }},
            add_links(LinksTail, Tree3, TreeId,
                [{Ref, FinalResponse} | Responses], AddedKeys);
        _ ->
            add_links(LinksTail, Tree3, TreeId, [Response | Responses], [])
    end;
add_links([{LinkName, {LinkTarget, LinkRev}} | LinksTail], Tree, TreeId,
    Responses, [_ | AddedKeys]) ->
    {Response, Tree3} = batch_link_apply(Tree, fun(Tree2) ->
        {{ok, #link{
            tree_id = TreeId,
            name = LinkName,
            target = LinkTarget,
            rev = LinkRev
        }}, Tree2}
    end),
    add_links(LinksTail, Tree3, TreeId, [Response | Responses], AddedKeys).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies request on a batch.
%% @end
%%--------------------------------------------------------------------
-spec batch_apply(batch(), fun((batch()) -> {term(), batch()})) ->
    {{reference(), term()}, batch()}.
batch_apply(Batch, Fun) ->
    Ref = make_ref(),
    Batch2 = datastore_doc_batch:init_request(Ref, Batch),
    {Response, Batch3} = Fun(Batch2),
    {{Ref, Response}, Batch3}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies request on a batch.
%% @end
%%--------------------------------------------------------------------
-spec batch_link_apply(tree(), fun((tree()) -> {term(), tree()})) ->
    {{reference(), term()}, tree()}.
batch_link_apply(Tree, Fun) ->
    Ref = make_ref(),
    Tree2 = bp_tree:update_store_state(fun(State) ->
        links_tree:update_batch(fun(Batch) ->
            datastore_doc_batch:init_request(Ref, Batch)
        end, State)
    end, Tree),
    {Response, Tree3} = Fun(Tree2),
    {{Ref, Response}, Tree3}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies request on a links tree.
%% @end
%%--------------------------------------------------------------------
-spec links_tree_apply(ctx(), key(), tree_id(), batch(),
    fun((tree()) -> {term(), tree()})) -> {term(), batch()}.
links_tree_apply(Ctx, Key, TreeId, Batch, Fun) ->
    case datastore_links:init_tree(set_mutator_pid(Ctx), Key, TreeId, Batch) of
        {ok, Tree} ->
            {Result, Tree2} = Fun(Tree),
            {Result, datastore_links:finalize_tree_operation(Tree2)};
        Error ->
            {Error, Batch}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies request on a links tree mask.
%% @end
%%--------------------------------------------------------------------
-spec links_mask_apply(ctx(), key(), tree_id(), batch(),
    fun((mask()) -> {term(), mask()})) -> {term(), batch()}.
links_mask_apply(Ctx, Key, TreeId, Batch, Fun) ->
    Ctx2 = set_mutator_pid(Ctx),
    case datastore_links_mask:init(Ctx2, Key, TreeId, Batch) of
        {ok, Mask, _} ->
            case Fun(Mask) of
                {{error, Reason}, Mask2} ->
                    {_, Batch2} = datastore_links_mask:terminate(Mask2),
                    {{error, Reason}, Batch2};
                {Result, Mask2} ->
                    case datastore_links_mask:terminate(Mask2) of
                        {ok, Batch2} -> {Result, Batch2};
                        {{error, Reason}, Batch2} -> {{error, Reason}, Batch2}
                    end
            end;
        {{error, Reason}, Mask, _} ->
            {_, Batch2} = datastore_links_mask:terminate(Mask),
            {{error, Reason}, Batch2}
    end.

%% @private
-spec send_responses(list(), batch()) -> batch().
send_responses([], Batch) ->
    Batch;
send_responses([Response | Responses], Batch) ->
    send_response(Response, Batch),
    send_responses(Responses, Batch).

%% @private
-spec send_response(request() | [request()], batch()) -> any().
send_response({Pid, Ref, {_ReqRef, {error, Reason}}}, _Batch) ->
    Pid ! {Ref, {error, Reason}};
send_response({Pid, Ref, {ReqRef, Response}}, Batch) ->
    case datastore_doc_batch:terminate_request(ReqRef, Batch) of
        ok -> Pid ! {Ref, Response};
        {error, Reason} -> Pid ! {Ref, {error, Reason}}
    end;
send_response({Pid, Ref, Responses}, Batch) ->
    Responses2 = lists:map(fun
        ({_ReqRef, {error, Reason}}) ->
            {error, Reason};
        ({ReqRef, Response}) ->
            case datastore_doc_batch:terminate_request(ReqRef, Batch) of
                ok -> Response;
                {error, Reason} -> {error, Reason}
            end
    end, lists:reverse(Responses)),
    Pid ! {Ref, Responses2}.

%% @private
-spec send_proxy_info(datastore_writer:requests_internal(), term()) -> ok.
send_proxy_info([], _Info) ->
    ok;
send_proxy_info([#datastore_internal_request{pid = Pid, ref = Ref} | Requests], Info) ->
    Pid ! {Ref, Info},
    send_proxy_info(Requests, Info).

%% @private
-spec schedule_flush(state()) -> state().
schedule_flush(State) ->
    schedule_flush(State, ?DATASTORE_WRITER_FLUSH_DELAY).

%% @private
-spec schedule_flush(state(), non_neg_integer()) -> state().
schedule_flush(State = #state{cached_keys_to_flush = Map, requests_ref = undefined}, _Delay)
    when map_size(Map) == 0 ->
    % Nothing to flush and no requests are waiting for flush confirmation
    State;
schedule_flush(State = #state{cached_keys_to_flush = Map, keys_in_flush = KiF}, _Delay)
    when map_size(Map) == 0, map_size(KiF) =/= 0 ->
    % There is a request waiting for flush confirmation
    % but there is also pending flush request that will send confirmation
    % or trigger next flush
    State;
schedule_flush(State = #state{flush_timer = OldTimer}, Delay) ->
    case {OldTimer, Delay} of
        {undefined, _} ->
            Timer = erlang:send_after(Delay, self(), flush),
            State#state{flush_timer = Timer};
        {_, ?FLUSH_INTERVAL} ->
            case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_fast_flush, on) of
                on ->
                    erlang:cancel_timer(OldTimer, [{async, true}, {info, false}]),
                    Timer = erlang:send_after(Delay, self(), flush),
                    State#state{flush_timer = Timer};
                _ ->
                    State
            end;
        _ ->
            State
    end.

%% @private
-spec set_mutator_pid(ctx()) -> ctx().
set_mutator_pid(Ctx) ->
    Ctx#{mutator_pid => self()}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Inactivates keys if too many keys are stored in state.
%% @end
%%--------------------------------------------------------------------
-spec check_inactivate(state()) -> state().
check_inactivate(#state{
    keys_to_inactivate = ToInactivate,
    cached_keys_to_flush = CachedKeys,
    keys_in_flush = KiF,
    keys_to_expire = ToExpire,
    flush_countdown_timers = FT,
    link_tokens = LT,
    inactivate_timer = OldTimer
} = State) ->
    {ToExpire2, ExpireMaxTime} =
        maps:fold(fun(Key, ExpiryTimer, {Acc, MaxTime}) ->
            case countdown_timer:seconds_left(ExpiryTimer) of
                0 ->
                    {Acc, MaxTime};
                SecondsLeft ->
                    {maps:put(Key, ExpiryTimer, Acc), max(MaxTime, SecondsLeft)}
            end
        end, {#{}, 1}, ToExpire),

    Exclude = maps:keys(CachedKeys) ++ maps:keys(KiF) ++ maps:keys(ToExpire2),
    ToInactivate2 = maps:without(Exclude, ToInactivate),
    ExcludeMap = maps:with(Exclude, ToInactivate),

    datastore_cache:inactivate(ToInactivate2),
    State2 = handle_ha_inactivate(ToInactivate2, State),
    State3 = State2#state{keys_to_inactivate = ExcludeMap,
        flush_countdown_timers = maps:with(Exclude, FT)},

    LT2 = maps:filter(fun(_K, {_, TimerInstance}) ->
        not countdown_timer:is_expired(TimerInstance)
    end, LT),

    case maps:size(ToExpire2) + maps:size(State3#state.keys_to_inactivate) == 0 orelse OldTimer =/= undefined of
        true ->
            State3#state{link_tokens = LT2, keys_to_expire = ToExpire2};
        _ ->
            Timer = erlang:send_after(timer:seconds(ExpireMaxTime), self(), inactivate),
            State3#state{link_tokens = LT2, keys_to_expire = ToExpire2, inactivate_timer = Timer}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets link token from cache in batch.
%% @end
%%--------------------------------------------------------------------
-spec get_link_token(cached_token_map(), datastore_links_iter:token()) ->
    datastore_links_iter:token().
get_link_token(Tokens,
    #link_token{restart_token = {cached_token, Token}} = FullToken) ->
    {Token2, _} = maps:get(Token, Tokens, {undefined, ok}),
    FullToken#link_token{restart_token = Token2};
get_link_token(_Batch, Token) ->
    Token.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Puts link token in batch's cache.
%% @end
%%--------------------------------------------------------------------
-spec set_link_token(cached_token_map(), datastore_links_iter:token(),
    datastore_links_iter:token()) -> {datastore_links_iter:token(), cached_token_map()}.
set_link_token(Tokens, #link_token{restart_token = Token} = FullToken,
    #link_token{restart_token = {cached_token, Token2}}) ->
    CacheTimeout = application:get_env(?CLUSTER_WORKER_APP_NAME,
        fold_cache_timeout, timer:seconds(30)),
    Timer = countdown_timer:start_millis(CacheTimeout),
    {FullToken#link_token{restart_token = {cached_token, Token2}},
        maps:put(Token2, {Token, Timer}, Tokens)};
set_link_token(Tokens, Token, #link_token{} = OldToken) ->
    Token2 = erlang:make_ref(),
    set_link_token(Tokens, Token,
        OldToken#link_token{restart_token = {cached_token, Token2}});
set_link_token(Tokens, Token, _OldToken) ->
    set_link_token(Tokens, Token, #link_token{}).

-spec prepare_check_and_add_ans(list(), list()) -> list().
prepare_check_and_add_ans([], []) ->
    [];
prepare_check_and_add_ans([{_, {error, not_found}} | FetchTail], [AddAns | AddTail]) ->
    [AddAns | prepare_check_and_add_ans(FetchTail, AddTail)];
prepare_check_and_add_ans([{Ref, {ok, _}} | FetchTail], AddAnsList) ->
    [{Ref, {error, already_exists}} | prepare_check_and_add_ans(FetchTail, AddAnsList)];
prepare_check_and_add_ans([FetchAns | FetchTail], AddAnsList) ->
    [FetchAns | prepare_check_and_add_ans(FetchTail, AddAnsList)].

-spec generate_error_ans(non_neg_integer(), term()) -> list().
generate_error_ans(0, _Error) ->
    [];
generate_error_ans(N, Error) ->
    [{make_ref(), Error} | generate_error_ans(N - 1, Error)].

-spec handle_ha_requests(cached_keys(), [datastore_cache:cache_save_request()], is_failover_request(), state()) -> state().
handle_ha_requests(CachedKeys, CacheRequests, false, #state{process_key = ProcessKey, ha_master_data = HAData} = State) ->
    HAData2 = ha_datastore_master:store_backup(ProcessKey, CachedKeys, CacheRequests, HAData),
    State#state{ha_master_data = HAData2};
handle_ha_requests(CachedKeys, CacheRequests, true,
    #state{master_pid = MasterPid, ha_failover_requests_data = FailoverData} = State) ->
    % TODO VFS-6169 - check requests with local routing set in Ctx
    FailoverData2 = ha_datastore_slave:report_failover_request_handled(MasterPid, CachedKeys, CacheRequests, FailoverData),
    State#state{ha_failover_requests_data = FailoverData2}.

-spec handle_ha_inactivate(cached_keys(), state()) -> state().
handle_ha_inactivate(Inactivated,
    #state{master_pid = MasterPid, ha_master_data = HAData, ha_failover_requests_data = FailoverData} = State) ->
    ha_datastore_master:forget_backup(Inactivated, HAData),
    FailoverData2 = ha_datastore_slave:report_keys_flushed(MasterPid, Inactivated, FailoverData),
    State#state{ha_failover_requests_data = FailoverData2}.

-spec allow_terminate(state()) -> boolean().
allow_terminate(#state{
    requests_ref = undefined,
    cached_keys_to_flush = ToFlush,
    keys_in_flush = KiF,
    keys_to_expire = ToExpire
}) when map_size(ToFlush) =:= 0, map_size(ToExpire) =:= 0, map_size(KiF) =:= 0 ->
    true;
allow_terminate(_) ->
    false.

-spec schedule_terminate_check(ReplyTo :: {pid(), Tag :: term()}) -> TimerRef :: reference().
schedule_terminate_check(ReplyTo) ->
    erlang:send_after(?TERMINATE_CHECK_INTERVAL, self(), {check_terminate, ReplyTo}).