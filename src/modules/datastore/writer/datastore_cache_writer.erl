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
-include("modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/1, save_master_pid/1, get_master_pid/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    master_pid :: pid(),
    disc_writer_pid :: pid(),
    cached_keys_to_flush = #{} :: cached_keys(),
    keys_in_flush = #{} :: #{key() => reference()},
    keys_to_inactivate = #{} :: cached_keys(),
    keys_times = #{} :: #{key() => erlang:timestamp()},
    flush_times = #{} :: #{key() => erlang:timestamp()},
    requests_ref = undefined :: undefined | reference(),
    flush_ref = undefined :: undefined | reference(),
    link_tokens = #{} :: cached_token_map()
}).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type tree_id() :: datastore_links:tree_id().
-type tree() :: datastore_links:tree().
-type mask() :: datastore_links_mask:mask().
-type batch() :: datastore_doc_batch:batch().
-type cached_keys() :: datastore_doc_batch:cached_keys().
-type request() :: term().
-type state() :: #state{}.
-type cached_token_map() ::
    #{reference() =>{datastore_links_iter:token(), erlang:timestamp()}}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts and links datastore cache writer process to the caller.
%% @end
%%--------------------------------------------------------------------
-spec start_link(pid()) -> {ok, pid()} | {error, term()}.
start_link(MasterPid) ->
    gen_server:start_link(?MODULE, [MasterPid], []).

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
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([MasterPid]) ->
    {ok, DiscWriterPid} = datastore_disc_writer:start_link(MasterPid, self()),
    save_master_pid(MasterPid),
    {ok, #state{master_pid = MasterPid, disc_writer_pid = DiscWriterPid}}.

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
handle_call({handle, Ref, Requests}, From, State = #state{master_pid = Pid}) ->
    gen_server:reply(From, ok),
    State2 = handle_requests(Requests, State#state{requests_ref = Ref}),
    gen_server:cast(Pid, {mark_cache_writer_idle, Ref}),
    {noreply, schedule_flush(State2)};
handle_call(terminate, _From, #state{
    requests_ref = undefined,
    cached_keys_to_flush = #{},
    keys_to_inactivate = ToInactivate,
    disc_writer_pid = Pid} = State) ->
    gen_server:call(Pid, terminate, infinity),
    datastore_cache:inactivate(ToInactivate),
    {stop, normal, ok, State};
handle_call(terminate, _From, State) ->
    {reply, working, State};
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
    flush_times = FT
}) ->
    NewKeys = maps:merge(NotFlushed, CachedKeys),

    {KIF2, FT2} = case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_fast_flush, on) of
        on ->
            Filtered = maps:filter(fun(_K, V) ->
                V =/= Ref
            end, KIF),

            Timestamp = os:timestamp(),
            NewFT = maps:fold(fun(K, _V, Acc) ->
                maps:put(K, Timestamp, Acc)
            end, FT, maps:without(maps:keys(Filtered), KIF)),

            {Filtered, NewFT};
        _ ->
            {#{}, #{}}
    end,

    NewRef = case Ref of
        CurrentRef -> undefined;
        _ -> CurrentRef
    end,

    tp_router:update_process_size(Pid, maps:size(NewKeys)),
    {noreply, schedule_flush(check_inactivate(State#state{
        cached_keys_to_flush = NewKeys,
        flush_ref = undefined,
        requests_ref = NewRef,
        keys_in_flush = KIF2,
        flush_times = FT2
    }), 0)};
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
    disc_writer_pid = Pid,
    requests_ref = Ref,
    keys_in_flush = KiF,
    cached_keys_to_flush = CachedKeys,
    keys_to_inactivate = ToInactivate,
    flush_times = FT
}) ->
    {NewCachedKeys, NewKiF, NewFT, Schedule} =
        case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_fast_flush, on) of
        on ->
            KiFKeys = maps:keys(KiF),
            ToFlush0 = maps:without(KiFKeys, CachedKeys),

            Cooldown = application:get_env(?CLUSTER_WORKER_APP_NAME,
                flush_key_cooldown_sek, 3),
            CooldownUS = timer:seconds(Cooldown) * 1000,

            Now = os:timestamp(),
            ToFlush = maps:filter(fun(K, _V) ->
                FlushTime = maps:get(K, FT, {0,0,0}),
                timer:now_diff(Now, FlushTime) > CooldownUS
            end, ToFlush0),

            case {maps:size(ToFlush), maps:size(ToFlush0)} of
                {0, 0} ->
                    {CachedKeys, KiF, FT, false};
                {0, _} ->
                    {CachedKeys, KiF, FT, true};
                _ ->
                    Waiting = maps:without(maps:keys(ToFlush), CachedKeys),

                    KiF2 = maps:fold(fun(K, _V, Acc) ->
                        maps:put(K, Ref, Acc)
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

                    Timestamp = os:timestamp(),
                    FT2 = maps:fold(fun(K, _V, Acc) ->
                        maps:put(K, Timestamp, Acc)
                    end, FT, ToFlush2),

                    {Waiting, KiF2, FT2, false}
            end;
        _ ->
            tp_router:delete_process_size(Pid),
            gen_server:call(Pid, {flush, Ref, CachedKeys}, infinity),
            KiF2 = maps:fold(fun(K, _V, Acc) ->
                maps:put(K, Ref, Acc)
            end, KiF, CachedKeys),

            {#{}, KiF2, #{}, false}
    end,

    case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_gc, on) of
        on ->
            erlang:garbage_collect();
        _ ->
            ok
    end,

    State2 = State#state{
        cached_keys_to_flush = NewCachedKeys,
        keys_in_flush = NewKiF,
        keys_to_inactivate = maps:merge(ToInactivate, CachedKeys),
        flush_times = NewFT
    },

    State3 = case Schedule of
        true -> schedule_flush(State2);
        _ -> State2
    end,

    {noreply, State3};
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
-spec handle_requests(list(), state()) -> state().
handle_requests(Requests, State = #state{
    cached_keys_to_flush = CachedKeys,
    keys_times = KeysTimes,
    master_pid = Pid,
    link_tokens = LT
}) ->
    Batch = datastore_doc_batch:init(),
    {Responses, Batch2, LT2} = batch_requests(Requests, [], Batch, LT),
    Batch3 = datastore_doc_batch:apply(Batch2),
    Batch4 = send_responses(Responses, Batch3),
    CachedKeys2 = datastore_doc_batch:terminate(Batch4),

    NewKeys = maps:merge(CachedKeys, CachedKeys2),
    tp_router:update_process_size(Pid, maps:size(NewKeys)),

    Timestamp = os:timestamp(),
    Times = maps:map(fun(_K, _V) -> Timestamp end, NewKeys),
    KeysTimes2 = maps:merge(KeysTimes, Times),

    State#state{cached_keys_to_flush = NewKeys, keys_times = KeysTimes2,
        link_tokens = LT2}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Batches requests.
%% @end
%%--------------------------------------------------------------------
-spec batch_requests(list(), list(), batch(), cached_token_map()) ->
    {list(), batch(), cached_token_map()}.
batch_requests([], Responses, Batch, LinkTokens) ->
    {lists:reverse(Responses), Batch, LinkTokens};
batch_requests([{Pid, Ref, Request} | Requests], Responses, Batch, LinkTokens) ->
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
batch_request({create, [Ctx, Key, Doc]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:create(set_mutator_pid(Ctx), Key, Doc, Batch2)
    end);
batch_request({save, [Ctx, Key, Doc]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:save(set_mutator_pid(Ctx), Key, Doc, Batch2)
    end);
batch_request({update, [Ctx, Key, Diff]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:update(set_mutator_pid(Ctx), Key, Diff, Batch2)
    end);
batch_request({update, [Ctx, Key, Diff, Doc]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:update(set_mutator_pid(Ctx), Key, Diff, Doc, Batch2)
    end);
batch_request({fetch, [Ctx, Key]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:fetch(set_mutator_pid(Ctx), Key, Batch2)
    end);
batch_request({delete, [Ctx, Key, Pred]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:delete(set_mutator_pid(Ctx), Key, Pred, Batch2)
    end);
batch_request({add_links, [Ctx, Key, TreeId, Links]}, Batch, _LinkTokens) ->
    links_tree_apply(Ctx, Key, TreeId, Batch, fun(Tree) ->
        lists:foldl(fun({LinkName, LinkTarget}, {Responses, Tree2}) ->
            {Response, Tree4} = batch_link_apply(Tree2, fun(Tree3) ->
                datastore_links:add(LinkName, LinkTarget, Tree3)
            end),
            {[Response | Responses], Tree4}
        end, {[], Tree}, Links)
    end);
batch_request({fetch_links, [Ctx, Key, TreeIds, LinkNames]}, Batch, _LinkTokens) ->
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
batch_request({delete_links, [Ctx, Key, TreeId, Links]}, Batch, _LinkTokens) ->
    % TODO - sort links !!!
    FinalAns = links_tree_apply(Ctx, Key, TreeId, Batch, fun(Tree) ->
        {FinalResponse, FinalTree, _, _} = lists:foldl(fun
            ({LinkName, LinkRev}, {Responses, Tree2, [_ | TmpLinks], []}) ->
                put(next_ops, TmpLinks),
                {Response, Tree4} = batch_link_apply(Tree2, fun(Tree3) ->
                        datastore_links:delete(LinkName, LinkRev, Tree3)
                end),
%%                ?info("jjjjj ~p", [length(get(next_ops_finished))]),
                {[Response | Responses], Tree4, TmpLinks, get(next_ops_finished)};
            (_, {Responses, Tree2, [_ | TmpLinks], [_ | Processed]}) ->
                {Response, Tree4} = batch_link_apply(Tree2, fun(Tree3) ->
                    {ok, Tree3}
                end),
                {[Response | Responses], Tree4, TmpLinks, Processed}
        end, {[], Tree, Links, []}, Links),
        {FinalResponse, FinalTree}
    end),
    erase(next_ops),
    erase(next_ops_finished),
    FinalAns;
batch_request({mark_links_deleted, [Ctx, Key, TreeId, Links]}, Batch, _LinkTokens) ->
    lists:foldl(fun({LinkName, LinkRev}, {Responses, Batch2}) ->
        {Response, Batch4} = batch_apply(Batch2, fun(Batch3) ->
            links_mask_apply(Ctx, Key, TreeId, Batch3, fun(Mask) ->
                datastore_links:mark_deleted(LinkName, LinkRev, Mask)
            end)
        end),
        {[Response | Responses], Batch4}
    end, {[], Batch}, Links);
batch_request({fold_links, [Ctx, Key, TreeIds, Fun, Acc, Opts]}, Batch, LinkTokens) ->
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

    {Result, ForestIt} = datastore_links_iter:fold(Ctx2, Key, TreeIds,
        Fun, Acc, Opts2, Batch2),

    Batch3 = datastore_links_iter:terminate(ForestIt),
    case CacheToken of
        true ->
            case maps:get(token, Opts, undefined) of
                undefined ->
                    {{Ref, Result}, Batch3, LinkTokens};
                OldToken ->
                    {Result0, Token} = Result,
                    {NewToken, LinkTokens2} =
                        set_link_token(LinkTokens, Token, OldToken),
                    {{Ref, {Result0, NewToken}}, Batch3, LinkTokens2}
            end;
        _ ->
            {{Ref, Result}, Batch3, LinkTokens}
    end;
batch_request({fetch_links_trees, [Ctx, Key]}, Batch, _LinkTokens) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_links:get_links_trees(set_mutator_pid(Ctx), Key, Batch2)
    end);
batch_request({Function, Args}, Batch, _LinkTokens) ->
    apply(datastore_doc_batch, Function, Args ++ [Batch]).

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
    {ok, Tree} = datastore_links:init_tree(
        set_mutator_pid(Ctx), Key, TreeId, Batch
    ),
    {Result, Tree2} = Fun(Tree),
    {Result, datastore_links:terminate_tree(Tree2)}.

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
        {ok, Mask} ->
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
        {{error, Reason}, Mask} ->
            {_, Batch2} = datastore_links_mask:terminate(Mask),
            {{error, Reason}, Batch2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends responses.
%% @end
%%--------------------------------------------------------------------
-spec send_responses(list(), batch()) -> batch().
send_responses([], Batch) ->
    Batch;
send_responses([Response | Responses], Batch) ->
    send_response(Response, Batch),
    send_responses(Responses, Batch).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends response.
%% @end
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules flush.
%% @end
%%--------------------------------------------------------------------
-spec schedule_flush(state()) -> state().
schedule_flush(State) ->
    Delay = application:get_env(cluster_worker, datastore_writer_flush_delay,
        timer:seconds(1)),
    schedule_flush(State, Delay).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules flush.
%% @end
%%--------------------------------------------------------------------
-spec schedule_flush(state(), non_neg_integer()) -> state().
schedule_flush(State = #state{flush_ref = undefined}, Delay) ->
    erlang:send_after(Delay, self(), flush),
    State#state{flush_ref = make_ref()};
schedule_flush(State = #state{}, Delay) ->
    case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_fast_flush, on) of
        on ->
            erlang:send_after(Delay, self(), flush),
            State#state{flush_ref = make_ref()};
        _ ->
            State
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets mutator pid.
%% @end
%%--------------------------------------------------------------------
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
    keys_times = KeysTimes,
    flush_times = FT,
    link_tokens = LT
} = State) ->
    Max = application:get_env(?CLUSTER_WORKER_APP_NAME, max_key_tp_mem, 100),
    Exclude = maps:keys(CachedKeys) ++ maps:keys(KiF),
    ToInactivate2 = maps:without(Exclude, ToInactivate),
    ExcludeMap = maps:with(Exclude, ToInactivate),
    Now = os:timestamp(),

    State2 = case maps:size(ToInactivate) > Max of
        true ->
            datastore_cache:inactivate(ToInactivate2),
            State#state{keys_to_inactivate = ExcludeMap,
                keys_times = maps:with(Exclude, KeysTimes),
                flush_times = maps:with(Exclude, FT)};
        _ ->
            MaxTime = application:get_env(?CLUSTER_WORKER_APP_NAME,
                tp_active_mem_sek, 5),
            MaxTimeUS = timer:seconds(MaxTime) * 1000,

            ToInactivate3 = maps:filter(fun(K, _V) ->
                KeyTime = maps:get(K, KeysTimes, {0,0,0}),
                timer:now_diff(Now, KeyTime) > MaxTimeUS
            end, ToInactivate2),

            ExcludeMap2 = maps:without(maps:keys(ToInactivate3), ToInactivate2),
            datastore_cache:inactivate(ToInactivate3),
            NewToInactivate = maps:merge(ExcludeMap, ExcludeMap2),
            NewToInactivateKeys = maps:keys(NewToInactivate),
            State#state{keys_to_inactivate = NewToInactivate,
                keys_times = maps:with(NewToInactivateKeys, KeysTimes),
                flush_times = maps:with(NewToInactivateKeys, FT)}
    end,

    MaxLinkTime = application:get_env(?CLUSTER_WORKER_APP_NAME,
        fold_cache_timeout, timer:seconds(30)),
    MaxLinkTimeUS = MaxLinkTime * 1000,
    LT2 = maps:filter(fun(_K, {_, Time}) ->
        timer:now_diff(Now, Time) =< MaxLinkTimeUS
    end, LT),

    State2#state{link_tokens = LT2}.

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
    {FullToken#link_token{restart_token = {cached_token, Token2}},
        maps:put(Token2, {Token, os:timestamp()}, Tokens)};
set_link_token(Tokens, Token, #link_token{} = OldToken) ->
    Token2 = erlang:make_ref(),
    set_link_token(Tokens, Token,
        OldToken#link_token{restart_token = {cached_token, Token2}});
set_link_token(Tokens, Token, _OldToken) ->
    set_link_token(Tokens, Token, #link_token{}).
