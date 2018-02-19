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
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    master_pid :: pid(),
    disc_writer_pid :: pid(),
    cached_keys_to_flush = #{} :: cached_keys(),
    keys_to_inactivate = #{} :: cached_keys(),
    requests_ref = undefined :: undefined | reference(),
    flush_ref = undefined :: undefined | reference()
}).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type tree_id() :: datastore_links:tree_id().
-type tree_ids() :: datastore_links:tree_ids().
-type forest_it() :: datastore_links:forest_it().
-type tree() :: datastore_links:tree().
-type mask() :: datastore_links_mask:mask().
-type batch() :: datastore_doc_batch:batch().
-type cached_keys() :: datastore_doc_batch:cached_keys().
-type request() :: term().
-type state() :: #state{}.

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
    requests_ref = Ref
}) ->
    {noreply, schedule_flush(check_inactivate(State#state{
        cached_keys_to_flush = maps:merge(NotFlushed, CachedKeys),
        flush_ref = undefined,
        requests_ref = undefined
    }))};
handle_cast({flushed, _Ref, NotFlushed}, State = #state{
    cached_keys_to_flush = CachedKeys
}) ->
    {noreply, schedule_flush(check_inactivate(State#state{
        cached_keys_to_flush = maps:merge(NotFlushed, CachedKeys),
        flush_ref = undefined
    }))};
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
    cached_keys_to_flush = CachedKeys
}) ->
    gen_server:call(Pid, {flush, Ref, CachedKeys}, infinity),
    {noreply, State#state{
        cached_keys_to_flush = #{}
    }};
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
    keys_to_inactivate = ToInactivate,
    master_pid = Pid
}) ->
    Batch = datastore_doc_batch:init(),
    {Responses, Batch2} = batch_requests(Requests, [], Batch),
    Batch3 = datastore_doc_batch:apply(Batch2),
    Batch4 = send_responses(Responses, Batch3),
    CachedKeys2 = datastore_doc_batch:terminate(Batch4),

    NewKeys = maps:merge(CachedKeys, CachedKeys2),
    tp_router:update_process_size(Pid, maps:size(NewKeys)),
    State#state{cached_keys_to_flush = NewKeys,
        keys_to_inactivate = maps:merge(ToInactivate, CachedKeys2)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Batches requests.
%% @end
%%--------------------------------------------------------------------
-spec batch_requests(list(), list(), batch()) -> {list(), batch()}.
batch_requests([], Responses, Batch) ->
    {lists:reverse(Responses), Batch};
batch_requests([{Pid, Ref, Request} | Requests], Responses, Batch) ->
    {Response, Batch2} = batch_request(Request, Batch),
    batch_requests(Requests, [{Pid, Ref, Response} | Responses], Batch2).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Batches request.
%% @end
%%--------------------------------------------------------------------
-spec batch_request(term(), batch()) -> {term(), batch()}.
batch_request({create, [Ctx, Key, Doc]}, Batch) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:create(set_mutator_pid(Ctx), Key, Doc, Batch2)
    end);
batch_request({save, [Ctx, Key, Doc]}, Batch) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:save(set_mutator_pid(Ctx), Key, Doc, Batch2)
    end);
batch_request({update, [Ctx, Key, Diff]}, Batch) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:update(set_mutator_pid(Ctx), Key, Diff, Batch2)
    end);
batch_request({update, [Ctx, Key, Diff, Doc]}, Batch) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:update(set_mutator_pid(Ctx), Key, Diff, Doc, Batch2)
    end);
batch_request({fetch, [Ctx, Key]}, Batch) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:fetch(set_mutator_pid(Ctx), Key, Batch2)
    end);
batch_request({delete, [Ctx, Key, Pred]}, Batch) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_doc:delete(set_mutator_pid(Ctx), Key, Pred, Batch2)
    end);
batch_request({add_links, [Ctx, Key, TreeId, Links]}, Batch) ->
    lists:foldl(fun({LinkName, LinkTarget}, {Responses, Batch2}) ->
        {Response, Batch4} = batch_apply(Batch2, fun(Batch3) ->
            links_tree_apply(Ctx, Key, TreeId, Batch3, fun(Tree) ->
                datastore_links:add(LinkName, LinkTarget, Tree)
            end)
        end),
        {[Response | Responses], Batch4}
    end, {[], Batch}, Links);
batch_request({fetch_links, [Ctx, Key, TreeIds, LinkNames]}, Batch) ->
    lists:foldl(fun(LinkName, {Responses, Batch2}) ->
        {Response, Batch4} = batch_apply(Batch2, fun(Batch3) ->
            links_forest_apply(Ctx, Key, TreeIds, Batch3, fun(ForestIt) ->
                datastore_links:get(LinkName, ForestIt)
            end)
        end),
        {[Response | Responses], Batch4}
    end, {[], Batch}, LinkNames);
batch_request({delete_links, [Ctx, Key, TreeId, Links]}, Batch) ->
    lists:foldl(fun({LinkName, LinkRev}, {Responses, Batch2}) ->
        {Response, Batch4} = batch_apply(Batch2, fun(Batch3) ->
            links_tree_apply(Ctx, Key, TreeId, Batch3, fun(Tree) ->
                datastore_links:delete(LinkName, LinkRev, Tree)
            end)
        end),
        {[Response | Responses], Batch4}
    end, {[], Batch}, Links);
batch_request({mark_links_deleted, [Ctx, Key, TreeId, Links]}, Batch) ->
    lists:foldl(fun({LinkName, LinkRev}, {Responses, Batch2}) ->
        {Response, Batch4} = batch_apply(Batch2, fun(Batch3) ->
            links_mask_apply(Ctx, Key, TreeId, Batch3, fun(Mask) ->
                datastore_links:mark_deleted(LinkName, LinkRev, Mask)
            end)
        end),
        {[Response | Responses], Batch4}
    end, {[], Batch}, Links);
batch_request({fold_links, [Ctx, Key, TreeIds, Fun, Acc, Opts]}, Batch) ->
    batch_apply(Batch, fun(Batch2) ->
        links_forest_apply(Ctx, Key, TreeIds, Batch2, fun(ForestIt) ->
            datastore_links:fold(Fun, Acc, ForestIt, Opts)
        end)
    end);
batch_request({fetch_links_trees, [Ctx, Key]}, Batch) ->
    batch_apply(Batch, fun(Batch2) ->
        datastore_links:get_links_trees(set_mutator_pid(Ctx), Key, Batch2)
    end);
batch_request({Function, Args}, Batch) ->
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
%% Applies request on a links tree forest.
%% @end
%%--------------------------------------------------------------------
-spec links_forest_apply(ctx(), key(), tree_ids(), batch(),
    fun((forest_it()) -> {term(), forest_it()})) -> {term(), batch()}.
links_forest_apply(Ctx, Key, TreeIds, Batch, Fun) ->
    Ctx2 = set_mutator_pid(Ctx),
    case datastore_links_iter:init(Ctx2, Key, TreeIds, Batch) of
        {ok, ForestIt} ->
            {Result, ForestIt2} = Fun(ForestIt),
            {Result, datastore_links_iter:terminate(ForestIt2)};
        {{error, Reason}, ForestIt} ->
            {{error, Reason}, datastore_links_iter:terminate(ForestIt)}
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
schedule_flush(State = #state{flush_ref = undefined}) ->
    Delay = application:get_env(cluster_worker, datastore_writer_flush_delay,
        timer:seconds(1)),
    erlang:send_after(Delay, self(), flush),
    State#state{flush_ref = make_ref()};
schedule_flush(State = #state{}) ->
    State.

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
check_inactivate(#state{keys_to_inactivate = ToInactivate} = State) ->
    Max = application:get_env(?CLUSTER_WORKER_APP_NAME, max_key_tp_mem, 100),
    case maps:size(ToInactivate) > Max of
        true ->
            datastore_cache:inactivate(ToInactivate),
            State#state{keys_to_inactivate = #{}};
        _ ->
            State
    end.