%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for serializing modification requests
%%% to a datastore document and its links.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_writer).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/3, save/3, update/3, update/4, fetch/2, delete/3]).
-export([add_links/4, fetch_links/4, delete_links/4, mark_links_deleted/4]).
-export([fold_links/6, fetch_links_trees/2]).
-export([call/4, call_async/4, wait/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    requests = [] :: list(),
    cache_writer_pid :: pid(),
    cache_writer_state = idle :: idle | {active, reference()},
    disc_writer_state = idle :: idle | {active, reference()},
    terminate_msg_ref :: undefined | reference(),
    terminate_timer_ref :: undefined | reference()
}).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type value() :: datastore_doc:value().
-type doc() :: datastore_doc:doc(value()).
-type diff() :: datastore_doc:diff(value()).
-type pred() :: datastore_doc:pred(value()).
-type tree_id() :: datastore_links:tree_id().
-type tree_ids() :: datastore_links:tree_ids().
-type link() :: datastore_links:link().
-type link_name() :: datastore_links:link_name().
-type link_target() :: datastore_links:link_target().
-type link_rev() :: datastore_links:link_rev().
-type fold_fun() :: datastore_links:fold_fun().
-type fold_acc() :: datastore_links:fold_acc().
-type fold_opts() :: datastore_links:fold_opts().
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:create/3} implementation.
%% @end
%%--------------------------------------------------------------------
-spec create(ctx(), key(), doc()) -> {ok, doc()} | {error, term()}.
create(Ctx, Key, Doc = #document{}) ->
    call(Ctx, {doc, Key}, create, [Key, Doc]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:save/3} implementation.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), key(), doc()) -> {ok, doc()} | {error, term()}.
save(Ctx, Key, Doc = #document{}) ->
    call(Ctx, {doc, Key}, save, [Key, Doc]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:update/3} implementation.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff()) -> {ok, doc()} | {error, term()}.
update(Ctx, Key, Diff) ->
    call(Ctx, {doc, Key}, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:update/4} implementation.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff(), doc()) -> {ok, doc()} | {error, term()}.
update(Ctx, Key, Diff, Default) ->
    call(Ctx, {doc, Key}, update, [Key, Diff, Default]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:get/2} implementation.
%% @end
%%--------------------------------------------------------------------
-spec fetch(ctx(), key()) -> {ok, doc()} | {error, term()}.
fetch(Ctx, Key) ->
    call(Ctx, {doc, Key}, fetch, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:delete/3} implementation.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(), key(), pred()) -> ok | {error, term()}.
delete(Ctx, Key, Pred) ->
    call(Ctx, {doc, Key}, delete, [Key, Pred]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:add_links/4} implementation.
%% @end
%%--------------------------------------------------------------------
-spec add_links(ctx(), key(), tree_id(), [{link_name(), link_target()}]) ->
    [{ok, link()} | {error, term()}].
add_links(Ctx, Key, TreeId, Links) ->
    Size = length(Links),
    multi_call(Ctx, {links, Key}, add_links, [Key, TreeId, Links], Size).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:get_links/4} implementation.
%% @end
%%--------------------------------------------------------------------
-spec fetch_links(ctx(), key(), tree_ids(), [link_name()]) ->
    [{ok, [link()]} | {error, term()}].
fetch_links(Ctx, Key, TreeIds, LinkNames) ->
    Size = length(LinkNames),
    multi_call(Ctx, {links, Key}, fetch_links, [Key, TreeIds, LinkNames], Size).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:delete_links/4} implementation.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(ctx(), key(), tree_id(), [{link_name(), link_rev()}]) ->
    [ok | {error, term()}].
delete_links(Ctx, Key, TreeId, Links) ->
    Size = length(Links),
    multi_call(Ctx, {links, Key}, delete_links, [Key, TreeId, Links], Size).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:mark_links_deleted/4}
%% implementation.
%% @end
%%--------------------------------------------------------------------
-spec mark_links_deleted(ctx(), key(), tree_id(), [{link_name(), link_rev()}]) ->
    [ok | {error, term()}].
mark_links_deleted(Ctx, Key, TreeId, Links) ->
    Size = length(Links),
    multi_call(Ctx, {links, Key}, mark_links_deleted, [Key, TreeId, Links], Size).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:fold_links/6} implementation.
%% @end
%%--------------------------------------------------------------------
-spec fold_links(ctx(), key(), tree_ids(), fold_fun(), fold_acc(),
    fold_opts()) -> {ok, fold_acc()} | {error, term()}.
fold_links(Ctx, Key, TreeIds, Fun, Acc, Opts) ->
    call(Ctx, {links, Key}, fold_links, [Key, TreeIds, Fun, Acc, Opts]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:get_links_trees/6}
%% implementation.
%% @end
%%--------------------------------------------------------------------
-spec fetch_links_trees(ctx(), key()) -> {ok, [tree_id()]} | {error, term()}.
fetch_links_trees(Ctx, Key) ->
    call(Ctx, {links, Key}, fetch_links_trees, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Performs synchronous call to a datastore writer process associated
%% with a key.
%% @end
%%--------------------------------------------------------------------
-spec call(ctx(), {doc | links, key()}, atom(), list()) -> term().
call(Ctx, Key, Function, Args) ->
    case call_async(Ctx, Key, Function, Args) of
        {ok, Ref} -> wait(Ref);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Multiplies requests handling error.
%% @end
%%--------------------------------------------------------------------
-spec multi_call(ctx(), {doc | links, key()}, atom(), list(), non_neg_integer()) ->
    term().
multi_call(Ctx, Key, Function, Args, Size) ->
    case call(Ctx, Key, Function, Args) of
        {error, Reason} -> [{error, Reason} || _ <- lists:seq(1, Size)];
        Response -> Response
    end.

%%--------------------------------------------------------------------
%% @doc
%% Performs asynchronous call to a datastore writer process associated
%% with a key and returns response reference, which may be passed to
%% {@link wait/1} to collect result.
%% @end
%%--------------------------------------------------------------------
-spec call_async(ctx(), {doc | links, key()}, atom(), list()) ->
    {ok, reference()} | {error, term()}.
call_async(Ctx, Key, Function, Args) ->
    Timeout = application:get_env(?CLUSTER_WORKER_APP_NAME,
        datastore_writer_request_queueing_timeout, timer:minutes(1)),
    Attempts = application:get_env(?CLUSTER_WORKER_APP_NAME,
        datastore_writer_request_queueing_attempts, 3),
    Request = {handle, {Function, [Ctx | Args]}},
    tp:call(?MODULE, [], Key, Request, Timeout, Attempts).

%%--------------------------------------------------------------------
%% @doc
%% Waits for a completion of asynchronous call to datastore writer.
%% @end
%%--------------------------------------------------------------------
-spec wait(reference()) -> term() | {error, timeout}.
wait(Ref) ->
    Timeout = application:get_env(?CLUSTER_WORKER_APP_NAME,
        datastore_writer_request_handling_timeout, timer:minutes(30)),
    receive
        {Ref, Response} -> Response
    after
        Timeout -> {error, timeout}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes datastore writer process associated with a key.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init(_Args) ->
    {ok, Pid} = datastore_cache_writer:start_link(self()),
    {ok, schedule_terminate(#state{cache_writer_pid = Pid})}.

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
handle_call({handle, Request}, {Pid, _Tag}, State = #state{
    requests = Requests
}) ->
    Ref = make_ref(),
    State2 = State#state{requests = [{Pid, Ref, Request} | Requests]},
    {reply, {ok, Ref}, schedule_terminate(handle_requests(State2))};
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
handle_cast({mark_cache_writer_idle, Ref}, State = #state{
    cache_writer_state = {active, Ref}
}) ->
    {noreply, handle_requests(State#state{cache_writer_state = idle})};
handle_cast({mark_disc_writer_idle, Ref}, State = #state{
    disc_writer_state = {active, Ref}
}) ->
    {noreply, State#state{disc_writer_state = idle}};
handle_cast({mark_disc_writer_idle, _}, State = #state{}) ->
    {noreply, State};
handle_cast(Request, State = #state{}) ->
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
handle_info({terminate, MsgRef}, State = #state{
    requests = [], cache_writer_state = idle, disc_writer_state = idle,
    terminate_msg_ref = MsgRef
}) ->
    {stop, normal, State};
handle_info({terminate, MsgRef}, State = #state{terminate_msg_ref = MsgRef}) ->
    {noreply, schedule_terminate(State)};
handle_info({terminate, _}, State = #state{}) ->
    {noreply, State};
handle_info({'EXIT', _, Reason}, State = #state{}) ->
    {stop, Reason, State};
handle_info(Info, State = #state{}) ->
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
terminate(Reason, State = #state{
    requests = Requests, cache_writer_pid = Pid
}) ->
    gen_server:call(Pid, {terminate, Requests}, infinity),
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

-spec handle_requests(state()) -> state().
handle_requests(State = #state{requests = []}) ->
    State;
handle_requests(State = #state{cache_writer_state = {active, _}}) ->
    State;
handle_requests(State = #state{
    requests = Requests, cache_writer_pid = Pid, cache_writer_state = idle
}) ->
    Ref = make_ref(),
    gen_server:call(Pid, {handle, Ref, lists:reverse(Requests)}, infinity),
    State#state{
        requests = [],
        cache_writer_state = {active, Ref},
        disc_writer_state = {active, Ref}
    }.

-spec schedule_terminate(state()) -> state().
schedule_terminate(State = #state{terminate_timer_ref = undefined}) ->
    Timeout = datastore_throttling:get_idle_timeout(),
    MsgRef = make_ref(),
    Msg = {terminate, MsgRef},
    State#state{
        terminate_msg_ref = MsgRef,
        terminate_timer_ref = erlang:send_after(Timeout, self(), Msg)
    };
schedule_terminate(State = #state{terminate_timer_ref = Ref}) ->
    erlang:cancel_timer(Ref),
    schedule_terminate(State#state{terminate_timer_ref = undefined}).