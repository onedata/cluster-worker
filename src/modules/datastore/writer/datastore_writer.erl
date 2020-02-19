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
%%% It starts datastore cache writer which in turn starts datastore disc writer.
%%% All three processes are linked together and creates a single, higher-level
%%% serialization unit wrapped by {@link tp_server}.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_writer).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/3, save/3, update/3, update/4, fetch/2, delete/3]).
-export([add_links/4, check_and_add_links/5, fetch_links/4, delete_links/4, mark_links_deleted/4]).
-export([fold_links/6, fetch_links_trees/2]).
%% For ct tests
-export([call/4, call_async/4, wait/2]).

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
-type tp_key() :: datastore:key() | non_neg_integer()
    | {doc | links, datastore:key()}.

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
    call(Ctx, get_key(Ctx, Key, doc), create, [Key, Doc]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:save/3} implementation.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), key(), doc()) -> {ok, doc()} | {error, term()}.
save(Ctx, Key, Doc = #document{}) ->
    call(Ctx, get_key(Ctx, Key, doc), save, [Key, Doc]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:update/3} implementation.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff()) -> {ok, doc()} | {error, term()}.
update(Ctx, Key, Diff) ->
    call(Ctx, get_key(Ctx, Key, doc), update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:update/4} implementation.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff(), doc()) -> {ok, doc()} | {error, term()}.
update(Ctx, Key, Diff, Default) ->
    call(Ctx, get_key(Ctx, Key, doc), update, [Key, Diff, Default]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:get/2} implementation.
%% @end
%%--------------------------------------------------------------------
-spec fetch(ctx(), key()) -> {ok, doc()} | {error, term()}.
fetch(Ctx, Key) ->
    call(Ctx, get_key(Ctx, Key, doc), fetch, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:delete/3} implementation.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(), key(), pred()) -> ok | {error, term()}.
delete(Ctx, Key, Pred) ->
    call(Ctx, get_key(Ctx, Key, doc), delete, [Key, Pred]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:add_links/4} implementation.
%% @end
%%--------------------------------------------------------------------
-spec add_links(ctx(), key(), tree_id(), [{link_name(), link_target()}]) ->
    [{ok, link()} | {error, term()}].
add_links(Ctx, Key, TreeId, Links) ->
    Size = length(Links),
    multi_call(Ctx, get_key(Ctx, Key, links), add_links, [Key, TreeId, Links], Size).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:check_and_add_links/4} implementation.
%% @end
%%--------------------------------------------------------------------
-spec check_and_add_links(ctx(), key(), tree_id(), tree_ids(), [{link_name(), link_target()}]) ->
    [{ok, link()} | {error, term()}].
check_and_add_links(Ctx, Key, TreeId, CheckTrees, Links) ->
    Size = length(Links),
    multi_call(Ctx, get_key(Ctx, Key, links), check_and_add_links, [Key, TreeId, CheckTrees, Links], Size).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:get_links/4} implementation.
%% @end
%%--------------------------------------------------------------------
-spec fetch_links(ctx(), key(), tree_ids(), [link_name()]) ->
    [{ok, [link()]} | {error, term()}].
fetch_links(Ctx, Key, TreeIds, LinkNames) ->
    Size = length(LinkNames),
    multi_call(Ctx, get_key(Ctx, Key, links), fetch_links, [Key, TreeIds, LinkNames], Size).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:delete_links/4} implementation.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(ctx(), key(), tree_id(), [{link_name(), link_rev()}]) ->
    [ok | {error, term()}].
delete_links(Ctx, Key, TreeId, Links) ->
    Size = length(Links),
    multi_call(Ctx, get_key(Ctx, Key, links), delete_links, [Key, TreeId, Links], Size).

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
    multi_call(Ctx, get_key(Ctx, Key, links), mark_links_deleted, [Key, TreeId, Links], Size).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:fold_links/6} implementation.
%% @end
%%--------------------------------------------------------------------
-spec fold_links(ctx(), key(), tree_ids(), fold_fun(), fold_acc(),
    fold_opts()) -> {ok, fold_acc()} |
    {{ok, fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_links(Ctx, Key, TreeIds, Fun, Acc, Opts) ->
    call(Ctx, get_key(Ctx, Key, links), fold_links, [Key, TreeIds, Fun, Acc, Opts]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronous and thread safe {@link datastore:get_links_trees/6}
%% implementation.
%% @end
%%--------------------------------------------------------------------
-spec fetch_links_trees(ctx(), key()) -> {ok, [tree_id()]} | {error, term()}.
fetch_links_trees(Ctx, Key) ->
    call(Ctx, get_key(Ctx, Key, links), fetch_links_trees, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Performs synchronous call to a datastore writer process associated
%% with a key.
%% @end
%%--------------------------------------------------------------------
-spec call(ctx(), tp_key(), atom(), list()) -> term().
call(Ctx, Key, Function, Args) ->
    case call_async(Ctx, Key, Function, Args) of
        {{ok, Ref}, Pid} -> wait(Ref, Pid);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Multiplies requests handling error.
%% @end
%%--------------------------------------------------------------------
-spec multi_call(ctx(), tp_key(), atom(), list(), non_neg_integer()) ->
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
%% @equiv custom_call(Key, {handle, {Function, [Ctx | Args]}})
%% @end
%%--------------------------------------------------------------------
-spec call_async(ctx(), tp_key(), atom(), list()) ->
    {{ok, reference()}, pid()} | {error, term()}.
call_async(Ctx, Key, Function, Args) ->
    custom_call(Key, {handle, {Function, [Ctx | Args]}}).

%%--------------------------------------------------------------------
%% @doc
%% Performs custom call to datastore writer process. Returns answer and pid.
%% @end
%%--------------------------------------------------------------------
-spec custom_call(tp_key(), term()) -> {term(), pid()} | {error, term()}.
custom_call(Key, Request) ->
    Timeout = application:get_env(?CLUSTER_WORKER_APP_NAME,
        datastore_writer_request_queueing_timeout, timer:minutes(1)),
    Attempts = application:get_env(?CLUSTER_WORKER_APP_NAME,
        datastore_writer_request_queueing_attempts, 3),
    tp:call(?MODULE, [Key], Key, Request, Timeout, Attempts).

%%--------------------------------------------------------------------
%% @doc
%% Waits for a completion of asynchronous call to datastore writer.
%% @end
%%--------------------------------------------------------------------
-spec wait(reference(), pid()) -> term() | {error, timeout}.
wait(Ref, Pid) ->
    Timeout = application:get_env(?CLUSTER_WORKER_APP_NAME,
        datastore_writer_request_handling_timeout, timer:minutes(10)),
    receive
        {Ref, Response} -> Response
    after
        Timeout ->
            case erlang:is_process_alive(Pid) of
                true ->
                    wait(Ref, Pid);
                _ ->
                    {error, timeout}
            end
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
init([Key]) ->
    {ok, Pid} = datastore_cache_writer:start_link(self(), Key, [], []),
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
    terminate_msg_ref = MsgRef,
    cache_writer_pid = Pid
}) ->
    case gen_server:call(Pid, terminate, infinity) of
        ok -> {stop, normal, State};
        _ -> {noreply, schedule_terminate(State)}
    end;
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
    catch gen_server:call(Pid, {terminate, Requests}, infinity),
    tp_router:delete_process_size(self()),
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

    case application:get_env(?CLUSTER_WORKER_APP_NAME, tp_gc, on) of
        on ->
            erlang:garbage_collect();
        _ ->
            ok
    end,

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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets tp routing key.
%% @end
%%--------------------------------------------------------------------
-spec get_key(datastore:ctx(), datastore:key(), atom()) -> tp_key().
get_key(Ctx, Key, Type) ->
    RoutingKey = maps:get(routing_key, Ctx, Key),
    case application:get_env(?CLUSTER_WORKER_APP_NAME, aggregate_tp, false) of
        true ->
            case application:get_env(?CLUSTER_WORKER_APP_NAME,
                tp_space_size, 0) of
                0 ->
                    RoutingKey;
                Size ->
                    get_key_num(RoutingKey, Size)
            end;
        _ ->
            {Type, RoutingKey}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps key to area of key space.
%% @end
%%--------------------------------------------------------------------
-spec get_key_num(datastore:key(), non_neg_integer()) ->
    non_neg_integer().
get_key_num(Key, SpaceSize) when is_binary(Key) ->
    Id = binary:decode_unsigned(Key),
    Id rem SpaceSize + 1;
get_key_num(Key, SpaceSize) ->
    get_key_num(crypto:hash(md5, term_to_binary(Key)), SpaceSize).