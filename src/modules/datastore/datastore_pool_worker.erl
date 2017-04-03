%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for requests aggregation and batch processing.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_pool_worker).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("modules/datastore/datastore_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    driver :: undefined | module(),
    action :: undefined | datastore_pool:action(),
    requests = [] :: [{
        {pid(), reference()},
        {model_behaviour:model_config(), datastore:document()}
    }]
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the transaction process.
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server2:start_link(?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the transaction process.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([]) ->
    process_flag(trap_exit, true),
    gen_server:cast(?DATASTORE_POOL_MANAGER, {register, self()}),
    {ok, #state{}}.

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
handle_call(Request, _From, #state{} = State) ->
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
handle_cast({flush, Bucket}, #state{
    driver = Driver,
    action = Action,
    requests = Requests
} = State) ->
    {From, ModelConfigAndDocs} = lists:unzip(Requests),
    Responses = try
        case Action of
            save_docs -> Driver:save_docs(Bucket, ModelConfigAndDocs);
            delete_doc -> lists:map(fun({ModelConfig, Doc}) ->
                Driver:delete_doc_sync(ModelConfig, Doc)
            end, ModelConfigAndDocs)
        end
    catch
        _:Reason -> [{error, Reason} || _ <- lists:seq(1, length(From))]
    end,
    lists:foreach(fun({{Pid, Ref}, Response}) ->
        Pid ! {Ref, Response}
    end, lists:zip(From, Responses)),
    gen_server:cast(?DATASTORE_POOL_MANAGER, {register, self()}),
    {noreply, State#state{requests = []}};
handle_cast({Pid, Ref, {Action, Driver, ModelConfig, Doc}}, #state{
    requests = Requests
} = State) ->
    {noreply, State#state{
        driver = Driver,
        action = Action,
        requests = [{{Pid, Ref}, {ModelConfig, Doc}} | Requests]
    }}.

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
    gen_server:cast(?DATASTORE_POOL_MANAGER, {unregister, self()}),
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
