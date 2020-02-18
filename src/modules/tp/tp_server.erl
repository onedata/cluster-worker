%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is a wrapper around gen_server behaviour, which ensures that for
%%% a custom key there will be only one process at the time that handles
%%% requests. Singularity guarantees are kept only on per Erlang VM bases, i.e.
%%% there might be two processes associated with the same key running
%%% simultaneously on two different Erlang nodes.
%%% @end
%%%-------------------------------------------------------------------
-module(tp_server).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/tp/tp.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    module :: module(),
    key :: tp:key(),
    state :: tp:state()
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
-spec start_link(module(), tp:args(), tp:key()) ->
    {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(Module, Args, Key) ->
    gen_server:start_link(?MODULE, [Module, Args, Key], []).

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
init([Module, Args, Key]) ->
    Self = self(),
    case tp_router:create(Key, Self) of
        ok ->
            process_flag(trap_exit, true),
            {ok, State} = Module:init(Args),
            tp_router:report_process_initialized(Key, Self),
            {ok, #state{module = Module, key = Key, state = State}};
        {error, already_exists} ->
            ignore;
        {error, Reason} ->
            {stop, Reason}
    end.

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
handle_call(Request, From, #state{module = Module, state = State} = S) ->
    case Module:handle_call(Request, From, State) of
        {reply, Reply, State2} ->
            {reply, Reply, S#state{state = State2}};
        {reply, Reply, State2, Timeout} ->
            {reply, Reply, S#state{state = State2}, Timeout};
        {noreply, State2} ->
            {noreply, S#state{state = State2}};
        {noreply, State2, Timeout} ->
            {noreply, S#state{state = State2}, Timeout};
        {stop, Reason, Reply, State2} ->
            {stop, Reason, Reply, S#state{state = State2}};
        {stop, Reason, State2} ->
            {stop, Reason, S#state{state = State2}}
    end.
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
handle_cast(Request, #state{module = Module, state = State} = S) ->
    case Module:handle_cast(Request, State) of
        {noreply, State2} ->
            {noreply, S#state{state = State2}};
        {noreply, State2, Timeout} ->
            {noreply, S#state{state = State2}, Timeout};
        {stop, Reason, State2} ->
            {stop, Reason, S#state{state = State2}}
    end.

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
handle_info(Info, #state{module = Module, state = State} = S) ->
    case Module:handle_info(Info, State) of
        {noreply, State2} ->
            {noreply, S#state{state = State2}};
        {noreply, State2, Timeout} ->
            {noreply, S#state{state = State2}, Timeout};
        {stop, Reason, State2} ->
            {stop, Reason, S#state{state = State2}}
    end.

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
terminate(Reason, #state{key = Key, module = Module, state = State}) ->
    Module:terminate(Reason, State),
    tp_router:delete(Key).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(OldVsn, #state{module = Module, state = State} = S, Extra) ->
    case Module:code_change(OldVsn, State, Extra) of
        {ok, State2} -> {ok, S#state{state = State2}};
        {error, Reason} -> {error, Reason}
    end.
