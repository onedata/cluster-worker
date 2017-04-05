%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and represents single connection
%%% to the CouchBase database. It is responsible for requests aggregation and
%%% batch processing.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_pool_worker).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include_lib("ctool/include/logging.hrl").
-include("modules/datastore/datastore_models_def.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    pool :: pid()
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the datastore pool worker.
%% @end
%%--------------------------------------------------------------------
-spec start_link(pid()) -> {ok, pid()} | {error, Reason :: term()}.
start_link(Pool) ->
    gen_server2:start_link(?MODULE, [Pool], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the datastore pool worker.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([Pool]) ->
    {ok, #state{pool = Pool}}.

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
handle_cast({post, Requests}, #state{pool = Pool} = State) ->
    {SaveDocRequests, OtherRequests} = lists:partition(fun
        ({_, _, {save_doc, _}}) -> true;
        (_) -> false
    end, Requests),
    {SaveRefs, SaveDocs} = lists:foldl(fun
        ({Ref, From, {save_doc, [MC, Doc]}}, {Acc1, Acc2}) ->
            {[{Ref, From} | Acc1], [{MC, Doc} | Acc2]}
    end, {[], []}, SaveDocRequests),

    SaveDocResponses = try
        SaveDocs2 = lists:reverse(SaveDocs),
        case SaveDocs2 of
            [{MC, _} | _] ->
                couchdb_datastore_driver:save_docs_direct(MC, SaveDocs);
            _ ->
                []
        end
    catch
        _:Reason ->
            [{error, Reason} || _ <- lists:seq(1, length(SaveDocRequests))]
    end,
    lists:foreach(fun({{Ref, From}, Response}) ->
        From ! {Ref, Response}
    end, lists:zip(SaveRefs, SaveDocResponses)),

    lists:foreach(fun({Ref, From, {Function, Args}}) ->
        Response = try
            apply(couchdb_datastore_driver, Function, Args)
        catch
            _:Reason2 -> {error, Reason2}
        end,
        From ! {Ref, Response}
    end, OtherRequests),
    gen_server2:cast(Pool, {worker, self()}),
    {noreply, State};
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
