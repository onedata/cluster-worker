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
    {Requesters, Requests2} = split_requests(Requests),
    Requests3 = aggregate_save_requests(Requests2),
    Responses = handle_requests(Requests3, []),
    send_responses(Requesters, Responses),
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Splits requests into requesters and actual requests.
%% @end
%%--------------------------------------------------------------------
-spec split_requests([{reference(), pid(), term()}]) ->
    {[{reference(), pid()}], [{reference(), term()}]}.
split_requests(Requests) ->
    {Requesters, Requests2} = lists:foldl(fun
        ({Ref, From, Requests}, {Acc1, Acc2}) ->
            {[{Ref, From} | Acc1], [{Ref, Requests} | Acc2]}
    end, {[], []}, Requests),
    {lists:reverse(Requesters), lists:reverse(Requests2)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Aggregates save requests.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_save_requests([{reference(), term()}]) ->
    [{reference() | [reference()], term()}].
aggregate_save_requests(Requests) ->
    {SaveRequests, OtherRequests} = lists:partition(fun
        ({_, {save_doc, _}}) -> true;
        (_) -> false
    end, Requests),
    {Refs, Docs} = lists:foldl(fun({Ref, {save_doc, [MC, Doc]}}, {Acc1, Acc2}) ->
        {[Ref | Acc1], [{MC, Doc} | Acc2]}
    end, {[], []}, SaveRequests),
    Refs2 = lists:reverse(Refs),
    Docs2 = lists:reverse(Docs),
    case Docs2 of
        [{MC, _} | _] ->
            [{Refs2, {save_docs_direct, [MC, Docs2]}} | OtherRequests];
        _ ->
            OtherRequests
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles requests and returns responses.
%% @end
%%--------------------------------------------------------------------
-spec handle_requests([{reference() | [reference()], term()}],
    [{reference(), term()}]) -> [{reference(), term()}].
handle_requests([], Responses) ->
    Responses;
handle_requests([{[_ | _] = Refs, {Function, Args}} | Requests], Responses) ->
    Responses2 = try
        apply(couchdb_datastore_driver, Function, Args)
    catch
        _:Reason -> [{error, Reason} || _ <- lists:seq(1, length(Refs))]
    end,
    Responses3 = lists:zip(Refs, Responses2),
    handle_requests(Requests, Responses ++ Responses3);
handle_requests([{Ref, {Function, Args}} | Requests], Responses) ->
    Response = try
        apply(couchdb_datastore_driver, Function, Args)
    catch
        _:Reason -> {error, Reason}
    end,
    handle_requests(Requests, [{Ref, Response} | Responses]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends responses to requesters.
%% @end
%%--------------------------------------------------------------------
-spec send_responses([{reference(), pid()}], [{reference(), term()}]) -> ok.
send_responses(Requesters, Responses) ->
    lists:foreach(fun({Ref, From}) ->
        {Ref, Response} = lists:keyfind(Ref, 1, Responses),
        From ! {Ref, Response}
    end, Requesters).