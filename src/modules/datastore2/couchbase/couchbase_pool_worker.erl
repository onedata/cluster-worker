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
-module(couchbase_pool_worker).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include_lib("ctool/include/logging.hrl").
-include("modules/datastore/datastore_models_def.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    pool :: pid(),
    worker :: pid()
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase pool worker.
%% @end
%%--------------------------------------------------------------------
-spec start_link(pid(), couchbase_driver:bucket(), couchbase_driver:db_host()) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(Pool, Bucket, DbHosts) ->
    gen_server2:start_link(?MODULE, [Pool, Bucket, DbHosts], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes CouchBase pool worker.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([Pool, Bucket, DbHosts]) ->
    Host = lists:foldl(fun(DbHost, Acc) ->
        <<Acc/binary, ";", DbHost/binary>>
    end, hd(DbHosts), tl(DbHosts)),
    {ok, Worker} = cberl_worker:start_link([
        {host, binary_to_list(Host)},
        {username, ""},
        {password, ""},
        {bucketname, binary_to_list(Bucket)},
        {transcoder, cberl_transcoder}
    ]),
    {ok, #state{
        pool = Pool,
        worker = Worker
    }}.

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
handle_cast({post, Requests}, #state{pool = Pool, worker = Worker} = State) ->
    lists:foreach(fun({Ref, From, Request}) ->
        Response = try
            handle_request(Worker, Request)
        catch
            Class:Reason -> {error, {Class, Reason, erlang:get_stacktrace()}}
        end,
        From ! {Ref, Response}
    end, Requests),
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
%% Handles request and returns response.
%% @end
%%--------------------------------------------------------------------
-spec handle_request(pid(), couchbase_pool:request()) ->
    couchbase_pool:response().
handle_request(Worker, {save, Key, Value}) ->
    gen_server2:call(Worker, {store, set, Key, Value, standard, 0, 0});
handle_request(Worker, {mget, Keys}) ->
    lists:map(fun
        ({_Key, {error, Reason}}) -> {error, Reason};
        ({_Key, _Cas, Value}) -> {ok, Value}
    end, gen_server2:call(Worker, {mget, Keys, 0, 0}));
handle_request(Worker, {remove, Key}) ->
    gen_server2:call(Worker, {remove, Key, 0});
handle_request(Worker, {update_counter, Key, Offset, Default}) ->
    case gen_server2:call(Worker, {arithmetic, Key, Offset, 0, 1, Default}) of
        {ok, _Cas, Value} -> {ok, binary_to_integer(Value)};
        {error, Reason} -> {error, Reason}
    end;
handle_request(Worker, {save_design_doc, DesignName, EJson}) ->
    Path = binary_to_list(<<"_design/", DesignName/binary>>),
    Body = binary_to_list(jiffy:encode(EJson)),
    ContentType = "application/json",
    Response = gen_server2:call(Worker, {http, Path, Body, ContentType, 2, 0}),
    parse_design_doc_response(Response);
handle_request(Worker, {delete_design_doc, DesignName}) ->
    Path = binary_to_list(<<"_design/", DesignName/binary>>),
    ContentType = "application/json",
    Response = gen_server2:call(Worker, {http, Path, "", ContentType, 3, 0}),
    parse_design_doc_response(Response);
handle_request(Worker, {query_view, DesignName, ViewName, Opts}) ->
    Path = <<"_design/", DesignName/binary, "/_view/", ViewName/binary>>,
    Path2 = <<Path/binary, (view_opts(Opts))/binary>>,
    ContentType = "application/json",
    case gen_server2:call(Worker, {http, Path2, "", ContentType, 0, 0}) of
        {ok, _Code, Response} ->
            case jiffy:decode(Response) of
                {[{<<"total_rows">>, _TotalRows}, {<<"rows">>, Rows}]} ->
                    {ok, {lists:map(fun({Row}) -> Row end, Rows)}};
                {[{<<"rows">>, Rows}]} ->
                    {ok, {lists:map(fun({Row}) -> Row end, Rows)}};
                {[{<<"error">>, Error}, {<<"reason">>, Reason}]} ->
                    {error, {Error, Reason}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Parses add/delete design document response.
%% @end
%%--------------------------------------------------------------------
-spec parse_design_doc_response({ok, non_neg_integer(), binary()} |
{error, term()}) -> ok | {error, Reason :: term()}.
parse_design_doc_response({ok, Code, _Response})
    when 200 =< Code andalso Code < 300 ->
    ok;
parse_design_doc_response({ok, _Code, Response}) ->
    case jiffy:decode(Response) of
        {[{<<"error">>, Error}, {<<"reason">>, Reason}]} ->
            {error, {Error, Reason}};
        _ ->
            {error, {unknown_error, Response}}
    end;
parse_design_doc_response({error, Reason}) ->
    {error, Reason}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts view arguments to query parameters.
%% @end
%%--------------------------------------------------------------------
-spec view_opts([couchbase_driver:view_opt()]) -> binary().
view_opts([]) ->
    <<>>;
view_opts([Opt | Opts]) ->
    lists:foldl(fun(Opt2, Prefix) ->
        <<Prefix/binary, "&", (view_opt(Opt2))/binary>>
    end, <<"?", (view_opt(Opt))/binary>>, Opts).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts view argument to query parameter.
%% @end
%%--------------------------------------------------------------------
-spec view_opt(couchbase_driver:view_opt()) -> binary().
view_opt({descending, true}) -> <<"descending=true">>;
view_opt({descending, false}) -> <<"descending=false">>;
view_opt({endkey, Key}) -> <<"endkey=", Key/binary>>;
view_opt({endkey_docid, Id}) -> <<"endkey_docid=", Id/binary>>;
view_opt({full_set, true}) -> <<"full_set=true">>;
view_opt({full_set, false}) -> <<"full_set=false">>;
view_opt({group, true}) -> <<"group=true">>;
view_opt({group, false}) -> <<"group=false">>;
view_opt({group_level, L}) -> <<"group_level=", (integer_to_binary(L))/binary>>;
view_opt({inclusive_end, true}) -> <<"inclusive_end=true">>;
view_opt({inclusive_end, false}) -> <<"inclusive_end=false">>;
view_opt({key, Key}) -> <<"key=", (jiffy:encode(Key))/binary>>;
view_opt({keys, Keys}) -> <<"keys=", (jiffy:encode(Keys))/binary>>;
view_opt({limit, L}) -> <<"limit=", (integer_to_binary(L))/binary>>;
view_opt({on_error, continue}) -> <<"on_error=continue">>;
view_opt({on_error, stop}) -> <<"on_error=stop">>;
view_opt({reduce, true}) -> <<"reduce=true">>;
view_opt({reduce, false}) -> <<"reduce=false">>;
view_opt({skip, Skip}) -> <<"skip=", (integer_to_binary(Skip))/binary>>;
view_opt({stale, ok}) -> <<"stale=ok">>;
view_opt({stale, false}) -> <<"stale=false">>;
view_opt({stale, update_after}) -> <<"stale=update_after">>;
view_opt({startkey, Key}) -> <<"startkey=", Key/binary>>;
view_opt({startkey_docid, Id}) -> <<"startkey_docid=", Id/binary>>.