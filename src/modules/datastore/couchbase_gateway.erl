%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for spawning and monitoring third party executable "sync_gateway".
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_gateway).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    port :: port(),
    port_ref :: reference(),
    db_node :: datastore:db_node(),
    gw_admin_port :: integer(),
    bucket :: datastore:bucket(),
    data = <<>> :: binary()
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
-spec start_link(datastore:db_node(), datastore:bucket()) ->
    {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(DbNode, Bucket) ->
    gen_server2:start_link(?MODULE, [DbNode, Bucket], []).

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
init([{DbHost, _} = DbNode, Bucket]) ->
    process_flag(trap_exit, true),
    PrivDir = code:priv_dir(?CLUSTER_WORKER_APP_NAME),
    GwExecutable = filename:join(PrivDir, "sync_gateway"),
    GwPort = get_random_port(couchbase_gateway_api_port_range),
    GwAdminPort = get_random_port(couchbase_gateway_admin_port_range),
    Port = erlang:open_port({spawn_executable, GwExecutable},
        [binary, stderr_to_stdout, {line, 4 * 1024}, {args, [
            "-bucket", Bucket,
            "-dbname", Bucket,
            "-url", <<"http://", DbHost/binary, ":8091">>,
            "-adminInterface", "127.0.0.1:" ++ integer_to_list(GwAdminPort),
            "-interface", ":" ++ integer_to_list(GwPort)
        ]}]
    ),
    couchbase_gateway_sup:register_gateway(DbNode, Bucket, GwAdminPort),
    schedule_healthcheck(timer:seconds(10)),
    {ok, #state{
        port = Port,
        port_ref = erlang:monitor(port, Port),
        db_node = DbNode,
        bucket = Bucket,
        gw_admin_port = GwAdminPort
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
handle_info(healthcheck, #state{gw_admin_port = GwAdminPort} = State) ->
    Server = couchbeam:server_connection("localhost", GwAdminPort),
    case couchbeam:server_info(Server) of
        {ok, _} ->
            schedule_healthcheck(timer:seconds(1)),
            {noreply, State};
        _ ->
            {stop, restart, State}
    end;
handle_info({Port, {data, {noeol, Data2}}}, #state{
    port = Port, data = Data
} = State) ->
    {noreply, State#state{data = <<Data/binary, Data2/binary>>}};
handle_info({Port, {data, {eol, Data2}}}, #state{
    port = Port, bucket = Bucket, data = Data
} = State) ->
    ?debug("[~p][~s] ~s", [?MODULE, Bucket, <<Data/binary, Data2/binary>>]),
    {noreply, State#state{data = <<>>}};
handle_info({'DOWN', Ref, _, _, Reason}, #state{port_ref = Ref} = State) ->
    {stop, {closed, Reason}, State};
handle_info({'EXIT', Port, Reason}, #state{port = Port} = State) ->
    {stop, {closed, Reason}, State};
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
terminate(Reason, #state{
    db_node = DbNode, bucket = Bucket, port = Port
} = State) ->
    couchbase_gateway_sup:unregister_gateway(DbNode, Bucket),
    try erlang:port_info(Port, os_pid) of
        {os_pid, OsPid} -> os:cmd("kill -9 " ++ integer_to_list(OsPid));
        undefined -> ok
    catch
        _:_ -> ok
    end,
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
%% Returns random port value in a range given by an application env.
%% @end
%%--------------------------------------------------------------------
-spec get_random_port(atom()) -> integer().
get_random_port(RangeEnv) ->
    {ok, {Min, Max}} = application:get_env(?CLUSTER_WORKER_APP_NAME, RangeEnv),
    crypto:rand_uniform(Min, Max).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules health check.
%% @end
%%--------------------------------------------------------------------
-spec schedule_healthcheck(timeout()) -> ok.
schedule_healthcheck(Delay) ->
    erlang:send_after(Delay, self(), healthcheck),
    ok.
