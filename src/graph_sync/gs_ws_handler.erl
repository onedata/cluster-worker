%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements Graph Sync websocket handler that handles server
%%% endpoint of graph sync channel.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_ws_handler).
-author("Lukasz Opiola").

-behaviour(cowboy_websocket).

-include("global_definitions.hrl").
-include("graph_sync/graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-record(pre_handshake_state, {
    peer_ip :: ip_utils:ip(),
    cookies :: gs_protocol:cookies(),
    translator :: gs_server:translator()
}).

-type state() :: #pre_handshake_state{} | #gs_session{}.

-define(KEEPALIVE_INTERVAL_MILLIS, application:get_env(
    ?CLUSTER_WORKER_APP_NAME, graph_sync_websocket_keepalive, timer:seconds(15)
)).

-define(BATCH_PARALLELISM, application:get_env(
    ?CLUSTER_WORKER_APP_NAME, graph_sync_batch_parallelism, 10
)).

%% Cowboy WebSocket handler callbacks
-export([
    init/2,
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    terminate/3
]).
% API
-export([
    push/2,
    kill/1,
    keepalive_interval/0
]).


%%%===================================================================
%%% Cowboy WebSocket handler callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Upgrades the protocol to WebSocket.
%% @end
%%--------------------------------------------------------------------
-spec init(Req :: cowboy_req:req(), Opts :: any()) ->
    {ok | cowboy_websocket, cowboy_req:req(), #pre_handshake_state{}}.
init(Req, [Translator]) ->
    {PeerIp, _} = cowboy_req:peer(Req),
    Cookies = cowboy_req:parse_cookies(Req),
    {cowboy_websocket, Req, #pre_handshake_state{
        peer_ip = PeerIp,
        cookies = Cookies,
        translator = Translator
    }}.


%%--------------------------------------------------------------------
%% @doc
%% Initialize timer between sending keepalives/ping frames.
%% @end
%%--------------------------------------------------------------------
-spec websocket_init(state()) -> {ok, state()}.
websocket_init(State) ->
    erlang:send_after(?KEEPALIVE_INTERVAL_MILLIS, self(), keepalive),
    {ok, State}.


%%--------------------------------------------------------------------
%% @doc
%% Handles the data received from Websocket.
%% @end
%%--------------------------------------------------------------------
-spec websocket_handle(InFrame, State) ->
    {ok, State} | {ok, State, hibernate} |
    {reply, OutFrame | [OutFrame], State} |
    {reply, OutFrame | [OutFrame], State, hibernate} |
    {stop, State} when
    InFrame :: {text | binary | ping | pong, binary()},
    State :: state(),
    OutFrame :: cow_ws:frame().
websocket_handle({text, Data}, #pre_handshake_state{
    peer_ip = PeerIp, cookies = Cookies, translator = Translator} = State
) ->
    % If there was no handshake yet, expect only handshake messages
    {Response, NewState} = try
        case decode_body(?BASIC_PROTOCOL, Data) of
            {ok, #gs_req{request = #gs_req_handshake{}} = Request} ->
                case gs_server:handshake(self(), Translator, Request, PeerIp, Cookies) of
                    {ok, SessionData, HandshakeResp} ->
                        {gs_protocol:generate_success_response(Request, HandshakeResp), SessionData};
                    ErrMsg ->
                        {gs_protocol:generate_error_response(Request, ErrMsg), State}
                end;
            {ok, BadRequest} ->
                {gs_protocol:generate_error_response(
                    BadRequest, ?ERR_EXPECTED_HANDSHAKE_MESSAGE(?err_ctx())
                ), State};
            {error, _} = Error1 ->
                {gs_protocol:generate_error_push_message(Error1), State}
        end
    catch Class:Reason:Stacktrace ->
        Error2 = ?examine_exception(Class, Reason, Stacktrace),
        {gs_protocol:generate_error_push_message(Error2), State}
    end,
    {ok, JSONMap} = gs_protocol:encode(?BASIC_PROTOCOL, Response),
    {reply, {text, json_utils:encode(JSONMap)}, NewState};

websocket_handle({text, Data}, SessionData = #gs_session{protocol_version = ProtoVersion}) ->
    ResponseMsg = case decode_body(ProtoVersion, Data) of
        {ok, Request} ->
            process_request(SessionData, Request);
        {error, _} = Error ->
            gs_protocol:generate_error_push_message(Error)
    end,
    {ok, JSONMap} = gs_protocol:encode(ProtoVersion, ResponseMsg),
    {reply, {text, json_utils:encode(JSONMap)}, SessionData};

websocket_handle(ping, State) ->
    {ok, State};

websocket_handle({ping, _Payload}, State) ->
    {ok, State};

websocket_handle(pong, #pre_handshake_state{} = State) ->
    {ok, State};

websocket_handle(pong, #gs_session{} = SessionData) ->
    % pongs are received in response to the keepalive pings sent to the client
    % (see 'keepalive' periodical message)
    gs_server:report_heartbeat(SessionData),
    {ok, SessionData};

websocket_handle(Msg, SessionData) ->
    ?warning("Unexpected frame in GS websocket handler: ~tp", [Msg]),
    {ok, SessionData}.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when a message is sent to the process handling
%% the connection.
%% @end
%%--------------------------------------------------------------------
-spec websocket_info(Info, State) ->
    {ok, State} | {ok, State, hibernate} |
    {reply, OutFrame | [OutFrame], State} |
    {reply, OutFrame | [OutFrame], State, hibernate} |
    {stop, State} when
    Info :: any(),
    State :: state(),
    OutFrame :: cow_ws:frame().
websocket_info(terminate, SessionData) ->
    {stop, SessionData};

websocket_info(keepalive, SessionData) ->
    erlang:send_after(?KEEPALIVE_INTERVAL_MILLIS, self(), keepalive),
    {reply, ping, SessionData};

websocket_info({push, Msg}, #gs_session{protocol_version = ProtoVer} = SessionData) ->
    try
        {ok, JSONMap} = gs_protocol:encode(ProtoVer, Msg),
        {reply, {text, json_utils:encode(JSONMap)}, SessionData}
    catch
        Type:Message:Stacktrace ->
            ?error_stacktrace(
                "Discarding GS message to client as "
                "it cannot be encoded - ~tp:~tp~nMessage: ~tp", [
                    Type, Message, Msg
                ], Stacktrace),
            {ok, SessionData}
    end;

websocket_info(Msg, SessionData) ->
    ?warning("Unexpected message in GS websocket handler: ~tp", [Msg]),
    {ok, SessionData}.


%%--------------------------------------------------------------------
%% @doc
%% Performs any necessary cleanup.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason, Req, State) -> ok when
    Reason :: normal | stop | timeout |
    remote | {remote, cow_ws:close_code(), binary()} |
    {error, badencoding | badframe | closed | atom()} |
    {crash, error | exit | throw, any()},
    Req :: cowboy_req:req(),
    State :: state().
terminate(_Reason, _Req, #pre_handshake_state{}) ->
    ok;
terminate(_Reason, _Req, #gs_session{id = SessionId} = SessionData) ->
    ?debug("Graph Sync connection terminating, sessionId: ~ts", [SessionId]),
    gs_server:cleanup_session(SessionData),
    ok.

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Sends a request to websocket handler pid to push data to the client
%% (this can be a response to a request or a push message).
%% @end
%%--------------------------------------------------------------------
-spec push(gs_server:conn_ref(), gs_protocol:resp_wrapper() | gs_protocol:push_wrapper()) -> ok.
push(WebsocketPid, Msg) when WebsocketPid /= undefined ->
    WebsocketPid ! {push, Msg},
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Terminates websocket connection by given connection ref.
%% @end
%%--------------------------------------------------------------------
-spec kill(gs_server:conn_ref()) -> ok.
kill(WebsocketPid) when WebsocketPid /= undefined ->
    WebsocketPid ! terminate,
    ok.


-spec keepalive_interval() -> time:seconds().
keepalive_interval() ->
    ?KEEPALIVE_INTERVAL_MILLIS div 1000.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec decode_body(gs_protocol:protocol_version(), Data :: binary()) ->
    {ok, gs_protocol:req_wrapper()} | errors:error().
decode_body(ProtocolVersion, Data) ->
    try
        JSONMap = json_utils:decode(Data),
        gs_protocol:decode(ProtocolVersion, JSONMap)
    catch
        Class:Reason:Stacktrace ->
            ?debug_exception(Class, Reason, Stacktrace),
            ?ERR_BAD_MESSAGE(?err_ctx(), Data)
    end.


%% @private
% TODO VFS-12568 consider processing all GS requests on pool of processes, common for all clients
-spec process_request(gs_session:data(), gs_protocol:req_wrapper()) -> gs_protocol:resp_wrapper().
process_request(SessionData, #gs_req{request = #gs_req_batch{requests = Requests}} = BatchRequest) ->
    Result = ?catch_exceptions(
        % TODO VFS-12568 run this on a pool of processes, common for all clients
        {ok, lists_utils:pmap(fun(Request) ->
            process_request(SessionData, Request)
        end, Requests, ?BATCH_PARALLELISM)}
    ),
    case Result of
        {ok, BatchResponses} ->
            gs_protocol:generate_success_response(BatchRequest, #gs_resp_batch{responses = BatchResponses});
        {error, _} = Error ->
            gs_protocol:generate_error_response(BatchRequest, Error)
    end;
process_request(SessionData, Request) ->
    case gs_server:handle_request(SessionData, Request) of
        {ok, Resp} ->
            gs_protocol:generate_success_response(Request, Resp);
        {error, _} = Error ->
            gs_protocol:generate_error_response(Request, Error)
    end.
