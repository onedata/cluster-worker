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
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-record(pre_handshake_state, {
    peer_ip :: ip_utils:ip(),
    translator :: module()
}).

-record(state, {
    session_id = <<"">> :: gs_protocol:session_id(),
    protocol_version = 0 :: gs_protocol:protocol_version()
}).

-type state() :: #pre_handshake_state{} | #state{}.

-define(KEEPALIVE_INTERVAL, application:get_env(
    ?CLUSTER_WORKER_APP_NAME, graph_sync_websocket_keepalive, timer:seconds(30)
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
    kill/1
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
    {cowboy_websocket, Req, #pre_handshake_state{
        peer_ip = PeerIp,
        translator = Translator
    }}.


%%--------------------------------------------------------------------
%% @doc
%% Initialize timer between sending keepalives/ping frames.
%% @end
%%--------------------------------------------------------------------
-spec websocket_init(State :: state()) -> {ok, State :: state()}.
websocket_init(State) ->
    erlang:send_after(?KEEPALIVE_INTERVAL, self(), keepalive),
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
websocket_handle({text, Data}, #pre_handshake_state{peer_ip = PeerIp} = State) ->
    #pre_handshake_state{
        translator = Translator
    } = State,
    % If there was no handshake yet, expect only handshake messages
    {Response, NewState} = case decode_body(?BASIC_PROTOCOL, Data) of
        {ok, #gs_req{request = #gs_req_handshake{}} = Request} ->
            case gs_server:handshake(self(), Translator, Request, PeerIp) of
                {ok, Resp} ->
                    #gs_resp{
                        response = #gs_resp_handshake{
                            session_id = SessionId,
                            version = ProtoVersion
                        }} = Resp,
                    {Resp, #state{
                        session_id = SessionId,
                        protocol_version = ProtoVersion
                    }};
                {error, ErrMsg} ->
                    {ErrMsg, State}
            end;
        {ok, BadRequest} ->
            {gs_protocol:generate_error_response(
                BadRequest, ?ERROR_EXPECTED_HANDSHAKE_MESSAGE
            ), State};
        {error, _} = Error ->
            {gs_protocol:generate_error_push_message(Error), State}
    end,
    {ok, JSONMap} = gs_protocol:encode(?BASIC_PROTOCOL, Response),
    {reply, {text, json_utils:encode(JSONMap)}, NewState};

websocket_handle({text, Data}, State) ->
    #state{session_id = SessionId, protocol_version = ProtocolVersion} = State,
    case decode_body(ProtocolVersion, Data) of
        {ok, Requests} ->
            % process_request_async should not crash, but if it does,
            % cowboy will log the error.
            process_request_async(SessionId, Requests),
            {ok, State};
        {error, _} = Error ->
            ErrorMsg = gs_protocol:generate_error_push_message(Error),
            {ok, ErrorJSONMap} = gs_protocol:encode(ProtocolVersion, ErrorMsg),
            {reply, {text, json_utils:encode(ErrorJSONMap)}, State}
    end;

websocket_handle(ping, State) ->
    {ok, State};

websocket_handle(pong, State) ->
    {ok, State};

websocket_handle(Msg, State) ->
    ?warning("Unexpected frame in GS websocket handler: ~p", [Msg]),
    {ok, State}.


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
websocket_info(terminate, State) ->
    {stop, State};

websocket_info(keepalive, State) ->
    erlang:send_after(?KEEPALIVE_INTERVAL, self(), keepalive),
    {reply, ping, State};

websocket_info({push, Msg}, #state{protocol_version = ProtoVer} = State) ->
    try
        {ok, JSONMap} = gs_protocol:encode(ProtoVer, Msg),
        {reply, {text, json_utils:encode(JSONMap)}, State}
    catch
        Type:Message ->
            ?error_stacktrace(
                "Discarding GS message to client as "
                "it cannot be encoded - ~p:~p~nMessage: ~p", [
                    Type, Message, Msg
                ]),
            {ok, State}
    end;

websocket_info(Msg, State) ->
    ?warning("Unexpected message in GS websocket handler: ~p", [Msg]),
    {ok, State}.


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
terminate(_Reason, _Req, #state{session_id = SessionId}) ->
    ?debug("Graph Sync connection terminating, sessionId: ~s", [SessionId]),
    gs_server:cleanup_client_session(SessionId),
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Spawns a new process to process a request (or a group of processes in case of
%% multiple requests). After processing, the response is pushed to the client.
%% @end
%%--------------------------------------------------------------------
-spec process_request_async(gs_protocol:session_id(),
    gs_protocol:req_wrapper() | [gs_protocol:req_wrapper()]) -> ok.
process_request_async(SessionId, RequestList) when is_list(RequestList) ->
    lists:foreach(
        fun(Request) ->
            process_request_async(SessionId, Request)
        end, RequestList);
process_request_async(SessionId, Request) ->
    WebsocketPid = self(),
    spawn(fun() ->
        Response = try gs_server:handle_request(SessionId, Request) of
            {ok, Resp} ->
                gs_protocol:generate_success_response(Request, Resp);
            {error, _} = Error ->
                gs_protocol:generate_error_response(Request, Error)
        catch
            throw:{error, _} = Error ->
                gs_protocol:generate_error_response(Request, Error);
            Type:Message ->
                ?error_stacktrace(
                    "Unexpected error while handling graph_sync request - ~p:~p",
                    [Type, Message]
                ),
                gs_protocol:generate_error_response(
                    Request, ?ERROR_INTERNAL_SERVER_ERROR
                )
        end,
        push(WebsocketPid, Response)
    end),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Decodes a binary JSON message into gs request record(s).
%% @end
%%--------------------------------------------------------------------
-spec decode_body(gs_protocol:protocol_version(), Data :: binary()) ->
    {ok, gs_protocol:req_wrapper() | [gs_protocol:req_wrapper()]} | errors:error().
decode_body(ProtocolVersion, Data) ->
    try
        case json_utils:decode(Data) of
            #{<<"batch">> := BatchList} ->
                {ok, lists:map(fun(JSONMap) ->
                    {ok, Request} = gs_protocol:decode(ProtocolVersion, JSONMap),
                    Request
                end, BatchList)};
            JSONMap ->
                {ok, _Request} = gs_protocol:decode(ProtocolVersion, JSONMap)
        end
    catch
        _:_ ->
            ?ERROR_BAD_MESSAGE(Data)
    end.
