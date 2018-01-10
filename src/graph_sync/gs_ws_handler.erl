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

-include_lib("ctool/include/api_errors.hrl").
-include("graph_sync/graph_sync.hrl").
-include_lib("ctool/include/logging.hrl").

-record(pre_handshake_state, {
    % This is an opaque term to the gs handler
    client :: term(),
    translator :: module()
}).

-record(state, {
    session_id = <<"">> :: gs_protocol:session_id(),
    protocol_version = 0 :: gs_protocol:protocol_version()
}).

-type state() :: #pre_handshake_state{} | #state{}.

%% Cowboy WebSocket handler callbacks
-export([
    init/3,
    websocket_init/3,
    websocket_handle/3,
    websocket_info/3,
    websocket_terminate/3
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
-spec init({TransportName, ProtocolName}, Req, Opts) ->
    {upgrade, protocol, cowboy_websocket} when
    TransportName :: tcp | ssl | atom(),
    ProtocolName :: http | atom(),
    Req :: cowboy_req:req(),
    Opts :: any().
init(_Protocol, _Req, _Opts) ->
    {upgrade, protocol, cowboy_websocket}.

%%--------------------------------------------------------------------
%% @doc
%% Initializes the state for a session.
%% @end
%%--------------------------------------------------------------------
-spec websocket_init(TransportName, Req, Opts) ->
    {ok, Req, State} | {ok, Req, State, hibernate} |
    {ok, Req, State, Timeout} | {ok, Req, State, Timeout, hibernate} |
    {shutdown, Req} when
    TransportName :: tcp | ssl | atom(),
    Req :: cowboy_req:req(),
    Opts :: any(),
    State :: state(),
    Timeout :: timeout().
websocket_init(_Transport, Req, [Translator]) ->
    case gs_server:authorize(Req) of
        {ok, Client} ->
            {ok, Req, #pre_handshake_state{
                client = Client, translator = Translator
            }};
        ?ERROR_UNAUTHORIZED ->
            {ok, NewReq} = cowboy_req:reply(401, Req),
            {shutdown, NewReq}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles the data received from Websocket.
%% @end
%%--------------------------------------------------------------------
-spec websocket_handle(InFrame, Req, State) ->
    {ok, Req, State} | {ok, Req, State, hibernate} |
    {reply, OutFrame | [OutFrame], Req, State} |
    {reply, OutFrame | [OutFrame], Req, State, hibernate} |
    {shutdown, Req, State} when
    InFrame :: {text | binary | ping | pong, binary()},
    Req :: cowboy_req:req(),
    State :: state(),
    OutFrame :: cowboy_websocket:frame().
websocket_handle({text, Data}, Req, #pre_handshake_state{} = State) ->
    #pre_handshake_state{client = Client, translator = Translator} = State,
    % If there was no handshake yet, expect only handshake messages
    {Response, NewState} = case decode_body(?BASIC_PROTOCOL, Data) of
        {ok, #gs_req{request = #gs_req_handshake{}} = Request} ->
            case gs_server:handshake(Client, self(), Translator, Request) of
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
    {reply, {text, json_utils:encode_map(JSONMap)}, Req, NewState};

websocket_handle({text, Data}, Req, State) ->
    #state{session_id = SessionId, protocol_version = ProtocolVersion} = State,
    case decode_body(ProtocolVersion, Data) of
        {ok, Requests} ->
            % process_request_async should not crash, but if it does,
            % cowboy will log the error.
            process_request_async(SessionId, Requests),
            {ok, Req, State};
        {error, _} = Error ->
            ErrorMsg = gs_protocol:generate_error_push_message(Error),
            {ok, ErrorJSONMap} = gs_protocol:encode(ProtocolVersion, ErrorMsg),
            {reply, {text, json_utils:encode_map(ErrorJSONMap)}, Req, State}
    end;

websocket_handle(Msg, Req, State) ->
    ?warning("Unexpected frame in GS websocket handler: ~p", [Msg]),
    {ok, Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Callback called when a message is sent to the process handling
%% the connection.
%% @end
%%--------------------------------------------------------------------
-spec websocket_info(Info, Req, State) ->
    {ok, Req, State} | {ok, Req, State, hibernate} |
    {reply, OutFrame | [OutFrame], Req, State} |
    {reply, OutFrame | [OutFrame], Req, State, hibernate} |
    {shutdown, Req, State} when
    Info :: any(),
    Req :: cowboy_req:req(),
    State :: state(),
    OutFrame :: cowboy_websocket:frame().
websocket_info(terminate, Req, State) ->
    {shutdown, Req, State};

websocket_info({push, Msg}, Req, #state{protocol_version = ProtoVer} = State) ->
    try
        {ok, JSONMap} = gs_protocol:encode(ProtoVer, Msg),
        {reply, {text, json_utils:encode_map(JSONMap)}, Req, State}
    catch
        Type:Message ->
            ?error_stacktrace(
                "Discarding GS message to client as "
                "it cannot be encoded - ~p:~p~nMessage: ~p", [
                Type, Message, Msg
            ]),
            {ok, Req, State}
    end;

websocket_info(Msg, Req, State) ->
    ?warning("Unexpected message in GS websocket handler: ~p", [Msg]),
    {ok, Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Performs any necessary cleanup.
%% @end
%%--------------------------------------------------------------------
-spec websocket_terminate(Reason, Req, State) -> ok when
    Reason :: {normal, shutdown | timeout} | {remote, closed} |
    {remote, cowboy_websocket:close_code(), binary()} |
    {error, badencoding | badframe | closed | atom()},
    Req :: cowboy_req:req(),
    State :: state().
websocket_terminate(_Reason, _Req, #pre_handshake_state{}) ->
    ok;
websocket_terminate(_Reason, _Req, #state{session_id = SessionId}) ->
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
%% @doc
%% Decodes a binary JSON message into gs request record(s).
%% @end
%%--------------------------------------------------------------------
-spec decode_body(gs_protocol:protocol_version(), Data :: binary()) ->
    {ok, gs_protocol:req_wrapper() | [gs_protocol:req_wrapper()]} | gs_protocol:error().
decode_body(ProtocolVersion, Data) ->
    try
        case json_utils:decode_map(Data) of
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
