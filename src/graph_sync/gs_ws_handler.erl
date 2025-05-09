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


% API
-export([
    push/2,
    kill/1,
    keepalive_interval/0
]).

%% Cowboy WebSocket handler callbacks
-export([
    init/2,
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    terminate/3
]).


-record(pre_handshake_state, {
    conn_ref :: gs_server:conn_ref(),
    peer_ip :: ip_utils:ip(),
    cookies :: gs_protocol:cookies(),
    translator :: gs_server:translator()
}).

-record(state, {
    session_data :: gs_session:data(),
    worker_pool_tenant :: gs_worker_pool:tenant()
}).

-type state() :: #pre_handshake_state{} | #state{}.


-define(KEEPALIVE_INTERVAL_MILLIS, cluster_worker:get_env(graph_sync_websocket_keepalive, timer:seconds(15))).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Sends a request to websocket handler pid to push data to the client
%% (this can be a response to a request or a push message).
%% @end
%%--------------------------------------------------------------------
-spec push(gs_server:conn_ref(), gs_protocol:push_wrapper()) -> ok.
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
%%% Cowboy WebSocket handler callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Upgrades the protocol to WebSocket.
%% @end
%%--------------------------------------------------------------------
-spec init(Req :: cowboy_req:req(), Opts :: any()) ->
    {ok | cowboy_websocket, cowboy_req:req(), #pre_handshake_state{}, cowboy_websocket:opts()}.
init(Req, [Translator]) ->
    {PeerIp, _} = cowboy_req:peer(Req),
    Cookies = cowboy_req:parse_cookies(Req),
    {cowboy_websocket, Req, #pre_handshake_state{
        % best effort; this is before the connection is upgraded to WS
        % and the actual PID is not yet spawned
        conn_ref = self(),
        peer_ip = PeerIp,
        cookies = Cookies,
        translator = Translator
    }, #{active_n => gs_worker_pool:max_concurrent_requests()}}.


%%--------------------------------------------------------------------
%% @doc
%% Initialize timer between sending keepalives/ping frames.
%% @end
%%--------------------------------------------------------------------
-spec websocket_init(state()) -> {ok, state()}.
websocket_init(State) ->
    erlang:send_after(?KEEPALIVE_INTERVAL_MILLIS, self(), keepalive),
    {ok, State#pre_handshake_state{
        % update the connection PID to the actual one of the WS connection
        conn_ref = self()
    }}.


%%--------------------------------------------------------------------
%% @doc
%% Handles the data received from Websocket.
%% @end
%%--------------------------------------------------------------------
-spec websocket_handle(ping | pong | {text | ping, binary()}, state()) ->
    {cowboy_websocket:commands(), state()}.
websocket_handle({text, Data}, #pre_handshake_state{
    peer_ip = PeerIp, cookies = Cookies, translator = Translator} = State
) ->
    % if there was no handshake yet, expect only handshake messages
    {Response, NewState} = try
        case decode_request(State, Data) of
            {ok, #gs_req{request = #gs_req_handshake{} = HandshakeReq} = ReqWrapper} ->
                ClientAuth = HandshakeReq#gs_req_handshake.auth,
                case gs_server:handshake(self(), Translator, PeerIp, Cookies, HandshakeReq) of
                    {ok, SessionData, HandshakeResp} ->
                        ConnectedState = #state{
                            session_data = SessionData,
                            worker_pool_tenant = gs_worker_pool:new_tenant()
                        },
                        gs_verbose_logger:report_handshake_success(ClientAuth, PeerIp, Cookies, SessionData),
                        {gs_protocol:generate_success_response(ReqWrapper, HandshakeResp), ConnectedState};
                    HandshakeError ->
                        gs_verbose_logger:report_handshake_failure(ClientAuth, PeerIp, Cookies, HandshakeError),
                        {gs_protocol:generate_error_response(ReqWrapper, HandshakeError), State}
                end;
            {ok, BadRequest} ->
                WrongMessageError = ?ERR_EXPECTED_HANDSHAKE_MESSAGE(?err_ctx()),
                gs_verbose_logger:report_handshake_failure(undefined, PeerIp, Cookies, WrongMessageError),
                {gs_protocol:generate_error_response(BadRequest, WrongMessageError), State};
            {error, _} = DecodeError ->
                gs_verbose_logger:report_handshake_failure(undefined, PeerIp, Cookies, DecodeError),
                {gs_protocol:generate_error_push_message(DecodeError), State}
        end
    catch Class:Reason:Stacktrace ->
        UnexpectedError = ?examine_exception(Class, Reason, Stacktrace),
        gs_verbose_logger:report_handshake_failure(undefined, PeerIp, Cookies, UnexpectedError),
        {gs_protocol:generate_error_push_message(UnexpectedError), State}
    end,
    {[{text, encode_message(State, Response)}], NewState};

websocket_handle({text, Data}, #state{
    session_data = SessionData,
    worker_pool_tenant = WPTenant
} = State) ->
    _ResponseMsg = case decode_request(State, Data) of
        {ok, Request} ->
            % the result will be sent to this process as a message ?GS_WORKER_POOL_JOB_OUTCOME(Outcome)
            {ThrottlingRecommendation, UpdatedWPTenant} = gs_worker_pool:post_job(WPTenant, SessionData, Request),
            Commands = case ThrottlingRecommendation of
                start_throttling -> [{active, false}];
                resume_processing -> []
            end,
            {Commands, State#state{worker_pool_tenant = UpdatedWPTenant}};
        {error, _} = Error ->
            PushErrorMsg = gs_protocol:generate_error_push_message(Error),
            gs_verbose_logger:report_message_pushed(SessionData, PushErrorMsg),
            {[{text, encode_message(State, PushErrorMsg)}], State}
    end;

websocket_handle(ping, State) ->
    {[], State};

websocket_handle({ping, _Payload}, State) ->
    {[], State};

websocket_handle(pong, #pre_handshake_state{} = State) ->
    {[], State};

websocket_handle(pong, #state{session_data = SessionData} = State) ->
    % pongs are received in response to the keepalive pings sent to the client
    % (see 'keepalive' periodical message)
    gs_server:report_heartbeat(SessionData),
    gs_verbose_logger:report_heartbeat(SessionData),
    {[], State};

websocket_handle(Msg, State) ->
    ?warning("Unexpected frame in GS websocket handler: ~tp", [Msg]),
    {[], State}.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when a message is sent to the process handling
%% the connection.
%% @end
%%--------------------------------------------------------------------
-spec websocket_info(term(), state()) -> {cowboy_websocket:commands(), state()}.
websocket_info(?GS_WORKER_POOL_JOB_OUTCOME(_) = Outcome, #state{
    session_data = SessionData,
    worker_pool_tenant = WorkerPoolTenant
} = State) ->
    {ResponseMessage, {ThrottlingRecommendation, UpdatedWorkerPoolTenant}} = gs_worker_pool:process_outcome(
        WorkerPoolTenant, SessionData, Outcome
    ),
    ReplyCommand = [{text, encode_message(State, ResponseMessage)}],

    ActiveCommand = case ThrottlingRecommendation of
        resume_processing -> [{active, true}];
        start_throttling -> []
    end,

    {ActiveCommand ++ ReplyCommand, State#state{worker_pool_tenant = UpdatedWorkerPoolTenant}};

websocket_info(keepalive, #state{session_data = SessionData, worker_pool_tenant = WPTenant} = State) ->
    % the keepalive timer is also used to periodically check for stale requests
    {ResponseMessages, {ThrottlingRecommendation, UpdatedWPTenant}} = gs_worker_pool:prune_stale_requests(
        WPTenant, SessionData
    ),
    ReplyCommands = lists:map(fun(ResponseMessage) ->
        {text, encode_message(State, ResponseMessage)}
    end, ResponseMessages),

    ActiveCommand = case ThrottlingRecommendation of
        resume_processing -> [{active, true}];
        start_throttling -> []
    end,

    erlang:send_after(?KEEPALIVE_INTERVAL_MILLIS, self(), keepalive),
    {ActiveCommand ++ ReplyCommands ++ [ping], State#state{worker_pool_tenant = UpdatedWPTenant}};

websocket_info({push, Msg}, State) ->
    gs_verbose_logger:report_message_pushed(State#state.session_data, Msg),
    {[{text, encode_message(State, Msg)}], State};

websocket_info(terminate, State) ->
    {[close], State};

websocket_info(Msg, State) ->
    ?warning("Unexpected message in GS websocket handler: ~tp", [Msg]),
    {[], State}.


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
terminate(_Reason, _Req, #state{session_data = SessionData}) ->
    gs_server:cleanup_session(SessionData),
    gs_verbose_logger:report_connection_terminated(SessionData),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec decode_request(state(), Data :: binary()) -> {ok, gs_protocol:req_wrapper()} | errors:error().
decode_request(State, Data) ->
    ProtocolVersion = protocol_version(State),
    try
        JSONMap = json_utils:decode(Data),
        gs_protocol:decode(ProtocolVersion, JSONMap)
    catch
        Class:Reason:Stacktrace ->
            gs_verbose_logger:report_cannot_decode_request(State#state.session_data, Data, Class, Reason, Stacktrace),
            ?ERR_BAD_MESSAGE(?err_ctx(), Data)
    end.


%% @private
-spec encode_message(state(), gs_protocol:resp_wrapper() | gs_protocol:push_wrapper()) ->
    binary().
encode_message(State, Message) ->
    ProtocolVersion = protocol_version(State),
    {ok, JsonPayload} = case gs_protocol:encode(ProtocolVersion, Message) of
        {ok, Encoded} ->
            {ok, Encoded};
        ?ERR_BAD_MESSAGE(BadMessage) ->
            ConnRef = conn_ref(State),
            Error = ?report_internal_server_error(?autoformat_with_msg(
                "Discarding GS message to client as it cannot be encoded",
                [ConnRef, BadMessage]
            )),
            ErrorPushMsg = gs_protocol:generate_error_push_message(Error),
            gs_verbose_logger:report_message_pushed(State#state.session_data, ErrorPushMsg),
            gs_protocol:encode(ProtocolVersion, ErrorPushMsg)
    end,
    json_utils:encode(JsonPayload).


%% @private
-spec protocol_version(state()) -> gs_protocol:protocol_version().
protocol_version(#pre_handshake_state{}) ->
    % handshake messages do not change between versions
    ?BASIC_PROTOCOL;
protocol_version(#state{session_data = #gs_session{protocol_version = ProtocolVersion}}) ->
    ProtocolVersion.


%% @private
-spec conn_ref(state()) -> gs_server:conn_ref().
conn_ref(#pre_handshake_state{conn_ref = ConnRef}) ->
    ConnRef;
conn_ref(#state{session_data = #gs_session{conn_ref = ConnRef}}) ->
    ConnRef.


