%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements Graph Sync client, based on WebSocket protocol.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_client).
-author("Lukasz Opiola").

-behaviour(websocket_client_handler_behaviour).

-include("global_definitions.hrl").
-include("graph_sync/graph_sync.hrl").
-include("timeouts.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

% Client state record
-record(state, {
    handshake_status = false :: false | {pending, MsgId :: binary(), pid()} | true,
    session_id :: undefined | gs_protocol:session_id(),
    protocol_version = ?BASIC_PROTOCOL :: gs_protocol:protocol_version(),
    identity = ?SUB(nobody) :: aai:subject(),
    handshake_attributes = #{} :: gs_protocol:handshake_attributes(),
    push_callback = fun(_) -> ok end :: push_callback(),
    promises = #{} :: #{binary() => pid()}
}).

-type state() :: #state{}.

-define(KEEPALIVE_INTERVAL, application:get_env(
    ?CLUSTER_WORKER_APP_NAME, graph_sync_websocket_keepalive, timer:seconds(30)
)).

-type push_callback() :: fun((gs_protocol:push()) -> any()).
% Reference to GS client instance
-type client_ref() :: pid().
-export_type([push_callback/0, client_ref/0]).

-export([init/2, websocket_handle/3, websocket_info/3, websocket_terminate/3]).
-export([start_link/4, start_link/5, kill/1]).
-export([
    rpc_request/3,
    graph_request/3, graph_request/4, graph_request/5, graph_request/6,
    unsub_request/2,
    sync_request/2,
    async_request/2
]).

%%%===================================================================
%%% Start / stop API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv start_link(URL, Auth, SupportedVersions, PushCallback, []).
%% @end
%%--------------------------------------------------------------------
-spec start_link(URL :: string() | binary(), gs_protocol:client_auth(),
    SupportedVersions :: [gs_protocol:protocol_version()], push_callback()) ->
    {ok, client_ref(), gs_protocol:handshake_resp()} | errors:error().
start_link(URL, Auth, SupportedVersions, PushCallback) ->
    start_link(URL, Auth, SupportedVersions, PushCallback, []).


%%--------------------------------------------------------------------
%% @doc
%% Starts an instance of GS client and immediately performs a handshake. The
%% calling process is blocked until the procedure finishes, or there is a
%% timeout while waiting for initialization ACK.
%% Allows to pass options to websocket_client:start_link.
%% @end
%%--------------------------------------------------------------------
-spec start_link(URL :: string() | binary(), gs_protocol:client_auth(),
    SupportedVersions :: [gs_protocol:protocol_version()], push_callback(),
    Opts :: list()) ->
    {ok, client_ref(), gs_protocol:handshake_resp()} | errors:error().
start_link(URL, Auth, SupportedVersions, PushCallback, Opts) when is_binary(URL) ->
    start_link(binary_to_list(URL), Auth, SupportedVersions, PushCallback, Opts);
start_link(URL, Auth, SupportedVersions, PushCallback, Opts) ->
    case websocket_client:start_link(URL, ?MODULE, [], Opts) of
        {ok, Pid} ->
            Pid ! {init, self(), SupportedVersions, Auth, PushCallback},
            receive
                {init_result, #gs_resp_handshake{} = HandshakeResponse} ->
                    {ok, Pid, HandshakeResponse};
                {init_result, {error, _} = Error} ->
                    Error
            after
                ?GS_CLIENT_HANDSHAKE_TIMEOUT ->
                    Pid ! terminate,
                    ?ERROR_TIMEOUT
            end;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Terminates given GS client instance by sending termination message.
%% @end
%%--------------------------------------------------------------------
-spec kill(client_ref()) -> ok.
kill(ClientRef) ->
    ClientRef ! terminate,
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when connection is established.
%% @end
%%--------------------------------------------------------------------
-spec init([term()], websocket_req:req()) ->
    {ok, state()} | {ok, state(), Keepalive :: integer()}.
init([], _) ->
    {ok, #state{}, ?KEEPALIVE_INTERVAL}.


%%%===================================================================
%%% websocket client API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Callback called when data is received via WebSocket protocol.
%% @end
%%--------------------------------------------------------------------
-spec websocket_handle({text | binary | ping | pong, binary()},
    websocket_req:req(), state()) ->
    {ok, state()} |
    {reply, websocket_req:frame(), state()} |
    {close, Reply :: binary(), state()}.
websocket_handle({text, Data}, _, #state{protocol_version = ProtoVer} = State) ->
    try
        JSONMap = json_utils:decode(Data),
        {ok, DecodedRecord} = gs_protocol:decode(ProtoVer, JSONMap),
        handle_message(DecodedRecord, State)
    catch
        Type:Message ->
            ?error_stacktrace("Unexpected error in GS client - ~p:~p", [
                Type, Message
            ]),
            {ok, State}
    end;

websocket_handle({ping, <<"">>}, _, State) ->
    {ok, State};

websocket_handle({pong, <<"">>}, _, State) ->
    {ok, State};

websocket_handle(Msg, _, State) ->
    ?warning("Unexpected frame in GS client: ~p", [Msg]),
    {ok, State}.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when a message is sent to the process handling
%% the connection.
%% @end
%%--------------------------------------------------------------------
-spec websocket_info(term(), websocket_req:req(), state()) ->
    {ok, state()} |
    {reply, websocket_req:frame(), state()} |
    {close, Reply :: binary(), state()}.
websocket_info({init, CallerPid, SupportedVersions, Auth, PushCallback}, _, State) ->
    Id = datastore_key:new(),
    HandshakeRequest = #gs_req{
        id = Id, subtype = handshake, request = #gs_req_handshake{
            supported_versions = SupportedVersions,
            auth = Auth
        }},
    {ok, JSONMap} = gs_protocol:encode(?BASIC_PROTOCOL, HandshakeRequest),
    NewState = State#state{
        push_callback = PushCallback,
        handshake_status = {pending, Id, CallerPid}
    },
    {reply, {text, json_utils:encode(JSONMap)}, NewState};

websocket_info({push, Data}, _, State) ->
    {reply, {text, Data}, State};

websocket_info({queue_request, #gs_req{id = Id} = Request, Pid}, _, State) ->
    #state{protocol_version = ProtoVer} = State,
    case gs_protocol:encode(ProtoVer, Request) of
        {ok, JSONMap} ->
            NewState = State#state{
                promises = maps:put(Id, Pid, State#state.promises)
            },
            {reply, {text, json_utils:encode(JSONMap)}, NewState};
        {error, _} = Error ->
            ?error("Discarding GS request as it cannot be encoded: ~p", [Error]),
            Pid ! {response, Id, Error},
            {ok, State}
    end;

websocket_info(terminate, _, State) ->
    {close, <<"">>, State};

websocket_info(Msg, _, State) ->
    ?warning("Unexpected message in GS client: ~p", [Msg]),
    {ok, State}.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when the connection is closed.
%% @end
%%--------------------------------------------------------------------
-spec websocket_terminate({Reason, term()} | {Reason, integer(), binary()},
    websocket_req:req(), state()) -> ok when
    Reason :: normal | error | remote.
websocket_terminate(Reason, _ConnState, _State) ->
    ?debug("GS client terminating - ~p", [Reason]),
    ok.

%%%===================================================================
%%% Graph Sync client API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends a synchronous RPC request to GS server using given GS client instance
%% (waits for response or returns with a timeout error).
%% @end
%%--------------------------------------------------------------------
-spec rpc_request(client_ref(), gs_protocol:rpc_function(), gs_protocol:rpc_args()) ->
    {ok, gs_protocol:rpc_resp()} | errors:error().
rpc_request(ClientRef, Function, Args) ->
    sync_request(ClientRef, #gs_req_rpc{function = Function, args = Args}).


%%--------------------------------------------------------------------
%% @doc
%% @equiv graph_request(ClientRef, GRI, Operation, undefined, false, undefined).
%% @end
%%--------------------------------------------------------------------
-spec graph_request(client_ref(), gri:gri(), gs_protocol:operation()) ->
    {ok, gs_protocol:graph_resp()} | errors:error().
graph_request(ClientRef, GRI, Operation) ->
    graph_request(ClientRef, GRI, Operation, undefined, false, undefined).


%%--------------------------------------------------------------------
%% @doc
%% @equiv graph_request(ClientRef, GRI, Operation, Data, false, undefined).
%% @end
%%--------------------------------------------------------------------
-spec graph_request(client_ref(), gri:gri(), gs_protocol:operation(),
    gs_protocol:data()) -> {ok, gs_protocol:graph_resp()} | errors:error().
graph_request(ClientRef, GRI, Operation, Data) ->
    graph_request(ClientRef, GRI, Operation, Data, false, undefined).


%%--------------------------------------------------------------------
%% @doc
%% @equiv graph_request(ClientRef, GRI, Operation, Data, Subscribe, undefined).
%% @end
%%--------------------------------------------------------------------
-spec graph_request(client_ref(), gri:gri(), gs_protocol:operation(),
    gs_protocol:data(), Subscribe :: boolean()) ->
    {ok, gs_protocol:graph_resp()} | errors:error().
graph_request(ClientRef, GRI, Operation, Data, Subscribe) ->
    graph_request(ClientRef, GRI, Operation, Data, Subscribe, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Sends a synchronous GRAPH request to GS server using given GS client instance
%% (waits for response or returns with a timeout error).
%% Allows to specify data that will be sent with the request, if the client
%% wishes to subscribe for the resource and authorization hint.
%% @end
%%--------------------------------------------------------------------
-spec graph_request(client_ref(), gri:gri(), gs_protocol:operation(),
    gs_protocol:data(), Subscribe :: boolean(), gs_protocol:auth_hint()) ->
    {ok, gs_protocol:graph_resp()} | errors:error().
graph_request(ClientRef, GRI, Operation, Data, Subscribe, AuthHint) ->
    sync_request(ClientRef, #gs_req_graph{
        gri = GRI,
        operation = Operation,
        data = Data,
        subscribe = Subscribe,
        auth_hint = AuthHint
    }).


%%--------------------------------------------------------------------
%% @doc
%% Sends a synchronous UNSUB request to GS server using given GS client instance
%% (waits for response or returns with a timeout error).
%% @end
%%--------------------------------------------------------------------
-spec unsub_request(client_ref(), gri:gri()) ->
    {ok, gs_protocol:unsub_resp()} | errors:error().
unsub_request(ClientRef, GRI) ->
    sync_request(ClientRef, #gs_req_unsub{gri = GRI}).


%%--------------------------------------------------------------------
%% @doc
%% Sends a synchronous request to GS server using given GS client instance
%% (waits for response or returns with a timeout error). It is possible that the
%% response message still reaches the caller process after the timeout.
%% @end
%%--------------------------------------------------------------------
-spec sync_request(client_ref(), gs_protocol:req_wrapper() |
    gs_protocol:rpc_req() | gs_protocol:graph_req() | gs_protocol:unsub_req()) ->
    {ok, gs_protocol:rpc_resp() | gs_protocol:graph_resp() |
    gs_protocol:unsub_resp()} | errors:error().
sync_request(ClientRef, #gs_req_rpc{} = RPCReq) ->
    sync_request(ClientRef, #gs_req{subtype = rpc, request = RPCReq});
sync_request(ClientRef, #gs_req_graph{} = GraphReq) ->
    sync_request(ClientRef, #gs_req{subtype = graph, request = GraphReq});
sync_request(ClientRef, #gs_req_unsub{} = UnsubReq) ->
    sync_request(ClientRef, #gs_req{subtype = unsub, request = UnsubReq});
sync_request(ClientRef, Request) ->
    Id = async_request(ClientRef, Request),
    receive
        {response, Id, Response} ->
            Response
    after
        ?GS_CLIENT_REQUEST_TIMEOUT ->
            ?ERROR_TIMEOUT
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sends an asynchronous request to GS server using given GS client instance.
%% Returns the request Id. Caller process should expect an answer in form
%% {response, Id, Response} (Response can be {error, _} or {ok, #gs_resp_*}).
%% @end
%%--------------------------------------------------------------------
-spec async_request(client_ref(), gs_protocol:req_wrapper()) ->
    gs_protocol:message_id().
async_request(ClientRef, #gs_req{id = undefined} = Request) ->
    async_request(ClientRef, Request#gs_req{id = datastore_key:new()});
async_request(ClientRef, #gs_req{id = Id} = Request) ->
    ClientRef ! {queue_request, Request, self()},
    Id.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles decoded message, returns (optional) response and new handler state.
%% @end
%%--------------------------------------------------------------------
-spec handle_message(gs_protocol:resp_wrapper() | gs_protocol:push_wrapper(), state()) ->
    {ok, State :: state()} |
    {reply, websocket_req:frame(), state()} |
    {close, Reply :: binary(), state()}.
handle_message(
    #gs_resp{id = Id, subtype = handshake, success = false, error = Error},
    #state{handshake_status = {pending, Id, CallerPid}} = State
) ->
    CallerPid ! {init_result, Error},
    {close, <<"">>, State};

handle_message(
    #gs_resp{id = Id, subtype = handshake, success = true, response = Resp},
    #state{handshake_status = {pending, Id, CallerPid}} = State
) ->
    #gs_resp_handshake{
        version = Version, session_id = SessionId,
        identity = Identity, attributes = Attributes
    } = Resp,
    CallerPid ! {init_result, Resp},
    {ok, State#state{
        handshake_status = true,
        protocol_version = Version,
        session_id = SessionId,
        identity = Identity,
        handshake_attributes = Attributes
    }};

handle_message(#gs_resp{id = Id} = GSResp, State) ->
    CallingProcess = maps:get(Id, State#state.promises),
    case GSResp of
        #gs_resp{success = false, error = Error} ->
            CallingProcess ! {response, Id, Error};
        #gs_resp{success = true, response = Resp} ->
            CallingProcess ! {response, Id, {ok, Resp}}
    end,
    {ok, State#state{
        promises = maps:remove(Id, State#state.promises)
    }};

handle_message(#gs_push{message = Message}, State) ->
    PushCallback = State#state.push_callback,
    erlang:apply(PushCallback, [Message]),
    {ok, State}.
