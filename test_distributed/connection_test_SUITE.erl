%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests protocol handler
%%% @end
%%%--------------------------------------------------------------------
-module(connection_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneproxy/oneproxy_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include("modules/datastore/datastore.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("clproto/include/oneproxy_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).
-export([cert_connection_test/1, token_connection_test/1, protobuf_msg_test/1,
    multi_message_test/1, client_send_test/1, client_communicate_test/1,
    client_communiate_async_test/1, multi_ping_pong_test/1,
    sequential_ping_pong_test/1, multi_connection_test/1, bandwidth_test/1,
    python_client_test/1, proto_version_test/1]).

% todo repair oneproxy error:
% todo [error] <0.1099.0>@oneproxy:main_loop:234 [ oneproxy 5555 ] handle_client_read failed due to: tlsv1 alert internal error
-performance({test_cases, [multi_message_test, multi_ping_pong_test,
    sequential_ping_pong_test, multi_connection_test, bandwidth_test,
    python_client_test]}).
all() ->
    [token_connection_test, cert_connection_test, protobuf_msg_test,
        multi_message_test, client_send_test, client_communicate_test,
        client_communiate_async_test, multi_ping_pong_test,
        sequential_ping_pong_test, multi_connection_test, bandwidth_test,
        python_client_test, proto_version_test].

-define(TOKEN, <<"TOKEN_VALUE">>).

%%%===================================================================
%%% Test function
%%%===================================================================

token_connection_test(Config) ->
    % given
    remove_pending_messages(),
    [Worker1, _] = ?config(op_worker_nodes, Config),

    % then
    {ok, {Sock, _}} = connect_via_token(Worker1),
    ok = ssl:close(Sock),
    ?assertMatch({error, _}, ssl:connection_info(Sock)).

cert_connection_test(Config) ->
    % given
    remove_pending_messages(),
    [Worker1, _] = Workers = ?config(op_worker_nodes, Config),
    HandshakeReq = #'ClientMessage'{message_body = {handshake_request, #'HandshakeRequest'{
        session_id = <<"session_id">>
    }}},
    HandshakeReqRaw = messages:encode_msg(HandshakeReq),
    Pid = self(),
    test_utils:mock_expect(Workers, serializator, deserialize_oneproxy_certificate_info_message,
        fun(Data) ->
            Ans = meck:passthrough([Data]),
            Pid ! {certificate_info_message, Ans},
            Ans
        end),
    Cert = ?TEST_FILE(Config, "peer.pem"),

    % when
    {ok, Sock} = ssl:connect(utils:get_host_as_atom(Worker1), 5555, [binary, {packet, 4},
        {active, true}, {certfile, Cert}, {cacertfile, Cert}]),
    {ok, {certificate_info_message, {ok, CertInfo}}} =
        test_utils:receive_any(timer:seconds(5)),
    ok = ssl:send(Sock, HandshakeReqRaw),
    {ok, {ssl, _, RawHandshakeResponse}} = test_utils:receive_any(timer:seconds(5)),

    % then
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    ?assertMatch(#certificate_info{}, CertInfo),
    ?assertNotEqual(undefined, CertInfo#certificate_info.client_session_id),
    ?assertNotEqual(undefined, CertInfo#certificate_info.client_subject_dn),
    ?assert(is_binary(CertInfo#certificate_info.client_session_id)),
    ?assert(is_binary(CertInfo#certificate_info.client_subject_dn)),
    HandshakeResponse = messages:decode_msg(RawHandshakeResponse, 'ServerMessage'),
    #'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{session_id =
    SessionId}}} = HandshakeResponse,
    ?assert(is_binary(SessionId)),
    ok = ssl:close(Sock),
    ?assertMatch({error, _}, ssl:connection_info(Sock)).

protobuf_msg_test(Config) ->
    % given
    remove_pending_messages(),
    [Worker1, _] = Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_expect(Workers, router, preroute_message,
        fun(#client_message{message_body = #read_event{}}) ->
            ok
        end),
    Msg = #'ClientMessage'{
        message_id = <<"0">>,
        message_body = {event, #'Event'{event =
        {read_event, #'ReadEvent'{counter = 1, file_id = <<"id">>, size = 1, blocks = []}}
        }}
    },
    RawMsg = messages:encode_msg(Msg),

    % when
    {ok, {Sock, _}} = connect_via_token(Worker1),
    ok = ssl:send(Sock, RawMsg),

    % then
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    ok = ssl:close(Sock).

-performance([
    {repeats, 3},
    {parameters, [
        [{name, msg_num}, {value, 1000}, {description, "Number of messages sent and received."}],
        [{name, transport}, {value, ssl}, {description, "Connection transport type."}]
    ]},
    {config, [{name, ssl_through_oneproxy},
        {parameters, [
            [{name, msg_num}, {value, 100000}]
        ]}
    ]},
    {config, [{name, tcp_direct},
        {parameters, [
            [{name, msg_num}, {value, 100000}],
            [{name, transport}, {value, gen_tcp}]
        ]}
    ]}
]).
multi_message_test(Config) ->
    % given
    remove_pending_messages(),
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    MsgNum = ?config(msg_num, Config),
    Transport = ?config(transport, Config),
    Self = self(),
    MsgNumbers = lists:seq(1, MsgNum),
    Events = lists:map(
        fun(N) ->
            #'ClientMessage'{message_body = {event, #'Event'{event =
            {read_event, #'ReadEvent'{
                counter = N,
                file_id = <<"id">>,
                size = 1,
                blocks = []
            }}}}}
        end, MsgNumbers),
    RawEvents = lists:map(fun(E) -> messages:encode_msg(E) end, Events),
    test_utils:mock_expect(Workers, router, route_message,
        fun(#client_message{message_body = #read_event{counter = Counter}}) ->
            Self ! Counter,
            ok
        end),

    % when
    {ok, {Sock, _}} = connect_via_token(Worker1, [{active, true}], Transport),
    T1 = os:timestamp(),
    lists:foreach(fun(E) -> ok = Transport:send(Sock, E) end, RawEvents),
    T2 = os:timestamp(),

    % then
    lists:foreach(
        fun(N) ->
            ?assertMatch({ok, N}, test_utils:receive_msg(N, timer:seconds(5)))
        end, MsgNumbers),
    T3 = os:timestamp(),
    ok = Transport:close(Sock),
    [
        #parameter{name = sending_time, value = utils:milliseconds_diff(T2, T1), unit = "ms"},
        #parameter{name = receiving_time, value = utils:milliseconds_diff(T3, T2), unit = "ms"},
        #parameter{name = full_time, value = utils:milliseconds_diff(T3, T1), unit = "ms"}
    ].

client_send_test(Config) ->
    % given
    remove_pending_messages(),
    [Worker1, _] = ?config(op_worker_nodes, Config),
    {ok, {Sock, SessionId}} = connect_via_token(Worker1),
    Code = 'VOK',
    Description = <<"desc">>,
    ServerMsgInternal = #server_message{
        message_body = #status{
            code = Code,
            description = Description
        }
    },
    ServerMessageProtobuf = #'ServerMessage'{
        message_id = undefined,
        message_body = {status, #'Status'{
            code = Code,
            description = Description}
        }
    },

    % when
    rpc:call(Worker1, communicator, send, [ServerMsgInternal, SessionId]),

    % then
    ?assertEqual(ServerMessageProtobuf, receive_server_message()),
    ok = ssl:close(Sock).

client_communicate_test(Config) ->
    % given
    remove_pending_messages(),
    [Worker1, _] = ?config(op_worker_nodes, Config),
    Status = #status{
        code = 'VOK',
        description = <<"desc">>
    },
    ServerMsgInternal = #server_message{message_body = Status},

    % when
    {ok, {Sock, SessionId}} = spawn_ssl_echo_client(Worker1),
    CommunicateResult = rpc:call(Worker1, communicator, communicate,
        [ServerMsgInternal, SessionId]),

    % then
    ?assertMatch({ok, #client_message{message_body = Status}}, CommunicateResult),
    ok = ssl:close(Sock).

client_communiate_async_test(Config) ->
    % given
    remove_pending_messages(),
    [Worker1, _] = Workers = ?config(op_worker_nodes, Config),
    Status = #status{
        code = 'VOK',
        description = <<"desc">>
    },
    ServerMsgInternal = #server_message{message_body = Status},
    Self = self(),
    {ok, {Sock, SessionId}} = spawn_ssl_echo_client(Worker1),

    % when
    {ok, MsgId} = rpc:call(Worker1, communicator, communicate_async,
        [ServerMsgInternal, SessionId, Self]),
    ReceivedMessage =
        receive
            #client_message{} = Msg -> Msg
        after timer:seconds(5) ->
            {error, timeout}
        end,

    % then
    ?assertMatch(#client_message{message_id = MsgId, message_body = Status},
        ReceivedMessage),

    % given
    test_utils:mock_expect(Workers, router, route_message,
        fun(#client_message{message_id = Id = #message_id{issuer = server,
            recipient = undefined}}) ->
            Self ! {router_message_called, Id},
            ok
        end),

    % when
    {ok, MsgId2} = rpc:call(Worker1, communicator, communicate_async,
        [ServerMsgInternal, SessionId]),
    RouterNotification = test_utils:receive_any(timer:seconds(5)),

    % then
    ?assertEqual({ok, {router_message_called, MsgId2}}, RouterNotification),
    ok = ssl:close(Sock).

-performance([
    {repeats, 3},
    {parameters, [
        [{name, connections_num}, {value, 10}, {description, "Number of connections."}],
        [{name, msg_num}, {value, 1000}, {description, "Number of messages sent and received."}],
        [{name, transport}, {value, ssl}, {description, "Connection transport type."}]
    ]},
    {description, "Opens 'connections_num' connections and for each connection, "
    "then sends 'msg_num' ping messages and finally receives 'msg_num' pong "
    "messages."},
    {config, [{name, ssl_through_oneproxy},
        {parameters, [
            [{name, msg_num}, {value, 100000}]
        ]}
    ]},
    {config, [{name, tcp_direct},
        {parameters, [
            [{name, msg_num}, {value, 100000}],
            [{name, transport}, {value, gen_tcp}]
        ]}
    ]}
]).
multi_ping_pong_test(Config) ->
    % given
    remove_pending_messages(),
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Transport = ?config(transport, Config),
    ConnNumbers = ?config(connections_num, Config),
    MsgNum = ?config(msg_num, Config),
    ConnNumbersList = [integer_to_binary(N) || N <- lists:seq(1, ConnNumbers)],
    MsgNumbers = lists:seq(1, MsgNum),
    MsgNumbersBin = lists:map(fun(N) -> integer_to_binary(N) end, MsgNumbers),
    Pings = lists:map(
        fun(N) ->
            #'ClientMessage'{message_id = N, message_body = {ping, #'Ping'{}}}
        end, MsgNumbersBin),
    RawPings = lists:map(fun(E) -> messages:encode_msg(E) end, Pings),
    Self = self(),

    T1 = os:timestamp(),
    [
        spawn_link(
            fun() ->
                % when
                {ok, {Sock, _}} = connect_via_token(Worker1, [{active, true}], Transport),
                lists:foreach(fun(E) ->
                    ok = Transport:send(Sock, E)
                end, RawPings),
                Received = lists:map(
                    fun(_) ->
                        Pong = receive_server_message(),
                        ?assertMatch(#'ServerMessage'{message_body = {pong, #'Pong'{}}}, Pong),
                        {binary_to_integer(Pong#'ServerMessage'.message_id), Pong}
                    end, MsgNumbersBin),

                % then
                {_, ReceivedInOrder} = lists:unzip(lists:keysort(1, Received)),
                IdToMessage = lists:zip(MsgNumbersBin, ReceivedInOrder),
                lists:foreach(
                    fun({Id, #'ServerMessage'{message_id = MsgId}}) ->
                        ?assertEqual(Id, MsgId)
                    end, IdToMessage),
                ok = Transport:close(Sock),
                Self ! success
            end)
        || _ <- ConnNumbersList
    ],
    lists:foreach(fun(_) ->
        {ok, success} = test_utils:receive_msg(success, infinity)
    end, ConnNumbersList),
    T2 = os:timestamp(),
    #parameter{name = full_time, value = utils:milliseconds_diff(T2, T1), unit = "ms"}.

-performance([
    {repeats, 3},
    {parameters, [
        [{name, msg_num}, {value, 1000}, {description, "Number of messages sent and received."}]
    ]},
    {description, "Opens connection and then sends and receives ping/pong message 'msg_num' times."},
    {config, [{name, sequential_ping_pong},
        {parameters, [
            [{name, msg_num}, {value, 100000}]
        ]}
    ]}
]).
sequential_ping_pong_test(Config) ->
    % given
    remove_pending_messages(),
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    MsgNum = ?config(msg_num, Config),
    MsgNumbers = lists:seq(1, MsgNum),
    MsgNumbersBin = lists:map(fun(N) -> integer_to_binary(N) end, MsgNumbers),
    Pings = lists:map(
        fun(N) ->
            #'ClientMessage'{message_id = N, message_body = {ping, #'Ping'{}}}
        end, MsgNumbersBin),
    RawPings = lists:map(fun(E) -> messages:encode_msg(E) end, Pings),

    % when
    {ok, {Sock, _}} = connect_via_token(Worker1),
    T1 = os:timestamp(),
    lists:foldl(fun(E, N) ->
        % send ping & receive pong
        ok = ssl:send(Sock, E),
        Pong = receive_server_message(),

        % validate pong
        BinaryN = integer_to_binary(N),
        ?assertMatch(#'ServerMessage'{message_body = {pong, #'Pong'{}},
            message_id = BinaryN},
            Pong),
        N + 1
    end, 1, RawPings),
    T2 = os:timestamp(),

    % then
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    ok = ssl:close(Sock),

    #parameter{name = full_time, value = utils:milliseconds_diff(T2, T1), unit = "ms"}.

-performance([
    {repeats, 10},
    {parameters, [
        [{name, connections_num}, {value, 100}, {description, "Number of connections."}]
    ]},
    {description, "Opens 'connections_num' connections to the server, checks "
    "their state, and closes them."},
    {config, [{name, multi_connection}]}
]).
multi_connection_test(Config) ->
    % given
    remove_pending_messages(),
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    ConnNumbers = ?config(connections_num, Config),
    ConnNumbersList = [integer_to_binary(N) || N <- lists:seq(1, ConnNumbers)],

    % when
    Connections = lists:map(
        fun(_) ->
            connect_via_token(Worker1, [])
        end, ConnNumbersList),

    % then
    lists:foreach(
        fun(ConnectionAns) ->
            ?assertMatch({ok, {_, _}}, ConnectionAns),
            {ok, {Sock, SessId}} = ConnectionAns,
            ?assert(is_binary(SessId)),
            ?assertMatch({ok, _}, ssl:connection_info(Sock))
        end, Connections),
    lists:foreach(fun({ok, {Sock, _}}) -> ssl:close(Sock) end, Connections).

-performance([
    {repeats, 3},
    {parameters, [
        [{name, packet_size}, {value, 1024}, {unit, "kB"}, {description, "Size of packet."}],
        [{name, packet_num}, {value, 10}, {description, "Number of packets."}],
        [{name, transport}, {value, ssl}, {description, "Connection transport type."}]
    ]},
    {config, [{name, ssl_through_oneproxy},
        {parameters, [
            [{name, packet_num}, {value, 1000}]
        ]}
    ]},
    {config, [{name, tcp_direct},
        {parameters, [
            [{name, packet_num}, {value, 1000}],
            [{name, transport}, {value, gen_tcp}]
        ]}
    ]}
]).
bandwidth_test(Config) ->
    % given
    remove_pending_messages(),
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    PacketSize = ?config(packet_size, Config),
    PacketNum = ?config(packet_num, Config),
    Transport = ?config(transport, Config),
    Data = crypto:rand_bytes(PacketSize * 1024),
    Packet = #'ClientMessage'{message_body = {ping, #'Ping'{data = Data}}},
    PacketRaw = messages:encode_msg(Packet),
    Self = self(),
    test_utils:mock_expect(Workers, router, route_message,
        fun(#client_message{message_body = #ping{}}) ->
            Self ! router_message_called,
            ok
        end),

    % when
    {ok, {Sock, _}} = connect_via_token(Worker1, [{active, true}], Transport),
    T1 = os:timestamp(),
    lists:foreach(fun(_) ->
        ok = Transport:send(Sock, PacketRaw)
    end, lists:seq(1, PacketNum)),
    T2 = os:timestamp(),

    % then
    lists:foreach(
        fun(_) ->
            ?assertEqual({ok, router_message_called},
                test_utils:receive_msg(router_message_called, timer:seconds(5)))
        end, lists:seq(1, PacketNum)),
    T3 = os:timestamp(),
    Transport:close(Sock),
    [
        #parameter{name = sending_time, value = utils:milliseconds_diff(T2, T1), unit = "ms"},
        #parameter{name = receiving_time, value = utils:milliseconds_diff(T3, T2), unit = "ms"},
        #parameter{name = full_time, value = utils:milliseconds_diff(T3, T1), unit = "ms"}
    ].

-performance([
    {repeats, 3},
    {parameters, [
        [{name, packet_size}, {value, 1024}, {unit, "kB"}, {description, "Size of packet."}],
        [{name, packet_num}, {value, 10}, {description, "Number of packets."}]
    ]},
    {description, "Same as bandwidth_test, but with ssl client written in python."},
    {config, [{name, python_client},
        {parameters, [
            [{name, packet_num}, {value, 1000}]
        ]}
    ]}
]).
python_client_test(Config) ->
    % given
    remove_pending_messages(),
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    PacketSize = ?config(packet_size, Config),
    PacketNum = ?config(packet_num, Config),

    Data = crypto:rand_bytes(PacketSize * 1024),
    Packet = #'ClientMessage'{message_body = {ping, #'Ping'{data = Data}}},
    PacketRaw = messages:encode_msg(Packet),

    HandshakeMessage = #'ClientMessage'{message_body =
    {handshake_request, #'HandshakeRequest'{session_id = <<"session_id">>,
        token = #'Token'{value = ?TOKEN}}}},
    HandshakeMessageRaw = messages:encode_msg(HandshakeMessage),

    Self = self(),
    test_utils:mock_expect(Workers, router, route_message,
        fun(#client_message{message_body = #ping{}}) ->
            Self ! router_message_called,
            ok
        end),

    ClientPath = ?TEST_FILE(Config, "ssl_client.py"),
    MessagePath = ?TEST_FILE(Config, "message.arg"),
    HandshakeMsgPath = ?TEST_FILE(Config, "handshake.arg"),
    file:write_file(MessagePath, PacketRaw),
    file:write_file(HandshakeMsgPath, HandshakeMessageRaw),
    Host = utils:get_host(Worker1),
    {ok, Port} = rpc:call(Worker1, application, get_env, [?APP_NAME, protocol_handler_port]),

    % when
    T1 = os:timestamp(),
    Args = [
        "--host", Host,
        "--port", integer_to_list(Port),
        "--handshake-message", HandshakeMsgPath,
        "--message", MessagePath,
        "--count", integer_to_list(PacketNum)
    ],
    PythonClient = open_port({spawn_executable, ClientPath}, [{args, Args}]),

    % then
    lists:foreach(
        fun(_) ->
            ?assertEqual({ok, router_message_called},
                test_utils:receive_msg(router_message_called, timer:seconds(15)))
        end, lists:seq(1, PacketNum)),
    T2 = os:timestamp(),
    catch port_close(PythonClient),
    #parameter{name = full_time, value = utils:milliseconds_diff(T2, T1), unit = "ms"}.

proto_version_test(Config) ->
    % given
    remove_pending_messages(),
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    MsgId = <<"message_id">>,
    GetProtoVersion = #'ClientMessage'{
        message_id = MsgId,
        message_body = {get_protocol_version, #'GetProtocolVersion'{}}
    },
    GetProtoVersionRaw = messages:encode_msg(GetProtoVersion),
    {ok, {Sock, _}} = connect_via_token(Worker1),

    % when
    ok = ssl:send(Sock, GetProtoVersionRaw),
    ProtoVersion = receive_server_message(),

    %then
    ?assertMatch(
        #'ServerMessage'{
            message_id = MsgId,
            message_body = {protocol_version, #'ProtocolVersion'{}}
        },
        ProtoVersion
    ),
    #'ServerMessage'{
        message_body = {_, #'ProtocolVersion'{major = Major, minor = Minor}}
    } = ProtoVersion,
    ?assert(is_integer(Major)),
    ?assert(is_integer(Minor)),
    ok = ssl:close(Sock).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(cert_connection_test, Config) ->
    ssl:start(),
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, serializator),
    mock_identity(Workers),
    Config;

init_per_testcase(Case, Config) when Case =:= protobuf_msg_test
    orelse Case =:= multi_message_test
    orelse Case =:= client_communiate_async_test
    orelse Case =:= bandwidth_test
    orelse Case =:= python_client_test ->
    ssl:start(),
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, router),
    mock_identity(Workers),
    Config;

init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    mock_identity(Workers),
    ssl:start(),
    Config.

end_per_testcase(cert_connection_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    unmock_identity(Workers),
    test_utils:mock_validate(Workers, serializator),
    test_utils:mock_unload(Workers, serializator),
    ssl:stop();

end_per_testcase(Case, Config) when Case =:= protobuf_msg_test
    orelse Case =:= multi_message_test
    orelse Case =:= client_communiate_async_test
    orelse Case =:= bandwidth_test ->
    Workers = ?config(op_worker_nodes, Config),
    unmock_identity(Workers),
    test_utils:mock_validate(Workers, router),
    test_utils:mock_unload(Workers, router),
    ssl:stop();

end_per_testcase(python_client_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    file:delete(?TEST_FILE(Config, "handshake.arg")),
    file:delete(?TEST_FILE(Config, "message.arg")),
    unmock_identity(Workers),
    test_utils:mock_validate(Workers, router),
    test_utils:mock_unload(Workers, router),
    ssl:stop();

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    unmock_identity(Workers),
    ssl:stop().

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using token, with default socket_opts
%% @equiv connect_via_token(Node, [{active, true}], ssl)
%% @end
%%--------------------------------------------------------------------
-spec connect_via_token(Node :: node()) ->
    {ok, {Sock :: ssl:sslsocket(), SessId :: session:id()}}.
connect_via_token(Node) ->
    connect_via_token(Node, [{active, true}], ssl).

connect_via_token(Node, SocketOpts) ->
    connect_via_token(Node, SocketOpts, ssl).

connect_via_token(Node, SocketOpts, Transport) ->
    connect_via_token(Node, SocketOpts, Transport, crypto:rand_bytes(10)).

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using token, with custom socket opts
%% @end
%%--------------------------------------------------------------------
-spec connect_via_token(Node :: node(), SocketOpts :: list(), ssl | tcp, session:id()) ->
    {ok, {Sock :: term(), SessId :: session:id()}}.
connect_via_token(Node, SocketOpts, Transport, SessId) ->
    % given
    TokenAuthMessage = #'ClientMessage'{message_body =
    {handshake_request, #'HandshakeRequest'{session_id = SessId,
        token = #'Token'{value = ?TOKEN}}}},
    TokenAuthMessageRaw = messages:encode_msg(TokenAuthMessage),
    ActiveOpt =
        case proplists:get_value(active, SocketOpts) of
            undefined -> [];
            Other -> [{active, Other}]
        end,
    OtherOpts = proplists:delete(active, SocketOpts),
    CertInfoMessageRaw = oneproxy_messages:encode_msg(#'CertificateInfo'{}),
    {ok, ExternalPort} = rpc:call(Node, application, get_env, [?APP_NAME, protocol_handler_port]),
    {Port, AdditionalOpts} =
        case Transport of
            gen_tcp ->
                {rpc:call(Node, oneproxy, get_local_port, [ExternalPort]), []};
            ssl ->
                {ExternalPort, [{reuse_sessions, false}]} % todo repair oneproxy and delete reuse_sessions flag
        end,

    % when
    {ok, Sock} = Transport:connect(utils:get_host_as_atom(Node), Port, [binary,
        {packet, 4}, {active, once}] ++ OtherOpts ++ AdditionalOpts),
    case Transport of
        gen_tcp -> ok = Transport:send(Sock, CertInfoMessageRaw);
        _ -> ok
    end,
    ok = Transport:send(Sock, TokenAuthMessageRaw),

    % then
    HandshakeResponse = receive_server_message(),
    ?assertMatch(#'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{}}}, HandshakeResponse),
    #'ServerMessage'{message_body = {handshake_response,
        #'HandshakeResponse'{session_id = SessionId}}} = HandshakeResponse,
    setopts(Transport, Sock, ActiveOpt),
    {ok, {Sock, SessionId}}.

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node, and send back each message received from the server
%% @end
%%--------------------------------------------------------------------
-spec spawn_ssl_echo_client(NodeToConnect :: node()) ->
    {ok, {Sock :: ssl:sslsocket(), SessId :: session:id()}}.
spawn_ssl_echo_client(NodeToConnect) ->
    {ok, {Sock, SessionId}} = connect_via_token(NodeToConnect, []),
    SslEchoClient =
        fun Loop() ->
            % receive data from server
            case ssl:recv(Sock, 0) of
                {ok, Data} ->
                    % decode
                    #'ServerMessage'{message_id = Id, message_body = Body} =
                        messages:decode_msg(Data, 'ServerMessage'),

                    % respond with the same data to the server (excluding stream_reset)
                    case Body of
                        {message_stream_reset, _} -> ok;
                        _ ->
                            ClientAnsProtobuf = #'ClientMessage'{message_id = Id, message_body = Body},
                            ClientAnsRaw = messages:encode_msg(ClientAnsProtobuf),
                            ?assertEqual(ok, ssl:send(Sock, ClientAnsRaw))
                    end,

                    %loop back
                    Loop();
                {error, closed} -> ok;
                Error -> ?error("ssl_echo_client error: ~p", [Error])
            end
        end,
    spawn_link(SslEchoClient),
    {ok, {Sock, SessionId}}.

%%--------------------------------------------------------------------
%% @doc
%% Set ssl/tcp socket opts
%% @end
%%--------------------------------------------------------------------
-spec setopts(ssl | tcp, Sock :: term(), list()) ->
    ok | {error, term()}.
setopts(ssl, Sock, Opts) ->
    ssl:setopts(Sock, Opts);
setopts(gen_tcp, Sock, Opts) ->
    inet:setopts(Sock, Opts).

mock_identity(Workers) ->
    test_utils:mock_new(Workers, identity),
    test_utils:mock_expect(Workers, identity, get_or_fetch,
        fun(#token{value = ?TOKEN}) ->
            {ok, #document{value = #identity{}}}
        end
    ).

unmock_identity(Workers) ->
    test_utils:mock_validate(Workers, identity),
    test_utils:mock_unload(Workers, identity).

receive_server_message() ->
    receive_server_message([message_stream_reset]).

receive_server_message(IgnoredMsgList) ->
    receive
        {_, _, Data} ->
            % ignore listed messages
            Msg = messages:decode_msg(Data, 'ServerMessage'),
            MsgType = element(1, Msg#'ServerMessage'.message_body),
            case lists:member(MsgType, IgnoredMsgList) of
                true ->
                    receive_server_message(IgnoredMsgList);
                false -> Msg
            end
    after timer:seconds(5) ->
        {error, timeout}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes messages for process messages queue.
%% @end
%%--------------------------------------------------------------------
-spec remove_pending_messages() -> ok.
remove_pending_messages() ->
    case test_utils:receive_any() of
        {error, timeout} -> ok;
        _ -> remove_pending_messages()
    end.
