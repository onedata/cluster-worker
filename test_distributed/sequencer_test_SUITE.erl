%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains sequencer API tests.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_test_SUITE).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("proto/oneclient/client_messages.hrl").
-include_lib("proto/oneclient/server_messages.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    open_stream_should_return_stream_id/1,
    open_stream_should_return_different_stream_ids/1,
    close_stream_should_notify_sequencer_manager/1,
    send_message_should_forward_message/1,
    send_message_should_inject_stream_id_into_message/1,
    route_message_should_forward_message/1,
    route_message_should_forward_messages_to_the_same_stream/1,
    route_message_should_forward_messages_to_different_streams/1
]).

-performance({test_cases, []}).
all() -> [
    open_stream_should_return_stream_id,
    open_stream_should_return_different_stream_ids,
    close_stream_should_notify_sequencer_manager,
    send_message_should_forward_message,
    send_message_should_inject_stream_id_into_message,
    route_message_should_forward_message,
    route_message_should_forward_messages_to_the_same_stream,
    route_message_should_forward_messages_to_different_streams
].

-define(TIMEOUT, timer:seconds(5)).

%%%===================================================================
%%% Test functions
%%%===================================================================

open_stream_should_return_stream_id(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, StmId} = open_stream(Worker, ?config(session_id, Config)),
    ?assert(is_integer(StmId)).

open_stream_should_return_different_stream_ids(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, StmId1} = open_stream(Worker, ?config(session_id, Config)),
    {ok, StmId2} = open_stream(Worker, ?config(session_id, Config)),
    ?assert(StmId1 =/= StmId2).

close_stream_should_notify_sequencer_manager(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, StmId} = open_stream(Worker, ?config(session_id, Config)),
    close_stream(Worker, ?config(session_id, Config), StmId).

send_message_should_forward_message(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StmId = ?config(stream_id, Config),
    send_message(Worker, ?config(session_id, Config), StmId, msg),
    ?assertReceivedMatch(
        #server_message{message_stream = #message_stream{}}, ?TIMEOUT
    ).

send_message_should_inject_stream_id_into_message(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StmId = ?config(stream_id, Config),
    send_message(Worker, ?config(session_id, Config), StmId, msg),
    ?assertReceivedMatch(#server_message{message_stream = #message_stream{
        stream_id = StmId
    }}, ?TIMEOUT).

route_message_should_forward_message(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Msg = client_message(1, 0),
    route_message(Worker, ?config(session_id, Config), Msg),
    ?assertReceivedMatch(Msg, ?TIMEOUT).

route_message_should_forward_messages_to_the_same_stream(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Msgs = lists:map(fun(SeqNum) ->
        Msg = client_message(1, SeqNum),
        route_message(Worker, SessId, Msg),
        Msg
    end, lists:seq(9, 0, -1)),
    lists:foreach(fun(Msg) ->
        ?assertReceivedNextMatch(Msg, ?TIMEOUT)
    end, lists:reverse(Msgs)).

route_message_should_forward_messages_to_different_streams(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Msgs = lists:map(fun(StmId) ->
        Msg = client_message(StmId, 0),
        route_message(Worker, SessId, Msg),
        Msg
    end, lists:seq(1, 10)),
    lists:foreach(fun(Msg) ->
        ?assertReceivedMatch(Msg, ?TIMEOUT)
    end, Msgs).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(Case, Config) when
    Case =:= send_message_should_forward_message;
    Case =:= send_message_should_inject_stream_id_into_message ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    mock_communicator(Worker),
    {ok, SessId} = create_session(Worker),
    {ok, StmId} = open_stream(Worker, SessId),
    [{session_id, SessId}, {stream_id, StmId} | Config];

init_per_testcase(Case, Config) when
    Case =:= route_message_should_forward_message;
    Case =:= route_message_should_forward_messages_to_the_same_stream;
    Case =:= route_message_should_forward_messages_to_different_streams ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    mock_router(Worker),
    mock_communicator(Worker, fun(_, _) -> ok end),
    {ok, SessId} = create_session(Worker),
    [{session_id, SessId} | Config];

init_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, SessId} = create_session(Worker),
    [{session_id, SessId} | Config].

end_per_testcase(Case, Config) when
    Case =:= send_message_should_forward_message;
    Case =:= send_message_should_inject_stream_id_into_message ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    close_stream(Worker, SessId, ?config(stream_id, Config)),
    remove_session(Worker, SessId),
    validate_and_unload_mocks(Worker, [communicator]),
    proplists:delete(stream_id, proplists:delete(session_id, Config));

end_per_testcase(Case, Config) when
    Case =:= route_message_should_forward_message;
    Case =:= route_message_should_forward_messages_to_the_same_stream;
    Case =:= route_message_should_forward_messages_to_different_streams ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    remove_session(Worker, SessId),
    validate_and_unload_mocks(Worker, [communicator, router]),
    proplists:delete(session_id, Config);

end_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    remove_session(Worker, ?config(session_id, Config)),
    proplists:delete(session_id, Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv create_session(Worker, <<"session_id">>
%% @end
%%--------------------------------------------------------------------
-spec create_session(Worker :: node()) -> {ok, SessId :: session:id()}.
create_session(Worker) ->
    create_session(Worker, <<"session_id">>).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session with given ID.
%% @end
%%--------------------------------------------------------------------
-spec create_session(Worker :: node(), SessId :: session:id()) ->
    {ok, SessId :: session:id()}.
create_session(Worker, SessId) ->
    Self = self(),
    Iden = #identity{user_id = <<"user_id">>},
    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, Iden, Self]
    )),
    {ok, SessId}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes session.
%% @end
%%--------------------------------------------------------------------
-spec remove_session(Worker :: node(), SessId :: session:id()) -> ok.
remove_session(Worker, SessId) ->
    ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec open_stream(Worker :: node(), SessId :: session:id()) ->
    {ok, StmId :: sequencer:stream_id()}.
open_stream(Worker, SessId) ->
    ?assertMatch({ok, _}, rpc:call(Worker, sequencer, open_stream, [SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Closes sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec close_stream(Worker :: node(), SessId :: session:id(),
    StmId :: sequencer:stream_id()) -> ok.
close_stream(Worker, SessId, StmId) ->
    ?assertEqual(ok, rpc:call(Worker, sequencer, close_stream, [StmId, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends message to sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec send_message(Worker :: node(), SessId :: session:id(),
    StmId :: sequencer:stream_id(), Msg :: term()) -> ok.
send_message(Worker, SessId, StmId, Msg) ->
    ?assertEqual(ok, rpc:call(Worker, sequencer, send_message, [Msg, StmId, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends message to sequencer stream for incomming messages.
%% @end
%%--------------------------------------------------------------------
-spec route_message(Worker :: node(), SessId :: session:id(), Msg :: term()) -> ok.
route_message(Worker, SessId, Msg) ->
    ?assertEqual(ok, rpc:call(Worker, sequencer, route_message, [Msg, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns client message as part of messages stream.
%% @end
%%--------------------------------------------------------------------
-spec client_message(StmId :: sequencer:stream_id(),
    SeqNum :: sequencer:sequence_number()) -> Msg :: #client_message{}.
client_message(StmId, SeqNum) ->
    #client_message{message_stream = #message_stream{
        stream_id = StmId, sequence_number = SeqNum
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks communicator, so that on send it forwards all messages to this process.
%% @end
%%--------------------------------------------------------------------
-spec mock_communicator(Worker :: node()) -> ok.
mock_communicator(Worker) ->
    Self = self(),
    mock_communicator(Worker, fun(Msg, _) -> Self ! Msg end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks communicator, so that send function calls provided mock function.
%% @end
%%--------------------------------------------------------------------
-spec mock_communicator(Worker :: node(), MockFun :: fun()) -> ok.
mock_communicator(Worker, MockFun) ->
    test_utils:mock_new(Worker, [communicator]),
    test_utils:mock_expect(Worker, communicator, send, MockFun).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks communicator, so that on send it forwards all messages to this process.
%% @end
%%--------------------------------------------------------------------
-spec mock_router(Worker :: node()) -> ok.
mock_router(Worker) ->
    Self = self(),
    test_utils:mock_new(Worker, [router]),
    test_utils:mock_expect(Worker, router, route_message, fun
        (Msg) -> Self ! Msg
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Validates and unloads mocks.
%% @end
%%--------------------------------------------------------------------
-spec validate_and_unload_mocks(Worker :: node(), Mocks :: [atom()]) -> ok.
validate_and_unload_mocks(Worker, Mocks) ->
    test_utils:mock_validate(Worker, Mocks),
    test_utils:mock_unload(Worker, Mocks).

