%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains sequencer manager tests.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_manager_test_SUITE).
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
    sequencer_manager_should_update_session_on_init/1,
    sequencer_manager_should_update_session_on_terminate/1,
    sequencer_manager_should_register_sequencer_in_stream/1,
    sequencer_manager_should_unregister_sequencer_in_stream/1,
    sequencer_manager_should_register_sequencer_out_stream/1,
    sequencer_manager_should_unregister_sequencer_out_stream/1,
    sequencer_manager_should_create_sequencer_stream_on_open_stream/1,
    sequencer_manager_should_send_end_of_message_stream_on_close_stream/1,
    sequencer_manager_should_forward_message_stream_reset/1,
    sequencer_manager_should_forward_message_request/1,
    sequencer_manager_should_forward_message_acknowledgement/1,
    sequencer_manager_should_start_sequencer_in_stream_on_first_message/1
]).

-performance({test_cases, []}).
all() -> [
    sequencer_manager_should_update_session_on_init,
    sequencer_manager_should_update_session_on_terminate,
    sequencer_manager_should_register_sequencer_in_stream,
    sequencer_manager_should_unregister_sequencer_in_stream,
    sequencer_manager_should_register_sequencer_out_stream,
    sequencer_manager_should_unregister_sequencer_out_stream,
    sequencer_manager_should_create_sequencer_stream_on_open_stream,
    sequencer_manager_should_send_end_of_message_stream_on_close_stream,
    sequencer_manager_should_forward_message_stream_reset,
    sequencer_manager_should_forward_message_request,
    sequencer_manager_should_forward_message_acknowledgement,
    sequencer_manager_should_start_sequencer_in_stream_on_first_message
].

-define(TIMEOUT, timer:seconds(5)).

%%%===================================================================
%%% Test functions
%%%===================================================================

sequencer_manager_should_update_session_on_init(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertMatch({ok, _}, rpc:call(Worker, session, get_sequencer_manager,
        [?config(session_id, Config)])).

sequencer_manager_should_update_session_on_terminate(Config) ->
    stop_sequencer_manager(?config(sequencer_manager, Config)),
    [Worker | _] = ?config(op_worker_nodes, Config),
    repeat(fun() -> rpc:call(
        Worker, session, get_sequencer_manager, [?config(session_id, Config)]
    ) end, {error, {not_found, missing}}, 10, 500).

sequencer_manager_should_register_sequencer_in_stream(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    gen_server:cast(SeqMan, {register_in_stream, 1, self()}),
    gen_server:cast(SeqMan, client_message()),
    ?assertReceivedMatch({'$gen_event', #client_message{}}, ?TIMEOUT).

sequencer_manager_should_unregister_sequencer_in_stream(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    gen_server:cast(SeqMan, {register_in_stream, 1, self()}),
    gen_server:cast(SeqMan, {unregister_in_stream, 1}),
    gen_server:cast(SeqMan, client_message()),
    ?assertNotReceivedMatch({'$gen_event', #client_message{}}, ?TIMEOUT).

sequencer_manager_should_register_sequencer_out_stream(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    gen_server:cast(SeqMan, {register_out_stream, 1, self()}),
    gen_server:cast(SeqMan, server_message()),
    ?assertReceivedMatch({'$gen_cast', #server_message{}}, ?TIMEOUT).

sequencer_manager_should_unregister_sequencer_out_stream(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    gen_server:cast(SeqMan, {register_out_stream, 1, self()}),
    gen_server:cast(SeqMan, {unregister_out_stream, 1}),
    gen_server:cast(SeqMan, server_message()),
    ?assertNotReceivedMatch({'$gen_cast', #server_message{}}, ?TIMEOUT).

sequencer_manager_should_create_sequencer_stream_on_open_stream(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    ?assertMatch({ok, _}, gen_server:call(SeqMan, open_stream)),
    ?assertReceivedMatch({start_sequencer_stream, _}, ?TIMEOUT).

sequencer_manager_should_send_end_of_message_stream_on_close_stream(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    gen_server:cast(SeqMan, {register_out_stream, 1, self()}),
    gen_server:cast(SeqMan, {close_stream, 1}),
    ?assertReceivedMatch({'$gen_cast', #server_message{
        message_body = #end_of_message_stream{}
    }}, ?TIMEOUT).

sequencer_manager_should_forward_message_stream_reset(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    Msg = #message_stream_reset{stream_id = 1},
    gen_server:cast(SeqMan, {register_out_stream, 1, self()}),
    gen_server:cast(SeqMan, #client_message{message_body = Msg}),
    ?assertReceivedMatch({'$gen_cast', Msg}, ?TIMEOUT).

sequencer_manager_should_forward_message_request(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    Msg = #message_request{stream_id = 1},
    gen_server:cast(SeqMan, {register_out_stream, 1, self()}),
    gen_server:cast(SeqMan, #client_message{message_body = Msg}),
    ?assertReceivedMatch({'$gen_cast', Msg}, ?TIMEOUT).

sequencer_manager_should_forward_message_acknowledgement(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    Msg = #message_acknowledgement{stream_id = 1},
    gen_server:cast(SeqMan, {register_out_stream, 1, self()}),
    gen_server:cast(SeqMan, #client_message{message_body = Msg}),
    ?assertReceivedMatch({'$gen_cast', Msg}, ?TIMEOUT).

sequencer_manager_should_start_sequencer_in_stream_on_first_message(Config) ->
    gen_server:cast(?config(sequencer_manager, Config), client_message()),
    ?assertReceivedMatch({start_sequencer_stream, _}, ?TIMEOUT),
    ?assertReceivedMatch({'$gen_event', #client_message{}}, ?TIMEOUT).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(Case, Config) when
    Case =:= sequencer_manager_should_create_sequencer_stream_on_open_stream;
    Case =:= sequencer_manager_should_start_sequencer_in_stream_on_first_message ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    mock_sequencer_stream_sup(Worker),
    init_per_testcase(default, Config);

init_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, SessId} = create_session(Worker),
    mock_sequencer_manager_sup(Worker),
    {ok, SeqMan} = start_sequencer_manager(Worker, SessId),
    [{sequencer_manager, SeqMan}, {session_id, SessId} | Config].

end_per_testcase(Case, Config) when
    Case =:= sequencer_manager_should_create_sequencer_stream_on_open_stream;
    Case =:= sequencer_manager_should_start_sequencer_in_stream_on_first_message ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    end_per_testcase(default, Config),
    validate_and_unload_mocks(Worker, [sequencer_stream_sup]);

end_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    stop_sequencer_manager(?config(sequencer_manager, Config)),
    validate_and_unload_mocks(Worker, [sequencer_manager_sup]),
    remove_session(Worker, ?config(session_id, Config)),
    remove_pending_messages(),
    proplists:delete(session_id, proplists:delete(sequencer_manager, Config)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts sequencer manager for given session.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer_manager(Worker :: node(), SessId :: session:id()) ->
    {ok, SeqMan :: pid()}.
start_sequencer_manager(Worker, SessId) ->
    SeqManSup = self(),
    ?assertMatch({ok, _}, rpc:call(Worker, gen_server, start, [
        sequencer_manager, [SeqManSup, SessId], []
    ])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops event stream.
%% @end
%%--------------------------------------------------------------------
-spec stop_sequencer_manager(SeqMan :: pid()) -> true.
stop_sequencer_manager(SeqMan) ->
    exit(SeqMan, shutdown).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session document in datastore.
%% @end
%%--------------------------------------------------------------------
-spec create_session(Worker :: node()) -> {ok, SessId :: session:id()}.
create_session(Worker) ->
    ?assertMatch({ok, _}, rpc:call(Worker, session, create, [#document{
        key = <<"session_id">>, value = #session{}
    }])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes session document from datastore.
%% @end
%%--------------------------------------------------------------------
-spec remove_session(Worker :: node(), SessId :: session:id()) -> ok.
remove_session(Worker, SessId) ->
    ?assertEqual(ok, rpc:call(Worker, session, delete, [SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns client message as part of a message stream with default stream ID.
%% @end
%%--------------------------------------------------------------------
-spec client_message() -> Msg :: #client_message{}.
client_message() ->
    #client_message{message_stream = #message_stream{stream_id = 1}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns server message as part of a message stream with default stream ID.
%% @end
%%--------------------------------------------------------------------
-spec server_message() -> Msg :: #server_message{}.
server_message() ->
    #server_message{message_stream = #message_stream{stream_id = 1}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks sequencer manager supervisor, so that it returns this process as event
%% stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec mock_sequencer_manager_sup(Worker :: node()) -> ok.
mock_sequencer_manager_sup(Worker) ->
    SeqStmSup = self(),
    test_utils:mock_new(Worker, [sequencer_manager_sup]),
    test_utils:mock_expect(Worker, sequencer_manager_sup,
        get_sequencer_stream_sup, fun
            (_, _) -> {ok, SeqStmSup}
        end
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks sequencer stream supervisor, so that it notifies this process when
%% sequecner stream is started. Moreover, returns this process as started
%% sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec mock_sequencer_stream_sup(Worker :: node()) -> ok.
mock_sequencer_stream_sup(Worker) ->
    Self = self(),
    test_utils:mock_new(Worker, [sequencer_stream_sup]),
    test_utils:mock_expect(Worker, sequencer_stream_sup,
        start_sequencer_stream, fun
            (_, _, StmId, _) ->
                Self ! {start_sequencer_stream, StmId},
                {ok, Self}
        end
    ).

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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Repeatedly executes provided function until it returns expected value or
%% attempts limit exceeds.
%% @end
%%--------------------------------------------------------------------
-spec repeat(Fun :: fun(), Expected :: term(), Attempts :: non_neg_integer(),
    Delay :: timeout()) -> ok.
repeat(Fun, Expected, 0, _) ->
    ?assertMatch(Expected, Fun());

repeat(Fun, Expected, Attempts, Delay) ->
    case Fun() of
        Expected -> ok;
        _ ->
            timer:sleep(Delay),
            repeat(Fun, Expected, Attempts - 1, Delay)
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