%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of event manager API.
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager_test_SUITE).
-author("Krzysztof Trzepla").

-include("workers/datastore/models/session.hrl").
-include("proto_internal/oneclient/common_messages.hrl").
-include("proto_internal/oneclient/event_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    event_stream_test/1,
    event_manager_test/1
]).

all() -> [
    event_stream_test,
    event_manager_test
].

-define(TIMEOUT, timer:seconds(5)).

%%%====================================================================
%%% Test function
%%%====================================================================

%% Test single subscription and execution of event handler.
event_stream_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId1 = <<"session_id_1">>,
    SessId2 = <<"session_id_2">>,
    Cred1 = #credentials{user_id = <<"user_id_1">>},
    Cred2 = #credentials{user_id = <<"user_id_2">>},

    session_setup(Worker1, SessId1, Cred1, Self),

    {ok, SubId} = subscribe(Worker2,
        all,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= 6 end,
        [fun(Evts) -> Self ! {handler, Evts} end]
    ),

    session_setup(Worker2, SessId2, Cred2, Self),

    % Check whether subscription message has been sent to clients.
    ?assertMatch({ok, #write_event_subscription{}}, test_utils:receive_any(?TIMEOUT)),
    ?assertMatch({ok, #write_event_subscription{}}, test_utils:receive_any(?TIMEOUT)),
    ?assertEqual({error, timeout}, test_utils:receive_any()),

    % Emit events.
    lists:foreach(fun(N) ->
        emit(Worker1, #write_event{size = 1, counter = 1, file_size = N + 1,
            blocks = [#file_block{offset = N, size = 1}]}, SessId1)
    end, lists:seq(0, 5)),

    % Check whether handlers have been executed.
    ?assertMatch({ok, _}, test_utils:receive_msg({handler, [#write_event{
        counter = 6, size = 6, file_size = 6,
        blocks = [#file_block{offset = 0, size = 6}]
    }]}, ?TIMEOUT)),
    ?assertEqual({error, timeout}, test_utils:receive_any()),

    FileId1 = <<"file_id_1">>,
    FileId2 = <<"file_id_2">>,

    % Check aggregation of 'different' events.
    lists:foreach(fun(FileId) ->
        lists:foreach(fun(Evt) ->
            emit(Worker1, Evt, SessId2)
        end, lists:duplicate(3, #write_event{
            file_id = FileId, counter = 1, size = 1, file_size = 1
        }))
    end, [FileId1, FileId2]),

    ?assertMatch({ok, _}, test_utils:receive_msg({handler, [
        #write_event{file_id = FileId2, counter = 3, size = 3, file_size = 1},
        #write_event{file_id = FileId1, counter = 3, size = 3, file_size = 1}
    ]}, ?TIMEOUT)),
    ?assertEqual({error, timeout}, test_utils:receive_any()),

    % Unsubscribe and check subscription cancellation message has been sent to
    % clients
    unsubscribe(Worker1, SubId),
    ?assertEqual({ok, #event_subscription_cancellation{id = SubId}},
        test_utils:receive_any(?TIMEOUT)),
    ?assertEqual({ok, #event_subscription_cancellation{id = SubId}},
        test_utils:receive_any(?TIMEOUT)),
    ?assertEqual({error, timeout}, test_utils:receive_any()),

    session_teardown(Worker1, SessId2),
    session_teardown(Worker2, SessId1),

    ok.

%% Test multiple subscription and execution of event handlers.
event_manager_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Self = self(),
    SubsCount = 10,
    EvtsCount = 100,

    % Create subscriptions for events associated with different files.
    {SubIds, FileIds} = lists:unzip(lists:map(fun(N) ->
        FileId = <<"file_id_", (integer_to_binary(N))/binary>>,
        {ok, SubId} = subscribe(Worker,
            gui,
            fun(#write_event{file_id = Id}) -> Id =:= FileId; (_) -> false end,
            fun(Meta) -> Meta >= EvtsCount end,
            [fun(Evts) -> Self ! {handler, Evts} end]
        ),
        {SubId, FileId}
    end, lists:seq(1, SubsCount))),

    % Emit events.
    utils:pforeach(fun(FileId) ->
        lists:foreach(fun(N) ->
            emit(Worker, #write_event{file_id = FileId, size = 1, counter = 1,
                file_size = N + 1, blocks = [#file_block{offset = N, size = 1}]},
                SessId)
        end, lists:seq(0, EvtsCount - 1))
    end, FileIds),

    % Check whether event handlers have been executed.
    lists:foreach(fun(FileId) ->
        ?assertMatch({ok, _}, test_utils:receive_msg({handler, [#write_event{
            file_id = FileId, size = EvtsCount, counter = EvtsCount,
            file_size = EvtsCount, blocks = [#file_block{
                offset = 0, size = EvtsCount
            }]
        }]}, ?TIMEOUT))
    end, FileIds),
    ?assertEqual({error, timeout}, test_utils:receive_any()),

    lists:foreach(fun(SubId) ->
        unsubscribe(Worker, SubId)
    end, SubIds),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(event_stream_test, Config) ->
    Self = self(),
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send, fun
        (#write_event_subscription{} = Msg, _) -> Self ! Msg, ok;
        (#event_subscription_cancellation{} = Msg, _) -> Self ! Msg, ok;
        (_, _) -> ok
    end),
    Config;

init_per_testcase(event_manager_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = <<"session_id">>,
    Cred = #credentials{user_id = <<"user_id">>},
    test_utils:mock_new(Worker, communicator),
    test_utils:mock_expect(Worker, communicator, send, fun
        (_, _) -> ok
    end),
    session_setup(Worker, SessId, Cred, Self),
    [{session_id, SessId} | Config].

end_per_testcase(event_stream_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, communicator),
    test_utils:mock_unload(Workers, communicator),
    Config;

end_per_testcase(event_manager_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    session_teardown(Worker, SessId),
    test_utils:mock_validate(Worker, communicator),
    test_utils:mock_unload(Worker, communicator),
    proplists:delete(session_id, Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new test session.
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node(), SessId :: session:id(),
    Cred :: session:credentials(), Con :: pid()) -> ok.
session_setup(Worker, SessId, Cred, Con) ->
    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, Cred, Con])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Remove existing test session.
%% @end
%%--------------------------------------------------------------------
-spec session_teardown(Worker :: node(), SessId :: session:id()) -> ok.
session_teardown(Worker, SessId) ->
    ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Emits an event.
%% @end
%%--------------------------------------------------------------------
-spec emit(Worker :: node(), Evt :: event_manager:event(), SessId :: session:id()) ->
    ok.
emit(Worker, Evt, SessId) ->
    ?assertEqual(ok, rpc:call(Worker, event_manager, emit, [Evt, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates event subscription.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), Producer :: event_manager:producer(),
    AdmRule :: event_stream:admission_rule(),
    EmRule :: event_stream:emission_rule(),
    Handlers :: [event_stream:event_handler()]) ->
    {ok, SubId :: event_manager:subscription_id()}.
subscribe(Worker, Producer, AdmRule, EmRule, Handlers) ->
    Sub = #write_event_subscription{
        producer = Producer,
        event_stream = ?WRITE_EVENT_STREAM#event_stream{
            metadata = 0,
            admission_rule = AdmRule,
            emission_rule = EmRule,
            handlers = Handlers
        }
    },
    SubAnswer = rpc:call(Worker, event_manager, subscribe, [Sub]),
    ?assertMatch({ok, _}, SubAnswer),
    SubAnswer.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes event subscription.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(Worker :: node(), SubId :: event_manager:subscription_id()) ->
    ok.
unsubscribe(Worker, SubId) ->
    ?assertEqual(ok, rpc:call(Worker, event_manager, unsubscribe, [SubId])).