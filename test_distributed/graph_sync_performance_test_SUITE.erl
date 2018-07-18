%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is used for performance testing of Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(graph_sync_performance_test_SUITE).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("graph_sync_mocks.hrl").
-include("performance_test_utils.hrl").

-define(SSL_OPTS(__Config), [{secure, only_verify_peercert}, {cacerts, get_cacerts(__Config)}]).

%% API
-export([all/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).

-export([
    concurrent_clients_spawning_performance/1, concurrent_clients_spawning_performance_base/1,
    concurrent_active_clients_spawning_performance/1, concurrent_active_clients_spawning_performance_base/1,
    update_propagation_performance/1, update_propagation_performance_base/1,
    subscriptions_performance/1, subscriptions_performance_base/1,
    subscribers_performance/1, subscribers_performance_base/1
]).

-define(PERFORMANCE_TEST_CASES, [
    concurrent_clients_spawning_performance,
    concurrent_active_clients_spawning_performance,
    update_propagation_performance,
    subscriptions_performance,
    subscribers_performance
]).

-define(CT_TEST_CASES, []).

-define(NO_OP_FUN, fun(_) -> ok end).
-define(USER_1_GRI, #gri{type = od_user, id = ?USER_1, aspect = instance}).


% Performance tests parameters
-define(CLIENT_NUM(Value), ?PERF_PARAM(
    client_num, Value, "", "Number of clients."
)).
-define(CLIENT_NUM, ?config(client_num, Config)).

-define(CHANGE_NUM(Value), ?PERF_PARAM(
    change_num, Value, "", "Number of changes sent to every client."
)).
-define(CHANGE_NUM, ?config(change_num, Config)).

-define(TIME_LIMIT_SECONDS(Value), ?PERF_PARAM(
    time_limit, Value, "sec", "Test time limit."
)).
-define(TIME_LIMIT_SECONDS, ?config(time_limit, Config)).

-define(REQUEST_INTERVAL_SECONDS(Value), ?PERF_PARAM(
    request_interval, Value, "", "How often each client performs a GS request."
)).
-define(REQUEST_INTERVAL_SECONDS, ?config(request_interval, Config)).

-define(START_SUBSCRIPTIONS(Value), ?PERF_PARAM(
    start_subscriptions, Value, "", "How many subscriptions of a client exist in the beginning."
)).
-define(START_SUBSCRIPTIONS, ?config(start_subscriptions, Config)).

-define(END_SUBSCRIPTIONS(Value), ?PERF_PARAM(
    end_subscriptions, Value, "", "How many subscriptions of a client exist in the end."
)).
-define(END_SUBSCRIPTIONS, ?config(end_subscriptions, Config)).

-define(START_SUBSCRIBERS(Value), ?PERF_PARAM(
    start_subscribers, Value, "", "How many subscribers for a resource exist in the beginning."
)).
-define(START_SUBSCRIBERS, ?config(start_subscribers, Config)).

-define(END_SUBSCRIBERS(Value), ?PERF_PARAM(
    end_subscribers, Value, "", "How many subscribers for a resource exist in the end."
)).
-define(END_SUBSCRIBERS, ?config(end_subscribers, Config)).

%%%===================================================================
%%% API functions
%%%===================================================================

all() ->
    ?ALL(?CT_TEST_CASES, ?PERFORMANCE_TEST_CASES).

%%%===================================================================
%%% Performance tests
%%%===================================================================

concurrent_clients_spawning_performance(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 5},
        {success_rate, 100},
        {description, "Checks the performance of spawning multiple, parallel GS "
        "clients."},
        {parameters, [?CLIENT_NUM(100)]},
        ?PERF_CFG(small, [?CLIENT_NUM(100)]),
        ?PERF_CFG(medium, [?CLIENT_NUM(500)]),
        ?PERF_CFG(large, [?CLIENT_NUM(1500)])
    ]).
concurrent_clients_spawning_performance_base(Config) ->
    ClientNum = ?CLIENT_NUM,

    %% ----------------------
    %% Start time measurement
    %% ----------------------
    StartTime = os:timestamp(),

    {ok, SupervisorPid, _} = spawn_supervisor(fun spawn_many_clients/2, [
        Config, ClientNum
    ]),

    Time = timer:now_diff(os:timestamp(), StartTime) / 1000,
    AvgPerClient = Time / ClientNum,
    %% ----------------------
    %% End time measurement
    %% ----------------------

    terminate_supervisor(Config, SupervisorPid),

    [
        #parameter{name = clients_spawning_time, value = Time, unit = "ms",
            description = "Time taken by clients spawning and handshaking."},
        #parameter{name = avg_time_per_client, value = AvgPerClient, unit = "ms",
            description = "Average time taken by one client to spawn and handshake."}
    ].


concurrent_active_clients_spawning_performance(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 5},
        {success_rate, 100},
        {description, "Checks the performance of spawning multiple, parallel GS "
        "clients that regularly make a request."},
        {parameters, [?TIME_LIMIT_SECONDS(100), ?REQUEST_INTERVAL_SECONDS(2)]},
        ?PERF_CFG(short, [?CLIENT_NUM(100), ?REQUEST_INTERVAL_SECONDS(2)]),
        ?PERF_CFG(medium, [?CLIENT_NUM(500), ?REQUEST_INTERVAL_SECONDS(2)]),
        ?PERF_CFG(long, [?CLIENT_NUM(1500), ?REQUEST_INTERVAL_SECONDS(2)])
    ]).
concurrent_active_clients_spawning_performance_base(Config) ->
    ClientNum = ?CLIENT_NUM,
    RequestInterval = ?REQUEST_INTERVAL_SECONDS,

    MakeRequestRegularly = fun(Client) ->
        Pid = spawn_link(fun Loop() ->
            receive
                perform_request ->
                    ?assertMatch(
                        {ok, #gs_resp_graph{}},
                        gs_client:graph_request(Client, ?USER_1_GRI, get)
                    ),
                    erlang:send_after(round(timer:seconds(RequestInterval)), self(), perform_request),
                    Loop()
            end
        end),
        erlang:send_after(rand:uniform(round(timer:seconds(RequestInterval))), Pid, perform_request)
    end,

    %% ----------------------
    %% Start time measurement
    %% ----------------------
    StartTime = os:timestamp(),

    {ok, SupervisorPid, _} = spawn_supervisor(fun spawn_many_clients/5, [
        Config, ClientNum, true, ?NO_OP_FUN, MakeRequestRegularly
    ]),

    Time = timer:now_diff(os:timestamp(), StartTime) / 1000,
    AvgPerClient = Time / ClientNum,
    %% ----------------------
    %% End time measurement
    %% ----------------------

    terminate_supervisor(Config, SupervisorPid),

    [
        #parameter{name = clients_spawning_time, value = Time, unit = "ms",
            description = "Time taken by clients spawning and making regular requests."},
        #parameter{name = avg_time_per_client, value = AvgPerClient, unit = "ms",
            description = "Average time taken by one client to spawn and make regular requests."}
    ].


update_propagation_performance(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 100},
        {description, "Checks update propagation times depending on the number "
        "of subscribed clients and record changes."},
        {parameters, [?CLIENT_NUM(20), ?CHANGE_NUM(100)]},
        ?PERF_CFG(small, [?CLIENT_NUM(10), ?CHANGE_NUM(10)]),
        ?PERF_CFG(medium, [?CLIENT_NUM(100), ?CHANGE_NUM(30)]),
        ?PERF_CFG(large, [?CLIENT_NUM(500), ?CHANGE_NUM(50)]),
        ?PERF_CFG(large_single_change, [?CLIENT_NUM(1000), ?CHANGE_NUM(1)])
    ]).
update_propagation_performance_base(Config) ->
    ClientNum = ?CLIENT_NUM,
    ChangeNum = ?CHANGE_NUM,

    Master = self(),

    GathererLoop = fun Loop({ClientMessagesMap, FinishedClients} = _State) ->
        {NewClientMessagesMap, NewFinishedClients} = receive
            {gather_message, ClientRef, Update} ->
                ClientMessages = maps:get(ClientRef, ClientMessagesMap, []),
                NewMessages = lists:usort([Update | ClientMessages]),
                ClientMessagesMap2 = maps:put(ClientRef, NewMessages, ClientMessagesMap),
                FinishedClients2 = case length(NewMessages) of
                    ChangeNum -> [ClientRef | FinishedClients];
                    _ -> FinishedClients
                end,
                {ClientMessagesMap2, FinishedClients2}
        end,
        case length(NewFinishedClients) of
            ClientNum -> Master ! finished;
            _ -> Loop({NewClientMessagesMap, NewFinishedClients})
        end
    end,

    GathererPid = spawn(fun() ->
        GathererLoop({#{}, []})
    end),

    GatherUpdate = fun(Push) ->
        #gs_push_graph{gri = #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, change_type = updated, data = #{<<"name">> := <<"name", Num/binary>>}} = Push,
        GathererPid ! {gather_message, self(), binary_to_integer(Num)}
    end,

    User1Data = (?USER_DATA_WITHOUT_GRI(?USER_1))#{
        <<"gri">> => gs_protocol:gri_to_string(#gri{type = od_user, id = ?USER_1, aspect = instance})
    },

    OnSuccessFun = fun(Client) ->
        ?assertMatch(
            {ok, #gs_resp_graph{result = User1Data}},
            gs_client:graph_request(Client, #gri{
                type = od_user, id = ?USER_1, aspect = instance
            }, get, #{}, true)
        )
    end,


    {ok, SupervisorPid, {Clients, _}} = spawn_supervisor(fun spawn_many_clients/5, [
        Config, ClientNum, true, GatherUpdate, OnSuccessFun
    ]),

    %% ----------------------
    %% Start time measurement
    %% ----------------------
    StartTime = os:timestamp(),

    lists:foreach(fun(Seq) ->
        {ok, #gs_resp_graph{}} = gs_client:graph_request(hd(Clients), #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, update, #{
            <<"name">> => <<"name", (integer_to_binary(Seq))/binary>>
        })
    end, lists:seq(1, ChangeNum)),

    receive finished -> ok end,

    Time = timer:now_diff(os:timestamp(), StartTime) / 1000,
    AvgPerUpdatePerClient = Time / ChangeNum / ClientNum,
    %% ----------------------
    %% End time measurement
    %% ----------------------

    terminate_supervisor(Config, SupervisorPid),

    [
        #parameter{name = updates_propagation_time, value = Time, unit = "ms",
            description = "Time of updates propagation alone."},
        #parameter{name = avg_time_per_client_per_update, value = AvgPerUpdatePerClient, unit = "ms",
            description = "Average time taken to send one update to one client."}
    ].


subscriptions_performance(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 5},
        {success_rate, 100},
        {description, "Checks the performance of adding and removing subscriptions of a client."},
        {parameters, [?START_SUBSCRIPTIONS(0), ?END_SUBSCRIPTIONS(100)]},
        ?PERF_CFG(small, [?START_SUBSCRIPTIONS(0), ?END_SUBSCRIPTIONS(100)]),
        ?PERF_CFG(medium, [?START_SUBSCRIPTIONS(1000), ?END_SUBSCRIPTIONS(1500)]),
        ?PERF_CFG(large, [?START_SUBSCRIPTIONS(10000), ?END_SUBSCRIPTIONS(11000)])
    ]).
subscriptions_performance_base(Config) ->
    StartSubscriptions = ?START_SUBSCRIPTIONS,
    EndSubscriptions = ?END_SUBSCRIPTIONS,

    Client = {client, user, ?USER_1},
    AuthHint = ?THROUGH_GROUP(?GROUP_1),
    SessionId = <<"12345">>,
    GRI = fun(Integer) ->
        #gri{type = od_user, id = integer_to_binary(Integer), aspect = instance}
    end,

    lists:map(fun(Seq) ->
        simulate_subscribe(Config, GRI(Seq), SessionId, Client, AuthHint)
    end, lists:seq(1, StartSubscriptions)),

    %% ----------------------
    %% Start time measurement
    %% ----------------------
    StartTime = os:timestamp(),

    lists:map(fun(Seq) ->
        simulate_subscribe(Config, GRI(Seq), SessionId, Client, AuthHint)
    end, lists:seq(StartSubscriptions + 1, EndSubscriptions)),

    lists:map(fun(Seq) ->
        simulate_unsubscribe(Config, GRI(Seq), SessionId)
    end, lists:seq(StartSubscriptions + 1, EndSubscriptions)),

    Time = timer:now_diff(os:timestamp(), StartTime) / 1000,
    AvgPerSubscription = Time / (EndSubscriptions - StartSubscriptions),
    %% ----------------------
    %% End time measurement
    %% ----------------------

    lists:map(fun(Seq) ->
        simulate_unsubscribe(Config, GRI(Seq), SessionId)
    end, lists:seq(1, StartSubscriptions)),

    [
        #parameter{name = subscribe_unsubscribe_time, value = Time, unit = "ms",
            description = "Time taken to add and remove subscriptions of a client."},
        #parameter{name = avg_time_per_subscription, value = AvgPerSubscription, unit = "ms",
            description = "Average time taken to add and remove one subscription."}
    ].


subscribers_performance(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 5},
        {success_rate, 100},
        {description, "Checks the performance of adding and removing subscribers of a resource."},
        {parameters, [?START_SUBSCRIBERS(0), ?END_SUBSCRIBERS(100)]},
        ?PERF_CFG(small, [?START_SUBSCRIBERS(0), ?END_SUBSCRIBERS(100)]),
        ?PERF_CFG(medium, [?START_SUBSCRIBERS(1000), ?END_SUBSCRIBERS(1500)]),
        ?PERF_CFG(large, [?START_SUBSCRIBERS(10000), ?END_SUBSCRIBERS(11000)])
    ]).
subscribers_performance_base(Config) ->
    StartSubscribers = ?START_SUBSCRIBERS,
    EndSubscribers = ?END_SUBSCRIBERS,

    Client = {client, user, ?USER_1},
    AuthHint = ?THROUGH_GROUP(?GROUP_1),
    SessionId = fun(Integer) ->
        integer_to_binary(Integer)
    end,

    lists:map(fun(Seq) ->
        simulate_subscribe(Config, ?USER_1_GRI, SessionId(Seq), Client, AuthHint)
    end, lists:seq(1, StartSubscribers)),

    %% ----------------------
    %% Start time measurement
    %% ----------------------
    StartTime = os:timestamp(),

    lists:map(fun(Seq) ->
        simulate_subscribe(Config, ?USER_1_GRI, SessionId(Seq), Client, AuthHint)
    end, lists:seq(StartSubscribers + 1, EndSubscribers)),

    lists:map(fun(Seq) ->
        simulate_unsubscribe(Config, ?USER_1_GRI, SessionId(Seq))
    end, lists:seq(StartSubscribers + 1, EndSubscribers)),

    Time = timer:now_diff(os:timestamp(), StartTime) / 1000,
    AvgPerSubscriber = Time / (EndSubscribers - StartSubscribers),
    %% ----------------------
    %% End time measurement
    %% ----------------------

    lists:map(fun(Seq) ->
        simulate_unsubscribe(Config, ?USER_1_GRI, SessionId(Seq))
    end, lists:seq(1, StartSubscribers)),

    [
        #parameter{name = subscribe_unsubscribe_time, value = Time, unit = "ms",
            description = "Time taken to add and remove subscriptions for given resource."},
        #parameter{name = avg_time_per_subscriber, value = AvgPerSubscriber, unit = "ms",
            description = "Average time taken to add and remove one subscriber."}
    ].


%%%===================================================================
%%% Internal functions
%%%===================================================================

spawn_supervisor(Fun, Args) ->
    Master = self(),
    Pid = spawn(fun() ->
        process_flag(trap_exit, true),
        Result = try
            {ok, erlang:apply(Fun, Args)}
        catch Type:Reason ->
            ct:print("Cannot start supervisor due to ~p:~p~nStacktrace: ~p", [
                Type, Reason, erlang:get_stacktrace()
            ]),
            error
        end,
        Master ! Result,
        case Result of
            {ok, _} ->
                % Wait infinitely
                receive finish -> ok end;
            error ->
                ok
        end
    end),
    erlang:monitor(process, Pid),
    receive
        {ok, Res} ->
            {ok, Pid, Res};
        error ->
            error(cannot_start_supervisor)
    after
        timer:minutes(5) ->
            error(timeout)
    end.


terminate_supervisor(Config, Pid) ->
    exit(Pid, kill),
    receive
        {'DOWN', _, process, Pid, _} ->
            ok,
            KeepaliveInterval = rpc:call(random_node(Config), application, get_env, [
                ?CLUSTER_WORKER_APP_NAME, graph_sync_websocket_keepalive, timer:seconds(5)
            ]),
            % Give some time for connection pids to close
            timer:sleep(KeepaliveInterval * 2)
    after
        timer:minutes(5) ->
            error(timeout)
    end.


spawn_many_clients(Config, Number) ->
    spawn_many_clients(Config, Number, true).

spawn_many_clients(Config, Number, RetryFlag) ->
    spawn_many_clients(Config, Number, RetryFlag, ?NO_OP_FUN).

spawn_many_clients(Config, Number, RetryFlag, CallbackFunction) ->
    spawn_many_clients(Config, Number, RetryFlag, CallbackFunction, ?NO_OP_FUN).

% Each client is spawned using an additional process to make the procedure asynchronous.
%   Master -> proxy-process -> graph-sync-client
% All processes are linked, and the master is trapping exit, so to kill all the
% connections it is enough to kill the proxies or graph sync clients.
% The proxy process simply waits on a receive till it's killed.
spawn_many_clients(Config, Number, RetryFlag, CallbackFunction, OnSuccessFun) ->
    Master = self(),
    ProxyPids = lists:map(fun(_) ->
        spawn_link(fun() ->
            try
                % Avoid bursts of requests
                RandomBackoff = rand:uniform(300),
                timer:sleep(RandomBackoff),
                ClientPid = spawn_client(Config, CallbackFunction, OnSuccessFun),
                Master ! {client_pid, self(), ClientPid},
                % Wait infinitely
                receive finish -> ok end
            catch _:_ ->
                % Let the process die silently, later all are checked for success
                ok
            end
        end)
    end, lists:seq(1, Number)),

    GatherConnections = fun
        Loop(Connections) when length(Connections) =:= Number ->
            Connections;
        Loop(Connections) ->
            receive
                {client_pid, Pid, ClientPid} ->
                    Loop([{Pid, ClientPid} | Connections]);
                {'EXIT', Pid, _} ->
                    Loop(Connections)
            after timer:seconds(10) ->
                Connections
            end
    end,

    SuccessfulConnections = GatherConnections([]),
    {SuccessfulProxyPids, SuccessfulClientPids} = lists:unzip(SuccessfulConnections),
    FailedConnections = ProxyPids -- SuccessfulProxyPids,

    case {length(SuccessfulClientPids), RetryFlag} of
        {Number, _} ->
            {SuccessfulClientPids, FailedConnections};
        {_, false} ->
            {SuccessfulClientPids, FailedConnections};
        {NumberOfSuccessful, true} ->
            {NewSuccessful, NewFailed} = spawn_many_clients(
                Config, Number - NumberOfSuccessful, RetryFlag, CallbackFunction, OnSuccessFun
            ),
            {SuccessfulClientPids ++ NewSuccessful, FailedConnections ++ NewFailed}
    end.


spawn_client(Config, CallbackFunction, OnSuccessFun) ->
    {ok, Client, #gs_resp_handshake{identity = {user, ?USER_1}}} = gs_client:start_link(
        get_gs_ws_url(Config),
        {cookie, {?GRAPH_SYNC_SESSION_COOKIE_NAME, ?USER_1_COOKIE}},
        ?SUPPORTED_PROTO_VERSIONS,
        CallbackFunction,
        ?SSL_OPTS(Config)
    ),
    OnSuccessFun(Client),
    Client.


simulate_subscribe(Config, Gri, SessionId, Client, AuthHint) ->
    rpc:call(random_node(Config), gs_persistence, add_subscriber, [Gri, SessionId, Client, AuthHint]),
    rpc:call(random_node(Config), gs_persistence, add_subscription, [SessionId, Gri]).


simulate_unsubscribe(Config, Gri, SessionId) ->
    rpc:call(random_node(Config), gs_persistence, remove_subscriber, [Gri, SessionId]),
    rpc:call(random_node(Config), gs_persistence, remove_subscription, [SessionId, Gri]).


random_node(Config) ->
    Nodes = ?config(cluster_worker_nodes, Config),
    lists:nth(rand:uniform(length(Nodes)), Nodes).


get_gs_ws_url(Config) ->
    NodeIP = test_utils:get_docker_ip(random_node(Config)),
    str_utils:format_bin("wss://~s:~B/", [NodeIP, ?GS_PORT]).


start_gs_listener(Config, Node) ->
    % Set the keepalive interval to a short one to ensure that client connections
    % are killed quickly after a disconnect.
    rpc:call(Node, application, set_env, [
        ?CLUSTER_WORKER_APP_NAME, graph_sync_websocket_keepalive, timer:seconds(5)
    ]),
    ok = rpc:call(Node, application, ensure_started, [cowboy]),
    {ok, _} = rpc:call(Node, cowboy, start_tls, [
        ?GS_LISTENER_ID,
        [
            {port, ?GS_PORT},
            {num_acceptors, ?GS_HTTPS_ACCEPTORS},
            {keyfile, ?TEST_FILE(Config, "web_key.pem")},
            {certfile, ?TEST_FILE(Config, "web_cert.pem")},
            {cacerts, get_cacerts(Config)},
            {verify, verify_peer},
            {ciphers, ssl_utils:safe_ciphers()}
        ],
        #{
            env => #{dispatch => cowboy_router:compile([
                {'_', [
                    {"/[...]", gs_ws_handler, [?GS_EXAMPLE_TRANSLATOR]}
                ]}
            ])}
        }
    ]).


stop_gs_listener(Node) ->
    rpc:call(Node, cowboy, stop_listener, [?GS_LISTENER_ID]).


get_cacerts(Config) ->
    cert_utils:load_ders(?TEST_FILE(Config, "web_cacert.pem")).


%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    ssl:start(),
    [{?LOAD_MODULES, [graph_sync_mocks]} | Config].


init_per_testcase(_, Config) ->
    Nodes = ?config(cluster_worker_nodes, Config),
    [start_gs_listener(Config, N) || N <- Nodes],
    graph_sync_mocks:mock_callbacks(Config),
    process_flag(trap_exit, true),
    Config.


end_per_testcase(_, Config) ->
    Nodes = ?config(cluster_worker_nodes, Config),
    [stop_gs_listener(N) || N <- Nodes],
    graph_sync_mocks:unmock_callbacks(Config).


end_per_suite(_Config) ->
    ssl:stop(),
    ok.
