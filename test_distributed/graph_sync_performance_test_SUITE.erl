%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is used for performance tests of Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(graph_sync_performance_test_SUITE).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("graph_sync_mocks.hrl").
-include("performance_test_utils.hrl").
-include_lib("ctool/include/aai/aai.hrl").

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
        {repeats, 3},
        {success_rate, 100},
        {description, "Checks the performance of spawning multiple, parallel GS "
        "clients."},
        {parameters, [?CLIENT_NUM(100)]},
        ?PERF_CFG(small, [?CLIENT_NUM(100)]),
        ?PERF_CFG(medium, [?CLIENT_NUM(500)]),
        ?PERF_CFG(large, [?CLIENT_NUM(1000)])
    ]).
concurrent_clients_spawning_performance_base(Config) ->
    ClientNum = ?CLIENT_NUM,

    ?begin_measurement(clients_spawning_time),
    {ok, SupervisorPid, _} = spawn_clients(Config, ClientNum),
    ?end_measurement(clients_spawning_time),

    ?derive_measurement(clients_spawning_time, avg_time_per_client, fun(M) ->
        M / ClientNum
    end),


    terminate_clients(Config, SupervisorPid),

    [
        ?format_measurement(clients_spawning_time, ms,
            "Time taken by clients spawning and handshaking."),
        ?format_measurement(avg_time_per_client, ms,
            "Average time taken by one client to spawn and handshake.")
    ].


concurrent_active_clients_spawning_performance(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 3},
        {success_rate, 100},
        {description, "Checks the performance of spawning multiple, parallel GS "
        "clients that regularly make a request."},
        {parameters, [?CLIENT_NUM(100), ?REQUEST_INTERVAL_SECONDS(2)]},
        ?PERF_CFG(small, [?CLIENT_NUM(100), ?REQUEST_INTERVAL_SECONDS(2)]),
        ?PERF_CFG(medium, [?CLIENT_NUM(500), ?REQUEST_INTERVAL_SECONDS(2)]),
        ?PERF_CFG(large, [?CLIENT_NUM(1000), ?REQUEST_INTERVAL_SECONDS(2)])
    ]).
concurrent_active_clients_spawning_performance_base(Config) ->
    ClientNum = ?CLIENT_NUM,
    RequestInterval = ?REQUEST_INTERVAL_SECONDS,

    MakeRequestRegularly = fun(Auth) ->
        Pid = spawn_link(fun Loop() ->
            receive
                perform_request ->
                    ?assertMatch(
                        {ok, #gs_resp_graph{}},
                        gs_client:graph_request(Auth, ?USER_1_GRI, get)
                    ),
                    erlang:send_after(round(timer:seconds(RequestInterval)), self(), perform_request),
                    Loop()
            end
        end),
        erlang:send_after(rand:uniform(round(timer:seconds(RequestInterval))), Pid, perform_request)
    end,


    ?begin_measurement(clients_spawning_time),
    {ok, SupervisorPid, _} = spawn_clients(
        Config, ClientNum, true, ?NO_OP_FUN, MakeRequestRegularly
    ),
    ?end_measurement(clients_spawning_time),

    ?derive_measurement(clients_spawning_time, avg_time_per_client, fun(M) ->
        M / ClientNum
    end),


    terminate_clients(Config, SupervisorPid),

    [
        ?format_measurement(clients_spawning_time, ms,
            "Time taken by clients spawning and making regular requests."),
        ?format_measurement(avg_time_per_client, ms,
            "Average time taken by one client to spawn and make regular requests.")
    ].


update_propagation_performance(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 3},
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
        <<"gri">> => gri:serialize(#gri{type = od_user, id = ?USER_1, aspect = instance}),
        <<"revision">> => 1
    },

    OnSuccessFun = fun(Auth) ->
        ?assertMatch(
            {ok, #gs_resp_graph{data = User1Data}},
            gs_client:graph_request(Auth, #gri{
                type = od_user, id = ?USER_1, aspect = instance
            }, get, #{}, true)
        )
    end,

    {ok, SupervisorPid, Auths} = spawn_clients(
        Config, ClientNum, true, GatherUpdate, OnSuccessFun
    ),

    utils:pforeach(fun(Seq) ->
        {ok, #gs_resp_graph{}} = gs_client:graph_request(hd(Auths), #gri{
            type = od_user, id = ?USER_1, aspect = instance
        }, update, #{
            <<"name">> => <<"name", (integer_to_binary(Seq))/binary>>
        })
    end, lists:seq(1, ChangeNum)),

    ?begin_measurement(updates_propagation_time),
    receive finished -> ok end,
    ?end_measurement(updates_propagation_time),

    ?derive_measurement(updates_propagation_time, avg_time_per_client_per_update, fun(M) ->
        M / ChangeNum / ClientNum
    end),


    terminate_clients(Config, SupervisorPid),

    [
        ?format_measurement(updates_propagation_time, ms,
            "Time of updates propagation alone."),
        ?format_measurement(avg_time_per_client_per_update, us,
            "Average time taken to send one update to one client.")
    ].


subscriptions_performance(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 3},
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

    Auth = ?USER(?USER_1),
    AuthHint = ?THROUGH_GROUP(?GROUP_1),
    SessionId = <<"12345">>,
    GRI = fun(Integer) ->
        #gri{type = od_user, id = integer_to_binary(Integer), aspect = instance}
    end,

    lists:map(fun(Seq) ->
        simulate_subscribe(Config, GRI(Seq), SessionId, Auth, AuthHint)
    end, lists:seq(1, StartSubscriptions)),


    ?begin_measurement(subscribe_unsubscribe_time),
    utils:pforeach(fun(Seq) ->
        simulate_subscribe(Config, GRI(Seq), SessionId, Auth, AuthHint)
    end, lists:seq(StartSubscriptions + 1, EndSubscriptions)),

    utils:pforeach(fun(Seq) ->
        simulate_unsubscribe(Config, GRI(Seq), SessionId)
    end, lists:seq(StartSubscriptions + 1, EndSubscriptions)),
    ?end_measurement(subscribe_unsubscribe_time),

    ?derive_measurement(subscribe_unsubscribe_time, avg_time_per_subscription, fun(M) ->
        M / (EndSubscriptions - StartSubscriptions)
    end),

    lists:map(fun(Seq) ->
        simulate_unsubscribe(Config, GRI(Seq), SessionId)
    end, lists:seq(1, StartSubscriptions)),

    [
        ?format_measurement(subscribe_unsubscribe_time, ms,
            "Time taken to add and remove subscriptions of a client."),
        ?format_measurement(avg_time_per_subscription, us,
            "Average time taken to add and remove one subscription.")
    ].


subscribers_performance(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 3},
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

    Auth = ?USER(?USER_1),
    AuthHint = ?THROUGH_GROUP(?GROUP_1),
    SessionId = fun(Integer) ->
        integer_to_binary(Integer)
    end,

    lists:map(fun(Seq) ->
        simulate_subscribe(Config, ?USER_1_GRI, SessionId(Seq), Auth, AuthHint)
    end, lists:seq(1, StartSubscribers)),


    ?begin_measurement(subscribe_unsubscribe_time),
    utils:pforeach(fun(Seq) ->
        simulate_subscribe(Config, ?USER_1_GRI, SessionId(Seq), Auth, AuthHint)
    end, lists:seq(StartSubscribers + 1, EndSubscribers)),

    utils:pforeach(fun(Seq) ->
        simulate_unsubscribe(Config, ?USER_1_GRI, SessionId(Seq))
    end, lists:seq(StartSubscribers + 1, EndSubscribers)),
    ?end_measurement(subscribe_unsubscribe_time),

    ?derive_measurement(subscribe_unsubscribe_time, avg_time_per_subscriber, fun(M) ->
        M / (EndSubscribers - StartSubscribers)
    end),

    lists:map(fun(Seq) ->
        simulate_unsubscribe(Config, ?USER_1_GRI, SessionId(Seq))
    end, lists:seq(1, StartSubscribers)),

    [
        ?format_measurement(subscribe_unsubscribe_time, ms,
            "Time taken to add and remove subscriptions for given resource."),
        ?format_measurement(avg_time_per_subscriber, us,
            "Average time taken to add and remove one subscriber.")
    ].


%%%===================================================================
%%% Internal functions
%%%===================================================================

spawn_clients(Config, ClientNum) ->
    spawn_clients(Config, ClientNum, true, ?NO_OP_FUN, ?NO_OP_FUN).

spawn_clients(Config, ClientNum, RetryFlag, CallbackFunction, OnSuccessFun) ->
    URL = get_gs_ws_url(Config),
    Auth = {token, ?USER_1_TOKEN},
    Identity = ?SUB(user, ?USER_1),
    AuthsAndIdentities = lists:duplicate(ClientNum, {Auth, Identity}),
    graph_sync_test_utils:spawn_clients(
        URL, ssl_opts(Config), AuthsAndIdentities, RetryFlag, CallbackFunction, OnSuccessFun
    ).


terminate_clients(Config, SupervisorPid) ->
    KeepaliveInterval = rpc:call(random_node(Config), application, get_env, [
        ?CLUSTER_WORKER_APP_NAME, graph_sync_websocket_keepalive, timer:seconds(5)
    ]),
    graph_sync_test_utils:terminate_clients(SupervisorPid, KeepaliveInterval * 2).


simulate_subscribe(Config, Gri, SessionId, Auth, AuthHint) ->
    rpc:call(random_node(Config), gs_persistence, add_subscriber, [Gri, SessionId, Auth, AuthHint]),
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


ssl_opts(Config) ->
    [{secure, only_verify_peercert}, {cacerts, get_cacerts(Config)}].


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
