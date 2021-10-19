%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains helper functions for performance tests using Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(graph_sync_test_utils).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("graph_sync/graph_sync.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-define(NO_OP_FUN, fun(_) -> ok end).

%% API
-export([spawn_clients/3, spawn_clients/4, spawn_clients/5, spawn_clients/6]).
-export([terminate_clients/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Spawns a number of Graph Sync clients, with given Authorizations and Identities.
%% Returns SupervisorPid - a pid that is linked to all the connections, and
%% can be simply killed to cause a complete cleanup.
%% @end
%%--------------------------------------------------------------------
-spec spawn_clients(URL :: string() | binary(), SslOpts :: list(),
    AuthsAndIdentities :: [{gs_protocol:client_auth(), aai:subject()}]) ->
    {ok, SupervisorPid :: pid(), Clients :: [pid()]}.
spawn_clients(URL, SslOpts, AuthsAndIdentities) ->
    spawn_clients(URL, SslOpts, AuthsAndIdentities, true).

%%--------------------------------------------------------------------
%% @doc
%% Spawns a number of Graph Sync clients, with given Authorizations and Identities.
%% Returns SupervisorPid - a pid that is linked to all the connections, and
%% can be simply killed to cause a complete cleanup.
%% RetryFlag indicates if failed connections should be retried or omitted.
%% @end
%%--------------------------------------------------------------------
-spec spawn_clients(URL :: string() | binary(), SslOpts :: list(),
    AuthsAndIdentities :: [{gs_protocol:client_auth(), aai:subject()}],
    RetryFlag :: boolean()) -> {ok, SupervisorPid :: pid(), Clients :: [pid()]}.
spawn_clients(URL, SslOpts, AuthsAndIdentities, RetryFlag) ->
    spawn_clients(URL, SslOpts, AuthsAndIdentities, RetryFlag, ?NO_OP_FUN).

%%--------------------------------------------------------------------
%% @doc
%% Spawns a number of Graph Sync clients, with given Authorizations and Identities.
%% Returns SupervisorPid - a pid that is linked to all the connections, and
%% can be simply killed to cause a complete cleanup.
%% RetryFlag indicates if failed connections should be retried or omitted.
%% CallbackFunction will be called when a push message comes.
%% @end
%%--------------------------------------------------------------------
-spec spawn_clients(URL :: string() | binary(), SslOpts :: list(),
    AuthsAndIdentities :: [{gs_protocol:client_auth(), aai:subject()}],
    RetryFlag :: boolean(), gs_client:push_callback()) ->
    {ok, SupervisorPid :: pid(), Clients :: [pid()]}.
spawn_clients(URL, SslOpts, AuthsAndIdentities, RetryFlag, PushCallback) ->
    spawn_clients(URL, SslOpts, AuthsAndIdentities, RetryFlag, PushCallback, ?NO_OP_FUN).

%%--------------------------------------------------------------------
%% @doc
%% Spawns a number of Graph Sync clients, with given Authorizations and Identities.
%% Returns SupervisorPid - a pid that is linked to all the connections, and
%% can be simply killed to cause a complete cleanup.
%% CallbackFunction will be called when a push message comes.
%% OnSuccessFun will be called upon successful connection.
%% RetryFlag indicates if failed connections should be retried or omitted. The function
%% will retry infinitely, until desired number of clients has been spawned. The code
%% using it sets timetraps, so even upon failure, the infinite loop will be stopped.
%% @end
%%--------------------------------------------------------------------
-spec spawn_clients(URL :: string() | binary(), SslOpts :: list(),
    AuthsAndIdentities :: [{gs_protocol:client_auth(), aai:subject()}],
    RetryFlag :: boolean(), gs_client:push_callback(),
    OnSuccessFun :: fun((gs_client:client_ref()) -> any())) ->
    {ok, SupervisorPid :: pid(), Clients :: [pid()]}.
spawn_clients(URL, SslOpts, AuthsAndIdentities, RetryFlag, PushCallback, OnSuccessFun) ->
    Master = self(),
    {SupervisorPid, _} = spawn_monitor(fun() ->
        process_flag(trap_exit, true),
        Result = try
            {ok, do_spawn_clients(URL, SslOpts, AuthsAndIdentities, RetryFlag, PushCallback, OnSuccessFun)}
        catch Type:Reason:Stacktrace ->
            ct:pal("Cannot start supervisor due to ~p:~p~nStacktrace: ~p", [
                Type, Reason, Stacktrace
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
    receive
        {ok, Clients} ->
            {ok, SupervisorPid, Clients};
        error ->
            error(cannot_start_supervisor)
    after
        timer:minutes(5) ->
            error(timeout)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Terminates all clients linked to given SupervisorPid, grace period is used to
%% allow for connections timeouts and cleanup.
%% @end
%%--------------------------------------------------------------------
-spec terminate_clients(SupervisorPid :: pid(), GracePeriod :: non_neg_integer()) -> ok.
terminate_clients(SupervisorPid, GracePeriod) ->
    exit(SupervisorPid, kill),
    receive
        {'DOWN', _, process, SupervisorPid, _} ->
            % Give some time for connection pids to close
            timer:sleep(GracePeriod)
    after
        timer:minutes(5) ->
            error(timeout)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Internal function for spawn_clients/6.
%% Each client is spawned using an additional process to make the procedure asynchronous.
%%   Master -> SupervisorPid -> graph-sync-client
%% All processes are linked, and the master is trapping exit, so to kill all the
%% connections it is enough to kill the SupervisorPid.
%% The SupervisorPid simply waits on a receive till it's killed.
%% @end
%%--------------------------------------------------------------------
-spec do_spawn_clients(URL :: string() | binary(), SslOpts :: list(),
    AuthsAndIdentities :: [{gs_protocol:client_auth(), aai:subject()}],
    RetryFlag :: boolean(), gs_client:push_callback(),
    OnSuccessFun :: fun((gs_client:client_ref()) -> any())) ->
    Clients :: [pid()].
do_spawn_clients(URL, SslOpts, AuthsAndIdentities, RetryFlag, PushCallback, OnSuccessFun) ->
    NumberOfClients = length(AuthsAndIdentities),
    Supervisor = self(),
    ProxyPids = lists:map(fun({Auth, Identity}) ->
        Pid = spawn_link(fun() ->
            try
                % Avoid bursts of requests
                RandomBackoff = rand:uniform(300),
                timer:sleep(RandomBackoff),
                ClientPid = spawn_client(URL, SslOpts, Auth, Identity, PushCallback, OnSuccessFun),
                Supervisor ! {client_pid, self(), ClientPid},
                % Wait infinitely
                receive finish -> ok end
            catch _:_ ->
                % Let the process die silently, later all are checked for success
                ok
            end
        end),
        {Pid, {Auth, Identity}}
    end, AuthsAndIdentities),

    GatherConnections = fun
        Loop(Connections) when length(Connections) =:= NumberOfClients ->
            Connections;
        Loop(Connections) ->
            receive
                {client_pid, Pid, ClientPid} ->
                    Loop([{ClientPid, proplists:get_value(Pid, ProxyPids)} | Connections]);
                {'EXIT', _, _} ->
                    Loop(Connections)
            after timer:seconds(10) ->
                Connections
            end
    end,

    SuccessfulConnections = GatherConnections([]),
    {SuccessfulClientPids, SuccessfulAuthsAndIdentities} = lists:unzip(SuccessfulConnections),

    case {length(SuccessfulClientPids), RetryFlag} of
        {NumberOfClients, _} ->
            SuccessfulClientPids;
        {_, false} ->
            SuccessfulClientPids;
        {_, true} ->
            Unsuccessful = AuthsAndIdentities -- SuccessfulAuthsAndIdentities,
            SuccessfulClientPids ++ do_spawn_clients(
                URL, SslOpts, Unsuccessful, RetryFlag, PushCallback, OnSuccessFun
            )
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Spawns a single Graph Sync client and evaluates OnSuccess fun.
%% @end
%%--------------------------------------------------------------------
-spec spawn_client(URL :: string() | binary(), SslOpts :: list(),
    gs_protocol:client_auth(), aai:subject(), gs_client:push_callback(),
    OnSuccessFun :: fun((gs_client:client_ref()) -> any())) -> Client :: pid().
spawn_client(URL, SslOpts, Auth, Identity, PushCallback, OnSuccessFun) ->
    spawn_client(URL, SslOpts, Auth, Identity, PushCallback, OnSuccessFun, 20).


spawn_client(URL, SslOpts, Auth, Identity, PushCallback, OnSuccessFun, AttemptsLeft) ->
    case gs_client:start_link(URL, Auth, ?SUPPORTED_PROTO_VERSIONS, PushCallback, SslOpts) of
        {ok, Client, #gs_resp_handshake{identity = Identity}} ->
            OnSuccessFun(Client),
            Client;
        {error, _} = Error ->
            case AttemptsLeft =< 1 of
                true ->
                    ct:print("Failed to spawn GS client, no retries left. Last error was: ~p", [Error]),
                    error(failed_to_spawn_gs_client);
                false ->
                    timer:sleep(rand:uniform(5000)),
                    spawn_client(URL, SslOpts, Auth, Identity, PushCallback, OnSuccessFun, AttemptsLeft - 1)
            end
    end.
