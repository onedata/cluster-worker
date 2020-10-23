%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides services for HA tests.
%%% @end
%%%-------------------------------------------------------------------
-module(ha_test_utils).
-author("Michał Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([start_service/2, set_envs/3, healthcheck_fun/1, stop_service/2]).
-export([assert_service_started/3, assert_healthcheck_done/3, assert_healthcheck_not_done/2]).
-export([flush_and_check_messages/3]).

%%%===================================================================
%%% API
%%%===================================================================

start_service(ServiceName, MasterProc) ->
    Pid = spawn(fun() -> service_proc(ServiceName, MasterProc) end),
    application:set_env(?CLUSTER_WORKER_APP_NAME, ServiceName, Pid),
    ok.

set_envs(Workers, ServiceName, MasterProc) ->
    test_utils:set_env(Workers, ?CLUSTER_WORKER_APP_NAME, ha_test_utils_data, {ServiceName, MasterProc}).

healthcheck_fun(_LastInterval) ->
    {ok, {ServiceName, MasterProc}} = application:get_env(ha_test_utils_data),
    % @TODO VFS-6841 switch to the clock module (all occurrences in this module)
    MasterProc ! {healthcheck, ServiceName, node(), os:timestamp()},
    ok.

stop_service(ServiceName, _MasterProc) ->
    Pid = application:get_env(?CLUSTER_WORKER_APP_NAME, ServiceName, undefined),
    application:unset_env(?CLUSTER_WORKER_APP_NAME, ServiceName),
    Pid ! stop,
    ok.

assert_service_started(ServiceName, ExpectedNode, MinTimestamp) ->
    check_msg_received(ServiceName, ExpectedNode, MinTimestamp, undefined, service_message).

assert_healthcheck_done(ServiceName, ExpectedNode, MinTimestamp) ->
    check_msg_received(ServiceName, ExpectedNode, MinTimestamp, undefined, healthcheck).

assert_healthcheck_not_done(ServiceName, ExpectedNode) ->
    ?assertNotReceivedMatch({healthcheck, ServiceName, ExpectedNode, _}, 5000).

flush_and_check_messages(ServiceName, ExcludedNode, CheckMinTimestamp) ->
    receive
        {ServiceName, Node, Timestamp} ->
            case timer:now_diff(Timestamp, CheckMinTimestamp) > 0 of
                true -> ?assertNotEqual(ExcludedNode, Node);
                false -> ok
            end,
            flush_and_check_messages(ServiceName, ExcludedNode, CheckMinTimestamp)
    after
        0 -> ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

service_proc(ServiceName, MasterProc) ->
    MasterProc ! {service_message, ServiceName, node(), os:timestamp()},
    receive
        stop -> ok
    after
        1000 -> service_proc(ServiceName, MasterProc)
    end.

check_msg_received(ServiceName, ExpectedNode, MinTimestamp, LastMessage, MessageType) ->
    Ans = receive
        {MessageType, ServiceName, Node, Timestamp} -> {ok, Node, Timestamp}
    after
        5000 -> {error, timeout, LastMessage}
    end,
    {ok, TestNode, TestTimestamp} = ?assertMatch({ok, _, _}, Ans),
    case ExpectedNode =:= TestNode andalso timer:now_diff(TestTimestamp, MinTimestamp) >= 0 of
        true -> ok;
        false -> check_msg_received(ServiceName, ExpectedNode, MinTimestamp, Ans, MessageType)
    end.
